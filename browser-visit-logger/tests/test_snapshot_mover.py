"""
Unit tests for native-host/snapshot_mover.py.

Each test sets up an isolated triplet of (source_dir, dest_dir, db_file)
under a temporary directory, then patches both `host` and `snapshot_mover`
module-level constants to point at it.

The mover scans the Downloads filesystem rather than querying the DB, so
tests create source files with the permanent datetime-prefixed filename that
host.py would have assigned at record time.

Run with:
    cd browser-visit-logger
    pytest tests/test_snapshot_mover.py -v
"""
import logging
import os
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import host             # resolved via conftest.py
import snapshot_mover   # resolved via conftest.py

# Fixed ISO timestamps for deterministic filename prefixes.
# TS1 / TS2 share the same UTC date (different times, same day).
# TS3 falls on a different UTC date.
ISO_TS1 = '2024-01-15T10:30:00.000Z'   # prefix: 2024-01-15T10-30-00Z
ISO_TS2 = '2024-01-15T10:31:00.000Z'   # prefix: 2024-01-15T10-31-00Z
ISO_TS3 = '2024-01-16T08:00:00.000Z'   # prefix: 2024-01-16T08-00-00Z


def _snap(iso_ts: str, orig_basename: str) -> str:
    """Build the datetime-prefixed snapshot filename (mirrors background.js logic).

    The prefix is derived from the ISO 8601 timestamp: colons replaced with
    dashes, milliseconds stripped.  The result is the file's permanent name
    for its entire lifetime.
    """
    date_part, time_rest = iso_ts.split('T', 1)
    time_part = time_rest[:8].replace(':', '-')
    return f'{date_part}T{time_part}Z-{orig_basename}'


# ---------------------------------------------------------------------------
# Base test case — isolated paths + DB initialised via host.ensure_db
# ---------------------------------------------------------------------------
class _MoverTestBase(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

        self.source_dir = os.path.join(self.tmp.name, 'downloads')
        self.dest_dir   = os.path.join(self.tmp.name, 'icloud')
        self.db_file    = os.path.join(self.tmp.name, 'visits.db')
        os.makedirs(self.source_dir)
        # dest_dir intentionally NOT created — main() should create the root

        for module, attrs in (
            (host,            {'DOWNLOADS_SNAPSHOTS_DIR': self.source_dir,
                               'ICLOUD_SNAPSHOTS_DIR':    self.dest_dir,
                               'DB_FILE':                 self.db_file}),
            (snapshot_mover,  {'DOWNLOADS_SNAPSHOTS_DIR': self.source_dir,
                               'ICLOUD_SNAPSHOTS_DIR':    self.dest_dir,
                               'DB_FILE':                 self.db_file}),
        ):
            for name, value in attrs.items():
                p = patch.object(module, name, value)
                p.start()
                self.addCleanup(p.stop)

        conn = sqlite3.connect(self.db_file)
        host.ensure_db(conn)
        conn.close()

    # -- helpers --

    def _make_event(self, table, url, iso_timestamp, orig_basename,
                    content=b'data', age_seconds=0, create_source=True):
        """Insert a visit + event row and (optionally) create the source file.

        The source file is created with the permanent datetime-prefixed name
        that background.js assigns at download time (computed via _snap).

        Returns the source file path if create_source=True, else None.
        """
        # Compute the permanent datetime-prefixed basename.
        prefixed = _snap(iso_timestamp, orig_basename)

        conn = sqlite3.connect(self.db_file)
        host.insert_visit(conn, 'ts-visit', url, 'Title')
        # Pass prefixed name directly — _insert_event stores os.path.basename,
        # which is a no-op since prefixed has no directory component.
        host._insert_event(conn, table, url, iso_timestamp,
                           table.replace('_events', ''), prefixed)
        conn.close()

        if create_source:
            path = os.path.join(self.source_dir, prefixed)
            Path(path).write_bytes(content)
            if age_seconds > 0:
                mtime = time.time() - age_seconds
                os.utime(path, (mtime, mtime))
            return path
        return None

    def _dest_info(self, prefixed_basename):
        """Return (date_subdir, dest_filename) for a datetime-prefixed snapshot.

        The filename is unchanged by the move; only the directory differs.
        """
        date_str = prefixed_basename[:10]   # 'YYYY-MM-DD'
        return os.path.join(self.dest_dir, date_str), prefixed_basename

    def _row(self, table, url):
        """Return the (filename, directory) DB row for url in table."""
        conn = sqlite3.connect(self.db_file)
        row = conn.execute(
            f"SELECT filename, directory FROM {table} WHERE url = ?", (url,)
        ).fetchone()
        conn.close()
        return row


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------
class TestMovePass(_MoverTestBase):

    def test_dest_placed_in_utc_date_subdir(self):
        prefixed = _snap(ISO_TS1, 'a.mhtml')
        self._make_event('read_events', 'https://a.com', ISO_TS1, 'a.mhtml',
                         age_seconds=700)
        date_subdir, _ = self._dest_info(prefixed)
        snapshot_mover.main()
        self.assertTrue(os.path.exists(os.path.join(date_subdir, prefixed)))

    def test_old_file_source_is_deleted(self):
        prefixed = _snap(ISO_TS1, 'a.mhtml')
        self._make_event('read_events', 'https://a.com', ISO_TS1, 'a.mhtml',
                         age_seconds=700)
        snapshot_mover.main()
        self.assertFalse(os.path.exists(os.path.join(self.source_dir, prefixed)))

    def test_old_file_db_directory_updated(self):
        prefixed = _snap(ISO_TS1, 'a.mhtml')
        self._make_event('read_events', 'https://a.com', ISO_TS1, 'a.mhtml',
                         age_seconds=700)
        date_subdir, _ = self._dest_info(prefixed)
        snapshot_mover.main()
        row = self._row('read_events', 'https://a.com')
        self.assertEqual(row[0], prefixed)       # filename unchanged
        self.assertEqual(row[1], date_subdir)    # directory updated

    def test_old_file_preserves_content(self):
        prefixed = _snap(ISO_TS1, 'a.mhtml')
        self._make_event('read_events', 'https://a.com', ISO_TS1, 'a.mhtml',
                         content=b'snapshot bytes', age_seconds=700)
        date_subdir, _ = self._dest_info(prefixed)
        snapshot_mover.main()
        moved = Path(date_subdir, prefixed).read_bytes()
        self.assertEqual(moved, b'snapshot bytes')

    def test_moved_file_is_read_only(self):
        prefixed = _snap(ISO_TS1, 'a.mhtml')
        self._make_event('read_events', 'https://a.com', ISO_TS1, 'a.mhtml',
                         age_seconds=700)
        date_subdir, _ = self._dest_info(prefixed)
        snapshot_mover.main()
        mode = os.stat(os.path.join(date_subdir, prefixed)).st_mode & 0o777
        self.assertEqual(mode, 0o444)

    def test_new_file_is_not_moved(self):
        prefixed = _snap(ISO_TS1, 'a.mhtml')
        self._make_event('read_events', 'https://a.com', ISO_TS1, 'a.mhtml',
                         age_seconds=30)    # well under the 1 min threshold
        snapshot_mover.main()
        self.assertTrue(os.path.exists(os.path.join(self.source_dir, prefixed)))
        self.assertEqual(os.listdir(self.dest_dir), [])
        self.assertEqual(self._row('read_events', 'https://a.com')[1], self.source_dir)

    def test_processes_both_read_and_skimmed_event_tables(self):
        pf_r = _snap(ISO_TS1, 'r.mhtml')
        pf_s = _snap(ISO_TS2, 's.mhtml')
        self._make_event('read_events',    'https://r.com', ISO_TS1, 'r.mhtml',
                         age_seconds=700)
        self._make_event('skimmed_events', 'https://s.com', ISO_TS2, 's.mhtml',
                         age_seconds=700)
        ds_r, _ = self._dest_info(pf_r)
        ds_s, _ = self._dest_info(pf_s)
        snapshot_mover.main()
        self.assertTrue(os.path.exists(os.path.join(ds_r, pf_r)))
        self.assertTrue(os.path.exists(os.path.join(ds_s, pf_s)))
        self.assertEqual(self._row('read_events',    'https://r.com'), (pf_r, ds_r))
        self.assertEqual(self._row('skimmed_events', 'https://s.com'), (pf_s, ds_s))

    def test_files_on_different_days_go_to_different_subdirs(self):
        pf1 = _snap(ISO_TS1, 'a.mhtml')  # 2024-01-15
        pf3 = _snap(ISO_TS3, 'b.mhtml')  # 2024-01-16
        self._make_event('read_events', 'https://a.com', ISO_TS1, 'a.mhtml',
                         age_seconds=700)
        self._make_event('read_events', 'https://b.com', ISO_TS3, 'b.mhtml',
                         age_seconds=700)
        ds1, _ = self._dest_info(pf1)
        ds3, _ = self._dest_info(pf3)
        snapshot_mover.main()
        self.assertNotEqual(ds1, ds3)
        self.assertTrue(os.path.exists(os.path.join(ds1, pf1)))
        self.assertTrue(os.path.exists(os.path.join(ds3, pf3)))

    def test_moves_file_even_without_db_row(self):
        # A file in Downloads with no corresponding DB row (e.g., the host.py
        # message was lost) should still be moved to clean up Downloads.
        prefixed = _snap(ISO_TS1, 'no-db-row.mhtml')
        src = os.path.join(self.source_dir, prefixed)
        Path(src).write_bytes(b'data')
        mtime = time.time() - 700
        os.utime(src, (mtime, mtime))

        date_subdir, _ = self._dest_info(prefixed)
        snapshot_mover.main()

        self.assertFalse(os.path.exists(src))
        self.assertTrue(os.path.exists(os.path.join(date_subdir, prefixed)))


# ---------------------------------------------------------------------------
# Idempotency / retry of partial failures
# ---------------------------------------------------------------------------
class TestIdempotency(_MoverTestBase):

    def test_running_twice_is_a_noop_after_success(self):
        prefixed = _snap(ISO_TS1, 'a.mhtml')
        self._make_event('read_events', 'https://a.com', ISO_TS1, 'a.mhtml',
                         age_seconds=700)
        date_subdir, _ = self._dest_info(prefixed)
        snapshot_mover.main()
        snapshot_mover.main()   # source gone — nothing left in Downloads
        self.assertTrue(os.path.exists(os.path.join(date_subdir, prefixed)))
        self.assertFalse(os.path.exists(os.path.join(self.source_dir, prefixed)))
        self.assertEqual(self._row('read_events', 'https://a.com'),
                         (prefixed, date_subdir))

    def test_retry_cleans_up_source_when_db_already_says_icloud(self):
        # Simulate: prior run did copy + DB update but crashed before unlinking.
        # Source still in Downloads; DB already says iCloud.
        prefixed = _snap(ISO_TS1, 'a.mhtml')
        self._make_event('read_events', 'https://a.com', ISO_TS1, 'a.mhtml',
                         age_seconds=700)
        date_subdir, _ = self._dest_info(prefixed)

        # Manually update DB to say iCloud (as if a prior run had done it).
        conn = sqlite3.connect(self.db_file)
        conn.execute("UPDATE read_events SET directory = ? WHERE url = ?",
                     (date_subdir, 'https://a.com'))
        conn.commit()
        conn.close()

        snapshot_mover.main()

        # Source should be cleaned up; DB is unchanged (already correct).
        self.assertFalse(os.path.exists(os.path.join(self.source_dir, prefixed)))
        self.assertEqual(self._row('read_events', 'https://a.com'),
                         (prefixed, date_subdir))

    def test_retry_recovers_from_crash_between_copy_and_db_update(self):
        # Source exists, dest already exists (from prior copy), DB still says Downloads.
        # Next run should overwrite the dest (safe, same data), update DB, unlink source.
        prefixed = _snap(ISO_TS1, 'a.mhtml')
        self._make_event('read_events', 'https://a.com', ISO_TS1, 'a.mhtml',
                         age_seconds=700)
        date_subdir, _ = self._dest_info(prefixed)

        # Pre-create the dest (as if a prior run had already copied it).
        os.makedirs(date_subdir)
        Path(date_subdir, prefixed).write_bytes(b'old-copy')
        # chmod read-only from prior run; copy2 (which re-writes) should still work.
        os.chmod(os.path.join(date_subdir, prefixed), 0o444)
        # Restore write access so copy2 can overwrite (iCloud permissions vary in prod)
        os.chmod(os.path.join(date_subdir, prefixed), 0o644)

        snapshot_mover.main()

        self.assertFalse(os.path.exists(os.path.join(self.source_dir, prefixed)))
        self.assertEqual(self._row('read_events', 'https://a.com'),
                         (prefixed, date_subdir))

    def test_skips_file_with_unrecognized_name_format(self):
        # Files whose names don't match the snapshot format are ignored.
        stray = os.path.join(self.source_dir, 'random-file.mhtml')
        Path(stray).write_bytes(b'x')
        mtime = time.time() - 700
        os.utime(stray, (mtime, mtime))

        snapshot_mover.main()

        self.assertTrue(os.path.exists(stray))
        self.assertEqual(os.listdir(self.dest_dir), [])

    def test_skips_subdirectory_entries_in_downloads(self):
        os.makedirs(os.path.join(self.source_dir, 'subdir'))
        snapshot_mover.main()
        self.assertEqual(os.listdir(self.dest_dir), [])


# ---------------------------------------------------------------------------
# Edge / error paths
# ---------------------------------------------------------------------------
class TestEdgeCases(_MoverTestBase):

    def test_creates_icloud_root_directory_if_absent(self):
        self.assertFalse(os.path.isdir(self.dest_dir))
        snapshot_mover.main()
        self.assertTrue(os.path.isdir(self.dest_dir))

    def test_creates_date_subdir_for_moved_file(self):
        prefixed = _snap(ISO_TS1, 'a.mhtml')
        self._make_event('read_events', 'https://a.com', ISO_TS1, 'a.mhtml',
                         age_seconds=700)
        date_subdir, _ = self._dest_info(prefixed)
        snapshot_mover.main()
        self.assertTrue(os.path.isdir(date_subdir))

    def test_no_db_file_is_a_noop(self):
        os.remove(self.db_file)
        snapshot_mover.main()   # should not raise
        self.assertTrue(os.path.isdir(self.dest_dir))

    def test_downloads_dir_absent_is_a_noop(self):
        os.rmdir(self.source_dir)   # empty, safe to remove
        snapshot_mover.main()       # should not raise
        self.assertTrue(os.path.isdir(self.dest_dir))

    def test_copy_failure_leaves_source_in_downloads(self):
        prefixed = _snap(ISO_TS1, 'a.mhtml')
        self._make_event('read_events', 'https://a.com', ISO_TS1, 'a.mhtml',
                         age_seconds=700)

        with patch('shutil.copy2', side_effect=OSError('disk full')), \
             self.assertLogs(snapshot_mover.logger, level='ERROR') as cm:
            snapshot_mover.main()

        self.assertTrue(any('Failed to move' in m for m in cm.output))
        self.assertTrue(os.path.exists(os.path.join(self.source_dir, prefixed)))
        self.assertEqual(self._row('read_events', 'https://a.com')[1], self.source_dir)


# ---------------------------------------------------------------------------
# Dry-run mode
# ---------------------------------------------------------------------------
class TestDryRun(_MoverTestBase):

    def test_dry_run_does_not_copy_unlink_or_update(self):
        prefixed = _snap(ISO_TS1, 'a.mhtml')
        self._make_event('read_events', 'https://a.com', ISO_TS1, 'a.mhtml',
                         age_seconds=700)

        with self.assertLogs(snapshot_mover.logger, level='INFO') as cm:
            snapshot_mover.main(dry_run=True)

        self.assertTrue(any('[dry-run] would move' in m for m in cm.output))
        # Source untouched, no date subdirs, DB unchanged.
        self.assertTrue(os.path.exists(os.path.join(self.source_dir, prefixed)))
        self.assertEqual(os.listdir(self.dest_dir), [])
        self.assertEqual(self._row('read_events', 'https://a.com')[1], self.source_dir)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
class TestCli(unittest.TestCase):
    """Tests for _parse_args / _apply_args / cli()."""

    def setUp(self):
        self._saved = {
            name: getattr(snapshot_mover, name)
            for name in ('DOWNLOADS_SNAPSHOTS_DIR', 'ICLOUD_SNAPSHOTS_DIR',
                         'DB_FILE', 'MIN_AGE_SECONDS')
        }
        self._saved_level = snapshot_mover.logger.level

    def tearDown(self):
        for name, value in self._saved.items():
            setattr(snapshot_mover, name, value)
        snapshot_mover.logger.setLevel(self._saved_level)

    def test_parse_args_defaults(self):
        ns = snapshot_mover._parse_args([])
        self.assertFalse(ns.dry_run)
        self.assertFalse(ns.verbose)
        self.assertIsNone(ns.min_age_seconds)
        self.assertIsNone(ns.source)
        self.assertIsNone(ns.dest)
        self.assertIsNone(ns.db)

    def test_parse_args_all_flags(self):
        ns = snapshot_mover._parse_args([
            '--dry-run', '--verbose',
            '--min-age-seconds', '120',
            '--source', '/tmp/src',
            '--dest', '/tmp/dst',
            '--db', '/tmp/test.db',
        ])
        self.assertTrue(ns.dry_run)
        self.assertTrue(ns.verbose)
        self.assertEqual(ns.min_age_seconds, 120)
        self.assertEqual(ns.source, '/tmp/src')
        self.assertEqual(ns.dest, '/tmp/dst')
        self.assertEqual(ns.db, '/tmp/test.db')

    def test_apply_args_overrides_paths_and_age(self):
        ns = snapshot_mover._parse_args([
            '--source', '/tmp/src',
            '--dest', '/tmp/dst',
            '--db', '/tmp/test.db',
            '--min-age-seconds', '5',
        ])
        snapshot_mover._apply_args(ns)
        self.assertEqual(snapshot_mover.DOWNLOADS_SNAPSHOTS_DIR, '/tmp/src')
        self.assertEqual(snapshot_mover.ICLOUD_SNAPSHOTS_DIR,    '/tmp/dst')
        self.assertEqual(snapshot_mover.DB_FILE,                 '/tmp/test.db')
        self.assertEqual(snapshot_mover.MIN_AGE_SECONDS,         5)

    def test_apply_args_verbose_sets_debug_log_level(self):
        ns = snapshot_mover._parse_args(['--verbose'])
        snapshot_mover._apply_args(ns)
        self.assertEqual(snapshot_mover.logger.level, logging.DEBUG)

    def test_apply_args_no_flags_leaves_defaults_alone(self):
        original = dict(self._saved)
        snapshot_mover._apply_args(snapshot_mover._parse_args([]))
        for name, value in original.items():
            self.assertEqual(getattr(snapshot_mover, name), value)

    def test_cli_with_nonexistent_db_is_noop_after_overrides(self):
        with tempfile.TemporaryDirectory() as tmp:
            snapshot_mover.cli([
                '--db',     os.path.join(tmp, 'nonexistent.db'),
                '--source', os.path.join(tmp, 'src'),
                '--dest',   os.path.join(tmp, 'dst'),
            ])
            self.assertTrue(os.path.isdir(os.path.join(tmp, 'dst')))

    def test_cli_dry_run_does_not_modify_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            source_dir = os.path.join(tmp, 'src')
            dest_dir   = os.path.join(tmp, 'dst')
            db_file    = os.path.join(tmp, 'visits.db')
            os.makedirs(source_dir)

            with patch.object(host, 'DOWNLOADS_SNAPSHOTS_DIR', source_dir), \
                 patch.object(host, 'ICLOUD_SNAPSHOTS_DIR',    dest_dir):
                conn = sqlite3.connect(db_file)
                host.ensure_db(conn)
                host.insert_visit(conn, 'ts-visit', 'https://a.com', 'Title')
                prefixed = _snap(ISO_TS1, 'a.mhtml')
                host._insert_event(conn, 'read_events', 'https://a.com',
                                   ISO_TS1, 'read', prefixed)
                conn.close()

            file_path = os.path.join(source_dir, prefixed)
            Path(file_path).write_bytes(b'data')
            mtime = time.time() - 700
            os.utime(file_path, (mtime, mtime))

            snapshot_mover.cli([
                '--dry-run',
                '--db', db_file, '--source', source_dir, '--dest', dest_dir,
                '--min-age-seconds', '0',
            ])

            self.assertTrue(os.path.exists(file_path))
            self.assertEqual(os.listdir(dest_dir), [])


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
