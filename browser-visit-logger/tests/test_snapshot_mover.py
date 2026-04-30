"""
Unit tests for native-host/snapshot_mover.py.

Each test sets up an isolated triplet of (source_dir, dest_dir, db_file)
under a temporary directory, then patches both `host` and `snapshot_mover`
module-level constants to point at it.

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
        # dest_dir intentionally NOT created — main() should create it

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

        # Initialise DB with our patched paths
        conn = sqlite3.connect(self.db_file)
        host.ensure_db(conn)
        conn.close()

    # -- helpers --

    def _make_event(self, table, url, timestamp, basename,
                    content=b'data', age_seconds=0, create_source=True):
        """Insert a visit + event row, and (optionally) create the source file."""
        conn = sqlite3.connect(self.db_file)
        host.insert_visit(conn, 'ts-visit', url, 'Title')
        host._insert_event(
            conn, table, url, timestamp,
            table.replace('_events', ''),  # 'read' or 'skimmed'
            basename,
        )
        conn.close()

        if create_source:
            path = os.path.join(self.source_dir, basename)
            Path(path).write_bytes(content)
            if age_seconds > 0:
                mtime = time.time() - age_seconds
                os.utime(path, (mtime, mtime))
            return path
        return None

    def _set_directory(self, table, url, directory):
        """Force the directory column on a row (used to simulate prior runs)."""
        conn = sqlite3.connect(self.db_file)
        conn.execute(
            f"UPDATE {table} SET directory = ? WHERE url = ?",
            (directory, url),
        )
        conn.commit()
        conn.close()

    def _row(self, table, url):
        conn = sqlite3.connect(self.db_file)
        row = conn.execute(
            f"SELECT filename, directory FROM {table} WHERE url = ?", (url,)
        ).fetchone()
        conn.close()
        return row

    def _exists(self, directory, basename):
        return os.path.exists(os.path.join(directory, basename))


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------
class TestMovePass(_MoverTestBase):

    def test_old_file_is_copied_to_dest(self):
        self._make_event('read_events', 'https://a.com', 'ts1', 'a.mhtml',
                         age_seconds=700)
        snapshot_mover.main()
        self.assertTrue(self._exists(self.dest_dir, 'a.mhtml'))

    def test_old_file_source_is_deleted(self):
        self._make_event('read_events', 'https://a.com', 'ts1', 'a.mhtml',
                         age_seconds=700)
        snapshot_mover.main()
        self.assertFalse(self._exists(self.source_dir, 'a.mhtml'))

    def test_old_file_db_directory_updated(self):
        self._make_event('read_events', 'https://a.com', 'ts1', 'a.mhtml',
                         age_seconds=700)
        snapshot_mover.main()
        row = self._row('read_events', 'https://a.com')
        self.assertEqual(row[0], 'a.mhtml')          # filename unchanged
        self.assertEqual(row[1], self.dest_dir)      # directory updated

    def test_old_file_preserves_content(self):
        self._make_event('read_events', 'https://a.com', 'ts1', 'a.mhtml',
                         content=b'snapshot bytes', age_seconds=700)
        snapshot_mover.main()
        moved = Path(self.dest_dir, 'a.mhtml').read_bytes()
        self.assertEqual(moved, b'snapshot bytes')

    def test_new_file_is_not_moved(self):
        self._make_event('read_events', 'https://a.com', 'ts1', 'a.mhtml',
                         age_seconds=60)  # well under 10 min threshold
        snapshot_mover.main()
        self.assertTrue(self._exists(self.source_dir, 'a.mhtml'))
        self.assertFalse(self._exists(self.dest_dir, 'a.mhtml'))
        row = self._row('read_events', 'https://a.com')
        self.assertEqual(row[1], self.source_dir)

    def test_processes_both_read_and_skimmed_event_tables(self):
        self._make_event('read_events',    'https://r.com', 'tsr', 'r.mhtml',
                         age_seconds=700)
        self._make_event('skimmed_events', 'https://s.com', 'tss', 's.mhtml',
                         age_seconds=700)
        snapshot_mover.main()
        self.assertTrue(self._exists(self.dest_dir, 'r.mhtml'))
        self.assertTrue(self._exists(self.dest_dir, 's.mhtml'))
        self.assertEqual(self._row('read_events',    'https://r.com')[1], self.dest_dir)
        self.assertEqual(self._row('skimmed_events', 'https://s.com')[1], self.dest_dir)


# ---------------------------------------------------------------------------
# Idempotency / orphan handling
# ---------------------------------------------------------------------------
class TestIdempotency(_MoverTestBase):

    def test_running_twice_is_a_noop_after_success(self):
        self._make_event('read_events', 'https://a.com', 'ts1', 'a.mhtml',
                         age_seconds=700)
        snapshot_mover.main()
        # Second run: nothing left to do
        snapshot_mover.main()
        # Final state still consistent
        self.assertTrue(self._exists(self.dest_dir, 'a.mhtml'))
        self.assertFalse(self._exists(self.source_dir, 'a.mhtml'))
        self.assertEqual(self._row('read_events', 'https://a.com')[1], self.dest_dir)

    def test_recovers_when_source_missing_but_dest_present_and_db_says_downloads(self):
        # Simulate: previous run did the copy (source -> dest), then crashed
        # before the DB UPDATE.  DB still says directory = downloads, but
        # source is gone and dest exists.
        self._make_event('read_events', 'https://a.com', 'ts1', 'a.mhtml',
                         age_seconds=700, create_source=False)
        Path(self.dest_dir).mkdir(parents=True, exist_ok=True)
        Path(self.dest_dir, 'a.mhtml').write_bytes(b'already-copied')

        snapshot_mover.main()

        # DB row should now reflect that the file lives in iCloud
        self.assertEqual(self._row('read_events', 'https://a.com')[1], self.dest_dir)
        # Dest file untouched
        self.assertEqual(Path(self.dest_dir, 'a.mhtml').read_bytes(), b'already-copied')

    def test_recovers_orphan_source_when_db_already_says_icloud(self):
        # Simulate: previous run copied + updated DB, then crashed before
        # unlinking the source.  DB says iCloud, but source still exists.
        self._make_event('read_events', 'https://a.com', 'ts1', 'a.mhtml',
                         age_seconds=700)
        # Pretend the DB was already updated by an earlier run
        self._set_directory('read_events', 'https://a.com', self.dest_dir)

        snapshot_mover.main()

        # Orphan source now removed
        self.assertFalse(self._exists(self.source_dir, 'a.mhtml'))
        # DB still says iCloud
        self.assertEqual(self._row('read_events', 'https://a.com')[1], self.dest_dir)

    def test_orphan_file_not_referenced_by_db_is_left_alone(self):
        # Drop a stray file into source_dir that has no DB row.
        Path(self.source_dir, 'orphan.mhtml').write_bytes(b'x')
        mtime = time.time() - 700
        os.utime(os.path.join(self.source_dir, 'orphan.mhtml'), (mtime, mtime))

        snapshot_mover.main()

        # File still in source, dest never created (no DB row to drive it)
        self.assertTrue(self._exists(self.source_dir, 'orphan.mhtml'))
        self.assertFalse(self._exists(self.dest_dir, 'orphan.mhtml'))


# ---------------------------------------------------------------------------
# Edge / error paths
# ---------------------------------------------------------------------------
class TestEdgeCases(_MoverTestBase):

    def test_creates_icloud_directory_if_absent(self):
        # dest_dir is NOT created by setUp; main() should mkdir it.
        self.assertFalse(os.path.isdir(self.dest_dir))
        snapshot_mover.main()
        self.assertTrue(os.path.isdir(self.dest_dir))

    def test_no_db_file_is_a_noop(self):
        # Remove the DB created in setUp; main() should log + return.
        os.remove(self.db_file)
        # Should not raise
        snapshot_mover.main()
        # iCloud dir still gets created (mkdir runs before the DB check)
        self.assertTrue(os.path.isdir(self.dest_dir))

    def test_missing_source_and_dest_logs_warning(self):
        # DB row points at source, but neither source nor dest file exists.
        self._make_event('read_events', 'https://a.com', 'ts1', 'a.mhtml',
                         create_source=False)

        with self.assertLogs(snapshot_mover.logger, level='WARNING') as cm:
            snapshot_mover.main()

        self.assertTrue(any('missing from both' in m for m in cm.output))
        # DB row unchanged
        self.assertEqual(self._row('read_events', 'https://a.com')[1], self.source_dir)

    def test_copy_failure_leaves_db_and_source_unchanged(self):
        self._make_event('read_events', 'https://a.com', 'ts1', 'a.mhtml',
                         age_seconds=700)

        with patch('shutil.copy2', side_effect=OSError('disk full')), \
             self.assertLogs(snapshot_mover.logger, level='ERROR') as cm:
            snapshot_mover.main()

        # Error logged, source still present, DB still says downloads
        self.assertTrue(any('Failed to move' in m for m in cm.output))
        self.assertTrue(self._exists(self.source_dir, 'a.mhtml'))
        self.assertEqual(self._row('read_events', 'https://a.com')[1], self.source_dir)

    def test_orphan_sweep_unlink_failure_is_logged(self):
        # Set up an orphan: source exists, DB already says iCloud.
        self._make_event('read_events', 'https://a.com', 'ts1', 'a.mhtml',
                         age_seconds=700)
        self._set_directory('read_events', 'https://a.com', self.dest_dir)

        with patch('os.unlink', side_effect=OSError('locked')), \
             self.assertLogs(snapshot_mover.logger, level='ERROR') as cm:
            snapshot_mover.main()

        self.assertTrue(any('Failed to remove orphan source' in m for m in cm.output))
        # Source still present (unlink raised)
        self.assertTrue(self._exists(self.source_dir, 'a.mhtml'))


# ---------------------------------------------------------------------------
# Dry-run mode
# ---------------------------------------------------------------------------
class TestDryRun(_MoverTestBase):

    def test_dry_run_does_not_copy_or_unlink_or_update(self):
        self._make_event('read_events', 'https://a.com', 'ts1', 'a.mhtml',
                         age_seconds=700)

        with self.assertLogs(snapshot_mover.logger, level='INFO') as cm:
            snapshot_mover.main(dry_run=True)

        # Logged the would-do action
        self.assertTrue(any('[dry-run] would move' in m for m in cm.output))
        # Source untouched, dest never created, DB unchanged
        self.assertTrue(self._exists(self.source_dir, 'a.mhtml'))
        self.assertFalse(self._exists(self.dest_dir, 'a.mhtml'))
        self.assertEqual(self._row('read_events', 'https://a.com')[1], self.source_dir)

    def test_dry_run_does_not_unlink_orphan_source(self):
        # Orphan: source still in Downloads, DB already says iCloud.
        self._make_event('read_events', 'https://a.com', 'ts1', 'a.mhtml',
                         age_seconds=700)
        self._set_directory('read_events', 'https://a.com', self.dest_dir)

        with self.assertLogs(snapshot_mover.logger, level='INFO') as cm:
            snapshot_mover.main(dry_run=True)

        self.assertTrue(any('[dry-run] would remove orphan source' in m for m in cm.output))
        # Source still present
        self.assertTrue(self._exists(self.source_dir, 'a.mhtml'))

    def test_dry_run_does_not_reconcile_db_when_source_missing(self):
        # Source missing, dest present, DB still says downloads → would
        # normally reconcile via DB UPDATE; dry-run should only log.
        self._make_event('read_events', 'https://a.com', 'ts1', 'a.mhtml',
                         age_seconds=700, create_source=False)
        Path(self.dest_dir).mkdir(parents=True, exist_ok=True)
        Path(self.dest_dir, 'a.mhtml').write_bytes(b'already-copied')

        with self.assertLogs(snapshot_mover.logger, level='INFO') as cm:
            snapshot_mover.main(dry_run=True)

        self.assertTrue(any('[dry-run] would reconcile' in m for m in cm.output))
        # DB row unchanged
        self.assertEqual(self._row('read_events', 'https://a.com')[1], self.source_dir)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
class TestCli(unittest.TestCase):
    """Tests for _parse_args / _apply_args / cli().

    These tests do NOT use _MoverTestBase because they're exercising the
    argument-application path (which mutates module globals).  They snapshot
    and restore the module attributes themselves.
    """

    def setUp(self):
        # Snapshot all globals cli() may mutate, so each test starts clean.
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

    # -- _parse_args --

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

    # -- _apply_args --

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

    # -- cli (end-to-end) --

    def test_cli_with_nonexistent_db_is_noop_after_overrides(self):
        # Point at a temp dir; DB doesn't exist → main() returns early.
        with tempfile.TemporaryDirectory() as tmp:
            snapshot_mover.cli([
                '--db',     os.path.join(tmp, 'nonexistent.db'),
                '--source', os.path.join(tmp, 'src'),
                '--dest',   os.path.join(tmp, 'dst'),
            ])
            # main() created the dest dir before checking the DB
            self.assertTrue(os.path.isdir(os.path.join(tmp, 'dst')))

    def test_cli_dry_run_does_not_modify_state(self):
        # Set up a real DB + source file via the helpers, then drive via cli().
        with tempfile.TemporaryDirectory() as tmp:
            source_dir = os.path.join(tmp, 'src')
            dest_dir   = os.path.join(tmp, 'dst')
            db_file    = os.path.join(tmp, 'visits.db')
            os.makedirs(source_dir)

            # Init DB with overridden paths so the DEFAULT clause embeds them.
            with patch.object(host, 'DOWNLOADS_SNAPSHOTS_DIR', source_dir), \
                 patch.object(host, 'ICLOUD_SNAPSHOTS_DIR',    dest_dir):
                conn = sqlite3.connect(db_file)
                host.ensure_db(conn)
                host.insert_visit(conn, 'ts-visit', 'https://a.com', 'Title')
                host._insert_event(conn, 'read_events', 'https://a.com',
                                   'ts-read', 'read', 'a.mhtml')
                conn.close()

            file_path = os.path.join(source_dir, 'a.mhtml')
            Path(file_path).write_bytes(b'data')
            mtime = time.time() - 700
            os.utime(file_path, (mtime, mtime))

            snapshot_mover.cli([
                '--dry-run',
                '--db', db_file, '--source', source_dir, '--dest', dest_dir,
                '--min-age-seconds', '0',
            ])

            # Source untouched
            self.assertTrue(os.path.exists(file_path))
            # Dest dir created (mkdir runs even in dry-run) but no files in it
            self.assertEqual(os.listdir(dest_dir), [])


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
