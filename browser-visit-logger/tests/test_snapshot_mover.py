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
import datetime
import errno
import io
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
        snapshot_mover._ensure_snapshots_table(conn)
        snapshot_mover._ensure_mover_errors_table(conn)
        conn.close()

        # Silence _notify_user so tests that incidentally trigger
        # escalation don't actually fire osascript on macOS.  Tests that
        # want to assert on notification calls re-patch in their body —
        # patch.object's stack semantics hide this outer mock for the
        # duration of the inner with block.
        p = patch.object(snapshot_mover, '_notify_user')
        p.start()
        self.addCleanup(p.stop)

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

    def test_move_pass_inserts_snapshots_row_for_new_date(self):
        # Moving a file into a new daily directory should also INSERT OR IGNORE
        # a row into the snapshots table so the seal pass can find it later.
        # Drive _move_pass directly (rather than main()) so the seal pass
        # doesn't immediately flip sealed=0 to 1 in the same call.
        self._make_event('read_events', 'https://a.com', ISO_TS1, 'a.mhtml',
                         age_seconds=700)
        os.makedirs(snapshot_mover.ICLOUD_SNAPSHOTS_DIR, exist_ok=True)
        conn = sqlite3.connect(self.db_file)
        try:
            snapshot_mover._move_pass(conn)
            row = conn.execute(
                "SELECT date, sealed FROM snapshots WHERE date = ?",
                (ISO_TS1[:10],),
            ).fetchone()
        finally:
            conn.close()
        self.assertEqual(row, (ISO_TS1[:10], 0))

    def test_move_pass_does_not_duplicate_snapshots_row(self):
        # Two files for the same UTC date should yield exactly one snapshots
        # row (PRIMARY KEY + INSERT OR IGNORE).
        self._make_event('read_events',    'https://a.com', ISO_TS1, 'a.mhtml',
                         age_seconds=700)
        self._make_event('skimmed_events', 'https://b.com', ISO_TS2, 'b.mhtml',
                         age_seconds=700)
        os.makedirs(snapshot_mover.ICLOUD_SNAPSHOTS_DIR, exist_ok=True)
        conn = sqlite3.connect(self.db_file)
        try:
            snapshot_mover._move_pass(conn)
            count = conn.execute(
                "SELECT COUNT(*) FROM snapshots WHERE date = ?", (ISO_TS1[:10],)
            ).fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(count, 1)

    def test_straggler_rewrites_manifest_to_include_new_file(self):
        # Pre-condition: a sealed daily dir on disk + DB, with one prior file
        # and a manifest listing only that file.
        date_str = ISO_TS1[:10]
        date_subdir = os.path.join(self.dest_dir, date_str)
        os.makedirs(date_subdir)
        pre_existing = _snap(ISO_TS1, 'old.mhtml')
        Path(date_subdir, pre_existing).write_bytes(b'old')
        os.chmod(os.path.join(date_subdir, pre_existing), 0o444)

        conn = sqlite3.connect(self.db_file)
        host.insert_visit(conn, 'ts-visit', 'https://old.com', 'Old Page')
        conn.execute(
            "INSERT INTO read_events (url, timestamp, filename, directory) "
            "VALUES (?, ?, ?, ?)",
            ('https://old.com', ISO_TS1, pre_existing, date_subdir),
        )
        manifest_path = os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)
        Path(manifest_path).write_text(
            'filename\ttag\ttimestamp\turl\ttitle\n'
            f'{pre_existing}\tread\t{ISO_TS1}\thttps://old.com\tOld Page\n',
            encoding='utf-8',
        )
        os.chmod(manifest_path, 0o444)
        conn.execute(
            "INSERT INTO snapshots (date, sealed) VALUES (?, 1)", (date_str,))
        conn.commit()
        conn.close()

        # A straggler shows up in Downloads with a date prefix matching the
        # already-sealed day.
        self._make_event('read_events', 'https://new.com', ISO_TS1, 'new.mhtml',
                         age_seconds=700)

        os.makedirs(snapshot_mover.ICLOUD_SNAPSHOTS_DIR, exist_ok=True)
        conn = sqlite3.connect(self.db_file)
        try:
            snapshot_mover._move_pass(conn)
        finally:
            conn.close()

        new_filename = _snap(ISO_TS1, 'new.mhtml')
        # Both files are present in the date dir.
        self.assertTrue(os.path.exists(os.path.join(date_subdir, pre_existing)))
        self.assertTrue(os.path.exists(os.path.join(date_subdir, new_filename)))
        # Manifest now lists the new file in addition to the old one.
        manifest = Path(manifest_path).read_text(encoding='utf-8')
        self.assertIn(pre_existing, manifest)
        self.assertIn(new_filename, manifest)
        # Manifest stayed read-only after the rewrite.
        self.assertEqual(os.stat(manifest_path).st_mode & 0o777, 0o444)
        # Sealed flag is still 1 — the day stays sealed, just with a fresh
        # manifest reflecting the new file.
        conn = sqlite3.connect(self.db_file)
        sealed = conn.execute(
            "SELECT sealed FROM snapshots WHERE date = ?", (date_str,)
        ).fetchone()[0]
        conn.close()
        self.assertEqual(sealed, 1)

    def test_non_straggler_move_does_not_write_manifest(self):
        # When the snapshots row is sealed=0 (or absent), the move pass
        # must not pre-emptively create a manifest — that's the seal pass's job.
        self._make_event('read_events', 'https://a.com', ISO_TS1, 'a.mhtml',
                         age_seconds=700)
        os.makedirs(snapshot_mover.ICLOUD_SNAPSHOTS_DIR, exist_ok=True)
        conn = sqlite3.connect(self.db_file)
        try:
            snapshot_mover._move_pass(conn)
        finally:
            conn.close()
        manifest = os.path.join(self.dest_dir, ISO_TS1[:10],
                                snapshot_mover.MANIFEST_FILENAME)
        self.assertFalse(os.path.exists(manifest))

    def test_straggler_rewrite_failure_is_logged_and_does_not_undo_move(self):
        # If rewriting the manifest fails (e.g. disk full), the file move
        # has already committed and must stay; only an ERROR log is emitted.
        date_str = ISO_TS1[:10]
        conn = sqlite3.connect(self.db_file)
        conn.execute(
            "INSERT INTO snapshots (date, sealed) VALUES (?, 1)", (date_str,))
        conn.commit()
        conn.close()

        self._make_event('read_events', 'https://a.com', ISO_TS1, 'a.mhtml',
                         age_seconds=700)
        os.makedirs(snapshot_mover.ICLOUD_SNAPSHOTS_DIR, exist_ok=True)

        with patch.object(snapshot_mover, '_write_manifest_file',
                          side_effect=OSError('disk full')), \
             self.assertLogs(snapshot_mover.logger, level='ERROR') as cm:
            conn = sqlite3.connect(self.db_file)
            try:
                snapshot_mover._move_pass(conn)
            finally:
                conn.close()

        self.assertTrue(any('Failed to rewrite' in m for m in cm.output))
        moved = os.path.join(self.dest_dir, date_str, _snap(ISO_TS1, 'a.mhtml'))
        self.assertTrue(os.path.exists(moved))

    def test_move_pass_preserves_sealed_flag_on_late_arrival(self):
        # If a late file arrives for an already-sealed day, INSERT OR IGNORE
        # must not flip sealed=1 back to 0.
        conn = sqlite3.connect(self.db_file)
        conn.execute(
            "INSERT INTO snapshots (date, sealed) VALUES (?, 1)",
            (ISO_TS1[:10],),
        )
        conn.commit()
        conn.close()

        self._make_event('read_events', 'https://a.com', ISO_TS1, 'late.mhtml',
                         age_seconds=700)
        os.makedirs(snapshot_mover.ICLOUD_SNAPSHOTS_DIR, exist_ok=True)
        conn = sqlite3.connect(self.db_file)
        try:
            snapshot_mover._move_pass(conn)
            sealed = conn.execute(
                "SELECT sealed FROM snapshots WHERE date = ?", (ISO_TS1[:10],)
            ).fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(sealed, 1)


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
        # Files whose names don't match the snapshot format are left in
        # Downloads (not moved), ERROR-logged, and recorded as an
        # 'invalid_filename' mover_errors row so the user is notified.
        stray = os.path.join(self.source_dir, 'random-file.mhtml')
        Path(stray).write_bytes(b'x')
        mtime = time.time() - 700
        os.utime(stray, (mtime, mtime))

        with self.assertLogs(snapshot_mover.logger, level='ERROR') as cm:
            snapshot_mover.main()

        self.assertTrue(os.path.exists(stray))
        self.assertEqual(os.listdir(self.dest_dir), [])
        self.assertTrue(any('does not match snapshot filename format'
                            in m for m in cm.output))
        conn = sqlite3.connect(self.db_file)
        row = conn.execute(
            "SELECT operation, target FROM mover_errors "
            "WHERE operation = 'invalid_filename'"
        ).fetchone()
        conn.close()
        self.assertEqual(row, ('invalid_filename', stray))

    def test_move_pass_clears_invalid_filename_error_when_stray_removed(self):
        # Pre-seed an invalid_filename row for a path that no longer exists
        # in Downloads.  The move-pass reconcile should clear it.
        gone = os.path.join(self.source_dir, 'never-existed.mhtml')
        conn = sqlite3.connect(self.db_file)
        snapshot_mover._record_error(
            conn, 'invalid_filename', gone,
            ValueError('synthetic'))
        conn.close()
        os.makedirs(snapshot_mover.ICLOUD_SNAPSHOTS_DIR, exist_ok=True)

        snapshot_mover.main()   # source_dir empty, gone is not present

        conn = sqlite3.connect(self.db_file)
        count = conn.execute(
            "SELECT COUNT(*) FROM mover_errors WHERE operation = 'invalid_filename'"
        ).fetchone()[0]
        conn.close()
        self.assertEqual(count, 0)

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
# Seal pass — write read-only MANIFEST.tsv into finished daily directories
# ---------------------------------------------------------------------------
class TestSealPass(_MoverTestBase):
    """Tests for the automatic seal pass invoked by main()."""

    # All tests in this class pin "today" to a fixed UTC date so that the
    # date-comparison logic is deterministic.  Sealable dirs are dated before
    # this; "today or future" dirs are dated on or after this.
    TODAY = datetime.date(2024, 1, 20)

    def setUp(self):
        super().setUp()
        p = patch.object(snapshot_mover, '_today_utc', return_value=self.TODAY)
        p.start()
        self.addCleanup(p.stop)

    # -- helpers --

    def _seed_dir(self, date_str, files=(), seed_snapshots_row=True, sealed=0):
        """Create dest_dir/<date_str>/ on disk and (by default) insert a
        matching row into the snapshots table.

        files is an iterable of (filename, table, url, ts, title) tuples.
        Pass table=None to leave a file with no DB row.  Returns the absolute
        path of the seeded date subdir.

        seed_snapshots_row=False simulates a directory that exists on disk
        but isn't tracked by the table (e.g. a directory imported from
        elsewhere); the auto seal pass should never touch it.
        """
        date_subdir = os.path.join(self.dest_dir, date_str)
        os.makedirs(date_subdir, exist_ok=True)
        conn = sqlite3.connect(self.db_file)
        try:
            if seed_snapshots_row:
                conn.execute(
                    "INSERT OR IGNORE INTO snapshots (date, sealed) VALUES (?, ?)",
                    (date_str, sealed),
                )
                conn.commit()
            for filename, table, url, ts, title in files:
                Path(date_subdir, filename).write_bytes(b'data')
                if table is None:
                    continue
                host.insert_visit(conn, ts, url, title)
                conn.execute(
                    f"INSERT INTO {table} (url, timestamp, filename, directory) "
                    f"VALUES (?, ?, ?, ?)",
                    (url, ts, filename, date_subdir),
                )
                conn.commit()
        finally:
            conn.close()
        return date_subdir

    def _snapshots_row(self, date_str):
        """Return the (date, sealed) row for date_str, or None."""
        conn = sqlite3.connect(self.db_file)
        try:
            return conn.execute(
                "SELECT date, sealed FROM snapshots WHERE date = ?",
                (date_str,),
            ).fetchone()
        finally:
            conn.close()

    def _read_manifest(self, date_str):
        return Path(self.dest_dir, date_str,
                    snapshot_mover.MANIFEST_FILENAME).read_text(encoding='utf-8')

    # -- tests --

    def test_creates_manifest_for_past_date_subdir(self):
        date_subdir = self._seed_dir('2024-01-15')
        snapshot_mover.main()
        self.assertTrue(os.path.exists(
            os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)))

    def test_does_not_seal_today(self):
        date_subdir = self._seed_dir('2024-01-20')
        snapshot_mover.main()
        self.assertFalse(os.path.exists(
            os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)))

    def test_does_not_seal_future_date(self):
        date_subdir = self._seed_dir('2024-02-01')
        snapshot_mover.main()
        self.assertFalse(os.path.exists(
            os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)))

    def test_manifest_is_read_only(self):
        date_subdir = self._seed_dir('2024-01-15')
        snapshot_mover.main()
        manifest = os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)
        mode = os.stat(manifest).st_mode & 0o777
        self.assertEqual(mode, 0o444)

    def test_manifest_starts_with_header_row(self):
        self._seed_dir('2024-01-15')
        snapshot_mover.main()
        first_line = self._read_manifest('2024-01-15').split('\n', 1)[0]
        self.assertEqual(first_line, 'filename\ttag\ttimestamp\turl\ttitle')

    def test_manifest_lists_every_snapshot_file(self):
        self._seed_dir('2024-01-15', [
            ('2024-01-15T10-00-00Z-a.mhtml', 'read_events',
             'https://a.com', '2024-01-15T10:00:00Z', 'A'),
            ('2024-01-15T11-00-00Z-b.mhtml', 'skimmed_events',
             'https://b.com', '2024-01-15T11:00:00Z', 'B'),
        ])
        snapshot_mover.main()
        lines = self._read_manifest('2024-01-15').splitlines()
        # Header + 2 data rows
        self.assertEqual(len(lines), 3)

    def test_manifest_rows_in_filename_order(self):
        # Filenames sort chronologically thanks to the datetime prefix —
        # the manifest order should follow.
        self._seed_dir('2024-01-15', [
            ('2024-01-15T11-00-00Z-late.mhtml',  'read_events',
             'https://late.com',  '2024-01-15T11:00:00Z', 'L'),
            ('2024-01-15T09-00-00Z-early.mhtml', 'read_events',
             'https://early.com', '2024-01-15T09:00:00Z', 'E'),
        ])
        snapshot_mover.main()
        lines = self._read_manifest('2024-01-15').splitlines()
        self.assertIn('early.mhtml', lines[1])
        self.assertIn('late.mhtml',  lines[2])

    def test_manifest_includes_url_and_title_from_visits(self):
        self._seed_dir('2024-01-15', [
            ('2024-01-15T10-00-00Z-a.mhtml', 'read_events',
             'https://example.com', '2024-01-15T10:00:00Z', 'Example Page'),
        ])
        snapshot_mover.main()
        manifest = self._read_manifest('2024-01-15')
        self.assertIn('https://example.com', manifest)
        self.assertIn('Example Page', manifest)

    def test_manifest_distinguishes_read_and_skimmed_tags(self):
        self._seed_dir('2024-01-15', [
            ('2024-01-15T10-00-00Z-r.mhtml', 'read_events',
             'https://r.com', '2024-01-15T10:00:00Z', 'R'),
            ('2024-01-15T11-00-00Z-s.mhtml', 'skimmed_events',
             'https://s.com', '2024-01-15T11:00:00Z', 'S'),
        ])
        snapshot_mover.main()
        manifest = self._read_manifest('2024-01-15')
        for line in manifest.splitlines()[1:]:
            fields = line.split('\t')
            if 'r.mhtml' in fields[0]:
                self.assertEqual(fields[1], 'read')
            elif 's.mhtml' in fields[0]:
                self.assertEqual(fields[1], 'skimmed')

    def test_manifest_excludes_orphan_files_and_records_orphan_error(self):
        # A conforming snapshot file with no events row is an orphan.
        # Per the "correct sealed directory" invariant, orphans must
        # NOT appear in the manifest — the mover excludes them and
        # records an 'orphan_file' mover_errors row instead.
        date_subdir = self._seed_dir('2024-01-15', [
            ('2024-01-15T10-00-00Z-orphan.mhtml', None, None, None, None),
        ])
        snapshot_mover.main()
        lines = self._read_manifest('2024-01-15').splitlines()
        # Header only — orphan excluded.
        self.assertEqual(lines, ['filename\ttag\ttimestamp\turl\ttitle'])
        conn = sqlite3.connect(self.db_file)
        row = conn.execute(
            "SELECT operation, target FROM mover_errors "
            "WHERE operation = 'orphan_file'"
        ).fetchone()
        conn.close()
        expected_target = os.path.join(
            date_subdir, '2024-01-15T10-00-00Z-orphan.mhtml')
        self.assertEqual(row, ('orphan_file', expected_target))

    def test_manifest_clears_orphan_file_error_when_events_row_appears(self):
        # Pre-seed an orphan_file error, then add the missing events row
        # and re-run main().  Reconcile should clear the error.
        date_str = '2024-01-15'
        date_subdir = self._seed_dir(date_str, [
            ('2024-01-15T10-00-00Z-x.mhtml', None, None, None, None),
        ])
        orphan_path = os.path.join(date_subdir, '2024-01-15T10-00-00Z-x.mhtml')
        conn = sqlite3.connect(self.db_file)
        snapshot_mover._record_error(
            conn, 'orphan_file', orphan_path,
            ValueError('previous orphan'))
        # Now add the missing events row so the file is no longer an orphan.
        host.insert_visit(conn, '2024-01-15T10:00:00Z', 'https://x.com', 'X')
        conn.execute(
            "INSERT INTO read_events (url, timestamp, filename, directory) "
            "VALUES (?, ?, ?, ?)",
            ('https://x.com', '2024-01-15T10:00:00Z',
             '2024-01-15T10-00-00Z-x.mhtml', date_subdir),
        )
        conn.commit()
        conn.close()

        snapshot_mover.main()

        conn = sqlite3.connect(self.db_file)
        count = conn.execute(
            "SELECT COUNT(*) FROM mover_errors WHERE operation = 'orphan_file'"
        ).fetchone()[0]
        conn.close()
        self.assertEqual(count, 0)

    def test_recovery_rewrites_existing_manifest_and_marks_sealed(self):
        # Crash-recovery branch: a prior run wrote the manifest but didn't
        # update the DB.  The manifest may be partial / truncated, so the
        # seal pass *always* rewrites it — and then flips sealed=1.
        date_subdir = self._seed_dir('2024-01-15')
        manifest = os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)
        Path(manifest).write_text('truncated-or-stale\n', encoding='utf-8')
        snapshot_mover.main()
        # Manifest replaced with a freshly-written one (header line at top).
        contents = Path(manifest).read_text(encoding='utf-8')
        self.assertNotEqual(contents, 'truncated-or-stale\n')
        self.assertTrue(contents.startswith('filename\ttag\ttimestamp\turl\ttitle\n'))
        # DB row was flipped to sealed.
        self.assertEqual(self._snapshots_row('2024-01-15'), ('2024-01-15', 1))

    def test_seal_processes_multiple_past_directories(self):
        d1 = self._seed_dir('2024-01-10')
        d2 = self._seed_dir('2024-01-15')
        snapshot_mover.main()
        for d in (d1, d2):
            self.assertTrue(os.path.exists(
                os.path.join(d, snapshot_mover.MANIFEST_FILENAME)))

    def test_dry_run_does_not_write_manifest(self):
        date_subdir = self._seed_dir('2024-01-15')
        with self.assertLogs(snapshot_mover.logger, level='INFO') as cm:
            snapshot_mover.main(dry_run=True)
        self.assertTrue(any('[dry-run] would seal' in m for m in cm.output))
        self.assertFalse(os.path.exists(
            os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)))

    def test_manifest_sanitises_tabs_and_newlines_in_title(self):
        self._seed_dir('2024-01-15', [
            ('2024-01-15T10-00-00Z-a.mhtml', 'read_events',
             'https://a.com', '2024-01-15T10:00:00Z', 'Has\ttab and\nnewline'),
        ])
        snapshot_mover.main()
        manifest = self._read_manifest('2024-01-15')
        # Three replaced characters: \t and \n become spaces, not preserved.
        self.assertNotIn('\ttab', manifest)
        self.assertNotIn('and\n', manifest.replace('newline\n', ''))
        self.assertIn('Has tab and newline', manifest)

    def test_manifest_for_empty_directory_has_only_header(self):
        # No snapshot files in the dir — manifest contains just the header row.
        self._seed_dir('2024-01-15')
        snapshot_mover.main()
        self.assertEqual(self._read_manifest('2024-01-15'),
                         'filename\ttag\ttimestamp\turl\ttitle\n')

    def test_manifest_excludes_itself_from_listing(self):
        # Seed a dir, run once → manifest written.  Seed *another* dir on a
        # different past date and rerun: the first dir's manifest must not
        # appear as a row in the second dir's manifest (different dirs anyway,
        # but this test verifies MANIFEST.tsv is excluded from a dir's own
        # file list).  Simulate by pre-creating MANIFEST.tsv-named non-snapshot
        # would be wrong; instead, verify the manifest doesn't list itself by
        # checking no row's filename is MANIFEST.tsv.
        self._seed_dir('2024-01-15', [
            ('2024-01-15T10-00-00Z-a.mhtml', 'read_events',
             'https://a.com', '2024-01-15T10:00:00Z', 'A'),
        ])
        snapshot_mover.main()
        manifest = self._read_manifest('2024-01-15')
        self.assertNotIn('MANIFEST.tsv', manifest)

    def test_seal_handles_oserror_writing_manifest(self):
        # If writing the manifest fails, log an error but don't propagate.
        self._seed_dir('2024-01-15')
        with patch('builtins.open', side_effect=OSError('disk full')), \
             self.assertLogs(snapshot_mover.logger, level='ERROR') as cm:
            snapshot_mover.main()
        self.assertTrue(any('Failed to seal' in m for m in cm.output))

    # ----------------------------------------------------------------------
    # snapshots-table behaviour
    # ----------------------------------------------------------------------

    def test_seal_pass_marks_db_row_sealed(self):
        self._seed_dir('2024-01-15')
        snapshot_mover.main()
        self.assertEqual(self._snapshots_row('2024-01-15'), ('2024-01-15', 1))

    def test_seal_pass_skips_already_sealed_rows(self):
        # An already-sealed row should be left alone — no manifest is created
        # (no file write attempted) and the DB row stays sealed=1.
        date_subdir = self._seed_dir('2024-01-15', sealed=1)
        snapshot_mover.main()
        self.assertFalse(os.path.exists(
            os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)))
        self.assertEqual(self._snapshots_row('2024-01-15'), ('2024-01-15', 1))

    def test_seal_pass_does_not_touch_directories_not_in_snapshots_table(self):
        # A directory that exists on disk but has no snapshots-table row is
        # never sealed by the auto pass — the table is the source of truth.
        date_subdir = self._seed_dir('2024-01-15', seed_snapshots_row=False)
        snapshot_mover.main()
        self.assertFalse(os.path.exists(
            os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)))

    def test_seal_pass_records_error_when_row_directory_is_missing(self):
        # Insert a snapshots row for a date whose directory doesn't exist
        # on disk.  The pass should ERROR-log, record a 'missing_directory'
        # mover_errors row so the user is notified, and leave the snapshots
        # row unsealed (we couldn't seal what isn't there).
        os.makedirs(self.dest_dir, exist_ok=True)
        conn = sqlite3.connect(self.db_file)
        conn.execute(
            "INSERT INTO snapshots (date, sealed) VALUES (?, 0)", ('2024-01-15',))
        conn.commit()
        conn.close()
        with self.assertLogs(snapshot_mover.logger, level='ERROR') as cm:
            snapshot_mover.main()
        self.assertTrue(any('no on-disk directory' in m for m in cm.output))
        # Row remains unsealed.
        self.assertEqual(self._snapshots_row('2024-01-15'), ('2024-01-15', 0))
        # mover_errors row was recorded.
        conn = sqlite3.connect(self.db_file)
        err_row = conn.execute(
            "SELECT operation, target FROM mover_errors "
            "WHERE operation = 'missing_directory'"
        ).fetchone()
        conn.close()
        expected_target = os.path.join(self.dest_dir, '2024-01-15')
        self.assertEqual(err_row, ('missing_directory', expected_target))

    def test_seal_pass_clears_missing_directory_error_when_dir_reappears(self):
        # Pre-seed a missing_directory error for a date whose dir has now
        # been re-created.  The next seal pass should clear the error row.
        os.makedirs(self.dest_dir, exist_ok=True)
        date_subdir = self._seed_dir('2024-01-15')   # also creates snapshots row
        conn = sqlite3.connect(self.db_file)
        snapshot_mover._record_error(
            conn, 'missing_directory', date_subdir,
            FileNotFoundError('previous miss'))
        conn.close()

        snapshot_mover.main()

        conn = sqlite3.connect(self.db_file)
        count = conn.execute(
            "SELECT COUNT(*) FROM mover_errors WHERE operation = 'missing_directory'"
        ).fetchone()[0]
        conn.close()
        self.assertEqual(count, 0)

    def test_manifest_excludes_files_with_invalid_filename_format(self):
        # Seed a directory with one conforming file (with an events row) and
        # one non-conforming file.  The manifest should list only the
        # conforming one; the other should produce an ERROR log + an
        # 'invalid_filename' mover_errors row.
        date_str = '2024-01-15'
        date_subdir = os.path.join(self.dest_dir, date_str)
        os.makedirs(date_subdir)
        good = '2024-01-15T10-00-00Z-good.mhtml'
        bad = 'random-non-snapshot.txt'
        Path(date_subdir, good).write_bytes(b'g')
        Path(date_subdir, bad).write_bytes(b'b')
        conn = sqlite3.connect(self.db_file)
        host.insert_visit(conn, '2024-01-15T10:00:00Z', 'https://g.com', 'Good')
        conn.execute(
            "INSERT INTO read_events (url, timestamp, filename, directory) "
            "VALUES (?, ?, ?, ?)",
            ('https://g.com', '2024-01-15T10:00:00Z', good, date_subdir),
        )
        conn.execute(
            "INSERT INTO snapshots (date, sealed) VALUES (?, 0)", (date_str,))
        conn.commit()
        conn.close()

        with self.assertLogs(snapshot_mover.logger, level='ERROR') as cm:
            snapshot_mover.main()

        # Manifest excludes the bad file.
        manifest = self._read_manifest(date_str)
        self.assertIn(good, manifest)
        self.assertNotIn(bad, manifest)
        # ERROR was logged for the bad file.
        self.assertTrue(any(bad in m and 'snapshot filename format' in m
                            for m in cm.output))
        # mover_errors row recorded for the bad file.
        bad_path = os.path.join(date_subdir, bad)
        conn = sqlite3.connect(self.db_file)
        row = conn.execute(
            "SELECT operation, target FROM mover_errors "
            "WHERE operation = 'invalid_filename'"
        ).fetchone()
        conn.close()
        self.assertEqual(row, ('invalid_filename', bad_path))

    def test_manifest_clears_invalid_filename_error_when_stray_removed(self):
        # Pre-seed an invalid_filename row for a non-existent file under
        # date_subdir.  After a manifest rebuild, the reconcile step should
        # clear it.
        date_str = '2024-01-15'
        date_subdir = self._seed_dir(date_str)
        gone = os.path.join(date_subdir, 'random-non-snapshot.txt')
        conn = sqlite3.connect(self.db_file)
        snapshot_mover._record_error(
            conn, 'invalid_filename', gone,
            ValueError('synthetic'))
        conn.close()

        snapshot_mover.main()

        conn = sqlite3.connect(self.db_file)
        count = conn.execute(
            "SELECT COUNT(*) FROM mover_errors WHERE operation = 'invalid_filename'"
        ).fetchone()[0]
        conn.close()
        self.assertEqual(count, 0)

    def test_seal_pass_with_empty_snapshots_table_is_noop(self):
        # No rows → no work, no errors, no manifest writes.
        os.makedirs(self.dest_dir, exist_ok=True)
        snapshot_mover.main()  # must not raise
        self.assertEqual(os.listdir(self.dest_dir), [])


class TestTodayUtc(unittest.TestCase):
    """Cover the real _today_utc() (TestSealPass mocks it everywhere else)."""

    def test_returns_a_date_object(self):
        self.assertIsInstance(snapshot_mover._today_utc(), datetime.date)


# ---------------------------------------------------------------------------
# snapshots-table schema
# ---------------------------------------------------------------------------
class TestSnapshotsTableSchema(unittest.TestCase):

    def _conn(self):
        conn = sqlite3.connect(':memory:')
        snapshot_mover._ensure_snapshots_table(conn)
        return conn

    def _cols(self, conn):
        return {r[1] for r in conn.execute('PRAGMA table_info(snapshots)')}

    def test_creates_snapshots_table(self):
        conn = self._conn()
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        conn.close()
        self.assertIn('snapshots', tables)

    def test_has_date_column(self):
        conn = self._conn()
        self.assertIn('date', self._cols(conn))
        conn.close()

    def test_has_sealed_column(self):
        conn = self._conn()
        self.assertIn('sealed', self._cols(conn))
        conn.close()

    def test_date_is_primary_key(self):
        conn = self._conn()
        pk_cols = {r[1] for r in conn.execute('PRAGMA table_info(snapshots)') if r[5] == 1}
        conn.close()
        self.assertEqual(pk_cols, {'date'})

    def test_sealed_defaults_to_zero(self):
        conn = self._conn()
        conn.execute("INSERT INTO snapshots (date) VALUES (?)", ('2024-01-15',))
        conn.commit()
        sealed = conn.execute(
            "SELECT sealed FROM snapshots WHERE date = ?", ('2024-01-15',)
        ).fetchone()[0]
        conn.close()
        self.assertEqual(sealed, 0)

    def test_is_idempotent(self):
        conn = sqlite3.connect(':memory:')
        snapshot_mover._ensure_snapshots_table(conn)
        snapshot_mover._ensure_snapshots_table(conn)  # must not raise
        self.assertIn('date', self._cols(conn))
        conn.close()


# ---------------------------------------------------------------------------
# mover_errors table — schema, helpers, classification, escalation, notify
# ---------------------------------------------------------------------------
class TestMoverErrorsSchema(unittest.TestCase):

    def _conn(self):
        conn = sqlite3.connect(':memory:')
        snapshot_mover._ensure_mover_errors_table(conn)
        return conn

    def _cols(self, conn):
        return {r[1] for r in conn.execute('PRAGMA table_info(mover_errors)')}

    def test_creates_mover_errors_table(self):
        conn = self._conn()
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        conn.close()
        self.assertIn('mover_errors', tables)

    def test_has_expected_columns(self):
        conn = self._conn()
        expected = {'key', 'operation', 'target', 'message',
                    'first_seen', 'last_seen', 'attempts',
                    'notified', 'immediate'}
        self.assertEqual(self._cols(conn), expected)
        conn.close()

    def test_key_is_primary_key(self):
        conn = self._conn()
        pk = {r[1] for r in conn.execute('PRAGMA table_info(mover_errors)')
              if r[5] == 1}
        conn.close()
        self.assertEqual(pk, {'key'})

    def test_is_idempotent(self):
        conn = sqlite3.connect(':memory:')
        snapshot_mover._ensure_mover_errors_table(conn)
        snapshot_mover._ensure_mover_errors_table(conn)  # must not raise
        self.assertIn('key', self._cols(conn))
        conn.close()


class TestErrorRecording(unittest.TestCase):
    """Direct tests for _record_error / _try_record_error / _clear_error /
    _is_immediate, all in-memory."""

    def setUp(self):
        self.conn = sqlite3.connect(':memory:')
        snapshot_mover._ensure_mover_errors_table(self.conn)

    def tearDown(self):
        self.conn.close()

    def _row(self, key):
        return self.conn.execute(
            "SELECT operation, target, message, attempts, notified, immediate "
            "FROM mover_errors WHERE key = ?", (key,)
        ).fetchone()

    # --- _is_immediate ---

    def test_is_immediate_top_level(self):
        self.assertTrue(snapshot_mover._is_immediate('top_level', RuntimeError('x')))

    def test_is_immediate_rewrite_manifest(self):
        # Single-shot op — natural retry loop won't re-encounter the same
        # target on the next tick, so it must escalate on first occurrence.
        self.assertTrue(snapshot_mover._is_immediate(
            'rewrite_manifest', OSError('boom')))

    def test_is_immediate_invalid_filename(self):
        # Same single-shot reasoning for the date-subdir case (which is
        # what classification has to cover for both Downloads and date-dir
        # variants of this op).
        self.assertTrue(snapshot_mover._is_immediate(
            'invalid_filename', ValueError('synthetic')))

    def test_is_immediate_disk_full_oserror(self):
        exc = OSError(errno.ENOSPC, 'No space left on device')
        self.assertTrue(snapshot_mover._is_immediate('move', exc))

    def test_is_immediate_readonly_filesystem_oserror(self):
        exc = OSError(errno.EROFS, 'Read-only file system')
        self.assertTrue(snapshot_mover._is_immediate('move', exc))

    def test_is_immediate_quota_exceeded_oserror(self):
        exc = OSError(errno.EDQUOT, 'Disk quota exceeded')
        self.assertTrue(snapshot_mover._is_immediate('move', exc))

    def test_is_immediate_db_integrity_error(self):
        # IntegrityError is a DatabaseError but NOT an OperationalError.
        self.assertTrue(snapshot_mover._is_immediate(
            'move', sqlite3.IntegrityError('corrupt')))

    def test_is_immediate_false_for_transient_oserror(self):
        exc = OSError(errno.EACCES, 'Permission denied')
        self.assertFalse(snapshot_mover._is_immediate('move', exc))

    def test_is_immediate_false_for_sqlite_operational_error(self):
        # OperationalError covers transient locks / busy timeouts.
        self.assertFalse(snapshot_mover._is_immediate(
            'move', sqlite3.OperationalError('database is locked')))

    # --- _record_error ---

    def test_record_error_inserts_first_occurrence(self):
        snapshot_mover._record_error(
            self.conn, 'move', '/path/file', OSError('boom'))
        row = self._row('move:/path/file')
        self.assertEqual(row[0], 'move')
        self.assertEqual(row[1], '/path/file')
        self.assertIn('boom', row[2])
        self.assertEqual(row[3], 1)        # attempts
        self.assertEqual(row[4], 0)        # notified
        self.assertEqual(row[5], 0)        # immediate (transient)

    def test_record_error_increments_attempts_on_repeat(self):
        for _ in range(3):
            snapshot_mover._record_error(
                self.conn, 'move', '/p', OSError('boom'))
        self.assertEqual(self._row('move:/p')[3], 3)

    def test_record_error_updates_message_on_repeat(self):
        snapshot_mover._record_error(self.conn, 'move', '/p', OSError('first'))
        snapshot_mover._record_error(self.conn, 'move', '/p', OSError('second'))
        self.assertIn('second', self._row('move:/p')[2])

    def test_record_error_preserves_first_seen_on_repeat(self):
        snapshot_mover._record_error(self.conn, 'move', '/p', OSError('a'))
        first_seen = self.conn.execute(
            "SELECT first_seen FROM mover_errors WHERE key = ?",
            ('move:/p',)).fetchone()[0]
        snapshot_mover._record_error(self.conn, 'move', '/p', OSError('b'))
        new_first_seen = self.conn.execute(
            "SELECT first_seen FROM mover_errors WHERE key = ?",
            ('move:/p',)).fetchone()[0]
        self.assertEqual(first_seen, new_first_seen)

    def test_record_error_promotes_immediate_to_one(self):
        # Persistent first, then catastrophic — immediate should flip 0→1.
        snapshot_mover._record_error(
            self.conn, 'move', '/p', OSError(errno.EACCES, 'no access'))
        self.assertEqual(self._row('move:/p')[5], 0)
        snapshot_mover._record_error(
            self.conn, 'move', '/p', OSError(errno.ENOSPC, 'disk full'))
        self.assertEqual(self._row('move:/p')[5], 1)

    def test_record_error_marks_rewrite_manifest_as_immediate(self):
        snapshot_mover._record_error(
            self.conn, 'rewrite_manifest', '/d', OSError('boom'))
        self.assertEqual(self._row('rewrite_manifest:/d')[5], 1)

    def test_record_error_marks_invalid_filename_as_immediate(self):
        snapshot_mover._record_error(
            self.conn, 'invalid_filename', '/p',
            ValueError('synthetic'))
        self.assertEqual(self._row('invalid_filename:/p')[5], 1)

    def test_record_error_does_not_demote_immediate(self):
        # Catastrophic first, then transient — immediate must stay 1.
        snapshot_mover._record_error(
            self.conn, 'move', '/p', OSError(errno.ENOSPC, 'disk full'))
        snapshot_mover._record_error(
            self.conn, 'move', '/p', OSError(errno.EACCES, 'no access'))
        self.assertEqual(self._row('move:/p')[5], 1)

    def test_record_error_sanitises_message(self):
        snapshot_mover._record_error(
            self.conn, 'move', '/p', RuntimeError('a\tb\nc\rd'))
        message = self._row('move:/p')[2]
        self.assertNotIn('\t', message)
        self.assertNotIn('\n', message)
        self.assertNotIn('\r', message)

    def test_record_error_truncates_long_messages(self):
        snapshot_mover._record_error(
            self.conn, 'move', '/p', RuntimeError('x' * 5000))
        self.assertLessEqual(len(self._row('move:/p')[2]), 500)

    # --- _try_record_error ---

    def test_try_record_error_swallows_db_failures(self):
        bad_conn = sqlite3.connect(':memory:')   # no mover_errors table
        with self.assertLogs(snapshot_mover.logger, level='ERROR') as cm:
            snapshot_mover._try_record_error(
                bad_conn, 'move', '/p', RuntimeError('orig'))
        bad_conn.close()
        self.assertTrue(any('Could not record' in m for m in cm.output))

    # --- _clear_error / _try_clear_error ---

    def test_clear_error_removes_matching_row(self):
        snapshot_mover._record_error(self.conn, 'move', '/p', OSError('boom'))
        snapshot_mover._record_error(self.conn, 'seal', '/d', OSError('bork'))
        snapshot_mover._clear_error(self.conn, 'move', '/p')
        self.assertIsNone(self._row('move:/p'))
        self.assertIsNotNone(self._row('seal:/d'))

    def test_clear_error_is_noop_when_no_matching_row(self):
        snapshot_mover._clear_error(self.conn, 'move', '/missing')   # must not raise
        self.assertIsNone(self._row('move:/missing'))

    def test_try_clear_error_swallows_db_failures(self):
        bad_conn = sqlite3.connect(':memory:')
        with self.assertLogs(snapshot_mover.logger, level='ERROR') as cm:
            snapshot_mover._try_clear_error(bad_conn, 'move', '/p')
        bad_conn.close()
        self.assertTrue(any('Could not clear' in m for m in cm.output))


class TestEscalation(unittest.TestCase):
    """Tests for _escalate_errors — when does it notify, when doesn't it?"""

    def setUp(self):
        self.conn = sqlite3.connect(':memory:')
        snapshot_mover._ensure_mover_errors_table(self.conn)
        # Pin threshold so tests don't depend on env var.
        self._saved_threshold = snapshot_mover.MOVER_ERROR_THRESHOLD
        snapshot_mover.MOVER_ERROR_THRESHOLD = 3

    def tearDown(self):
        snapshot_mover.MOVER_ERROR_THRESHOLD = self._saved_threshold
        self.conn.close()

    def _notified(self, key):
        return self.conn.execute(
            "SELECT notified FROM mover_errors WHERE key = ?", (key,)
        ).fetchone()[0]

    def test_persistent_below_threshold_does_not_notify(self):
        snapshot_mover._record_error(self.conn, 'move', '/p', OSError('boom'))
        snapshot_mover._record_error(self.conn, 'move', '/p', OSError('boom'))
        with patch.object(snapshot_mover, '_notify_user') as mock_notify:
            snapshot_mover._escalate_errors(self.conn)
        mock_notify.assert_not_called()
        self.assertEqual(self._notified('move:/p'), 0)

    def test_notification_body_includes_per_op_fix_hint(self):
        # Persistent-class error → notification body should append the
        # `_FIX_HINTS['move']` text so the user knows what to do.
        for _ in range(3):
            snapshot_mover._record_error(self.conn, 'move', '/p', OSError('boom'))
        with patch.object(snapshot_mover, '_notify_user') as mock_notify:
            snapshot_mover._escalate_errors(self.conn)
        mock_notify.assert_called_once()
        body = mock_notify.call_args[0][1]
        self.assertIn('Fix:', body)
        self.assertIn(snapshot_mover._FIX_HINTS['move'], body)

    def test_notification_body_for_top_level_includes_top_level_hint(self):
        snapshot_mover._record_error(
            self.conn, 'top_level', '', RuntimeError('crash'))
        with patch.object(snapshot_mover, '_notify_user') as mock_notify:
            snapshot_mover._escalate_errors(self.conn)
        body = mock_notify.call_args[0][1]
        self.assertIn(snapshot_mover._FIX_HINTS['top_level'], body)

    def test_notification_body_for_rewrite_manifest_includes_hint(self):
        # Single-shot op — escalates immediately on first occurrence, and
        # the rewrite_manifest hint pointing at snapshot_sealer is what
        # tells the user how to recover.
        snapshot_mover._record_error(
            self.conn, 'rewrite_manifest', '/d', OSError('boom'))
        with patch.object(snapshot_mover, '_notify_user') as mock_notify:
            snapshot_mover._escalate_errors(self.conn)
        body = mock_notify.call_args[0][1]
        self.assertIn(snapshot_mover._FIX_HINTS['rewrite_manifest'], body)

    def test_persistent_at_threshold_notifies_and_marks(self):
        for _ in range(3):
            snapshot_mover._record_error(self.conn, 'move', '/p', OSError('boom'))
        with patch.object(snapshot_mover, '_notify_user') as mock_notify:
            snapshot_mover._escalate_errors(self.conn)
        mock_notify.assert_called_once()
        title, body = mock_notify.call_args[0]
        self.assertIn('mover error', title)
        self.assertIn('move', body)
        self.assertIn('/p', body)
        self.assertEqual(self._notified('move:/p'), 1)

    def test_immediate_notifies_on_first_occurrence(self):
        snapshot_mover._record_error(
            self.conn, 'move', '/p', OSError(errno.ENOSPC, 'disk full'))
        with patch.object(snapshot_mover, '_notify_user') as mock_notify:
            snapshot_mover._escalate_errors(self.conn)
        mock_notify.assert_called_once()
        self.assertEqual(self._notified('move:/p'), 1)

    def test_top_level_notifies_on_first_occurrence(self):
        snapshot_mover._record_error(
            self.conn, 'top_level', '', RuntimeError('crash'))
        with patch.object(snapshot_mover, '_notify_user') as mock_notify:
            snapshot_mover._escalate_errors(self.conn)
        mock_notify.assert_called_once()
        title, body = mock_notify.call_args[0]
        self.assertIn('crashed', body)
        self.assertEqual(self._notified('top_level:'), 1)

    def test_already_notified_rows_are_skipped(self):
        for _ in range(3):
            snapshot_mover._record_error(self.conn, 'move', '/p', OSError('boom'))
        # First escalation marks notified=1.
        with patch.object(snapshot_mover, '_notify_user'):
            snapshot_mover._escalate_errors(self.conn)
        # Subsequent failures keep accruing attempts.
        snapshot_mover._record_error(self.conn, 'move', '/p', OSError('boom'))
        # Second escalation must not fire.
        with patch.object(snapshot_mover, '_notify_user') as mock_notify:
            snapshot_mover._escalate_errors(self.conn)
        mock_notify.assert_not_called()

    def test_escalate_db_query_failure_logs_and_returns(self):
        bad_conn = sqlite3.connect(':memory:')   # no mover_errors table
        with self.assertLogs(snapshot_mover.logger, level='ERROR') as cm:
            snapshot_mover._escalate_errors(bad_conn)
        bad_conn.close()
        self.assertTrue(any('Could not query mover_errors' in m for m in cm.output))

    def test_escalate_update_failure_logs_but_continues(self):
        for _ in range(3):
            snapshot_mover._record_error(self.conn, 'move', '/p', OSError('boom'))

        # Wrap the connection so the UPDATE for `notified=1` raises but
        # everything else (including the SELECT) passes through.
        # sqlite3.Connection.execute is a C method and can't be patched
        # directly, so a thin proxy is the cleanest workaround.
        real_conn = self.conn

        class _FailingOnUpdateConn:
            def execute(self, sql, *args, **kwargs):
                if sql.lstrip().upper().startswith('UPDATE MOVER_ERRORS'):
                    raise sqlite3.OperationalError('fake update failure')
                return real_conn.execute(sql, *args, **kwargs)

            def commit(self):
                return real_conn.commit()

        with patch.object(snapshot_mover, '_notify_user'), \
             self.assertLogs(snapshot_mover.logger, level='ERROR') as cm:
            snapshot_mover._escalate_errors(_FailingOnUpdateConn())
        self.assertTrue(any('Could not mark' in m for m in cm.output))


class TestNotifyUser(unittest.TestCase):
    """_notify_user is a thin shell around osascript / a marker file."""

    def setUp(self):
        # Redirect the attention-file path to a temp location so we don't
        # touch the real $HOME.
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self._saved = snapshot_mover._ATTENTION_FILE
        snapshot_mover._ATTENTION_FILE = os.path.join(
            self.tmp.name, 'mover-needs-attention')

    def tearDown(self):
        snapshot_mover._ATTENTION_FILE = self._saved

    def test_macos_invokes_osascript(self):
        with patch.object(snapshot_mover.sys, 'platform', 'darwin'), \
             patch.object(snapshot_mover.subprocess, 'run') as mock_run:
            snapshot_mover._notify_user('Title', 'Body')
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        self.assertEqual(args[0], 'osascript')
        # AppleScript should embed both title and body.
        joined = ' '.join(args)
        self.assertIn('Title', joined)
        self.assertIn('Body', joined)

    def test_macos_subprocess_failure_falls_back_to_marker_file(self):
        with patch.object(snapshot_mover.sys, 'platform', 'darwin'), \
             patch.object(snapshot_mover.subprocess, 'run',
                          side_effect=FileNotFoundError('osascript missing')), \
             self.assertLogs(snapshot_mover.logger, level='ERROR') as cm:
            snapshot_mover._notify_user('T', 'B')
        self.assertTrue(any('osascript' in m for m in cm.output))
        self.assertTrue(os.path.exists(snapshot_mover._ATTENTION_FILE))
        contents = Path(snapshot_mover._ATTENTION_FILE).read_text()
        self.assertIn('T', contents)
        self.assertIn('B', contents)

    def test_non_macos_writes_marker_file(self):
        with patch.object(snapshot_mover.sys, 'platform', 'linux'):
            snapshot_mover._notify_user('T', 'B')
        self.assertTrue(os.path.exists(snapshot_mover._ATTENTION_FILE))

    def test_non_macos_marker_file_failure_is_swallowed(self):
        # Point the marker path at an unwritable location.
        snapshot_mover._ATTENTION_FILE = os.path.join(
            self.tmp.name, 'no', 'such', 'dir', 'marker')
        with patch.object(snapshot_mover.sys, 'platform', 'linux'), \
             self.assertLogs(snapshot_mover.logger, level='ERROR') as cm:
            snapshot_mover._notify_user('T', 'B')   # must not raise
        self.assertTrue(any('attention file' in m for m in cm.output))

    def test_applescript_quote_escapes_quotes_and_backslashes(self):
        self.assertEqual(
            snapshot_mover._applescript_quote('hello "world"'),
            '"hello \\"world\\""',
        )
        self.assertEqual(
            snapshot_mover._applescript_quote(r'a\b'),
            '"a\\\\b"',
        )


class TestErrorWiring(_MoverTestBase):
    """End-to-end: per-op exceptions create rows; subsequent successes clear them."""

    def test_move_failure_records_error_row(self):
        # Inject an OSError into shutil.copy2 so the move pass fails.
        self._make_event('read_events', 'https://a.com', '2024-01-15T10:00:00.000Z',
                         'a.mhtml', age_seconds=700)
        os.makedirs(snapshot_mover.ICLOUD_SNAPSHOTS_DIR, exist_ok=True)
        with patch('shutil.copy2', side_effect=OSError(errno.EACCES, 'denied')):
            conn = sqlite3.connect(self.db_file)
            try:
                snapshot_mover._move_pass(conn)
            finally:
                conn.close()
        conn = sqlite3.connect(self.db_file)
        rows = conn.execute(
            "SELECT operation, attempts, immediate FROM mover_errors"
        ).fetchall()
        conn.close()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], 'move')
        self.assertEqual(rows[0][1], 1)
        self.assertEqual(rows[0][2], 0)   # EACCES is not immediate

    def test_repeated_move_failure_increments_attempts(self):
        self._make_event('read_events', 'https://a.com', '2024-01-15T10:00:00.000Z',
                         'a.mhtml', age_seconds=700)
        os.makedirs(snapshot_mover.ICLOUD_SNAPSHOTS_DIR, exist_ok=True)
        for _ in range(3):
            with patch('shutil.copy2',
                        side_effect=OSError(errno.EACCES, 'denied')):
                conn = sqlite3.connect(self.db_file)
                try:
                    snapshot_mover._move_pass(conn)
                finally:
                    conn.close()
        conn = sqlite3.connect(self.db_file)
        attempts = conn.execute(
            "SELECT attempts FROM mover_errors").fetchone()[0]
        conn.close()
        self.assertEqual(attempts, 3)

    def test_disk_full_during_move_records_immediate_error(self):
        self._make_event('read_events', 'https://a.com', '2024-01-15T10:00:00.000Z',
                         'a.mhtml', age_seconds=700)
        os.makedirs(snapshot_mover.ICLOUD_SNAPSHOTS_DIR, exist_ok=True)
        with patch('shutil.copy2',
                    side_effect=OSError(errno.ENOSPC, 'disk full')):
            conn = sqlite3.connect(self.db_file)
            try:
                snapshot_mover._move_pass(conn)
            finally:
                conn.close()
        conn = sqlite3.connect(self.db_file)
        immediate = conn.execute(
            "SELECT immediate FROM mover_errors").fetchone()[0]
        conn.close()
        self.assertEqual(immediate, 1)

    def test_successful_move_clears_prior_error_row(self):
        # Pre-seed an error row, then run a successful move.
        conn = sqlite3.connect(self.db_file)
        prefixed = _snap('2024-01-15T10:00:00.000Z', 'a.mhtml')
        source_path = os.path.join(self.source_dir, prefixed)
        snapshot_mover._record_error(
            conn, 'move', source_path, OSError('boom'))
        conn.close()
        # Now the same move succeeds.
        self._make_event('read_events', 'https://a.com', '2024-01-15T10:00:00.000Z',
                         'a.mhtml', age_seconds=700)
        os.makedirs(snapshot_mover.ICLOUD_SNAPSHOTS_DIR, exist_ok=True)
        conn = sqlite3.connect(self.db_file)
        try:
            snapshot_mover._move_pass(conn)
        finally:
            conn.close()
        conn = sqlite3.connect(self.db_file)
        count = conn.execute(
            "SELECT COUNT(*) FROM mover_errors WHERE operation = 'move'"
        ).fetchone()[0]
        conn.close()
        self.assertEqual(count, 0)

    def test_seal_failure_records_error_row(self):
        # Pre-seed an unsealed past-date row + dir, then inject a manifest
        # write failure.  Because the seal pass calls main(), patch
        # _today_utc so the row qualifies.
        date_subdir = os.path.join(self.dest_dir, '2024-01-15')
        os.makedirs(date_subdir)
        conn = sqlite3.connect(self.db_file)
        conn.execute("INSERT INTO snapshots (date, sealed) VALUES (?, 0)",
                     ('2024-01-15',))
        conn.commit()
        conn.close()
        with patch.object(snapshot_mover, '_today_utc',
                          return_value=datetime.date(2024, 1, 20)), \
             patch.object(snapshot_mover, '_write_manifest_file',
                          side_effect=OSError('disk full while sealing')):
            snapshot_mover.main()
        conn = sqlite3.connect(self.db_file)
        rows = conn.execute(
            "SELECT operation, target FROM mover_errors").fetchall()
        conn.close()
        self.assertTrue(any(op == 'seal' and target == date_subdir
                            for op, target in rows))

    def test_successful_seal_clears_prior_error_row(self):
        date_subdir = os.path.join(self.dest_dir, '2024-01-15')
        os.makedirs(date_subdir)
        conn = sqlite3.connect(self.db_file)
        conn.execute("INSERT INTO snapshots (date, sealed) VALUES (?, 0)",
                     ('2024-01-15',))
        snapshot_mover._record_error(
            conn, 'seal', date_subdir, OSError('previous failure'))
        conn.commit()
        conn.close()
        with patch.object(snapshot_mover, '_today_utc',
                          return_value=datetime.date(2024, 1, 20)):
            snapshot_mover.main()
        conn = sqlite3.connect(self.db_file)
        count = conn.execute(
            "SELECT COUNT(*) FROM mover_errors WHERE operation = 'seal'"
        ).fetchone()[0]
        conn.close()
        self.assertEqual(count, 0)

    def test_straggler_rewrite_failure_records_error_row(self):
        date_str = '2024-01-15'
        date_subdir = os.path.join(self.dest_dir, date_str)
        os.makedirs(date_subdir)
        conn = sqlite3.connect(self.db_file)
        conn.execute("INSERT INTO snapshots (date, sealed) VALUES (?, 1)",
                     (date_str,))
        conn.commit()
        conn.close()
        self._make_event('read_events', 'https://a.com', '2024-01-15T10:00:00.000Z',
                         'a.mhtml', age_seconds=700)
        os.makedirs(snapshot_mover.ICLOUD_SNAPSHOTS_DIR, exist_ok=True)
        with patch.object(snapshot_mover, '_write_manifest_file',
                          side_effect=OSError('disk full')):
            conn = sqlite3.connect(self.db_file)
            try:
                snapshot_mover._move_pass(conn)
            finally:
                conn.close()
        conn = sqlite3.connect(self.db_file)
        rows = conn.execute(
            "SELECT operation, target FROM mover_errors").fetchall()
        conn.close()
        self.assertTrue(any(op == 'rewrite_manifest' and target == date_subdir
                            for op, target in rows))

    def test_invalid_filename_in_downloads_notifies_on_first_occurrence(self):
        # Stray non-snapshot file in Downloads.  After one main() run, the
        # row should be marked notified=1 (escalated immediately because
        # 'invalid_filename' is now classified as immediate).
        stray = os.path.join(self.source_dir, 'random.bin')
        Path(stray).write_bytes(b'x')
        mtime = time.time() - 700
        os.utime(stray, (mtime, mtime))
        with patch.object(snapshot_mover, '_notify_user') as mock_notify:
            snapshot_mover.main()
        mock_notify.assert_called_once()
        body = mock_notify.call_args[0][1]
        self.assertIn(snapshot_mover._FIX_HINTS['invalid_filename'], body)
        conn = sqlite3.connect(self.db_file)
        notified = conn.execute(
            "SELECT notified FROM mover_errors WHERE operation = 'invalid_filename'"
        ).fetchone()[0]
        conn.close()
        self.assertEqual(notified, 1)

    def test_straggler_rewrite_failure_notifies_on_first_occurrence(self):
        # A single straggler whose manifest rewrite fails should escalate
        # without waiting for further stragglers (which may never arrive).
        date_str = '2024-01-15'
        date_subdir = os.path.join(self.dest_dir, date_str)
        os.makedirs(date_subdir)
        conn = sqlite3.connect(self.db_file)
        conn.execute("INSERT INTO snapshots (date, sealed) VALUES (?, 1)",
                     (date_str,))
        conn.commit()
        conn.close()
        self._make_event('read_events', 'https://a.com',
                         '2024-01-15T10:00:00.000Z', 'a.mhtml',
                         age_seconds=700)
        with patch.object(snapshot_mover, '_write_manifest_file',
                          side_effect=OSError('disk full')), \
             patch.object(snapshot_mover, '_notify_user') as mock_notify:
            snapshot_mover.main()
        mock_notify.assert_called_once()
        body = mock_notify.call_args[0][1]
        self.assertIn('rewrite_manifest', body)
        self.assertIn(snapshot_mover._FIX_HINTS['rewrite_manifest'], body)

    def test_top_level_failure_records_and_notifies_then_reraises(self):
        with patch.object(snapshot_mover, '_move_pass',
                          side_effect=RuntimeError('unexpected explosion')), \
             patch.object(snapshot_mover, '_notify_user') as mock_notify:
            with self.assertRaises(RuntimeError):
                snapshot_mover.main()
        mock_notify.assert_called_once()
        conn = sqlite3.connect(self.db_file)
        row = conn.execute(
            "SELECT operation, target, immediate, notified "
            "FROM mover_errors WHERE operation = 'top_level'"
        ).fetchone()
        conn.close()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], 'top_level')
        self.assertEqual(row[1], '')
        self.assertEqual(row[2], 1)   # immediate
        self.assertEqual(row[3], 1)   # notified

    def test_top_level_db_fallback_uses_direct_notification(self):
        # If the DB-recording path itself fails, _notify_user is still
        # invoked directly with a generic crash message.
        original_connect = sqlite3.connect

        def _failing_connect(path, *args, **kwargs):
            # Allow setUp's connection to succeed; fail any *new* one made
            # inside main().
            raise sqlite3.OperationalError('cannot open database')

        # Allow os.makedirs to succeed.  Wrap `os.path.exists` so that the
        # DB path appears to exist (forcing main() to reach the connect).
        with patch.object(snapshot_mover, 'sqlite3') as mock_sqlite, \
             patch.object(snapshot_mover.os.path, 'exists', return_value=True), \
             patch.object(snapshot_mover, '_notify_user') as mock_notify:
            mock_sqlite.connect.side_effect = sqlite3.OperationalError('boom')
            mock_sqlite.Error = sqlite3.Error
            mock_sqlite.OperationalError = sqlite3.OperationalError
            mock_sqlite.DatabaseError = sqlite3.DatabaseError
            with self.assertRaises(sqlite3.OperationalError):
                snapshot_mover.main()
        mock_notify.assert_called_once()
        title, body = mock_notify.call_args[0]
        self.assertIn('crashed', title.lower())


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
class TestCli(unittest.TestCase):
    """Tests for _parse_args / _apply_args / cli()."""

    def setUp(self):
        self._saved = {
            name: getattr(snapshot_mover, name)
            for name in ('DOWNLOADS_SNAPSHOTS_DIR', 'ICLOUD_SNAPSHOTS_DIR',
                         'DB_FILE', 'MIN_AGE_SECONDS', 'MOVER_ERROR_THRESHOLD')
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
            '--error-threshold', '5',
            '--source', '/tmp/src',
            '--dest', '/tmp/dst',
            '--db', '/tmp/test.db',
        ])
        self.assertTrue(ns.dry_run)
        self.assertTrue(ns.verbose)
        self.assertEqual(ns.min_age_seconds, 120)
        self.assertEqual(ns.error_threshold, 5)
        self.assertEqual(ns.source, '/tmp/src')
        self.assertEqual(ns.dest, '/tmp/dst')
        self.assertEqual(ns.db, '/tmp/test.db')

    def test_apply_args_overrides_paths_and_age(self):
        ns = snapshot_mover._parse_args([
            '--source', '/tmp/src',
            '--dest', '/tmp/dst',
            '--db', '/tmp/test.db',
            '--min-age-seconds', '5',
            '--error-threshold', '7',
        ])
        snapshot_mover._apply_args(ns)
        self.assertEqual(snapshot_mover.DOWNLOADS_SNAPSHOTS_DIR, '/tmp/src')
        self.assertEqual(snapshot_mover.ICLOUD_SNAPSHOTS_DIR,    '/tmp/dst')
        self.assertEqual(snapshot_mover.DB_FILE,                 '/tmp/test.db')
        self.assertEqual(snapshot_mover.MIN_AGE_SECONDS,         5)
        self.assertEqual(snapshot_mover.MOVER_ERROR_THRESHOLD,   7)

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


# ---------------------------------------------------------------------------
# Error-table CLI flags: --show-errors, --clear-errors, --clear-error N
# ---------------------------------------------------------------------------
class TestErrorCli(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_file = os.path.join(self.tmp.name, 'visits.db')
        # Save and restore DB_FILE since CLI calls mutate it.
        self._saved_db = snapshot_mover.DB_FILE
        self.addCleanup(lambda: setattr(snapshot_mover, 'DB_FILE', self._saved_db))

    def _seed_errors(self, *triples):
        """Insert (op, target, exc) triples in order, using _record_error."""
        snapshot_mover.DB_FILE = self.db_file
        conn = sqlite3.connect(self.db_file)
        snapshot_mover._ensure_mover_errors_table(conn)
        for op, target, exc in triples:
            snapshot_mover._record_error(conn, op, target, exc)
        conn.close()

    # --- mutual exclusivity ---

    def test_show_and_clear_errors_are_mutually_exclusive(self):
        with self.assertRaises(SystemExit):
            snapshot_mover._parse_args(['--show-errors', '--clear-errors'])

    def test_show_errors_and_clear_error_are_mutually_exclusive(self):
        with self.assertRaises(SystemExit):
            snapshot_mover._parse_args(['--show-errors', '--clear-error', '1'])

    # --- --show-errors ---

    def test_show_errors_with_empty_table_prints_no_errors_message(self):
        snapshot_mover.DB_FILE = self.db_file
        captured = io.StringIO()
        with patch('sys.stdout', captured):
            rc = snapshot_mover.cli(['--db', self.db_file, '--show-errors'])
        self.assertEqual(rc, 0)
        self.assertIn('No pending mover errors', captured.getvalue())

    def test_show_errors_lists_rows_with_index_and_metadata(self):
        self._seed_errors(
            ('move', '/path/file', OSError('boom')),
            ('seal', '/dir/2024-01-15', OSError('bork')),
        )
        captured = io.StringIO()
        with patch('sys.stdout', captured):
            rc = snapshot_mover.cli(['--db', self.db_file, '--show-errors'])
        out = captured.getvalue()
        self.assertEqual(rc, 0)
        self.assertIn('Pending mover errors (2)', out)
        self.assertIn('[1]', out)
        self.assertIn('[2]', out)
        self.assertIn('move:', out)
        self.assertIn('seal:', out)
        self.assertIn('/path/file', out)
        self.assertIn('/dir/2024-01-15', out)

    def test_show_errors_includes_fix_hint_per_row(self):
        # Each row should carry a `fix:` line with the per-op guidance,
        # so the user gets actionable direction without consulting docs.
        self._seed_errors(
            ('move', '/p', OSError('boom')),
            ('missing_directory', '/d', FileNotFoundError('gone')),
        )
        captured = io.StringIO()
        with patch('sys.stdout', captured):
            snapshot_mover.cli(['--db', self.db_file, '--show-errors'])
        out = captured.getvalue()
        self.assertIn('fix:', out)
        self.assertIn(snapshot_mover._FIX_HINTS['move'], out)
        self.assertIn(snapshot_mover._FIX_HINTS['missing_directory'], out)

    def test_show_errors_omits_fix_line_for_unknown_op(self):
        # Defensive: if an op without a hint somehow ends up in the table,
        # the line is simply omitted rather than printing an empty fix.
        snapshot_mover.DB_FILE = self.db_file
        conn = sqlite3.connect(self.db_file)
        snapshot_mover._ensure_mover_errors_table(conn)
        snapshot_mover._record_error(
            conn, 'unknown_op', '/x', RuntimeError('?'))
        conn.close()
        captured = io.StringIO()
        with patch('sys.stdout', captured):
            snapshot_mover.cli(['--db', self.db_file, '--show-errors'])
        out = captured.getvalue()
        self.assertNotIn('fix:', out)

    def test_show_errors_skips_move_and_seal_pass(self):
        # Even with sources present that would normally be moved, --show-errors
        # must not run the move pass.  Use a temp Downloads dir with one file
        # and a non-existent dest to make any move attempt visible.
        snapshot_mover.DB_FILE = self.db_file
        src = os.path.join(self.tmp.name, 'downloads')
        os.makedirs(src)
        Path(src, '2024-01-15T10-00-00Z-x.mhtml').write_bytes(b'data')
        captured = io.StringIO()
        with patch('sys.stdout', captured):
            snapshot_mover.cli([
                '--db', self.db_file,
                '--source', src,
                '--dest', os.path.join(self.tmp.name, 'icloud'),
                '--show-errors',
            ])
        # Source still there — no move pass ran.
        self.assertTrue(os.path.exists(
            os.path.join(src, '2024-01-15T10-00-00Z-x.mhtml')))

    # --- --clear-errors ---

    def test_clear_errors_with_empty_table_reports_zero(self):
        captured = io.StringIO()
        with patch('sys.stdout', captured):
            rc = snapshot_mover.cli(['--db', self.db_file, '--clear-errors'])
        self.assertEqual(rc, 0)
        self.assertIn('Cleared 0 error', captured.getvalue())

    def test_clear_errors_removes_all_rows(self):
        self._seed_errors(
            ('move', '/a', OSError('x')),
            ('move', '/b', OSError('y')),
            ('seal', '/d', OSError('z')),
        )
        captured = io.StringIO()
        with patch('sys.stdout', captured):
            rc = snapshot_mover.cli(['--db', self.db_file, '--clear-errors'])
        self.assertEqual(rc, 0)
        self.assertIn('Cleared 3 error', captured.getvalue())
        # Table is empty.
        conn = sqlite3.connect(self.db_file)
        count = conn.execute("SELECT COUNT(*) FROM mover_errors").fetchone()[0]
        conn.close()
        self.assertEqual(count, 0)

    def test_clear_errors_singular_phrasing_when_one_row(self):
        self._seed_errors(('move', '/only', OSError('x')))
        captured = io.StringIO()
        with patch('sys.stdout', captured):
            snapshot_mover.cli(['--db', self.db_file, '--clear-errors'])
        self.assertIn('Cleared 1 error row.', captured.getvalue())

    # --- --clear-error N ---

    def test_clear_error_n_deletes_only_that_row(self):
        self._seed_errors(
            ('move', '/a', OSError('x')),
            ('move', '/b', OSError('y')),
        )
        # Order is (first_seen ASC, key ASC); both inserted with the same
        # second-precision timestamp, so they tie on first_seen and the
        # secondary 'key' sort decides: 'move:/a' < 'move:/b'.
        captured = io.StringIO()
        with patch('sys.stdout', captured):
            rc = snapshot_mover.cli([
                '--db', self.db_file, '--clear-error', '1'])
        self.assertEqual(rc, 0)
        self.assertIn('Cleared error [1]', captured.getvalue())
        self.assertIn('/a', captured.getvalue())
        # Only /b remains.
        conn = sqlite3.connect(self.db_file)
        rows = conn.execute(
            "SELECT target FROM mover_errors").fetchall()
        conn.close()
        self.assertEqual([r[0] for r in rows], ['/b'])

    def test_clear_error_out_of_range_returns_nonzero(self):
        self._seed_errors(('move', '/only', OSError('x')))
        captured_err = io.StringIO()
        with patch('sys.stderr', captured_err):
            rc = snapshot_mover.cli([
                '--db', self.db_file, '--clear-error', '99'])
        self.assertEqual(rc, 1)
        self.assertIn('No error at index 99', captured_err.getvalue())
        # The lone row is untouched.
        conn = sqlite3.connect(self.db_file)
        count = conn.execute("SELECT COUNT(*) FROM mover_errors").fetchone()[0]
        conn.close()
        self.assertEqual(count, 1)

    def test_clear_error_zero_index_returns_nonzero(self):
        self._seed_errors(('move', '/only', OSError('x')))
        captured_err = io.StringIO()
        with patch('sys.stderr', captured_err):
            rc = snapshot_mover.cli([
                '--db', self.db_file, '--clear-error', '0'])
        self.assertEqual(rc, 1)
        self.assertIn('No error at index 0', captured_err.getvalue())

    def test_clear_error_with_empty_table_returns_nonzero(self):
        captured_err = io.StringIO()
        with patch('sys.stderr', captured_err):
            rc = snapshot_mover.cli([
                '--db', self.db_file, '--clear-error', '1'])
        self.assertEqual(rc, 1)
        self.assertIn('No pending mover errors', captured_err.getvalue())


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
