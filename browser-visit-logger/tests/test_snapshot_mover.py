"""
Unit tests for the Python helpers in native-host/snapshot_mover.py.

After the Swift port these helpers are imported only by
snapshot_sealer.py and visits_rebuilder.py.  This module covers them
in three layers:

  1. Bare schema / utility tests (TestTodayUtc, TestSnapshotsTable
     Schema, TestMoverErrorsSchema) for in-memory smoke coverage.
  2. Direct tests for the error-tracking helpers (TestErrorRecording)
     — _record_error, _clear_error, _try_*, _is_immediate,
     _reconcile_dir_scoped_errors — reachable from the sealer
     transitively via _build_manifest_rows.
  3. Direct tests for the seal helpers (TestBuildManifestRows,
     TestWriteManifestFile, TestSealDirectoryFailurePath,
     TestOrphanLogMergePass) — exercised through the sealer in its
     own test module, but covered here for branches the sealer's
     happy path doesn't reach (orphan files, non-conforming
     filenames, race-orphan log merges, etc.).
"""
import datetime
import errno
import os
import sqlite3
import tempfile
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


# ---------------------------------------------------------------------------
# Direct exercise of the seal helpers used by snapshot_sealer.py.
#
# The classes below cover branches that the sealer's own happy-path
# tests don't reach:
#   - _build_manifest_rows's invalid_filename / orphan branches.
#   - _write_manifest_file's "manifest already exists" unlink.
#   - _seal_directory's exception path.
#   - _orphan_log_merge_pass's race-orphan and missing-LOG_DIR branches.
#   - _reconcile_dir_scoped_errors's stale-row deletion.
#   - _lookup_event's read_events / skimmed_events return paths.
# ---------------------------------------------------------------------------

class _SealHelpersTestBase(unittest.TestCase):
    """Common setUp: tmp date subdir + tmp DB + log_dir + paths patched."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.icloud  = os.path.join(self.tmp.name, 'icloud')
        self.log_dir = os.path.join(self.tmp.name, 'logs')
        self.date    = '2024-01-15'
        self.subdir  = os.path.join(self.icloud, self.date)
        os.makedirs(self.subdir)
        os.makedirs(self.log_dir)
        self.db_path = os.path.join(self.tmp.name, 'visits.db')
        self.conn = sqlite3.connect(self.db_path)
        host.ensure_db(self.conn)
        snapshot_mover._ensure_snapshots_table(self.conn)
        snapshot_mover._ensure_mover_errors_table(self.conn)
        # Patch the module-level constants the helpers read.
        for module, attrs in (
            (snapshot_mover, {'ICLOUD_SNAPSHOTS_DIR': self.icloud,
                              'LOG_DIR':              self.log_dir}),
            (host,           {'LOG_DIR':              self.log_dir}),
        ):
            for name, value in attrs.items():
                p = patch.object(module, name, value)
                p.start()
                self.addCleanup(p.stop)

    def tearDown(self):
        self.conn.close()

    def _seed_visit_and_event(self, tag, ts, basename):
        """Insert a visits row + matching events row pointing at self.subdir."""
        host.insert_visit(self.conn, ts, 'https://a.com', 'A')
        table = 'read_events' if tag == 'read' else 'skimmed_events'
        self.conn.execute(
            f"INSERT INTO {table} (url, timestamp, filename, directory) "
            f"VALUES (?, ?, ?, ?)",
            ('https://a.com', ts, basename, self.subdir))
        self.conn.commit()


class TestBuildManifestRows(_SealHelpersTestBase):

    def test_returns_row_for_conforming_file_with_events_row(self):
        # Happy path — the file is conforming and has a matching events
        # row, so it appears in the manifest with full metadata.
        basename = '2024-01-15T10-00-00Z-abc.mhtml'
        Path(self.subdir, basename).write_bytes(b'data')
        self._seed_visit_and_event('read', '2024-01-15T10:00:00Z', basename)
        rows = snapshot_mover._build_manifest_rows(self.conn, self.subdir)
        self.assertEqual(rows, [(basename, 'read', '2024-01-15T10:00:00Z',
                                 'https://a.com', 'A')])

    def test_excludes_non_conforming_filename_and_records_invalid_filename(self):
        Path(self.subdir, 'random.mhtml').write_bytes(b'x')
        rows = snapshot_mover._build_manifest_rows(self.conn, self.subdir)
        self.assertEqual(rows, [])
        op_target = self.conn.execute(
            "SELECT operation, target FROM mover_errors").fetchall()
        self.assertEqual(op_target,
                         [('invalid_filename',
                           os.path.join(self.subdir, 'random.mhtml'))])

    def test_excludes_orphan_file_and_records_orphan_file(self):
        # Conforming filename but no events row in the DB.
        basename = '2024-01-15T10-00-00Z-orphan.mhtml'
        Path(self.subdir, basename).write_bytes(b'x')
        rows = snapshot_mover._build_manifest_rows(self.conn, self.subdir)
        self.assertEqual(rows, [])
        op_target = self.conn.execute(
            "SELECT operation, target FROM mover_errors").fetchall()
        self.assertEqual(op_target,
                         [('orphan_file',
                           os.path.join(self.subdir, basename))])

    def test_lookup_event_finds_skimmed_row(self):
        # Covers _lookup_event's skimmed_events branch.
        basename = '2024-01-15T10-00-00Z-skim.mhtml'
        Path(self.subdir, basename).write_bytes(b'x')
        self._seed_visit_and_event('skimmed', '2024-01-15T10:00:00Z', basename)
        rows = snapshot_mover._build_manifest_rows(self.conn, self.subdir)
        self.assertEqual(rows[0][1], 'skimmed')


class TestWriteManifestFile(_SealHelpersTestBase):

    def test_overwrites_existing_read_only_manifest(self):
        # Pre-create a 0o444 manifest so the unlink branch fires before
        # the rewrite (open(path, 'w') would otherwise fail).
        path = os.path.join(self.subdir, snapshot_mover.MANIFEST_FILENAME)
        Path(path).write_text('STALE\n')
        os.chmod(path, 0o444)
        # Seed a real row so the rewrite produces non-empty content.
        basename = '2024-01-15T10-00-00Z-abc.mhtml'
        Path(self.subdir, basename).write_bytes(b'x')
        self._seed_visit_and_event('read', '2024-01-15T10:00:00Z', basename)
        count = snapshot_mover._write_manifest_file(self.conn, self.subdir)
        self.assertEqual(count, 1)
        body = open(path, encoding='utf-8').read().splitlines()
        # Header + one data row.
        self.assertEqual(len(body), 2)
        self.assertNotIn('STALE', body[0])


class TestSealDirectoryFailurePath(_SealHelpersTestBase):

    def test_failure_recorded_to_mover_errors(self):
        # Force _write_manifest_file to raise so we hit _seal_directory's
        # except branch.  The 'seal' error row should appear.
        with patch.object(snapshot_mover, '_write_manifest_file',
                          side_effect=OSError('no space')):
            snapshot_mover._seal_directory(
                self.conn, self.subdir, dry_run=False, date_key=self.date)
        ops = self.conn.execute(
            "SELECT operation, target FROM mover_errors").fetchall()
        self.assertEqual(ops, [('seal', self.subdir)])


class TestOrphanLogMergePass(_SealHelpersTestBase):

    def test_log_dir_absent_is_a_silent_noop(self):
        # Patch LOG_DIR to a path that doesn't exist; the function should
        # return immediately without raising.
        bogus = os.path.join(self.tmp.name, 'never-created')
        with patch.object(snapshot_mover, 'LOG_DIR', bogus):
            snapshot_mover._orphan_log_merge_pass(self.conn)
        # No mover_errors row inserted.
        n = self.conn.execute(
            "SELECT COUNT(*) FROM mover_errors").fetchone()[0]
        self.assertEqual(n, 0)

    def test_race_orphan_appended_into_icloud_log_and_unlinked(self):
        # Set up: an iCloud log file already exists for a past date,
        # AND a fresh orphan with the same date sits in LOG_DIR.  The
        # pass should append the orphan into the iCloud log and unlink
        # the orphan.
        fname = snapshot_mover._log_filename_for(self.date)
        icloud_log = os.path.join(self.subdir, fname)
        Path(icloud_log).write_text('original\n', encoding='utf-8')
        os.chmod(icloud_log, 0o444)
        log_orphan = os.path.join(self.log_dir, fname)
        Path(log_orphan).write_text('appended\n', encoding='utf-8')
        # Use a future today so self.date is past-day.
        with patch.object(snapshot_mover, '_today_utc',
                          return_value=datetime.date(2099, 1, 1)):
            snapshot_mover._orphan_log_merge_pass(self.conn)
        # Orphan unlinked, iCloud log now contains both.
        self.assertFalse(os.path.exists(log_orphan))
        body = open(icloud_log, encoding='utf-8').read()
        self.assertIn('original',  body)
        self.assertIn('appended',  body)

    def test_missing_icloud_log_inserts_snapshots_row(self):
        # Past-day log in LOG_DIR with no iCloud counterpart → backfill
        # a snapshots row at sealed=0 so the next seal pass picks it up.
        fname = snapshot_mover._log_filename_for(self.date)
        Path(self.log_dir, fname).write_text('x', encoding='utf-8')
        with patch.object(snapshot_mover, '_today_utc',
                          return_value=datetime.date(2099, 1, 1)):
            snapshot_mover._orphan_log_merge_pass(self.conn)
        rows = self.conn.execute(
            "SELECT date, sealed FROM snapshots WHERE date = ?",
            (self.date,)).fetchall()
        self.assertEqual(rows, [(self.date, 0)])

    def test_oserror_during_merge_is_logged_and_recorded(self):
        # Trigger an OSError mid-merge and verify the except-block
        # records a 'seal' error row.
        fname = snapshot_mover._log_filename_for(self.date)
        Path(self.subdir, fname).write_text('x', encoding='utf-8')
        os.chmod(os.path.join(self.subdir, fname), 0o444)
        Path(self.log_dir, fname).write_text('y', encoding='utf-8')
        with patch.object(snapshot_mover, '_today_utc',
                          return_value=datetime.date(2099, 1, 1)), \
             patch('os.chmod', side_effect=OSError('denied')):
            snapshot_mover._orphan_log_merge_pass(self.conn)
        ops = self.conn.execute(
            "SELECT operation, target FROM mover_errors").fetchall()
        self.assertEqual(ops, [('seal', self.subdir)])


class TestReconcileDirScopedErrors(_SealHelpersTestBase):

    def test_clears_stale_row_under_dir_path(self):
        # Pre-seed an invalid_filename row whose target is inside the
        # date subdir, then run a reconcile with currentStrays=[] —
        # the row should be deleted because it's "no longer present".
        target = os.path.join(self.subdir, 'gone.mhtml')
        snapshot_mover._record_error(
            self.conn, 'invalid_filename', target, ValueError('synthetic'))
        snapshot_mover._reconcile_dir_scoped_errors(
            self.conn, 'invalid_filename', self.subdir, [])
        n = self.conn.execute(
            "SELECT COUNT(*) FROM mover_errors").fetchone()[0]
        self.assertEqual(n, 0)


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
