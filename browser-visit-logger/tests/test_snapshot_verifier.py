"""
Unit tests for native-host/snapshot_verifier.py.

Two layers of testing:

  - TestVerifyDirectory: direct calls to verify_directory(conn, dir),
    one assertion per check.
  - TestVerifierCli + TestVerifyAll + TestRecordIntegration: cli()
    end-to-end against a temp DB / dest dir, including the --record
    integration with mover_errors.

Run with:
    cd browser-visit-logger
    pytest tests/test_snapshot_verifier.py -v
"""
import io
import logging
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import host                # resolved via conftest.py
import snapshot_mover      # resolved via conftest.py
import snapshot_sealer     # resolved via conftest.py
import snapshot_verifier   # resolved via conftest.py


# ---------------------------------------------------------------------------
# Base — isolated dest + DB; restores snapshot_mover globals after each test
# ---------------------------------------------------------------------------
class _VerifierTestBase(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

        self.dest_dir = os.path.join(self.tmp.name, 'icloud')
        self.db_file  = os.path.join(self.tmp.name, 'visits.db')
        os.makedirs(self.dest_dir)

        conn = sqlite3.connect(self.db_file)
        host.ensure_db(conn)
        snapshot_mover._ensure_snapshots_table(conn)
        snapshot_mover._ensure_mover_errors_table(conn)
        conn.close()

        # Save & restore module-level globals the CLI mutates.
        self._saved = {
            name: getattr(snapshot_mover, name)
            for name in ('ICLOUD_SNAPSHOTS_DIR', 'DB_FILE')
        }
        self._saved_log_level = snapshot_mover.logger.level
        self.addCleanup(self._restore)

    def _restore(self):
        for name, value in self._saved.items():
            setattr(snapshot_mover, name, value)
        snapshot_mover.logger.setLevel(self._saved_log_level)

    # -- helpers --

    def _seed_dir(self, date_str, files=()):
        """Create dest_dir/<date_str>/ with the given files + visits/events
        rows + a sealed=1 snapshots row.

        files is an iterable of (filename, table, url, ts, title); pass
        table=None to leave a file with no DB row.  Returns the date subdir.
        """
        date_subdir = os.path.join(self.dest_dir, date_str)
        os.makedirs(date_subdir, exist_ok=True)
        conn = sqlite3.connect(self.db_file)
        try:
            conn.execute(
                "INSERT OR IGNORE INTO snapshots (date, sealed) VALUES (?, 1)",
                (date_str,))
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

    def _seal(self, date_str):
        """Run the sealer to produce a real, valid manifest in date_str."""
        snapshot_sealer.cli([
            '--db', self.db_file, '--dest', self.dest_dir, date_str,
        ])

    def _open_conn(self):
        conn = sqlite3.connect(self.db_file)
        snapshot_mover._ensure_mover_errors_table(conn)
        return conn


# ---------------------------------------------------------------------------
# verify_directory — one assertion per check
# ---------------------------------------------------------------------------
class TestVerifyDirectory(_VerifierTestBase):

    def _verify(self, date_subdir):
        conn = self._open_conn()
        try:
            return snapshot_verifier.verify_directory(conn, date_subdir)
        finally:
            conn.close()

    # --- happy path ---

    def test_healthy_manifest_passes_with_no_issues(self):
        self._seed_dir('2024-01-15', [
            ('2024-01-15T10-00-00Z-a.mhtml', 'read_events',
             'https://a.com', '2024-01-15T10:00:00Z', 'A'),
            ('2024-01-15T11-00-00Z-b.mhtml', 'skimmed_events',
             'https://b.com', '2024-01-15T11:00:00Z', 'B'),
        ])
        self._seal('2024-01-15')
        is_valid, issues = self._verify(
            os.path.join(self.dest_dir, '2024-01-15'))
        self.assertTrue(is_valid, f'unexpected issues: {issues}')
        self.assertEqual(issues, [])

    # --- check 1: existence ---

    def test_missing_manifest_fails(self):
        date_subdir = self._seed_dir('2024-01-15')
        # No seal — no manifest written.
        is_valid, issues = self._verify(date_subdir)
        self.assertFalse(is_valid)
        self.assertTrue(any('Manifest file not found' in i for i in issues))

    # --- check 2: read-only mode ---

    def test_writable_manifest_flags_mode(self):
        date_subdir = self._seed_dir('2024-01-15', [
            ('2024-01-15T10-00-00Z-a.mhtml', 'read_events',
             'https://a.com', '2024-01-15T10:00:00Z', 'A'),
        ])
        self._seal('2024-01-15')
        manifest = os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)
        os.chmod(manifest, 0o644)
        is_valid, issues = self._verify(date_subdir)
        self.assertFalse(is_valid)
        self.assertTrue(any('not read-only' in i for i in issues))

    # --- check 3: read failure ---

    def test_unreadable_manifest_is_reported(self):
        date_subdir = self._seed_dir('2024-01-15')
        manifest = os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)
        Path(manifest).write_text('whatever\n')
        with patch('builtins.open',
                    side_effect=OSError('permission denied')):
            is_valid, issues = self._verify(date_subdir)
        self.assertFalse(is_valid)
        self.assertTrue(any('Could not read manifest' in i for i in issues))

    # --- check 4: header / parse ---

    def test_empty_manifest_fails(self):
        date_subdir = self._seed_dir('2024-01-15')
        manifest = os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)
        Path(manifest).write_text('')
        os.chmod(manifest, 0o444)
        is_valid, issues = self._verify(date_subdir)
        self.assertFalse(is_valid)
        self.assertTrue(any('empty' in i.lower() for i in issues))

    def test_wrong_header_fails(self):
        date_subdir = self._seed_dir('2024-01-15')
        manifest = os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)
        Path(manifest).write_text('BAD\tHEADER\n')
        os.chmod(manifest, 0o444)
        is_valid, issues = self._verify(date_subdir)
        self.assertFalse(is_valid)
        self.assertTrue(any('Header mismatch' in i for i in issues))

    def test_row_with_wrong_column_count_is_flagged(self):
        date_subdir = self._seed_dir('2024-01-15', [
            ('2024-01-15T10-00-00Z-a.mhtml', 'read_events',
             'https://a.com', '2024-01-15T10:00:00Z', 'A'),
        ])
        self._seal('2024-01-15')
        manifest = os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)
        os.chmod(manifest, 0o644)
        Path(manifest).write_text(
            'filename\ttag\ttimestamp\turl\ttitle\n'
            'only-three\tcols\there\n'
        )
        os.chmod(manifest, 0o444)
        is_valid, issues = self._verify(date_subdir)
        self.assertFalse(is_valid)
        self.assertTrue(any('expected 5 columns' in i for i in issues))

    def test_duplicate_filename_in_manifest_is_flagged(self):
        date_subdir = self._seed_dir('2024-01-15', [
            ('2024-01-15T10-00-00Z-a.mhtml', 'read_events',
             'https://a.com', '2024-01-15T10:00:00Z', 'A'),
        ])
        self._seal('2024-01-15')
        manifest = os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)
        os.chmod(manifest, 0o644)
        Path(manifest).write_text(
            'filename\ttag\ttimestamp\turl\ttitle\n'
            '2024-01-15T10-00-00Z-a.mhtml\tread\t2024-01-15T10:00:00Z\thttps://a.com\tA\n'
            '2024-01-15T10-00-00Z-a.mhtml\tread\t2024-01-15T10:00:00Z\thttps://a.com\tA\n'
        )
        os.chmod(manifest, 0o444)
        is_valid, issues = self._verify(date_subdir)
        self.assertFalse(is_valid)
        self.assertTrue(any('duplicate filename' in i for i in issues))

    # --- check 5/6: set comparison ---

    def test_extra_file_in_directory_is_flagged(self):
        date_subdir = self._seed_dir('2024-01-15', [
            ('2024-01-15T10-00-00Z-a.mhtml', 'read_events',
             'https://a.com', '2024-01-15T10:00:00Z', 'A'),
        ])
        self._seal('2024-01-15')
        # Drop an extra conforming file in *after* sealing — manifest doesn't list it.
        extra = '2024-01-15T11-00-00Z-extra.mhtml'
        Path(date_subdir, extra).write_bytes(b'x')
        is_valid, issues = self._verify(date_subdir)
        self.assertFalse(is_valid)
        self.assertTrue(any(extra in i and 'not listed' in i for i in issues))

    def test_missing_file_referenced_by_manifest_is_flagged(self):
        date_subdir = self._seed_dir('2024-01-15', [
            ('2024-01-15T10-00-00Z-a.mhtml', 'read_events',
             'https://a.com', '2024-01-15T10:00:00Z', 'A'),
        ])
        self._seal('2024-01-15')
        # Delete the file referenced by the manifest.
        os.chmod(os.path.join(date_subdir, '2024-01-15T10-00-00Z-a.mhtml'), 0o644)
        os.unlink(os.path.join(date_subdir, '2024-01-15T10-00-00Z-a.mhtml'))
        is_valid, issues = self._verify(date_subdir)
        self.assertFalse(is_valid)
        self.assertTrue(any('no such file in directory' in i for i in issues))

    def test_non_conforming_file_in_directory_alone_is_flagged(self):
        # A non-conforming file sitting in the dir, NOT mentioned in the
        # manifest.  Used to be silently OK; now flagged as a violation
        # of the "no non-conforming files in a sealed dir" invariant.
        date_subdir = self._seed_dir('2024-01-15', [
            ('2024-01-15T10-00-00Z-a.mhtml', 'read_events',
             'https://a.com', '2024-01-15T10:00:00Z', 'A'),
        ])
        self._seal('2024-01-15')
        Path(date_subdir, 'README.txt').write_text('not a snapshot')
        is_valid, issues = self._verify(date_subdir)
        self.assertFalse(is_valid)
        self.assertTrue(any('Non-conforming file in directory' in i
                            and 'README.txt' in i for i in issues))

    def test_non_conforming_file_in_manifest_is_flagged(self):
        date_subdir = self._seed_dir('2024-01-15', [
            ('2024-01-15T10-00-00Z-a.mhtml', 'read_events',
             'https://a.com', '2024-01-15T10:00:00Z', 'A'),
        ])
        self._seal('2024-01-15')
        manifest = os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)
        os.chmod(manifest, 0o644)
        # Inject a non-conforming filename row.  Also drop the file on disk
        # so the set check doesn't double-flag it.
        Path(manifest).write_text(
            'filename\ttag\ttimestamp\turl\ttitle\n'
            'random-name.txt\t\t\t\t\n'
            '2024-01-15T10-00-00Z-a.mhtml\tread\t2024-01-15T10:00:00Z\thttps://a.com\tA\n'
        )
        os.chmod(manifest, 0o444)
        Path(date_subdir, 'random-name.txt').write_bytes(b'x')
        is_valid, issues = self._verify(date_subdir)
        self.assertFalse(is_valid)
        self.assertTrue(any("non-conforming filename" in i for i in issues))

    # --- check 7: metadata ---

    def test_metadata_mismatch_against_db_is_flagged(self):
        date_subdir = self._seed_dir('2024-01-15', [
            ('2024-01-15T10-00-00Z-a.mhtml', 'read_events',
             'https://a.com', '2024-01-15T10:00:00Z', 'A'),
        ])
        self._seal('2024-01-15')
        manifest = os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)
        # Simulate the DB having since been updated to a different title.
        conn = sqlite3.connect(self.db_file)
        conn.execute("UPDATE visits SET title = 'NEW TITLE' WHERE url = ?",
                     ('https://a.com',))
        conn.commit()
        conn.close()
        is_valid, issues = self._verify(date_subdir)
        self.assertFalse(is_valid)
        self.assertTrue(any('metadata mismatch' in i for i in issues))

    def test_orphan_file_in_directory_is_flagged(self):
        # An orphan (conforming filename, no events row) sitting in the
        # directory violates the "correct sealed directory" invariant
        # even when the mover correctly excluded it from the manifest.
        date_subdir = self._seed_dir('2024-01-15', [
            ('2024-01-15T10-00-00Z-orphan.mhtml', None, None, None, None),
        ])
        self._seal('2024-01-15')
        is_valid, issues = self._verify(date_subdir)
        self.assertFalse(is_valid)
        self.assertTrue(any('conforming file in directory has no '
                            'corresponding events row' in i
                            for i in issues))

    def test_orphan_row_in_manifest_is_flagged(self):
        # A manually-injected orphan row (no DB backing) must fail the
        # tightened "every manifest row must have a DB row" check —
        # regardless of whether the row's metadata fields are empty
        # (legacy orphan format) or populated (corruption).
        date_subdir = self._seed_dir('2024-01-15', [
            ('2024-01-15T10-00-00Z-a.mhtml', 'read_events',
             'https://a.com', '2024-01-15T10:00:00Z', 'A'),
        ])
        self._seal('2024-01-15')
        # Add another conforming file on disk (no events row), then
        # rewrite the manifest to include both — emulating either an
        # old-format orphan row or a manifest desync.
        manifest = os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)
        Path(date_subdir, '2024-01-15T11-00-00Z-orphan.mhtml').write_bytes(b'o')
        os.chmod(manifest, 0o644)
        Path(manifest).write_text(
            'filename\ttag\ttimestamp\turl\ttitle\n'
            '2024-01-15T10-00-00Z-a.mhtml\tread\t2024-01-15T10:00:00Z\thttps://a.com\tA\n'
            '2024-01-15T11-00-00Z-orphan.mhtml\t\t\t\t\n'
        )
        os.chmod(manifest, 0o444)
        is_valid, issues = self._verify(date_subdir)
        self.assertFalse(is_valid)
        self.assertTrue(any('manifest row has no corresponding events row'
                            in i for i in issues))

    def test_subdirectory_entry_inside_date_subdir_is_skipped(self):
        # A nested dir under date_subdir isn't a snapshot file; the
        # verifier must skip it (not treat it as either conforming or
        # non-conforming) and still pass for the rest.
        date_subdir = self._seed_dir('2024-01-15', [
            ('2024-01-15T10-00-00Z-a.mhtml', 'read_events',
             'https://a.com', '2024-01-15T10:00:00Z', 'A'),
        ])
        self._seal('2024-01-15')
        os.makedirs(os.path.join(date_subdir, 'nested-dir'))
        is_valid, issues = self._verify(date_subdir)
        self.assertTrue(is_valid, f'unexpected issues: {issues}')

    def test_multiple_issues_are_all_reported(self):
        date_subdir = self._seed_dir('2024-01-15', [
            ('2024-01-15T10-00-00Z-a.mhtml', 'read_events',
             'https://a.com', '2024-01-15T10:00:00Z', 'A'),
        ])
        self._seal('2024-01-15')
        manifest = os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)
        # Make manifest writable + introduce two unrelated issues at once.
        os.chmod(manifest, 0o644)
        # Issue 1: extra file added after sealing.
        Path(date_subdir, '2024-01-15T11-00-00Z-extra.mhtml').write_bytes(b'x')
        # Issue 2: writable mode (we just chmod'd above).
        is_valid, issues = self._verify(date_subdir)
        self.assertFalse(is_valid)
        self.assertGreaterEqual(len(issues), 2)


# ---------------------------------------------------------------------------
# CLI — happy path, error paths, --quiet, --record
# ---------------------------------------------------------------------------
class TestVerifierCli(_VerifierTestBase):

    def test_verify_by_date_passes_for_healthy_dir(self):
        self._seed_dir('2024-01-15', [
            ('2024-01-15T10-00-00Z-a.mhtml', 'read_events',
             'https://a.com', '2024-01-15T10:00:00Z', 'A'),
        ])
        self._seal('2024-01-15')
        captured = io.StringIO()
        with patch('sys.stdout', captured):
            rc = snapshot_verifier.cli([
                '--db', self.db_file, '--dest', self.dest_dir, '2024-01-15'])
        self.assertEqual(rc, 0)
        self.assertIn('OK', captured.getvalue())

    def test_verify_by_absolute_path_passes_for_healthy_dir(self):
        date_subdir = self._seed_dir('2024-01-15', [
            ('2024-01-15T10-00-00Z-a.mhtml', 'read_events',
             'https://a.com', '2024-01-15T10:00:00Z', 'A'),
        ])
        self._seal('2024-01-15')
        captured = io.StringIO()
        with patch('sys.stdout', captured):
            rc = snapshot_verifier.cli(['--db', self.db_file, date_subdir])
        self.assertEqual(rc, 0)
        self.assertIn('OK', captured.getvalue())

    def test_verify_returns_one_on_failure(self):
        # Seed a sealed dir, then truncate the manifest.
        date_subdir = self._seed_dir('2024-01-15')
        self._seal('2024-01-15')
        manifest = os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)
        os.chmod(manifest, 0o644)
        Path(manifest).write_text('BAD\n')
        os.chmod(manifest, 0o444)
        captured = io.StringIO()
        with patch('sys.stdout', captured):
            rc = snapshot_verifier.cli([
                '--db', self.db_file, '--dest', self.dest_dir, '2024-01-15'])
        self.assertEqual(rc, 1)
        self.assertIn('FAILED', captured.getvalue())

    def test_quiet_suppresses_ok_output_on_success(self):
        self._seed_dir('2024-01-15', [
            ('2024-01-15T10-00-00Z-a.mhtml', 'read_events',
             'https://a.com', '2024-01-15T10:00:00Z', 'A'),
        ])
        self._seal('2024-01-15')
        captured = io.StringIO()
        with patch('sys.stdout', captured):
            rc = snapshot_verifier.cli([
                '--db', self.db_file, '--dest', self.dest_dir,
                '--quiet', '2024-01-15'])
        self.assertEqual(rc, 0)
        self.assertEqual(captured.getvalue(), '')

    def test_quiet_still_prints_failures(self):
        date_subdir = self._seed_dir('2024-01-15')
        self._seal('2024-01-15')
        manifest = os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)
        os.chmod(manifest, 0o644)
        Path(manifest).write_text('BAD\n')
        os.chmod(manifest, 0o444)
        captured = io.StringIO()
        with patch('sys.stdout', captured):
            rc = snapshot_verifier.cli([
                '--db', self.db_file, '--dest', self.dest_dir,
                '--quiet', '2024-01-15'])
        self.assertEqual(rc, 1)
        self.assertIn('FAILED', captured.getvalue())

    def test_missing_db_returns_one(self):
        rc = snapshot_verifier.cli([
            '--db', os.path.join(self.tmp.name, 'nope.db'),
            '--dest', self.dest_dir, '2024-01-15'])
        self.assertEqual(rc, 1)

    def test_missing_target_returns_one(self):
        rc = snapshot_verifier.cli([
            '--db', self.db_file, '--dest', self.dest_dir, '2024-12-31'])
        self.assertEqual(rc, 1)

    def test_no_arg_and_no_all_is_a_parse_error(self):
        with self.assertRaises(SystemExit):
            snapshot_verifier._parse_args(['--db', self.db_file])

    def test_directory_combined_with_all_is_a_parse_error(self):
        with self.assertRaises(SystemExit):
            snapshot_verifier._parse_args([
                '--db', self.db_file, '--all', '2024-01-15'])

    def test_verbose_sets_debug_log_level(self):
        # verbose run with --all and no sealed dirs is a quick no-op
        snapshot_verifier.cli(['--db', self.db_file, '-v', '--all'])
        self.assertEqual(snapshot_mover.logger.level, logging.DEBUG)

    def test_dest_override_is_applied(self):
        # Use an alternate dest dir with a healthy dir under it.
        alt_dest = os.path.join(self.tmp.name, 'alt-icloud')
        os.makedirs(alt_dest)
        self._saved['ICLOUD_SNAPSHOTS_DIR'] = snapshot_mover.ICLOUD_SNAPSHOTS_DIR
        snapshot_mover.ICLOUD_SNAPSHOTS_DIR = alt_dest
        date_str = '2024-01-15'
        date_subdir = os.path.join(alt_dest, date_str)
        os.makedirs(date_subdir)
        # Use the sealer to write a real manifest in the alt location.
        snapshot_sealer.cli(['--db', self.db_file, '--dest', alt_dest, date_str])
        captured = io.StringIO()
        with patch('sys.stdout', captured):
            rc = snapshot_verifier.cli([
                '--db', self.db_file, '--dest', alt_dest, date_str])
        self.assertEqual(rc, 0)
        self.assertIn('OK', captured.getvalue())


# ---------------------------------------------------------------------------
# --all
# ---------------------------------------------------------------------------
class TestVerifyAll(_VerifierTestBase):

    def test_all_with_no_sealed_dirs_prints_message_and_returns_zero(self):
        captured = io.StringIO()
        with patch('sys.stdout', captured):
            rc = snapshot_verifier.cli([
                '--db', self.db_file, '--dest', self.dest_dir, '--all'])
        self.assertEqual(rc, 0)
        self.assertIn('No sealed directories', captured.getvalue())

    def test_all_with_no_sealed_dirs_quiet_prints_nothing(self):
        captured = io.StringIO()
        with patch('sys.stdout', captured):
            rc = snapshot_verifier.cli([
                '--db', self.db_file, '--dest', self.dest_dir,
                '--all', '--quiet'])
        self.assertEqual(rc, 0)
        self.assertEqual(captured.getvalue(), '')

    def test_all_passes_when_every_sealed_dir_is_healthy(self):
        for date in ('2024-01-15', '2024-01-16'):
            self._seed_dir(date, [
                (f'{date}T10-00-00Z-a.mhtml', 'read_events',
                 f'https://{date}.com', f'{date}T10:00:00Z', 'A'),
            ])
            self._seal(date)
        captured = io.StringIO()
        with patch('sys.stdout', captured):
            rc = snapshot_verifier.cli([
                '--db', self.db_file, '--dest', self.dest_dir, '--all'])
        self.assertEqual(rc, 0)
        out = captured.getvalue()
        self.assertIn('2024-01-15', out)
        self.assertIn('2024-01-16', out)
        self.assertIn('OK', out)

    def test_all_returns_one_when_any_sealed_dir_fails(self):
        # Two sealed dirs; corrupt one.
        for date in ('2024-01-15', '2024-01-16'):
            self._seed_dir(date, [
                (f'{date}T10-00-00Z-a.mhtml', 'read_events',
                 f'https://{date}.com', f'{date}T10:00:00Z', 'A'),
            ])
            self._seal(date)
        manifest = os.path.join(
            self.dest_dir, '2024-01-16', snapshot_mover.MANIFEST_FILENAME)
        os.chmod(manifest, 0o644)
        Path(manifest).write_text('BAD\n')
        os.chmod(manifest, 0o444)
        captured = io.StringIO()
        with patch('sys.stdout', captured):
            rc = snapshot_verifier.cli([
                '--db', self.db_file, '--dest', self.dest_dir, '--all'])
        self.assertEqual(rc, 1)
        self.assertIn('OK', captured.getvalue())
        self.assertIn('FAILED', captured.getvalue())

    def test_all_skips_sealed_rows_with_missing_directories(self):
        # Insert a sealed row whose dir doesn't exist on disk.  --all
        # should silently skip it (the seal pass already reports it).
        conn = sqlite3.connect(self.db_file)
        conn.execute(
            "INSERT INTO snapshots (date, sealed) VALUES (?, 1)",
            ('2024-01-15',))
        conn.commit()
        conn.close()
        captured = io.StringIO()
        with patch('sys.stdout', captured):
            rc = snapshot_verifier.cli([
                '--db', self.db_file, '--dest', self.dest_dir, '--all'])
        self.assertEqual(rc, 0)
        self.assertNotIn('2024-01-15', captured.getvalue())


# ---------------------------------------------------------------------------
# --record integration with mover_errors
# ---------------------------------------------------------------------------
class TestRecordIntegration(_VerifierTestBase):

    def _errors(self):
        conn = sqlite3.connect(self.db_file)
        try:
            return conn.execute(
                "SELECT operation, target, message, immediate, notified "
                "FROM mover_errors WHERE operation = 'manifest_invalid'"
            ).fetchall()
        finally:
            conn.close()

    def test_record_creates_mover_errors_row_on_failure(self):
        date_subdir = self._seed_dir('2024-01-15')
        self._seal('2024-01-15')
        manifest = os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)
        os.chmod(manifest, 0o644)
        Path(manifest).write_text('BAD\n')
        os.chmod(manifest, 0o444)
        snapshot_verifier.cli([
            '--db', self.db_file, '--dest', self.dest_dir,
            '--quiet', '--record', '2024-01-15'])
        rows = self._errors()
        self.assertEqual(len(rows), 1)
        op, target, message, immediate, _notified = rows[0]
        self.assertEqual(op, 'manifest_invalid')
        self.assertEqual(target, date_subdir)
        self.assertIn('Header mismatch', message)
        self.assertEqual(immediate, 1)   # manifest_invalid is immediate

    def test_record_clears_existing_mover_errors_row_on_success(self):
        # Pre-seed an outstanding manifest_invalid row, then verify the
        # (now-healthy) directory.  The row should be cleared.
        date_subdir = self._seed_dir('2024-01-15', [
            ('2024-01-15T10-00-00Z-a.mhtml', 'read_events',
             'https://a.com', '2024-01-15T10:00:00Z', 'A'),
        ])
        self._seal('2024-01-15')
        conn = sqlite3.connect(self.db_file)
        snapshot_mover._record_error(
            conn, 'manifest_invalid', date_subdir,
            ValueError('previous failure'))
        conn.close()
        snapshot_verifier.cli([
            '--db', self.db_file, '--dest', self.dest_dir,
            '--quiet', '--record', '2024-01-15'])
        self.assertEqual(self._errors(), [])

    def test_no_record_does_not_touch_mover_errors(self):
        date_subdir = self._seed_dir('2024-01-15')
        self._seal('2024-01-15')
        manifest = os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)
        os.chmod(manifest, 0o644)
        Path(manifest).write_text('BAD\n')
        os.chmod(manifest, 0o444)
        snapshot_verifier.cli([
            '--db', self.db_file, '--dest', self.dest_dir,
            '--quiet', '2024-01-15'])
        self.assertEqual(self._errors(), [])

    def test_record_drains_notification_queue_via_escalate(self):
        # With --record, the verifier should call _escalate_errors so the
        # immediate-class manifest_invalid row notifies the user without
        # waiting for the next snapshot_mover.py tick.
        date_subdir = self._seed_dir('2024-01-15')
        self._seal('2024-01-15')
        manifest = os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)
        os.chmod(manifest, 0o644)
        Path(manifest).write_text('BAD\n')
        os.chmod(manifest, 0o444)
        with patch.object(snapshot_mover, '_notify_user') as mock_notify:
            snapshot_verifier.cli([
                '--db', self.db_file, '--dest', self.dest_dir,
                '--quiet', '--record', '2024-01-15'])
        mock_notify.assert_called_once()
        body = mock_notify.call_args[0][1]
        self.assertIn('manifest_invalid', body)
        # Row was flipped to notified=1 by the escalate pass.
        self.assertEqual(self._errors()[0][4], 1)

    def test_no_record_does_not_call_escalate(self):
        # Without --record, _escalate_errors must not run — the verifier
        # behaves as a pure read-only check.
        date_subdir = self._seed_dir('2024-01-15')
        self._seal('2024-01-15')
        manifest = os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)
        os.chmod(manifest, 0o644)
        Path(manifest).write_text('BAD\n')
        os.chmod(manifest, 0o444)
        with patch.object(snapshot_mover, '_notify_user') as mock_notify, \
             patch.object(snapshot_mover, '_escalate_errors') as mock_escalate:
            snapshot_verifier.cli([
                '--db', self.db_file, '--dest', self.dest_dir,
                '--quiet', '2024-01-15'])
        mock_notify.assert_not_called()
        mock_escalate.assert_not_called()

    def test_record_with_all_records_failures_per_directory(self):
        for date in ('2024-01-15', '2024-01-16'):
            self._seed_dir(date, [
                (f'{date}T10-00-00Z-a.mhtml', 'read_events',
                 f'https://{date}.com', f'{date}T10:00:00Z', 'A'),
            ])
            self._seal(date)
        # Corrupt the second one.
        manifest = os.path.join(
            self.dest_dir, '2024-01-16', snapshot_mover.MANIFEST_FILENAME)
        os.chmod(manifest, 0o644)
        Path(manifest).write_text('BAD\n')
        os.chmod(manifest, 0o444)
        snapshot_verifier.cli([
            '--db', self.db_file, '--dest', self.dest_dir,
            '--quiet', '--record', '--all'])
        rows = self._errors()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1],
                         os.path.join(self.dest_dir, '2024-01-16'))


# ---------------------------------------------------------------------------
# _resolve_target unit tests
# ---------------------------------------------------------------------------
class TestResolveTarget(unittest.TestCase):

    def setUp(self):
        self._saved = snapshot_mover.ICLOUD_SNAPSHOTS_DIR
        snapshot_mover.ICLOUD_SNAPSHOTS_DIR = '/var/icloud'

    def tearDown(self):
        snapshot_mover.ICLOUD_SNAPSHOTS_DIR = self._saved

    def test_bare_date_joined_under_icloud_dir(self):
        self.assertEqual(snapshot_verifier._resolve_target('2024-01-15'),
                         '/var/icloud/2024-01-15')

    def test_absolute_path_used_verbatim(self):
        self.assertEqual(snapshot_verifier._resolve_target('/some/abs/path'),
                         '/some/abs/path')

    def test_relative_path_with_separator_used_verbatim(self):
        self.assertEqual(snapshot_verifier._resolve_target('rel/2024-01-15'),
                         'rel/2024-01-15')


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
