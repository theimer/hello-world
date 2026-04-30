"""
Unit tests for native-host/snapshot_sealer.py.

The sealer is a thin CLI wrapper around snapshot_mover._seal_directory:
it parses argv, applies overrides to snapshot_mover's module-level
constants, validates that the target directory exists and isn't already
sealed, and delegates the actual manifest write.

Run with:
    cd browser-visit-logger
    pytest tests/test_snapshot_sealer.py -v
"""
import logging
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

import host                # resolved via conftest.py
import snapshot_mover      # resolved via conftest.py
import snapshot_sealer     # resolved via conftest.py


class _SealerTestBase(unittest.TestCase):
    """Isolated dest dir + DB; restores snapshot_mover globals after each test."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

        self.dest_dir = os.path.join(self.tmp.name, 'icloud')
        self.db_file  = os.path.join(self.tmp.name, 'visits.db')
        os.makedirs(self.dest_dir)

        # Initialize DB schema so sealing has somewhere to look up rows.
        conn = sqlite3.connect(self.db_file)
        host.ensure_db(conn)
        conn.close()

        # Save and later restore snapshot_mover module-level state that the
        # sealer's CLI mutates (--db / --dest / --verbose).
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

    def _make_dir(self, name='2024-01-15'):
        path = os.path.join(self.dest_dir, name)
        os.makedirs(path)
        return path


# ---------------------------------------------------------------------------
# Successful seal paths
# ---------------------------------------------------------------------------
class TestSealerHappyPath(_SealerTestBase):

    def test_seals_directory_by_date(self):
        date_subdir = self._make_dir('2024-01-15')
        rc = snapshot_sealer.cli([
            '--db', self.db_file, '--dest', self.dest_dir, '2024-01-15',
        ])
        self.assertEqual(rc, 0)
        self.assertTrue(os.path.exists(
            os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)))

    def test_manifest_is_read_only(self):
        date_subdir = self._make_dir('2024-01-15')
        snapshot_sealer.cli([
            '--db', self.db_file, '--dest', self.dest_dir, '2024-01-15',
        ])
        manifest = os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)
        mode = os.stat(manifest).st_mode & 0o777
        self.assertEqual(mode, 0o444)

    def test_seals_directory_by_absolute_path(self):
        # When the argument contains a path separator (or is absolute), it's
        # used verbatim — no joining with ICLOUD_SNAPSHOTS_DIR.
        date_subdir = self._make_dir('2024-01-15')
        rc = snapshot_sealer.cli(['--db', self.db_file, date_subdir])
        self.assertEqual(rc, 0)
        self.assertTrue(os.path.exists(
            os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)))

    def test_seals_today_or_future_unconditionally(self):
        # Unlike the auto seal pass, manual sealing ignores the past-date guard.
        date_subdir = self._make_dir('2099-12-31')
        rc = snapshot_sealer.cli([
            '--db', self.db_file, '--dest', self.dest_dir, '2099-12-31',
        ])
        self.assertEqual(rc, 0)
        self.assertTrue(os.path.exists(
            os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)))

    def test_dry_run_does_not_write_manifest(self):
        date_subdir = self._make_dir('2024-01-15')
        rc = snapshot_sealer.cli([
            '--db', self.db_file, '--dest', self.dest_dir,
            '--dry-run', '2024-01-15',
        ])
        self.assertEqual(rc, 0)
        self.assertFalse(os.path.exists(
            os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)))

    def test_uses_module_default_dest_when_flag_omitted(self):
        # If --dest is not given, the sealer uses snapshot_mover's module-level
        # ICLOUD_SNAPSHOTS_DIR; tweak it directly to point at our test dir.
        snapshot_mover.ICLOUD_SNAPSHOTS_DIR = self.dest_dir
        date_subdir = self._make_dir('2024-01-15')
        rc = snapshot_sealer.cli(['--db', self.db_file, '2024-01-15'])
        self.assertEqual(rc, 0)
        self.assertTrue(os.path.exists(
            os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)))

    def test_uses_module_default_db_when_flag_omitted(self):
        # If --db is not given, the sealer uses snapshot_mover.DB_FILE.
        snapshot_mover.DB_FILE = self.db_file
        date_subdir = self._make_dir('2024-01-15')
        rc = snapshot_sealer.cli(['--dest', self.dest_dir, '2024-01-15'])
        self.assertEqual(rc, 0)
        self.assertTrue(os.path.exists(
            os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)))

    def test_verbose_flag_sets_debug_log_level(self):
        self._make_dir('2024-01-15')
        snapshot_sealer.cli([
            '--db', self.db_file, '--dest', self.dest_dir,
            '--verbose', '2024-01-15',
        ])
        self.assertEqual(snapshot_mover.logger.level, logging.DEBUG)


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------
class TestSealerErrorPaths(_SealerTestBase):

    def test_nonexistent_directory_returns_nonzero(self):
        rc = snapshot_sealer.cli([
            '--db', self.db_file, '--dest', self.dest_dir, '2024-12-31',
        ])
        self.assertNotEqual(rc, 0)

    def test_already_sealed_directory_returns_nonzero(self):
        date_subdir = self._make_dir('2024-01-15')
        Path(date_subdir, snapshot_mover.MANIFEST_FILENAME).write_text(
            'preexisting\n', encoding='utf-8'
        )
        rc = snapshot_sealer.cli([
            '--db', self.db_file, '--dest', self.dest_dir, '2024-01-15',
        ])
        self.assertNotEqual(rc, 0)

    def test_already_sealed_does_not_overwrite_existing_manifest(self):
        date_subdir = self._make_dir('2024-01-15')
        manifest = os.path.join(date_subdir, snapshot_mover.MANIFEST_FILENAME)
        Path(manifest).write_text('preexisting\n', encoding='utf-8')
        snapshot_sealer.cli([
            '--db', self.db_file, '--dest', self.dest_dir, '2024-01-15',
        ])
        self.assertEqual(Path(manifest).read_text(encoding='utf-8'),
                         'preexisting\n')

    def test_missing_db_returns_nonzero(self):
        self._make_dir('2024-01-15')
        rc = snapshot_sealer.cli([
            '--db', os.path.join(self.tmp.name, 'nonexistent.db'),
            '--dest', self.dest_dir, '2024-01-15',
        ])
        self.assertNotEqual(rc, 0)


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
        self.assertEqual(snapshot_sealer._resolve_target('2024-01-15'),
                         '/var/icloud/2024-01-15')

    def test_absolute_path_used_verbatim(self):
        self.assertEqual(snapshot_sealer._resolve_target('/some/abs/path'),
                         '/some/abs/path')

    def test_relative_path_with_separator_used_verbatim(self):
        # Once any path separator appears, treat as a path and don't reroot.
        self.assertEqual(snapshot_sealer._resolve_target('rel/2024-01-15'),
                         'rel/2024-01-15')


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
