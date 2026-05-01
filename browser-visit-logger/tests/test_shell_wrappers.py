"""
Smoke tests for the four executable Bash wrappers at the repo root.

The wrappers are intentionally thin — they resolve their own location,
forward all arguments to the matching Python script, and intercept
--help / -h to print a short wrapper note before delegating to the
Python script's own --help output.

These tests verify each wrapper is:
  - Present at the expected path
  - Executable (mode includes the user-execute bit)
  - Accepts --help and -h, and the output contains both the wrapper
    blurb and the Python script's argparse 'usage:' line
  - Forwards regular CLI flags through to the Python script

Run with:
    cd browser-visit-logger
    pytest tests/test_shell_wrappers.py -v
"""
import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# (wrapper_name, target_python_script_basename)
WRAPPERS = [
    ('move_snapshot',             'snapshot_mover.py'),
    ('seal_snapshot_directory',   'snapshot_sealer.py'),
    ('verify_snapshot_directory', 'snapshot_verifier.py'),
    ('reset_visits_data',         'reset.py'),
    ('rebuild_visits_data',       'visits_rebuilder.py'),
]


class TestWrapperPresence(unittest.TestCase):
    """File-level checks: each wrapper exists and is executable."""

    def test_each_wrapper_exists_at_repo_root(self):
        for name, _ in WRAPPERS:
            with self.subTest(wrapper=name):
                path = REPO_ROOT / name
                self.assertTrue(path.is_file(), f'{path} not found')

    def test_each_wrapper_is_executable(self):
        for name, _ in WRAPPERS:
            with self.subTest(wrapper=name):
                path = REPO_ROOT / name
                mode = path.stat().st_mode
                self.assertTrue(mode & stat.S_IXUSR,
                                f'{path} is not user-executable')

    def test_each_wrapper_uses_bash_shebang(self):
        for name, _ in WRAPPERS:
            with self.subTest(wrapper=name):
                first_line = (REPO_ROOT / name).read_text(
                    encoding='utf-8').splitlines()[0]
                self.assertIn('bash', first_line,
                              f'{name} should use a bash shebang')


class TestWrapperHelp(unittest.TestCase):
    """`--help` and `-h` print both the wrapper blurb and Python --help."""

    def _help(self, name, flag):
        result = subprocess.run(
            [str(REPO_ROOT / name), flag],
            capture_output=True, text=True, timeout=10,
        )
        # argparse exits 0 on --help.
        self.assertEqual(result.returncode, 0,
                         f'{name} {flag} exited {result.returncode}; '
                         f'stderr: {result.stderr}')
        return result.stdout

    def test_long_help_flag_invokes_python_help(self):
        for name, target_basename in WRAPPERS:
            with self.subTest(wrapper=name):
                out = self._help(name, '--help')
                # Wrapper blurb mentions the wrapper's name…
                self.assertIn(name, out)
                # …and the Python script's argparse 'usage:' header.
                self.assertIn('usage:', out)
                # And references the underlying script (so users see what's
                # being delegated to).
                self.assertIn(target_basename, out)

    def test_short_help_flag_invokes_python_help(self):
        for name, _ in WRAPPERS:
            with self.subTest(wrapper=name):
                out = self._help(name, '-h')
                self.assertIn(name, out)
                self.assertIn('usage:', out)


class TestWrapperForwarding(unittest.TestCase):
    """Spot-check that non-help arguments reach the underlying Python script."""

    def test_move_snapshot_forwards_show_errors_against_temp_db(self):
        # move_snapshot --db <path> --show-errors should reach the mover's
        # CLI, which creates the DB on the fly and prints the empty-table
        # message.
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, 'visits.db')
            result = subprocess.run(
                [str(REPO_ROOT / 'move_snapshot'),
                 '--db', db, '--show-errors'],
                capture_output=True, text=True, timeout=10,
            )
        self.assertEqual(result.returncode, 0,
                         f'stderr: {result.stderr}')
        self.assertIn('No pending mover errors', result.stdout)

    def test_seal_snapshot_directory_forwards_dry_run(self):
        # seal_snapshot_directory --dry-run on a real but empty date dir
        # should exit 0 without writing a manifest.
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, 'visits.db')
            # Touch the DB so the sealer's "no DB" guard doesn't fire.
            Path(db).touch()
            dest = os.path.join(tmp, 'icloud')
            date_dir = os.path.join(dest, '2024-01-15')
            os.makedirs(date_dir)
            result = subprocess.run(
                [str(REPO_ROOT / 'seal_snapshot_directory'),
                 '--db', db, '--dest', dest, '--dry-run', '2024-01-15'],
                capture_output=True, text=True, timeout=10,
            )
            self.assertEqual(result.returncode, 0,
                             f'stderr: {result.stderr}')
            self.assertFalse(
                (Path(date_dir) / 'MANIFEST.tsv').exists(),
                'dry-run should not write a manifest')

    def test_verify_snapshot_directory_forwards_all_against_empty_db(self):
        # verify_snapshot_directory --all --quiet against a fresh DB with
        # no sealed rows should exit 0 with empty stdout.
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, 'visits.db')
            Path(db).touch()
            dest = os.path.join(tmp, 'icloud')
            os.makedirs(dest)
            result = subprocess.run(
                [str(REPO_ROOT / 'verify_snapshot_directory'),
                 '--db', db, '--dest', dest, '--all', '--quiet'],
                capture_output=True, text=True, timeout=10,
            )
            self.assertEqual(result.returncode, 0,
                             f'stderr: {result.stderr}')
            self.assertEqual(result.stdout, '')

    def test_reset_visits_data_forwards_log_flag_with_force(self):
        # reset_visits_data --log -f against env-isolated paths should run
        # without a confirmation prompt and exit 0.
        with tempfile.TemporaryDirectory() as tmp:
            env = os.environ.copy()
            env['BVL_LOG_FILE']  = os.path.join(tmp, 'visits.log')
            env['BVL_HOST_LOG']  = os.path.join(tmp, 'host.log')
            env['BVL_MOVER_LOG'] = os.path.join(tmp, 'mover.log')
            env['BVL_DB_FILE']   = os.path.join(tmp, 'visits.db')
            env['BVL_DOWNLOADS_SNAPSHOTS_DIR'] = os.path.join(tmp, 'dl')
            result = subprocess.run(
                [str(REPO_ROOT / 'reset_visits_data'), '--log', '-f'],
                capture_output=True, text=True, timeout=10, env=env,
            )
            self.assertEqual(result.returncode, 0,
                             f'stderr: {result.stderr}')

    def test_rebuild_visits_data_forwards_overrides_against_empty_log(self):
        # rebuild_visits_data --log <empty file> --db <new path> --source/--dest
        # against an empty log should reach the rebuilder, exit 0, and print
        # the per-phase summary lines.
        with tempfile.TemporaryDirectory() as tmp:
            log = os.path.join(tmp, 'visits.log')
            Path(log).touch()
            db   = os.path.join(tmp, 'visits.db')
            src  = os.path.join(tmp, 'dl');     os.makedirs(src)
            dest = os.path.join(tmp, 'icloud'); os.makedirs(dest)
            result = subprocess.run(
                [str(REPO_ROOT / 'rebuild_visits_data'),
                 '--log', log, '--db', db, '--source', src, '--dest', dest],
                capture_output=True, text=True, timeout=10,
            )
            self.assertEqual(result.returncode, 0,
                             f'stderr: {result.stderr}')
            self.assertIn('replay:',    result.stdout)
            self.assertIn('rehydrate:', result.stdout)


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
