"""
Smoke tests for the executable Bash wrappers at the repo root.

The wrappers are intentionally thin — they resolve their own location,
forward all arguments to the matching Python script (or Swift binary
for verify_snapshot_directory) and intercept --help / -h to print a
short wrapper note before delegating to the underlying tool's --help.

These tests verify each wrapper is:
  - Present at the expected path
  - Executable (mode includes the user-execute bit)
  - Accepts --help and -h
  - Forwards regular CLI flags through to the underlying tool

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

# Python-backed wrappers — invoke `python3 native-host/<script>.py "$@"`.
# Their --help output goes through argparse, so it includes "usage:".
PYTHON_WRAPPERS = [
    ('seal_snapshot_directory', 'snapshot_sealer.py'),
    ('reset_visits_data',       'reset.py'),
    ('rebuild_visits_data',     'visits_rebuilder.py'),
]

# verify_snapshot_directory wraps the Swift BVLVerifier binary built
# under swift/.build/release/.  Tested separately from the Python
# wrappers because its --help is a hand-rolled string, not argparse.
SWIFT_WRAPPER = 'verify_snapshot_directory'

ALL_WRAPPERS = [name for name, _ in PYTHON_WRAPPERS] + [SWIFT_WRAPPER]


class TestWrapperPresence(unittest.TestCase):
    """File-level checks: each wrapper exists, is executable, uses bash."""

    def test_each_wrapper_exists_at_repo_root(self):
        for name in ALL_WRAPPERS:
            with self.subTest(wrapper=name):
                self.assertTrue((REPO_ROOT / name).is_file(),
                                f'{name} not found')

    def test_each_wrapper_is_executable(self):
        for name in ALL_WRAPPERS:
            with self.subTest(wrapper=name):
                mode = (REPO_ROOT / name).stat().st_mode
                self.assertTrue(mode & stat.S_IXUSR,
                                f'{name} is not user-executable')

    def test_each_wrapper_uses_bash_shebang(self):
        for name in ALL_WRAPPERS:
            with self.subTest(wrapper=name):
                first_line = (REPO_ROOT / name).read_text(
                    encoding='utf-8').splitlines()[0]
                self.assertIn('bash', first_line)


class TestPythonWrapperHelp(unittest.TestCase):
    """Python wrappers' --help / -h prints the argparse `usage:` line and
    references the underlying script name."""

    def _help(self, name, flag):
        result = subprocess.run(
            [str(REPO_ROOT / name), flag],
            capture_output=True, text=True, timeout=10,
        )
        self.assertEqual(result.returncode, 0,
                         f'{name} {flag} exited {result.returncode}; '
                         f'stderr: {result.stderr}')
        return result.stdout

    def test_long_help_flag_invokes_python_help(self):
        for name, target in PYTHON_WRAPPERS:
            with self.subTest(wrapper=name):
                out = self._help(name, '--help')
                self.assertIn(name,   out)
                self.assertIn('usage:', out)
                self.assertIn(target, out)

    def test_short_help_flag_invokes_python_help(self):
        for name, _ in PYTHON_WRAPPERS:
            with self.subTest(wrapper=name):
                out = self._help(name, '-h')
                self.assertIn(name,   out)
                self.assertIn('usage:', out)


class TestSwiftWrapperHelp(unittest.TestCase):
    """The verify wrapper delegates to BVLVerifier, whose --help is a
    hand-rolled summary (no argparse)."""

    def _help(self, flag):
        result = subprocess.run(
            [str(REPO_ROOT / SWIFT_WRAPPER), flag],
            capture_output=True, text=True, timeout=10,
        )
        self.assertEqual(result.returncode, 0,
                         f'verify_snapshot_directory {flag} exited '
                         f'{result.returncode}; stderr: {result.stderr}')
        return result.stdout

    def test_long_help_flag_invokes_binary_help(self):
        out = self._help('--help')
        self.assertIn(SWIFT_WRAPPER, out)
        # BVLVerifier's --help describes the operation flags.
        self.assertIn('BVLVerifier', out)
        self.assertIn('--verify-all', out)

    def test_short_help_flag_invokes_binary_help(self):
        out = self._help('-h')
        self.assertIn('BVLVerifier', out)
        self.assertIn('--verify-all', out)


class TestPythonWrapperForwarding(unittest.TestCase):
    """Spot-check that non-help arguments reach the underlying script."""

    def test_seal_snapshot_directory_forwards_dry_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, 'visits.db')
            Path(db).touch()
            dest = os.path.join(tmp, 'icloud')
            date_dir = os.path.join(dest, '2024-01-15')
            os.makedirs(date_dir)
            result = subprocess.run(
                [str(REPO_ROOT / 'seal_snapshot_directory'),
                 '--db', db, '--dest', dest, '--dry-run', '2024-01-15'],
                capture_output=True, text=True, timeout=10,
            )
            self.assertEqual(result.returncode, 0, f'stderr: {result.stderr}')
            self.assertFalse(
                (Path(date_dir) / 'MANIFEST.tsv').exists(),
                'dry-run should not write a manifest')

    def test_reset_visits_data_forwards_log_flag_with_force(self):
        with tempfile.TemporaryDirectory() as tmp:
            env = os.environ.copy()
            env['BVL_LOG_DIR']   = tmp
            env['BVL_HOST_LOG']  = os.path.join(tmp, 'host.log')
            env['BVL_MOVER_LOG'] = os.path.join(tmp, 'mover.log')
            env['BVL_DB_FILE']   = os.path.join(tmp, 'visits.db')
            env['BVL_DOWNLOADS_SNAPSHOTS_DIR'] = os.path.join(tmp, 'dl')
            Path(tmp, 'browser-visits-2026-01-15.log').touch()
            result = subprocess.run(
                [str(REPO_ROOT / 'reset_visits_data'), '--log', '-f'],
                capture_output=True, text=True, timeout=10, env=env,
            )
            self.assertEqual(result.returncode, 0, f'stderr: {result.stderr}')
            self.assertFalse(Path(tmp, 'browser-visits-2026-01-15.log').exists())

    def test_rebuild_visits_data_forwards_overrides_against_empty_log_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = os.path.join(tmp, 'logs');   os.makedirs(log_dir)
            db      = os.path.join(tmp, 'visits.db')
            src     = os.path.join(tmp, 'dl');     os.makedirs(src)
            dest    = os.path.join(tmp, 'icloud'); os.makedirs(dest)
            result = subprocess.run(
                [str(REPO_ROOT / 'rebuild_visits_data'),
                 '--log-dir', log_dir, '--db', db,
                 '--source', src, '--dest', dest],
                capture_output=True, text=True, timeout=10,
            )
            self.assertEqual(result.returncode, 0, f'stderr: {result.stderr}')
            self.assertIn('replay:',    result.stdout)
            self.assertIn('rehydrate:', result.stdout)


class TestSwiftWrapperForwarding(unittest.TestCase):
    """The verify wrapper invokes BVLVerifier; flags reach the binary."""

    def test_show_errors_against_temp_db(self):
        # --show-errors creates the DB on the fly via Schema.ensureMoverErrorsTable
        # and prints "No pending mover errors." on an empty table.
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, 'visits.db')
            result = subprocess.run(
                [str(REPO_ROOT / SWIFT_WRAPPER), '--db', db, '--show-errors'],
                capture_output=True, text=True, timeout=10,
            )
        self.assertEqual(result.returncode, 0, f'stderr: {result.stderr}')
        self.assertIn('No pending mover errors', result.stdout)

    def test_verify_all_against_empty_db_quiet(self):
        # --verify-all --quiet on a fresh DB with no sealed rows exits
        # 0 with empty stdout.
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, 'visits.db')
            dest = os.path.join(tmp, 'icloud')
            os.makedirs(dest)
            result = subprocess.run(
                [str(REPO_ROOT / SWIFT_WRAPPER),
                 '--db', db, '--dest', dest,
                 '--verify-all', '--quiet'],
                capture_output=True, text=True, timeout=10,
            )
        self.assertEqual(result.returncode, 0, f'stderr: {result.stderr}')
        self.assertEqual(result.stdout, '')


class TestSwiftWrapperGuard(unittest.TestCase):
    """If the Swift binary hasn't been built yet, the wrapper should
    print a helpful error and exit 1 rather than crashing with bash's
    'no such file' message."""

    def test_missing_binary_prints_install_hint(self):
        # Run the wrapper from a copy of the repo where swift/.build/
        # doesn't exist.  We do this by symlinking the wrapper script
        # into a temp dir whose layout has no swift/.build/.
        wrapper_src = REPO_ROOT / SWIFT_WRAPPER
        with tempfile.TemporaryDirectory() as tmp:
            shadow = Path(tmp, SWIFT_WRAPPER)
            shadow.write_text(wrapper_src.read_text(encoding='utf-8'),
                              encoding='utf-8')
            shadow.chmod(0o755)
            result = subprocess.run(
                [str(shadow), '--show-errors'],
                capture_output=True, text=True, timeout=10,
            )
        self.assertEqual(result.returncode, 1)
        self.assertIn('Swift binary not built', result.stderr)
        self.assertIn('install.sh', result.stderr)


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
