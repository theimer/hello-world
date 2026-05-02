#!/usr/bin/env python3
"""
snapshot_verifier.py — Verify that a sealed daily snapshot directory has
a correct MANIFEST.tsv.

Designed to be invoked from either:

  - A terminal, for ad-hoc checking after manual interventions or as
    part of a maintenance ritual.
  - A background process (a periodic launchd LaunchAgent or cron job),
    in which case `--record` integrates findings into the same
    `mover_errors` notification pipeline that snapshot_mover.py uses.

Checks performed against each target directory:

  1. MANIFEST.tsv exists.
  2. The manifest is read-only (mode 0o444).
  3. The first line is the canonical header
     ('filename', 'tag', 'timestamp', 'url', 'title') joined by tabs.
  4. Every data row has exactly 5 tab-delimited columns; no duplicate
     filenames within the manifest.
  5. Every file in the directory (other than MANIFEST.tsv itself) has
     a conforming snapshot filename.  Non-conforming files are flagged
     whether or not they appear in the manifest.
  6. The set of conforming snapshot files in the directory equals the
     set of filenames listed in the manifest.
  7. For each manifest row whose file is present in the directory, the
     row has a corresponding events row in the DB and the
     (tag, timestamp, url, title) fields match.  Orphan rows
     (manifest entries with no DB backing) are always invalid.
  8. Every conforming file in the directory has a corresponding events
     row in the DB.  Orphan files (conforming files with no DB row)
     are always invalid, even if absent from the manifest.

Usage
-----
    # Verify by date (resolved under ICLOUD_SNAPSHOTS_DIR)
    python3 native-host/snapshot_verifier.py 2026-04-30

    # Verify by absolute or relative path
    python3 native-host/snapshot_verifier.py /Users/.../snapshots/2026-04-30

    # Verify every sealed directory tracked in the snapshots table
    python3 native-host/snapshot_verifier.py --all

    # Background mode: silent on success, record findings to mover_errors
    python3 native-host/snapshot_verifier.py --quiet --record --all

Exit codes: 0 if every directory verified passes; 1 if any directory
fails verification, if the target doesn't exist, or on argument errors.
"""

import argparse
import logging
import os
import sqlite3
import sys

import snapshot_mover


# ---------------------------------------------------------------------------
# Core verification — also importable for in-process use
# ---------------------------------------------------------------------------

def verify_directory(conn, date_subdir):
    """Run all checks against MANIFEST.tsv in date_subdir.

    Returns (is_valid: bool, issues: list[str]) — `issues` is a list of
    human-readable problem descriptions, empty when valid.
    """
    issues = []
    manifest_path = os.path.join(
        date_subdir, snapshot_mover.MANIFEST_FILENAME)

    # 1. Existence
    if not os.path.exists(manifest_path):
        issues.append(f'Manifest file not found at {manifest_path}')
        return False, issues

    # 2. Read-only
    mode = os.stat(manifest_path).st_mode & 0o777
    if mode != 0o444:
        issues.append(f'Manifest is not read-only (mode {mode:#o})')

    # 3. Read content
    try:
        with open(manifest_path, 'r', encoding='utf-8') as f:
            lines = f.read().splitlines()
    except OSError as exc:
        issues.append(f'Could not read manifest: {exc}')
        return False, issues

    # 4. Header
    expected_header = '\t'.join(snapshot_mover._MANIFEST_HEADER)
    if not lines:
        issues.append('Manifest file is empty')
        return False, issues
    if lines[0] != expected_header:
        issues.append(
            f'Header mismatch: expected {expected_header!r}, '
            f'got {lines[0]!r}')
        # Header wrong → the data-row layout is unreliable.  Stop here.
        return False, issues

    # 5. Parse data rows
    manifest_entries = {}   # filename → (tag, timestamp, url, title)
    for i, line in enumerate(lines[1:], start=2):
        fields = line.split('\t')
        if len(fields) != 5:
            issues.append(f'Row {i}: expected 5 columns, got {len(fields)}')
            continue
        filename, tag, timestamp, url, title = fields
        if filename in manifest_entries:
            issues.append(f'Row {i}: duplicate filename {filename!r}')
            continue
        manifest_entries[filename] = (tag, timestamp, url, title)

    # Determine the expected per-day log filename for this directory.
    # Only date-named directories have one — a manually-sealed
    # non-date directory does not.
    basename = os.path.basename(os.path.normpath(date_subdir))
    expected_log = (
        snapshot_mover._log_filename_for(basename)
        if snapshot_mover._DATE_DIR_RE.match(basename)
        else None
    )

    # 6. File-level check: every file in the directory (other than the
    #    manifest and the per-day log) must have a conforming snapshot
    #    filename.  Non-conforming files are flagged regardless of
    #    whether they appear in the manifest.
    conforming_on_disk = set()
    for f in os.listdir(date_subdir):
        full = os.path.join(date_subdir, f)
        if f == snapshot_mover.MANIFEST_FILENAME:
            continue
        if expected_log is not None and f == expected_log:
            continue
        if not os.path.isfile(full):
            continue
        if not snapshot_mover._SNAPSHOT_FILENAME_RE.match(f):
            issues.append(f'Non-conforming file in directory: {f}')
            if f in manifest_entries:
                issues.append(
                    f'Manifest also contains non-conforming filename {f!r}')
            continue
        conforming_on_disk.add(f)

    # 7. Set comparison: manifest filenames vs on-disk conforming files.
    manifest_filenames = set(manifest_entries.keys())
    in_manifest_only = manifest_filenames - conforming_on_disk
    in_dir_only = conforming_on_disk - manifest_filenames
    for f in sorted(in_manifest_only):
        # Skip if this name is non-conforming — already flagged above.
        if not snapshot_mover._SNAPSHOT_FILENAME_RE.match(f):
            continue
        issues.append(
            f'Manifest references {f} but no such file in directory')
    for f in sorted(in_dir_only):
        issues.append(
            f'File {f} is in directory but not listed in manifest')

    # 8. Per-row check: every manifest row must have a corresponding
    #    events row (orphan rows are not allowed), and the (tag,
    #    timestamp, url, title) fields must match.
    for filename in sorted(manifest_filenames & conforming_on_disk):
        manifest_meta = manifest_entries[filename]
        info = snapshot_mover._lookup_event(conn, filename, date_subdir)
        if info is None:
            issues.append(
                f'{filename}: manifest row has no corresponding events '
                f'row in DB')
            continue
        expected = (
            info['tag'],
            snapshot_mover._tsv_sanitise(info['timestamp']),
            snapshot_mover._tsv_sanitise(info['url']),
            snapshot_mover._tsv_sanitise(info['title']),
        )
        if manifest_meta != expected:
            issues.append(
                f'{filename}: metadata mismatch — '
                f'manifest={manifest_meta!r} DB={expected!r}')

    # 9. Orphan-file check: every conforming file in the directory must
    #    also have an events row (catches conforming files that aren't
    #    in the manifest at all — e.g. the mover excluded them as
    #    orphans, but their presence in the directory still violates
    #    the invariant).
    for filename in sorted(conforming_on_disk - manifest_filenames):
        info = snapshot_mover._lookup_event(conn, filename, date_subdir)
        if info is None:
            issues.append(
                f'{filename}: conforming file in directory has no '
                f'corresponding events row in DB')

    # 10. Per-day log file: must be present, a regular file, mode 0o444.
    #     The sealer always moves the day's log into the sealed dir as
    #     part of the seal flow, so absence here means an incomplete
    #     seal or post-seal tampering.  Non-date directories (manual
    #     seals of e.g. an imported archive) have no expected log.
    if expected_log is not None:
        log_path = os.path.join(date_subdir, expected_log)
        if not os.path.exists(log_path):
            issues.append(f'Per-day log file not found at {log_path}')
        elif not os.path.isfile(log_path):
            issues.append(f'Per-day log {log_path} is not a regular file')
        else:
            log_mode = os.stat(log_path).st_mode & 0o777
            if log_mode != 0o444:
                issues.append(
                    f'Per-day log is not read-only (mode {log_mode:#o})')

    return len(issues) == 0, issues


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args(argv=None):
    p = argparse.ArgumentParser(
        prog='snapshot_verifier.py',
        description='Verify that a sealed daily snapshot directory has '
                    'a correct MANIFEST.tsv.',
    )
    p.add_argument('directory', nargs='?',
                   help="date 'YYYY-MM-DD' (resolved under ICLOUD_SNAPSHOTS_DIR) "
                        "or an explicit path to the directory to verify")
    p.add_argument('--all', action='store_true',
                   help='verify every sealed directory in the snapshots table')
    p.add_argument('--quiet', action='store_true',
                   help='only print a summary line (and any issues) for '
                        'directories that fail verification — suitable for '
                        'background invocation')
    p.add_argument('--record', action='store_true',
                   help="UPSERT each failure into mover_errors as op "
                        "'manifest_invalid' (and clear the row on success), "
                        "so findings surface via the standard mover "
                        "notification pipeline")
    p.add_argument('-v', '--verbose', action='store_true',
                   help='enable DEBUG logging')
    p.add_argument('--dest', metavar='DIR',
                   help=f'override the iCloud snapshots root directory '
                        f'(default {snapshot_mover.ICLOUD_SNAPSHOTS_DIR})')
    p.add_argument('--db', metavar='FILE',
                   help=f'override the SQLite database path '
                        f'(default {snapshot_mover.DB_FILE})')
    args = p.parse_args(argv)
    if args.all and args.directory is not None:
        p.error('cannot combine `directory` with --all')
    if not args.all and args.directory is None:
        p.error('one of `directory` or --all is required')
    return args


def _resolve_target(directory_arg):
    """Resolve the CLI directory argument the same way snapshot_sealer does:
    bare `YYYY-MM-DD` joins under ICLOUD_SNAPSHOTS_DIR; anything that looks
    like a path (absolute, or contains a path separator) is used verbatim."""
    if os.path.isabs(directory_arg) or os.sep in directory_arg:
        return directory_arg
    return os.path.join(snapshot_mover.ICLOUD_SNAPSHOTS_DIR, directory_arg)


def cli(argv=None):
    args = _parse_args(argv)

    if args.verbose:
        snapshot_mover.logger.setLevel(logging.DEBUG)
    if args.dest is not None:
        snapshot_mover.ICLOUD_SNAPSHOTS_DIR = args.dest
    if args.db is not None:
        snapshot_mover.DB_FILE = args.db

    if not os.path.exists(snapshot_mover.DB_FILE):
        print(f'No DB at {snapshot_mover.DB_FILE}', file=sys.stderr)
        return 1

    conn = sqlite3.connect(snapshot_mover.DB_FILE)
    try:
        # Ensure the mover_errors table exists in case --record is given
        # before the mover has ever run.
        snapshot_mover._ensure_mover_errors_table(conn)
        if args.all:
            snapshot_mover._ensure_snapshots_table(conn)
            rc = _verify_all(conn, args)
        else:
            rc = _verify_one(conn, args)
        # When --record was used, drain the notification queue so any
        # findings (and any other unread mover_errors rows that have
        # crossed the escalation threshold) reach the user without
        # needing the next snapshot_mover.py tick.
        if args.record:
            snapshot_mover._escalate_errors(conn)
        return rc
    finally:
        conn.close()


def _verify_one(conn, args):
    target = _resolve_target(args.directory)
    if not os.path.isdir(target):
        print(f'No such directory: {target}', file=sys.stderr)
        return 1
    is_valid, issues = verify_directory(conn, target)
    if args.record:
        _update_error_state(conn, target, is_valid, issues)
    _print_result(target, is_valid, issues, args.quiet)
    return 0 if is_valid else 1


def _verify_all(conn, args):
    rows = conn.execute(
        "SELECT date FROM snapshots WHERE sealed = 1 ORDER BY date"
    ).fetchall()
    if not rows:
        if not args.quiet:
            print('No sealed directories to verify.')
        return 0
    any_failed = False
    for (date_str,) in rows:
        target = os.path.join(snapshot_mover.ICLOUD_SNAPSHOTS_DIR, date_str)
        if not os.path.isdir(target):
            # Already covered by the seal pass's missing_directory error.
            # Skip it here so we don't double-report.
            continue
        is_valid, issues = verify_directory(conn, target)
        if args.record:
            _update_error_state(conn, target, is_valid, issues)
        _print_result(target, is_valid, issues, args.quiet)
        if not is_valid:
            any_failed = True
    return 1 if any_failed else 0


def _update_error_state(conn, target, is_valid, issues):
    """Sync the mover_errors table to the verification outcome."""
    if is_valid:
        snapshot_mover._try_clear_error(conn, 'manifest_invalid', target)
    else:
        message = '; '.join(issues)
        snapshot_mover._try_record_error(
            conn, 'manifest_invalid', target, ValueError(message))


def _print_result(target, is_valid, issues, quiet):
    if is_valid:
        if not quiet:
            print(f'{target}: OK')
        return
    n = len(issues)
    print(f'{target}: FAILED ({n} issue{"" if n == 1 else "s"})')
    for issue in issues:
        print(f'  - {issue}')


if __name__ == '__main__':  # pragma: no cover
    sys.exit(cli())
