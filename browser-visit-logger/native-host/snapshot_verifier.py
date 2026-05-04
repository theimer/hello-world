#!/usr/bin/env python3
"""
snapshot_verifier.py — Sole background agent for the Browser Visit Logger.

Run periodically by a launchd LaunchAgent (default 86400 s = daily, see
the matching plist template).  Each tick performs three groups of work,
in order:

  1. Sweep — scan ~/Downloads/browser-visit-snapshots/ for stragglers
     (files left behind by a host.py crash between Chrome's download
     and host.py's synchronous archive); copy each to its iCloud date
     subdir, chmod read-only, update the matching events row, unlink
     the source.  MIN_AGE_SECONDS gate avoids racing in-flight
     downloads.

  2. Bookkeeping — seal completed UTC days (write MANIFEST.tsv, move
     the per-day log into the sealed dir, flip sealed=1); reconcile
     orphan per-day logs in LOG_DIR with their iCloud counterparts;
     escalate persistent / catastrophic mover_errors rows via
     Notification Center.

  3. Verification — for every sealed daily snapshot directory tracked
     in the snapshots table, verify MANIFEST.tsv passes all checks
     (existence, mode, header, row format, file/manifest set match,
     per-row events-row backing, per-day log presence/mode).  Failures
     are recorded as op 'manifest_invalid' in mover_errors and surface
     via the same notification pipeline as everything else.

Designed to run inside a code-signed app bundle
(BrowserVisitLoggerVerifier.app) so it has its own stable TCC identity:
the user grants the app Files & Folders → Downloads + ~/Documents
access once, and every subsequent tick can read both.

Verification checks (per directory)
-----------------------------------
  1. MANIFEST.tsv exists.
  2. The manifest is read-only (mode 0o444).
  3. The first line is the canonical header
     ('filename', 'tag', 'timestamp', 'url', 'title') joined by tabs.
  4. Every data row has exactly 5 tab-delimited columns; no duplicate
     filenames within the manifest.
  5. Every file in the directory (other than MANIFEST.tsv and the
     per-day log) has a conforming snapshot filename.  Non-conforming
     files are flagged whether or not they appear in the manifest.
  6. The set of conforming snapshot files in the directory equals the
     set of filenames listed in the manifest.
  7. For each manifest row whose file is present in the directory, the
     row has a corresponding events row in the DB and the
     (tag, timestamp, url, title) fields match.  Orphan rows
     (manifest entries with no DB backing) are always invalid.
  8. Every conforming file in the directory has a corresponding events
     row in the DB.  Orphan files are always invalid, even if absent
     from the manifest.
  9. The per-day log file is present, a regular file, mode 0o444.

Usage
-----
    # Run a full tick (sweep + seal + orphan-merge + escalate + verify)
    python3 native-host/snapshot_verifier.py

    # Verify one directory by date or path (no sweep / seal / escalate)
    python3 native-host/snapshot_verifier.py --verify 2026-04-30
    python3 native-host/snapshot_verifier.py --verify /Users/.../snapshots/2026-04-30

    # Verify every sealed directory (no sweep / seal / escalate)
    python3 native-host/snapshot_verifier.py --verify-all

    # Background mode: silent on success, record findings to mover_errors
    python3 native-host/snapshot_verifier.py --quiet --record

    # Inspect / clear pending errors
    python3 native-host/snapshot_verifier.py --show-errors
    python3 native-host/snapshot_verifier.py --clear-errors
    python3 native-host/snapshot_verifier.py --clear-error N

    # Dry-run a tick (don't touch files or the DB; print what would happen)
    python3 native-host/snapshot_verifier.py --dry-run

Exit codes: 0 on success; 1 if any directory fails verification, if a
target doesn't exist, or on argument errors.
"""

import argparse
import logging
import os
import sqlite3
import sys

import snapshot_mover


# ---------------------------------------------------------------------------
# Tick — the main work the LaunchAgent performs
# ---------------------------------------------------------------------------

def run_tick(conn, dry_run=False, record=True, quiet=True):
    """Run one full housekeeping tick.

    Order matters: sweep first (so the seal pass sees freshly-archived
    files), seal, orphan-log-merge, then verify.  Error escalation runs
    last so it picks up any errors recorded by earlier passes in the
    same tick.

    Returns True iff every sealed directory passed verification.
    """
    snapshot_mover.sweep_pass(conn, dry_run=dry_run)
    snapshot_mover.seal_pass(conn, dry_run=dry_run)
    if not dry_run:
        snapshot_mover.orphan_log_merge_pass(conn)
    all_ok = _verify_all_sealed(conn, record=record, quiet=quiet,
                                dry_run=dry_run)
    if not dry_run:
        snapshot_mover.escalate_errors(conn)
    return all_ok


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
    #    in the manifest at all — e.g. the seal pass excluded them as
    #    orphans, but their presence in the directory still violates
    #    the invariant).
    for filename in sorted(conforming_on_disk - manifest_filenames):
        info = snapshot_mover._lookup_event(conn, filename, date_subdir)
        if info is None:
            issues.append(
                f'{filename}: conforming file in directory has no '
                f'corresponding events row in DB')

    # 10. Per-day log file: must be present, a regular file, mode 0o444.
    #     The seal pass always moves the day's log into the sealed dir
    #     as part of the seal flow, so absence here means an incomplete
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
        description='Browser Visit Logger background agent: sweep, seal, '
                    'verify sealed daily snapshot directories, and '
                    'escalate persistent errors.',
    )
    # Mutually-exclusive operation modes.  Default (none specified) is a
    # full housekeeping tick; the others narrow it.
    g = p.add_mutually_exclusive_group()
    g.add_argument('--verify', metavar='DIR_OR_DATE',
                   help="verify one directory by date "
                        "'YYYY-MM-DD' (resolved under ICLOUD_SNAPSHOTS_DIR) "
                        "or path; skip sweep / seal / orphan-merge / escalate")
    g.add_argument('--verify-all', action='store_true',
                   help='verify every sealed directory tracked in the '
                        'snapshots table; skip sweep / seal / orphan-merge / '
                        'escalate')
    g.add_argument('--show-errors', action='store_true',
                   help='print pending mover_errors rows and exit')
    g.add_argument('--clear-errors', action='store_true',
                   help='delete every row from mover_errors and exit')
    g.add_argument('--clear-error', type=int, metavar='N',
                   help='delete the Nth row (1-indexed, matching '
                        '--show-errors order) from mover_errors and exit')

    p.add_argument('--dry-run', action='store_true',
                   help='report what a tick would do without touching '
                        'files or the DB (seal/sweep only)')
    p.add_argument('--quiet', action='store_true',
                   help='only print issues for directories that fail '
                        'verification — suitable for background invocation')
    p.add_argument('--record', action='store_true',
                   help="UPSERT each verification failure into mover_errors "
                        "as op 'manifest_invalid' (and clear the row on "
                        "success).  Default tick mode always records; this "
                        "flag enables it for the --verify / --verify-all modes")
    p.add_argument('-v', '--verbose', action='store_true',
                   help='enable DEBUG logging')
    p.add_argument('--dest', metavar='DIR',
                   help=f'override the iCloud snapshots root directory '
                        f'(default {snapshot_mover.ICLOUD_SNAPSHOTS_DIR})')
    p.add_argument('--source', metavar='DIR',
                   help=f'override the Downloads snapshots source dir '
                        f'(default {snapshot_mover.DOWNLOADS_SNAPSHOTS_DIR})')
    p.add_argument('--db', metavar='FILE',
                   help=f'override the SQLite database path '
                        f'(default {snapshot_mover.DB_FILE})')
    p.add_argument('--min-age-seconds', type=int, metavar='N',
                   help=f'override the sweep file-age threshold '
                        f'(default {snapshot_mover.MIN_AGE_SECONDS}s)')
    return p.parse_args(argv)


def _resolve_target(directory_arg):
    """Resolve a CLI directory argument: bare 'YYYY-MM-DD' joins under
    ICLOUD_SNAPSHOTS_DIR; anything that looks like a path (absolute, or
    contains a path separator) is used verbatim."""
    if os.path.isabs(directory_arg) or os.sep in directory_arg:
        return directory_arg
    return os.path.join(snapshot_mover.ICLOUD_SNAPSHOTS_DIR, directory_arg)


def _apply_overrides(args):
    if args.verbose:
        snapshot_mover.logger.setLevel(logging.DEBUG)
    if args.dest is not None:
        snapshot_mover.ICLOUD_SNAPSHOTS_DIR = args.dest
    if args.source is not None:
        snapshot_mover.DOWNLOADS_SNAPSHOTS_DIR = args.source
    if args.db is not None:
        snapshot_mover.DB_FILE = args.db
    if args.min_age_seconds is not None:
        snapshot_mover.MIN_AGE_SECONDS = args.min_age_seconds


def cli(argv=None):
    args = _parse_args(argv)
    # Configure root logging on entry so snapshot_mover's library logger
    # (which has no handlers of its own) reaches stderr.  The verifier
    # LaunchAgent's plist captures stderr to ~/browser-visits-verifier.log;
    # interactive runs see the lines directly in the terminal.  This is a
    # no-op when the root logger already has handlers (e.g. under pytest).
    logging.basicConfig(
        stream=sys.stderr,
        level=logging.INFO,
        format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
    )
    _apply_overrides(args)

    # Ensure the iCloud root exists before any operation that may write
    # there — matches the old mover's main() guarantee.
    try:
        os.makedirs(snapshot_mover.ICLOUD_SNAPSHOTS_DIR, exist_ok=True)
    except OSError as exc:
        print(f'Could not create iCloud snapshots dir '
              f'{snapshot_mover.ICLOUD_SNAPSHOTS_DIR}: {exc}',
              file=sys.stderr)
        return 1

    if args.show_errors:
        return _cli_show_errors()
    if args.clear_errors:
        return _cli_clear_errors()
    if args.clear_error is not None:
        return _cli_clear_error(args.clear_error)

    if not os.path.exists(snapshot_mover.DB_FILE):
        print(f'No DB at {snapshot_mover.DB_FILE}', file=sys.stderr)
        return 1

    conn = sqlite3.connect(snapshot_mover.DB_FILE)
    try:
        snapshot_mover._ensure_snapshots_table(conn)
        snapshot_mover._ensure_mover_errors_table(conn)
        if args.verify is not None:
            return _verify_one(conn, args)
        if args.verify_all:
            rc = _verify_all_sealed(
                conn, record=args.record, quiet=args.quiet,
                dry_run=False)
            return 0 if rc else 1
        # Default: full tick.
        all_ok = run_tick(
            conn, dry_run=args.dry_run, record=True, quiet=args.quiet)
        return 0 if all_ok else 1
    except Exception as exc:
        # Top-level: best-effort record + escalate, then re-raise so
        # launchd captures the traceback.
        try:
            snapshot_mover._record_error(conn, 'top_level', '', exc)
            snapshot_mover._escalate_errors(conn)
        except Exception as inner:                              # noqa: BLE001
            snapshot_mover.logger.error(
                'Could not record top-level failure to DB (%s); '
                'falling back to direct notification', inner)
            snapshot_mover._notify_user(
                'Browser Visit Logger: verifier crashed',
                f'Top-level failure: {exc}')
        raise
    finally:
        conn.close()


def _verify_one(conn, args):
    target = _resolve_target(args.verify)
    if not os.path.isdir(target):
        print(f'No such directory: {target}', file=sys.stderr)
        return 1
    is_valid, issues = verify_directory(conn, target)
    if args.record:
        _update_error_state(conn, target, is_valid, issues)
        snapshot_mover._escalate_errors(conn)
    _print_result(target, is_valid, issues, args.quiet)
    return 0 if is_valid else 1


def _verify_all_sealed(conn, record, quiet, dry_run):
    """Verify every sealed directory; return True iff all passed.

    Always records when called from run_tick.  --verify-all from the CLI
    only records when the user passes --record.
    """
    if dry_run:
        snapshot_mover.logger.info(
            '[dry-run] would verify every sealed snapshot directory')
        return True
    rows = conn.execute(
        "SELECT date FROM snapshots WHERE sealed = 1 ORDER BY date"
    ).fetchall()
    if not rows:
        if not quiet:
            print('No sealed directories to verify.')
        return True
    any_failed = False
    for (date_str,) in rows:
        target = os.path.join(snapshot_mover.ICLOUD_SNAPSHOTS_DIR, date_str)
        if not os.path.isdir(target):
            # Already covered by the seal pass's missing_directory error.
            # Skip to avoid double-reporting.
            continue
        is_valid, issues = verify_directory(conn, target)
        if record:
            _update_error_state(conn, target, is_valid, issues)
        _print_result(target, is_valid, issues, quiet)
        if not is_valid:
            any_failed = True
    return not any_failed


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


# ---------------------------------------------------------------------------
# Error-table CLI ops — adopted from the old snapshot_mover CLI.  These
# are the user-facing levers for inspecting and acknowledging persistent
# failures in the mover_errors table.
# ---------------------------------------------------------------------------

def _open_errors_conn():
    conn = sqlite3.connect(snapshot_mover.DB_FILE)
    snapshot_mover._ensure_mover_errors_table(conn)
    return conn


def _cli_show_errors():
    conn = _open_errors_conn()
    try:
        rows = snapshot_mover.fetch_pending_errors(conn)
    finally:
        conn.close()
    if not rows:
        print('No pending mover errors.')
        return 0
    print(f'Pending mover errors ({len(rows)}):')
    print()
    for i, (_key, op, target, message, attempts, first_seen,
            last_seen, notified) in enumerate(rows, start=1):
        target_repr = target or '(no target)'
        print(f'  [{i}] {op}: {target_repr}')
        print(f'      attempts: {attempts} '
              f'(since {first_seen}, last {last_seen})')
        print(f'      error:    {message}')
        hint = snapshot_mover._FIX_HINTS.get(op)
        if hint:
            print(f'      fix:      {hint}')
        print(f'      notified: {"yes" if notified else "no"}')
        print()
    return 0


def _cli_clear_errors():
    conn = _open_errors_conn()
    try:
        cursor = conn.execute("DELETE FROM mover_errors")
        conn.commit()
        n = cursor.rowcount
    finally:
        conn.close()
    print(f'Cleared {n} error row{"" if n == 1 else "s"}.')
    return 0


def _cli_clear_error(n):
    conn = _open_errors_conn()
    try:
        rows = snapshot_mover.fetch_pending_errors(conn)
        if not rows:
            print('No pending mover errors to clear.', file=sys.stderr)
            return 1
        if n < 1 or n > len(rows):
            print(f'No error at index {n} (table has {len(rows)} row'
                  f'{"" if len(rows) == 1 else "s"}).',
                  file=sys.stderr)
            return 1
        key, op, target = rows[n - 1][0], rows[n - 1][1], rows[n - 1][2]
        conn.execute("DELETE FROM mover_errors WHERE key = ?", (key,))
        conn.commit()
    finally:
        conn.close()
    print(f'Cleared error [{n}]: {op}: {target or "(no target)"}')
    return 0


if __name__ == '__main__':  # pragma: no cover
    sys.exit(cli())
