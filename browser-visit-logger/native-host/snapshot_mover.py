#!/usr/bin/env python3
"""
snapshot_mover.py — Periodically archive snapshot files from the local
Downloads folder to the iCloud-synced Documents folder, updating the
SQLite database to point at the new location.

Designed to be run by a launchd LaunchAgent every N seconds (default 1 h);
each invocation does one pass and exits.

Algorithm
---------
1. mkdir -p ICLOUD_SNAPSHOTS_DIR.
2. Open the SQLite database.
3. Move pass — scan the Downloads directory for snapshot files:
     For each file whose name matches the snapshot format
     '<YYYY-MM-DDTHH-MM-SSZ>-<hash>.<ext>' and whose mtime is at least
     MIN_AGE_SECONDS old:
       a. Derive the UTC date from the filename prefix.
       b. mkdir -p ICLOUD_SNAPSHOTS_DIR/<YYYY-MM-DD>/
       c. shutil.copy2(source, dest)          — preserves mtime; safe to repeat
       d. os.chmod(dest, 0o444)               — make archived copy read-only
       e. UPDATE {read,skimmed}_events SET directory = <date_subdir>
          WHERE filename = <this file> AND directory = DOWNLOADS_SNAPSHOTS_DIR
       f. commit()
       g. source.unlink()
4. Close the DB.

The filesystem scan (rather than a DB query) means that any file left in
Downloads by a failed prior run is automatically retried — no special
"orphan sweep" is required.  Crash-safety analysis:

  Crash after (c) only:  source still present, dest exists → next run copies
    again (copy2 overwrites safely), updates DB, unlinks.  ✓
  Crash after (e)/(f):   source still present, DB already points to iCloud
    → next run re-copies, UPDATE matches 0 rows (no-op), then unlinks.  ✓

Filename convention
-------------------
host.py renames each snapshot to its permanent datetime-prefixed name at
record time:
    <YYYY-MM-DDTHH-MM-SSZ>-<hash>.<ext>
    e.g.  2026-04-30T14-35-22Z-abc123.mhtml

The date portion (first 10 characters) determines the iCloud date subdir:
    ICLOUD_SNAPSHOTS_DIR/2026-04-30/2026-04-30T14-35-22Z-abc123.mhtml

Files in Downloads that do not match this format are silently skipped.

Configuration
-------------
DB_FILE                  — BVL_DB_FILE, default ~/browser-visits.db
DOWNLOADS_SNAPSHOTS_DIR  — BVL_DOWNLOADS_SNAPSHOTS_DIR, default ~/Downloads/browser-visit-snapshots
ICLOUD_SNAPSHOTS_DIR     — BVL_ICLOUD_SNAPSHOTS_DIR,    default ~/Documents/browser-visit-logger/snapshots
MIN_AGE_SECONDS          — BVL_MOVER_MIN_AGE_SECONDS,   default 60 (1 min)

Logging is to stderr; the launchd plist routes stderr to a log file.

Manual invocation (for testing)
-------------------------------
The script is run hourly by launchd in normal operation, but it can also
be invoked from a terminal — useful for ad-hoc testing or one-off archive
sweeps.

    # do one pass with the configured defaults
    python3 native-host/snapshot_mover.py

    # show what would be moved without touching anything
    python3 native-host/snapshot_mover.py --dry-run --verbose

    # ignore the 1-minute age gate (move everything immediately)
    python3 native-host/snapshot_mover.py --min-age-seconds 0

    # operate on isolated paths (e.g. against a test DB)
    python3 native-host/snapshot_mover.py \\
        --db /tmp/test.db --source /tmp/src --dest /tmp/dst

CLI flags override env vars, which override defaults.
"""

import argparse
import logging
import os
import re
import shutil
import sqlite3
import sys
import time

HOME = os.path.expanduser('~')
DB_FILE = os.environ.get('BVL_DB_FILE', os.path.join(HOME, 'browser-visits.db'))
DOWNLOADS_SNAPSHOTS_DIR = os.environ.get(
    'BVL_DOWNLOADS_SNAPSHOTS_DIR',
    os.path.join(HOME, 'Downloads', 'browser-visit-snapshots'),
)
ICLOUD_SNAPSHOTS_DIR = os.environ.get(
    'BVL_ICLOUD_SNAPSHOTS_DIR',
    os.path.join(HOME, 'Documents', 'browser-visit-logger', 'snapshots'),
)
MIN_AGE_SECONDS = int(os.environ.get('BVL_MOVER_MIN_AGE_SECONDS', '60'))

EVENTS_TABLES = ('read_events', 'skimmed_events')

logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [snapshot_mover] %(message)s',
)
logger = logging.getLogger('snapshot_mover')

# Matches the permanent snapshot filename format assigned by host.py:
#   <YYYY-MM-DD>T<HH>-<MM>-<SS>Z-<hash>.<ext>
# group(1) — UTC date string 'YYYY-MM-DD' (used to select the date subdir)
_SNAPSHOT_FILENAME_RE = re.compile(
    r'^(\d{4}-\d{2}-\d{2})T\d{2}-\d{2}-\d{2}Z-.+$'
)


def _move_pass(conn: sqlite3.Connection, dry_run: bool = False) -> None:
    """Scan the Downloads directory and move every old-enough snapshot to iCloud.

    Uses the filesystem as the source of truth, so any file left in Downloads
    by a failed prior run is automatically retried.
    """
    if not os.path.isdir(DOWNLOADS_SNAPSHOTS_DIR):
        return

    now = time.time()
    for filename in os.listdir(DOWNLOADS_SNAPSHOTS_DIR):
        source = os.path.join(DOWNLOADS_SNAPSHOTS_DIR, filename)
        if not os.path.isfile(source):
            continue

        m = _SNAPSHOT_FILENAME_RE.match(filename)
        if not m:
            logger.debug('Skipping %s — does not match snapshot filename format', filename)
            continue

        age = now - os.path.getmtime(source)
        if age < MIN_AGE_SECONDS:
            logger.debug('Skipping %s — only %.0fs old (< %ds)', filename, age, MIN_AGE_SECONDS)
            continue

        date_str = m.group(1)   # 'YYYY-MM-DD'
        _move_one(conn, source, filename, date_str, dry_run=dry_run)


def _move_one(
    conn: sqlite3.Connection, source: str, filename: str, date_str: str,
    dry_run: bool = False,
) -> None:
    date_subdir = os.path.join(ICLOUD_SNAPSHOTS_DIR, date_str)
    dest        = os.path.join(date_subdir, filename)

    if dry_run:
        logger.info('[dry-run] would move %s -> %s', source, dest)
        return

    try:
        # (a) Ensure the date subdir exists.
        os.makedirs(date_subdir, exist_ok=True)
        # (b) Copy, preserving mtime.  Overwriting an existing dest is safe.
        shutil.copy2(source, dest)
        # (c) Make the archived copy read-only.
        os.chmod(dest, 0o444)
        # (d) Update DB rows that still record this file as living in Downloads.
        #     Rows already pointing to iCloud (from a prior partial run) are
        #     untouched by the WHERE clause — that's correct.
        for table in EVENTS_TABLES:
            conn.execute(
                f"UPDATE {table} SET directory = ?"
                f" WHERE filename = ? AND directory = ?",
                (date_subdir, filename, DOWNLOADS_SNAPSHOTS_DIR),
            )
        conn.commit()
        # (e) Remove the original.
        os.unlink(source)
        logger.info('Moved %s -> %s (read-only)', source, dest)
    except (OSError, sqlite3.Error) as exc:
        logger.error('Failed to move %s: %s', source, exc)


def main(dry_run: bool = False) -> None:
    """Run a single mover pass against the current module-level constants."""
    os.makedirs(ICLOUD_SNAPSHOTS_DIR, exist_ok=True)

    if not os.path.exists(DB_FILE):
        logger.info('No DB at %s; nothing to do', DB_FILE)
        return

    conn = sqlite3.connect(DB_FILE)
    try:
        _move_pass(conn, dry_run=dry_run)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# CLI entry point — for manual / testing invocation
# ---------------------------------------------------------------------------

def _parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog='snapshot_mover.py',
        description='Move browser-snapshot files from the local Downloads dir '
                    'to the iCloud-synced Documents archive.',
    )
    p.add_argument('--dry-run', action='store_true',
                   help='report what would happen without touching files or the DB')
    p.add_argument('-v', '--verbose', action='store_true',
                   help='enable DEBUG logging')
    p.add_argument('--min-age-seconds', type=int, metavar='N',
                   help=f'override the file-age threshold '
                        f'(default {MIN_AGE_SECONDS}s)')
    p.add_argument('--source', metavar='DIR',
                   help=f'override the source (Downloads) directory '
                        f'(default {DOWNLOADS_SNAPSHOTS_DIR})')
    p.add_argument('--dest', metavar='DIR',
                   help=f'override the destination (iCloud) directory '
                        f'(default {ICLOUD_SNAPSHOTS_DIR})')
    p.add_argument('--db', metavar='FILE',
                   help=f'override the SQLite database path '
                        f'(default {DB_FILE})')
    return p.parse_args(argv)


def _apply_args(args: argparse.Namespace) -> None:
    """Apply parsed CLI args to module-level constants and the logger."""
    global DOWNLOADS_SNAPSHOTS_DIR, ICLOUD_SNAPSHOTS_DIR, DB_FILE, MIN_AGE_SECONDS
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    if args.source is not None:
        DOWNLOADS_SNAPSHOTS_DIR = args.source
    if args.dest is not None:
        ICLOUD_SNAPSHOTS_DIR = args.dest
    if args.db is not None:
        DB_FILE = args.db
    if args.min_age_seconds is not None:
        MIN_AGE_SECONDS = args.min_age_seconds


def cli(argv=None) -> None:
    """Parse argv, apply overrides, run one mover pass."""
    args = _parse_args(argv)
    _apply_args(args)
    main(dry_run=args.dry_run)


if __name__ == '__main__':  # pragma: no cover
    cli()
