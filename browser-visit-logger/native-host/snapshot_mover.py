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
3. Orphan-source sweep — for each (filename) in {read,skimmed}_events
   whose directory is already ICLOUD_SNAPSHOTS_DIR but whose source file
   still exists in DOWNLOADS_SNAPSHOTS_DIR (i.e. a previous run crashed
   between the DB update and the source unlink): delete the orphan source.
4. Move pass — for each (rowid, filename, table) in {read,skimmed}_events
   whose directory is DOWNLOADS_SNAPSHOTS_DIR:
     a. Skip if the source file's mtime is less than MIN_AGE_SECONDS old
        (defends against snapshots still being written to disk).
     b. Special-case: if source is missing but dest already exists, just
        update the row's directory column (recovery from prior crash
        between the copy and the DB update).
     c. Special-case: if both source and dest are missing, log a warning
        and continue.
     d. Otherwise:
          (i)   shutil.copy2(source, dest)        — preserves mtime
          (ii)  UPDATE row.directory = ICLOUD     — commits the move
          (iii) source.unlink()                   — removes the original
        Each step is independently safe to repeat, making the workflow
        idempotent: a failure between any two steps is recovered on the
        next run.
5. Close the DB.

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

    # ignore the 10-minute age gate (move everything immediately)
    python3 native-host/snapshot_mover.py --min-age-seconds 0

    # operate on isolated paths (e.g. against a test DB)
    python3 native-host/snapshot_mover.py \\
        --db /tmp/test.db --source /tmp/src --dest /tmp/dst

CLI flags override env vars, which override defaults.
"""

import argparse
import logging
import os
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


def _orphan_source_sweep(conn: sqlite3.Connection, dry_run: bool = False) -> None:
    """Delete source files left behind when a prior run crashed between the
    UPDATE (step b) and the unlink (step c).

    Such files have a row in the events table with directory = ICLOUD but
    the source path still exists in DOWNLOADS.
    """
    for table in EVENTS_TABLES:
        rows = conn.execute(
            f"SELECT filename FROM {table} WHERE directory = ?",
            (ICLOUD_SNAPSHOTS_DIR,),
        ).fetchall()
        for (filename,) in rows:
            source = os.path.join(DOWNLOADS_SNAPSHOTS_DIR, filename)
            if os.path.exists(source):
                if dry_run:
                    logger.info('[dry-run] would remove orphan source %s', source)
                    continue
                try:
                    os.unlink(source)
                    logger.info('Removed orphan source %s', source)
                except OSError as exc:
                    logger.error('Failed to remove orphan source %s: %s', source, exc)


def _move_pass(conn: sqlite3.Connection, dry_run: bool = False) -> None:
    """Copy each old-enough snapshot from Downloads to iCloud, update DB,
    delete source.  Idempotent and crash-safe; see module docstring.
    """
    now = time.time()
    for table in EVENTS_TABLES:
        rows = conn.execute(
            f"SELECT rowid, filename FROM {table} WHERE directory = ?",
            (DOWNLOADS_SNAPSHOTS_DIR,),
        ).fetchall()
        for rowid, filename in rows:
            _move_one(conn, table, rowid, filename, now, dry_run=dry_run)


def _move_one(
    conn: sqlite3.Connection, table: str, rowid: int, filename: str, now: float,
    dry_run: bool = False,
) -> None:
    source = os.path.join(DOWNLOADS_SNAPSHOTS_DIR, filename)
    dest   = os.path.join(ICLOUD_SNAPSHOTS_DIR, filename)

    if not os.path.exists(source):
        if os.path.exists(dest):
            # Recovery: previous run copied the file but crashed before the
            # DB update.  Just update the row.
            if dry_run:
                logger.info('[dry-run] would reconcile DB for already-copied %s', filename)
            else:
                conn.execute(
                    f"UPDATE {table} SET directory = ? WHERE rowid = ?",
                    (ICLOUD_SNAPSHOTS_DIR, rowid),
                )
                conn.commit()
                logger.info('Reconciled DB for already-copied %s', filename)
        else:
            logger.warning(
                'Snapshot referenced by %s rowid %d is missing from both '
                'source (%s) and dest (%s); leaving DB row unchanged',
                table, rowid, source, dest,
            )
        return

    age = now - os.path.getmtime(source)
    if age < MIN_AGE_SECONDS:
        logger.debug('Skipping %s — only %.0fs old (< %ds)', source, age, MIN_AGE_SECONDS)
        return

    if dry_run:
        logger.info('[dry-run] would move %s -> %s (and update %s rowid %d)',
                    source, dest, table, rowid)
        return

    try:
        # (a) copy (preserves mtime via copy2 — overwrite is safe).
        shutil.copy2(source, dest)
        # (a2) make the archived copy read-only.
        os.chmod(dest, 0o444)
        # (b) commit the move in the DB.
        conn.execute(
            f"UPDATE {table} SET directory = ? WHERE rowid = ?",
            (ICLOUD_SNAPSHOTS_DIR, rowid),
        )
        conn.commit()
        # (c) remove the original.
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
        _orphan_source_sweep(conn, dry_run=dry_run)
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
