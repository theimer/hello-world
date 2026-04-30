#!/usr/bin/env python3
"""
snapshot_mover.py — Periodically archive snapshot files from the local
Downloads folder to the iCloud-synced Documents folder, updating the
SQLite database to point at the new location, and sealing each daily
archive directory once its UTC date has fully passed.

Designed to be run by a launchd LaunchAgent every N seconds (default 1 h);
each invocation does one pass and exits.

Algorithm
---------
1. mkdir -p ICLOUD_SNAPSHOTS_DIR.
2. Open the SQLite database.
3. Ensure the `snapshots` table exists (one row per daily directory the
   mover has ever created; stores its sealed flag).
4. Move pass — scan the Downloads directory for snapshot files:
     For each file whose name matches the snapshot format
     '<YYYY-MM-DDTHH-MM-SSZ>-<hash>.<ext>' and whose mtime is at least
     MIN_AGE_SECONDS old:
       a. Derive the UTC date from the filename prefix.
       b. mkdir -p ICLOUD_SNAPSHOTS_DIR/<YYYY-MM-DD>/
       c. shutil.copy2(source, dest)          — preserves mtime; safe to repeat
       d. os.chmod(dest, 0o444)               — make archived copy read-only
       e. UPDATE {read,skimmed}_events SET directory = <date_subdir>
          WHERE filename = <this file> AND directory = DOWNLOADS_SNAPSHOTS_DIR
       f. INSERT OR IGNORE INTO snapshots (date, sealed) VALUES (<date>, 0)
       g. commit()
       h. source.unlink()
       i. Straggler handling — if the snapshots row for <date> was already
          sealed=1 before step (f), the file is a straggler arriving after
          the day was sealed.  Remove the existing read-only manifest and
          rewrite it so it now includes the just-moved file.  The sealed
          flag stays 1.
5. Seal pass — DB-driven (no filesystem rescan):
     SELECT date FROM snapshots WHERE sealed = 0 AND date < today_utc.
     For each row:
       a. Verify ICLOUD_SNAPSHOTS_DIR/<date>/ exists; warn and skip if not.
       b. If MANIFEST.tsv already exists (recovery from a partial prior
          seal that crashed between the file write and the DB update),
          leave the file alone.
          Otherwise: list every file in the directory, look up its row in
          read_events / skimmed_events (joined with visits for the page
          title), write a tab-delimited MANIFEST.tsv (header + one data
          row per file: filename, tag, timestamp, url, title), and chmod
          it 0o444.  Files with no DB row appear with empty metadata.
       c. UPDATE snapshots SET sealed = 1 WHERE date = <date>.
6. Close the DB.

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
import datetime
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

# Name of the per-directory manifest file written by the seal pass.
# Its presence on disk marks the directory sealed; combined with the
# `snapshots` table it provides redundancy that lets a partial-seal
# crash recover on the next run.
MANIFEST_FILENAME = 'MANIFEST.tsv'

# Header columns of the manifest (must stay in sync with _build_manifest_rows).
_MANIFEST_HEADER = ('filename', 'tag', 'timestamp', 'url', 'title')

# Matches the daily snapshot subdir name (UTC date, ISO format).
_DATE_DIR_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')


# ---------------------------------------------------------------------------
# Schema for the `snapshots` table — owned by the mover/sealer, not host.py.
#
# One row per daily snapshot directory the mover has ever created.  The
# move pass inserts a row (sealed=0) whenever a file lands in a new daily
# dir; the seal pass (and the manual sealer) flip sealed=1 once the
# directory's MANIFEST.tsv has been written.  The seal pass uses this
# table to avoid rescanning the iCloud filesystem on every sweep.
# ---------------------------------------------------------------------------

def _ensure_snapshots_table(conn: sqlite3.Connection) -> None:
    """Create the snapshots table if it doesn't already exist.

    Called by main() and by snapshot_sealer.cli() so the table is present
    even on the very first invocation against an existing DB.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            date   TEXT PRIMARY KEY,         -- 'YYYY-MM-DD' (UTC)
            sealed INTEGER NOT NULL DEFAULT 0
        )
    """)
    conn.commit()

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
        # (e) Detect a "straggler": a file whose date maps to a directory
        #     whose snapshots row already says sealed=1.  We need to know
        #     this *before* the INSERT OR IGNORE below, since that statement
        #     no-ops on an existing row and so can't tell us the prior state.
        prev = conn.execute(
            "SELECT sealed FROM snapshots WHERE date = ?", (date_str,),
        ).fetchone()
        is_straggler = prev is not None and prev[0] == 1
        # (f) Track the (possibly new) daily directory in the snapshots table.
        #     INSERT OR IGNORE preserves any existing row — including a row
        #     that's already sealed=1, so a straggler doesn't reopen a sealed
        #     day; the manifest is rewritten in step (i) instead.
        conn.execute(
            "INSERT OR IGNORE INTO snapshots (date, sealed) VALUES (?, 0)",
            (date_str,),
        )
        conn.commit()
        # (g) Remove the original.
        os.unlink(source)
        logger.info('Moved %s -> %s (read-only)', source, dest)
        # (h) If this was a straggler, rebuild the manifest so it includes
        #     the just-moved file.  Sealed flag stays 1.
        if is_straggler:
            _rewrite_manifest_for_straggler(conn, date_subdir)
    except (OSError, sqlite3.Error) as exc:
        logger.error('Failed to move %s: %s', source, exc)


# ---------------------------------------------------------------------------
# Seal pass — write a read-only MANIFEST.tsv into each finished daily dir
# ---------------------------------------------------------------------------

def _today_utc() -> datetime.date:
    """Return today's UTC date.  Indirected so tests can patch it."""
    return datetime.datetime.now(datetime.timezone.utc).date()


def _tsv_sanitise(s: str) -> str:
    """Strip tab/newline/CR characters so the field is safe for TSV output."""
    return (s or '').replace('\t', ' ').replace('\n', ' ').replace('\r', '')


def _seal_pass(conn: sqlite3.Connection, dry_run: bool = False) -> None:
    """Seal every snapshots-table row whose date is strictly before today (UTC)
    and whose `sealed` flag is still 0.

    Driven entirely by the database — no filesystem rescan.  The mover's
    move pass keeps the table in sync with the on-disk daily directories
    by inserting a row each time it creates one.
    """
    today = _today_utc()
    rows = conn.execute(
        "SELECT date FROM snapshots WHERE sealed = 0 AND date < ? ORDER BY date",
        (today.isoformat(),),
    ).fetchall()
    for (date_str,) in rows:
        date_subdir = os.path.join(ICLOUD_SNAPSHOTS_DIR, date_str)
        if not os.path.isdir(date_subdir):
            # The DB says this day exists but the directory is gone (e.g. user
            # manually deleted it).  Skip with a warning rather than failing.
            logger.warning(
                'snapshots row %s has no on-disk directory at %s; skipping',
                date_str, date_subdir,
            )
            continue
        _seal_directory(conn, date_subdir, dry_run=dry_run, date_key=date_str)


def _seal_directory(
    conn: sqlite3.Connection,
    date_subdir: str,
    dry_run: bool = False,
    date_key: 'str | None' = None,
) -> None:
    """Write a read-only MANIFEST.tsv listing every snapshot file in the dir,
    then mark the directory's snapshots-table row as sealed.

    date_key, when given, is the YYYY-MM-DD primary key of the snapshots row
    to flip to sealed=1.  The auto seal pass always passes it; the manual
    sealer passes it only when the directory's basename is itself a valid
    date (so non-date directories get a manifest but no DB row).

    Crash-safety: if the manifest already exists but the DB row is still
    sealed=0 (a prior run that crashed between the file write and the DB
    update), don't rewrite the manifest — the file is already correct and
    is now read-only.  Just flip the flag.

    Errors writing/chmod'ing the manifest, or updating the DB, are logged
    but do not propagate.
    """
    manifest_path = os.path.join(date_subdir, MANIFEST_FILENAME)

    if dry_run:
        logger.info('[dry-run] would seal %s', date_subdir)
        return

    try:
        if os.path.exists(manifest_path):
            # Recovery branch: a prior run wrote the manifest but didn't update
            # the DB.  The manifest is read-only and shouldn't be rewritten —
            # treat its presence as authoritative and just sync the DB.
            logger.info('Manifest already exists at %s; marking sealed',
                        date_subdir)
        else:
            count = _write_manifest_file(conn, date_subdir)
            logger.info('Sealed %s (%d entries)', date_subdir, count)
        if date_key is not None:
            # Upsert: the auto seal pass always finds an existing row (the
            # mover inserted it).  The manual sealer may not — e.g. if the
            # user is sealing a directory imported from elsewhere — so we
            # tolerate the no-row case by inserting first.
            conn.execute(
                "INSERT INTO snapshots (date, sealed) VALUES (?, 1) "
                "ON CONFLICT(date) DO UPDATE SET sealed = 1",
                (date_key,),
            )
            conn.commit()
    except (OSError, sqlite3.Error) as exc:
        logger.error('Failed to seal %s: %s', date_subdir, exc)


def _write_manifest_file(
    conn: sqlite3.Connection, date_subdir: str,
) -> int:
    """Build, write, and chmod 0o444 the manifest in date_subdir.

    Returns the number of data rows written (excluding the header).  Removes
    any existing manifest first so we can write over a previously read-only
    file (`open(..., 'w')` can't truncate a 0o444 file).  Caller is
    responsible for catching OSError / sqlite3.Error.
    """
    manifest_path = os.path.join(date_subdir, MANIFEST_FILENAME)
    if os.path.exists(manifest_path):
        os.unlink(manifest_path)
    rows = _build_manifest_rows(conn, date_subdir)
    with open(manifest_path, 'w', encoding='utf-8') as f:
        f.write('\t'.join(_MANIFEST_HEADER) + '\n')
        for row in rows:
            f.write('\t'.join(row) + '\n')
    os.chmod(manifest_path, 0o444)
    return len(rows)


def _rewrite_manifest_for_straggler(
    conn: sqlite3.Connection, date_subdir: str,
) -> None:
    """Rewrite the manifest after a straggler file was moved into a directory
    whose snapshots row was already sealed=1.

    Called by _move_one after a successful move + DB commit.  The directory's
    sealed flag stays 1 — the dir is still sealed, just with a refreshed
    manifest that includes the new file.

    Errors are logged but never propagate: the file move and DB update have
    already committed; a stale manifest is recoverable manually with
    snapshot_sealer.py.
    """
    manifest_path = os.path.join(date_subdir, MANIFEST_FILENAME)
    try:
        count = _write_manifest_file(conn, date_subdir)
        logger.info(
            'Rewrote %s after straggler arrival (%d entries)',
            manifest_path, count,
        )
    except (OSError, sqlite3.Error) as exc:
        logger.error(
            'Failed to rewrite %s after straggler arrival: %s',
            manifest_path, exc,
        )


def _build_manifest_rows(
    conn: sqlite3.Connection, date_subdir: str,
):
    """Return one tuple per snapshot file in the directory, sorted by filename.

    The standard datetime-prefixed filenames sort chronologically, so the
    manifest naturally lists events in the order they happened.  Files with
    no matching DB row appear with empty metadata fields.
    """
    files = sorted(
        f for f in os.listdir(date_subdir)
        if f != MANIFEST_FILENAME
        and os.path.isfile(os.path.join(date_subdir, f))
    )
    rows = []
    for filename in files:
        info = _lookup_event(conn, filename, date_subdir)
        if info is None:
            rows.append((_tsv_sanitise(filename), '', '', '', ''))
        else:
            rows.append((
                _tsv_sanitise(filename),
                info['tag'],
                _tsv_sanitise(info['timestamp']),
                _tsv_sanitise(info['url']),
                _tsv_sanitise(info['title']),
            ))
    return rows


def _lookup_event(
    conn: sqlite3.Connection, filename: str, directory: str,
):
    """Find the read_events / skimmed_events row for filename in directory.

    Returns a dict with tag, url, timestamp, title (joined from visits for the
    page title), or None if no matching row is found in either table.

    table names are trusted internal constants; safe to interpolate into SQL.
    """
    for table, tag in (('read_events', 'read'), ('skimmed_events', 'skimmed')):
        row = conn.execute(
            f"SELECT e.url, e.timestamp, COALESCE(v.title, '')"
            f"  FROM {table} e LEFT JOIN visits v ON v.url = e.url"
            f" WHERE e.filename = ? AND e.directory = ?",
            (filename, directory),
        ).fetchone()
        if row is not None:
            return {'tag': tag, 'url': row[0],
                    'timestamp': row[1], 'title': row[2]}
    return None


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(dry_run: bool = False) -> None:
    """Run a single mover pass against the current module-level constants."""
    os.makedirs(ICLOUD_SNAPSHOTS_DIR, exist_ok=True)

    if not os.path.exists(DB_FILE):
        logger.info('No DB at %s; nothing to do', DB_FILE)
        return

    conn = sqlite3.connect(DB_FILE)
    try:
        _ensure_snapshots_table(conn)
        _move_pass(conn, dry_run=dry_run)
        _seal_pass(conn, dry_run=dry_run)
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
