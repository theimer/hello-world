#!/usr/bin/env python3
"""
snapshot_mover.py — Periodically archive snapshot files from the local
staging directory to the iCloud-synced Documents folder, updating the
SQLite database to point at the new location, and sealing each daily
archive directory once its UTC date has fully passed.

Designed to be run by a launchd LaunchAgent every N seconds (default 1 h);
each invocation does one pass and exits.

The staging dir is populated by host.py: Chrome saves each snapshot into
~/Downloads (its downloads API forces files there), then host.py — which
inherits Chrome's TCC grant for ~/Downloads — relocates the file into the
staging dir.  This mover never reads ~/Downloads itself; doing so would
fail under launchd because the launchd-spawned process has no TCC grant.

Algorithm
---------
1. mkdir -p ICLOUD_SNAPSHOTS_DIR.
2. Open the SQLite database.
3. Ensure the `snapshots` table exists (one row per daily directory the
   mover has ever created; stores its sealed flag).
4. Move pass — scan the staging directory for snapshot files:
     For each file whose name matches the snapshot format
     '<YYYY-MM-DDTHH-MM-SSZ>-<hash>.<ext>' and whose mtime is at least
     MIN_AGE_SECONDS old:
       a. Derive the UTC date from the filename prefix.
       b. mkdir -p ICLOUD_SNAPSHOTS_DIR/<YYYY-MM-DD>/
       c. shutil.copy2(source, dest)          — preserves mtime; safe to repeat
       d. os.chmod(dest, 0o444)               — make archived copy read-only
       e. UPDATE {read,skimmed}_events SET directory = <date_subdir>
          WHERE filename = <this file> AND directory = STAGING_SNAPSHOTS_DIR
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
the staging dir by a failed prior run is automatically retried — no special
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

Files in the staging dir that do not match this format are silently skipped.

Configuration
-------------
DB_FILE                  — BVL_DB_FILE, default ~/browser-visits.db
STAGING_SNAPSHOTS_DIR    — BVL_STAGING_SNAPSHOTS_DIR,
                           default ~/Library/Application Support/browser-visit-logger/inbox
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
import errno
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import time

HOME = os.path.expanduser('~')
DB_FILE = os.environ.get('BVL_DB_FILE', os.path.join(HOME, 'browser-visits.db'))
STAGING_SNAPSHOTS_DIR = os.environ.get(
    'BVL_STAGING_SNAPSHOTS_DIR',
    os.path.join(HOME, 'Library', 'Application Support',
                 'browser-visit-logger', 'inbox'),
)
ICLOUD_SNAPSHOTS_DIR = os.environ.get(
    'BVL_ICLOUD_SNAPSHOTS_DIR',
    os.path.join(HOME, 'Documents', 'browser-visit-logger', 'snapshots'),
)
# host.py writes per-day visit logs into this dir; the sealer collects each
# completed day's log into the matching iCloud snapshot subdir.
LOG_DIR = os.environ.get('BVL_LOG_DIR', HOME)
MIN_AGE_SECONDS = int(os.environ.get('BVL_MOVER_MIN_AGE_SECONDS', '60'))

# Number of consecutive same-(op, target) failures before a "persistent"
# error escalates to a user notification.  Catastrophic categories ignore
# this and notify on the first occurrence.
MOVER_ERROR_THRESHOLD = int(os.environ.get('BVL_MOVER_ERROR_THRESHOLD', '3'))

# Marker file written when we can't pop a macOS notification (non-macOS,
# headless run, osascript unavailable).  The user can spot it with `ls ~`.
_ATTENTION_FILE = os.path.join(HOME, 'browser-visits-mover-needs-attention')

# OSError errno values that warrant an immediate notification regardless of
# operation.  These are signs the underlying disk / filesystem is in a state
# the mover can't recover from on its own.
_IMMEDIATE_OSERROR_ERRNOS = frozenset({errno.ENOSPC, errno.EROFS, errno.EDQUOT})

# Per-operation actionable guidance, surfaced in both the macOS notification
# body and `--show-errors` output.  Keeps the error report self-explanatory
# for users who don't have the README handy.
_FIX_HINTS = {
    'move':
        'Check that the iCloud destination is writable and has free '
        'space, then wait for the next mover run.',
    'seal':
        'Check the iCloud destination, then wait for the next mover '
        'run — the seal pass retries every tick until it succeeds.',
    'rewrite_manifest':
        'Run `snapshot_sealer.py <date>` to rebuild the manifest, then '
        '`snapshot_mover.py --clear-error N` to clear this row.',
    'invalid_filename':
        "Rename the file to match '<YYYY-MM-DDTHH-MM-SSZ>-<hash>.<ext>' "
        'or remove it; the row clears on the next mover run.',
    'orphan_file':
        'Snapshot file has no matching events row.  Either delete the '
        'file, or re-tag its URL via the popup to recreate the row; '
        'either way clears on the next mover run.',
    'missing_directory':
        "Re-create the directory, OR remove the snapshots row "
        "(`sqlite3 <db> \"DELETE FROM snapshots WHERE date='<YYYY-MM-DD>'\"`).",
    'top_level':
        'Check ~/browser-visits-mover.log for the traceback, then '
        '`snapshot_mover.py --clear-errors` once the bug is fixed.',
    'manifest_invalid':
        'Re-seal the directory: delete MANIFEST.tsv and run '
        '`snapshot_sealer.py <date>`, then `snapshot_mover.py '
        '--clear-error N` to clear this row.',
}

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


# ---------------------------------------------------------------------------
# Schema for the `mover_errors` table — tracks unresolved mover failures
# so the user can be notified about persistent / catastrophic ones.
#
# One row per (operation, target) that is currently broken.  The mover
# upserts on each failure (incrementing `attempts`) and deletes on the
# first subsequent success.  `_escalate_errors` notifies the user when
# a row crosses the threshold (or immediately, for catastrophic ones)
# and flips `notified=1` so we don't re-notify every tick.
# ---------------------------------------------------------------------------

def _ensure_mover_errors_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS mover_errors (
            key        TEXT PRIMARY KEY,           -- '<op>:<target>'
            operation  TEXT NOT NULL,              -- 'move'|'seal'|'rewrite_manifest'|'top_level'
            target     TEXT NOT NULL,              -- file path / date dir / '' for top-level
            message    TEXT NOT NULL,              -- str(exc), sanitised
            first_seen TEXT NOT NULL,              -- ISO timestamp (UTC)
            last_seen  TEXT NOT NULL,
            attempts   INTEGER NOT NULL DEFAULT 1,
            notified   INTEGER NOT NULL DEFAULT 0,
            immediate  INTEGER NOT NULL DEFAULT 0  -- 1 = notify on first sight, 0 = wait for threshold
        )
    """)
    conn.commit()


def _now_iso() -> str:
    """Current UTC time as an ISO 8601 string.  Indirected for test mocking."""
    return datetime.datetime.now(datetime.timezone.utc).replace(
        microsecond=0).isoformat()


def _record_error(conn, op, target, exc):
    """UPSERT a mover_errors row for this failure.

    On insert: attempts=1, first_seen=last_seen=now, notified=0.
    On update: attempts incremented, last_seen and message refreshed,
    first_seen and notified preserved (so a single notification per
    streak of failures).

    Propagates sqlite3.Error if the DB is broken; callers in per-op
    except blocks should wrap with _try_record_error.
    """
    key = f'{op}:{target}'
    now = _now_iso()
    message = _tsv_sanitise(str(exc))[:500]
    immediate = 1 if _is_immediate(op, exc) else 0
    # On conflict: bump attempts, refresh last_seen + message, and only
    # *promote* immediate (0 → 1) — never demote an already-immediate row.
    conn.execute(
        "INSERT INTO mover_errors "
        "  (key, operation, target, message, first_seen, last_seen, "
        "   attempts, immediate) "
        "VALUES (?, ?, ?, ?, ?, ?, 1, ?) "
        "ON CONFLICT(key) DO UPDATE SET "
        "  attempts = attempts + 1, "
        "  last_seen = excluded.last_seen, "
        "  message = excluded.message, "
        "  immediate = MAX(immediate, excluded.immediate)",
        (key, op, target, message, now, now, immediate),
    )
    conn.commit()


def _try_record_error(conn, op, target, exc):
    """Best-effort wrapper around _record_error for use inside per-op except
    blocks: log on failure, never raise (the caller has its own work to do)."""
    try:
        _record_error(conn, op, target, exc)
    except sqlite3.Error as inner:
        logger.error(
            'Could not record %s error for %s: %s (original: %s)',
            op, target or '<top_level>', inner, exc,
        )


def _clear_error(conn, op, target):
    """DELETE the mover_errors row matching (op, target).

    Idempotent: a no-op if no row exists.  Safe to call after every
    successful operation without checking first.  Propagates sqlite3.Error.
    """
    conn.execute(
        "DELETE FROM mover_errors WHERE key = ?", (f'{op}:{target}',))
    conn.commit()


def _try_clear_error(conn, op, target):
    """Best-effort wrapper around _clear_error: log on failure, never raise."""
    try:
        _clear_error(conn, op, target)
    except sqlite3.Error as inner:
        logger.error(
            'Could not clear %s error for %s: %s', op, target, inner)


def _reconcile_dir_scoped_errors(conn, op, dir_path, current_strays):
    """Auto-heal `op` rows whose target lives under dir_path.

    Removes rows whose target is under dir_path but isn't in the
    current_strays set — i.e. files the user has since renamed,
    removed, or otherwise resolved.  Used by:

      - _move_pass with op='invalid_filename' on the staging dir.
      - _build_manifest_rows with op='invalid_filename' on the date dir
        (non-conforming filenames excluded from the manifest).
      - _build_manifest_rows with op='orphan_file' on the date dir
        (conforming files with no DB row, also excluded from the manifest).

    Best-effort: logs and returns on DB error.
    """
    try:
        prior = conn.execute(
            "SELECT key, target FROM mover_errors WHERE operation = ?",
            (op,),
        ).fetchall()
        prefix = os.path.join(dir_path, '')   # ensures trailing path separator
        current_set = set(current_strays)
        for key, target in prior:
            if target.startswith(prefix) and target not in current_set:
                conn.execute(
                    "DELETE FROM mover_errors WHERE key = ?", (key,))
        conn.commit()
    except sqlite3.Error as inner:  # pragma: no cover
        # Defensive: a mid-reconcile DB failure means the user already
        # has bigger problems.  Best-effort log; the seal pass continues.
        logger.error(
            'Could not reconcile %s errors under %s: %s',
            op, dir_path, inner,
        )


def _is_immediate(op, exc):
    """Return True iff this error should trigger a user notification on the
    first occurrence (rather than after MOVER_ERROR_THRESHOLD attempts).

    Three immediate categories:

    1. Top-level uncaught exception — by definition unexpected; the user
       needs to know now.
    2. Single-shot ops — operations whose natural retry loop won't
       re-attempt the same target on subsequent ticks, so the threshold-
       based escalation never fires:
         - 'rewrite_manifest' runs once per straggler arrival; if no more
           stragglers come (the common case), attempts stays at 1 forever.
         - 'invalid_filename' inside a date subdir runs once per
           successful seal of that dir; after sealed=1 the row is never
           re-visited.
       (The staging-side 'invalid_filename' rows DO accumulate on each
       tick that re-encounters the file, but treating both as immediate
       keeps the classification simple and gets the user notified faster.)
    3. Catastrophic OSError errnos and DB integrity errors — the
       underlying disk / DB needs attention before any retry can succeed.
    """
    if op in ('top_level', 'rewrite_manifest', 'invalid_filename',
              'orphan_file', 'manifest_invalid'):
        return True
    if isinstance(exc, OSError) and exc.errno in _IMMEDIATE_OSERROR_ERRNOS:
        return True
    # sqlite3.DatabaseError minus its OperationalError subclass (which
    # covers transient locks / busy timeouts) — what's left is integrity
    # / corruption: not safe to keep retrying without user action.
    if (isinstance(exc, sqlite3.DatabaseError)
            and not isinstance(exc, sqlite3.OperationalError)):
        return True
    return False


def _escalate_errors(conn):
    """Walk currently-unresolved error rows and notify the user about ones
    that warrant it.  Best-effort — logs and returns on any failure.

    A row is escalated when notified=0 AND (immediate=1 OR attempts >=
    MOVER_ERROR_THRESHOLD).  After notifying, notified is flipped to 1 so
    the same row isn't surfaced repeatedly — even if it keeps failing.
    """
    try:
        rows = conn.execute(
            "SELECT key, operation, target, message, attempts, "
            "       first_seen, immediate "
            "FROM mover_errors "
            "WHERE notified = 0 AND (immediate = 1 OR attempts >= ?) "
            "ORDER BY first_seen ASC, key ASC",
            (MOVER_ERROR_THRESHOLD,),
        ).fetchall()
    except sqlite3.Error as inner:
        logger.error('Could not query mover_errors during escalation: %s', inner)
        return

    for key, op, target, message, attempts, first_seen, _immediate in rows:
        title = 'Browser Visit Logger: mover error'
        if op == 'top_level':
            body = f'Mover crashed: {message}'
        else:
            body = (f'{op} failed {attempts}× since {first_seen}: '
                    f'{target or "(no target)"} — {message}')
        hint = _FIX_HINTS.get(op)
        if hint:
            body = f'{body}  Fix: {hint}'
        _notify_user(title, body)
        try:
            conn.execute(
                "UPDATE mover_errors SET notified = 1 WHERE key = ?", (key,))
            conn.commit()
        except sqlite3.Error as inner:
            logger.error(
                'Could not mark %s error notified: %s', op, inner)


def _notify_user(title, message):
    """Surface an unresolvable mover error to the user.

    macOS: pop a Notification Center banner via osascript.
    Other / headless / osascript missing: touch a marker file at
    ~/browser-visits-mover-needs-attention so the user can spot it.

    Best-effort: catches everything, never propagates.
    """
    body = message[:240]   # macOS notifications truncate around 256 chars
    if sys.platform == 'darwin':
        try:
            script = (
                f'display notification {_applescript_quote(body)} '
                f'with title {_applescript_quote(title)}'
            )
            subprocess.run(
                ['osascript', '-e', script],
                check=False, timeout=5,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            return
        except Exception as exc:    # noqa: BLE001 — truly best-effort
            logger.error('osascript notification failed: %s', exc)
    # Fallback: touch the attention file.
    try:
        with open(_ATTENTION_FILE, 'a', encoding='utf-8') as f:
            f.write(f'{_now_iso()}\t{title}\t{body}\n')
    except OSError as exc:
        logger.error('Could not write attention file %s: %s',
                     _ATTENTION_FILE, exc)


def _applescript_quote(s):
    """Escape a string for safe interpolation into AppleScript."""
    return '"' + s.replace('\\', '\\\\').replace('"', '\\"') + '"'

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
    """Scan the staging directory and move every old-enough snapshot to iCloud.

    Uses the filesystem as the source of truth, so any file left in the
    staging dir by a failed prior run is automatically retried.
    """
    if not os.path.isdir(STAGING_SNAPSHOTS_DIR):
        return

    now = time.time()
    current_invalid = []   # paths flagged this pass; used to reconcile errors
    for filename in os.listdir(STAGING_SNAPSHOTS_DIR):
        source = os.path.join(STAGING_SNAPSHOTS_DIR, filename)
        if not os.path.isfile(source):
            continue

        m = _SNAPSHOT_FILENAME_RE.match(filename)
        if not m:
            logger.error(
                'Skipping %s — does not match snapshot filename format; '
                'leaving in staging dir', source,
            )
            _try_record_error(
                conn, 'invalid_filename', source,
                ValueError('filename does not match snapshot format'),
            )
            current_invalid.append(source)
            continue

        age = now - os.path.getmtime(source)
        if age < MIN_AGE_SECONDS:
            logger.debug('Skipping %s — only %.0fs old (< %ds)', filename, age, MIN_AGE_SECONDS)
            continue

        date_str = m.group(1)   # 'YYYY-MM-DD'
        _move_one(conn, source, filename, date_str, dry_run=dry_run)

    # Auto-heal: clear invalid_filename rows for files in the staging dir
    # that the user has since renamed or removed.
    _reconcile_dir_scoped_errors(
        conn, 'invalid_filename', STAGING_SNAPSHOTS_DIR, current_invalid)


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
        # (d) Update DB rows that still record this file as living in staging.
        #     Rows already pointing to iCloud (from a prior partial run) are
        #     untouched by the WHERE clause — that's correct.
        for table in EVENTS_TABLES:
            conn.execute(
                f"UPDATE {table} SET directory = ?"
                f" WHERE filename = ? AND directory = ?",
                (date_subdir, filename, STAGING_SNAPSHOTS_DIR),
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
        # (h) Move succeeded — clear any prior 'move' error for this source.
        _try_clear_error(conn, 'move', source)
        # (i) If this was a straggler, rebuild the manifest so it includes
        #     the just-moved file.  Sealed flag stays 1.
        if is_straggler:
            _rewrite_manifest_for_straggler(conn, date_subdir)
    except (OSError, sqlite3.Error) as exc:
        logger.error('Failed to move %s: %s', source, exc)
        _try_record_error(conn, 'move', source, exc)


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

    Driven by the database.  Rows come from two sources: the move pass
    inserts one when a snapshot file lands in a new date dir; host.py
    inserts one on every write invocation (covers activity-only days
    that produced no snapshot files).

    The iCloud date dir is created on the fly if absent — both branches
    above can produce a snapshots row without a matching dir on disk.
    """
    today = _today_utc()
    rows = conn.execute(
        "SELECT date FROM snapshots WHERE sealed = 0 AND date < ? ORDER BY date",
        (today.isoformat(),),
    ).fetchall()
    for (date_str,) in rows:
        date_subdir = os.path.join(ICLOUD_SNAPSHOTS_DIR, date_str)
        if not os.path.isdir(date_subdir):
            if dry_run:
                logger.info(
                    '[dry-run] would create %s for activity-only day',
                    date_subdir)
            else:
                try:
                    os.makedirs(date_subdir, exist_ok=True)
                except OSError as exc:
                    logger.error('Failed to create %s: %s', date_subdir, exc)
                    _try_record_error(conn, 'seal', date_subdir, exc)
                    continue
        _try_clear_error(conn, 'missing_directory', date_subdir)
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

    The manifest is always (re)written — even if one already exists.  A
    prior crash may have left a partial manifest behind, and the cost of
    rebuilding from the events tables is small compared to the risk of
    leaving truncated content in place.  _write_manifest_file unlinks any
    existing read-only manifest before writing.

    Errors writing/chmod'ing the manifest, or updating the DB, are logged
    but do not propagate.
    """
    if dry_run:
        logger.info('[dry-run] would seal %s', date_subdir)
        return

    try:
        count = _write_manifest_file(conn, date_subdir)
        # Move the day's log file into the sealed dir if one exists in
        # LOG_DIR.  Done before flipping sealed=1 so a failure leaves the
        # row at sealed=0 and the seal will retry on the next tick.
        if date_key is not None:
            _move_log_into_sealed_dir(date_subdir, date_key)
        logger.info('Sealed %s (%d entries)', date_subdir, count)
        if date_key is not None:
            # Upsert: the auto seal pass always finds an existing row (the
            # mover or host inserted it).  The manual sealer may not — e.g.
            # if the user is sealing a directory imported from elsewhere —
            # so we tolerate the no-row case by inserting first.
            conn.execute(
                "INSERT INTO snapshots (date, sealed) VALUES (?, 1) "
                "ON CONFLICT(date) DO UPDATE SET sealed = 1",
                (date_key,),
            )
            conn.commit()
        # Seal succeeded — clear any prior 'seal' error for this directory.
        _try_clear_error(conn, 'seal', date_subdir)
    except (OSError, sqlite3.Error) as exc:
        logger.error('Failed to seal %s: %s', date_subdir, exc)
        _try_record_error(conn, 'seal', date_subdir, exc)


def _log_filename_for(date_iso: str) -> str:
    """Per-day log filename used by both host.py and the sealer."""
    return f'browser-visits-{date_iso}.log'


def _move_log_into_sealed_dir(date_subdir: str, date_iso: str) -> None:
    """Move <LOG_DIR>/browser-visits-<date>.log into date_subdir, chmod 0o444.

    No-op if no log file for that date exists in LOG_DIR (e.g. the day
    had read/skimmed events from a clock-skewed host but no auto-log).
    Caller is responsible for catching OSError.
    """
    src = os.path.join(LOG_DIR, _log_filename_for(date_iso))
    if not os.path.exists(src):
        return
    dst = os.path.join(date_subdir, _log_filename_for(date_iso))
    shutil.move(src, dst)
    os.chmod(dst, 0o444)


_LOG_FILENAME_RE = re.compile(r'^browser-visits-(\d{4}-\d{2}-\d{2})\.log$')


def _orphan_log_merge_pass(conn: sqlite3.Connection) -> None:
    """Anti-entropy: scan LOG_DIR for past-day per-day logs and reconcile.

    Two cases:
      1. **Race orphan** — the iCloud counterpart already exists.  This
         means the seal pass moved an earlier copy and a host invocation
         in the seal-window race has since recreated the file in LOG_DIR
         with stragglers.  Append the orphan into the iCloud log and
         delete the orphan.
      2. **Lost snapshots row** — no iCloud counterpart exists and there
         is no snapshots row for the date (e.g. the host crashed between
         the snapshots INSERT OR IGNORE and the first log write).
         Backfill the snapshots row at sealed=0 so the next normal seal
         pass picks it up.

    Skips today's UTC log file — it's still being written.
    """
    if not os.path.isdir(LOG_DIR):
        return
    today_iso = _today_utc().isoformat()
    for entry in sorted(os.listdir(LOG_DIR)):
        m = _LOG_FILENAME_RE.match(entry)
        if not m:
            continue
        date_iso = m.group(1)
        if date_iso >= today_iso:
            continue
        src = os.path.join(LOG_DIR, entry)
        date_subdir = os.path.join(ICLOUD_SNAPSHOTS_DIR, date_iso)
        dst = os.path.join(date_subdir, entry)
        try:
            if os.path.exists(dst):
                # Race orphan — chmod +w the iCloud log, append, chmod 0o444.
                os.chmod(dst, 0o644)
                with open(src, 'r', encoding='utf-8') as fsrc, \
                        open(dst, 'a', encoding='utf-8') as fdst:
                    fdst.write(fsrc.read())
                os.chmod(dst, 0o444)
                os.unlink(src)
                logger.info('Merged orphan log %s into %s', src, dst)
                _try_clear_error(conn, 'seal', date_subdir)
            else:
                # No iCloud counterpart — make sure the seal pass picks
                # it up on the next tick.  INSERT OR IGNORE is a no-op if
                # the row already exists (the common case — host inserted
                # it).  Defensive for the host-crash-mid-startup scenario.
                conn.execute(
                    "INSERT OR IGNORE INTO snapshots (date, sealed) VALUES (?, 0)",
                    (date_iso,),
                )
                conn.commit()
        except OSError as exc:
            logger.error('orphan-merge for %s: %s', src, exc)
            _try_record_error(conn, 'seal', date_subdir, exc)


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
        _try_clear_error(conn, 'rewrite_manifest', date_subdir)
    except (OSError, sqlite3.Error) as exc:
        logger.error(
            'Failed to rewrite %s after straggler arrival: %s',
            manifest_path, exc,
        )
        _try_record_error(conn, 'rewrite_manifest', date_subdir, exc)


def _build_manifest_rows(
    conn: sqlite3.Connection, date_subdir: str,
):
    """Return one tuple per *conforming, non-orphan* snapshot file in the
    directory, sorted by filename.

    Two categories are excluded from the manifest, each producing an
    ERROR log line plus a mover_errors row so the user is notified:

      - Non-conforming filenames (don't match
        '<YYYY-MM-DDTHH-MM-SSZ>-<hash>.<ext>') → op 'invalid_filename'.
      - Conforming files with no matching events row in the DB
        (orphans) → op 'orphan_file'.

    The exclusion of orphan files matches the "correct sealed directory"
    invariant enforced by snapshot_verifier.py: every manifest row must
    correspond to a real DB row.
    """
    # Determine the per-day log filename to skip alongside MANIFEST.tsv.
    # Only date-named directories have one; manual seals of non-date dirs
    # have no expected log filename (and so nothing to skip).
    basename = os.path.basename(os.path.normpath(date_subdir))
    expected_log = (
        _log_filename_for(basename)
        if _DATE_DIR_RE.match(basename)
        else None
    )
    files = sorted(
        f for f in os.listdir(date_subdir)
        if f != MANIFEST_FILENAME
        and f != expected_log
        and os.path.isfile(os.path.join(date_subdir, f))
    )
    rows = []
    current_invalid = []
    current_orphan = []
    for filename in files:
        full_path = os.path.join(date_subdir, filename)
        if not _SNAPSHOT_FILENAME_RE.match(filename):
            logger.error(
                'Excluding %s from manifest — does not match snapshot '
                'filename format', full_path,
            )
            _try_record_error(
                conn, 'invalid_filename', full_path,
                ValueError('filename does not match snapshot format'),
            )
            current_invalid.append(full_path)
            continue
        info = _lookup_event(conn, filename, date_subdir)
        if info is None:
            # Orphan file: a conforming snapshot file with no events row.
            # Per the "correct sealed directory" definition, such files
            # are not allowed in the manifest.  Exclude and record so the
            # user can either re-tag the URL or remove the file.
            logger.error(
                'Excluding %s from manifest — no events row in DB for '
                'this file (orphan)', full_path,
            )
            _try_record_error(
                conn, 'orphan_file', full_path,
                ValueError('snapshot file has no matching events row'),
            )
            current_orphan.append(full_path)
            continue
        rows.append((
            _tsv_sanitise(filename),
            info['tag'],
            _tsv_sanitise(info['timestamp']),
            _tsv_sanitise(info['url']),
            _tsv_sanitise(info['title']),
        ))

    # Auto-heal: clear invalid_filename / orphan_file rows for files in
    # this dir that the user has since fixed (renamed, removed, or — for
    # orphans — recorded an events row for).
    _reconcile_dir_scoped_errors(
        conn, 'invalid_filename', date_subdir, current_invalid)
    _reconcile_dir_scoped_errors(
        conn, 'orphan_file', date_subdir, current_orphan)
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
    """Run a single mover pass against the current module-level constants.

    Wraps the body in a top-level catch so that any unexpected exception is
    recorded as an `op='top_level'` row (immediate-category, so the user is
    notified on the first occurrence) and surfaced via _escalate_errors,
    *then* re-raised so launchd captures the traceback in the log.
    """
    conn = None
    try:
        os.makedirs(ICLOUD_SNAPSHOTS_DIR, exist_ok=True)
        if not os.path.exists(DB_FILE):
            logger.info('No DB at %s; nothing to do', DB_FILE)
            return
        conn = sqlite3.connect(DB_FILE)
        _ensure_snapshots_table(conn)
        _ensure_mover_errors_table(conn)
        _move_pass(conn, dry_run=dry_run)
        _seal_pass(conn, dry_run=dry_run)
        if not dry_run:
            _orphan_log_merge_pass(conn)
        _escalate_errors(conn)
    except Exception as exc:
        # Top-level: best-effort record + escalate, then fall back to a
        # direct notification if the DB path is broken, then re-raise.
        try:
            if conn is None:
                conn = sqlite3.connect(DB_FILE)
            _ensure_mover_errors_table(conn)
            _record_error(conn, 'top_level', '', exc)
            _escalate_errors(conn)
        except Exception as inner:                  # noqa: BLE001
            logger.error(
                'Could not record top-level failure to DB (%s); '
                'falling back to direct notification', inner,
            )
            _notify_user(
                'Browser Visit Logger: mover crashed',
                f'Top-level failure: {exc}',
            )
        raise
    finally:
        if conn is not None:
            conn.close()


# ---------------------------------------------------------------------------
# CLI entry point — for manual / testing invocation
# ---------------------------------------------------------------------------

def _parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog='snapshot_mover.py',
        description='Move browser-snapshot files from the local staging dir '
                    'to the iCloud-synced Documents archive.',
    )
    p.add_argument('--dry-run', action='store_true',
                   help='report what would happen without touching files or the DB')
    p.add_argument('-v', '--verbose', action='store_true',
                   help='enable DEBUG logging')
    p.add_argument('--min-age-seconds', type=int, metavar='N',
                   help=f'override the file-age threshold '
                        f'(default {MIN_AGE_SECONDS}s)')
    p.add_argument('--error-threshold', type=int, metavar='N',
                   help=f'override the persistent-error notification threshold '
                        f'(default {MOVER_ERROR_THRESHOLD})')
    p.add_argument('--source', metavar='DIR',
                   help=f'override the source (staging) directory '
                        f'(default {STAGING_SNAPSHOTS_DIR})')
    p.add_argument('--dest', metavar='DIR',
                   help=f'override the destination (iCloud) directory '
                        f'(default {ICLOUD_SNAPSHOTS_DIR})')
    p.add_argument('--db', metavar='FILE',
                   help=f'override the SQLite database path '
                        f'(default {DB_FILE})')
    # Error-table inspection / acknowledgement.  Mutually exclusive with
    # each other; specifying any of them skips the move/seal pass.
    g = p.add_mutually_exclusive_group()
    g.add_argument('--show-errors', action='store_true',
                   help='print the pending mover_errors table rows and exit')
    g.add_argument('--clear-errors', action='store_true',
                   help='delete every row from mover_errors and exit')
    g.add_argument('--clear-error', type=int, metavar='N',
                   help='delete the Nth row (1-indexed, matching '
                        '--show-errors order) from mover_errors and exit')
    return p.parse_args(argv)


def _apply_args(args: argparse.Namespace) -> None:
    """Apply parsed CLI args to module-level constants and the logger."""
    global STAGING_SNAPSHOTS_DIR, ICLOUD_SNAPSHOTS_DIR, DB_FILE
    global MIN_AGE_SECONDS, MOVER_ERROR_THRESHOLD
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    if args.source is not None:
        STAGING_SNAPSHOTS_DIR = args.source
    if args.dest is not None:
        ICLOUD_SNAPSHOTS_DIR = args.dest
    if args.db is not None:
        DB_FILE = args.db
    if args.min_age_seconds is not None:
        MIN_AGE_SECONDS = args.min_age_seconds
    if args.error_threshold is not None:
        MOVER_ERROR_THRESHOLD = args.error_threshold


def cli(argv=None) -> int:
    """Parse argv, apply overrides, run one mover pass (or one error CLI op).

    Returns the process exit code (0 on success, non-zero on user-visible
    error CLI failures such as out-of-range --clear-error).
    """
    args = _parse_args(argv)
    _apply_args(args)
    if args.show_errors:
        return _cli_show_errors()
    if args.clear_errors:
        return _cli_clear_errors()
    if args.clear_error is not None:
        return _cli_clear_error(args.clear_error)
    main(dry_run=args.dry_run)
    return 0


def _open_errors_conn():
    """Open the DB and ensure the mover_errors table exists.  The error CLI
    flags create the DB if missing — handy for users who haven't yet
    triggered any mover activity."""
    conn = sqlite3.connect(DB_FILE)
    _ensure_mover_errors_table(conn)
    return conn


def _fetch_pending_errors(conn):
    """SELECT all rows ordered for stable indexing."""
    return conn.execute(
        "SELECT key, operation, target, message, attempts, "
        "       first_seen, last_seen, notified "
        "FROM mover_errors "
        "ORDER BY first_seen ASC, key ASC"
    ).fetchall()


def _cli_show_errors() -> int:
    conn = _open_errors_conn()
    try:
        rows = _fetch_pending_errors(conn)
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
        hint = _FIX_HINTS.get(op)
        if hint:
            print(f'      fix:      {hint}')
        print(f'      notified: {"yes" if notified else "no"}')
        print()
    return 0


def _cli_clear_errors() -> int:
    conn = _open_errors_conn()
    try:
        cursor = conn.execute("DELETE FROM mover_errors")
        conn.commit()
        n = cursor.rowcount
    finally:
        conn.close()
    print(f'Cleared {n} error row{"" if n == 1 else "s"}.')
    return 0


def _cli_clear_error(n: int) -> int:
    conn = _open_errors_conn()
    try:
        rows = _fetch_pending_errors(conn)
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
