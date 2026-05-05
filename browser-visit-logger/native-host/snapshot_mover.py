"""
snapshot_mover.py — Library of seal helpers used by snapshot_sealer.py
and visits_rebuilder.py.

This module used to host the periodic move + seal + verify pipeline
that ran from a launchd LaunchAgent.  All of that runtime work moved
to Swift (see swift/Sources/BVLCore/ and swift/Sources/BVLVerifier/).
What remains here are the helpers the surviving Python tools still
need:

  - The `snapshots` and `mover_errors` schema setters
    (`_ensure_snapshots_table`, `_ensure_mover_errors_table`).
  - `_seal_directory` and its supporting helpers (`_write_manifest_file`,
    `_build_manifest_rows`, `_lookup_event`, `_move_log_into_sealed_dir`,
    `_log_filename_for`), called by `snapshot_sealer.py` for manual
    one-off seals.
  - `_orphan_log_merge_pass`, also called by `snapshot_sealer.py` after
    a manual seal so race orphans in `BVL_LOG_DIR` get cleaned up.
  - The error-tracking helpers (`_record_error`, `_clear_error`,
    `_try_*`, `_reconcile_dir_scoped_errors`, `_is_immediate`) —
    transitively reachable from `_build_manifest_rows`, which records
    `invalid_filename` / `orphan_file` rows for files that get
    excluded from the manifest.
  - The `_DATE_DIR_RE`, `_SNAPSHOT_FILENAME_RE`, `_LOG_FILENAME_RE`
    regexes and `_today_utc` / `_tsv_sanitise` utilities, used by both
    the sealer and `visits_rebuilder.py`.

Filename convention
-------------------
Snapshots have permanent datetime-prefixed names:
    <YYYY-MM-DDTHH-MM-SSZ>-<hash>.<ext>
    e.g.  2026-04-30T14-35-22Z-abc123.mhtml

The date portion (first 10 characters) determines the iCloud date
subdir:
    ICLOUD_SNAPSHOTS_DIR/2026-04-30/2026-04-30T14-35-22Z-abc123.mhtml
"""

import datetime
import errno
import logging
import os
import re
import shutil
import sqlite3


HOME = os.path.expanduser('~')
DB_FILE = os.environ.get('BVL_DB_FILE', os.path.join(HOME, 'browser-visits.db'))
ICLOUD_SNAPSHOTS_DIR = os.environ.get(
    'BVL_ICLOUD_SNAPSHOTS_DIR',
    os.path.join(HOME, 'Documents', 'browser-visit-logger', 'snapshots'),
)
# Per-day visit logs live here; the seal pass collects each completed
# day's log into the matching iCloud snapshot subdir.
LOG_DIR = os.environ.get('BVL_LOG_DIR', HOME)

# OSError errno values that warrant an immediate notification regardless of
# operation.  These are signs the underlying disk / filesystem is in a state
# the surviving code can't recover from on its own.  Used by `_is_immediate`,
# which `_record_error` consults when classifying a failure.
_IMMEDIATE_OSERROR_ERRNOS = frozenset({errno.ENOSPC, errno.EROFS, errno.EDQUOT})

# Name of the per-directory manifest file written by the seal pass.
# Its presence on disk marks the directory sealed; combined with the
# `snapshots` table it provides redundancy that lets a partial-seal
# crash recover on the next run.
MANIFEST_FILENAME = 'MANIFEST.tsv'

# Header columns of the manifest (must stay in sync with _build_manifest_rows).
_MANIFEST_HEADER = ('filename', 'tag', 'timestamp', 'url', 'title')

# Filenames the seal pass treats as "not part of the snapshot set" —
# silently skipped, never flagged as invalid_filename, never included
# in the manifest.  Currently just `.DS_Store`, which Finder writes
# any time the user opens the iCloud-synced sealed dir in Finder.
_IGNORED_NAMES = frozenset({'.DS_Store'})

# Matches the daily snapshot subdir name (UTC date, ISO format).
_DATE_DIR_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')

# Library: don't configure root logging at import time.  Standalone
# callers (snapshot_sealer.cli, visits_rebuilder.cli) call
# logging.basicConfig in their CLI entry points.
logger = logging.getLogger('snapshot_mover')
logger.setLevel(logging.INFO)

# Matches the permanent snapshot filename format:
#   <YYYY-MM-DD>T<HH>-<MM>-<SS>Z-<hash>.<ext>
# group(1) — UTC date string 'YYYY-MM-DD' (used to select the date subdir).
_SNAPSHOT_FILENAME_RE = re.compile(
    r'^(\d{4}-\d{2}-\d{2})T\d{2}-\d{2}-\d{2}Z-.+$'
)

# Matches the per-day visit-log filename (`browser-visits-YYYY-MM-DD.log`).
_LOG_FILENAME_RE = re.compile(r'^browser-visits-(\d{4}-\d{2}-\d{2})\.log$')


# ---------------------------------------------------------------------------
# Schema for the `snapshots` table — co-owned by host.py (Python) and
# the Swift production code.  Sealer needs to ensure it exists before
# upserting.
# ---------------------------------------------------------------------------

def _ensure_snapshots_table(conn: sqlite3.Connection) -> None:
    """Create the snapshots table if it doesn't already exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            date   TEXT PRIMARY KEY,         -- 'YYYY-MM-DD' (UTC)
            sealed INTEGER NOT NULL DEFAULT 0
        )
    """)
    conn.commit()


# ---------------------------------------------------------------------------
# Schema for the `mover_errors` table — tracks unresolved failures.
# Reached transitively from `_build_manifest_rows` via `_try_record_error`,
# so the sealer needs the table present before sealing a dir that has
# any non-conforming filenames or orphan files.
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
    streak of failures).  Propagates sqlite3.Error if the DB is
    broken; callers in per-op except blocks should wrap with
    `_try_record_error`.
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
    """Best-effort wrapper around `_record_error` for use inside per-op except
    blocks: log on failure, never raise (the caller has its own work to do)."""
    try:
        _record_error(conn, op, target, exc)
    except sqlite3.Error as inner:
        logger.error(
            'Could not record %s error for %s: %s (original: %s)',
            op, target or '<top_level>', inner, exc,
        )


def _clear_error(conn, op, target):
    """DELETE the mover_errors row matching (op, target).  Idempotent —
    no-op if no row exists.  Propagates sqlite3.Error."""
    conn.execute(
        "DELETE FROM mover_errors WHERE key = ?", (f'{op}:{target}',))
    conn.commit()


def _try_clear_error(conn, op, target):
    """Best-effort wrapper around `_clear_error`: log on failure, never raise."""
    try:
        _clear_error(conn, op, target)
    except sqlite3.Error as inner:
        logger.error(
            'Could not clear %s error for %s: %s', op, target, inner)


def _reconcile_dir_scoped_errors(conn, op, dir_path, current_strays):
    """Auto-heal `op` rows whose target lives under `dir_path`.

    Removes rows whose target is under `dir_path` but isn't in the
    `current_strays` set — i.e. files the user has since renamed,
    removed, or otherwise resolved.  Used by `_build_manifest_rows`
    with op='invalid_filename' / 'orphan_file' on the date dir
    (filenames excluded from the manifest).

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
    first occurrence (rather than after the persistent-error threshold).

    Three immediate categories:

    1. Top-level uncaught exception — by definition unexpected; the user
       needs to know now.
    2. Single-shot ops — operations whose natural retry loop won't
       re-attempt the same target on subsequent ticks, so threshold-
       based escalation never fires:
         - 'rewrite_manifest' runs once per straggler arrival; if no
           more stragglers come (the common case), attempts stays at 1
           forever.
         - 'invalid_filename' inside a date subdir runs once per
           successful seal of that dir; after sealed=1 the row is
           never re-visited.
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


# ---------------------------------------------------------------------------
# Time / string helpers
# ---------------------------------------------------------------------------

def _today_utc() -> datetime.date:
    """Return today's UTC date.  Indirected so tests can patch it."""
    return datetime.datetime.now(datetime.timezone.utc).date()


def _tsv_sanitise(s: str) -> str:
    """Strip tab/newline/CR characters so the field is safe for TSV output."""
    return (s or '').replace('\t', ' ').replace('\n', ' ').replace('\r', '')


# ---------------------------------------------------------------------------
# Seal helpers — write a read-only MANIFEST.tsv into a daily directory
# and (when sealing a date-named dir) move the per-day log in alongside.
# ---------------------------------------------------------------------------

def _seal_directory(
    conn: sqlite3.Connection,
    date_subdir: str,
    dry_run: bool = False,
    date_key: 'str | None' = None,
) -> None:
    """Write a read-only MANIFEST.tsv listing every snapshot file in the dir,
    then mark the directory's snapshots-table row as sealed.

    `date_key`, when given, is the YYYY-MM-DD primary key of the snapshots
    row to flip to sealed=1.  The manual sealer passes it only when the
    directory's basename is itself a valid date (so non-date directories
    get a manifest but no DB row).

    The manifest is always (re)written — even if one already exists.  A
    prior crash may have left a partial manifest behind, and the cost of
    rebuilding from the events tables is small compared to the risk of
    leaving truncated content in place.  `_write_manifest_file` unlinks
    any existing read-only manifest before writing.

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
            # Upsert: callers may not have a snapshots row pre-existing
            # (e.g. the user is sealing a directory imported from
            # elsewhere) — tolerate the no-row case by inserting first.
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
    """Per-day log filename used by the host and the sealer."""
    return f'browser-visits-{date_iso}.log'


def _move_log_into_sealed_dir(date_subdir: str, date_iso: str) -> None:
    """Move <LOG_DIR>/browser-visits-<date>.log into `date_subdir`, chmod 0o444.

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


def _orphan_log_merge_pass(conn: sqlite3.Connection) -> None:
    """Anti-entropy: scan LOG_DIR for past-day per-day logs and reconcile.

    Two cases:

      1. **Race orphan** — the iCloud counterpart already exists.  An
         earlier seal moved one copy and a host invocation in the
         seal-window race recreated the file in LOG_DIR with stragglers.
         Append the orphan into the iCloud log and delete the orphan.
      2. **Lost snapshots row** — no iCloud counterpart and no snapshots
         row.  Backfill the snapshots row at sealed=0 so the next normal
         seal picks it up.

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
                # the row already exists.
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
    """Build, write, and chmod 0o444 the manifest in `date_subdir`.

    Returns the number of data rows written (excluding the header).
    Removes any existing manifest first so we can write over a previously
    read-only file (`open(..., 'w')` can't truncate a 0o444 file).  Caller
    is responsible for catching OSError / sqlite3.Error.
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
    invariant enforced by the verifier: every manifest row must
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
        and f not in _IGNORED_NAMES
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
    """Find the read_events / skimmed_events row for `filename` in `directory`.

    Returns a dict with tag, url, timestamp, title (joined from visits for
    the page title), or None if no matching row is found in either table.

    Table names are trusted internal constants; safe to interpolate into SQL.
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
