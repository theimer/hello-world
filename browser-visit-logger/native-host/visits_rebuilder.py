#!/usr/bin/env python3
"""
visits_rebuilder.py — rebuild ``browser-visits.db`` from the on-disk log
and the iCloud snapshot archive.

The DB is normally the source of truth.  When it's lost or corrupt, this
tool restores it from two side-channels that *are* still durable:

  1. ``browser-visits.log`` — append-only TSV every host.py invocation
     writes.  Each invocation produces an action line followed by a
     result line, both prefixed with the same record_id (UUID hex).
     Phase 1 ("log replay") parses this file and re-applies every
     successful action via the existing host.py helpers.

  2. The iCloud snapshot archive — daily ``YYYY-MM-DD`` subdirectories
     under ICLOUD_SNAPSHOTS_DIR, optionally containing a MANIFEST.tsv
     once the directory has been sealed.  Phase 2 ("filesystem
     rehydration") repopulates the ``snapshots`` table and updates the
     ``directory`` column on event rows whose snapshot file has since
     been moved out of Downloads.

``mover_errors`` is intentionally *not* recovered.  Filesystem-derived
rows (orphan_file, invalid_filename, missing_directory,
manifest_invalid) repopulate naturally on the next mover/sealer/
verifier pass.  Transient failures that have since healed are
correctly dropped.
"""

import argparse
import logging
import os
import re
import sqlite3
import sys
from dataclasses import dataclass, field
from typing import Optional

import host
import snapshot_mover

logger = logging.getLogger('visits_rebuilder')

# A record_id is uuid.uuid4().hex — exactly 32 lowercase hex chars.
_UUID_RE = re.compile(r'^[0-9a-f]{32}$')

# Tags that include a trailing filename column on the action line.
_FILENAME_TAGS = ('read', 'skimmed')


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

@dataclass
class ReplayStats:
    visits_inserted:    int = 0
    of_interest_set:    int = 0
    read_events:        int = 0
    skimmed_events:     int = 0
    success_records:    int = 0
    error_records:      int = 0    # action+error pair: counted, not applied
    orphan_actions:     int = 0    # action with no matching result
    orphan_results:     int = 0    # result with no matching action
    malformed_lines:    int = 0    # bad UUID prefix, wrong field count, etc.

    @property
    def has_skipped_lines(self) -> bool:
        """True if any action/result line was skipped (excluding error pairs)."""
        return bool(self.orphan_actions or self.orphan_results or self.malformed_lines)


@dataclass
class RehydrateStats:
    snapshots_upserted:    int = 0
    sealed_dirs:           int = 0
    unsealed_dirs:         int = 0
    events_relocated:      int = 0
    files_without_events:  int = 0


@dataclass
class RebuildStats:
    replay:    Optional[ReplayStats]    = None
    rehydrate: Optional[RehydrateStats] = None
    truncated: bool = False


# ---------------------------------------------------------------------------
# Phase 1 — log replay
# ---------------------------------------------------------------------------

def _looks_like_uuid(field_str: str) -> bool:
    return bool(_UUID_RE.fullmatch(field_str))


def _parse_action_fields(parts: list) -> Optional[dict]:
    """Parse an action line's fields (excluding the leading record_id).

    parts has the record_id already stripped.  Returns a dict on success
    or None if the field count is wrong.
    """
    # auto-log: timestamp, url, title  → 3 fields
    # of_interest: timestamp, url, title, tag  → 4 fields
    # read/skimmed: timestamp, url, title, tag, filename  → 5 fields
    if len(parts) == 3:
        return {'timestamp': parts[0], 'url': parts[1], 'title': parts[2],
                'tag': '', 'filename': ''}
    if len(parts) == 4:
        return {'timestamp': parts[0], 'url': parts[1], 'title': parts[2],
                'tag': parts[3], 'filename': ''}
    if len(parts) == 5:
        return {'timestamp': parts[0], 'url': parts[1], 'title': parts[2],
                'tag': parts[3], 'filename': parts[4]}
    return None


def _is_result_payload(payload: str) -> bool:
    return payload == 'success' or payload.startswith('error: ')


# Per-day log filename pattern: 'browser-visits-YYYY-MM-DD.log'.
_LOG_FILENAME_RE = re.compile(r'^browser-visits-(\d{4}-\d{2}-\d{2})\.log$')


def _collect_log_paths(log_dir: str, icloud_dir: str) -> list:
    """Return the list of per-day log files to replay, in chronological
    order (sorted by the date in the filename).

    Sources:
      1. <log_dir>/browser-visits-<date>.log — today's log + any past-day
         logs that haven't been moved yet (race orphans, log-only days
         pending the next sealer tick).
      2. <icloud_dir>/<YYYY-MM-DD>/browser-visits-<same-date>.log — every
         sealed iCloud subdir's log.  Defensive: if a log inside a date
         subdir has a different embedded date, it's skipped with a warning.

    Sort key is the date in the filename, so a URL first visited day 1
    and re-visited day 5 ends up with day-1's timestamp in
    ``visits.timestamp`` (INSERT OR IGNORE keeps the first replayed value).
    """
    # Each entry: (date_iso, source_priority, path).  source_priority sorts
    # iCloud (0) before log_dir (1) so when both have a same-date file
    # (the cross-day seal-race case) the iCloud copy is processed first
    # — its action lines populate `pending` before log_dir's stragglers
    # arrive with the matching result lines.
    discovered = []

    if os.path.isdir(icloud_dir):
        for date_entry in os.listdir(icloud_dir):
            if not snapshot_mover._DATE_DIR_RE.match(date_entry):
                continue
            date_dir = os.path.join(icloud_dir, date_entry)
            if not os.path.isdir(date_dir):
                continue
            for entry in os.listdir(date_dir):
                m = _LOG_FILENAME_RE.match(entry)
                if not m:
                    continue
                full = os.path.join(date_dir, entry)
                if not os.path.isfile(full):
                    continue
                if m.group(1) != date_entry:
                    logger.warning(
                        'Log file %s embedded date does not match its '
                        'parent directory %s — skipping',
                        full, date_dir)
                    continue
                discovered.append((m.group(1), 0, full))

    if os.path.isdir(log_dir):
        for entry in os.listdir(log_dir):
            m = _LOG_FILENAME_RE.match(entry)
            if not m:
                continue
            full = os.path.join(log_dir, entry)
            if os.path.isfile(full):
                discovered.append((m.group(1), 1, full))

    discovered.sort(key=lambda t: (t[0], t[1]))
    return [(date_iso, path) for date_iso, _, path in discovered]


def _replay_one_file(conn: sqlite3.Connection, log_path: str,
                     pending: dict, stats: ReplayStats) -> None:
    """Replay a single per-day log file using the shared pending dict.

    Action lines accumulate into pending; result lines pop their matching
    action and apply (success) or count (error).  pending may carry over
    UUIDs across files — the cross-file race window described in
    docs/rebuild-visits-from-log.md.  Orphan reporting is left to the
    caller (replay_logs) so that pending is fully drained before
    reporting.
    """
    with open(log_path, 'r', encoding='utf-8') as f:
        for raw in f:
            line = raw.rstrip('\n')
            if not line:
                continue
            parts = line.split('\t')
            if not _looks_like_uuid(parts[0]):
                logger.warning(
                    'Skipping malformed log line in %s (non-UUID prefix): %r',
                    log_path, raw)
                stats.malformed_lines += 1
                continue
            record_id = parts[0]
            rest = parts[1:]

            # Result lines have exactly one field after the record_id, and
            # that field is either 'success' or starts with 'error: '.
            if len(rest) == 1 and _is_result_payload(rest[0]):
                action = pending.pop(record_id, None)
                if action is None:
                    logger.warning(
                        'Result line for %s in %s has no matching action — skipping',
                        record_id, log_path)
                    stats.orphan_results += 1
                    continue
                if rest[0] == 'success':
                    _apply_action(conn, action, stats)
                    stats.success_records += 1
                else:
                    stats.error_records += 1
                continue

            # Otherwise it's an action line.
            action = _parse_action_fields(rest)
            if action is None:
                logger.warning('Skipping malformed action line in %s: %r',
                               log_path, raw)
                stats.malformed_lines += 1
                continue
            if record_id in pending:
                # Duplicate UUID for a pending action — the host should
                # never emit this.  Drop the prior, count the new one.
                logger.warning(
                    'Duplicate record_id %s in %s while a prior action is '
                    'pending — dropping the prior orphan', record_id, log_path)
                stats.orphan_actions += 1
            pending[record_id] = action


def replay_logs(conn: sqlite3.Connection,
                log_dir: str, icloud_dir: str) -> ReplayStats:
    """Phase 1: enumerate every per-day log under log_dir and icloud_dir
    and replay them chronologically into the DB.

    A single ``pending`` dict spans all files so a UUID's action half in
    one file pairs with its result half in another (handles the
    cross-day seal race).  Anything left in pending at the end is an
    orphan action.
    """
    stats = ReplayStats()
    pending: dict = {}

    for date_iso, log_path in _collect_log_paths(log_dir, icloud_dir):
        logger.info('Replaying %s', log_path)
        # Mirror host.py's per-invocation INSERT OR IGNORE: each day with
        # a log file gets a snapshots row.  Phase 2's rehydrate may flip
        # sealed=1 if the iCloud dir for that date has a manifest; if not,
        # the row stays sealed=0 (e.g. today's log, which the sealer
        # wouldn't touch anyway).
        conn.execute(
            "INSERT OR IGNORE INTO snapshots (date, sealed) VALUES (?, 0)",
            (date_iso,),
        )
        _replay_one_file(conn, log_path, pending, stats)

    # Anything still pending after all files are exhausted is an orphan action.
    if pending:
        for record_id in pending:
            logger.warning('Action %s has no matching result line', record_id)
        stats.orphan_actions += len(pending)

    conn.commit()
    return stats


def _apply_action(conn: sqlite3.Connection, action: dict, stats: ReplayStats) -> None:
    """Apply a (parsed action, success) pair against the DB."""
    timestamp = action['timestamp']
    url       = action['url']
    title     = action['title']
    tag       = action['tag']
    filename  = action['filename']

    host.insert_visit(conn, timestamp, url, title)
    stats.visits_inserted += 1

    if tag == 'of_interest':
        conn.execute(
            "UPDATE visits SET of_interest = 1 WHERE url = ?", (url,)
        )
        stats.of_interest_set += 1
    elif tag in ('read', 'skimmed'):
        host.tag_visit(conn, url, tag, timestamp, filename)
        if tag == 'read':
            stats.read_events += 1
        else:
            stats.skimmed_events += 1
    elif tag:
        # Unknown tag — should have been rejected by host.py at write time.
        logger.warning('Unknown tag %r in record for %s — ignoring', tag, url)


# ---------------------------------------------------------------------------
# Phase 2 — filesystem rehydration
# ---------------------------------------------------------------------------

def rehydrate_filesystem(
    conn: sqlite3.Connection, icloud_dir: str, downloads_dir: str,
) -> RehydrateStats:
    """Phase 2: walk the iCloud archive, repopulate snapshots, relocate events.

    Files matching the snapshot filename pattern have any matching event
    rows updated to point at the date subdirectory (only rows whose
    ``directory`` column still says Downloads — already-relocated rows
    are left alone).  Orphan files are *not* deleted; the next
    snapshot_verifier pass will flag them.
    """
    stats = RehydrateStats()
    if not os.path.isdir(icloud_dir):
        logger.warning('iCloud snapshots dir %s does not exist — phase 2 noop',
                       icloud_dir)
        return stats

    snapshot_mover._ensure_snapshots_table(conn)

    for entry in sorted(os.listdir(icloud_dir)):
        if not snapshot_mover._DATE_DIR_RE.match(entry):
            continue
        date_dir = os.path.join(icloud_dir, entry)
        if not os.path.isdir(date_dir):
            continue
        manifest_path = os.path.join(date_dir, 'MANIFEST.tsv')
        sealed = 1 if os.path.exists(manifest_path) else 0
        conn.execute(
            "INSERT INTO snapshots (date, sealed) VALUES (?, ?) "
            "ON CONFLICT(date) DO UPDATE SET sealed = excluded.sealed",
            (entry, sealed),
        )
        stats.snapshots_upserted += 1
        if sealed:
            stats.sealed_dirs += 1
        else:
            stats.unsealed_dirs += 1

        # Skip the manifest and the per-day log file; phase 1 already
        # replayed the log, and the manifest is verified separately.
        log_filename = snapshot_mover._log_filename_for(entry)
        for fname in sorted(os.listdir(date_dir)):
            if fname == 'MANIFEST.tsv' or fname == log_filename:
                continue
            if not snapshot_mover._SNAPSHOT_FILENAME_RE.match(fname):
                continue
            relocated_here = 0
            for table in ('read_events', 'skimmed_events'):
                cur = conn.execute(
                    f"UPDATE {table} SET directory = ? "
                    f"WHERE filename = ? AND directory = ?",
                    (date_dir, fname, downloads_dir),
                )
                relocated_here += cur.rowcount
            if relocated_here > 0:
                stats.events_relocated += relocated_here
            else:
                # No matching events row in either table.  This file will
                # be flagged as an orphan by the next snapshot_verifier
                # pass; the rebuild tool itself does not delete or record.
                logger.info(
                    'No event row for %s in %s — snapshot_verifier will flag it',
                    fname, date_dir)
                stats.files_without_events += 1

    conn.commit()
    return stats


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

_REBUILDABLE_TABLES = ('visits', 'read_events', 'skimmed_events', 'snapshots')


def _truncate_rebuildable_tables(conn: sqlite3.Connection) -> None:
    """DROP the four log/FS-recoverable tables.  ``mover_errors`` is left alone."""
    for table in _REBUILDABLE_TABLES:
        conn.execute(f"DROP TABLE IF EXISTS {table}")
    conn.commit()


def rebuild(
    conn: sqlite3.Connection, *,
    log_dir: str, icloud_dir: str, downloads_dir: str,
    do_log: bool = True, do_rehydrate: bool = True, truncate: bool = True,
) -> RebuildStats:
    stats = RebuildStats(truncated=truncate)
    if truncate:
        _truncate_rebuildable_tables(conn)
    host.ensure_db(conn)
    snapshot_mover._ensure_snapshots_table(conn)

    if do_log:
        stats.replay = replay_logs(conn, log_dir, icloud_dir)
    if do_rehydrate:
        stats.rehydrate = rehydrate_filesystem(conn, icloud_dir, downloads_dir)
    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

# Exit codes
_EXIT_OK              = 0
_EXIT_INPUT_ERROR     = 1   # log/DB unreachable, orphans/malformed lines, bad args
_EXIT_UNEXPECTED      = 2   # unhandled exception


def _parse_args(argv=None):
    p = argparse.ArgumentParser(
        prog='visits_rebuilder.py',
        description='Rebuild browser-visits.db by replaying browser-visits.log '
                    'and rehydrating directory state from the iCloud snapshot '
                    'archive.',
    )
    truncate_group = p.add_mutually_exclusive_group()
    truncate_group.add_argument(
        '--truncate', dest='truncate', action='store_true', default=True,
        help='DROP and recreate visits / read_events / skimmed_events / '
             'snapshots before rebuilding (default).  mover_errors is left '
             'alone.')
    truncate_group.add_argument(
        '--no-truncate', dest='truncate', action='store_false',
        help='skip the wipe; rely on idempotency of replay + rehydrate')
    p.add_argument('--log-only', action='store_true',
                   help='skip phase 2 (filesystem rehydration)')
    p.add_argument('--rehydrate-only', action='store_true',
                   help='skip phase 1 (log replay)')
    p.add_argument('--log-dir', metavar='DIR', dest='log_dir',
                   help=f'override the per-day logs directory '
                        f'(default {host.LOG_DIR})')
    p.add_argument('--db', metavar='FILE', dest='db_path',
                   help=f'override the SQLite database path (default {host.DB_FILE})')
    p.add_argument('--source', metavar='DIR',
                   help=f'override the Downloads snapshots root '
                        f'(default {host.DOWNLOADS_SNAPSHOTS_DIR})')
    p.add_argument('--dest', metavar='DIR',
                   help=f'override the iCloud snapshots root '
                        f'(default {snapshot_mover.ICLOUD_SNAPSHOTS_DIR})')
    p.add_argument('-v', '--verbose', action='store_true',
                   help='enable DEBUG logging')

    args = p.parse_args(argv)
    if args.log_only and args.rehydrate_only:
        p.error('cannot combine --log-only with --rehydrate-only')
    return args


def cli(argv=None) -> int:
    args = _parse_args(argv)

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        stream=sys.stderr, level=log_level,
        format='%(asctime)s %(levelname)s [visits_rebuilder] %(message)s',
    )
    logger.setLevel(log_level)

    log_dir       = args.log_dir  or host.LOG_DIR
    db_path       = args.db_path  or host.DB_FILE
    downloads_dir = args.source   or host.DOWNLOADS_SNAPSHOTS_DIR
    icloud_dir    = args.dest     or snapshot_mover.ICLOUD_SNAPSHOTS_DIR

    do_log       = not args.rehydrate_only
    do_rehydrate = not args.log_only

    # Apply overrides to the imported modules so helpers downstream (e.g.
    # _ensure_events_table's directory DEFAULT, tag_visit's recorded
    # directory) use the same values the user passed on the CLI.
    host.DOWNLOADS_SNAPSHOTS_DIR = downloads_dir
    host.DB_FILE                 = db_path
    host.LOG_DIR                 = log_dir
    snapshot_mover.ICLOUD_SNAPSHOTS_DIR = icloud_dir
    snapshot_mover.LOG_DIR              = log_dir

    if do_log and not os.path.isdir(log_dir):
        print(f'No log directory at {log_dir}', file=sys.stderr)
        return _EXIT_INPUT_ERROR

    db_dir = os.path.dirname(db_path) or '.'
    if not os.path.isdir(db_dir):
        print(f'DB parent directory {db_dir} does not exist', file=sys.stderr)
        return _EXIT_INPUT_ERROR

    try:
        conn = sqlite3.connect(db_path)
        try:
            stats = rebuild(
                conn,
                log_dir=log_dir,
                icloud_dir=icloud_dir,
                downloads_dir=downloads_dir,
                do_log=do_log, do_rehydrate=do_rehydrate,
                truncate=args.truncate,
            )
        finally:
            conn.close()
    except Exception as exc:
        logger.exception('Rebuild failed: %s', exc)
        return _EXIT_UNEXPECTED

    _print_summary(stats)
    if stats.replay is not None and stats.replay.has_skipped_lines:
        return _EXIT_INPUT_ERROR
    return _EXIT_OK


def _print_summary(stats: RebuildStats) -> None:
    parts = []
    if stats.replay is not None:
        r = stats.replay
        parts.append(
            f'replay: visits={r.visits_inserted} of_interest={r.of_interest_set} '
            f'read={r.read_events} skimmed={r.skimmed_events} '
            f'errors={r.error_records} orphan_actions={r.orphan_actions} '
            f'orphan_results={r.orphan_results} malformed={r.malformed_lines}'
        )
    if stats.rehydrate is not None:
        h = stats.rehydrate
        parts.append(
            f'rehydrate: snapshots={h.snapshots_upserted} '
            f'sealed={h.sealed_dirs} unsealed={h.unsealed_dirs} '
            f'relocated={h.events_relocated} '
            f'files_without_events={h.files_without_events}'
        )
    print('\n'.join(parts))


if __name__ == '__main__':  # pragma: no cover
    sys.exit(cli())
