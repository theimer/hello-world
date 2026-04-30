#!/usr/bin/env python3
"""
snapshot_sealer.py — Manually seal a single daily snapshot directory.

snapshot_mover.py automatically seals each daily archive directory once its
UTC date has fully passed (the seal pass runs after the move pass on every
mover invocation).  This script is the manual complement: it forces sealing
of one specific directory immediately, regardless of whether the day has
fully passed.  Useful for ad-hoc closing of a directory or for testing.

A "sealed" directory contains a read-only MANIFEST.tsv enumerating every
snapshot file in the directory along with its DB metadata
(filename, tag, event timestamp, url, page title).

Usage
-----
    # Seal by date (resolved relative to ICLOUD_SNAPSHOTS_DIR)
    python3 native-host/snapshot_sealer.py 2026-04-30

    # Seal by absolute or relative path
    python3 native-host/snapshot_sealer.py /Users/me/.../snapshots/2026-04-30

    # Show what would happen without writing
    python3 native-host/snapshot_sealer.py --dry-run 2026-04-30

    # Override DB / archive paths (matches snapshot_mover.py's flags)
    python3 native-host/snapshot_sealer.py \\
        --db /tmp/test.db --dest /tmp/snapshots 2026-04-30

The script refuses to overwrite an existing manifest; to re-seal a directory,
delete the manifest first.
"""

import argparse
import datetime
import logging
import os
import sqlite3
import sys

import snapshot_mover


def _parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog='snapshot_sealer.py',
        description='Manually seal a daily snapshot directory by writing '
                    'a read-only MANIFEST.tsv summarising its contents.',
    )
    p.add_argument('directory',
                   help="date 'YYYY-MM-DD' (resolved under ICLOUD_SNAPSHOTS_DIR) "
                        "or an explicit path to the directory to seal")
    p.add_argument('--dry-run', action='store_true',
                   help='report what would happen without writing the manifest')
    p.add_argument('-v', '--verbose', action='store_true',
                   help='enable DEBUG logging')
    p.add_argument('--dest', metavar='DIR',
                   help=f'override the iCloud snapshots root directory '
                        f'(default {snapshot_mover.ICLOUD_SNAPSHOTS_DIR})')
    p.add_argument('--db', metavar='FILE',
                   help=f'override the SQLite database path '
                        f'(default {snapshot_mover.DB_FILE})')
    return p.parse_args(argv)


def _resolve_target(directory_arg: str) -> str:
    """Resolve the CLI directory argument.

    A bare 'YYYY-MM-DD' is joined under ICLOUD_SNAPSHOTS_DIR.  Anything that
    looks like a path (absolute, or contains a path separator) is used verbatim.
    """
    if os.path.isabs(directory_arg) or os.sep in directory_arg:
        return directory_arg
    return os.path.join(snapshot_mover.ICLOUD_SNAPSHOTS_DIR, directory_arg)


def _extract_date_key(target: str) -> 'str | None':
    """Return target's basename if it's a valid YYYY-MM-DD date, else None.

    The snapshots table is keyed by date, so only date-named directories
    yield a row.  Manually sealing a non-date-named directory still writes
    the manifest but leaves the table untouched.
    """
    base = os.path.basename(os.path.normpath(target))
    if not snapshot_mover._DATE_DIR_RE.match(base):
        return None
    try:
        datetime.date.fromisoformat(base)
    except ValueError:
        return None
    return base


def cli(argv=None) -> int:
    """Parse argv, apply overrides, seal one directory.  Returns an exit code."""
    args = _parse_args(argv)

    if args.verbose:
        snapshot_mover.logger.setLevel(logging.DEBUG)
    if args.dest is not None:
        snapshot_mover.ICLOUD_SNAPSHOTS_DIR = args.dest
    if args.db is not None:
        snapshot_mover.DB_FILE = args.db

    target = _resolve_target(args.directory)

    if not os.path.isdir(target):
        print(f'No such directory: {target}', file=sys.stderr)
        return 1

    manifest_path = os.path.join(target, snapshot_mover.MANIFEST_FILENAME)
    if os.path.exists(manifest_path):
        print(f'Already sealed (manifest exists): {manifest_path}',
              file=sys.stderr)
        return 1

    if not os.path.exists(snapshot_mover.DB_FILE):
        print(f'No DB at {snapshot_mover.DB_FILE}', file=sys.stderr)
        return 1

    conn = sqlite3.connect(snapshot_mover.DB_FILE)
    try:
        snapshot_mover._ensure_snapshots_table(conn)
        snapshot_mover._seal_directory(
            conn, target,
            dry_run=args.dry_run,
            date_key=_extract_date_key(target),
        )
    finally:
        conn.close()
    return 0


if __name__ == '__main__':  # pragma: no cover
    sys.exit(cli())
