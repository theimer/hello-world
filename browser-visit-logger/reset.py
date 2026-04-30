#!/usr/bin/env python3
"""
reset.py — Delete all local data produced by the Browser Visit Logger extension.

Files and directories managed:
  browser-visits.log                       — TSV visit/action log       (BVL_LOG_FILE)
  browser-visits-host.log                  — native host process log    (BVL_HOST_LOG)
  browser-visits-mover.log                 — snapshot mover process log (BVL_MOVER_LOG)
  browser-visits.db                        — SQLite visit database      (BVL_DB_FILE)
  ~/Downloads/browser-visit-snapshots/     — local snapshot staging dir
  ~/Documents/browser-visit-logger/        — iCloud-synced archive (snapshots and any
                                              other future data under this directory)

Usage:
    python reset.py                 # reset everything (with confirmation)
    python reset.py --log           # reset only the visit log
    python reset.py --host-log      # reset only the host and mover process logs
    python reset.py --db            # reset only the database
    python reset.py --snapshots     # reset only the local Downloads snapshots dir
    python reset.py --icloud        # reset only the iCloud archive directory
    python reset.py -f              # skip confirmation prompt

The same BVL_* environment variables used by host.py are respected here,
so custom paths work automatically.
"""

import argparse
import os
import shutil
import sys

HOME       = os.path.expanduser('~')
LOG_FILE   = os.environ.get('BVL_LOG_FILE',   os.path.join(HOME, 'browser-visits.log'))
HOST_LOG   = os.environ.get('BVL_HOST_LOG',   os.path.join(HOME, 'browser-visits-host.log'))
MOVER_LOG  = os.environ.get('BVL_MOVER_LOG',  os.path.join(HOME, 'browser-visits-mover.log'))
DB_FILE    = os.environ.get('BVL_DB_FILE',    os.path.join(HOME, 'browser-visits.db'))
SNAP_DIR   = os.environ.get('BVL_DOWNLOADS_SNAPSHOTS_DIR',
                            os.path.join(HOME, 'Downloads', 'browser-visit-snapshots'))
# iCloud archive root — wipe the whole tree (currently snapshots/ but we may
# add other subdirectories in the future).
ICLOUD_DIR = os.path.join(HOME, 'Documents', 'browser-visit-logger')


def _delete_file(path: str, label: str) -> None:
    if os.path.exists(path):
        os.remove(path)
        print(f'Deleted {label}: {path}')
    else:
        print(f'{label} not found, skipping: {path}')


def _delete_dir(path: str, label: str) -> None:
    if os.path.isdir(path):
        shutil.rmtree(path)
        print(f'Deleted {label}: {path}')
    else:
        print(f'{label} not found, skipping: {path}')


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Delete all local data produced by the Browser Visit Logger extension.',
    )
    parser.add_argument('--log',       action='store_true', help='reset only the visit log')
    parser.add_argument('--host-log',  action='store_true', help='reset only the host and mover process logs')
    parser.add_argument('--db',        action='store_true', help='reset only the database')
    parser.add_argument('--snapshots', action='store_true',
                        help='reset only the local Downloads snapshots directory')
    parser.add_argument('--icloud',    action='store_true',
                        help='reset only the iCloud archive directory')
    parser.add_argument('-f', '--force', action='store_true', help='skip confirmation prompt')
    args = parser.parse_args()

    reset_all    = not (args.log or args.host_log or args.db or args.snapshots or args.icloud)
    do_log       = args.log       or reset_all
    do_host_log  = args.host_log  or reset_all
    do_db        = args.db        or reset_all
    do_snapshots = args.snapshots or reset_all
    do_icloud    = args.icloud    or reset_all

    # Each entry: (path, label, kind)  where kind is 'file' or 'dir'
    targets = []
    if do_log:
        targets.append((LOG_FILE,   'visit log',                       'file'))
    if do_host_log:
        targets.append((HOST_LOG,   'host log',                        'file'))
        targets.append((MOVER_LOG,  'mover log',                       'file'))
    if do_db:
        targets.append((DB_FILE,    'database',                        'file'))
    if do_snapshots:
        targets.append((SNAP_DIR,   'Downloads snapshots directory',   'dir'))
    if do_icloud:
        targets.append((ICLOUD_DIR, 'iCloud archive directory',        'dir'))

    print('The following will be permanently deleted:')
    for path, label, kind in targets:
        status = 'exists' if os.path.exists(path) else 'not found'
        print(f'  [{status}] {path}')

    if not args.force:
        try:
            answer = input('\nProceed? [y/N] ').strip().lower()
        except (EOFError, KeyboardInterrupt):
            print('\nAborted.')
            sys.exit(0)
        if answer not in ('y', 'yes'):
            print('Aborted.')
            sys.exit(0)

    print()
    for path, label, kind in targets:
        if kind == 'dir':
            _delete_dir(path, label)
        else:
            _delete_file(path, label)


if __name__ == '__main__':
    main()
