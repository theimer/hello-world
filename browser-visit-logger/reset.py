#!/usr/bin/env python3
"""
reset.py — Delete browser-visits.log and/or browser-visits.db.

Usage:
    python reset.py           # reset both (with confirmation)
    python reset.py --log     # reset only the log file
    python reset.py --db      # reset only the database
    python reset.py -f        # skip confirmation prompt

The same BVL_LOG_FILE and BVL_DB_FILE environment variables used by host.py
are respected here, so custom paths work automatically.
"""

import argparse
import os
import sys

HOME     = os.path.expanduser('~')
LOG_FILE = os.environ.get('BVL_LOG_FILE', os.path.join(HOME, 'browser-visits.log'))
DB_FILE  = os.environ.get('BVL_DB_FILE',  os.path.join(HOME, 'browser-visits.db'))


def _delete(path: str, label: str) -> None:
    if os.path.exists(path):
        os.remove(path)
        print(f'Deleted {label}: {path}')
    else:
        print(f'{label} not found, skipping: {path}')


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Delete browser-visits log file and/or SQLite database.',
    )
    parser.add_argument('--log', action='store_true', help='reset only the log file')
    parser.add_argument('--db',  action='store_true', help='reset only the database')
    parser.add_argument('-f', '--force', action='store_true',
                        help='skip confirmation prompt')
    args = parser.parse_args()

    reset_both = not args.log and not args.db
    do_log = args.log or reset_both
    do_db  = args.db  or reset_both

    targets = []
    if do_log:
        targets.append((LOG_FILE, 'log file'))
    if do_db:
        targets.append((DB_FILE, 'database'))

    print('The following files will be permanently deleted:')
    for path, label in targets:
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
    for path, label in targets:
        _delete(path, label)


if __name__ == '__main__':
    main()
