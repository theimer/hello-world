#!/usr/bin/env python3
"""
Point the native host manifest at a worktree's host.py for pre-merge testing.

Looks in .claude/worktrees/ relative to the project root:
  - If exactly one worktree exists, uses it automatically.
  - If multiple exist, lists them and prompts for a choice.

After running, fully quit and restart Chrome for the change to take effect.
To revert, run install.sh from the project root.
"""

import json
import os
import sys

MANIFEST = os.path.expanduser(
    '~/Library/Application Support/Google/Chrome/NativeMessagingHosts/com.browser.visit.logger.json'
)

SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
WORKTREES_DIR = os.path.join(PROJECT_ROOT, '.claude', 'worktrees')


def get_worktrees():
    if not os.path.isdir(WORKTREES_DIR):
        return []
    return sorted(
        d for d in os.listdir(WORKTREES_DIR)
        if os.path.isdir(os.path.join(WORKTREES_DIR, d))
    )


def main():
    worktrees = get_worktrees()

    if not worktrees:
        print(f'No worktrees found in {WORKTREES_DIR}')
        sys.exit(1)

    if len(worktrees) == 1:
        chosen = worktrees[0]
        print(f'Using worktree: {chosen}')
    else:
        print('Multiple worktrees found:')
        for i, w in enumerate(worktrees, 1):
            print(f'  {i}. {w}')
        try:
            answer = input(f'\nWhich one? [1-{len(worktrees)}] ').strip()
            idx = int(answer) - 1
            if not 0 <= idx < len(worktrees):
                raise ValueError
            chosen = worktrees[idx]
        except (ValueError,):
            print('Invalid selection. Aborted.')
            sys.exit(1)
        except (EOFError, KeyboardInterrupt):
            print('\nAborted.')
            sys.exit(0)

    host_py = os.path.join(WORKTREES_DIR, chosen, 'browser-visit-logger', 'native-host', 'host.py')

    if not os.path.exists(host_py):
        print(f'Error: host.py not found at {host_py}')
        sys.exit(1)

    with open(MANIFEST) as f:
        d = json.load(f)
    d['path'] = host_py
    with open(MANIFEST, 'w') as f:
        json.dump(d, f, indent=2)
        f.write('\n')

    print(f'Manifest now points to:\n  {host_py}')
    print('Fully quit and restart Chrome for the change to take effect.')


if __name__ == '__main__':
    main()
