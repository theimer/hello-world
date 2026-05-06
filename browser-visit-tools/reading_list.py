#!/usr/bin/env python3
"""
reading_list.py — Generate a Markdown reading list from the visit DB.

Selects every visit that's been marked **of_interest** but not yet
**read**, and writes them into ``~/Documents/browser-visit-logger/
reading_list.md`` as two clickable Markdown tables:

  1. *Skimmed* — pages with at least one skimmed event but no read
     events.  Sorted by most-recent skimmed timestamp.
  2. *To skim* — pages with no skimmed events and no read events.
     Sorted by most-recent first-visit timestamp.

Pages that have ever been read are excluded from both tables; the
reading list is "what's still ahead", not a history.

This tool is a read-only consumer of the database produced by the
sibling ``browser-visit-logger`` project.  It depends only on the
schema (visits + skimmed_events) — no Python imports cross the
directory boundary.

Usage
-----
    # Default — read ~/browser-visits.db, write to
    # ~/Documents/browser-visit-logger/reading_list.md
    python3 browser-visit-tools/reading_list.py

    # Override DB / output path
    python3 browser-visit-tools/reading_list.py --db /tmp/test.db
    python3 browser-visit-tools/reading_list.py --output /tmp/reading_list.md

The output file is overwritten on every run (it's derived state).
The parent directory is created if missing.
"""

import argparse
import datetime
import logging
import os
import sqlite3
import sys


HOME    = os.path.expanduser('~')
DB_FILE = os.environ.get('BVL_DB_FILE', os.path.join(HOME, 'browser-visits.db'))
DEFAULT_OUTPUT = os.path.join(
    HOME, 'Documents', 'browser-visit-logger', 'reading_list.md',
)


def _parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog='reading_list.py',
        description='Generate reading_list.md from of_interest-but-not-read '
                    'visits in the browser-visits database.',
    )
    p.add_argument('--db', metavar='FILE', default=DB_FILE,
                   help=f'visits database path (default {DB_FILE})')
    p.add_argument('--output', metavar='FILE', default=DEFAULT_OUTPUT,
                   help=f'output markdown path (default {DEFAULT_OUTPUT})')
    p.add_argument('-v', '--verbose', action='store_true',
                   help='enable DEBUG logging')
    return p.parse_args(argv)


# ---------------------------------------------------------------------------
# Markdown helpers
# ---------------------------------------------------------------------------

def _escape_cell(text: str) -> str:
    """Sanitise text for a Markdown table cell.

    Escapes ``|`` (column separator) and ``[``/``]`` (link syntax),
    and collapses tabs / CRs / newlines to spaces so a row stays on
    a single line.
    """
    text = text.replace('\r', ' ').replace('\n', ' ').replace('\t', ' ')
    text = text.replace('|', '\\|')
    text = text.replace('[', '\\[').replace(']', '\\]')
    return text.strip()


def _escape_url(url: str) -> str:
    """Sanitise a URL for use as a Markdown link target.

    Markdown links break on unbalanced parentheses; encode them as
    ``%28``/``%29``.  Spaces become ``%20``.  Other characters
    (including unicode) are left as-is — most renderers accept them.
    """
    return (
        url.replace('(', '%28')
           .replace(')', '%29')
           .replace(' ', '%20')
    )


def _format_link(title: str, url: str) -> str:
    label = _escape_cell(title) if title and title.strip() else _escape_cell(url)
    return f'[{label}]({_escape_url(url)})'


def _format_timestamp(iso: str) -> str:
    """Render an ISO-8601 UTC timestamp as 'YYYY-MM-DD HH:MM UTC'."""
    if not iso:
        return ''
    try:
        dt = datetime.datetime.fromisoformat(iso.replace('Z', '+00:00'))
    except ValueError:
        return iso
    return dt.strftime('%Y-%m-%d %H:%M UTC')


# ---------------------------------------------------------------------------
# DB query
# ---------------------------------------------------------------------------

# of_interest is non-NULL once tagged; read=0 means no read events have
# ever been recorded.  We join skimmed_events to pull the most-recent
# skimmed timestamp for the "skimmed" table; LEFT JOIN keeps unskimmed
# rows in the result with last_skimmed=NULL.
_QUERY = """
    SELECT v.url, v.title, v.timestamp, v.skimmed,
           MAX(s.timestamp) AS last_skimmed
      FROM visits v
      LEFT JOIN skimmed_events s ON s.url = v.url
     WHERE v.of_interest IS NOT NULL
       AND v.read = 0
     GROUP BY v.url
"""


def _fetch_rows(conn: sqlite3.Connection):
    """Return (skimmed, unskimmed) lists of dicts.

    skimmed:   rows with skimmed > 0, ordered by last_skimmed DESC
    unskimmed: rows with skimmed = 0, ordered by first-visit timestamp DESC
    """
    skimmed, unskimmed = [], []
    for url, title, ts, skim_count, last_skim in conn.execute(_QUERY):
        row = {
            'url': url, 'title': title, 'first_visited': ts,
            'last_skimmed': last_skim,
        }
        (skimmed if skim_count and skim_count > 0 else unskimmed).append(row)

    skimmed.sort(key=lambda r: r['last_skimmed'] or '', reverse=True)
    unskimmed.sort(key=lambda r: r['first_visited'] or '', reverse=True)
    return skimmed, unskimmed


# ---------------------------------------------------------------------------
# Markdown rendering
# ---------------------------------------------------------------------------

def _render(skimmed, unskimmed) -> str:
    lines = [
        '# Reading list',
        '',
        'Pages tagged **★ Of Interest** that have not been **✓ Read** yet.',
        'Generated by `reading_list.py`; rerun to refresh.',
        '',
        f'## Unread URLs that have been skimmed ({len(skimmed)})',
        '',
    ]
    if skimmed:
        lines.append('| Title | Last skimmed | First visited |')
        lines.append('|-------|--------------|---------------|')
        for r in skimmed:
            lines.append(
                f"| {_format_link(r['title'], r['url'])} "
                f"| {_format_timestamp(r['last_skimmed'])} "
                f"| {_format_timestamp(r['first_visited'])} |"
            )
    else:
        lines.append('_(none)_')
    lines.extend(['', f'## Unread URLs ({len(unskimmed)})', ''])
    if unskimmed:
        lines.append('| Title | First visited |')
        lines.append('|-------|---------------|')
        for r in unskimmed:
            lines.append(
                f"| {_format_link(r['title'], r['url'])} "
                f"| {_format_timestamp(r['first_visited'])} |"
            )
    else:
        lines.append('_(none)_')
    lines.append('')
    return '\n'.join(lines)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(argv=None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(message)s',
    )
    log = logging.getLogger('reading_list')

    if not os.path.exists(args.db):
        log.error('Database not found: %s', args.db)
        return 1

    with sqlite3.connect(args.db) as conn:
        skimmed, unskimmed = _fetch_rows(conn)

    output = _render(skimmed, unskimmed)

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, 'w', encoding='utf-8') as f:
        f.write(output)

    log.info('Wrote %d skimmed + %d unskimmed entries to %s',
             len(skimmed), len(unskimmed), args.output)
    return 0


if __name__ == '__main__':
    sys.exit(main())
