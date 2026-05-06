#!/usr/bin/env python3
"""
reading_list.py — Generate a reading list from the visit DB.

Selects every visit that's been marked **of_interest** but not yet
**read**, and writes them into ``~/Documents/browser-visit-logger/
reading_list.html`` (default) or ``reading_list.md`` (with
``--format markdown``) as two clickable tables:

  1. *Unread URLs that have been skimmed* — pages with at least one
     skimmed event but no read events.
  2. *Unread URLs* — pages with no skimmed events and no read events.

Both tables sort by first-visit timestamp, most recent first.  Pages
that have ever been read are excluded; the reading list is "what's
still ahead", not a history.

This tool is a read-only consumer of the database produced by the
sibling ``browser-visit-logger`` project.  It depends only on the
schema (visits + skimmed_events) — no Python imports cross the
directory boundary.

Usage
-----
    # Default — HTML output to ~/Documents/browser-visit-logger/reading_list.html
    python3 browser-visit-tools/reading_list.py

    # Markdown instead
    python3 browser-visit-tools/reading_list.py --format markdown

    # Override DB / output path
    python3 browser-visit-tools/reading_list.py --db /tmp/test.db
    python3 browser-visit-tools/reading_list.py --output /tmp/reading_list.html

The output file is overwritten on every run (it's derived state).
The parent directory is created if missing.
"""

import argparse
import datetime
import html
import logging
import os
import sqlite3
import sys


HOME    = os.path.expanduser('~')
DB_FILE = os.environ.get('BVL_DB_FILE', os.path.join(HOME, 'browser-visits.db'))
_OUTPUT_DIR = os.path.join(HOME, 'Documents', 'browser-visit-logger')
_FORMATS = ('html', 'markdown')
_DEFAULT_FORMAT = 'html'
_EXTENSIONS = {'html': 'html', 'markdown': 'md'}


def _default_output_for(fmt: str) -> str:
    return os.path.join(_OUTPUT_DIR, f'reading_list.{_EXTENSIONS[fmt]}')


def _parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog='reading_list.py',
        description='Generate a reading list (HTML by default, Markdown on '
                    'request) of of_interest-but-not-read visits in the '
                    'browser-visits database.',
    )
    p.add_argument('--db', metavar='FILE', default=DB_FILE,
                   help=f'visits database path (default {DB_FILE})')
    p.add_argument('--format', dest='format', choices=_FORMATS,
                   default=_DEFAULT_FORMAT,
                   help=f'output format (default {_DEFAULT_FORMAT})')
    p.add_argument('--output', metavar='FILE', default=None,
                   help='output path (default '
                        f'{_default_output_for(_DEFAULT_FORMAT)} for html, '
                        f'{_default_output_for("markdown")} for markdown)')
    p.add_argument('-v', '--verbose', action='store_true',
                   help='enable DEBUG logging')
    args = p.parse_args(argv)
    if args.output is None:
        args.output = _default_output_for(args.format)
    return args


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
    """Render an ISO-8601 UTC timestamp in local time as
    'YYYY-MM-DD HH:MM <ZONE>' (e.g. '2026-04-30 07:35 PDT').

    Stored timestamps are always UTC ('...Z'); the rebuilder and host
    only ever write that format.  We parse, attach UTC, then convert
    to the user's local zone via ``astimezone()`` (no argument =
    system local zone).
    """
    if not iso:
        return ''
    try:
        dt = datetime.datetime.fromisoformat(iso.replace('Z', '+00:00'))
    except ValueError:
        return iso
    return dt.astimezone().strftime('%Y-%m-%d %H:%M %Z')


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

    Both lists are sorted by first-visit timestamp, most recent first.
    """
    skimmed, unskimmed = [], []
    for url, title, ts, skim_count, last_skim in conn.execute(_QUERY):
        row = {
            'url': url, 'title': title, 'first_visited': ts,
            'last_skimmed': last_skim,
        }
        (skimmed if skim_count and skim_count > 0 else unskimmed).append(row)

    by_first_visit = lambda r: r['first_visited'] or ''
    skimmed.sort(key=by_first_visit, reverse=True)
    unskimmed.sort(key=by_first_visit, reverse=True)
    return skimmed, unskimmed


# ---------------------------------------------------------------------------
# Markdown rendering
# ---------------------------------------------------------------------------

_SKIMMED_HEADING   = 'Unread URLs that have been skimmed'
_UNSKIMMED_HEADING = 'Unread URLs'
_INTRO = ('Pages tagged ★ Of Interest that have not been ✓ Read yet.  '
          'Generated by reading_list.py; rerun to refresh.')


def _render_markdown(skimmed, unskimmed) -> str:
    lines = [
        '# Reading list',
        '',
        'Pages tagged **★ Of Interest** that have not been **✓ Read** yet.',
        'Generated by `reading_list.py`; rerun to refresh.',
        '',
        f'## {_SKIMMED_HEADING} ({len(skimmed)})',
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
    lines.extend(['', f'## {_UNSKIMMED_HEADING} ({len(unskimmed)})', ''])
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
# HTML rendering — standalone document with minimal inline CSS so the file
# renders cleanly in a browser without external dependencies.
# ---------------------------------------------------------------------------

_HTML_STYLE = """\
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
       max-width: 1100px; margin: 2em auto; padding: 0 1em; line-height: 1.5;
       color: #1a1a1a; }
h1 { border-bottom: 2px solid #ccc; padding-bottom: 0.3em; }
h2 { color: #333; margin-top: 2em; }
table { border-collapse: collapse; width: 100%; margin-top: 0.5em; }
th, td { border: 1px solid #ddd; padding: 0.5em 0.75em; text-align: left;
         vertical-align: top; }
th { background: #f4f4f4; }
tr:nth-child(even) td { background: #fafafa; }
a { color: #0366d6; text-decoration: none; word-break: break-word; }
a:hover { text-decoration: underline; }
.empty { color: #888; font-style: italic; }
.intro { color: #555; }
.timestamp { white-space: nowrap; color: #555; }
"""


def _format_link_html(title: str, url: str) -> str:
    label = title.strip() if title and title.strip() else url
    # html.escape with quote=True covers <, >, &, ", ' for both attribute
    # and text contexts.  Spaces in href are tolerated by browsers but we
    # percent-encode them anyway for strict-validator friendliness.
    href = html.escape(url.replace(' ', '%20'), quote=True)
    return f'<a href="{href}">{html.escape(label)}</a>'


def _row_html(*cells: str) -> str:
    return '<tr>' + ''.join(f'<td>{c}</td>' for c in cells) + '</tr>'


def _ts_cell(iso: str) -> str:
    formatted = html.escape(_format_timestamp(iso))
    return f'<span class="timestamp">{formatted}</span>'


def _render_html(skimmed, unskimmed) -> str:
    parts = [
        '<!DOCTYPE html>',
        '<html lang="en">',
        '<head>',
        '<meta charset="utf-8">',
        '<title>Reading list</title>',
        f'<style>{_HTML_STYLE}</style>',
        '</head>',
        '<body>',
        '<h1>Reading list</h1>',
        f'<p class="intro">{html.escape(_INTRO)}</p>',
        f'<h2>{html.escape(_SKIMMED_HEADING)} ({len(skimmed)})</h2>',
    ]
    if skimmed:
        parts.append('<table>')
        parts.append('<thead>'
                     '<tr><th>Title</th><th>Last skimmed</th><th>First visited</th></tr>'
                     '</thead><tbody>')
        for r in skimmed:
            parts.append(_row_html(
                _format_link_html(r['title'], r['url']),
                _ts_cell(r['last_skimmed']),
                _ts_cell(r['first_visited']),
            ))
        parts.append('</tbody></table>')
    else:
        parts.append('<p class="empty">(none)</p>')

    parts.append(f'<h2>{html.escape(_UNSKIMMED_HEADING)} ({len(unskimmed)})</h2>')
    if unskimmed:
        parts.append('<table>')
        parts.append('<thead>'
                     '<tr><th>Title</th><th>First visited</th></tr>'
                     '</thead><tbody>')
        for r in unskimmed:
            parts.append(_row_html(
                _format_link_html(r['title'], r['url']),
                _ts_cell(r['first_visited']),
            ))
        parts.append('</tbody></table>')
    else:
        parts.append('<p class="empty">(none)</p>')

    parts.extend(['</body>', '</html>', ''])
    return '\n'.join(parts)


_RENDERERS = {
    'html':     _render_html,
    'markdown': _render_markdown,
}


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

    output = _RENDERERS[args.format](skimmed, unskimmed)

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, 'w', encoding='utf-8') as f:
        f.write(output)

    log.info('Wrote %d skimmed + %d unskimmed entries to %s',
             len(skimmed), len(unskimmed), args.output)
    return 0


if __name__ == '__main__':
    sys.exit(main())
