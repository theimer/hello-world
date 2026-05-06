"""Unit tests for browser-visit-tools/reading_list.py.

Covers the SQL filter (of_interest set, read=0, with/without skimmed),
the markdown rendering (link escaping, URL escaping, timestamp
formatting, empty-table fallback), and the CLI entry point
(missing DB, custom output path, parent-dir creation).

The tool is a standalone consumer of the visits DB — no
browser-visit-logger code is imported here.  The schema is created
inline (matching what BVLHost / host.py emit) so this directory's
tests are independent.
"""
import io
import os
import shutil
import sqlite3
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

import reading_list


# Schema mirror.  Matches what BVLHost (Swift) and host.py (Python)
# create.  Kept inline so this test file doesn't import from the
# logger project.
_SCHEMA = """
    CREATE TABLE visits (
        url         TEXT PRIMARY KEY,
        timestamp   TEXT NOT NULL,
        title       TEXT NOT NULL DEFAULT '',
        of_interest TEXT,
        read        INTEGER NOT NULL DEFAULT 0,
        skimmed     INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE skimmed_events (
        url       TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        filename  TEXT NOT NULL DEFAULT '',
        directory TEXT NOT NULL DEFAULT '',
        PRIMARY KEY (url, timestamp)
    );
"""


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(_SCHEMA)
    conn.commit()


def _seed(conn: sqlite3.Connection, rows):
    """Helper: rows is a list of dicts with url/title/of_interest/read/skimmed
    keys plus optional skim_events (list of timestamps) for that URL.
    """
    _ensure_schema(conn)
    for r in rows:
        conn.execute(
            "INSERT INTO visits (url, timestamp, title, of_interest, read, skimmed) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (r['url'], r.get('timestamp', '2026-01-01T00:00:00Z'),
             r.get('title', ''), r.get('of_interest'),
             r.get('read', 0), r.get('skimmed', 0)),
        )
        for ts in r.get('skim_events', []):
            conn.execute(
                "INSERT INTO skimmed_events (url, timestamp, filename, directory) "
                "VALUES (?, ?, '', '')",
                (r['url'], ts),
            )
    conn.commit()


class _ReadingListTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.db = os.path.join(self.tmp, 'visits.db')
        self.out = os.path.join(self.tmp, 'sub', 'reading_list.md')
        self.conn = sqlite3.connect(self.db)

    def tearDown(self):
        self.conn.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def run_cli(self, *extra):
        argv = ['--db', self.db, '--output', self.out, *extra]
        return reading_list.main(argv)

    def output(self) -> str:
        with open(self.out, encoding='utf-8') as f:
            return f.read()


# ---------------------------------------------------------------------------
# SQL filter — what shows up in which table
# ---------------------------------------------------------------------------

class TestFilter(_ReadingListTestBase):

    def test_not_of_interest_excluded(self):
        _seed(self.conn, [{'url': 'https://a/', 'of_interest': None, 'read': 0, 'skimmed': 0}])
        self.run_cli()
        self.assertNotIn('https://a/', self.output())

    def test_of_interest_and_read_excluded(self):
        _seed(self.conn, [{
            'url': 'https://a/', 'of_interest': '1', 'read': 1, 'skimmed': 0,
        }])
        self.run_cli()
        self.assertNotIn('https://a/', self.output())

    def test_of_interest_only_in_to_skim_table(self):
        _seed(self.conn, [{
            'url': 'https://a/', 'title': 'Page A',
            'of_interest': '1', 'read': 0, 'skimmed': 0,
        }])
        self.run_cli()
        out = self.output()
        # Heading + counts: 0 skimmed, 1 unskimmed
        self.assertIn('## Skimmed (0)', out)
        self.assertIn('## To skim (1)', out)
        # Skimmed section should be empty
        skimmed_section, to_skim_section = out.split('## To skim')
        self.assertNotIn('https://a/', skimmed_section)
        self.assertIn('[Page A](https://a/)', to_skim_section)

    def test_of_interest_skimmed_only_in_skimmed_table(self):
        _seed(self.conn, [{
            'url': 'https://b/', 'title': 'Page B',
            'of_interest': '1', 'read': 0, 'skimmed': 2,
            'skim_events': ['2026-04-30T10:00:00Z', '2026-05-01T10:00:00Z'],
        }])
        self.run_cli()
        out = self.output()
        self.assertIn('## Skimmed (1)', out)
        self.assertIn('## To skim (0)', out)
        skimmed_section, to_skim_section = out.split('## To skim')
        self.assertIn('[Page B](https://b/)', skimmed_section)
        self.assertNotIn('https://b/', to_skim_section)

    def test_skimmed_table_uses_max_skim_timestamp(self):
        _seed(self.conn, [{
            'url': 'https://b/', 'of_interest': '1', 'read': 0, 'skimmed': 2,
            'skim_events': ['2026-04-30T10:00:00Z', '2026-05-01T11:00:00Z'],
        }])
        self.run_cli()
        out = self.output()
        # The newer of the two skim timestamps shows up
        self.assertIn('2026-05-01 11:00 UTC', out)
        self.assertNotIn('2026-04-30 10:00 UTC', out)


# ---------------------------------------------------------------------------
# Sort order
# ---------------------------------------------------------------------------

class TestSorting(_ReadingListTestBase):

    def test_skimmed_sorted_by_last_skim_desc(self):
        _seed(self.conn, [
            {'url': 'https://old/',  'of_interest': '1', 'read': 0, 'skimmed': 1,
             'skim_events': ['2026-04-01T00:00:00Z']},
            {'url': 'https://new/',  'of_interest': '1', 'read': 0, 'skimmed': 1,
             'skim_events': ['2026-05-01T00:00:00Z']},
        ])
        self.run_cli()
        out = self.output()
        self.assertLess(out.index('https://new/'), out.index('https://old/'))

    def test_to_skim_sorted_by_first_visit_desc(self):
        _seed(self.conn, [
            {'url': 'https://old/', 'timestamp': '2026-04-01T00:00:00Z',
             'of_interest': '1', 'read': 0, 'skimmed': 0},
            {'url': 'https://new/', 'timestamp': '2026-05-01T00:00:00Z',
             'of_interest': '1', 'read': 0, 'skimmed': 0},
        ])
        self.run_cli()
        out = self.output()
        self.assertLess(out.index('https://new/'), out.index('https://old/'))


# ---------------------------------------------------------------------------
# Markdown rendering details
# ---------------------------------------------------------------------------

class TestRendering(_ReadingListTestBase):

    def test_empty_db_writes_both_table_placeholders(self):
        _ensure_schema(self.conn)
        self.run_cli()
        out = self.output()
        self.assertIn('## Skimmed (0)', out)
        self.assertIn('## To skim (0)', out)
        # Both sections should have the empty marker, not a table header
        self.assertEqual(out.count('_(none)_'), 2)
        self.assertNotIn('| Title |', out)

    def test_empty_title_uses_url_as_link_label(self):
        _seed(self.conn, [{
            'url': 'https://a/', 'title': '',
            'of_interest': '1', 'read': 0, 'skimmed': 0,
        }])
        self.run_cli()
        self.assertIn('[https://a/](https://a/)', self.output())

    def test_whitespace_only_title_uses_url(self):
        _seed(self.conn, [{
            'url': 'https://a/', 'title': '   ',
            'of_interest': '1', 'read': 0, 'skimmed': 0,
        }])
        self.run_cli()
        self.assertIn('[https://a/](https://a/)', self.output())

    def test_pipe_in_title_is_escaped(self):
        _seed(self.conn, [{
            'url': 'https://a/', 'title': 'Foo | Bar',
            'of_interest': '1', 'read': 0, 'skimmed': 0,
        }])
        self.run_cli()
        out = self.output()
        # Escaped pipe in the link label.
        self.assertIn('[Foo \\| Bar](https://a/)', out)

    def test_brackets_in_title_are_escaped(self):
        _seed(self.conn, [{
            'url': 'https://a/', 'title': 'Foo [bar]',
            'of_interest': '1', 'read': 0, 'skimmed': 0,
        }])
        self.run_cli()
        self.assertIn('Foo \\[bar\\]', self.output())

    def test_newline_in_title_collapses_to_space(self):
        _seed(self.conn, [{
            'url': 'https://a/', 'title': 'Foo\nBar',
            'of_interest': '1', 'read': 0, 'skimmed': 0,
        }])
        self.run_cli()
        out = self.output()
        # The whole row stays on one line, with the newline replaced.
        self.assertIn('Foo Bar', out)
        rows = [ln for ln in out.splitlines() if 'https://a/' in ln]
        self.assertEqual(len(rows), 1)

    def test_parens_in_url_are_percent_encoded(self):
        _seed(self.conn, [{
            'url': 'https://a/(weird)', 'title': 'Page',
            'of_interest': '1', 'read': 0, 'skimmed': 0,
        }])
        self.run_cli()
        # Otherwise Markdown's link parser would close at the first ).
        self.assertIn('(https://a/%28weird%29)', self.output())

    def test_space_in_url_is_percent_encoded(self):
        _seed(self.conn, [{
            'url': 'https://a/with space', 'title': 'Page',
            'of_interest': '1', 'read': 0, 'skimmed': 0,
        }])
        self.run_cli()
        self.assertIn('(https://a/with%20space)', self.output())

    def test_invalid_iso_timestamp_passed_through_verbatim(self):
        _seed(self.conn, [{
            'url': 'https://a/', 'timestamp': 'not-a-date',
            'of_interest': '1', 'read': 0, 'skimmed': 0,
        }])
        self.run_cli()
        # Falls back to the raw string rather than crashing.
        self.assertIn('not-a-date', self.output())

    def test_iso_timestamp_formatted_as_human_readable(self):
        _seed(self.conn, [{
            'url': 'https://a/', 'timestamp': '2026-04-30T14:35:22Z',
            'of_interest': '1', 'read': 0, 'skimmed': 0,
        }])
        self.run_cli()
        self.assertIn('2026-04-30 14:35 UTC', self.output())

    def test_format_timestamp_returns_empty_on_falsy_input(self):
        # The skimmed table renders r['last_skimmed'] which is None for the
        # degenerate case skimmed>0 yet no skimmed_events row exists — and
        # also for empty-string defensiveness.
        self.assertEqual(reading_list._format_timestamp(''), '')
        self.assertEqual(reading_list._format_timestamp(None), '')


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

class TestCli(_ReadingListTestBase):

    def test_missing_db_returns_exit_code_1(self):
        rc = reading_list.main(
            ['--db', os.path.join(self.tmp, 'nope.db'), '--output', self.out],
        )
        self.assertEqual(rc, 1)
        self.assertFalse(os.path.exists(self.out))

    def test_creates_parent_directory_for_output(self):
        _ensure_schema(self.conn)
        nested = os.path.join(self.tmp, 'a', 'b', 'c', 'reading_list.md')
        rc = reading_list.main(['--db', self.db, '--output', nested])
        self.assertEqual(rc, 0)
        self.assertTrue(os.path.exists(nested))

    def test_overwrites_existing_output(self):
        _ensure_schema(self.conn)
        os.makedirs(os.path.dirname(self.out), exist_ok=True)
        with open(self.out, 'w') as f:
            f.write('STALE CONTENTS')
        self.run_cli()
        self.assertNotIn('STALE', self.output())

    def test_verbose_flag_does_not_crash(self):
        _ensure_schema(self.conn)
        rc = self.run_cli('-v')
        self.assertEqual(rc, 0)

    def test_main_runs_when_invoked_as_script(self):
        # Triggers the `if __name__ == '__main__'` line so coverage hits
        # the entry-point shim at the bottom of the file.  run_path
        # propagates the script's `sys.exit(main())` as SystemExit; a
        # clean run is exit code 0.
        _ensure_schema(self.conn)
        with patch.object(sys, 'argv',
                          ['reading_list.py', '--db', self.db, '--output', self.out]), \
             redirect_stdout(io.StringIO()):
            from runpy import run_path
            with self.assertRaises(SystemExit) as cm:
                run_path(reading_list.__file__, run_name='__main__')
        self.assertEqual(cm.exception.code, 0)
        self.assertTrue(os.path.exists(self.out))


if __name__ == '__main__':
    unittest.main()
