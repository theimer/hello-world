"""
Unit and integration tests for native-host/host.py.

Run with:
    cd browser-visit-logger
    pytest tests/test_host.py -v

Or the full suite:
    pytest tests/ -v
"""
import contextlib
import io
import json
import os
import sqlite3
import struct
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import host  # resolved via conftest.py sys.path insertion


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _frame(data: dict) -> bytes:
    """Encode a dict as a native-messaging framed message (4-byte LE len + JSON)."""
    encoded = json.dumps(data).encode('utf-8')
    return struct.pack('<I', len(encoded)) + encoded


def _unframe(raw: bytes) -> dict:
    """Decode a native-messaging framed response back to a dict."""
    length = struct.unpack('<I', raw[:4])[0]
    return json.loads(raw[4:4 + length])


HOST_PY = str(Path(__file__).parent.parent / 'native-host' / 'host.py')


# ---------------------------------------------------------------------------
# Message framing
# ---------------------------------------------------------------------------

class TestReadMessage(unittest.TestCase):

    def _read(self, data: dict) -> dict:
        fake_stdin = MagicMock()
        fake_stdin.buffer = io.BytesIO(_frame(data))
        with patch('sys.stdin', fake_stdin):
            return host.read_message()

    def test_basic_round_trip(self):
        payload = {'timestamp': 'ts', 'url': 'https://example.com', 'title': 'Example'}
        self.assertEqual(self._read(payload), payload)

    def test_unicode_content(self):
        payload = {'title': '日本語タイトル — Ünïcödé'}
        result = self._read(payload)
        self.assertEqual(result['title'], payload['title'])

    def test_eof_before_header_raises(self):
        fake_stdin = MagicMock()
        fake_stdin.buffer = io.BytesIO(b'')
        with patch('sys.stdin', fake_stdin):
            with self.assertRaises(EOFError):
                host.read_message()

    def test_eof_mid_message_raises(self):
        payload = json.dumps({'url': 'x'}).encode('utf-8')
        truncated = struct.pack('<I', len(payload)) + payload[:2]
        fake_stdin = MagicMock()
        fake_stdin.buffer = io.BytesIO(truncated)
        with patch('sys.stdin', fake_stdin):
            with self.assertRaises(EOFError):
                host.read_message()


class TestWriteMessage(unittest.TestCase):

    def _write(self, data: dict) -> dict:
        buf = io.BytesIO()
        mock_stdout = MagicMock()
        mock_stdout.buffer = buf
        with patch('sys.stdout', mock_stdout):
            host.write_message(data)
        buf.seek(0)
        return _unframe(buf.read())

    def test_basic_status_ok(self):
        self.assertEqual(self._write({'status': 'ok'}), {'status': 'ok'})

    def test_unicode_preserved(self):
        data = {'title': '日本語', 'url': 'https://example.com'}
        self.assertEqual(self._write(data), data)

    def test_round_trip_complex(self):
        data = {'status': 'error', 'errors': ['log: permission denied', 'db: disk full']}
        self.assertEqual(self._write(data), data)


# ---------------------------------------------------------------------------
# Log file (append_log)
# ---------------------------------------------------------------------------

class TestAppendLog(unittest.TestCase):

    def _run(self, timestamp, url, title, tag='') -> str:
        """Call append_log with a temp file and return its contents."""
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'visits.log')
            with patch.object(host, 'LOG_FILE', path):
                host.append_log(timestamp, url, title, tag)
            return Path(path).read_text(encoding='utf-8')

    def test_tsv_format(self):
        content = self._run('2026-01-01T00:00:00Z', 'https://example.com', 'Example Domain')
        self.assertEqual(content, '2026-01-01T00:00:00Z\thttps://example.com\tExample Domain\n')

    def test_tab_in_title_replaced(self):
        content = self._run('ts', 'https://a.com', 'Part1\tPart2').rstrip('\n')
        parts = content.split('\t')
        self.assertEqual(parts[2], 'Part1 Part2')

    def test_newline_in_title_replaced(self):
        content = self._run('ts', 'https://a.com', 'Line1\nLine2')
        self.assertEqual(len(content.splitlines()), 1)
        self.assertIn('Line1 Line2', content)

    def test_carriage_return_stripped(self):
        # \r is removed entirely (not replaced with a space), collapsing the surrounding text
        content = self._run('ts', 'https://a.com', 'Title\rStuff').rstrip('\n')
        parts = content.split('\t')
        self.assertEqual(parts[2], 'TitleStuff')

    def test_unicode_preserved(self):
        content = self._run('ts', 'https://a.com', '日本語タイトル')
        self.assertIn('日本語タイトル', content)

    def test_appends_multiple_calls(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'visits.log')
            with patch.object(host, 'LOG_FILE', path):
                host.append_log('ts1', 'https://a.com', 'A')
                host.append_log('ts2', 'https://b.com', 'B')
            lines = Path(path).read_text().splitlines()
        self.assertEqual(len(lines), 2)
        self.assertIn('https://a.com', lines[0])
        self.assertIn('https://b.com', lines[1])

    def test_tag_produces_four_fields(self):
        content = self._run('ts', 'https://example.com', 'Example', tag='of_interest')
        parts = content.rstrip('\n').split('\t')
        self.assertEqual(len(parts), 4)
        self.assertEqual(parts[3], 'of_interest')

    def test_append_result_log_writes_single_field(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'visits.log')
            with patch.object(host, 'LOG_FILE', path):
                host.append_result_log('success')
            content = Path(path).read_text(encoding='utf-8')
        self.assertEqual(content, 'success\n')

    def test_append_result_log_sanitises_tabs(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'visits.log')
            with patch.object(host, 'LOG_FILE', path):
                host.append_result_log('error: foo\tbar')
            content = Path(path).read_text(encoding='utf-8').rstrip('\n')
        self.assertEqual(content, 'error: foo bar')

    def test_no_tag_produces_three_fields(self):
        content = self._run('ts', 'https://example.com', 'Example')
        parts = content.rstrip('\n').split('\t')
        self.assertEqual(len(parts), 3)

    def test_tag_sanitised(self):
        content = self._run('ts', 'https://example.com', 'Example', tag='mem\torable')
        parts = content.rstrip('\n').split('\t')
        self.assertEqual(parts[3], 'mem orable')


# ---------------------------------------------------------------------------
# Database (ensure_db / insert_visit)
# ---------------------------------------------------------------------------

class TestDatabase(unittest.TestCase):

    def _conn(self):
        conn = sqlite3.connect(':memory:')
        host.ensure_db(conn)
        return conn

    def _cols(self, conn):
        return {r[1] for r in conn.execute('PRAGMA table_info(visits)')}

    def _tables(self, conn):
        return {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}

    def test_ensure_db_creates_visits_table(self):
        conn = self._conn()
        self.assertIn('visits', self._tables(conn))
        conn.close()

    def test_ensure_db_creates_read_events_table(self):
        conn = self._conn()
        self.assertIn('read_events', self._tables(conn))
        conn.close()

    def test_ensure_db_migrates_legacy_read_column_to_read_events(self):
        # Simulate a database that has a non-NULL visits.read value
        conn = sqlite3.connect(':memory:')
        conn.execute("""
            CREATE TABLE visits (
                url         TEXT PRIMARY KEY,
                timestamp   TEXT NOT NULL,
                title       TEXT NOT NULL DEFAULT '',
                of_interest TEXT,
                read        TEXT,
                skimmed     TEXT
            )
        """)
        conn.execute(
            "INSERT INTO visits (url, timestamp, title, read) VALUES (?, ?, ?, ?)",
            ('https://example.com', 'ts-visit', 'Example', 'ts-read'),
        )
        conn.commit()
        host.ensure_db(conn)
        # Value migrated into read_events
        row = conn.execute(
            "SELECT timestamp FROM read_events WHERE url = ?",
            ('https://example.com',)
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], 'ts-read')
        # Old visits.read column nulled out
        old = conn.execute("SELECT read FROM visits").fetchone()
        conn.close()
        self.assertIsNone(old[0])

    def test_ensure_db_creates_timestamp_index(self):
        conn = self._conn()
        indexes = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        )}
        self.assertIn('idx_visits_timestamp', indexes)
        conn.close()

    def test_ensure_db_creates_of_interest_column(self):
        conn = self._conn()
        self.assertIn('of_interest', self._cols(conn))
        conn.close()

    def test_ensure_db_creates_read_column(self):
        conn = self._conn()
        self.assertIn('read', self._cols(conn))
        conn.close()

    def test_ensure_db_creates_skimmed_column(self):
        conn = self._conn()
        self.assertIn('skimmed', self._cols(conn))
        conn.close()

    def test_ensure_db_migrates_memorable_to_of_interest(self):
        # Simulate a database using the old 'memorable' column name
        conn = sqlite3.connect(':memory:')
        conn.execute("""
            CREATE TABLE visits (
                url       TEXT PRIMARY KEY,
                timestamp TEXT NOT NULL,
                title     TEXT NOT NULL DEFAULT '',
                memorable TEXT,
                read      TEXT,
                skimmed   TEXT
            )
        """)
        conn.execute(
            "INSERT INTO visits (url, timestamp, title, memorable) VALUES (?, ?, ?, ?)",
            ('https://example.com', 'ts1', 'Example', 'ts-mem'),
        )
        conn.commit()
        host.ensure_db(conn)
        cols = self._cols(conn)
        self.assertIn('of_interest', cols)
        self.assertNotIn('memorable', cols)
        # Existing value was carried over from memorable → of_interest
        row = conn.execute('SELECT of_interest FROM visits').fetchone()
        conn.close()
        self.assertEqual(row[0], 'ts-mem')

    def test_ensure_db_migrates_memorable_without_skimmed(self):
        # Simulate an even older schema: memorable + read but no skimmed
        conn = sqlite3.connect(':memory:')
        conn.execute("""
            CREATE TABLE visits (
                url       TEXT PRIMARY KEY,
                timestamp TEXT NOT NULL,
                title     TEXT NOT NULL DEFAULT '',
                memorable TEXT,
                read      TEXT
            )
        """)
        conn.commit()
        host.ensure_db(conn)
        cols = self._cols(conn)
        self.assertIn('of_interest', cols)
        self.assertIn('skimmed', cols)
        self.assertNotIn('memorable', cols)
        conn.close()

    def test_ensure_db_url_is_primary_key(self):
        conn = self._conn()
        pk_cols = {r[1] for r in conn.execute('PRAGMA table_info(visits)') if r[5] == 1}
        conn.close()
        self.assertEqual(pk_cols, {'url'})

    def test_ensure_db_adds_skimmed_to_url_pk_schema_missing_it(self):
        # Simulate a url-PK schema that predates the skimmed column
        conn = sqlite3.connect(':memory:')
        conn.execute("""
            CREATE TABLE visits (
                url         TEXT PRIMARY KEY,
                timestamp   TEXT NOT NULL,
                title       TEXT NOT NULL DEFAULT '',
                of_interest TEXT,
                read        TEXT
            )
        """)
        conn.commit()
        host.ensure_db(conn)
        cols = self._cols(conn)
        conn.close()
        self.assertIn('skimmed', cols)

    def test_ensure_db_is_idempotent(self):
        conn = sqlite3.connect(':memory:')
        host.ensure_db(conn)
        host.ensure_db(conn)  # second call must not raise or corrupt the schema
        cols = self._cols(conn)
        tables = self._tables(conn)
        conn.close()
        self.assertIn('url', cols)
        self.assertIn('of_interest', cols)
        self.assertIn('read', cols)
        self.assertIn('skimmed', cols)
        self.assertIn('read_events', tables)

    def test_ensure_db_migrates_old_id_based_schema(self):
        # Simulate a database created before the url-PK redesign
        conn = sqlite3.connect(':memory:')
        conn.execute("""
            CREATE TABLE visits (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                url       TEXT NOT NULL,
                title     TEXT NOT NULL DEFAULT '',
                tag       TEXT NOT NULL DEFAULT ''
            )
        """)
        conn.execute(
            "INSERT INTO visits (timestamp, url, title) VALUES (?, ?, ?)",
            ('ts1', 'https://example.com', 'Example'),
        )
        conn.commit()
        host.ensure_db(conn)
        # Old columns gone, new schema in place
        cols = self._cols(conn)
        self.assertNotIn('id', cols)
        self.assertNotIn('tag', cols)
        self.assertIn('of_interest', cols)
        self.assertIn('read', cols)
        # Existing data preserved
        row = conn.execute('SELECT url, timestamp, title FROM visits').fetchone()
        conn.close()
        self.assertEqual(row, ('https://example.com', 'ts1', 'Example'))

    def test_insert_visit_stores_all_fields(self):
        conn = self._conn()
        host.insert_visit(conn, '2026-01-01T00:00:00Z', 'https://example.com', 'Example')
        row = conn.execute('SELECT url, timestamp, title FROM visits').fetchone()
        conn.close()
        self.assertEqual(row, ('https://example.com', '2026-01-01T00:00:00Z', 'Example'))

    def test_insert_visit_of_interest_and_read_default_to_null(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Title')
        row = conn.execute('SELECT of_interest, read, skimmed FROM visits').fetchone()
        conn.close()
        self.assertIsNone(row[0])
        self.assertIsNone(row[1])
        self.assertIsNone(row[2])

    def test_insert_visit_empty_title(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', '')
        row = conn.execute('SELECT title FROM visits').fetchone()
        conn.close()
        self.assertEqual(row[0], '')

    def test_insert_visit_duplicate_url_ignored(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts1', 'https://example.com', 'First')
        host.insert_visit(conn, 'ts2', 'https://example.com', 'Second')
        count = conn.execute('SELECT COUNT(*) FROM visits').fetchone()[0]
        conn.close()
        self.assertEqual(count, 1)

    def test_insert_visit_duplicate_preserves_original_timestamp(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts1', 'https://example.com', 'First')
        host.insert_visit(conn, 'ts2', 'https://example.com', 'Second')
        row = conn.execute('SELECT timestamp, title FROM visits').fetchone()
        conn.close()
        self.assertEqual(row, ('ts1', 'First'))

    def test_insert_visit_different_urls_create_separate_records(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts1', 'https://a.com', 'A')
        host.insert_visit(conn, 'ts2', 'https://b.com', 'B')
        rows = conn.execute('SELECT url FROM visits ORDER BY url').fetchall()
        conn.close()
        self.assertEqual(rows, [('https://a.com',), ('https://b.com',)])

    def test_insert_visit_unicode(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', '日本語タイトル')
        row = conn.execute('SELECT title FROM visits').fetchone()
        conn.close()
        self.assertEqual(row[0], '日本語タイトル')


# ---------------------------------------------------------------------------
# tag_visit
# ---------------------------------------------------------------------------

class TestTagVisit(unittest.TestCase):

    def _conn(self):
        conn = sqlite3.connect(':memory:')
        host.ensure_db(conn)
        return conn

    def test_tag_visit_sets_of_interest(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'of_interest', '2026-01-01T12:00:00Z')
        row = conn.execute('SELECT of_interest FROM visits').fetchone()
        conn.close()
        # SQLite TEXT affinity coerces the integer literal 1 to the string '1'
        self.assertEqual(row[0], '1')

    def test_tag_visit_inserts_read_event(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'read', '2026-01-01T12:00:00Z')
        row = conn.execute(
            "SELECT timestamp FROM read_events WHERE url = ?", ('https://example.com',)
        ).fetchone()
        conn.close()
        self.assertEqual(row[0], '2026-01-01T12:00:00Z')

    def test_tag_visit_read_does_not_update_visits_read_column(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'read', '2026-01-01T12:00:00Z')
        row = conn.execute('SELECT read FROM visits').fetchone()
        conn.close()
        self.assertIsNone(row[0])

    def test_tag_visit_read_twice_stores_both_timestamps(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'read', '2026-01-01T12:00:00Z')
        host.tag_visit(conn, 'https://example.com', 'read', '2026-01-02T12:00:00Z')
        rows = conn.execute(
            "SELECT timestamp FROM read_events WHERE url = ? ORDER BY timestamp ASC",
            ('https://example.com',)
        ).fetchall()
        conn.close()
        self.assertEqual([r[0] for r in rows],
                         ['2026-01-01T12:00:00Z', '2026-01-02T12:00:00Z'])

    def test_tag_visit_of_interest_does_not_create_read_event(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'of_interest', '2026-01-01T12:00:00Z')
        count = conn.execute("SELECT COUNT(*) FROM read_events").fetchone()[0]
        conn.close()
        self.assertEqual(count, 0)

    def test_tag_visit_sets_skimmed(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'skimmed', '2026-01-01T12:00:00Z')
        row = conn.execute('SELECT skimmed FROM visits').fetchone()
        conn.close()
        self.assertEqual(row[0], '2026-01-01T12:00:00Z')

    def test_tag_visit_skimmed_does_not_set_of_interest_or_read(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'skimmed', '2026-01-01T12:00:00Z')
        row = conn.execute('SELECT of_interest, read FROM visits').fetchone()
        conn.close()
        self.assertIsNone(row[0])
        self.assertIsNone(row[1])

    def test_tag_visit_no_existing_visit_returns_false(self):
        conn = self._conn()
        found = host.tag_visit(conn, 'https://example.com', 'of_interest', 'ts')
        count = conn.execute('SELECT COUNT(*) FROM visits').fetchone()[0]
        conn.close()
        self.assertFalse(found)
        self.assertEqual(count, 0)

    def test_tag_visit_existing_visit_returns_true(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        found = host.tag_visit(conn, 'https://example.com', 'of_interest', 'ts-tag')
        conn.close()
        self.assertTrue(found)

    def test_invalid_tag_returns_false_without_modifying_db(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        result = host.tag_visit(conn, 'https://example.com', 'favourite', 'ts-tag')
        row = conn.execute('SELECT of_interest, read, skimmed FROM visits').fetchone()
        conn.close()
        self.assertFalse(result)
        self.assertIsNone(row[0])  # of_interest untouched
        self.assertIsNone(row[1])  # read untouched
        self.assertIsNone(row[2])  # skimmed untouched

    def test_tag_visit_does_not_affect_other_urls(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts1', 'https://a.com', 'A')
        host.insert_visit(conn, 'ts2', 'https://b.com', 'B')
        host.tag_visit(conn, 'https://a.com', 'of_interest', '2026-01-01T12:00:00Z')
        rows = conn.execute('SELECT url, of_interest FROM visits ORDER BY url').fetchall()
        conn.close()
        self.assertEqual(rows[0][0], 'https://a.com')
        self.assertEqual(rows[0][1], '1')  # of_interest set; TEXT affinity stores literal 1 as '1'
        self.assertIsNone(rows[1][1])  # https://b.com unchanged


# ---------------------------------------------------------------------------
# query_visit
# ---------------------------------------------------------------------------

class TestQueryVisit(unittest.TestCase):

    def _conn(self):
        conn = sqlite3.connect(':memory:')
        host.ensure_db(conn)
        return conn

    def test_returns_none_for_unknown_url(self):
        conn = self._conn()
        result = host.query_visit(conn, 'https://unknown.com')
        conn.close()
        self.assertIsNone(result)

    def test_returns_record_for_known_url(self):
        conn = self._conn()
        host.insert_visit(conn, '2026-01-01T00:00:00Z', 'https://example.com', 'Example')
        result = host.query_visit(conn, 'https://example.com')
        conn.close()
        self.assertIsNotNone(result)
        self.assertEqual(result['timestamp'], '2026-01-01T00:00:00Z')
        self.assertEqual(result['title'], 'Example')

    def test_tag_fields_are_none_before_tagging(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        result = host.query_visit(conn, 'https://example.com')
        conn.close()
        self.assertIsNone(result['of_interest'])
        self.assertEqual(result['read'], [])   # empty list — never read
        self.assertIsNone(result['skimmed'])

    def test_tag_fields_reflect_applied_tags(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts-visit', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'of_interest', 'ts-mem')
        host.tag_visit(conn, 'https://example.com', 'read', 'ts-read')
        result = host.query_visit(conn, 'https://example.com')
        conn.close()
        self.assertTrue(result['of_interest'])  # boolean True (stored as 1)
        self.assertEqual(result['read'], ['ts-read'])
        self.assertIsNone(result['skimmed'])

    def test_query_visit_returns_all_read_events_in_order(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts-visit', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'read', '2026-01-01T10:00:00Z')
        host.tag_visit(conn, 'https://example.com', 'read', '2026-01-02T10:00:00Z')
        result = host.query_visit(conn, 'https://example.com')
        conn.close()
        self.assertEqual(result['read'],
                         ['2026-01-01T10:00:00Z', '2026-01-02T10:00:00Z'])

    def test_does_not_return_record_for_different_url(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://a.com', 'A')
        result = host.query_visit(conn, 'https://b.com')
        conn.close()
        self.assertIsNone(result)


# ---------------------------------------------------------------------------
# main() — unit tests that call main() directly with mocked I/O
# (subprocess-based integration tests don't contribute to coverage)
# ---------------------------------------------------------------------------

class TestMain(unittest.TestCase):
    """Exercise every branch of main() by calling it in-process."""

    def _call_main(self, message: dict, tmp: str, extra_patches=()) -> dict:
        """Call host.main() with fake stdin/stdout and isolated temp paths."""
        out_buf = io.BytesIO()
        mock_stdout = MagicMock()
        mock_stdout.buffer = out_buf

        mock_stdin = MagicMock()
        mock_stdin.buffer = io.BytesIO(_frame(message))

        base_patches = [
            patch('sys.stdin',  mock_stdin),
            patch('sys.stdout', mock_stdout),
            patch.object(host, 'LOG_FILE', os.path.join(tmp, 'visits.log')),
            patch.object(host, 'DB_FILE',  os.path.join(tmp, 'visits.db')),
        ]

        with contextlib.ExitStack() as stack:
            for p in (*base_patches, *extra_patches):
                stack.enter_context(p)
            host.main()

        out_buf.seek(0)
        return _unframe(out_buf.read())

    # --- read_message failure ---

    def test_read_message_failure_returns_error(self):
        out_buf = io.BytesIO()
        mock_stdout = MagicMock()
        mock_stdout.buffer = out_buf
        mock_stdin = MagicMock()
        mock_stdin.buffer = io.BytesIO(b'')  # empty → EOFError

        with patch('sys.stdin', mock_stdin), patch('sys.stdout', mock_stdout):
            host.main()

        out_buf.seek(0)
        resp = _unframe(out_buf.read())
        self.assertEqual(resp['status'], 'error')
        self.assertIn('stdin', resp['message'])

    # --- query action ---

    def test_query_missing_url_returns_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._call_main({'action': 'query'}, tmp)
        self.assertEqual(resp['status'], 'error')
        self.assertIn('url', resp['message'])

    def test_query_db_failure_returns_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._call_main(
                {'action': 'query', 'url': 'https://example.com'},
                tmp,
                extra_patches=[patch('sqlite3.connect',
                                     side_effect=sqlite3.OperationalError('disk full'))],
            )
        self.assertEqual(resp['status'], 'error')

    def test_query_unknown_url_returns_null_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._call_main({'action': 'query', 'url': 'https://unknown.com'}, tmp)
        self.assertEqual(resp['status'], 'ok')
        self.assertIsNone(resp['record'])

    def test_query_known_url_returns_full_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._call_main(
                {'timestamp': 'ts-visit', 'url': 'https://example.com', 'title': 'Example'},
                tmp,
            )
            resp = self._call_main({'action': 'query', 'url': 'https://example.com'}, tmp)
        self.assertEqual(resp['status'], 'ok')
        self.assertEqual(resp['record']['timestamp'], 'ts-visit')
        self.assertEqual(resp['record']['title'], 'Example')

    def test_query_does_not_write_log(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._call_main({'action': 'query', 'url': 'https://example.com'}, tmp)
            self.assertFalse(Path(tmp, 'visits.log').exists())

    # --- input validation ---

    def test_missing_url_returns_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._call_main({'timestamp': 'ts', 'title': 'Title'}, tmp)
        self.assertEqual(resp['status'], 'error')
        self.assertIn('url', resp['message'])

    def test_empty_url_returns_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._call_main({'timestamp': 'ts', 'url': '', 'title': 'Title'}, tmp)
        self.assertEqual(resp['status'], 'error')
        self.assertIn('url', resp['message'])

    def test_missing_timestamp_returns_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._call_main({'url': 'https://example.com', 'title': 'Title'}, tmp)
        self.assertEqual(resp['status'], 'error')
        self.assertIn('timestamp', resp['message'])

    def test_empty_timestamp_returns_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._call_main(
                {'timestamp': '', 'url': 'https://example.com', 'title': 'Title'}, tmp)
        self.assertEqual(resp['status'], 'error')
        self.assertIn('timestamp', resp['message'])

    def test_invalid_tag_returns_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._call_main(
                {'timestamp': 'ts', 'url': 'https://example.com',
                 'title': 'Title', 'tag': 'favourite'},
                tmp,
            )
        self.assertEqual(resp['status'], 'error')
        self.assertIn('tag', resp['message'])

    # --- auto-log (no tag) success path ---

    def test_auto_log_returns_ok(self):
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._call_main(
                {'timestamp': 'ts', 'url': 'https://example.com', 'title': 'Title'}, tmp)
        self.assertEqual(resp['status'], 'ok')

    def test_auto_log_writes_action_and_success_lines(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._call_main(
                {'timestamp': 'ts', 'url': 'https://example.com', 'title': 'Title'}, tmp)
            lines = Path(tmp, 'visits.log').read_text().splitlines()
        self.assertEqual(len(lines), 2)
        self.assertIn('https://example.com', lines[0])
        self.assertEqual(lines[1], 'success')

    def test_auto_log_inserts_db_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._call_main(
                {'timestamp': 'ts', 'url': 'https://example.com', 'title': 'Title'}, tmp)
            row = sqlite3.connect(os.path.join(tmp, 'visits.db')).execute(
                'SELECT url, timestamp, title FROM visits').fetchone()
        self.assertEqual(row, ('https://example.com', 'ts', 'Title'))

    # --- log write failure ---

    def test_log_write_failure_returns_error_with_log_prefix(self):
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._call_main(
                {'timestamp': 'ts', 'url': 'https://example.com', 'title': 'Title'},
                tmp,
                extra_patches=[patch.object(host, 'append_log',
                                            side_effect=OSError('permission denied'))],
            )
        self.assertEqual(resp['status'], 'error')
        self.assertTrue(any('log' in e for e in resp.get('errors', [])))

    # --- db write failure ---

    def test_db_write_failure_returns_error_with_db_prefix(self):
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._call_main(
                {'timestamp': 'ts', 'url': 'https://example.com', 'title': 'Title'},
                tmp,
                extra_patches=[patch('sqlite3.connect',
                                     side_effect=sqlite3.OperationalError('disk full'))],
            )
        self.assertEqual(resp['status'], 'error')
        self.assertTrue(any('db' in e for e in resp.get('errors', [])))

    # --- tag path ---

    def test_tag_found_returns_ok(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._call_main(
                {'timestamp': 'ts-visit', 'url': 'https://example.com', 'title': 'Title'}, tmp)
            resp = self._call_main(
                {'timestamp': 'ts-tag', 'url': 'https://example.com',
                 'title': 'Title', 'tag': 'of_interest'},
                tmp,
            )
        self.assertEqual(resp['status'], 'ok')

    def test_tag_not_found_returns_no_record_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._call_main(
                {'timestamp': 'ts', 'url': 'https://example.com',
                 'title': 'Title', 'tag': 'of_interest'},
                tmp,
            )
        self.assertEqual(resp['status'], 'error')
        self.assertIn('No record found', resp['message'])

    def test_tag_not_found_writes_error_result_to_log(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._call_main(
                {'timestamp': 'ts', 'url': 'https://example.com',
                 'title': 'Title', 'tag': 'of_interest'},
                tmp,
            )
            lines = Path(tmp, 'visits.log').read_text().splitlines()
        # line 0 = action (4-field TSV), line 1 = 'error: No record found...'
        self.assertEqual(len(lines), 2)
        self.assertIn('No record found', lines[1])

    def test_all_three_valid_tags_succeed_after_visit(self):
        for tag in ('of_interest', 'read', 'skimmed'):
            with self.subTest(tag=tag), tempfile.TemporaryDirectory() as tmp:
                self._call_main(
                    {'timestamp': 'ts-visit', 'url': 'https://example.com', 'title': 'T'}, tmp)
                resp = self._call_main(
                    {'timestamp': 'ts-tag', 'url': 'https://example.com',
                     'title': 'T', 'tag': tag},
                    tmp,
                )
                self.assertEqual(resp['status'], 'ok')

    # --- result-log write failure ---

    def test_append_result_log_failure_still_returns_response(self):
        """If append_result_log raises, main() logs the error but still sends a response."""
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._call_main(
                {'timestamp': 'ts', 'url': 'https://example.com', 'title': 'Title'},
                tmp,
                extra_patches=[patch.object(host, 'append_result_log',
                                            side_effect=OSError('disk full'))],
            )
        # The DB write succeeded, so the response should still be ok
        self.assertEqual(resp['status'], 'ok')

    def test_result_log_failure_on_error_path_still_returns_error_response(self):
        """append_result_log failure on the error path doesn't swallow the error response."""
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._call_main(
                {'timestamp': 'ts', 'url': 'https://example.com',
                 'title': 'Title', 'tag': 'of_interest'},  # no prior visit → no_record
                tmp,
                extra_patches=[patch.object(host, 'append_result_log',
                                            side_effect=OSError('disk full'))],
            )
        self.assertEqual(resp['status'], 'error')
        self.assertIn('No record found', resp['message'])


# ---------------------------------------------------------------------------
# Integration: run host.py as a subprocess (mirrors Chrome's usage)
# ---------------------------------------------------------------------------

class TestIntegration(unittest.TestCase):

    def _invoke(self, message: dict, tmp: str) -> dict:
        """Send one native message to host.py as a subprocess; return response."""
        env = os.environ.copy()
        env['BVL_LOG_FILE'] = os.path.join(tmp, 'visits.log')
        env['BVL_DB_FILE']  = os.path.join(tmp, 'visits.db')
        env['BVL_HOST_LOG'] = os.path.join(tmp, 'host.log')

        result = subprocess.run(
            [sys.executable, HOST_PY],
            input=_frame(message),
            capture_output=True,
            timeout=10,
            env=env,
        )
        self.assertEqual(result.returncode, 0,
                         f'host.py non-zero exit; stderr: {result.stderr.decode()}')
        return _unframe(result.stdout)

    def test_returns_ok(self):
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._invoke(
                {'timestamp': '2026-01-01T00:00:00Z', 'url': 'https://example.com', 'title': 'Example'},
                tmp,
            )
        self.assertEqual(resp, {'status': 'ok'})

    def test_log_file_content(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._invoke(
                {'timestamp': '2026-01-01T00:00:00Z', 'url': 'https://example.com', 'title': 'Example Domain'},
                tmp,
            )
            lines = Path(tmp, 'visits.log').read_text().splitlines()
        self.assertEqual(lines[0], '2026-01-01T00:00:00Z\thttps://example.com\tExample Domain')
        self.assertEqual(lines[1], 'success')

    def test_sqlite_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._invoke(
                {'timestamp': '2026-01-01T00:00:00Z', 'url': 'https://example.com', 'title': 'Example Domain'},
                tmp,
            )
            conn = sqlite3.connect(os.path.join(tmp, 'visits.db'))
            row = conn.execute(
                'SELECT url, timestamp, title, of_interest FROM visits'
            ).fetchone()
            read_count = conn.execute('SELECT COUNT(*) FROM read_events').fetchone()[0]
            conn.close()
        self.assertEqual(row[:3], ('https://example.com', '2026-01-01T00:00:00Z', 'Example Domain'))
        self.assertIsNone(row[3])  # of_interest
        self.assertEqual(read_count, 0)  # no reads yet

    def test_missing_timestamp_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._invoke({'url': 'https://example.com', 'title': 'No Timestamp'}, tmp)
            self.assertEqual(resp['status'], 'error')
            self.assertIn('timestamp', resp.get('message', ''))
            self.assertFalse(Path(tmp, 'visits.log').exists())
            self.assertFalse(Path(tmp, 'visits.db').exists())

    def test_empty_timestamp_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._invoke({'timestamp': '', 'url': 'https://example.com', 'title': 'Title'}, tmp)
            self.assertEqual(resp['status'], 'error')
            self.assertIn('timestamp', resp.get('message', ''))
            self.assertFalse(Path(tmp, 'visits.log').exists())

    def test_sequential_invocations_accumulate(self):
        with tempfile.TemporaryDirectory() as tmp:
            for i in range(3):
                self._invoke(
                    {'timestamp': f'2026-01-01T00:0{i}:00Z',
                     'url': f'https://site{i}.com',
                     'title': f'Site {i}'},
                    tmp,
                )
            log_lines = Path(tmp, 'visits.log').read_text().splitlines()
            self.assertEqual(len(log_lines), 6)  # 2 lines per invocation

            conn = sqlite3.connect(os.path.join(tmp, 'visits.db'))
            count = conn.execute('SELECT COUNT(*) FROM visits').fetchone()[0]
            conn.close()
        self.assertEqual(count, 3)

    def test_duplicate_url_creates_single_db_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            for _ in range(3):
                self._invoke(
                    {'timestamp': '2026-01-01T00:00:00Z', 'url': 'https://example.com', 'title': 'Example'},
                    tmp,
                )
            conn = sqlite3.connect(os.path.join(tmp, 'visits.db'))
            count = conn.execute('SELECT COUNT(*) FROM visits').fetchone()[0]
            conn.close()
        self.assertEqual(count, 1)

    def test_duplicate_url_writes_multiple_log_lines(self):
        with tempfile.TemporaryDirectory() as tmp:
            for _ in range(3):
                self._invoke(
                    {'timestamp': '2026-01-01T00:00:00Z', 'url': 'https://example.com', 'title': 'Example'},
                    tmp,
                )
            lines = Path(tmp, 'visits.log').read_text().splitlines()
        self.assertEqual(len(lines), 6)  # 2 lines per invocation

    def test_duplicate_url_preserves_original_timestamp(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._invoke(
                {'timestamp': 'ts-first', 'url': 'https://example.com', 'title': 'First'},
                tmp,
            )
            self._invoke(
                {'timestamp': 'ts-second', 'url': 'https://example.com', 'title': 'Second'},
                tmp,
            )
            conn = sqlite3.connect(os.path.join(tmp, 'visits.db'))
            row = conn.execute('SELECT timestamp FROM visits').fetchone()
            conn.close()
        self.assertEqual(row[0], 'ts-first')

    def test_null_url_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._invoke({'timestamp': 'ts', 'url': None, 'title': 'Title'}, tmp)
            self.assertEqual(resp['status'], 'error')
            self.assertIn('url', resp.get('message', ''))
            self.assertFalse(Path(tmp, 'visits.log').exists())

    def test_null_timestamp_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._invoke({'timestamp': None, 'url': 'https://example.com', 'title': 'Title'}, tmp)
            self.assertEqual(resp['status'], 'error')
            self.assertIn('timestamp', resp.get('message', ''))
            self.assertFalse(Path(tmp, 'visits.log').exists())

    def test_null_title_treated_as_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._invoke({'timestamp': 'ts', 'url': 'https://example.com', 'title': None}, tmp)
            self.assertEqual(resp['status'], 'ok')
            row = sqlite3.connect(os.path.join(tmp, 'visits.db')).execute(
                'SELECT title FROM visits'
            ).fetchone()
        self.assertEqual(row[0], '')

    def test_empty_url_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._invoke({'timestamp': 'ts', 'url': '', 'title': 'Whatever'}, tmp)
            self.assertEqual(resp['status'], 'error')
            self.assertIn('url', resp.get('message', ''))
            self.assertFalse(Path(tmp, 'visits.log').exists())
            self.assertFalse(Path(tmp, 'visits.db').exists())

    def test_whitespace_only_url_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._invoke({'timestamp': 'ts', 'url': '   ', 'title': 'Whatever'}, tmp)
            self.assertEqual(resp['status'], 'error')
            self.assertFalse(Path(tmp, 'visits.log').exists())

    def test_missing_url_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._invoke({'timestamp': 'ts', 'title': 'No URL at all'}, tmp)
            self.assertEqual(resp['status'], 'error')
            self.assertFalse(Path(tmp, 'visits.log').exists())

    def test_empty_title_with_valid_url_accepted(self):
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._invoke({'timestamp': 'ts', 'url': 'https://example.com', 'title': ''}, tmp)
            self.assertEqual(resp['status'], 'ok')
            row = sqlite3.connect(os.path.join(tmp, 'visits.db')).execute(
                'SELECT url, title FROM visits'
            ).fetchone()
        self.assertEqual(row, ('https://example.com', ''))

    def test_invalid_tag_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._invoke(
                {'timestamp': 'ts', 'url': 'https://example.com', 'title': 'Title', 'tag': 'favourite'},
                tmp,
            )
            self.assertEqual(resp['status'], 'error')
            self.assertIn('tag', resp.get('message', ''))

    def test_log_written_even_when_db_path_is_unwritable(self):
        """Log file write proceeds even if the DB path is a directory (can't be opened)."""
        with tempfile.TemporaryDirectory() as tmp:
            db_collision = os.path.join(tmp, 'visits.db')
            os.makedirs(db_collision)

            env = os.environ.copy()
            env['BVL_LOG_FILE'] = os.path.join(tmp, 'visits.log')
            env['BVL_DB_FILE']  = db_collision
            env['BVL_HOST_LOG'] = os.path.join(tmp, 'host.log')

            result = subprocess.run(
                [sys.executable, HOST_PY],
                input=_frame({'timestamp': 'ts', 'url': 'https://example.com', 'title': 'Title'}),
                capture_output=True,
                timeout=10,
                env=env,
            )
            resp = _unframe(result.stdout)
            log_content = Path(tmp, 'visits.log').read_text()

        self.assertEqual(resp['status'], 'error')
        self.assertTrue(any('db' in e for e in resp.get('errors', [])))
        self.assertIn('https://example.com', log_content)

    def test_tag_message_sets_of_interest_column(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._invoke(
                {'timestamp': 'ts-visit', 'url': 'https://example.com', 'title': 'Example'},
                tmp,
            )
            resp = self._invoke(
                {'timestamp': 'ts-tag', 'url': 'https://example.com', 'title': 'Example', 'tag': 'of_interest'},
                tmp,
            )
            self.assertEqual(resp['status'], 'ok')
            conn = sqlite3.connect(os.path.join(tmp, 'visits.db'))
            row = conn.execute('SELECT of_interest, skimmed FROM visits').fetchone()
            read_count = conn.execute('SELECT COUNT(*) FROM read_events').fetchone()[0]
            conn.close()
        self.assertEqual(row[0], '1')  # of_interest set; TEXT affinity stores literal 1 as '1'
        self.assertIsNone(row[1])      # skimmed untouched
        self.assertEqual(read_count, 0)

    def test_tag_message_inserts_read_event(self):
        url = 'https://example.com'
        with tempfile.TemporaryDirectory() as tmp:
            self._invoke(
                {'timestamp': 'ts-visit', 'url': url, 'title': 'Example'}, tmp)
            resp = self._invoke(
                {'timestamp': 'ts-tag', 'url': url, 'title': 'Example', 'tag': 'read'}, tmp)
            self.assertEqual(resp['status'], 'ok')
            conn = sqlite3.connect(os.path.join(tmp, 'visits.db'))
            event = conn.execute(
                'SELECT timestamp FROM read_events WHERE url = ?', (url,)
            ).fetchone()
            conn.close()
        self.assertIsNotNone(event)
        self.assertEqual(event[0], 'ts-tag')

    def test_tag_message_read_twice_stores_both_events(self):
        url = 'https://example.com'
        with tempfile.TemporaryDirectory() as tmp:
            self._invoke(
                {'timestamp': 'ts-visit', 'url': url, 'title': 'Example'}, tmp)
            self._invoke(
                {'timestamp': 'ts-read-1', 'url': url, 'title': 'Example', 'tag': 'read'}, tmp)
            resp = self._invoke(
                {'timestamp': 'ts-read-2', 'url': url, 'title': 'Example', 'tag': 'read'}, tmp)
            self.assertEqual(resp['status'], 'ok')
            conn = sqlite3.connect(os.path.join(tmp, 'visits.db'))
            events = conn.execute(
                'SELECT timestamp FROM read_events WHERE url = ? ORDER BY timestamp ASC', (url,)
            ).fetchall()
            conn.close()
        self.assertEqual([e[0] for e in events], ['ts-read-1', 'ts-read-2'])

    def test_tag_message_sets_skimmed_column(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._invoke(
                {'timestamp': 'ts-visit', 'url': 'https://example.com', 'title': 'Example'},
                tmp,
            )
            resp = self._invoke(
                {'timestamp': 'ts-tag', 'url': 'https://example.com', 'title': 'Example', 'tag': 'skimmed'},
                tmp,
            )
            self.assertEqual(resp['status'], 'ok')
            conn = sqlite3.connect(os.path.join(tmp, 'visits.db'))
            row = conn.execute('SELECT of_interest, skimmed FROM visits').fetchone()
            read_count = conn.execute('SELECT COUNT(*) FROM read_events').fetchone()[0]
            conn.close()
        self.assertIsNone(row[0])       # of_interest untouched
        self.assertEqual(row[1], 'ts-tag')
        self.assertEqual(read_count, 0)

    def test_tag_message_appends_four_field_log_line(self):
        url = 'https://example.com'
        with tempfile.TemporaryDirectory() as tmp:
            self._invoke(
                {'timestamp': 'ts-visit', 'url': url, 'title': 'Example'}, tmp)
            self._invoke(
                {'timestamp': 'ts-tag', 'url': url, 'title': 'Example', 'tag': 'read'}, tmp)
            lines = Path(tmp, 'visits.log').read_text().splitlines()
        # lines[0]=visit action, lines[1]=visit result, lines[2]=tag action, lines[3]=tag result
        self.assertEqual(len(lines), 4)
        tag_action = lines[2].split('\t')
        self.assertEqual(len(tag_action), 4)
        self.assertEqual(tag_action[3], 'read')
        self.assertEqual(lines[3], 'success')

    def test_auto_log_appends_three_field_log_line(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._invoke(
                {'timestamp': 'ts', 'url': 'https://example.com', 'title': 'Example'},
                tmp,
            )
            lines = Path(tmp, 'visits.log').read_text().splitlines()
        self.assertEqual(len(lines), 2)
        self.assertEqual(len(lines[0].split('\t')), 3)  # action: 3 fields
        self.assertEqual(lines[1], 'success')

    def test_tag_without_prior_visit_returns_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._invoke(
                {'timestamp': 'ts', 'url': 'https://example.com', 'title': 'Example', 'tag': 'of_interest'},
                tmp,
            )
            self.assertEqual(resp['status'], 'error')
            self.assertIn('No record found', resp.get('message', ''))
            lines = Path(tmp, 'visits.log').read_text().splitlines()
            self.assertEqual(len(lines), 2)
            action = lines[0].split('\t')
            self.assertEqual(len(action), 4)
            self.assertEqual(action[3], 'of_interest')
            self.assertIn('No record found', lines[1])

    def test_query_unknown_url_returns_null_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._invoke({'action': 'query', 'url': 'https://example.com'}, tmp)
            self.assertEqual(resp['status'], 'ok')
            self.assertIsNone(resp['record'])

    def test_query_known_url_returns_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._invoke(
                {'timestamp': 'ts-visit', 'url': 'https://example.com', 'title': 'Example'},
                tmp,
            )
            resp = self._invoke({'action': 'query', 'url': 'https://example.com'}, tmp)
            self.assertEqual(resp['status'], 'ok')
            self.assertIsNotNone(resp['record'])
            self.assertEqual(resp['record']['timestamp'], 'ts-visit')
            self.assertEqual(resp['record']['title'], 'Example')

    def test_query_reflects_applied_tags(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._invoke(
                {'timestamp': 'ts-visit', 'url': 'https://example.com', 'title': 'Example'},
                tmp,
            )
            self._invoke(
                {'timestamp': 'ts-mem', 'url': 'https://example.com', 'title': 'Example', 'tag': 'of_interest'},
                tmp,
            )
            resp = self._invoke({'action': 'query', 'url': 'https://example.com'}, tmp)
            self.assertTrue(resp['record']['of_interest'])  # boolean True
            self.assertEqual(resp['record']['read'], [])    # never read → empty list
            self.assertIsNone(resp['record']['skimmed'])

    def test_query_does_not_write_log(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._invoke({'action': 'query', 'url': 'https://example.com'}, tmp)
            self.assertFalse(Path(tmp, 'visits.log').exists())

    def test_query_missing_url_returns_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._invoke({'action': 'query'}, tmp)
            self.assertEqual(resp['status'], 'error')
            self.assertIn('url', resp.get('message', ''))


if __name__ == '__main__':
    unittest.main()
