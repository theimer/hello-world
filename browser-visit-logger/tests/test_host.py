"""
Unit and integration tests for native-host/host.py.

Run with:
    cd browser-visit-logger
    pytest tests/test_host.py -v

Or the full suite:
    pytest tests/ -v
"""
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
        content = self._run('ts', 'https://example.com', 'Example', tag='memorable')
        parts = content.rstrip('\n').split('\t')
        self.assertEqual(len(parts), 4)
        self.assertEqual(parts[3], 'memorable')

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

    def test_ensure_db_creates_visits_table(self):
        conn = self._conn()
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        self.assertIn('visits', tables)
        conn.close()

    def test_ensure_db_creates_timestamp_index(self):
        conn = self._conn()
        indexes = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        )}
        self.assertIn('idx_visits_timestamp', indexes)
        conn.close()

    def test_ensure_db_creates_memorable_column(self):
        conn = self._conn()
        self.assertIn('memorable', self._cols(conn))
        conn.close()

    def test_ensure_db_creates_read_column(self):
        conn = self._conn()
        self.assertIn('read', self._cols(conn))
        conn.close()

    def test_ensure_db_creates_skimmed_column(self):
        conn = self._conn()
        self.assertIn('skimmed', self._cols(conn))
        conn.close()

    def test_ensure_db_adds_skimmed_column_to_existing_schema(self):
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
        self.assertIn('skimmed', self._cols(conn))
        conn.close()

    def test_ensure_db_url_is_primary_key(self):
        conn = self._conn()
        pk_cols = {r[1] for r in conn.execute('PRAGMA table_info(visits)') if r[5] == 1}
        conn.close()
        self.assertEqual(pk_cols, {'url'})

    def test_ensure_db_is_idempotent(self):
        conn = sqlite3.connect(':memory:')
        host.ensure_db(conn)
        host.ensure_db(conn)  # second call must not raise
        conn.close()

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
        self.assertIn('memorable', cols)
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

    def test_insert_visit_memorable_and_read_default_to_null(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Title')
        row = conn.execute('SELECT memorable, read, skimmed FROM visits').fetchone()
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

    def test_tag_visit_sets_memorable(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'memorable', '2026-01-01T12:00:00Z')
        row = conn.execute('SELECT memorable FROM visits').fetchone()
        conn.close()
        self.assertEqual(row[0], '2026-01-01T12:00:00Z')

    def test_tag_visit_sets_read(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'read', '2026-01-01T12:00:00Z')
        row = conn.execute('SELECT read FROM visits').fetchone()
        conn.close()
        self.assertEqual(row[0], '2026-01-01T12:00:00Z')

    def test_tag_visit_memorable_does_not_set_read(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'memorable', '2026-01-01T12:00:00Z')
        row = conn.execute('SELECT read FROM visits').fetchone()
        conn.close()
        self.assertIsNone(row[0])

    def test_tag_visit_sets_skimmed(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'skimmed', '2026-01-01T12:00:00Z')
        row = conn.execute('SELECT skimmed FROM visits').fetchone()
        conn.close()
        self.assertEqual(row[0], '2026-01-01T12:00:00Z')

    def test_tag_visit_skimmed_does_not_set_memorable_or_read(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'skimmed', '2026-01-01T12:00:00Z')
        row = conn.execute('SELECT memorable, read FROM visits').fetchone()
        conn.close()
        self.assertIsNone(row[0])
        self.assertIsNone(row[1])

    def test_tag_visit_no_existing_visit_returns_false(self):
        conn = self._conn()
        found = host.tag_visit(conn, 'https://example.com', 'memorable', 'ts')
        count = conn.execute('SELECT COUNT(*) FROM visits').fetchone()[0]
        conn.close()
        self.assertFalse(found)
        self.assertEqual(count, 0)

    def test_tag_visit_existing_visit_returns_true(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        found = host.tag_visit(conn, 'https://example.com', 'memorable', 'ts-tag')
        conn.close()
        self.assertTrue(found)

    def test_tag_visit_does_not_affect_other_urls(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts1', 'https://a.com', 'A')
        host.insert_visit(conn, 'ts2', 'https://b.com', 'B')
        host.tag_visit(conn, 'https://a.com', 'memorable', '2026-01-01T12:00:00Z')
        rows = conn.execute('SELECT url, memorable FROM visits ORDER BY url').fetchall()
        conn.close()
        self.assertEqual(rows[0], ('https://a.com', '2026-01-01T12:00:00Z'))
        self.assertIsNone(rows[1][1])  # https://b.com unchanged


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
                'SELECT url, timestamp, title, memorable, read FROM visits'
            ).fetchone()
            conn.close()
        self.assertEqual(row[:3], ('https://example.com', '2026-01-01T00:00:00Z', 'Example Domain'))
        self.assertIsNone(row[3])  # memorable
        self.assertIsNone(row[4])  # read

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

    def test_tag_message_sets_memorable_column(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._invoke(
                {'timestamp': 'ts-visit', 'url': 'https://example.com', 'title': 'Example'},
                tmp,
            )
            resp = self._invoke(
                {'timestamp': 'ts-tag', 'url': 'https://example.com', 'title': 'Example', 'tag': 'memorable'},
                tmp,
            )
            self.assertEqual(resp['status'], 'ok')
            conn = sqlite3.connect(os.path.join(tmp, 'visits.db'))
            row = conn.execute('SELECT memorable, read, skimmed FROM visits').fetchone()
            conn.close()
        self.assertEqual(row[0], 'ts-tag')
        self.assertIsNone(row[1])
        self.assertIsNone(row[2])

    def test_tag_message_sets_read_column(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._invoke(
                {'timestamp': 'ts-visit', 'url': 'https://example.com', 'title': 'Example'},
                tmp,
            )
            resp = self._invoke(
                {'timestamp': 'ts-tag', 'url': 'https://example.com', 'title': 'Example', 'tag': 'read'},
                tmp,
            )
            self.assertEqual(resp['status'], 'ok')
            conn = sqlite3.connect(os.path.join(tmp, 'visits.db'))
            row = conn.execute('SELECT memorable, read, skimmed FROM visits').fetchone()
            conn.close()
        self.assertIsNone(row[0])
        self.assertEqual(row[1], 'ts-tag')
        self.assertIsNone(row[2])

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
            row = conn.execute('SELECT memorable, read, skimmed FROM visits').fetchone()
            conn.close()
        self.assertIsNone(row[0])
        self.assertIsNone(row[1])
        self.assertEqual(row[2], 'ts-tag')

    def test_tag_message_appends_four_field_log_line(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._invoke(
                {'timestamp': 'ts-visit', 'url': 'https://example.com', 'title': 'Example'},
                tmp,
            )
            self._invoke(
                {'timestamp': 'ts-tag', 'url': 'https://example.com', 'title': 'Example', 'tag': 'read'},
                tmp,
            )
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
                {'timestamp': 'ts', 'url': 'https://example.com', 'title': 'Example', 'tag': 'memorable'},
                tmp,
            )
            self.assertEqual(resp['status'], 'error')
            self.assertIn('No record found', resp.get('message', ''))
            lines = Path(tmp, 'visits.log').read_text().splitlines()
            self.assertEqual(len(lines), 2)
            action = lines[0].split('\t')
            self.assertEqual(len(action), 4)
            self.assertEqual(action[3], 'memorable')
            self.assertIn('No record found', lines[1])


# ---------------------------------------------------------------------------
# save_snapshot
# ---------------------------------------------------------------------------

class TestSaveSnapshot(unittest.TestCase):

    def test_moves_file_to_snapshots_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, 'snap.mhtml')
            Path(src).write_text('mhtml content')
            snapshots_dir = os.path.join(tmp, 'snapshots')
            with patch.object(host, 'SNAPSHOTS_DIR', snapshots_dir):
                dest = host.save_snapshot('https://example.com', src)
            self.assertTrue(os.path.exists(dest))
            self.assertFalse(os.path.exists(src))

    def test_creates_snapshots_dir_if_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, 'snap.mhtml')
            Path(src).write_text('mhtml content')
            snapshots_dir = os.path.join(tmp, 'new', 'snapshots')
            with patch.object(host, 'SNAPSHOTS_DIR', snapshots_dir):
                host.save_snapshot('https://example.com', src)
            self.assertTrue(os.path.isdir(snapshots_dir))

    def test_destination_name_is_md5_of_url(self):
        import hashlib
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, 'snap.mhtml')
            Path(src).write_text('mhtml content')
            snapshots_dir = os.path.join(tmp, 'snapshots')
            url = 'https://example.com/page'
            with patch.object(host, 'SNAPSHOTS_DIR', snapshots_dir):
                dest = host.save_snapshot(url, src)
            expected_name = hashlib.md5(url.encode('utf-8')).hexdigest() + '.mhtml'
            self.assertEqual(os.path.basename(dest), expected_name)

    def test_different_urls_produce_different_filenames(self):
        with tempfile.TemporaryDirectory() as tmp:
            snapshots_dir = os.path.join(tmp, 'snapshots')
            results = []
            for i, url in enumerate(['https://a.com', 'https://b.com']):
                src = os.path.join(tmp, f'snap{i}.mhtml')
                Path(src).write_text('content')
                with patch.object(host, 'SNAPSHOTS_DIR', snapshots_dir):
                    results.append(os.path.basename(host.save_snapshot(url, src)))
            self.assertNotEqual(results[0], results[1])

    def test_same_url_produces_same_filename(self):
        import hashlib
        url = 'https://example.com'
        with tempfile.TemporaryDirectory() as tmp:
            snapshots_dir = os.path.join(tmp, 'snapshots')
            src = os.path.join(tmp, 'snap.mhtml')
            Path(src).write_text('content')
            with patch.object(host, 'SNAPSHOTS_DIR', snapshots_dir):
                dest = host.save_snapshot(url, src)
            expected = hashlib.md5(url.encode('utf-8')).hexdigest() + '.mhtml'
            self.assertEqual(os.path.basename(dest), expected)


class TestIntegrationSnapshot(unittest.TestCase):

    def _invoke(self, message: dict, tmp: str) -> dict:
        env = os.environ.copy()
        env['BVL_LOG_FILE']      = os.path.join(tmp, 'visits.log')
        env['BVL_DB_FILE']       = os.path.join(tmp, 'visits.db')
        env['BVL_HOST_LOG']      = os.path.join(tmp, 'host.log')
        env['BVL_SNAPSHOTS_DIR'] = os.path.join(tmp, 'snapshots')

        import struct as _struct, json as _json
        encoded = _json.dumps(message).encode('utf-8')
        framed  = _struct.pack('<I', len(encoded)) + encoded

        result = subprocess.run(
            [sys.executable, HOST_PY],
            input=framed,
            capture_output=True,
            timeout=10,
            env=env,
        )
        length = _struct.unpack('<I', result.stdout[:4])[0]
        return _json.loads(result.stdout[4:4 + length])

    def test_read_tag_with_snapshot_moves_file(self):
        import hashlib
        with tempfile.TemporaryDirectory() as tmp:
            # Create a fake downloaded MHTML file
            snap_src = os.path.join(tmp, 'bvl-snapshot-123.mhtml')
            Path(snap_src).write_text('<mhtml content>')

            # First insert a visit record
            self._invoke(
                {'timestamp': 'ts-visit', 'url': 'https://example.com', 'title': 'Example'},
                tmp,
            )
            resp = self._invoke(
                {'timestamp': 'ts-tag', 'url': 'https://example.com', 'title': 'Example',
                 'tag': 'read', 'snapshot_download_path': snap_src},
                tmp,
            )
            self.assertEqual(resp['status'], 'ok')
            expected_name = hashlib.md5('https://example.com'.encode()).hexdigest() + '.mhtml'
            dest = os.path.join(tmp, 'snapshots', expected_name)
            self.assertTrue(os.path.exists(dest))
            self.assertFalse(os.path.exists(snap_src))

    def test_read_tag_without_snapshot_path_still_succeeds(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._invoke(
                {'timestamp': 'ts-visit', 'url': 'https://example.com', 'title': 'Example'},
                tmp,
            )
            resp = self._invoke(
                {'timestamp': 'ts-tag', 'url': 'https://example.com', 'title': 'Example', 'tag': 'read'},
                tmp,
            )
            self.assertEqual(resp['status'], 'ok')
            self.assertFalse(os.path.isdir(os.path.join(tmp, 'snapshots')))

    def test_snapshot_failure_does_not_affect_response_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._invoke(
                {'timestamp': 'ts-visit', 'url': 'https://example.com', 'title': 'Example'},
                tmp,
            )
            resp = self._invoke(
                {'timestamp': 'ts-tag', 'url': 'https://example.com', 'title': 'Example',
                 'tag': 'read', 'snapshot_download_path': '/nonexistent/path.mhtml'},
                tmp,
            )
            self.assertEqual(resp['status'], 'ok')

    def test_snapshot_failure_noted_in_log(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._invoke(
                {'timestamp': 'ts-visit', 'url': 'https://example.com', 'title': 'Example'},
                tmp,
            )
            self._invoke(
                {'timestamp': 'ts-tag', 'url': 'https://example.com', 'title': 'Example',
                 'tag': 'read', 'snapshot_download_path': '/nonexistent/path.mhtml'},
                tmp,
            )
            lines = Path(tmp, 'visits.log').read_text().splitlines()
            result_line = lines[-1]
            self.assertIn('snapshot error', result_line)


if __name__ == '__main__':
    unittest.main()
