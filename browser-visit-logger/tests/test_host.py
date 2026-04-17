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

    def _run(self, timestamp, url, title) -> str:
        """Call append_log with a temp file and return its contents."""
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'visits.log')
            with patch.object(host, 'LOG_FILE', path):
                host.append_log(timestamp, url, title)
            return Path(path).read_text(encoding='utf-8')

    def test_tsv_format(self):
        content = self._run('2026-01-01T00:00:00Z', 'https://example.com', 'Example Domain')
        self.assertEqual(content, '2026-01-01T00:00:00Z\thttps://example.com\tExample Domain\n')

    def test_exactly_three_fields(self):
        content = self._run('ts', 'https://a.com', 'Title').rstrip('\n')
        self.assertEqual(len(content.split('\t')), 3)

    def test_tab_in_title_replaced(self):
        content = self._run('ts', 'https://a.com', 'Part1\tPart2').rstrip('\n')
        parts = content.split('\t')
        self.assertEqual(parts[2], 'Part1 Part2')

    def test_newline_in_title_replaced(self):
        content = self._run('ts', 'https://a.com', 'Line1\nLine2')
        # Must be exactly one line
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


# ---------------------------------------------------------------------------
# Database (ensure_db / insert_visit)
# ---------------------------------------------------------------------------

class TestDatabase(unittest.TestCase):

    def _conn(self):
        conn = sqlite3.connect(':memory:')
        host.ensure_db(conn)
        return conn

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

    def test_ensure_db_creates_url_index(self):
        conn = self._conn()
        indexes = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        )}
        self.assertIn('idx_visits_url', indexes)
        conn.close()

    def test_ensure_db_is_idempotent(self):
        conn = sqlite3.connect(':memory:')
        host.ensure_db(conn)
        host.ensure_db(conn)  # second call must not raise
        conn.close()

    def test_insert_visit_stores_all_fields(self):
        conn = self._conn()
        host.insert_visit(conn, '2026-01-01T00:00:00Z', 'https://example.com', 'Example')
        row = conn.execute('SELECT timestamp, url, title FROM visits').fetchone()
        conn.close()
        self.assertEqual(row, ('2026-01-01T00:00:00Z', 'https://example.com', 'Example'))

    def test_insert_visit_empty_title(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', '')
        row = conn.execute('SELECT title FROM visits').fetchone()
        conn.close()
        self.assertEqual(row[0], '')

    def test_insert_visit_multiple_rows_ordered_by_id(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts1', 'https://a.com', 'A')
        host.insert_visit(conn, 'ts2', 'https://b.com', 'B')
        rows = conn.execute('SELECT url FROM visits ORDER BY id').fetchall()
        conn.close()
        self.assertEqual(rows, [('https://a.com',), ('https://b.com',)])

    def test_insert_visit_unicode(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', '日本語タイトル')
        row = conn.execute('SELECT title FROM visits').fetchone()
        conn.close()
        self.assertEqual(row[0], '日本語タイトル')


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
            content = Path(tmp, 'visits.log').read_text()
        self.assertEqual(content, '2026-01-01T00:00:00Z\thttps://example.com\tExample Domain\n')

    def test_sqlite_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._invoke(
                {'timestamp': '2026-01-01T00:00:00Z', 'url': 'https://example.com', 'title': 'Example Domain'},
                tmp,
            )
            conn = sqlite3.connect(os.path.join(tmp, 'visits.db'))
            row = conn.execute('SELECT timestamp, url, title FROM visits').fetchone()
            conn.close()
        self.assertEqual(row, ('2026-01-01T00:00:00Z', 'https://example.com', 'Example Domain'))

    def test_missing_timestamp_falls_back(self):
        from datetime import datetime
        with tempfile.TemporaryDirectory() as tmp:
            self._invoke({'url': 'https://example.com', 'title': 'No Timestamp'}, tmp)
            conn = sqlite3.connect(os.path.join(tmp, 'visits.db'))
            ts = conn.execute('SELECT timestamp FROM visits').fetchone()[0]
            conn.close()
        # Verify the fallback is a parseable ISO 8601 datetime, not just any string
        parsed = datetime.fromisoformat(ts)  # raises ValueError if not a valid datetime
        self.assertGreater(parsed.year, 2000)

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
            self.assertEqual(len(log_lines), 3)

            conn = sqlite3.connect(os.path.join(tmp, 'visits.db'))
            count = conn.execute('SELECT COUNT(*) FROM visits').fetchone()[0]
            conn.close()
        self.assertEqual(count, 3)

    def test_empty_url_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._invoke({'timestamp': 'ts', 'url': '', 'title': 'Whatever'}, tmp)
            self.assertEqual(resp['status'], 'error')
            self.assertIn('url', resp.get('message', ''))
            # Nothing should have been written
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
        # Title is optional — a page may simply have no <title>
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._invoke({'timestamp': 'ts', 'url': 'https://example.com', 'title': ''}, tmp)
            self.assertEqual(resp['status'], 'ok')
            row = sqlite3.connect(os.path.join(tmp, 'visits.db')).execute(
                'SELECT url, title FROM visits'
            ).fetchone()
        self.assertEqual(row, ('https://example.com', ''))

    def test_log_written_even_when_db_path_is_unwritable(self):
        """Log file write proceeds even if the DB path is a directory (can't be opened)."""
        with tempfile.TemporaryDirectory() as tmp:
            # Make the DB path a directory so sqlite3.connect() will fail
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

        # DB write failed, so the response should report an error
        self.assertEqual(resp['status'], 'error')
        self.assertTrue(any('db' in e for e in resp.get('errors', [])))
        # But the log write must have succeeded independently
        self.assertIn('https://example.com', log_content)


if __name__ == '__main__':
    unittest.main()
