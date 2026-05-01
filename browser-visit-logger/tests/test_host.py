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

FIXED_REC_ID = '0' * 32  # placeholder UUID hex; tests assert on field positions


class TestAppendLog(unittest.TestCase):

    def _run(self, timestamp, url, title, tag='', filename='',
             record_id=FIXED_REC_ID) -> str:
        """Call append_log with a temp file and return its contents."""
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'visits.log')
            with patch.object(host, 'LOG_FILE', path):
                host.append_log(record_id, timestamp, url, title, tag, filename)
            return Path(path).read_text(encoding='utf-8')

    def test_tsv_format(self):
        content = self._run('2026-01-01T00:00:00Z', 'https://example.com', 'Example Domain')
        self.assertEqual(
            content,
            f'{FIXED_REC_ID}\t2026-01-01T00:00:00Z\thttps://example.com\tExample Domain\n',
        )

    def test_tab_in_title_replaced(self):
        content = self._run('ts', 'https://a.com', 'Part1\tPart2').rstrip('\n')
        parts = content.split('\t')
        self.assertEqual(parts[3], 'Part1 Part2')

    def test_newline_in_title_replaced(self):
        content = self._run('ts', 'https://a.com', 'Line1\nLine2')
        self.assertEqual(len(content.splitlines()), 1)
        self.assertIn('Line1 Line2', content)

    def test_carriage_return_stripped(self):
        # \r is removed entirely (not replaced with a space), collapsing the surrounding text
        content = self._run('ts', 'https://a.com', 'Title\rStuff').rstrip('\n')
        parts = content.split('\t')
        self.assertEqual(parts[3], 'TitleStuff')

    def test_unicode_preserved(self):
        content = self._run('ts', 'https://a.com', '日本語タイトル')
        self.assertIn('日本語タイトル', content)

    def test_appends_multiple_calls(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'visits.log')
            with patch.object(host, 'LOG_FILE', path):
                host.append_log(FIXED_REC_ID, 'ts1', 'https://a.com', 'A')
                host.append_log(FIXED_REC_ID, 'ts2', 'https://b.com', 'B')
            lines = Path(path).read_text().splitlines()
        self.assertEqual(len(lines), 2)
        self.assertIn('https://a.com', lines[0])
        self.assertIn('https://b.com', lines[1])

    def test_no_tag_produces_four_fields(self):
        content = self._run('ts', 'https://example.com', 'Example')
        parts = content.rstrip('\n').split('\t')
        self.assertEqual(len(parts), 4)
        self.assertEqual(parts[0], FIXED_REC_ID)

    def test_of_interest_tag_produces_five_fields_no_filename(self):
        content = self._run('ts', 'https://example.com', 'Example', tag='of_interest')
        parts = content.rstrip('\n').split('\t')
        self.assertEqual(len(parts), 5)
        self.assertEqual(parts[4], 'of_interest')

    def test_read_tag_produces_six_fields_with_filename(self):
        content = self._run('ts', 'https://example.com', 'Example',
                            tag='read', filename='abc.mhtml')
        parts = content.rstrip('\n').split('\t')
        self.assertEqual(len(parts), 6)
        self.assertEqual(parts[4], 'read')
        self.assertEqual(parts[5], 'abc.mhtml')

    def test_skimmed_tag_produces_six_fields_with_filename(self):
        content = self._run('ts', 'https://example.com', 'Example',
                            tag='skimmed', filename='def.mhtml')
        parts = content.rstrip('\n').split('\t')
        self.assertEqual(len(parts), 6)
        self.assertEqual(parts[4], 'skimmed')
        self.assertEqual(parts[5], 'def.mhtml')

    def test_read_tag_with_empty_filename_still_writes_field(self):
        # When the message arrives without a filename (defensive), the column
        # is still emitted so field-count parsing in replay stays uniform.
        content = self._run('ts', 'https://example.com', 'Example', tag='read')
        parts = content.rstrip('\n').split('\t')
        self.assertEqual(len(parts), 6)
        self.assertEqual(parts[5], '')

    def test_filename_sanitised(self):
        content = self._run('ts', 'https://a.com', 'Example',
                            tag='read', filename='nasty\tname.mhtml')
        parts = content.rstrip('\n').split('\t')
        self.assertEqual(parts[5], 'nasty name.mhtml')

    def test_record_id_sanitised(self):
        # Defensive: even though uuid4().hex never contains tabs, the helper
        # sanitises every field so a corrupted caller can't break the format.
        content = self._run('ts', 'https://a.com', 'Example',
                            record_id='broken\tid')
        parts = content.rstrip('\n').split('\t')
        self.assertEqual(parts[0], 'broken id')

    def test_append_result_log_writes_two_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'visits.log')
            with patch.object(host, 'LOG_FILE', path):
                host.append_result_log(FIXED_REC_ID, 'success')
            content = Path(path).read_text(encoding='utf-8')
        self.assertEqual(content, f'{FIXED_REC_ID}\tsuccess\n')

    def test_append_result_log_sanitises_tabs(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'visits.log')
            with patch.object(host, 'LOG_FILE', path):
                host.append_result_log(FIXED_REC_ID, 'error: foo\tbar')
            content = Path(path).read_text(encoding='utf-8').rstrip('\n')
        self.assertEqual(content, f'{FIXED_REC_ID}\terror: foo bar')

    def test_tag_sanitised(self):
        content = self._run('ts', 'https://example.com', 'Example', tag='mem\torable')
        parts = content.rstrip('\n').split('\t')
        self.assertEqual(parts[4], 'mem orable')


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

    def test_ensure_db_creates_skimmed_events_table(self):
        conn = self._conn()
        self.assertIn('skimmed_events', self._tables(conn))
        conn.close()

    def _event_cols(self, conn, table):
        return {r[1] for r in conn.execute(f'PRAGMA table_info({table})').fetchall()}

    def test_ensure_db_creates_filename_column_in_read_events(self):
        conn = self._conn()
        self.assertIn('filename', self._event_cols(conn, 'read_events'))
        conn.close()

    def test_ensure_db_creates_filename_column_in_skimmed_events(self):
        conn = self._conn()
        self.assertIn('filename', self._event_cols(conn, 'skimmed_events'))
        conn.close()

    def test_ensure_db_creates_directory_column_in_read_events(self):
        conn = self._conn()
        self.assertIn('directory', self._event_cols(conn, 'read_events'))
        conn.close()

    def test_ensure_db_creates_directory_column_in_skimmed_events(self):
        conn = self._conn()
        self.assertIn('directory', self._event_cols(conn, 'skimmed_events'))
        conn.close()

    def test_directory_column_defaults_to_downloads_dir(self):
        # An ad-hoc INSERT (without specifying directory) should pick up the
        # column's DEFAULT, which embeds DOWNLOADS_SNAPSHOTS_DIR at table
        # creation time.
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        conn.execute(
            "INSERT INTO read_events (url, timestamp, filename) VALUES (?, ?, ?)",
            ('https://example.com', 'ts-read', 'abc.mhtml'),
        )
        conn.commit()
        directory = conn.execute(
            "SELECT directory FROM read_events WHERE url = ?", ('https://example.com',)
        ).fetchone()[0]
        conn.close()
        self.assertEqual(directory, host.DOWNLOADS_SNAPSHOTS_DIR)

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

    def test_ensure_db_url_is_primary_key(self):
        conn = self._conn()
        pk_cols = {r[1] for r in conn.execute('PRAGMA table_info(visits)') if r[5] == 1}
        conn.close()
        self.assertEqual(pk_cols, {'url'})

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
        self.assertIn('skimmed_events', tables)

    def test_insert_visit_stores_all_fields(self):
        conn = self._conn()
        host.insert_visit(conn, '2026-01-01T00:00:00Z', 'https://example.com', 'Example')
        row = conn.execute('SELECT url, timestamp, title FROM visits').fetchone()
        conn.close()
        self.assertEqual(row, ('https://example.com', '2026-01-01T00:00:00Z', 'Example'))

    def test_insert_visit_does_not_set_status_fields(self):
        # insert_visit only writes url/timestamp/title; it must not touch any
        # status field — of_interest, read, or skimmed in visits.
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Title')
        row = conn.execute('SELECT of_interest, read, skimmed FROM visits').fetchone()
        conn.close()
        self.assertIsNone(row[0])   # of_interest stays NULL
        self.assertEqual(row[1], 0) # read counter stays 0
        self.assertEqual(row[2], 0) # skimmed counter stays 0

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

    def test_tag_visit_read_stores_basename_filename(self):
        # tag_visit should normalize Chrome's relative path to just the basename;
        # the parent directory lives in the directory column.
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'read', '2026-01-01T12:00:00Z',
                       filename='browser-visit-snapshots/abc123.mhtml')
        row = conn.execute(
            "SELECT filename FROM read_events WHERE url = ?", ('https://example.com',)
        ).fetchone()
        conn.close()
        self.assertEqual(row[0], 'abc123.mhtml')

    def test_tag_visit_read_records_default_directory(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'read', '2026-01-01T12:00:00Z',
                       filename='browser-visit-snapshots/abc.mhtml')
        directory = conn.execute(
            "SELECT directory FROM read_events WHERE url = ?", ('https://example.com',)
        ).fetchone()[0]
        conn.close()
        self.assertEqual(directory, host.DOWNLOADS_SNAPSHOTS_DIR)

    def test_tag_visit_read_increments_visits_read_counter(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'read', '2026-01-01T12:00:00Z')
        row = conn.execute('SELECT read FROM visits').fetchone()
        conn.close()
        self.assertEqual(row[0], 1)

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

    def test_tag_visit_read_twice_increments_counter_to_two(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'read', '2026-01-01T12:00:00Z')
        host.tag_visit(conn, 'https://example.com', 'read', '2026-01-02T12:00:00Z')
        count = conn.execute('SELECT read FROM visits').fetchone()[0]
        conn.close()
        self.assertEqual(count, 2)

    def test_tag_visit_read_duplicate_timestamp_does_not_increment_counter(self):
        # Regression test for the rowcount-gated counter increment: a duplicate
        # (url, timestamp) pair is dropped by INSERT OR IGNORE, so the visits.read
        # counter must not advance.  Replay safety hinges on this.
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'read', '2026-01-01T12:00:00Z')
        host.tag_visit(conn, 'https://example.com', 'read', '2026-01-01T12:00:00Z')
        count = conn.execute('SELECT read FROM visits').fetchone()[0]
        rows = conn.execute(
            "SELECT COUNT(*) FROM read_events WHERE url = ?", ('https://example.com',)
        ).fetchone()[0]
        conn.close()
        self.assertEqual(count, 1)
        self.assertEqual(rows, 1)

    def test_tag_visit_of_interest_does_not_touch_read_or_skimmed(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'of_interest', '2026-01-01T12:00:00Z')
        row = conn.execute('SELECT read, skimmed FROM visits').fetchone()
        conn.close()
        self.assertEqual(row[0], 0)  # read counter untouched
        self.assertEqual(row[1], 0)  # skimmed counter untouched

    def test_tag_visit_inserts_skimmed_event(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'skimmed', '2026-01-01T12:00:00Z')
        row = conn.execute(
            "SELECT timestamp FROM skimmed_events WHERE url = ?", ('https://example.com',)
        ).fetchone()
        conn.close()
        self.assertEqual(row[0], '2026-01-01T12:00:00Z')

    def test_tag_visit_skimmed_stores_basename_filename(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'skimmed', '2026-01-01T12:00:00Z',
                       filename='browser-visit-snapshots/def456.mhtml')
        row = conn.execute(
            "SELECT filename FROM skimmed_events WHERE url = ?", ('https://example.com',)
        ).fetchone()
        conn.close()
        self.assertEqual(row[0], 'def456.mhtml')

    def test_tag_visit_skimmed_records_default_directory(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'skimmed', '2026-01-01T12:00:00Z',
                       filename='browser-visit-snapshots/def.mhtml')
        directory = conn.execute(
            "SELECT directory FROM skimmed_events WHERE url = ?", ('https://example.com',)
        ).fetchone()[0]
        conn.close()
        self.assertEqual(directory, host.DOWNLOADS_SNAPSHOTS_DIR)

    def test_tag_visit_skimmed_increments_visits_skimmed_counter(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'skimmed', '2026-01-01T12:00:00Z')
        row = conn.execute('SELECT skimmed FROM visits').fetchone()
        conn.close()
        self.assertEqual(row[0], 1)

    def test_tag_visit_skimmed_twice_stores_both_timestamps(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'skimmed', '2026-01-01T12:00:00Z')
        host.tag_visit(conn, 'https://example.com', 'skimmed', '2026-01-02T12:00:00Z')
        rows = conn.execute(
            "SELECT timestamp FROM skimmed_events WHERE url = ? ORDER BY timestamp ASC",
            ('https://example.com',)
        ).fetchall()
        conn.close()
        self.assertEqual([r[0] for r in rows],
                         ['2026-01-01T12:00:00Z', '2026-01-02T12:00:00Z'])

    def test_tag_visit_skimmed_twice_increments_counter_to_two(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'skimmed', '2026-01-01T12:00:00Z')
        host.tag_visit(conn, 'https://example.com', 'skimmed', '2026-01-02T12:00:00Z')
        count = conn.execute('SELECT skimmed FROM visits').fetchone()[0]
        conn.close()
        self.assertEqual(count, 2)

    def test_tag_visit_skimmed_duplicate_timestamp_does_not_increment_counter(self):
        # Mirror of the read-side regression test: duplicate (url, timestamp) is
        # dropped by INSERT OR IGNORE, so visits.skimmed must not advance.
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'skimmed', '2026-01-01T12:00:00Z')
        host.tag_visit(conn, 'https://example.com', 'skimmed', '2026-01-01T12:00:00Z')
        count = conn.execute('SELECT skimmed FROM visits').fetchone()[0]
        rows = conn.execute(
            "SELECT COUNT(*) FROM skimmed_events WHERE url = ?", ('https://example.com',)
        ).fetchone()[0]
        conn.close()
        self.assertEqual(count, 1)
        self.assertEqual(rows, 1)

    def test_tag_visit_skimmed_does_not_set_of_interest_or_read(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'skimmed', '2026-01-01T12:00:00Z')
        row = conn.execute('SELECT of_interest, read FROM visits').fetchone()
        conn.close()
        self.assertIsNone(row[0])    # of_interest untouched
        self.assertEqual(row[1], 0)  # read counter untouched

    def test_tag_visit_no_existing_visit_returns_false(self):
        conn = self._conn()
        found = host.tag_visit(conn, 'https://example.com', 'of_interest', 'ts')
        count = conn.execute('SELECT COUNT(*) FROM visits').fetchone()[0]
        conn.close()
        self.assertFalse(found)
        self.assertEqual(count, 0)

    def test_tag_visit_read_returns_false_when_visit_does_not_exist(self):
        # Exercises the False branch of `if exists:` in _insert_event: with no
        # matching visit row, no event row should be inserted and tag_visit
        # returns False.
        conn = self._conn()
        found = host.tag_visit(conn, 'https://nope.com', 'read', 'ts-read',
                               filename='browser-visit-snapshots/foo.mhtml')
        event_count = conn.execute('SELECT COUNT(*) FROM read_events').fetchone()[0]
        conn.close()
        self.assertFalse(found)
        self.assertEqual(event_count, 0)

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
        self.assertIsNone(row[0])    # of_interest untouched
        self.assertEqual(row[1], 0)  # read counter untouched
        self.assertEqual(row[2], 0)  # skimmed counter untouched

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
        self.assertEqual(result['read'],    [])   # empty list — never read
        self.assertEqual(result['skimmed'], [])   # empty list — never skimmed

    def test_tag_fields_reflect_applied_tags(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts-visit', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'of_interest', 'ts-mem')
        host.tag_visit(conn, 'https://example.com', 'read', 'ts-read',
                       filename='browser-visit-snapshots/abc.mhtml')
        result = host.query_visit(conn, 'https://example.com')
        conn.close()
        self.assertTrue(result['of_interest'])       # boolean True (stored as 1)
        self.assertEqual(result['read'], [
            {'timestamp': 'ts-read',
             'filename': 'abc.mhtml',
             'directory': host.DOWNLOADS_SNAPSHOTS_DIR},
        ])
        self.assertEqual(result['skimmed'], [])       # not skimmed — empty list

    def test_query_visit_returns_all_read_events_in_order(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts-visit', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'read', '2026-01-01T10:00:00Z',
                       filename='browser-visit-snapshots/f1.mhtml')
        host.tag_visit(conn, 'https://example.com', 'read', '2026-01-02T10:00:00Z',
                       filename='browser-visit-snapshots/f2.mhtml')
        result = host.query_visit(conn, 'https://example.com')
        conn.close()
        self.assertEqual(result['read'], [
            {'timestamp': '2026-01-01T10:00:00Z', 'filename': 'f1.mhtml',
             'directory': host.DOWNLOADS_SNAPSHOTS_DIR},
            {'timestamp': '2026-01-02T10:00:00Z', 'filename': 'f2.mhtml',
             'directory': host.DOWNLOADS_SNAPSHOTS_DIR},
        ])

    def test_query_visit_returns_all_skimmed_events_in_order(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts-visit', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'skimmed', '2026-01-01T10:00:00Z',
                       filename='browser-visit-snapshots/s1.mhtml')
        host.tag_visit(conn, 'https://example.com', 'skimmed', '2026-01-02T10:00:00Z',
                       filename='browser-visit-snapshots/s2.mhtml')
        result = host.query_visit(conn, 'https://example.com')
        conn.close()
        self.assertEqual(result['skimmed'], [
            {'timestamp': '2026-01-01T10:00:00Z', 'filename': 's1.mhtml',
             'directory': host.DOWNLOADS_SNAPSHOTS_DIR},
            {'timestamp': '2026-01-02T10:00:00Z', 'filename': 's2.mhtml',
             'directory': host.DOWNLOADS_SNAPSHOTS_DIR},
        ])

    def test_query_visit_includes_basename_filename_in_read_events(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'read', 'ts-read',
                       filename='browser-visit-snapshots/myfile.mhtml')
        result = host.query_visit(conn, 'https://example.com')
        conn.close()
        self.assertEqual(result['read'][0]['filename'], 'myfile.mhtml')

    def test_query_visit_includes_basename_filename_in_skimmed_events(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'skimmed', 'ts-skim',
                       filename='browser-visit-snapshots/myfile.pdf')
        result = host.query_visit(conn, 'https://example.com')
        conn.close()
        self.assertEqual(result['skimmed'][0]['filename'], 'myfile.pdf')

    def test_query_visit_includes_directory_in_read_events(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'read', 'ts-read',
                       filename='browser-visit-snapshots/myfile.mhtml')
        result = host.query_visit(conn, 'https://example.com')
        conn.close()
        self.assertEqual(result['read'][0]['directory'], host.DOWNLOADS_SNAPSHOTS_DIR)

    def test_query_visit_includes_directory_in_skimmed_events(self):
        conn = self._conn()
        host.insert_visit(conn, 'ts', 'https://example.com', 'Example')
        host.tag_visit(conn, 'https://example.com', 'skimmed', 'ts-skim',
                       filename='browser-visit-snapshots/myfile.pdf')
        result = host.query_visit(conn, 'https://example.com')
        conn.close()
        self.assertEqual(result['skimmed'][0]['directory'], host.DOWNLOADS_SNAPSHOTS_DIR)

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
        action_parts = lines[0].split('\t')
        result_parts = lines[1].split('\t')
        # Action and result must share the same record_id (UUID hex prefix).
        self.assertEqual(action_parts[0], result_parts[0])
        self.assertRegex(action_parts[0], r'^[0-9a-f]{32}$')
        self.assertIn('https://example.com', lines[0])
        self.assertEqual(result_parts[1], 'success')

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

    def test_tag_without_prior_visit_still_returns_ok(self):
        # Tagging creates the visit row implicitly (INSERT OR IGNORE) so there is
        # no longer a "no record found" error — the tag always succeeds.
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._call_main(
                {'timestamp': 'ts', 'url': 'https://example.com',
                 'title': 'Title', 'tag': 'of_interest'},
                tmp,
            )
        self.assertEqual(resp['status'], 'ok')

    def test_tag_without_prior_visit_creates_visit_record(self):
        # When a tag message arrives for a URL not yet in the DB, a visit row
        # must be created so that subsequent queries return the record.
        with tempfile.TemporaryDirectory() as tmp:
            self._call_main(
                {'timestamp': 'ts-tag', 'url': 'https://example.com',
                 'title': 'Title', 'tag': 'of_interest'},
                tmp,
            )
            conn = sqlite3.connect(os.path.join(tmp, 'visits.db'))
            row = conn.execute(
                'SELECT url, timestamp, of_interest FROM visits'
            ).fetchone()
            conn.close()
        self.assertEqual(row[0], 'https://example.com')
        self.assertEqual(row[1], 'ts-tag')
        self.assertEqual(row[2], '1')  # of_interest was applied

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
        """append_result_log failure on the DB-error path doesn't swallow the error response."""
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._call_main(
                {'timestamp': 'ts', 'url': 'https://example.com', 'title': 'Title'},
                tmp,
                extra_patches=[
                    patch('sqlite3.connect',
                          side_effect=sqlite3.OperationalError('disk full')),
                    patch.object(host, 'append_result_log',
                                 side_effect=OSError('disk full')),
                ],
            )
        self.assertEqual(resp['status'], 'error')
        self.assertTrue(any('db' in e for e in resp.get('errors', [])))


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
        action_parts = lines[0].split('\t')
        result_parts = lines[1].split('\t')
        self.assertRegex(action_parts[0], r'^[0-9a-f]{32}$')
        self.assertEqual(action_parts[1:], ['2026-01-01T00:00:00Z',
                                            'https://example.com', 'Example Domain'])
        self.assertEqual(result_parts, [action_parts[0], 'success'])

    def test_sqlite_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._invoke(
                {'timestamp': '2026-01-01T00:00:00Z', 'url': 'https://example.com', 'title': 'Example Domain'},
                tmp,
            )
            conn = sqlite3.connect(os.path.join(tmp, 'visits.db'))
            row = conn.execute(
                'SELECT url, timestamp, title, of_interest, read, skimmed FROM visits'
            ).fetchone()
            conn.close()
        self.assertEqual(row[:3], ('https://example.com', '2026-01-01T00:00:00Z', 'Example Domain'))
        self.assertIsNone(row[3])   # of_interest
        self.assertEqual(row[4], 0) # read counter starts at 0
        self.assertEqual(row[5], 0) # skimmed counter starts at 0

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
            of_interest   = conn.execute('SELECT of_interest FROM visits').fetchone()[0]
            read_counter  = conn.execute('SELECT read    FROM visits').fetchone()[0]
            skim_counter  = conn.execute('SELECT skimmed FROM visits').fetchone()[0]
            read_count    = conn.execute('SELECT COUNT(*) FROM read_events').fetchone()[0]
            skimmed_count = conn.execute('SELECT COUNT(*) FROM skimmed_events').fetchone()[0]
            conn.close()
        self.assertEqual(of_interest, '1')  # TEXT affinity stores literal 1 as '1'
        self.assertEqual(read_counter,  0)   # read counter untouched
        self.assertEqual(skim_counter,  0)   # skimmed counter untouched
        self.assertEqual(read_count,    0)   # no read_events row created
        self.assertEqual(skimmed_count, 0)   # no skimmed_events row created

    def test_tag_message_inserts_read_event(self):
        url = 'https://example.com'
        with tempfile.TemporaryDirectory() as tmp:
            self._invoke(
                {'timestamp': 'ts-visit', 'url': url, 'title': 'Example'}, tmp)
            resp = self._invoke(
                {'timestamp': 'ts-tag', 'url': url, 'title': 'Example', 'tag': 'read'}, tmp)
            self.assertEqual(resp['status'], 'ok')
            conn = sqlite3.connect(os.path.join(tmp, 'visits.db'))
            event        = conn.execute(
                'SELECT timestamp FROM read_events WHERE url = ?', (url,)
            ).fetchone()
            read_counter = conn.execute('SELECT read FROM visits').fetchone()[0]
            conn.close()
        self.assertIsNotNone(event)
        self.assertEqual(event[0], 'ts-tag')
        self.assertEqual(read_counter, 1)  # counter incremented to 1

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
            events       = conn.execute(
                'SELECT timestamp FROM read_events WHERE url = ? ORDER BY timestamp ASC', (url,)
            ).fetchall()
            read_counter = conn.execute('SELECT read FROM visits').fetchone()[0]
            conn.close()
        self.assertEqual([e[0] for e in events], ['ts-read-1', 'ts-read-2'])
        self.assertEqual(read_counter, 2)  # counter incremented twice

    def test_tag_message_inserts_skimmed_event(self):
        url = 'https://example.com'
        with tempfile.TemporaryDirectory() as tmp:
            self._invoke(
                {'timestamp': 'ts-visit', 'url': url, 'title': 'Example'}, tmp)
            resp = self._invoke(
                {'timestamp': 'ts-tag', 'url': url, 'title': 'Example', 'tag': 'skimmed'}, tmp)
            self.assertEqual(resp['status'], 'ok')
            conn = sqlite3.connect(os.path.join(tmp, 'visits.db'))
            event         = conn.execute(
                'SELECT timestamp FROM skimmed_events WHERE url = ?', (url,)
            ).fetchone()
            of_interest   = conn.execute('SELECT of_interest FROM visits').fetchone()[0]
            read_count    = conn.execute('SELECT COUNT(*) FROM read_events').fetchone()[0]
            skim_counter  = conn.execute('SELECT skimmed FROM visits').fetchone()[0]
            conn.close()
        self.assertIsNotNone(event)
        self.assertEqual(event[0], 'ts-tag')
        self.assertIsNone(of_interest)    # of_interest untouched
        self.assertEqual(read_count,   0) # no read_events row created
        self.assertEqual(skim_counter, 1) # skimmed counter incremented to 1

    def test_tag_message_skimmed_twice_stores_both_events(self):
        url = 'https://example.com'
        with tempfile.TemporaryDirectory() as tmp:
            self._invoke(
                {'timestamp': 'ts-visit', 'url': url, 'title': 'Example'}, tmp)
            self._invoke(
                {'timestamp': 'ts-skim-1', 'url': url, 'title': 'Example', 'tag': 'skimmed'}, tmp)
            resp = self._invoke(
                {'timestamp': 'ts-skim-2', 'url': url, 'title': 'Example', 'tag': 'skimmed'}, tmp)
            self.assertEqual(resp['status'], 'ok')
            conn = sqlite3.connect(os.path.join(tmp, 'visits.db'))
            events       = conn.execute(
                'SELECT timestamp FROM skimmed_events WHERE url = ? ORDER BY timestamp ASC', (url,)
            ).fetchall()
            skim_counter = conn.execute('SELECT skimmed FROM visits').fetchone()[0]
            conn.close()
        self.assertEqual([e[0] for e in events], ['ts-skim-1', 'ts-skim-2'])
        self.assertEqual(skim_counter, 2)  # counter incremented twice

    def test_tag_message_appends_six_field_log_line(self):
        url = 'https://example.com'
        with tempfile.TemporaryDirectory() as tmp:
            self._invoke(
                {'timestamp': 'ts-visit', 'url': url, 'title': 'Example'}, tmp)
            self._invoke(
                {'timestamp': 'ts-tag', 'url': url, 'title': 'Example',
                 'tag': 'read', 'filename': 'browser-visit-snapshots/abc.mhtml'},
                tmp)
            lines = Path(tmp, 'visits.log').read_text().splitlines()
        # lines[0]=visit action, lines[1]=visit result, lines[2]=tag action, lines[3]=tag result
        self.assertEqual(len(lines), 4)
        tag_action = lines[2].split('\t')
        tag_result = lines[3].split('\t')
        # 6 fields: record_id, timestamp, url, title, tag, filename
        self.assertEqual(len(tag_action), 6)
        self.assertRegex(tag_action[0], r'^[0-9a-f]{32}$')
        self.assertEqual(tag_action[4], 'read')
        # filename is logged as Chrome reports it (Downloads-relative); host
        # normalises to basename only when writing the DB row.
        self.assertEqual(tag_action[5], 'browser-visit-snapshots/abc.mhtml')
        self.assertEqual(tag_result, [tag_action[0], 'success'])

    def test_auto_log_appends_four_field_log_line(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._invoke(
                {'timestamp': 'ts', 'url': 'https://example.com', 'title': 'Example'},
                tmp,
            )
            lines = Path(tmp, 'visits.log').read_text().splitlines()
        action_parts = lines[0].split('\t')
        self.assertEqual(len(lines), 2)
        # 4 fields: record_id, timestamp, url, title
        self.assertEqual(len(action_parts), 4)
        self.assertRegex(action_parts[0], r'^[0-9a-f]{32}$')
        self.assertEqual(lines[1].split('\t'), [action_parts[0], 'success'])

    def test_tag_without_prior_visit_succeeds_and_creates_visit(self):
        # Tagging a URL with no prior auto-log entry must now succeed: the host
        # implicitly inserts the visit row before applying the tag.
        with tempfile.TemporaryDirectory() as tmp:
            resp = self._invoke(
                {'timestamp': 'ts', 'url': 'https://example.com', 'title': 'Example', 'tag': 'of_interest'},
                tmp,
            )
            self.assertEqual(resp['status'], 'ok')
            # The visit row was created with the tag timestamp
            conn = sqlite3.connect(os.path.join(tmp, 'visits.db'))
            row = conn.execute(
                'SELECT timestamp, of_interest FROM visits WHERE url = ?',
                ('https://example.com',)
            ).fetchone()
            conn.close()
            # Log should show action line + success (not an error)
            lines = Path(tmp, 'visits.log').read_text().splitlines()
        self.assertIsNotNone(row)
        self.assertEqual(row[0], 'ts')       # timestamp from tag message
        self.assertEqual(row[1], '1')        # of_interest applied
        self.assertEqual(len(lines), 2)
        self.assertEqual(lines[1].split('\t')[1], 'success')

    def test_two_invocations_use_distinct_record_ids(self):
        # Each main() call generates its own UUID; concurrent hosts can
        # interleave safely because the replay tool joins by record_id.
        with tempfile.TemporaryDirectory() as tmp:
            self._invoke(
                {'timestamp': 'ts1', 'url': 'https://a.com', 'title': 'A'}, tmp)
            self._invoke(
                {'timestamp': 'ts2', 'url': 'https://b.com', 'title': 'B'}, tmp)
            lines = Path(tmp, 'visits.log').read_text().splitlines()
        first_id  = lines[0].split('\t')[0]
        second_id = lines[2].split('\t')[0]
        self.assertNotEqual(first_id, second_id)
        # Each invocation's action and result share their own record_id
        self.assertEqual(lines[1].split('\t')[0], first_id)
        self.assertEqual(lines[3].split('\t')[0], second_id)

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
            self.assertTrue(resp['record']['of_interest'])       # boolean True
            self.assertEqual(resp['record']['read'],    [])      # never read → empty list
            self.assertEqual(resp['record']['skimmed'], [])      # never skimmed → empty list

    def test_tag_message_stores_basename_filename_in_read_events(self):
        url = 'https://example.com'
        with tempfile.TemporaryDirectory() as tmp:
            self._invoke(
                {'timestamp': 'ts-visit', 'url': url, 'title': 'Example'}, tmp)
            self._invoke(
                {'timestamp': 'ts-read', 'url': url, 'title': 'Example',
                 'tag': 'read', 'filename': 'browser-visit-snapshots/abc.mhtml'}, tmp)
            conn = sqlite3.connect(os.path.join(tmp, 'visits.db'))
            row = conn.execute(
                'SELECT filename, directory FROM read_events WHERE url = ?', (url,)
            ).fetchone()
            conn.close()
        self.assertEqual(row[0], 'abc.mhtml')
        self.assertEqual(row[1], host.DOWNLOADS_SNAPSHOTS_DIR)

    def test_tag_message_stores_basename_filename_in_skimmed_events(self):
        url = 'https://example.com'
        with tempfile.TemporaryDirectory() as tmp:
            self._invoke(
                {'timestamp': 'ts-visit', 'url': url, 'title': 'Example'}, tmp)
            self._invoke(
                {'timestamp': 'ts-skim', 'url': url, 'title': 'Example',
                 'tag': 'skimmed', 'filename': 'browser-visit-snapshots/def.pdf'}, tmp)
            conn = sqlite3.connect(os.path.join(tmp, 'visits.db'))
            row = conn.execute(
                'SELECT filename, directory FROM skimmed_events WHERE url = ?', (url,)
            ).fetchone()
            conn.close()
        self.assertEqual(row[0], 'def.pdf')
        self.assertEqual(row[1], host.DOWNLOADS_SNAPSHOTS_DIR)

    def test_query_returns_filename_and_directory_in_read_event(self):
        url = 'https://example.com'
        with tempfile.TemporaryDirectory() as tmp:
            self._invoke(
                {'timestamp': 'ts-visit', 'url': url, 'title': 'Example'}, tmp)
            self._invoke(
                {'timestamp': 'ts-read', 'url': url, 'title': 'Example',
                 'tag': 'read', 'filename': 'browser-visit-snapshots/snap.mhtml'}, tmp)
            resp = self._invoke({'action': 'query', 'url': url}, tmp)
        self.assertEqual(resp['record']['read'], [
            {'timestamp': 'ts-read', 'filename': 'snap.mhtml',
             'directory': host.DOWNLOADS_SNAPSHOTS_DIR},
        ])

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
