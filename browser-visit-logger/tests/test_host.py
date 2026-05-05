"""Unit tests for the Python helpers in native-host/host.py.

After the Swift port these helpers (ensure_db, insert_visit, tag_visit)
are imported only by visits_rebuilder.py.  The native-messaging
protocol surface (read_message / write_message / main) lives in
swift/Sources/BVLHost/ and isn't exercised here any more.
"""
import sqlite3
import unittest

import host  # resolved via conftest.py sys.path insertion


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

    def test_ensure_db_creates_snapshots_table(self):
        # Owned by host.ensure_db so per-invocation snapshots-row inserts
        # succeed even on a brand-new install with no mover run yet.
        conn = self._conn()
        self.assertIn('snapshots', self._tables(conn))
        cols = {r[1] for r in conn.execute('PRAGMA table_info(snapshots)')}
        self.assertEqual(cols, {'date', 'sealed'})
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


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
