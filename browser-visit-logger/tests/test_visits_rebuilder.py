"""
Tests for native-host/visits_rebuilder.py.

Covers:

- Phase 1 ("log replay"): hand-built logs exercise the happy path
  (auto-log / of_interest / read / skimmed), error result skipping,
  orphan handling, malformed-line handling, and idempotency.
- Phase 2 ("filesystem rehydration"): an iCloud-archive layout with
  sealed and unsealed daily dirs and conforming + non-conforming files
  produces the expected snapshots rows and events.directory updates.
- CLI: --truncate / --no-truncate / --log-only / --rehydrate-only
  override flags, exit codes, mover_errors preservation, and a full
  end-to-end round-trip via the host.py + snapshot_mover pipeline.
"""
import io
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import host
import snapshot_mover
import visits_rebuilder as vr


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

REC_A = '0' * 32
REC_B = '1' * 32
REC_C = '2' * 32
REC_D = '3' * 32


def _write_log(path, *lines):
    """Write a log file from a list of TSV-pre-joined strings."""
    with open(path, 'w', encoding='utf-8') as f:
        for line in lines:
            f.write(line + '\n')


def _action(rec, ts, url, title, tag='', filename=None):
    """Compose an action line.  filename=None omits the field; '' includes empty."""
    fields = [rec, ts, url, title]
    if tag:
        fields.append(tag)
        if filename is not None:
            fields.append(filename)
    return '\t'.join(fields)


def _result(rec, payload):
    return rec + '\t' + payload


def _fresh_db(tmp):
    """Fresh DB with all four rebuildable tables created."""
    db = os.path.join(tmp, 'visits.db')
    conn = sqlite3.connect(db)
    host.ensure_db(conn)
    snapshot_mover._ensure_snapshots_table(conn)
    conn.close()
    return db


def _row_count(db, table):
    conn = sqlite3.connect(db)
    try:
        return conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Phase 1 — log replay
# ---------------------------------------------------------------------------

class TestReplayLog(unittest.TestCase):

    def setUp(self):
        self._tmpdirs = []

    def tearDown(self):
        import shutil
        for d in self._tmpdirs:
            shutil.rmtree(d, ignore_errors=True)

    def _replay(self, *lines, date_iso='2026-01-01'):
        """Common harness: write a single per-day log, replay against a
        fresh DB, return (db_path, stats).  The temp dir lives until
        tearDown.

        log_dir = <tmp>/logs (containing one browser-visits-<date>.log)
        icloud_dir = <tmp>/icloud (empty — phase 1 only walks the iCloud
                     dir for additional log files).
        """
        tmp = tempfile.mkdtemp()
        self._tmpdirs.append(tmp)
        log_dir    = os.path.join(tmp, 'logs');   os.makedirs(log_dir)
        icloud_dir = os.path.join(tmp, 'icloud'); os.makedirs(icloud_dir)
        log_path = os.path.join(log_dir, f'browser-visits-{date_iso}.log')
        _write_log(log_path, *lines)
        db = _fresh_db(tmp)
        conn = sqlite3.connect(db)
        try:
            stats = vr.replay_logs(conn, log_dir, icloud_dir)
        finally:
            conn.close()
        return db, stats

    def test_round_trips_visits(self):
        db, stats = self._replay(
            _action(REC_A, '2026-01-01T00:00:00Z', 'https://a.com', 'A'),
            _result(REC_A, 'success'),
            _action(REC_B, '2026-01-02T00:00:00Z', 'https://b.com', 'B'),
            _result(REC_B, 'success'),
        )
        conn = sqlite3.connect(db)
        rows = conn.execute(
            'SELECT url, timestamp, title FROM visits ORDER BY url'
        ).fetchall()
        conn.close()
        self.assertEqual(rows, [
            ('https://a.com', '2026-01-01T00:00:00Z', 'A'),
            ('https://b.com', '2026-01-02T00:00:00Z', 'B'),
        ])
        self.assertEqual(stats.visits_inserted, 2)
        self.assertEqual(stats.success_records, 2)
        self.assertFalse(stats.has_skipped_lines)

    def test_applies_of_interest(self):
        db, _ = self._replay(
            _action(REC_A, 'ts', 'https://a.com', 'A'),
            _result(REC_A, 'success'),
            _action(REC_B, 'ts', 'https://a.com', 'A', tag='of_interest'),
            _result(REC_B, 'success'),
        )
        conn = sqlite3.connect(db)
        of_interest = conn.execute(
            'SELECT of_interest FROM visits WHERE url = ?', ('https://a.com',)
        ).fetchone()[0]
        conn.close()
        # Stored as TEXT '1' per the schema.
        self.assertEqual(of_interest, '1')

    def test_applies_read_with_filename(self):
        db, stats = self._replay(
            _action(REC_A, 'ts0', 'https://a.com', 'A'),
            _result(REC_A, 'success'),
            _action(REC_B, 'ts1', 'https://a.com', 'A',
                    tag='read', filename='2026-01-01T00-00-00Z-abc.mhtml'),
            _result(REC_B, 'success'),
        )
        conn = sqlite3.connect(db)
        events = conn.execute(
            'SELECT timestamp, filename FROM read_events WHERE url = ?',
            ('https://a.com',),
        ).fetchall()
        counter = conn.execute('SELECT read FROM visits').fetchone()[0]
        conn.close()
        self.assertEqual(events, [('ts1', '2026-01-01T00-00-00Z-abc.mhtml')])
        self.assertEqual(counter, 1)
        self.assertEqual(stats.read_events, 1)

    def test_applies_skimmed_with_filename(self):
        db, stats = self._replay(
            _action(REC_A, 'ts0', 'https://a.com', 'A'),
            _result(REC_A, 'success'),
            _action(REC_B, 'ts1', 'https://a.com', 'A',
                    tag='skimmed', filename='2026-01-02T00-00-00Z-def.mhtml'),
            _result(REC_B, 'success'),
        )
        conn = sqlite3.connect(db)
        events = conn.execute(
            'SELECT timestamp, filename FROM skimmed_events WHERE url = ?',
            ('https://a.com',),
        ).fetchall()
        conn.close()
        self.assertEqual(events, [('ts1', '2026-01-02T00-00-00Z-def.mhtml')])
        self.assertEqual(stats.skimmed_events, 1)

    def test_skips_error_results(self):
        db, stats = self._replay(
            _action(REC_A, 'ts', 'https://a.com', 'A'),
            _result(REC_A, 'error: log: permission denied; db: disk full'),
            _action(REC_B, 'ts', 'https://b.com', 'B'),
            _result(REC_B, 'success'),
        )
        conn = sqlite3.connect(db)
        urls = [r[0] for r in conn.execute(
            'SELECT url FROM visits ORDER BY url').fetchall()]
        conn.close()
        # Only B should have made it in; A was a recorded failure.
        self.assertEqual(urls, ['https://b.com'])
        self.assertEqual(stats.error_records, 1)
        # An error pair is NOT an orphan — the host correctly emitted both lines.
        self.assertEqual(stats.orphan_actions, 0)
        self.assertEqual(stats.orphan_results, 0)
        self.assertFalse(stats.has_skipped_lines)

    def test_skips_orphan_action(self):
        db, stats = self._replay(
            _action(REC_A, 'ts', 'https://a.com', 'A'),
            # No matching result — orphan action.
            _action(REC_B, 'ts', 'https://b.com', 'B'),
            _result(REC_B, 'success'),
        )
        conn = sqlite3.connect(db)
        urls = [r[0] for r in conn.execute('SELECT url FROM visits').fetchall()]
        conn.close()
        self.assertEqual(urls, ['https://b.com'])
        self.assertEqual(stats.orphan_actions, 1)
        self.assertTrue(stats.has_skipped_lines)

    def test_skips_orphan_result(self):
        db, stats = self._replay(
            _result(REC_A, 'success'),  # no matching action
            _action(REC_B, 'ts', 'https://b.com', 'B'),
            _result(REC_B, 'success'),
        )
        conn = sqlite3.connect(db)
        urls = [r[0] for r in conn.execute('SELECT url FROM visits').fetchall()]
        conn.close()
        self.assertEqual(urls, ['https://b.com'])
        self.assertEqual(stats.orphan_results, 1)
        self.assertTrue(stats.has_skipped_lines)

    def test_skips_malformed_lines(self):
        db, stats = self._replay(
            'this is not a uuid line at all',
            _action(REC_A, 'ts', 'https://a.com', 'A'),
            _result(REC_A, 'success'),
        )
        conn = sqlite3.connect(db)
        n = conn.execute('SELECT COUNT(*) FROM visits').fetchone()[0]
        conn.close()
        self.assertEqual(n, 1)
        self.assertEqual(stats.malformed_lines, 1)
        self.assertTrue(stats.has_skipped_lines)

    def test_skips_action_with_wrong_field_count(self):
        # Right UUID prefix, wrong number of trailing fields → malformed.
        db, stats = self._replay(
            REC_A + '\textra1\textra2\textra3\textra4\textra5\textra6',
            _action(REC_B, 'ts', 'https://b.com', 'B'),
            _result(REC_B, 'success'),
        )
        conn = sqlite3.connect(db)
        n = conn.execute('SELECT COUNT(*) FROM visits').fetchone()[0]
        conn.close()
        self.assertEqual(n, 1)
        self.assertEqual(stats.malformed_lines, 1)

    def test_idempotent_with_no_truncate(self):
        # Replay the same log twice against the same DB — the second pass
        # must produce no additional rows or counter increments.  This is
        # the regression test for the rowcount-gated _insert_event fix.
        with tempfile.TemporaryDirectory() as tmp:
            log_dir    = os.path.join(tmp, 'logs');   os.makedirs(log_dir)
            icloud_dir = os.path.join(tmp, 'icloud'); os.makedirs(icloud_dir)
            log_path = os.path.join(log_dir, 'browser-visits-2026-01-01.log')
            _write_log(
                log_path,
                _action(REC_A, 'ts0', 'https://a.com', 'A'),
                _result(REC_A, 'success'),
                _action(REC_B, 'ts1', 'https://a.com', 'A',
                        tag='read', filename='abc.mhtml'),
                _result(REC_B, 'success'),
                _action(REC_C, 'ts2', 'https://a.com', 'A',
                        tag='skimmed', filename='def.mhtml'),
                _result(REC_C, 'success'),
                _action(REC_D, 'ts3', 'https://a.com', 'A', tag='of_interest'),
                _result(REC_D, 'success'),
            )
            db = _fresh_db(tmp)
            conn = sqlite3.connect(db)
            try:
                vr.replay_logs(conn, log_dir, icloud_dir)
                first = conn.execute(
                    'SELECT url, timestamp, title, of_interest, read, skimmed '
                    'FROM visits'
                ).fetchall()
                first_re = conn.execute(
                    'SELECT timestamp, filename FROM read_events'
                ).fetchall()
                first_se = conn.execute(
                    'SELECT timestamp, filename FROM skimmed_events'
                ).fetchall()

                vr.replay_logs(conn, log_dir, icloud_dir)
                second = conn.execute(
                    'SELECT url, timestamp, title, of_interest, read, skimmed '
                    'FROM visits'
                ).fetchall()
                second_re = conn.execute(
                    'SELECT timestamp, filename FROM read_events'
                ).fetchall()
                second_se = conn.execute(
                    'SELECT timestamp, filename FROM skimmed_events'
                ).fetchall()
            finally:
                conn.close()
        self.assertEqual(first, second)
        self.assertEqual(first_re, second_re)
        self.assertEqual(first_se, second_se)
        # Counters must remain 1, not 2.
        self.assertEqual(second[0][4], 1)  # read
        self.assertEqual(second[0][5], 1)  # skimmed

    def test_blank_lines_are_silently_skipped(self):
        # Empty lines in the middle of the log should not count as malformed —
        # they're a no-op (matches generic-text-log conventions).
        db, stats = self._replay(
            '',
            _action(REC_A, 'ts', 'https://a.com', 'A'),
            '',
            _result(REC_A, 'success'),
            '',
        )
        conn = sqlite3.connect(db)
        n = conn.execute('SELECT COUNT(*) FROM visits').fetchone()[0]
        conn.close()
        self.assertEqual(n, 1)
        self.assertEqual(stats.malformed_lines, 0)

    def test_duplicate_record_id_for_pending_action_drops_prior(self):
        # Defensive: should never happen with uuid4, but if a host process
        # somehow emits two action lines with the same record_id before any
        # result, the prior one is treated as orphan and the newer wins.
        db, stats = self._replay(
            _action(REC_A, 'ts0', 'https://a.com', 'A'),
            _action(REC_A, 'ts1', 'https://b.com', 'B'),  # collision
            _result(REC_A, 'success'),
        )
        conn = sqlite3.connect(db)
        urls = [r[0] for r in conn.execute('SELECT url FROM visits').fetchall()]
        conn.close()
        # The second action wins (it owns the result).
        self.assertEqual(urls, ['https://b.com'])
        self.assertEqual(stats.orphan_actions, 1)

    def test_unknown_tag_is_logged_but_does_not_crash(self):
        # Forward-compatibility: a future tag in the log shouldn't crash
        # the rebuild.  The visit row is still created; the tag itself is
        # ignored — counters stay 0, of_interest stays NULL, and no event
        # rows are created in either events table.
        db, stats = self._replay(
            _action(REC_A, 'ts', 'https://a.com', 'A', tag='wat'),
            _result(REC_A, 'success'),
        )
        conn = sqlite3.connect(db)
        row = conn.execute(
            'SELECT of_interest, read, skimmed FROM visits WHERE url = ?',
            ('https://a.com',),
        ).fetchone()
        n_read = conn.execute('SELECT COUNT(*) FROM read_events').fetchone()[0]
        n_skim = conn.execute('SELECT COUNT(*) FROM skimmed_events').fetchone()[0]
        conn.close()
        self.assertIsNotNone(row)
        self.assertIsNone(row[0])     # of_interest untouched
        self.assertEqual(row[1], 0)   # read counter untouched
        self.assertEqual(row[2], 0)   # skimmed counter untouched
        self.assertEqual(n_read, 0)
        self.assertEqual(n_skim, 0)
        self.assertEqual(stats.success_records, 1)
        self.assertEqual(stats.read_events, 0)
        self.assertEqual(stats.skimmed_events, 0)
        self.assertEqual(stats.of_interest_set, 0)


class TestReplayLogsMultiFile(unittest.TestCase):
    """Replay across multiple per-day log files in log_dir + iCloud subdirs."""

    def setUp(self):
        self._tmpdirs = []

    def tearDown(self):
        import shutil
        for d in self._tmpdirs:
            shutil.rmtree(d, ignore_errors=True)

    def _setup(self):
        tmp = tempfile.mkdtemp()
        self._tmpdirs.append(tmp)
        log_dir    = os.path.join(tmp, 'logs');   os.makedirs(log_dir)
        icloud_dir = os.path.join(tmp, 'icloud'); os.makedirs(icloud_dir)
        db = _fresh_db(tmp)
        return tmp, log_dir, icloud_dir, db

    def test_reads_logs_from_log_dir_and_icloud_subdirs(self):
        tmp, log_dir, icloud_dir, db = self._setup()
        # Past-day log inside iCloud (sealed day).
        sealed_dir = os.path.join(icloud_dir, '2026-04-29')
        os.makedirs(sealed_dir)
        _write_log(
            os.path.join(sealed_dir, 'browser-visits-2026-04-29.log'),
            _action(REC_A, '2026-04-29T10:00:00Z', 'https://past.com', 'Past'),
            _result(REC_A, 'success'),
        )
        # Today-ish log in log_dir.
        _write_log(
            os.path.join(log_dir, 'browser-visits-2026-05-01.log'),
            _action(REC_B, '2026-05-01T10:00:00Z', 'https://today.com', 'Today'),
            _result(REC_B, 'success'),
        )
        conn = sqlite3.connect(db)
        try:
            stats = vr.replay_logs(conn, log_dir, icloud_dir)
            urls = sorted(r[0] for r in conn.execute(
                'SELECT url FROM visits').fetchall())
        finally:
            conn.close()
        self.assertEqual(urls, ['https://past.com', 'https://today.com'])
        self.assertEqual(stats.visits_inserted, 2)

    def test_chronological_order_keeps_first_visit_timestamp(self):
        # A URL first visited day 1 and re-visited day 5: visits.timestamp
        # must end up as day 1's value (INSERT OR IGNORE keeps first).
        # We deliberately set up day-5's file lexicographically before
        # day-1's via os.listdir-order shenanigans; the rebuilder must
        # still process day-1 first because of the date-sort.
        tmp, log_dir, icloud_dir, db = self._setup()
        # Day 5 log in iCloud (under its sealed dir).
        os.makedirs(os.path.join(icloud_dir, '2026-05-05'))
        _write_log(
            os.path.join(icloud_dir, '2026-05-05',
                         'browser-visits-2026-05-05.log'),
            _action(REC_B, '2026-05-05T10:00:00Z',
                    'https://x.com', 'X (later)'),
            _result(REC_B, 'success'),
        )
        # Day 1 log in log_dir.
        _write_log(
            os.path.join(log_dir, 'browser-visits-2026-05-01.log'),
            _action(REC_A, '2026-05-01T10:00:00Z',
                    'https://x.com', 'X (first)'),
            _result(REC_A, 'success'),
        )
        conn = sqlite3.connect(db)
        try:
            vr.replay_logs(conn, log_dir, icloud_dir)
            row = conn.execute(
                'SELECT url, timestamp, title FROM visits WHERE url = ?',
                ('https://x.com',),
            ).fetchone()
        finally:
            conn.close()
        # First-visit timestamp wins.
        self.assertEqual(row, ('https://x.com', '2026-05-01T10:00:00Z',
                               'X (first)'))

    def test_split_by_seal_race_pairs_action_with_result(self):
        # Race orphan: action line in the iCloud log, matching result line
        # in log_dir's same-date log.  The rebuilder's shared `pending`
        # dict spans files, so it pairs them as a normal success.
        tmp, log_dir, icloud_dir, db = self._setup()
        date = '2026-04-29'
        os.makedirs(os.path.join(icloud_dir, date))
        # iCloud log has the action.
        _write_log(
            os.path.join(icloud_dir, date, f'browser-visits-{date}.log'),
            _action(REC_A, f'{date}T10:00:00Z', 'https://race.com', 'Race'),
        )
        # log_dir has the matching result.
        _write_log(
            os.path.join(log_dir, f'browser-visits-{date}.log'),
            _result(REC_A, 'success'),
        )
        conn = sqlite3.connect(db)
        try:
            stats = vr.replay_logs(conn, log_dir, icloud_dir)
            urls = [r[0] for r in conn.execute(
                'SELECT url FROM visits').fetchall()]
        finally:
            conn.close()
        # Action and result paired; no orphan reports.
        self.assertEqual(urls, ['https://race.com'])
        self.assertEqual(stats.success_records, 1)
        self.assertEqual(stats.orphan_actions, 0)
        self.assertEqual(stats.orphan_results, 0)

    def test_log_inside_icloud_dir_with_mismatched_date_is_skipped(self):
        # Defensive: a log file whose filename embeds date X but lives
        # inside iCloud's date-Y subdir is skipped with a warning (could
        # indicate corruption or a misplaced file).
        tmp, log_dir, icloud_dir, db = self._setup()
        os.makedirs(os.path.join(icloud_dir, '2026-04-29'))
        # File named for 2026-05-01 but inside the 2026-04-29 dir.
        _write_log(
            os.path.join(icloud_dir, '2026-04-29',
                         'browser-visits-2026-05-01.log'),
            _action(REC_A, 'ts', 'https://x.com', 'X'),
            _result(REC_A, 'success'),
        )
        conn = sqlite3.connect(db)
        try:
            with self.assertLogs(vr.logger, level='WARNING') as cm:
                stats = vr.replay_logs(conn, log_dir, icloud_dir)
            n = conn.execute('SELECT COUNT(*) FROM visits').fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(n, 0)
        self.assertEqual(stats.success_records, 0)
        self.assertTrue(any('does not match its parent directory' in m
                            for m in cm.output))

    def test_replay_inserts_snapshots_row_per_log_file(self):
        # Mirror of host.py: every log file's date should produce a
        # snapshots row (sealed=0).  Phase 2 may then flip sealed=1 for
        # iCloud-resident logs.
        tmp, log_dir, icloud_dir, db = self._setup()
        _write_log(
            os.path.join(log_dir, 'browser-visits-2026-05-01.log'),
            _action(REC_A, 'ts', 'https://a.com', 'A'),
            _result(REC_A, 'success'),
        )
        os.makedirs(os.path.join(icloud_dir, '2026-04-29'))
        _write_log(
            os.path.join(icloud_dir, '2026-04-29',
                         'browser-visits-2026-04-29.log'),
            _action(REC_B, 'ts', 'https://b.com', 'B'),
            _result(REC_B, 'success'),
        )
        conn = sqlite3.connect(db)
        try:
            vr.replay_logs(conn, log_dir, icloud_dir)
            rows = sorted(conn.execute(
                'SELECT date, sealed FROM snapshots').fetchall())
        finally:
            conn.close()
        # Both dates represented; both at sealed=0 (phase 2 not run here).
        self.assertEqual(rows, [('2026-04-29', 0), ('2026-05-01', 0)])

    def test_no_logs_anywhere_is_a_clean_noop(self):
        tmp, log_dir, icloud_dir, db = self._setup()
        conn = sqlite3.connect(db)
        try:
            stats = vr.replay_logs(conn, log_dir, icloud_dir)
            n = conn.execute('SELECT COUNT(*) FROM visits').fetchone()[0]
        finally:
            conn.close()
        self.assertEqual(n, 0)
        self.assertEqual(stats.visits_inserted, 0)
        self.assertEqual(stats.success_records, 0)

    def test_collect_log_paths_skips_non_log_entries(self):
        # _collect_log_paths must filter out:
        #  - non-date entries at the iCloud root
        #  - date-named *files* (not dirs) at the iCloud root
        #  - non-log entries inside a date subdir
        #  - non-regular-file log entries inside a date subdir
        #  - non-matching entries in log_dir
        tmp, log_dir, icloud_dir, db = self._setup()
        # Junk in iCloud root: a non-date dir and a date-named file.
        os.makedirs(os.path.join(icloud_dir, 'not-a-date'))
        Path(os.path.join(icloud_dir, '2026-04-30')).touch()
        # Inside a real date dir: a non-log filename and a directory-shaped
        # entry whose name matches the log regex.
        date_dir = os.path.join(icloud_dir, '2026-04-29')
        os.makedirs(date_dir)
        Path(os.path.join(date_dir, 'README.txt')).touch()
        os.makedirs(os.path.join(date_dir, 'browser-visits-2026-04-29.log'))
        # Junk in log_dir: a non-matching filename.
        Path(os.path.join(log_dir, 'unrelated.txt')).touch()
        # A real log file that should be picked up.
        _write_log(
            os.path.join(log_dir, 'browser-visits-2026-05-01.log'),
            _action(REC_A, 'ts', 'https://a.com', 'A'),
            _result(REC_A, 'success'),
        )

        paths = vr._collect_log_paths(log_dir, icloud_dir)
        self.assertEqual(
            paths,
            [('2026-05-01',
              os.path.join(log_dir, 'browser-visits-2026-05-01.log'))],
        )


# ---------------------------------------------------------------------------
# Phase 2 — filesystem rehydration
# ---------------------------------------------------------------------------

class TestRehydrateFilesystem(unittest.TestCase):

    def _setup_archive(self, tmp):
        """Build a minimal iCloud archive with one sealed + one unsealed dir,
        plus one non-conforming file, plus one conforming-but-orphan file."""
        icloud = os.path.join(tmp, 'icloud')
        sealed_date   = '2026-04-29'
        unsealed_date = '2026-04-30'
        os.makedirs(os.path.join(icloud, sealed_date))
        os.makedirs(os.path.join(icloud, unsealed_date))
        # MANIFEST present → sealed
        Path(os.path.join(icloud, sealed_date, 'MANIFEST.tsv')).write_text(
            'filename\ttag\ttimestamp\turl\ttitle\n')
        # Conforming snapshot files
        good_sealed   = '2026-04-29T10-00-00Z-aaa.mhtml'
        good_unsealed = '2026-04-30T11-00-00Z-bbb.mhtml'
        orphan_file   = '2026-04-30T12-00-00Z-ccc.mhtml'
        Path(os.path.join(icloud, sealed_date,   good_sealed)).touch()
        Path(os.path.join(icloud, unsealed_date, good_unsealed)).touch()
        Path(os.path.join(icloud, unsealed_date, orphan_file)).touch()
        # A non-conforming filename that should be silently ignored.
        Path(os.path.join(icloud, unsealed_date, 'README.txt')).touch()
        return icloud, good_sealed, good_unsealed, orphan_file, sealed_date, unsealed_date

    def test_upserts_snapshots_with_sealed_flag(self):
        with tempfile.TemporaryDirectory() as tmp:
            icloud, *_, sealed_date, unsealed_date = self._setup_archive(tmp)
            db = _fresh_db(tmp)
            conn = sqlite3.connect(db)
            try:
                stats = vr.rehydrate_filesystem(
                    conn, icloud, host.DOWNLOADS_SNAPSHOTS_DIR)
                rows = dict(conn.execute(
                    'SELECT date, sealed FROM snapshots ORDER BY date'
                ).fetchall())
            finally:
                conn.close()
        self.assertEqual(rows, {sealed_date: 1, unsealed_date: 0})
        self.assertEqual(stats.snapshots_upserted, 2)
        self.assertEqual(stats.sealed_dirs, 1)
        self.assertEqual(stats.unsealed_dirs, 1)

    def test_relocates_event_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            (icloud, good_sealed, good_unsealed, _orphan,
             sealed_date, unsealed_date) = self._setup_archive(tmp)
            db = _fresh_db(tmp)
            conn = sqlite3.connect(db)
            host.insert_visit(conn, 'ts0', 'https://a.com', 'A')
            host.insert_visit(conn, 'ts0', 'https://b.com', 'B')
            host.tag_visit(conn, 'https://a.com', 'read', 'ts1',
                           filename=good_sealed)
            host.tag_visit(conn, 'https://b.com', 'skimmed', 'ts2',
                           filename=good_unsealed)
            try:
                stats = vr.rehydrate_filesystem(
                    conn, icloud, host.DOWNLOADS_SNAPSHOTS_DIR)
                read_dir = conn.execute(
                    'SELECT directory FROM read_events WHERE url = ?',
                    ('https://a.com',)
                ).fetchone()[0]
                skim_dir = conn.execute(
                    'SELECT directory FROM skimmed_events WHERE url = ?',
                    ('https://b.com',)
                ).fetchone()[0]
            finally:
                conn.close()
        self.assertEqual(read_dir, os.path.join(icloud, sealed_date))
        self.assertEqual(skim_dir, os.path.join(icloud, unsealed_date))
        # 1 read event + 1 skimmed event relocated.
        self.assertEqual(stats.events_relocated, 2)
        # The orphan file (no events row) is reported but not relocated.
        self.assertEqual(stats.files_without_events, 1)

    def test_does_not_touch_already_relocated_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            icloud, good_sealed, *_, sealed_date, _ = self._setup_archive(tmp)
            db = _fresh_db(tmp)
            conn = sqlite3.connect(db)
            host.insert_visit(conn, 'ts0', 'https://a.com', 'A')
            # Manually insert with iCloud directory already set.
            already = os.path.join(icloud, sealed_date)
            conn.execute(
                "INSERT INTO read_events (url, timestamp, filename, directory) "
                "VALUES (?, ?, ?, ?)",
                ('https://a.com', 'ts1', good_sealed, already),
            )
            conn.commit()
            try:
                vr.rehydrate_filesystem(
                    conn, icloud, host.DOWNLOADS_SNAPSHOTS_DIR)
                row = conn.execute(
                    'SELECT directory FROM read_events WHERE url = ?',
                    ('https://a.com',)
                ).fetchone()
            finally:
                conn.close()
        # Directory unchanged (and still equal to the original iCloud path).
        self.assertEqual(row[0], already)

    def test_skips_non_date_entries_at_icloud_root(self):
        # A file named like a date and a directory not named like a date are
        # both ignored.  Only entries that match _DATE_DIR_RE *and* are
        # directories produce snapshots rows.
        with tempfile.TemporaryDirectory() as tmp:
            icloud = os.path.join(tmp, 'icloud')
            os.makedirs(os.path.join(icloud, '2026-04-29'))   # real date dir
            os.makedirs(os.path.join(icloud, 'not-a-date'))   # ignored dir
            Path(os.path.join(icloud, '2026-04-30')).touch()  # date-named file
            db = _fresh_db(tmp)
            conn = sqlite3.connect(db)
            try:
                stats = vr.rehydrate_filesystem(
                    conn, icloud, host.DOWNLOADS_SNAPSHOTS_DIR)
                rows = conn.execute(
                    'SELECT date FROM snapshots ORDER BY date'
                ).fetchall()
            finally:
                conn.close()
        self.assertEqual([r[0] for r in rows], ['2026-04-29'])
        self.assertEqual(stats.snapshots_upserted, 1)

    def test_missing_icloud_dir_is_a_noop(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = _fresh_db(tmp)
            conn = sqlite3.connect(db)
            try:
                stats = vr.rehydrate_filesystem(
                    conn, os.path.join(tmp, 'no-such-icloud'),
                    host.DOWNLOADS_SNAPSHOTS_DIR)
                n = conn.execute('SELECT COUNT(*) FROM snapshots').fetchone()[0]
            finally:
                conn.close()
        self.assertEqual(n, 0)
        self.assertEqual(stats.snapshots_upserted, 0)

    def test_rehydrate_skips_per_day_log_file_in_date_dir(self):
        # A sealed iCloud date dir contains the per-day log alongside
        # snapshot files.  The log file must NOT be reported as
        # files_without_events — it's expected and replayed by phase 1.
        with tempfile.TemporaryDirectory() as tmp:
            icloud = os.path.join(tmp, 'icloud')
            date = '2026-04-29'
            date_dir = os.path.join(icloud, date)
            os.makedirs(date_dir)
            Path(os.path.join(date_dir, 'MANIFEST.tsv')).write_text(
                'filename\ttag\ttimestamp\turl\ttitle\n')
            Path(os.path.join(date_dir, f'browser-visits-{date}.log')).write_text(
                '')
            db = _fresh_db(tmp)
            conn = sqlite3.connect(db)
            try:
                stats = vr.rehydrate_filesystem(
                    conn, icloud, host.DOWNLOADS_SNAPSHOTS_DIR)
            finally:
                conn.close()
        # Log file is not counted as a file_without_events.
        self.assertEqual(stats.files_without_events, 0)
        # Snapshots row was inserted as sealed=1.
        self.assertEqual(stats.snapshots_upserted, 1)
        self.assertEqual(stats.sealed_dirs, 1)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

class _CLIBase(unittest.TestCase):
    """Shared setup: build a temp dir with a log + DB + source/dest dirs and
    return the CLI flag list."""

    def _mk(self, lines=(), date_iso='2026-01-01'):
        """Create a tmp dir and return (tmp, args, paths).

        log_dir holds <log_dir>/browser-visits-<date_iso>.log seeded with
        the given lines.  Use date_iso to pin the filename so tests can
        also read it back from paths['log'].
        """
        tmp = tempfile.mkdtemp()
        paths = {
            'log_dir': os.path.join(tmp, 'logs'),
            'log':     None,  # filled in once log_dir exists
            'db':      os.path.join(tmp, 'visits.db'),
            'src':     os.path.join(tmp, 'dl'),
            'dest':    os.path.join(tmp, 'icloud'),
        }
        os.makedirs(paths['log_dir'])
        os.makedirs(paths['src'])
        os.makedirs(paths['dest'])
        paths['log'] = os.path.join(
            paths['log_dir'], f'browser-visits-{date_iso}.log')
        _write_log(paths['log'], *lines)
        args = ['--log-dir', paths['log_dir'],
                '--db',      paths['db'],
                '--source',  paths['src'],
                '--dest',    paths['dest']]
        return tmp, args, paths

    def _run(self, args):
        """Run vr.cli with the given args, capturing stdout, restoring globals."""
        # cli() mutates host.* and snapshot_mover.* globals; restore them so
        # one test doesn't pollute the next.
        saved = (
            host.DOWNLOADS_SNAPSHOTS_DIR, host.DB_FILE, host.LOG_DIR,
            snapshot_mover.ICLOUD_SNAPSHOTS_DIR, snapshot_mover.LOG_DIR,
        )
        buf = io.StringIO()
        try:
            with redirect_stdout(buf):
                rc = vr.cli(args)
        finally:
            (host.DOWNLOADS_SNAPSHOTS_DIR, host.DB_FILE, host.LOG_DIR,
             snapshot_mover.ICLOUD_SNAPSHOTS_DIR, snapshot_mover.LOG_DIR) = saved
        return rc, buf.getvalue()


class TestCLI(_CLIBase):

    def test_clean_run_exits_zero(self):
        tmp, args, paths = self._mk(lines=(
            _action(REC_A, 'ts', 'https://a.com', 'A'),
            _result(REC_A, 'success'),
        ))
        try:
            rc, out = self._run(args)
            self.assertEqual(rc, 0, out)
            self.assertIn('replay:',    out)
            self.assertIn('rehydrate:', out)
            self.assertEqual(_row_count(paths['db'], 'visits'), 1)
        finally:
            __import__('shutil').rmtree(tmp, ignore_errors=True)

    def test_orphans_trip_nonzero_exit_but_well_formed_records_still_apply(self):
        # Orphans should report (non-zero exit) without aborting the rebuild —
        # the well-formed records around them must still be applied.
        tmp, args, paths = self._mk(lines=(
            _action(REC_A, 'ts', 'https://a.com', 'A'),
            # orphan action — no result
            _action(REC_B, 'ts', 'https://b.com', 'B'),
            _result(REC_B, 'success'),
        ))
        try:
            rc, _ = self._run(args)
            self.assertEqual(rc, 1)
            urls = sorted(r[0] for r in sqlite3.connect(paths['db']).execute(
                'SELECT url FROM visits').fetchall())
            self.assertEqual(urls, ['https://b.com'])
        finally:
            __import__('shutil').rmtree(tmp, ignore_errors=True)

    def test_missing_log_dir_exits_nonzero(self):
        with tempfile.TemporaryDirectory() as tmp:
            args = ['--log-dir', os.path.join(tmp, 'no-such-dir'),
                    '--db',      os.path.join(tmp, 'visits.db')]
            rc, _ = self._run(args)
        self.assertEqual(rc, 1)

    def test_truncate_default_wipes_rebuildable_tables_but_keeps_mover_errors(self):
        tmp, args, paths = self._mk(lines=(
            _action(REC_A, 'ts0', 'https://a.com', 'A'),
            _result(REC_A, 'success'),
        ))
        try:
            # Pre-populate every rebuildable table + mover_errors with junk
            # rows so we can assert each is wiped (or preserved) correctly.
            conn = sqlite3.connect(paths['db'])
            host.ensure_db(conn)
            snapshot_mover._ensure_snapshots_table(conn)
            snapshot_mover._ensure_mover_errors_table(conn)
            host.insert_visit(conn, 'old-ts', 'https://stale.example', 'stale')
            conn.execute(
                "INSERT INTO read_events (url, timestamp, filename, directory) "
                "VALUES (?, ?, ?, ?)",
                ('https://stale.example', 'old-r', 'old.mhtml', '/old/dir'),
            )
            conn.execute(
                "INSERT INTO skimmed_events (url, timestamp, filename, directory) "
                "VALUES (?, ?, ?, ?)",
                ('https://stale.example', 'old-s', 'old.mhtml', '/old/dir'),
            )
            conn.execute(
                "INSERT INTO snapshots (date, sealed) VALUES (?, ?)",
                ('1999-01-01', 1),
            )
            conn.execute(
                "INSERT INTO mover_errors "
                "(key, operation, target, message, first_seen, last_seen) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                ('move:foo', 'move', 'foo', 'some msg', 'ts', 'ts'),
            )
            conn.commit()
            conn.close()

            rc, _ = self._run(args)
            self.assertEqual(rc, 0)

            conn = sqlite3.connect(paths['db'])
            urls = [r[0] for r in conn.execute(
                'SELECT url FROM visits ORDER BY url').fetchall()]
            stale_re = conn.execute(
                "SELECT 1 FROM read_events WHERE timestamp = 'old-r'"
            ).fetchone()
            stale_se = conn.execute(
                "SELECT 1 FROM skimmed_events WHERE timestamp = 'old-s'"
            ).fetchone()
            stale_snap = conn.execute(
                "SELECT 1 FROM snapshots WHERE date = '1999-01-01'"
            ).fetchone()
            mover_errors_count = conn.execute(
                'SELECT COUNT(*) FROM mover_errors').fetchone()[0]
            conn.close()
            # All four rebuildable tables truncated; only the replayed row remains.
            self.assertEqual(urls, ['https://a.com'])
            self.assertIsNone(stale_re)
            self.assertIsNone(stale_se)
            self.assertIsNone(stale_snap)
            # mover_errors row preserved across the rebuild.
            self.assertEqual(mover_errors_count, 1)
        finally:
            __import__('shutil').rmtree(tmp, ignore_errors=True)

    def test_no_truncate_preserves_existing_rows(self):
        tmp, args, paths = self._mk(lines=(
            _action(REC_A, 'ts', 'https://a.com', 'A'),
            _result(REC_A, 'success'),
        ))
        try:
            conn = sqlite3.connect(paths['db'])
            host.ensure_db(conn)
            host.insert_visit(conn, 'old-ts', 'https://kept.example', 'kept')
            conn.close()

            rc, _ = self._run(args + ['--no-truncate'])
            self.assertEqual(rc, 0)
            urls = sorted(r[0] for r in sqlite3.connect(paths['db']).execute(
                'SELECT url FROM visits').fetchall())
            self.assertEqual(urls, ['https://a.com', 'https://kept.example'])
        finally:
            __import__('shutil').rmtree(tmp, ignore_errors=True)

    def test_log_only_skips_rehydrate(self):
        tmp, args, paths = self._mk(lines=(
            _action(REC_A, 'ts', 'https://a.com', 'A'),
            _result(REC_A, 'success'),
        ))
        try:
            # Pre-create an iCloud date dir so rehydrate, if invoked, would
            # add a snapshots row for it; --log-only must suppress that.
            os.makedirs(os.path.join(paths['dest'], '2026-04-29'))
            rc, out = self._run(args + ['--log-only'])
            self.assertEqual(rc, 0)
            self.assertIn('replay:',     out)
            self.assertNotIn('rehydrate:', out)
            # Phase 1 inserts a snapshots row per log file's date; phase 2
            # was skipped, so the iCloud-only date 2026-04-29 must NOT
            # have produced a row.
            conn = sqlite3.connect(paths['db'])
            dates = sorted(r[0] for r in conn.execute(
                'SELECT date FROM snapshots').fetchall())
            conn.close()
            self.assertNotIn('2026-04-29', dates)
            # The log's date IS expected (phase 1 mirrors host.py's INSERT).
            self.assertEqual(dates, ['2026-01-01'])
        finally:
            __import__('shutil').rmtree(tmp, ignore_errors=True)

    def test_rehydrate_only_skips_replay_and_tolerates_missing_log(self):
        tmp, args, paths = self._mk(lines=())
        # Point at a non-existent log; --rehydrate-only must not require it.
        os.unlink(paths['log'])
        try:
            os.makedirs(os.path.join(paths['dest'], '2026-04-29'))
            rc, out = self._run(args + ['--rehydrate-only'])
            self.assertEqual(rc, 0, out)
            self.assertNotIn('replay:',  out)
            self.assertIn('rehydrate:', out)
            self.assertEqual(_row_count(paths['db'], 'snapshots'), 1)
            self.assertEqual(_row_count(paths['db'], 'visits'), 0)
        finally:
            __import__('shutil').rmtree(tmp, ignore_errors=True)

    def test_missing_db_parent_dir_exits_nonzero(self):
        with tempfile.TemporaryDirectory() as tmp:
            args = ['--log-dir', tmp,
                    '--db',      os.path.join(tmp, 'no-such-dir', 'visits.db')]
            rc, _ = self._run(args)
        self.assertEqual(rc, 1)

    def test_unexpected_exception_exits_two(self):
        # If the orchestrator raises (e.g. a bug or a corrupt DB file),
        # the CLI should swallow the traceback and exit with code 2 rather
        # than dumping a stacktrace at the user.
        tmp, args, _ = self._mk(lines=())
        try:
            with patch.object(vr, 'rebuild', side_effect=RuntimeError('boom')):
                rc, _ = self._run(args)
            self.assertEqual(rc, 2)
        finally:
            __import__('shutil').rmtree(tmp, ignore_errors=True)

    def test_verbose_flag_enables_debug_logging(self):
        tmp, args, _ = self._mk(lines=())
        try:
            rc, _ = self._run(args + ['-v'])
            self.assertEqual(rc, 0)
            self.assertEqual(vr.logger.level, __import__('logging').DEBUG)
        finally:
            # Reset logger level so we don't leak DEBUG into other tests.
            vr.logger.setLevel(__import__('logging').WARNING)
            __import__('shutil').rmtree(tmp, ignore_errors=True)

    def test_log_only_and_rehydrate_only_are_mutually_exclusive(self):
        tmp, args, _ = self._mk(lines=())
        try:
            with self.assertRaises(SystemExit):
                self._run(args + ['--log-only', '--rehydrate-only'])
        finally:
            __import__('shutil').rmtree(tmp, ignore_errors=True)


# ---------------------------------------------------------------------------
# End-to-end via host.py + snapshot_mover
# ---------------------------------------------------------------------------

class TestEndToEnd(unittest.TestCase):
    """Full pipeline round-trip: drive host.main() for a few messages, run
    the mover/sealer pass, snapshot the DB, wipe the DB, run rebuild, and
    diff.  The rebuilt DB must match the original (modulo mover_errors)."""

    def _drive_host(self, message, tmp):
        """Call host.main() inline with patched paths and a JSON message
        delivered via stdin in the native-messaging framing format."""
        import json, struct
        payload = json.dumps(message).encode('utf-8')
        framed = struct.pack('<I', len(payload)) + payload

        log_dir  = os.path.join(tmp, 'logs')
        os.makedirs(log_dir, exist_ok=True)
        db_path  = os.path.join(tmp, 'visits.db')
        src      = os.path.join(tmp, 'dl')

        stdin  = io.BytesIO(framed)
        stdout = io.BytesIO()
        stdin.buffer  = stdin   # host expects sys.stdin.buffer
        stdout.buffer = stdout  # and sys.stdout.buffer

        with patch.object(host, 'LOG_DIR', log_dir), \
             patch.object(host, 'DB_FILE', db_path), \
             patch.object(host, 'DOWNLOADS_SNAPSHOTS_DIR', src), \
             patch.object(sys, 'stdin',  stdin), \
             patch.object(sys, 'stdout', stdout):
            host.main()

    def _snapshot_tables(self, db_path):
        """Return a dict {table: list[tuple]} for the rebuildable tables.
        mover_errors is intentionally excluded — it's not log-recoverable
        and the rebuild leaves it untouched."""
        snap = {}
        conn = sqlite3.connect(db_path)
        try:
            snap['visits'] = conn.execute(
                'SELECT url, timestamp, title, of_interest, read, skimmed '
                'FROM visits ORDER BY url'
            ).fetchall()
            snap['read_events'] = conn.execute(
                'SELECT url, timestamp, filename, directory FROM read_events '
                'ORDER BY url, timestamp'
            ).fetchall()
            snap['skimmed_events'] = conn.execute(
                'SELECT url, timestamp, filename, directory FROM skimmed_events '
                'ORDER BY url, timestamp'
            ).fetchall()
            snap['snapshots'] = conn.execute(
                'SELECT date, sealed FROM snapshots ORDER BY date'
            ).fetchall()
        finally:
            conn.close()
        return snap

    def test_round_trip_matches_original(self):
        with tempfile.TemporaryDirectory() as tmp:
            src     = os.path.join(tmp, 'dl');     os.makedirs(src)
            dest    = os.path.join(tmp, 'icloud'); os.makedirs(dest)
            log_dir = os.path.join(tmp, 'logs')    # _drive_host creates it
            db      = os.path.join(tmp, 'visits.db')

            # Seed a few host invocations: visit, of_interest, read, skimmed.
            self._drive_host({'timestamp': '2026-04-29T10:00:00Z',
                              'url': 'https://a.com', 'title': 'A'}, tmp)
            self._drive_host({'timestamp': '2026-04-29T10:00:00Z',
                              'url': 'https://a.com', 'title': 'A',
                              'tag': 'of_interest'}, tmp)

            # Place a snapshot file so the mover has work to do.
            read_filename = '2026-04-29T11-00-00Z-aaa.mhtml'
            Path(os.path.join(src, read_filename)).touch()
            # Backdate so the mover's freshness gate (60s) lets it pass.
            os.utime(os.path.join(src, read_filename), (1, 1))
            self._drive_host({'timestamp': '2026-04-29T11:00:00Z',
                              'url': 'https://a.com', 'title': 'A',
                              'tag': 'read', 'filename': read_filename}, tmp)

            skim_filename = '2026-04-29T12-00-00Z-bbb.mhtml'
            Path(os.path.join(src, skim_filename)).touch()
            os.utime(os.path.join(src, skim_filename), (1, 1))
            self._drive_host({'timestamp': '2026-04-29T12:00:00Z',
                              'url': 'https://b.com', 'title': 'B',
                              'tag': 'skimmed', 'filename': skim_filename}, tmp)

            # Run a mover + seal pass against these paths.
            saved = (
                snapshot_mover.DOWNLOADS_SNAPSHOTS_DIR,
                snapshot_mover.ICLOUD_SNAPSHOTS_DIR,
                snapshot_mover.LOG_DIR,
                snapshot_mover.DB_FILE,
            )
            try:
                snapshot_mover.DOWNLOADS_SNAPSHOTS_DIR = src
                snapshot_mover.ICLOUD_SNAPSHOTS_DIR    = dest
                snapshot_mover.LOG_DIR                 = log_dir
                snapshot_mover.DB_FILE                 = db
                conn = sqlite3.connect(db)
                snapshot_mover._ensure_snapshots_table(conn)
                snapshot_mover._ensure_mover_errors_table(conn)
                snapshot_mover._move_pass(conn)
                snapshot_mover._seal_pass(conn)
                snapshot_mover._orphan_log_merge_pass(conn)
                conn.close()
            finally:
                (snapshot_mover.DOWNLOADS_SNAPSHOTS_DIR,
                 snapshot_mover.ICLOUD_SNAPSHOTS_DIR,
                 snapshot_mover.LOG_DIR,
                 snapshot_mover.DB_FILE) = saved

            # Snapshot table contents, wipe the DB, run the rebuild.
            before = self._snapshot_tables(db)
            os.unlink(db)

            saved2 = (host.DOWNLOADS_SNAPSHOTS_DIR, host.DB_FILE, host.LOG_DIR,
                      snapshot_mover.ICLOUD_SNAPSHOTS_DIR,
                      snapshot_mover.LOG_DIR)
            try:
                rc = vr.cli(['--log-dir', log_dir, '--db', db,
                             '--source', src, '--dest', dest])
            finally:
                (host.DOWNLOADS_SNAPSHOTS_DIR, host.DB_FILE, host.LOG_DIR,
                 snapshot_mover.ICLOUD_SNAPSHOTS_DIR,
                 snapshot_mover.LOG_DIR) = saved2
            self.assertEqual(rc, 0)

            after = self._snapshot_tables(db)

        # All four log/FS-recoverable tables must round-trip identically.
        # mover_errors is intentionally not compared — it's allowed to
        # diverge (rebuild leaves it untouched / starts empty).
        self.assertEqual(before, after)


# ---------------------------------------------------------------------------
# Wrapper smoke (subprocess so we exercise the bash side too)
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent


class TestWrapperSmoke(unittest.TestCase):

    def test_wrapper_runs_against_empty_log_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = os.path.join(tmp, 'logs');   os.makedirs(log_dir)
            db      = os.path.join(tmp, 'visits.db')
            src     = os.path.join(tmp, 'dl');     os.makedirs(src)
            dest    = os.path.join(tmp, 'icloud'); os.makedirs(dest)
            result = subprocess.run(
                [str(REPO_ROOT / 'rebuild_visits_data'),
                 '--log-dir', log_dir, '--db', db,
                 '--source', src, '--dest', dest],
                capture_output=True, text=True, timeout=10,
            )
        self.assertEqual(result.returncode, 0,
                         f'stderr: {result.stderr}')
        self.assertIn('replay:',    result.stdout)
        self.assertIn('rehydrate:', result.stdout)


if __name__ == '__main__':  # pragma: no cover
    unittest.main()
