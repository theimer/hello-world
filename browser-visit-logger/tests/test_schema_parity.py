"""Parity check: schema.sql (canonical) vs swift/Sources/BVLCore/Schema.swift.

The Swift production writer can't share a runtime SQL string with the
Python rebuilder — it's compiled, not interpreted.  Instead we keep
schema.sql as the source of truth and assert here that every CREATE
TABLE / CREATE INDEX statement Schema.swift emits matches one in
schema.sql.

Strategy: extract the embedded SQL from each file, execute on an
in-memory SQLite, then introspect via sqlite_master / PRAGMA.
Comparing structured columns is far more robust than comparing
DDL text.

If this test fails, somebody changed one schema and not the other.
Update both, then re-run.
"""
import re
import sqlite3
import unittest
from pathlib import Path


REPO_ROOT     = Path(__file__).resolve().parent.parent
SCHEMA_SQL    = REPO_ROOT / 'schema.sql'
SCHEMA_SWIFT  = REPO_ROOT / 'swift' / 'Sources' / 'BVLCore' / 'Schema.swift'

# Both sides resolve their respective placeholder to the absolute downloads
# dir at runtime; for parity comparison we substitute a stable test path.
_PY_SENTINEL          = '__BVL_DOWNLOADS_SNAPSHOTS_DIR__'
_TEST_DOWNLOADS_PATH  = '/tmp/bvl-downloads'

_EVENTS_TABLE_NAMES = ('read_events', 'skimmed_events')


def _load_canonical_sql() -> str:
    return SCHEMA_SQL.read_text(encoding='utf-8').replace(
        _PY_SENTINEL, _TEST_DOWNLOADS_PATH,
    )


def _load_swift_sql() -> str:
    """Pull the embedded SQL out of Schema.swift.

    Two interpolations need preprocessing:

      `\\(name)`           — events-table function templates the table
                             name; expand into one block per call site.
      `\\(defaultDirLit)`  — a single-quoted absolute-path string built
                             at runtime; replace with the test path.
    """
    text = SCHEMA_SWIFT.read_text(encoding='utf-8')
    blocks = re.findall(r'db\.execute\(\"\"\"(.*?)\"\"\"\)', text, re.DOTALL)

    expanded = []
    for block in blocks:
        if r'\(name)' in block:
            for table in _EVENTS_TABLE_NAMES:
                expanded.append(block.replace(r'\(name)', table))
        else:
            expanded.append(block)

    sql = ';\n'.join(b.strip() for b in expanded) + ';'
    sql = re.sub(r"DEFAULT\s+\\\(defaultDirLit\)",
                 f"DEFAULT '{_TEST_DOWNLOADS_PATH}'", sql)
    return sql


def _introspect(sql: str) -> dict:
    """Exec sql on an in-memory DB and return a structured schema dict.

    Result shape:
        {
            'tables': {
                <name>: {
                    'columns': [(name, type, notnull, dflt, pk), ...],
                    'pk_cols': [<col>, ...],   # composite PK from PRAGMA
                },
                ...
            },
            'indexes': {(<name>, <table>, (<cols>,)), ...},
        }
    """
    with sqlite3.connect(':memory:') as conn:
        conn.executescript(sql)

        tables = {}
        for (name,) in conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
            "ORDER BY name"
        ):
            cols = [
                (cid_name_type[1], cid_name_type[2].upper(),
                 cid_name_type[3], cid_name_type[4], cid_name_type[5])
                for cid_name_type in conn.execute(f"PRAGMA table_info({name})")
            ]
            # Composite PRIMARY KEY (e.g. read_events) — table_info pk
            # column is 1-based for PK members, 0 otherwise.  Order them
            # by the pk index value to preserve column order.
            pk_cols = [c[0] for c in sorted(
                (col for col in cols if col[4] > 0),
                key=lambda col: col[4],
            )]
            tables[name] = {'columns': cols, 'pk_cols': tuple(pk_cols)}

        indexes = set()
        for (idx_name, table) in conn.execute(
            "SELECT name, tbl_name FROM sqlite_master "
            "WHERE type='index' AND name NOT LIKE 'sqlite_%'"
        ):
            cols = tuple(
                row[2] for row in conn.execute(f"PRAGMA index_info({idx_name})")
            )
            indexes.add((idx_name, table, cols))

    return {'tables': tables, 'indexes': indexes}


class TestSchemaParity(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.sql_schema   = _introspect(_load_canonical_sql())
        cls.swift_schema = _introspect(_load_swift_sql())

    def test_same_table_set(self):
        self.assertEqual(
            set(self.sql_schema['tables']),
            set(self.swift_schema['tables']),
        )

    def test_per_table_columns_match(self):
        for name in sorted(self.sql_schema['tables']):
            with self.subTest(table=name):
                self.assertEqual(
                    self.sql_schema['tables'][name]['columns'],
                    self.swift_schema['tables'][name]['columns'],
                )

    def test_per_table_primary_keys_match(self):
        for name in sorted(self.sql_schema['tables']):
            with self.subTest(table=name):
                self.assertEqual(
                    self.sql_schema['tables'][name]['pk_cols'],
                    self.swift_schema['tables'][name]['pk_cols'],
                )

    def test_indexes_match(self):
        self.assertEqual(self.sql_schema['indexes'], self.swift_schema['indexes'])

    def test_canonical_schema_includes_every_known_table(self):
        # Sanity: every table the production code references is in
        # schema.sql.  Catches drift where someone adds a new Swift
        # table but forgets schema.sql.
        expected = {'visits', 'read_events', 'skimmed_events',
                    'snapshots', 'mover_errors'}
        self.assertEqual(expected, set(self.sql_schema['tables']))


if __name__ == '__main__':
    unittest.main()
