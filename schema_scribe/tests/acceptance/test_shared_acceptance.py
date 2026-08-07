"""Shared connector acceptance suite — the v1 qualification gate.

An adapter is supported by v1 only after it passes the shared acceptance
suite (docs/PRODUCT.md:55-57). This suite is that gate.

Tiers
-----
Real-engine tier (sqlite, duckdb): both connectors open the SAME
deterministic clean fixture (``db_fixtures.build_sqlite(path, "clean")``
— users/products/orders with declared PKs, two FKs on orders, no views)
and must satisfy IDENTICAL assertions for the full lifecycle: connect →
get_tables → get_columns (types, is_pk) → get_views → get_foreign_keys →
profile (3 stats, no values) → read-only write-attempt rejection →
close.

Driver-mocked tier (postgres, mariadb, snowflake): the same lifecycle and
the same assertions, run against a patched DBAPI driver whose scripted
cursor emulates the clean fixture schema. Coverage is params-check depth
only — it proves the connector issues the expected statements with the
expected parameters, not that a real server returns those rows. This
tier does NOT qualify an adapter for v1.

Qualification status (recorded 2026-08-07)
------------------------------------------
- sqlite: V1-QUALIFIED — the real-engine suite passes.
- duckdb: NOT V1-QUALIFIED. The real-engine suite fails three steps:
  get_tables (duckdb 1.5.5 ``SHOW ALL TABLES`` returns 6 columns, so the
  connector's ``row[0]`` is the database name, not a table name),
  get_views (``duckdb_views()`` lists 47 internal
  information_schema/pg_catalog views unfiltered), and get_foreign_keys
  (the duckdb sqlite extension drops ``REFERENCES`` clauses on import,
  so no FOREIGN KEY constraints are visible to introspect). Those steps
  are recorded as expected failures (xfail, strict) so the suite stays
  red-forcing when the connector is fixed: a real-engine acceptance run
  must pass and be recorded before duckdb is qualified.
- postgres, mariadb, snowflake: NOT V1-QUALIFIED — driver-mocked tier
  only; qualification requires a real-engine acceptance run executed and
  recorded (CI or a documented manual run).

Engine-specific facts pinned by probe runs: sqlite reports INTEGER/TEXT/
REAL and marks PK columns via PRAGMA; duckdb 1.5.5 reports BIGINT/
VARCHAR/DOUBLE and marks PK columns via DESCRIBE's "PRI" key slot.
Profile stats on the empty fixture tables are identical for every
connector (0 rows → null_ratio 0.0, distinct_count 0, is_unique True).
"""

import pytest

from schema_scribe.tests.acceptance.conftest import (
    CLEAN_COLUMNS,
    CLEAN_FOREIGN_KEYS,
    CLEAN_PK_COLUMNS,
    CLEAN_TABLES,
    COLUMN_TYPES,
    EMPTY_PROFILE,
    MOCKED_TIER,
    REAL_ENGINE_TIER,
)

ALL_CONNECTOR_IDS = REAL_ENGINE_TIER + MOCKED_TIER

# Real-engine failures verified against duckdb 1.5.5 on 2026-08-07.
# strict=True: an unexpected pass fails the suite until this record is
# updated and the qualification status above is revised.
DUCKDB_XFAIL_REASONS = {
    "get_tables": (
        "duckdb 1.5.5 SHOW ALL TABLES returns 6 columns; DuckDBConnector reads row[0] "
        "(the database name) instead of the table name. NOT v1-qualified (PRODUCT.md:55-57)."
    ),
    "get_views": (
        "duckdb_views() lists 47 internal information_schema/pg_catalog/duckdb_* views; "
        "the connector returns them unfiltered although the clean fixture defines none. "
        "NOT v1-qualified (PRODUCT.md:55-57)."
    ),
    "get_foreign_keys": (
        "the duckdb sqlite extension drops REFERENCES clauses on import, so "
        "duckdb_constraints() exposes no FOREIGN KEY rows; get_foreign_keys() returns [] "
        "even though the fixture declares 2 FKs. NOT v1-qualified (PRODUCT.md:55-57)."
    ),
}


def _params(step=None):
    """Parametrize over all connectors; mark the known duckdb failures."""
    items = []
    for connector_id in ALL_CONNECTOR_IDS:
        if connector_id == "duckdb" and step in DUCKDB_XFAIL_REASONS:
            items.append(
                pytest.param(
                    "duckdb",
                    marks=pytest.mark.xfail(
                        strict=True, reason=DUCKDB_XFAIL_REASONS[step]
                    ),
                )
            )
        else:
            items.append(connector_id)
    return items


def _connect(spec):
    spec["connector"].connect(spec["params"])
    assert spec["connector"].connection is not None


def _assert_sql_calls(spec, step):
    """Params-check depth: every expected statement was issued."""
    for fragment, params in spec["sql_checks"][step]:
        assert any(
            fragment in sql and (params is None or params == actual)
            for sql, actual in spec["cursor"].calls
        ), f"{spec['connector_id']}: expected call containing {fragment!r}"


def _fk_keys(foreign_keys):
    return sorted(
        (
            fk["source_table"],
            fk["source_column"],
            fk["target_table"],
            fk["target_column"],
        )
        for fk in foreign_keys
    )


@pytest.mark.parametrize("connector_id", _params())
def test_lifecycle_connect(connector_id, connector_spec):
    spec = connector_spec(connector_id)
    _connect(spec)
    if spec["tier"] == "mock":
        expected = {
            "postgres": dict(
                host="localhost",
                port=5432,
                user="u",
                password="p",
                dbname="d",
                options="-c default_transaction_read_only=on",
            ),
            "mariadb": dict(
                host="localhost",
                port=3306,
                user="u",
                password="p",
                database="d",
            ),
            "snowflake": dict(
                user="u",
                password="p",
                account="acc",
                database="d",
                schema="public",
                warehouse="w",
            ),
        }[connector_id]
        spec["connect_mock"].assert_called_once_with(**expected)
    spec["connector"].close()


@pytest.mark.parametrize("connector_id", _params("get_tables"))
def test_lifecycle_get_tables(connector_id, connector_spec):
    spec = connector_spec(connector_id)
    _connect(spec)
    tables = spec["connector"].get_tables()
    assert set(tables) == set(CLEAN_TABLES)
    if spec["tier"] == "mock":
        _assert_sql_calls(spec, "tables")
    spec["connector"].close()


@pytest.mark.parametrize("connector_id", _params())
def test_lifecycle_get_columns(connector_id, connector_spec):
    spec = connector_spec(connector_id)
    _connect(spec)
    for table in CLEAN_COLUMNS:
        columns = spec["connector"].get_columns(table)
        by_name = {column["name"]: column for column in columns}
        assert set(by_name) == set(CLEAN_COLUMNS[table])
        for name, type_name in COLUMN_TYPES[connector_id][table].items():
            column = by_name[name]
            assert column["type"] == type_name
            assert column["is_pk"] == (name in CLEAN_PK_COLUMNS[table])
            assert isinstance(column["is_nullable"], bool)
            assert column["description"] == ""
    if spec["tier"] == "mock":
        _assert_sql_calls(spec, "columns")
    spec["connector"].close()


@pytest.mark.parametrize("connector_id", _params("get_views"))
def test_lifecycle_get_views(connector_id, connector_spec):
    spec = connector_spec(connector_id)
    _connect(spec)
    views = spec["connector"].get_views()
    assert views == []
    if spec["tier"] == "mock":
        _assert_sql_calls(spec, "views")
    spec["connector"].close()


@pytest.mark.parametrize("connector_id", _params("get_foreign_keys"))
def test_lifecycle_get_foreign_keys(connector_id, connector_spec):
    spec = connector_spec(connector_id)
    _connect(spec)
    foreign_keys = spec["connector"].get_foreign_keys()
    assert _fk_keys(foreign_keys) == _fk_keys(CLEAN_FOREIGN_KEYS)
    if spec["tier"] == "mock":
        _assert_sql_calls(spec, "foreign_keys")
    spec["connector"].close()


@pytest.mark.parametrize("connector_id", _params())
def test_lifecycle_profile(connector_id, connector_spec):
    spec = connector_spec(connector_id)
    _connect(spec)
    stats = spec["connector"].get_column_profile("users", "id")
    assert set(stats) == {"null_ratio", "distinct_count", "is_unique"}
    assert stats == EMPTY_PROFILE
    if spec["tier"] == "mock":
        _assert_sql_calls(spec, "profile")
    spec["connector"].close()


@pytest.mark.parametrize("connector_id", _params())
def test_lifecycle_read_only_write_rejection(connector_id, connector_spec):
    spec = connector_spec(connector_id)
    _connect(spec)
    if spec["tier"] == "real":
        with pytest.raises(spec["write_error"]):
            spec["connector"].connection.execute(
                "CREATE TABLE _probe (id INTEGER)"
            )
    else:
        if connector_id == "postgres":
            kwargs = spec["connect_mock"].call_args.kwargs
            assert kwargs.get("options") == "-c default_transaction_read_only=on"
        elif connector_id == "mariadb":
            assert any(
                "SET SESSION TRANSACTION READ ONLY" in sql
                for sql, _ in spec["cursor"].calls
            )
        else:
            assert "options" not in spec["connect_mock"].call_args.kwargs
    spec["connector"].close()


@pytest.mark.parametrize("connector_id", _params())
def test_lifecycle_close(connector_id, connector_spec):
    spec = connector_spec(connector_id)
    _connect(spec)
    spec["connector"].close()
    assert spec["connector"].connection is None
    assert spec["connector"].cursor is None
    if spec["tier"] == "mock":
        spec["connection_mock"].close.assert_called_once_with()
