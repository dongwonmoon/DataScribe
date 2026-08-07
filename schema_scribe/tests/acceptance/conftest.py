"""Shared fixture factory for the connector acceptance suite.

The factory is keyed by connector id and returns a spec dict carrying the
connector instance, its connection params, the tier ("real" or "mock"),
and the expected metadata for the deterministic clean fixture database.

Two tiers:
- Real-engine tier (sqlite, duckdb): both connectors open the same
  fixture file built by ``db_fixtures.build_sqlite``. DuckDB 1.5.5
  auto-detects the SQLite format and loads the sqlite extension, so both
  connectors introspect the identical schema.
- Driver-mocked tier (postgres, mariadb, snowflake): the DBAPI driver is
  patched and a ``ScriptedCursor`` emulates the clean fixture schema at
  params-check depth. Nothing in this tier touches a real server.
"""

import sqlite3

import duckdb
import pytest

from schema_scribe.components.db_connectors import (
    DuckDBConnector,
    MariaDBConnector,
    PostgresConnector,
    SnowflakeConnector,
    SQLiteConnector,
)
from schema_scribe.tests.fixtures import db_fixtures

REAL_ENGINE_TIER = ("sqlite", "duckdb")
MOCKED_TIER = ("postgres", "mariadb", "snowflake")

# Expected metadata for the db_fixtures "clean" variant (users, products,
# orders; declared PKs on every id; two FKs on orders; no views).
CLEAN_TABLES = ("users", "products", "orders")

CLEAN_COLUMNS = {
    "users": ("id", "name", "email"),
    "products": ("id", "name", "price"),
    "orders": ("id", "user_id", "product_id", "order_date"),
}

CLEAN_PK_COLUMNS = {"users": {"id"}, "products": {"id"}, "orders": {"id"}}

CLEAN_FOREIGN_KEYS = (
    {
        "source_table": "orders",
        "source_column": "user_id",
        "target_table": "users",
        "target_column": "id",
    },
    {
        "source_table": "orders",
        "source_column": "product_id",
        "target_table": "products",
        "target_column": "id",
    },
)

# Per-engine type spellings reported for the identical fixture schema.
# Real values are pinned by probe runs; mocked values are the rows the
# scripted cursor returns for the same logical schema.
COLUMN_TYPES = {
    "sqlite": {
        "users": {"id": "INTEGER", "name": "TEXT", "email": "TEXT"},
        "products": {"id": "INTEGER", "name": "TEXT", "price": "REAL"},
        "orders": {
            "id": "INTEGER",
            "user_id": "INTEGER",
            "product_id": "INTEGER",
            "order_date": "TEXT",
        },
    },
    "duckdb": {
        "users": {"id": "BIGINT", "name": "VARCHAR", "email": "VARCHAR"},
        "products": {"id": "BIGINT", "name": "VARCHAR", "price": "DOUBLE"},
        "orders": {
            "id": "BIGINT",
            "user_id": "BIGINT",
            "product_id": "BIGINT",
            "order_date": "VARCHAR",
        },
    },
    "postgres": {
        "users": {"id": "integer", "name": "text", "email": "text"},
        "products": {"id": "integer", "name": "text", "price": "numeric"},
        "orders": {
            "id": "integer",
            "user_id": "integer",
            "product_id": "integer",
            "order_date": "date",
        },
    },
    "mariadb": {
        "users": {"id": "int", "name": "varchar", "email": "varchar"},
        "products": {"id": "int", "name": "varchar", "price": "decimal"},
        "orders": {
            "id": "int",
            "user_id": "int",
            "product_id": "int",
            "order_date": "date",
        },
    },
    "snowflake": {
        "users": {"id": "NUMBER", "name": "VARCHAR", "email": "VARCHAR"},
        "products": {"id": "NUMBER", "name": "VARCHAR", "price": "FLOAT"},
        "orders": {
            "id": "NUMBER",
            "user_id": "NUMBER",
            "product_id": "NUMBER",
            "order_date": "DATE",
        },
    },
}

# Profile stats on the empty fixture tables: 0 rows -> 0.0 null ratio,
# 0 distinct, trivially unique. Identical for every connector.
EMPTY_PROFILE = {"null_ratio": 0.0, "distinct_count": 0, "is_unique": True}


class ScriptedCursor:
    """DBAPI cursor that records calls and serves scripted rows.

    Row routing is by SQL substring, ordered so the most specific
    statement wins (e.g. ``SHOW PRIMARY KEYS`` before the generic
    information_schema column query). Any unlisted statement is treated
    as the profiling query and returns the empty-table stats.
    """

    def __init__(self, table_rows, column_rows_by_table, view_rows, fk_rows):
        self.calls = []
        self._table_rows = table_rows
        self._column_rows_by_table = column_rows_by_table
        self._view_rows = view_rows
        self._fk_rows = fk_rows
        self._pk_rows = [("", "", "", "", "id")]
        self._imported_fk_rows = [
            (
                "2024-01-01 00:00:00",
                "db",
                "public",
                "users",
                "id",
                "db",
                "public",
                "orders",
                "user_id",
            ),
            (
                "2024-01-01 00:00:00",
                "db",
                "public",
                "products",
                "id",
                "db",
                "public",
                "orders",
                "product_id",
            ),
        ]
        self._next_rows = []

    @staticmethod
    def _column_table(sql, params):
        """Table name from a get_columns call (params[-1] or the SQL)."""
        if isinstance(params, (tuple, list)) and len(params) >= 2:
            return params[-1]
        if '"' in sql:
            return sql.split('"')[1]
        return params[-1]

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        if "SHOW PRIMARY KEYS" in sql:
            self._next_rows = self._pk_rows
        elif "SHOW IMPORTED KEYS" in sql:
            self._next_rows = self._imported_fk_rows
        elif "USE SCHEMA" in sql:
            self._next_rows = []
        elif "SET SESSION TRANSACTION READ ONLY" in sql:
            self._next_rows = []
        elif "information_schema.columns" in sql:
            self._next_rows = self._column_rows_by_table[
                self._column_table(sql, params)
            ]
        elif "information_schema.views" in sql:
            self._next_rows = self._view_rows
        elif "information_schema.tables" in sql:
            self._next_rows = self._table_rows
        elif "referential_constraints" in sql:
            self._next_rows = self._fk_rows
        else:
            self._next_rows = []
        return self

    def fetchall(self):
        return self._next_rows

    def fetchone(self):
        return (0, 0, 0) if not self._next_rows else self._next_rows[0]

    def close(self):
        pass


def _sqlbase_rows(connector_id):
    """Rows the SqlBaseConnector information_schema queries expect."""
    columns = COLUMN_TYPES[connector_id]
    column_rows_by_table = {}
    for table, cols in columns.items():
        column_rows_by_table[table] = [
            (name, type_name, "NO" if name in CLEAN_PK_COLUMNS[table] else "YES",
             name in CLEAN_PK_COLUMNS[table])
            for name, type_name in cols.items()
        ]
    fk_rows = [
        (fk["source_table"], fk["source_column"], fk["target_table"], fk["target_column"])
        for fk in CLEAN_FOREIGN_KEYS
    ]
    return column_rows_by_table, fk_rows


def _snowflake_rows():
    """Rows the SnowflakeConnector overrides expect."""
    columns = COLUMN_TYPES["snowflake"]
    return {
        table: [
            (name, type_name, "NO" if name in CLEAN_PK_COLUMNS[table] else "YES")
            for name, type_name in cols.items()
        ]
        for table, cols in columns.items()
    }


@pytest.fixture
def connector_spec(tmp_path, mocker):
    """Fixture factory keyed by connector id.

    Returns a spec dict with the connector, its params, the tier
    ("real" or "mock"), the engine-specific write-error type, and — for
    the mocked tier — the scripted cursor plus expected SQL call
    fragments for params-check assertions.
    """

    fixture_path = tmp_path / "clean.db"
    db_fixtures.build_sqlite(str(fixture_path), "clean")

    def _build(connector_id):
        if connector_id == "sqlite":
            return {
                "connector_id": connector_id,
                "tier": "real",
                "connector": SQLiteConnector(),
                "params": {"path": str(fixture_path)},
                "write_error": sqlite3.Error,
            }

        if connector_id == "duckdb":
            return {
                "connector_id": connector_id,
                "tier": "real",
                "connector": DuckDBConnector(),
                "params": {"path": str(fixture_path)},
                "write_error": duckdb.Error,
            }

        if connector_id == "postgres":
            column_rows, fk_rows = _sqlbase_rows("postgres")
            cursor = ScriptedCursor(
                table_rows=[(name,) for name in CLEAN_TABLES],
                column_rows_by_table=column_rows,
                view_rows=[],
                fk_rows=fk_rows,
            )
            mock_lib = mocker.patch(
                "schema_scribe.components.db_connectors.postgres_connector.psycopg2"
            )
            mock_connection = mocker.MagicMock()
            mock_connection.cursor.return_value = cursor
            mock_lib.connect.return_value = mock_connection
            params = {
                "host": "localhost",
                "port": 5432,
                "user": "u",
                "password": "p",
                "dbname": "d",
                "schema": "public",
            }
            return {
                "connector_id": connector_id,
                "tier": "mock",
                "connector": PostgresConnector(),
                "params": params,
                "cursor": cursor,
                "connection_mock": mock_connection,
                "connect_mock": mock_lib.connect,
                "sql_checks": {
                    "tables": [("information_schema.tables", ("public",))],
                    "columns": [("information_schema.columns", ("public", "users"))],
                    "views": [("information_schema.views", ("public",))],
                    "foreign_keys": [("referential_constraints", ("public",))],
                    "profile": [("COUNT(*)", None)],
                },
            }

        if connector_id == "mariadb":
            column_rows, fk_rows = _sqlbase_rows("mariadb")
            cursor = ScriptedCursor(
                table_rows=[(name,) for name in CLEAN_TABLES],
                column_rows_by_table=column_rows,
                view_rows=[],
                fk_rows=fk_rows,
            )
            mock_lib = mocker.patch(
                "schema_scribe.components.db_connectors.mariadb_connector.mysql.connector"
            )
            mock_connection = mocker.MagicMock()
            mock_connection.cursor.return_value = cursor
            mock_lib.connect.return_value = mock_connection
            params = {
                "host": "localhost",
                "port": 3306,
                "user": "u",
                "password": "p",
                "dbname": "d",
            }
            return {
                "connector_id": connector_id,
                "tier": "mock",
                "connector": MariaDBConnector(),
                "params": params,
                "cursor": cursor,
                "connection_mock": mock_connection,
                "connect_mock": mock_lib.connect,
                "sql_checks": {
                    "tables": [("information_schema.tables", ("d",))],
                    "columns": [("information_schema.columns", ("d", "users"))],
                    "views": [("information_schema.views", ("d",))],
                    "foreign_keys": [("referential_constraints", ("d",))],
                    "profile": [("COUNT(*)", None)],
                },
            }

        if connector_id == "snowflake":
            cursor = ScriptedCursor(
                table_rows=[(name,) for name in CLEAN_TABLES],
                column_rows_by_table=_snowflake_rows(),
                view_rows=[],
                fk_rows=[],
            )
            mock_lib = mocker.patch(
                "schema_scribe.components.db_connectors.snowflake_connector.snowflake.connector"
            )
            mock_connection = mocker.MagicMock()
            mock_connection.cursor.return_value = cursor
            mock_lib.connect.return_value = mock_connection
            params = {
                "user": "u",
                "password": "p",
                "account": "acc",
                "database": "d",
                "schema": "public",
                "warehouse": "w",
            }
            return {
                "connector_id": connector_id,
                "tier": "mock",
                "connector": SnowflakeConnector(),
                "params": params,
                "cursor": cursor,
                "connection_mock": mock_connection,
                "connect_mock": mock_lib.connect,
                "sql_checks": {
                    "tables": [('"d".information_schema.tables', ("public",))],
                    "columns": [
                        ("SHOW PRIMARY KEYS", None),
                        ('"d".information_schema.columns', ("public", "users")),
                    ],
                    "views": [('"d".information_schema.views', ("public",))],
                    "foreign_keys": [
                        ("USE SCHEMA", None),
                        ("SHOW IMPORTED KEYS", None),
                    ],
                    "profile": [("COUNT(*)", None)],
                },
            }

        raise ValueError(f"unknown connector id: {connector_id!r}")

    return _build
