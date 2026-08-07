"""
Unit tests for the deterministic landscape service (Slice 8.1).

`build_landscape` is pure and LLM-free: metadata is collected through a
connector before the call, and every output is deterministically ordered
(sort all the things). The legacy-rich fixture exercises the scale story —
192 cryptic-name tables grouped by convention, FK-centrality hubs = master
tables.
"""

import re

from schema_scribe.components.db_connectors.sqlite_connector import SQLiteConnector
from schema_scribe.services.landscape import build_landscape
from schema_scribe.tests.fixtures import db_fixtures


def _collect(path):
    """Collect metadata through a real connector and close it (brief's shape)."""
    connector = SQLiteConnector()
    connector.connect({"path": str(path)})
    tables = connector.get_tables()
    cols = {t: connector.get_columns(t) for t in tables}
    fks = connector.get_foreign_keys()
    connector.close()
    return tables, cols, fks


def test_landscape_on_legacy_fixture(tmp_path):
    db_path = tmp_path / "legacy.db"
    db_fixtures.build_sqlite(str(db_path), "legacy-rich")
    connector = SQLiteConnector()
    connector.connect({"path": str(db_path)})
    tables = connector.get_tables()
    cols = {t: connector.get_columns(t) for t in tables}
    fks = connector.get_foreign_keys()
    connector.close()
    landscape = build_landscape(tables, cols, fks)
    assert landscape["scale"]["tables"] == len(tables)
    assert landscape["core_tables"]  # non-empty on legacy-rich
    assert set(landscape["relationship_map"]["nodes"]) == set(tables)  # isolated tables included


def test_clusters_legacy_rich_forty_eight_groups(tmp_path):
    """Fixture pattern TBL_<DOMAIN>_<TYPE>_<YEAR> groups into 12 x 4 clusters, year ignored."""
    db_path = tmp_path / "legacy.db"
    db_fixtures.build_sqlite(str(db_path), "legacy-rich")
    tables, cols, fks = _collect(db_path)
    landscape = build_landscape(tables, cols, fks)

    clusters = landscape["clusters"]
    assert clusters["other"] == []  # first-class output, not an error state
    named = {k: v for k, v in clusters.items() if k != "other"}
    assert len(named) == 48  # 12 domains x 4 types
    assert all(re.fullmatch(r"TBL_[A-Z0-9]+_[A-Z0-9]+", k) for k in named)
    assert sum(len(v) for v in clusters.values()) == len(tables)
    placed = sorted(name for members in clusters.values() for name in members)
    assert placed == sorted(tables)  # every table in exactly one cluster


def test_clusters_real_world_conventions(tmp_path):
    """dim_/fact_/stg_ prefixes and _audit suffix form family clusters on the conventions fixture."""
    db_path = tmp_path / "conv.db"
    db_fixtures.build_sqlite(str(db_path), "legacy-conventions")
    tables, cols, fks = _collect(db_path)
    clusters = build_landscape(tables, cols, fks)["clusters"]

    assert clusters["dim"] == sorted(t for t in tables if t.startswith("dim_"))
    assert clusters["fact"] == sorted(t for t in tables if t.startswith("fact_"))
    assert clusters["stg"] == sorted(t for t in tables if t.startswith("stg_"))
    assert clusters["audit"] == sorted(t for t in tables if t.endswith("_audit"))
    assert clusters["other"] == []
    assert len(clusters) == 5


def test_uncategorized_tables_land_in_other():
    """A table matching no convention is a first-class 'other' cluster member, not an error."""
    tables = ["dim_customer", "fact_sales", "random_orders", "users"]
    cols = {t: [{"name": "id", "type": "INTEGER"}] for t in tables}
    fks = []
    clusters = build_landscape(tables, cols, fks)["clusters"]

    assert clusters["dim"] == ["dim_customer"]
    assert clusters["fact"] == ["fact_sales"]
    assert sorted(clusters["other"]) == ["random_orders", "users"]


def test_mixed_case_names_cluster_with_convention():
    """Rules are case-insensitive: mixed-case legacy and convention names keep their group."""
    tables = ["Tbl_Sls_Hdr_2021", "DIM_CUSTOMER", "Fact_Sales", "stg_orders", "Orders_AUDIT"]
    cols = {t: [{"name": "id", "type": "INTEGER"}] for t in tables}
    clusters = build_landscape(tables, cols, [])["clusters"]

    assert clusters["TBL_SLS_HDR"] == ["Tbl_Sls_Hdr_2021"]
    assert clusters["dim"] == ["DIM_CUSTOMER"]
    assert clusters["fact"] == ["Fact_Sales"]
    assert clusters["stg"] == ["stg_orders"]
    assert clusters["audit"] == ["Orders_AUDIT"]
    assert clusters["other"] == []


def test_yearless_legacy_names_share_yearful_group():
    """TBL_SLS_HDR without a year clusters with its yearful siblings, not 'other'."""
    tables = ["TBL_SLS_HDR_2021", "TBL_SLS_HDR", "TBL_CUST_MST"]
    cols = {t: [{"name": "id", "type": "INTEGER"}] for t in tables}
    clusters = build_landscape(tables, cols, [])["clusters"]

    assert clusters["TBL_SLS_HDR"] == ["TBL_SLS_HDR", "TBL_SLS_HDR_2021"]
    assert clusters["TBL_CUST_MST"] == ["TBL_CUST_MST"]
    assert clusters["other"] == []


def test_scale_counts_missing_column_type_as_unknown():
    """A column dict without 'type' counts under 'UNKNOWN', never a None key."""
    tables = ["users"]
    cols = {"users": [{"name": "id", "type": "INTEGER"}, {"name": "raw"}]}
    scale = build_landscape(tables, cols, [])["scale"]

    assert scale["column_types"] == {"INTEGER": 1, "UNKNOWN": 1}


def test_core_tables_masters_rank_top(tmp_path):
    """Probe result: FK-centrality top-10 on legacy-rich is all *_MST_2021 hub tables."""
    db_path = tmp_path / "legacy.db"
    db_fixtures.build_sqlite(str(db_path), "legacy-rich")
    tables, cols, fks = _collect(db_path)
    core = build_landscape(tables, cols, fks)["core_tables"]

    assert [c["table"] for c in core] == [
        "TBL_ACC_MST_2021",
        "TBL_CUST_MST_2021",
        "TBL_EMP_MST_2021",
        "TBL_INV_MST_2021",
        "TBL_ORD_MST_2021",
        "TBL_PAY_MST_2021",
        "TBL_PRD_MST_2021",
        "TBL_SHP_MST_2021",
        "TBL_SLS_MST_2021",
        "TBL_STO_MST_2021",
    ]
    assert len({c["degree"] for c in core}) == 1  # all tied at the hub maximum
    assert core[0]["degree"] > 0


def test_core_tables_sparse_graph_ranks_touched_tables(tmp_path):
    """legacy-poor has 2 FKs; only the 4 touched tables qualify, in deterministic order."""
    db_path = tmp_path / "poor.db"
    db_fixtures.build_sqlite(str(db_path), "legacy-poor")
    tables, cols, fks = _collect(db_path)
    core = build_landscape(tables, cols, fks)["core_tables"]

    assert [(c["table"], c["degree"]) for c in core] == [
        ("TBL_CUST_HDR_2021", 1),
        ("TBL_CUST_MST_2021", 1),
        ("TBL_SLS_HDR_2021", 1),
        ("TBL_SLS_MST_2021", 1),
    ]


def test_core_tables_empty_without_foreign_keys():
    """No FK graph, no hubs: core_tables is empty rather than an arbitrary tie-break."""
    tables = ["alpha", "beta", "gamma"]
    cols = {t: [{"name": "id", "type": "INTEGER"}] for t in tables}
    assert build_landscape(tables, cols, [])["core_tables"] == []


def test_relationship_map_shape(tmp_path):
    """Nodes = all tables (isolated included); edges sorted and column-level."""
    db_path = tmp_path / "legacy.db"
    db_fixtures.build_sqlite(str(db_path), "legacy-rich")
    tables, cols, fks = _collect(db_path)
    rel = build_landscape(tables, cols, fks)["relationship_map"]

    assert rel["nodes"] == sorted(tables)
    assert len(rel["edges"]) == len(fks)  # fixture FKs are 100% resolvable
    edge = rel["edges"][0]
    assert set(edge) == {"source", "source_column", "target", "target_column"}
    keys = [(e["source"], e["source_column"], e["target"], e["target_column"]) for e in rel["edges"]]
    assert keys == sorted(keys)


def test_relationship_map_filters_unresolvable_edges():
    """Edges to tables outside the metadata are dropped so nodes/edges stay consistent."""
    tables = ["orders", "customers"]
    cols = {t: [{"name": "id", "type": "INTEGER"}] for t in tables}
    fks = [
        {"source_table": "orders", "source_column": "customer_id",
         "target_table": "customers", "target_column": "id"},
        {"source_table": "orders", "source_column": "ghost_id",
         "target_table": "deleted_table", "target_column": "id"},
    ]
    rel = build_landscape(tables, cols, fks)["relationship_map"]
    assert rel["nodes"] == ["customers", "orders"]
    assert rel["edges"] == [
        {"source": "orders", "source_column": "customer_id",
         "target": "customers", "target_column": "id"},
    ]


def test_scale_counts(tmp_path):
    """scale reports table count, total column count, and per-column-type distribution."""
    db_path = tmp_path / "legacy.db"
    db_fixtures.build_sqlite(str(db_path), "legacy-rich")
    tables, cols, fks = _collect(db_path)
    scale = build_landscape(tables, cols, fks)["scale"]

    assert scale["tables"] == len(tables)
    assert scale["columns"] == sum(len(c) for c in cols.values())
    assert sum(scale["column_types"].values()) == scale["columns"]
    assert set(scale["column_types"]) == {"INTEGER", "TEXT"}
    assert scale["column_types"]["INTEGER"] > 0


def test_output_is_deterministic():
    """Same metadata in different input order yields byte-identical output."""
    tables = ["users", "products", "orders", "order_items", "dim_customer", "stg_orders"]
    cols = {t: [{"name": "id", "type": "INTEGER"}] for t in tables}
    fks = [
        {"source_table": "orders", "source_column": "user_id",
         "target_table": "users", "target_column": "id"},
        {"source_table": "order_items", "source_column": "order_id",
         "target_table": "orders", "target_column": "id"},
    ]
    first = build_landscape(tables, cols, fks)
    second = build_landscape(list(reversed(tables)), {t: cols[t] for t in reversed(tables)}, list(reversed(fks)))
    assert first == second


def test_empty_database():
    """Zero tables: scale is zero, 'other' is present and empty, nothing else crashes."""
    landscape = build_landscape([], {}, [])
    assert landscape["scale"] == {"tables": 0, "columns": 0, "column_types": {}}
    assert landscape["clusters"] == {"other": []}
    assert landscape["core_tables"] == []
    assert landscape["relationship_map"] == {"nodes": [], "edges": []}
