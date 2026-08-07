"""Deterministic, LLM-free database landscape analysis (Slice 8.1).

`build_landscape` is a pure function over connector-collected metadata:
table names, columns per table, and foreign-key pairs. It answers the
product question "opening the database is scary" without an LLM —

- how big is this database (`scale`),
- what are the naming-convention groups (`clusters`),
- which tables are the FK hubs to start with (`core_tables`),
- what does the FK graph look like (`relationship_map`).

Every output is sorted, so identical metadata always yields identical
output regardless of input order.

Clustering rules (first match wins, case-insensitive):

1. ``^TBL_(<DOMAIN>)_(<KIND>)_(<YEAR>)$`` → ``TBL_<DOMAIN>_<KIND>`` —
   legacy-style ``TBL_SLS_HDR_2021`` tables. The year is excluded from the
   group key so versioned tables of one domain/type cluster together
   (probe: 12 domains x 4 kinds = 48 groups).
2. ``^T_(<DOMAIN>)_(<KIND>)$`` → ``T_<DOMAIN>_<KIND>`` — ``T_SLS_HDR``-style
   legacy names without a year part.
3. ``^dim_*`` → ``dim``, ``^fact_*`` → ``fact``, ``^stg_*`` → ``stg`` —
   warehouse conventions, grouped at family level (one cluster per family).
4. ``*_audit$`` → ``audit`` — audit trail tables.
5. Everything else → ``other``. The uncategorized bucket is a first-class
   output, not an error state; it is always present, possibly empty.

Core tables are the FK-centrality hubs: in-degree + out-degree on the FK
graph (column-level edges), top ``_TOP_N`` by (degree desc, name asc).
Tables with degree zero are excluded — a schema without foreign keys has
no hubs, and an arbitrary alphabetical tie-break would be noise.

``relationship_map`` lists every table as a node (isolated tables included)
and every resolvable FK pair as a column-level edge. Edges whose source or
target table is absent from the metadata are dropped so the graph stays
consistent.
"""

import re
from typing import Any, Dict, List

_TOP_N = 10

# First match wins; order is part of the contract.
_CLUSTER_RULES: List[tuple] = [
    (re.compile(r"^TBL_(?P<domain>[A-Z0-9]+)_(?P<kind>[A-Z0-9]+)_(?P<year>\d{4})$"), "TBL"),
    (re.compile(r"^T_(?P<domain>[A-Z0-9]+)_(?P<kind>[A-Z0-9]+)$"), "T"),
    (re.compile(r"^dim_(?P<subject>.+)$"), "dim"),
    (re.compile(r"^fact_(?P<subject>.+)$"), "fact"),
    (re.compile(r"^stg_(?P<subject>.+)$"), "stg"),
    (re.compile(r"^(?P<subject>.+)_audit$"), "audit"),
]


def _cluster_key(table_name: str) -> str:
    """Cluster name for a table, or ``"other"`` when no convention matches."""
    for pattern, family in _CLUSTER_RULES:
        match = pattern.fullmatch(table_name)
        if not match:
            continue
        if family in ("TBL", "T"):
            domain = match.group("domain").upper()
            kind = match.group("kind").upper()
            return f"{family}_{domain}_{kind}"
        return family
    return "other"


def _clusters(tables: List[str]) -> Dict[str, List[str]]:
    """Group tables by naming convention; ``"other"`` always present."""
    grouped: Dict[str, List[str]] = {"other": []}
    for table in sorted(tables):
        grouped.setdefault(_cluster_key(table), []).append(table)
    return {name: grouped[name] for name in sorted(grouped)}


def _scale(
    tables: List[str], columns_by_table: Dict[str, List[Dict[str, Any]]]
) -> Dict[str, Any]:
    """Table count, total column count, per-column-type distribution."""
    column_types: Dict[str, int] = {}
    total_columns = 0
    for table in sorted(tables):
        for column in columns_by_table.get(table, []):
            total_columns += 1
            column_type = column.get("type")
            column_types[column_type] = column_types.get(column_type, 0) + 1
    return {
        "tables": len(tables),
        "columns": total_columns,
        "column_types": {
            name: column_types[name] for name in sorted(column_types)
        },
    }


def _resolvable_edges(
    tables: List[str], foreign_keys: List[Dict[str, str]]
) -> List[Dict[str, str]]:
    """Column-level FK edges sorted deterministically, endpoints in ``tables`` only."""
    known = set(tables)
    edges = {
        (
            edge["source_table"],
            edge["source_column"],
            edge["target_table"],
            edge["target_column"],
        )
        for edge in foreign_keys
        if edge["source_table"] in known and edge["target_table"] in known
    }
    return [
        {
            "source": source,
            "source_column": source_column,
            "target": target,
            "target_column": target_column,
        }
        for source, source_column, target, target_column in sorted(edges)
    ]


def _core_tables(
    tables: List[str], edges: List[Dict[str, str]]
) -> List[Dict[str, Any]]:
    """Top-N FK-centrality ranking: in-degree + out-degree, degree desc then name asc.

    Tables with degree zero are excluded — no FK involvement, no hub signal.
    """
    degree: Dict[str, int] = {table: 0 for table in tables}
    for edge in edges:
        degree[edge["source"]] += 1
        degree[edge["target"]] += 1
    ranked = sorted(
        (table for table, d in degree.items() if d > 0),
        key=lambda name: (-degree[name], name),
    )
    return [
        {"table": name, "degree": degree[name]} for name in ranked[:_TOP_N]
    ]


def build_landscape(
    tables: List[str],
    columns_by_table: Dict[str, List[Dict[str, Any]]],
    foreign_keys: List[Dict[str, str]],
) -> Dict[str, Any]:
    """Build the deterministic landscape dict from collected metadata.

    Pure function: no I/O, no LLM. All outputs are sorted.

    Args:
        tables: Table names from ``get_tables()``.
        columns_by_table: ``{table: [column dicts]}`` from ``get_columns()``.
        foreign_keys: FK pair dicts (``source_table``, ``source_column``,
            ``target_table``, ``target_column``) from ``get_foreign_keys()``.

    Returns:
        ``{"scale", "clusters", "core_tables", "relationship_map"}``.
    """
    edges = _resolvable_edges(tables, foreign_keys)
    return {
        "scale": _scale(tables, columns_by_table),
        "clusters": _clusters(tables),
        "core_tables": _core_tables(tables, edges),
        "relationship_map": {"nodes": sorted(tables), "edges": edges},
    }
