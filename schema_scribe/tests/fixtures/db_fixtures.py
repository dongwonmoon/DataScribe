"""Deterministic SQLite fixture builder for synthetic databases.

Builds small schema-only databases used by the acceptance suite and
change-classification tests. Every variant is generated purely
algorithmically over fixed literal vocabularies — no PRNG, no timestamps,
no input-dependent ordering — so building the same variant twice yields
byte-identical SQLite files.
"""

import sqlite3
from pathlib import Path

VARIANT_NAMES = ("clean", "legacy-rich", "legacy-poor", "legacy-conventions")

_LEGACY_YEARS = (2021, 2022, 2023, 2024)

# Fixed vocabulary of cryptic legacy-style stems, kinds, and prefixes.
_LEGACY_STEMS = (
    "CUST",
    "SLS",
    "ORD",
    "PRD",
    "INV",
    "PAY",
    "USR",
    "ACC",
    "STO",
    "EMP",
    "VND",
    "SHP",
)
_LEGACY_KINDS = ("MST", "HDR", "DTL", "TRN")


def _legacy_table_names():
    """All (stem, kind, year, name) tuples in fixed creation order."""
    return [
        (stem, kind, year, f"TBL_{stem}_{kind}_{year}")
        for stem in _LEGACY_STEMS
        for kind in _LEGACY_KINDS
        for year in _LEGACY_YEARS
    ]


def _legacy_fks(table, stem, kind, year, dense):
    """Declared FKs for one legacy table; dense vs sparse per variant."""
    year_index = _LEGACY_YEARS.index(year)
    stem_index = _LEGACY_STEMS.index(stem)
    prev_stem = _LEGACY_STEMS[stem_index - 1]
    if kind == "MST":
        if dense:
            # Version chain within the stem, plus a cross-stem chain.
            fks = []
            if year_index > 0:
                prev_year = _LEGACY_YEARS[year_index - 1]
                fks.append((f"prev_{stem.lower()}_mst_id", f"TBL_{stem}_MST_{prev_year}"))
            fks.append((f"parent_{prev_stem.lower()}_mst_id", f"TBL_{prev_stem}_MST_2021"))
            return fks
        return []
    if kind == "HDR":
        if dense or (stem in ("CUST", "SLS") and year == 2021):
            return [(f"{stem.lower()}_mst_id", f"TBL_{stem}_MST_{year}")]
        return []
    if kind == "DTL":
        if dense:
            return [(f"{stem.lower()}_hdr_id", f"TBL_{stem}_HDR_{year}")]
        return []
    # TRN
    if dense:
        return [
            (f"{stem.lower()}_hdr_id", f"TBL_{stem}_HDR_{year}"),
            (f"{stem.lower()}_mst_id", f"TBL_{stem}_MST_{year}"),
        ]
    return []


def _legacy_ddl(dense):
    statements = []
    for stem, kind, year, name in _legacy_table_names():
        cols = [
            "id INTEGER PRIMARY KEY",
            f"{stem.lower()}_code TEXT",
            f"{stem.lower()}_name TEXT",
            "reg_dt TEXT",
        ]
        for col_name, parent in _legacy_fks(None, stem, kind, year, dense):
            cols.append(f"{col_name} INTEGER REFERENCES {parent}(id)")
        statements.append(f"CREATE TABLE {name} ({', '.join(cols)})")
    return statements


# --- legacy-conventions vocabulary ---

_CONV_DIM_STEMS = (
    "customer",
    "product",
    "date",
    "store",
    "region",
    "employee",
    "channel",
    "category",
    "brand",
    "promotion",
    "payment_method",
    "currency",
    "language",
    "device",
    "campaign",
    "warehouse",
    "supplier",
    "price_list",
    "geography",
    "account",
    "customer_segment",
    "order_status",
    "delivery_type",
    "vat_rate",
    "fiscal_year",
)

_CONV_FACT_STEMS = (
    "sales",
    "orders",
    "inventory",
    "payments",
    "shipments",
    "returns",
    "receipts",
    "purchases",
    "clickstream",
    "daily_balance",
    "quotations",
    "work_orders",
    "timesheets",
    "invoices",
    "coupons_redeemed",
    "discounts",
    "forecasts",
    "targets",
    "ledgers",
    "budgets",
)

_CONV_STG_STEMS = (
    "customers",
    "orders",
    "products",
    "payments",
    "shipments",
    "returns",
    "receipts",
    "purchases",
    "clickstream",
    "campaigns",
    "suppliers",
    "warehouses",
    "currencies",
    "promotions",
    "price_lists",
    "invoices",
    "quotes",
    "budgets",
    "targets",
    "forecasts",
    "work_orders",
    "timesheets",
    "discounts",
    "coupons",
    "ledgers",
    "geographies",
    "accounts",
    "devices",
    "segments",
    "channels",
)

_CONV_AUDIT_STEMS = _CONV_FACT_STEMS + _CONV_DIM_STEMS[:10]


def _conventions_ddl():
    statements = []
    dims = [f"dim_{stem}" for stem in _CONV_DIM_STEMS]
    facts = [f"fact_{stem}" for stem in _CONV_FACT_STEMS]
    third_parents = [d for d in dims if d not in ("dim_date", "dim_store")]
    for name in dims:
        statements.append(
            f"CREATE TABLE {name} (id INTEGER PRIMARY KEY, name TEXT, code TEXT, created_at TEXT)"
        )
    for index, name in enumerate(facts):
        cols = ["id INTEGER PRIMARY KEY", "amount REAL", "quantity INTEGER", "recorded_at TEXT"]
        for parent in (
            "dim_date",
            "dim_store",
            third_parents[index % len(third_parents)],
        ):
            cols.append(f"{parent.removeprefix('dim_')}_id INTEGER REFERENCES {parent}(id)")
        statements.append(f"CREATE TABLE {name} ({', '.join(cols)})")
    for stem in _CONV_STG_STEMS:
        statements.append(
            f"CREATE TABLE stg_{stem} (id INTEGER PRIMARY KEY, raw_payload TEXT, loaded_at TEXT)"
        )
    for stem in _CONV_AUDIT_STEMS:
        parent = f"fact_{stem}" if stem in _CONV_FACT_STEMS else f"dim_{stem}"
        cols = ["id INTEGER PRIMARY KEY", "operation TEXT", "changed_at TEXT"]
        cols.append(f"{stem}_id INTEGER REFERENCES {parent}(id)")
        statements.append(f"CREATE TABLE {stem}_audit ({', '.join(cols)})")
    return statements


_CLEAN_TABLES = (
    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)",
    "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)",
    "CREATE TABLE orders ("
    "id INTEGER PRIMARY KEY, "
    "user_id INTEGER, "
    "product_id INTEGER, "
    "order_date TEXT, "
    "FOREIGN KEY(user_id) REFERENCES users(id), "
    "FOREIGN KEY(product_id) REFERENCES products(id))",
)


def build_sqlite(path: str, variant: str) -> None:
    """Create a deterministic schema-only SQLite database at ``path``.

    Any existing file at ``path`` is removed first, so rebuilding a variant
    always yields the same bytes regardless of prior file state.
    """
    if variant == "clean":
        statements = list(_CLEAN_TABLES)
    elif variant == "legacy-rich":
        statements = _legacy_ddl(dense=True)
    elif variant == "legacy-poor":
        statements = _legacy_ddl(dense=False)
    elif variant == "legacy-conventions":
        statements = _conventions_ddl()
    else:
        raise ValueError(f"unknown variant: {variant!r} (expected one of {VARIANT_NAMES})")

    Path(path).unlink(missing_ok=True)
    conn = sqlite3.connect(path)
    try:
        for statement in statements:
            conn.execute(statement)
        conn.commit()
    finally:
        conn.close()
