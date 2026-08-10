#!/usr/bin/env python
"""Reproducible benchmark for the db workflow (PRODUCT.md:117).

Measures, on a FIXED seeded fixture with a MOCKED LLM (fully
deterministic, no network):
  - database query count (metadata + profiling collection cost)
  - LLM call count (currently tables + columns + views — the batching
    baseline for the planned 13 -> 3 call reduction)
  - elapsed time (deterministic pipeline cost only)

Usage:
  python scripts/benchmark.py [--db PATH] [--json OUT.json]

The default fixture is the seeded clean variant (users/orders/products),
built on the fly into a temp dir.
"""

import argparse
import logging
import json
import tempfile
import time
from datetime import datetime

from schema_scribe.components.db_connectors.sqlite_connector import SQLiteConnector
from schema_scribe.components.writers.markdown_writer import MarkdownWriter
from schema_scribe.tests.fixtures import db_fixtures
from schema_scribe.workflows.db_workflow import DbWorkflow


class CountingCursor:
    """Proxy over a sqlite3 cursor that counts execute() calls."""

    def __init__(self, cursor):
        self._cursor = cursor
        self.count = 0

    def execute(self, sql, *args):
        self.count += 1
        return self._cursor.execute(sql, *args)

    def fetchall(self):
        return self._cursor.fetchall()

    def fetchone(self):
        return self._cursor.fetchone()

    def close(self):
        self._cursor.close()


class CountingLLM:
    """Mock LLM that returns instantly and counts calls."""

    def __init__(self):
        self.count = 0
        self.model = "benchmark-mock"

    def get_description(self, prompt, max_tokens):
        self.count += 1
        return "(benchmark mock description)"


def main():
    logging.disable(logging.INFO)  # quiet: the report is the output
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db", default=None, help="SQLite path (default: seeded clean fixture)"
    )
    parser.add_argument("--json", default=None, help="Write the report as JSON")
    args = parser.parse_args()

    fixture = "clean (seeded)"
    if args.db:
        db_path = args.db
        fixture = "provided"
    else:
        tmp_dir = tempfile.mkdtemp(prefix="bench-")
        db_path = f"{tmp_dir}/clean.db"
        db_fixtures.build_sqlite(db_path, "clean")

    with tempfile.TemporaryDirectory() as tmp_out:
        connector = SQLiteConnector()
        connector.connect({"path": db_path})
        counting_cursor = CountingCursor(connector.cursor)
        connector.cursor = counting_cursor

        llm = CountingLLM()
        writer = MarkdownWriter()
        wf = DbWorkflow(
            connector,
            llm_client=llm,
            writer=writer,
            db_profile_name="clean",
            writer_params={"output_filename": f"{tmp_out}/catalog.md"},
        )

        t0 = time.perf_counter()
        wf.run()
        total_ms = round((time.perf_counter() - t0) * 1000, 1)
        query_count = counting_cursor.count
        llm_count = llm.count
        connector.close()

    report = {
        "fixture": fixture,
        "db_queries": query_count,
        "llm_calls": llm_count,
        "total_ms": total_ms,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "note": "mock LLM; deterministic pipeline cost only",
    }
    print(json.dumps(report, indent=2, ensure_ascii=False))
    if args.json:
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"wrote {args.json}")


if __name__ == "__main__":
    main()
