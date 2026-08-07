#!/usr/bin/env python
"""Fixed-fixture evaluation for SchemaScribe.

Generates a human review sheet from a real-LLM catalog of a fixed fixture,
and parses the filled sheet into acceptance/edit/rejection rates.

Usage:
  run_evaluation.py generate --config CONFIG --db PROFILE --output SHEET.md
  run_evaluation.py parse SHEET.md

The fixture is the deterministic "clean" DB (users/orders/products) from
schema_scribe.tests.fixtures — build it first:
  python scripts/build_fixture_dbs.py --out-dir <dir> --variant clean
and point a db profile at it in config.yaml. See docs/TESTING.md for the
rubric.
"""

import argparse
import re
import sys
from datetime import datetime

from schema_scribe.config.manager import ConfigManager
from schema_scribe.services.catalog_generator import CatalogGenerator


def _collect_items(catalog):
    """Flattens a catalog into review items (type, label, text)."""
    items = []
    for table in catalog.get("tables", []):
        items.append(
            (
                "table summary",
                f"{table['name']}",
                table.get("ai_summary", ""),
            )
        )
        for col in table.get("columns", []):
            items.append(
                (
                    "column",
                    f"{table['name']}.{col['name']} ({col['type']})",
                    col.get("description", ""),
                )
            )
    for view in catalog.get("views", []):
        items.append(("view", view["name"], view.get("ai_summary", "")))
    return items


def generate(args):
    cfg = ConfigManager(args.config)
    connector, db_name = cfg.get_db_connector(args.db)
    llm, _ = cfg.get_llm_client(None)
    provider = cfg.get_llm_provider_name(None)
    model = getattr(llm, "model", "unknown")

    try:
        catalog = CatalogGenerator(connector, llm).generate_catalog(db_name)
    finally:
        connector.close()

    items = _collect_items(catalog)
    lines = [
        "# Evaluation Review Sheet",
        "",
        f"- Fixture: `{db_name}` (deterministic clean fixture)",
        f"- Provider: `{provider}`  Model: `{model}`",
        f"- Generated: {datetime.now().isoformat(timespec='minutes')}",
        f"- Items: {len(items)}",
        "",
        "## Instructions",
        "",
        "For each item mark exactly one box (`[x]`). EDIT = wrong or weak:",
        "write the corrected text on the `Correction:` line. REJECT = unusable.",
        "",
        "## Items",
        "",
    ]
    for i, (kind, label, text) in enumerate(items, 1):
        text = (text or "").strip().replace("\n", " ")
        lines += [
            f"### {i}. [{kind}] {label}",
            "",
            f"> {text}",
            "",
            "- [ ] ACCEPT",
            "- [ ] EDIT",
            "- [ ] REJECT",
            "Correction: ",
            "",
        ]
    with open(args.output, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"Wrote review sheet ({len(items)} items) to {args.output}")


def parse(args):
    text = open(args.sheet, encoding="utf-8").read()
    blocks = re.split(r"\n### ", text)[1:]
    counts = {"ACCEPT": 0, "EDIT": 0, "REJECT": 0}
    per_kind = {}
    corrections = []
    for block in blocks:
        header = block.split("\n", 1)[0]
        kind = re.search(r"\[(\w+ [\w/]+)\]", header)
        kind = kind.group(1) if kind else "unknown"
        checked = re.findall(r"- \[x\]\s*(ACCEPT|EDIT|REJECT)", block)
        verdict = checked[0] if checked else "UNMARKED"
        counts[verdict] = counts.get(verdict, 0) + 1
        per_kind.setdefault(kind, {"ACCEPT": 0, "EDIT": 0, "REJECT": 0})
        per_kind[kind][verdict] = per_kind[kind].get(verdict, 0) + 1
        corr = re.search(r"Correction:\s*(.+)", block)
        if verdict == "EDIT" and corr and corr.group(1).strip():
            corrections.append((header, corr.group(1).strip()))

    total = sum(counts.values())
    print(f"Total: {total}  ACCEPT: {counts.get('ACCEPT', 0)}  "
          f"EDIT: {counts.get('EDIT', 0)}  REJECT: {counts.get('REJECT', 0)}  "
          f"UNMARKED: {counts.get('UNMARKED', 0)}")
    if total:
        print(f"Acceptance rate (ACCEPT / total): "
              f"{counts.get('ACCEPT', 0) / total:.0%}")
        print(f"Usable rate (ACCEPT+EDIT / total): "
              f"{(counts.get('ACCEPT', 0) + counts.get('EDIT', 0)) / total:.0%}")
    print("\nPer kind:")
    for kind, c in per_kind.items():
        print(f"  {kind}: {c}")
    if corrections:
        print("\nCorrections:")
        for header, corr in corrections:
            print(f"  - {header}: {corr}")
    return 0 if counts.get("UNMARKED", 0) == 0 else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    g = sub.add_parser("generate")
    g.add_argument("--config", required=True)
    g.add_argument("--db", required=True)
    g.add_argument("--output", required=True)
    g.set_defaults(func=generate)

    p = sub.add_parser("parse")
    p.add_argument("sheet")
    p.set_defaults(func=parse)

    args = parser.parse_args()
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
