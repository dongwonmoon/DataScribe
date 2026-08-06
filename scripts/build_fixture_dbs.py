#!/usr/bin/env python
"""Materialize the deterministic SQLite fixture databases to disk.

Thin CLI wrapper over ``schema_scribe.tests.fixtures.db_fixtures``.
Builds all variants by default; repeat ``--variant`` to pick specific ones.
"""

import argparse
import sys
from pathlib import Path

from schema_scribe.tests.fixtures import db_fixtures


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Build deterministic SQLite fixture databases."
    )
    parser.add_argument("--out-dir", required=True, help="Directory to write .db files into.")
    parser.add_argument(
        "--variant",
        action="append",
        choices=db_fixtures.VARIANT_NAMES,
        help="Variant to build (repeatable; default: all variants).",
    )
    args = parser.parse_args(argv)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    variants = args.variant or list(db_fixtures.VARIANT_NAMES)

    created = []
    for variant in variants:
        path = out_dir / f"{variant}.db"
        db_fixtures.build_sqlite(str(path), variant)
        created.append(str(path))

    for path in created:
        print(path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
