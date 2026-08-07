"""
Schema state sidecar service.

Persists a compact, classifier-ready snapshot of the catalog after each
successful run. The snapshot is written as a JSON sidecar next to the
documentation output (<output_filename>.schema-state.json) and consumed
by the change classifier (Task 5.2) and `db --check` (Slice 6).
"""

import json
import os
import stat
import tempfile
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


class SchemaState:
    """
    Builds, persists, and loads schema state snapshots.

    snapshot() is a pure function of the catalog dict. save() writes
    atomically using the Task 4.1 pattern: the full JSON is composed in
    memory, written to a temp file in the target directory, then moved
    onto the target with os.replace; the temp file is removed on any
    failure. load() never raises — a missing or corrupt sidecar yields
    None.
    """

    @staticmethod
    def snapshot(catalog: Dict[str, Any]) -> Dict[str, Any]:
        """
        Reduces the catalog to the classifier-ready snapshot shape.

        The `fks` entries carry both the source column and the
        "table.column" target so the classifier can detect an FK target
        change, not only FK presence.
        """
        tables = {}
        for table in catalog["tables"]:
            name = table["name"]
            tables[name] = {
                "columns": {col["name"]: col["type"] for col in table["columns"]},
                "pk": [
                    col["name"]
                    for col in table["columns"]
                    if col.get("is_pk", False)
                ],
                "fks": [],
            }
        for fk in catalog["foreign_keys"]:
            tables[fk["source_table"]]["fks"].append(
                {
                    "source": fk["source_column"],
                    "target": f"{fk['target_table']}.{fk['target_column']}",
                }
            )
        return {
            "tables": tables,
            "views": [view["name"] for view in catalog["views"]],
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    @staticmethod
    def save(snapshot: Dict[str, Any], path: str) -> None:
        """
        Writes `snapshot` to `path` atomically; raises on failure.
        """
        content = json.dumps(snapshot, indent=2)
        target_dir = os.path.dirname(os.path.abspath(path))
        fd, tmp_name = tempfile.mkstemp(dir=target_dir, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(content)
            if os.path.exists(path):
                os.chmod(tmp_name, stat.S_IMODE(os.stat(path).st_mode))
            else:
                current_umask = os.umask(0)
                os.umask(current_umask)
                os.chmod(tmp_name, 0o666 & ~current_umask)
            os.replace(tmp_name, path)
        except BaseException:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
            raise

    @staticmethod
    def load(path: str) -> Optional[Dict[str, Any]]:
        """
        Loads a snapshot, returning None for a missing or corrupt file.
        """
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (OSError, ValueError):
            return None

    @staticmethod
    def classify(
        prev: Optional[Dict[str, Any]], curr: Dict[str, Any]
    ) -> Dict[str, List[str]]:
        """
        Classifies tables as added, removed, or structurally changed
        against the previous snapshot. `prev=None` (no baseline) marks
        every table as added.

        A table is structurally changed when its column map
        ({col: type} — a type change or a column added/removed), its PK
        set, or its FK set (compared as (source, target) pairs, so a
        changed FK target counts) differs. Unchanged tables appear in
        neither list. All lists are returned in sorted order.
        """
        if prev is None:
            return {
                "added": sorted(curr["tables"]),
                "removed": [],
                "structurally_changed": [],
            }
        prev_names = set(prev["tables"])
        curr_names = set(curr["tables"])
        changed = []
        for name in sorted(prev_names & curr_names):
            prev_table = prev["tables"][name]
            curr_table = curr["tables"][name]
            prev_fks = sorted((fk["source"], fk["target"]) for fk in prev_table["fks"])
            curr_fks = sorted((fk["source"], fk["target"]) for fk in curr_table["fks"])
            if (
                prev_table["columns"] != curr_table["columns"]
                or sorted(prev_table["pk"]) != sorted(curr_table["pk"])
                or prev_fks != curr_fks
            ):
                changed.append(name)
        return {
            "added": sorted(curr_names - prev_names),
            "removed": sorted(prev_names - curr_names),
            "structurally_changed": changed,
        }
