import json
import sqlite3
from unittest.mock import MagicMock

from schema_scribe.components.db_connectors.sqlite_connector import SQLiteConnector
from schema_scribe.components.writers.markdown_writer import MarkdownWriter
from schema_scribe.core.interfaces import BaseLLMClient
from schema_scribe.workflows.db_workflow import DbWorkflow


def test_real_sqlite_composition(tmp_path):
    db_path = tmp_path / "fixture.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email VARCHAR NOT NULL)")
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id))")
    conn.commit()
    conn.close()

    connector = SQLiteConnector()
    connector.connect({"path": str(db_path)})
    llm = MagicMock(spec=BaseLLMClient)
    llm.get_description.return_value = "draft description"
    out = tmp_path / "catalog.md"
    writer = MarkdownWriter()
    wf = DbWorkflow(connector, llm, writer, db_profile_name="fixture",
                    writer_params={"output_filename": str(out)})
    wf.run()
    assert out.exists()
    assert "draft description" in out.read_text()

    sidecar = tmp_path / "catalog.md.schema-state.json"
    assert sidecar.exists()
    state = json.loads(sidecar.read_text())
    assert state["tables"]["users"]["pk"] == ["id"]
    assert state["tables"]["orders"]["fks"] == [
        {"source": "user_id", "target": "users.id"},
    ]


def test_catalog_carries_is_pk(tmp_path):
    db_path = tmp_path / "pk.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    conn.commit()
    conn.close()
    connector = SQLiteConnector()
    connector.connect({"path": str(db_path)})
    llm = MagicMock(spec=BaseLLMClient)
    llm.get_description.return_value = "draft"
    wf = DbWorkflow(connector, llm, writer=None, db_profile_name="fixture")
    catalog = wf.generate_catalog()
    col = catalog["tables"][0]["columns"][0]
    assert col["name"] == "id" and col["is_pk"] is True
