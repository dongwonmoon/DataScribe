"""API tests for the thin hosted demo (Phase 3a).

The job manager runs on the app's event loop via lifespan. The engine-run
seam (_run_db_scan_job) is monkeypatched with a fake for lifecycle tests;
the real seam is exercised by test_metrics_wiring.
"""

import time

import pytest
from fastapi.testclient import TestClient

from schema_scribe import server
from schema_scribe.server import main as server_main
from schema_scribe.tests.fixtures import db_fixtures

TEST_CONFIG = None


@pytest.fixture
def test_config(tmp_path):
    global TEST_CONFIG
    db_path = tmp_path / "fixture.db"
    db_fixtures.build_sqlite(str(db_path), "clean")
    cfg = tmp_path / "server-config.yaml"
    cfg.write_text(
        "default:\n  db: fixture\n"
        "db_connections:\n"
        f"  fixture:\n    type: sqlite\n    path: {db_path}\n"
        "output_profiles:\n  md:\n    type: markdown\n"
        "    output_filename: catalog.md\n"
    )
    TEST_CONFIG = str(cfg)
    return str(cfg)


@pytest.fixture
def client(test_config, monkeypatch):
    monkeypatch.setattr(server_main, "CONFIG_PATH", test_config)
    with TestClient(server_main.app) as c:
        yield c


def test_submit_and_poll_lifecycle(client, monkeypatch):
    """POST /api/jobs -> 202 + job_id; poll -> succeeded with metrics;
    unknown job -> 404; unknown profile -> 400."""
    def fake_job(config_path, db_profile):
        return {
            "catalog": {"tables": [], "views": [], "foreign_keys": []},
            "metrics": {"db_queries": 19, "llm_calls": 3, "engine_ms": 42.0},
        }

    monkeypatch.setattr(server_main, "_run_db_scan_job", fake_job)

    r = client.post("/api/jobs", json={"db_profile": "fixture"})
    assert r.status_code == 202, r.text
    job_id = r.json()["job_id"]

    state = None
    for _ in range(100):
        state = client.get(f"/api/jobs/{job_id}").json()
        if state["status"] in ("succeeded", "failed"):
            break
        time.sleep(0.05)
    assert state["status"] == "succeeded", state
    assert state["metrics"]["llm_calls"] == 3
    assert state["has_catalog"] is True

    assert client.get("/api/jobs/nope").status_code == 404
    bad = client.post("/api/jobs", json={"db_profile": "does-not-exist"})
    assert bad.status_code == 400
    assert bad.json()["detail"]["error"]["code"] == "unknown_profile"


def test_job_board_lists_jobs(client, monkeypatch):
    def fake_job(config_path, db_profile):
        return {"catalog": {}, "metrics": {"db_queries": 0, "llm_calls": 0, "engine_ms": 1.0}}

    monkeypatch.setattr(server_main, "_run_db_scan_job", fake_job)
    client.post("/api/jobs", json={"db_profile": "fixture"})
    board = client.get("/api/jobs").json()
    assert len(board["jobs"]) >= 1
    assert "job_id" in board["jobs"][0]


def test_metrics_wiring_counts_engine_calls(monkeypatch):
    """The real _run_db_scan_job wraps the engine with the counters and
    reports query/LLM counts and timing."""
    from unittest.mock import MagicMock

    from schema_scribe.core.interfaces import BaseConnector, BaseLLMClient
    from schema_scribe.tests.conftest import mock_batch_response

    connector = MagicMock(spec=BaseConnector)
    connector.cursor = MagicMock()  # the counting wrapper proxies this
    connector.get_tables.return_value = ["users", "orders"]
    connector.get_columns.return_value = [{"name": "id", "type": "INTEGER", "is_pk": True}]
    connector.get_views.return_value = []
    connector.get_foreign_keys.return_value = []
    llm = MagicMock(spec=BaseLLMClient)
    llm.get_description.side_effect = mock_batch_response

    class FakeConfig:
        def __init__(self, path):
            pass

        def get_db_connector(self, profile):
            return connector, "fixture"

        def get_llm_client(self, profile):
            return llm, "test_llm"

    monkeypatch.setattr(server_main, "ConfigManager", FakeConfig)
    result = server_main._run_db_scan_job("ignored.yaml", "fixture")
    assert result["metrics"]["llm_calls"] == 2  # two tables, one batch each
    assert result["metrics"]["db_queries"] >= 0
    assert result["metrics"]["engine_ms"] >= 0
    assert result["catalog"]["tables"]
