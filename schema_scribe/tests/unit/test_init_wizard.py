"""Tests for the init wizard's emitted configuration (panel finding C10,
2026-08-11: the wizard wrote `.env` keys with a stray quote and `${KEY}`
placeholders with a space that `expand_env_vars` never matches — the happy
path for password-bearing DB types silently failed auth)."""

import os

from typer.testing import CliRunner

from schema_scribe.app import app
from schema_scribe.utils.utils import load_config


def _run_init_wizard(tmp_path, monkeypatch, db_type="2"):
    """Drives the wizard: select postgres (2 in the registry), fill params,
    skip LLM/output."""
    answers = iter(
        [db_type, "dev_pg", "localhost", "5432", "admin", "secret",
         "testdb", "public", "0", "0"]
    )
    monkeypatch.setattr(
        "schema_scribe.app.typer.prompt", lambda *a, **k: next(answers)
    )
    monkeypatch.chdir(tmp_path)
    result = CliRunner().invoke(app, ["init"])
    assert result.exit_code == 0, result.output
    return tmp_path / "config.yaml", tmp_path / ".env"


def test_init_wizard_emits_clean_env_key(tmp_path, monkeypatch):
    """The .env line must be KEY=value, not KEY"=value."""
    _, env_file = _run_init_wizard(tmp_path, monkeypatch)
    env = env_file.read_text()
    assert "DEV_PG_PASSWORD=secret" in env
    assert 'DEV_PG_PASSWORD"=' not in env


def test_init_wizard_placeholder_roundtrips(tmp_path, monkeypatch):
    """config.yaml's password placeholder must be ${KEY} (no space) and
    resolve through expand_env_vars when the env var is set."""
    config_file, env_file = _run_init_wizard(tmp_path, monkeypatch)
    config_text = config_file.read_text()
    assert "password: ${DEV_PG_PASSWORD}" in config_text
    assert "$ {" not in config_text

    # Simulate load_dotenv: push the .env values into the environment, then
    # load_config must resolve the placeholder to the real password.
    for line in env_file.read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            key, _, value = line.partition("=")
            os.environ[key.strip()] = value.strip()
    config = load_config(str(config_file))
    assert config["db_connections"]["dev_pg"]["password"] == "secret"
