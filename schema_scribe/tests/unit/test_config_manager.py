"""
Unit tests for the ConfigManager LLM provider-name accessor.

The accessor resolves the effective LLM profile (CLI value or the
configured default, same `_get_profile_name` logic) and returns the
`provider` field — the name disclosed to the user — without constructing
any LLM client. The test config's profile name ('test_llm') deliberately
differs from its provider ('openai') so the two cannot be confused.
"""

import pytest
import typer

from schema_scribe.config.manager import ConfigManager


def _manager(config_path):
    return ConfigManager(config_path)


def test_get_llm_provider_name_resolves_cli_profile(test_config, tmp_path):
    mgr = _manager(test_config(str(tmp_path / "x.db")))
    assert mgr.get_llm_provider_name("test_llm") == "openai"


def test_get_llm_provider_name_resolves_default_profile(test_config, tmp_path):
    """
    With no CLI profile, the default.llm profile is resolved and its
    provider field returned.
    """
    mgr = _manager(test_config(str(tmp_path / "x.db")))
    assert mgr.get_llm_provider_name(None) == "openai"


def test_get_llm_provider_name_constructs_no_client(test_config, tmp_path, monkeypatch):
    """
    The accessor must not construct an LLM client (a dry run must never
    perform provider-side work); it only reads config.
    """
    import schema_scribe.config.manager as manager_mod

    def _fail(*args, **kwargs):
        raise AssertionError("get_llm_client must not be called")

    monkeypatch.setattr(manager_mod, "get_llm_client", _fail)
    mgr = _manager(test_config(str(tmp_path / "x.db")))
    assert mgr.get_llm_provider_name("test_llm") == "openai"


def test_get_llm_provider_name_missing_profile_exits(test_config, tmp_path):
    mgr = _manager(test_config(str(tmp_path / "x.db")))
    with pytest.raises(typer.Exit):
        mgr.get_llm_provider_name("nope")
