"""
Unit tests for the utils module.

This test suite verifies the utility functions in `data_scribe.utils.utils`,
such as configuration loading and environment variable expansion.
"""

import pytest
import os
from unittest.mock import patch
import yaml

from data_scribe.utils.utils import expand_env_vars, load_config
from data_scribe.core.exceptions import ConfigError


# --- Tests for expand_env_vars ---


def test_expand_env_vars_success():
    """Tests that environment variables are correctly expanded."""
    content = "password: ${TEST_DB_PASSWORD}"
    with patch.dict(os.environ, {"TEST_DB_PASSWORD": "secret_password"}):
        expanded_content = expand_env_vars(content)
        assert expanded_content == "password: secret_password"


def test_expand_env_vars_multiple_vars():
    """Tests expansion of multiple environment variables in one string."""
    content = "user: ${USER}, password: ${PASSWORD}"
    env_vars = {"USER": "admin", "PASSWORD": "123"}
    with patch.dict(os.environ, env_vars):
        expanded_content = expand_env_vars(content)
        assert expanded_content == "user: admin, password: 123"


def test_expand_env_vars_missing_variable_raises_error():
    """
    Tests that a ConfigError is raised if a referenced environment variable is not set.
    """
    content = "password: ${MISSING_VAR}"
    # Ensure the variable is not set
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ConfigError) as excinfo:
            expand_env_vars(content)
        assert "Environment variable 'MISSING_VAR' is not set" in str(
            excinfo.value
        )


def test_expand_env_vars_no_vars():
    """Tests that content without variables is returned unchanged."""
    content = "password: plain_text_password"
    expanded_content = expand_env_vars(content)
    assert expanded_content == content


# --- Tests for load_config ---


def test_load_config_success_with_env_vars(tmp_path):
    """
    Tests that load_config successfully loads a YAML file and expands env vars.
    """
    config_content = "key: ${MY_VAR}"
    config_file = tmp_path / "config.yml"
    config_file.write_text(config_content)

    with patch.dict(os.environ, {"MY_VAR": "my_value"}):
        config = load_config(str(config_file))
        assert config["key"] == "my_value"


def test_load_config_file_not_found():
    """
    Tests that load_config raises FileNotFoundError for a non-existent config file.
    """
    with pytest.raises(FileNotFoundError):
        load_config("non_existent_file.yml")


def test_load_config_invalid_yaml(tmp_path):
    """
    Tests that load_config raises YAMLError for a malformed YAML file.
    """
    config_content = "key: value:\n  - item1"  # Invalid YAML
    config_file = tmp_path / "config.yml"
    config_file.write_text(config_content)

    with pytest.raises(yaml.YAMLError):
        load_config(str(config_file))


def test_load_config_missing_env_var_raises_error(tmp_path):
    """
    Tests that load_config raises ConfigError if an env var is missing during load.
    """
    config_content = "key: ${MISSING_VAR_IN_LOAD}"
    config_file = tmp_path / "config.yml"
    config_file.write_text(config_content)

    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ConfigError, match="MISSING_VAR_IN_LOAD"):
            load_config(str(config_file))
