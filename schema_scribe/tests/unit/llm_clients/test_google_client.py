"""
Unit tests for the GoogleGenAIClient (google.genai SDK).
"""

import pytest
from unittest.mock import patch, MagicMock

from schema_scribe.components.llm_clients import GoogleGenAIClient
from schema_scribe.core.exceptions import ConfigError, LLMClientError


@patch("schema_scribe.components.llm_clients.google_client.genai")
def test_google_client_initialization(mock_genai, mocker):
    """Tests successful initialization of GoogleGenAIClient."""
    mock_settings = mocker.patch(
        "schema_scribe.components.llm_clients.google_client.settings"
    )
    mock_settings.google_api_key = "fake_api_key"

    client = GoogleGenAIClient(model="gemini-test")

    mock_genai.Client.assert_called_once_with(api_key="fake_api_key")
    assert client.model == "gemini-test"


def test_google_client_missing_api_key(mocker):
    """Tests that GoogleGenAIClient raises ConfigError if API key is missing."""
    mocker.patch(
        "schema_scribe.components.llm_clients.google_client.settings"
    ).google_api_key = None
    with pytest.raises(ConfigError, match="GOOGLE_API_KEY must be set"):
        GoogleGenAIClient(model="gemini-test")


@patch("schema_scribe.components.llm_clients.google_client.genai")
def test_google_client_get_description(mock_genai, mocker):
    """Tests the get_description method of GoogleGenAIClient."""
    mocker.patch(
        "schema_scribe.components.llm_clients.google_client.settings"
    ).google_api_key = "fake_key"

    mock_client = MagicMock()
    mock_client.models.generate_content.return_value.text = "Google response"
    mock_genai.Client.return_value = mock_client

    client = GoogleGenAIClient(model="gemini-test")
    description = client.get_description("test prompt", 150)

    assert description == "Google response"
    mock_client.models.generate_content.assert_called_once_with(
        model="gemini-test",
        contents="test prompt",
        config={"max_output_tokens": 150},
    )


@patch("schema_scribe.components.llm_clients.google_client.genai")
def test_google_client_empty_response_raises_llm_error(mock_genai, mocker):
    """A None response.text must raise LLMClientError, not AttributeError."""
    mocker.patch(
        "schema_scribe.components.llm_clients.google_client.settings"
    ).google_api_key = "fake_key"

    mock_client = MagicMock()
    mock_client.models.generate_content.return_value.text = None
    mock_genai.Client.return_value = mock_client

    client = GoogleGenAIClient(model="gemini-test")
    with pytest.raises(LLMClientError, match="returned no text"):
        client.get_description("test prompt", 50)


@patch("schema_scribe.components.llm_clients.google_client.genai")
def test_google_client_retries_once_with_doubled_budget(mock_genai, mocker):
    """Thought-only responses retry once with a doubled output budget."""
    mocker.patch(
        "schema_scribe.components.llm_clients.google_client.settings"
    ).google_api_key = "fake_key"

    mock_client = MagicMock()
    first = MagicMock()
    first.text = None
    second = MagicMock()
    second.text = "retried answer"
    mock_client.models.generate_content.side_effect = [first, second]
    mock_genai.Client.return_value = mock_client

    client = GoogleGenAIClient(model="gemini-test")
    description = client.get_description("test prompt", 100)

    assert description == "retried answer"
    calls = mock_client.models.generate_content.call_args_list
    assert calls[0].kwargs["config"] == {"max_output_tokens": 100}
    assert calls[1].kwargs["config"] == {"max_output_tokens": 200}


@patch("schema_scribe.components.llm_clients.google_client.genai")
def test_google_client_retry_still_empty_raises(mock_genai, mocker):
    """Two empty responses in a row still raise LLMClientError."""
    mocker.patch(
        "schema_scribe.components.llm_clients.google_client.settings"
    ).google_api_key = "fake_key"

    mock_client = MagicMock()
    empty = MagicMock()
    empty.text = None
    mock_client.models.generate_content.return_value = empty
    mock_genai.Client.return_value = mock_client

    client = GoogleGenAIClient(model="gemini-test")
    with pytest.raises(LLMClientError, match="returned no text"):
        client.get_description("test prompt", 50)
    assert mock_client.models.generate_content.call_count == 2


class _QuotaError(Exception):
    """Mimics google.genai ClientError with a 429 code."""

    code = 429

    def __init__(self, message):
        super().__init__(message)
        self.message = message


@patch("schema_scribe.components.llm_clients.google_client.genai")
@patch("schema_scribe.components.llm_clients.google_client.time.sleep")
def test_google_client_429_retries_then_succeeds(mock_sleep, mock_genai, mocker):
    """429 responses retry with the server-advised delay, then succeed."""
    mocker.patch(
        "schema_scribe.components.llm_clients.google_client.settings"
    ).google_api_key = "fake_key"

    mock_client = MagicMock()
    ok = MagicMock()
    ok.text = "quota survivor"
    mock_client.models.generate_content.side_effect = [
        _QuotaError("Please retry in 12.5s."),
        ok,
    ]
    mock_genai.Client.return_value = mock_client

    client = GoogleGenAIClient(model="gemini-test")
    description = client.get_description("test prompt", 100)

    assert description == "quota survivor"
    mock_sleep.assert_called_once_with(12.5)
    assert mock_client.models.generate_content.call_count == 2


@patch("schema_scribe.components.llm_clients.google_client.genai")
@patch("schema_scribe.components.llm_clients.google_client.time.sleep")
def test_google_client_429_exhausted_raises(mock_sleep, mock_genai, mocker):
    """Persistent 429 after 3 retries raises LLMClientError."""
    mocker.patch(
        "schema_scribe.components.llm_clients.google_client.settings"
    ).google_api_key = "fake_key"

    mock_client = MagicMock()
    mock_client.models.generate_content.side_effect = _QuotaError(
        "Please retry in 30s."
    )
    mock_genai.Client.return_value = mock_client

    client = GoogleGenAIClient(model="gemini-test")
    with pytest.raises(LLMClientError, match="API call failed"):
        client.get_description("test prompt", 100)
    assert mock_client.models.generate_content.call_count == 4  # 1 + 3 retries
    assert mock_sleep.call_count == 3


@patch("schema_scribe.components.llm_clients.google_client.genai")
def test_google_client_whitespace_response_raises(mock_genai, mocker):
    """A whitespace-only response must raise LLMClientError, not return ''."""
    mocker.patch(
        "schema_scribe.components.llm_clients.google_client.settings"
    ).google_api_key = "fake_key"

    mock_client = MagicMock()
    empty = MagicMock()
    empty.text = "   "
    mock_client.models.generate_content.return_value = empty
    mock_genai.Client.return_value = mock_client

    client = GoogleGenAIClient(model="gemini-test")
    with pytest.raises(LLMClientError, match="empty description"):
        client.get_description("test prompt", 100)


@patch("schema_scribe.components.llm_clients.google_client.genai")
@patch("schema_scribe.components.llm_clients.google_client.time.sleep")
def test_google_client_429_then_thought_then_success(mock_sleep, mock_genai, mocker):
    """Combined path: 429 retry, then thought-only (doubled budget), then success."""
    mocker.patch(
        "schema_scribe.components.llm_clients.google_client.settings"
    ).google_api_key = "fake_key"

    mock_client = MagicMock()
    thought_only = MagicMock()
    thought_only.text = None
    ok = MagicMock()
    ok.text = "final answer"
    mock_client.models.generate_content.side_effect = [
        _QuotaError("Please retry in 5s."),
        thought_only,
        ok,
    ]
    mock_genai.Client.return_value = mock_client

    client = GoogleGenAIClient(model="gemini-test")
    description = client.get_description("test prompt", 100)

    assert description == "final answer"
    budgets = [c.kwargs["config"]["max_output_tokens"] for c in mock_client.models.generate_content.call_args_list]
    # 429 retry stays at 100; the thought-only response surfaces from the
    # quota loop's second attempt, then the doubled-budget retry uses 200.
    assert budgets == [100, 100, 200]
    mock_sleep.assert_called_once_with(5.0)
