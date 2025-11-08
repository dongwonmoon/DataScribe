"""
Unit tests for the LLM clients.

This test suite verifies that the LLM client classes in `data_scribe.components.llm_clients`
are initialized correctly, handle configuration errors, and call the underlying APIs
with the expected parameters. All actual API calls are mocked.
"""

import pytest
from unittest.mock import patch, MagicMock

from data_scribe.components.llm_clients import (
    OpenAIClient,
    GoogleGenAIClient,
    OllamaClient,
)
from data_scribe.core.exceptions import ConfigError, LLMClientError


# --- OpenAIClient Tests ---

def test_openai_client_initialization(mocker):
    """Tests successful initialization of OpenAIClient."""
    mock_settings = mocker.patch("data_scribe.components.llm_clients.openai_client.settings")
    mock_settings.openai_api_key = "fake_api_key"
    mock_openai_constructor = mocker.patch("data_scribe.components.llm_clients.openai_client.OpenAI")

    client = OpenAIClient(model="gpt-test")
    assert client.model == "gpt-test"
    mock_openai_constructor.assert_called_once_with(api_key="fake_api_key")


def test_openai_client_missing_api_key(mocker):
    """Tests that OpenAIClient raises ConfigError if API key is missing."""
    mock_settings = mocker.patch("data_scribe.components.llm_clients.openai_client.settings")
    mock_settings.openai_api_key = None
    with pytest.raises(ConfigError, match="OPENAI_API_KEY environment variable not set"):
        OpenAIClient()


def test_openai_client_get_description(mocker):
    """Tests the get_description method of OpenAIClient."""
    mock_settings = mocker.patch("data_scribe.components.llm_clients.openai_client.settings")
    mock_settings.openai_api_key = "fake_api_key"

    mock_openai_instance = MagicMock()
    mock_openai_instance.chat.completions.create.return_value.choices[0].message.content = "  OpenAI response  "
    mocker.patch("data_scribe.components.llm_clients.openai_client.OpenAI", return_value=mock_openai_instance)

    client = OpenAIClient(model="gpt-test")
    description = client.get_description("test prompt", 100)

    assert description == "OpenAI response"
    mock_openai_instance.chat.completions.create.assert_called_once_with(
        model="gpt-test",
        messages=[{"role": "system", "content": "test prompt"}],
        max_tokens=100,
    )


# --- GoogleGenAIClient Tests ---

@patch("data_scribe.components.llm_clients.google_client.genai")
def test_google_client_initialization(mock_genai, mocker):
    """Tests successful initialization of GoogleGenAIClient."""
    mock_settings = mocker.patch("data_scribe.components.llm_clients.google_client.settings")
    mock_settings.google_api_key = "fake_api_key"

    client = GoogleGenAIClient(model="gemini-test")
    
    mock_genai.configure.assert_called_once_with(api_key="fake_api_key")
    mock_genai.GenerativeModel.assert_called_once_with("gemini-test")
    assert client.model is not None


def test_google_client_missing_api_key(mocker):
    """Tests that GoogleGenAIClient raises ConfigError if API key is missing."""
    mocker.patch("data_scribe.components.llm_clients.google_client.settings").google_api_key = None
    with pytest.raises(ConfigError, match="GOOGLE_API_KEY must be set"):
        GoogleGenAIClient(model="gemini-test")


@patch("data_scribe.components.llm_clients.google_client.genai")
def test_google_client_get_description(mock_genai, mocker):
    """Tests the get_description method of GoogleGenAIClient."""
    mocker.patch("data_scribe.components.llm_clients.google_client.settings").google_api_key = "fake_key"
    
    mock_model_instance = MagicMock()
    mock_model_instance.generate_content.return_value.text = "Google response"
    mock_genai.GenerativeModel.return_value = mock_model_instance

    client = GoogleGenAIClient(model="gemini-test")
    description = client.get_description("test prompt", 150)

    assert description == "Google response"
    mock_model_instance.generate_content.assert_called_once_with("test prompt")


# --- OllamaClient Tests ---

@patch("data_scribe.components.llm_clients.ollama_client.ollama.Client")
def test_ollama_client_initialization(mock_ollama_client):
    """Tests successful initialization of OllamaClient."""
    client = OllamaClient(model="llama-test", host="http://ollama:11434")
    assert client.model == "llama-test"
    mock_ollama_client.assert_called_once_with(host="http://ollama:11434")
    # Check that pull was called
    mock_ollama_client.return_value.pull.assert_called_once_with("llama-test")


@patch("data_scribe.components.llm_clients.ollama_client.ollama.Client")
def test_ollama_client_get_description(mock_ollama_client):
    """Tests the get_description method of OllamaClient."""
    mock_instance = mock_ollama_client.return_value
    mock_instance.chat.return_value = {"message": {"content": "Ollama response"}}

    client = OllamaClient(model="llama-test", host="http://ollama:11434")
    description = client.get_description("test prompt", 200)

    assert description == "Ollama response"
    mock_instance.chat.assert_called_once_with(
        model="llama-test",
        messages=[{"role": "system", "content": "test prompt"}],
        options={"num_predict": 200},
    )


# --- General Error Handling Test ---

def test_llm_client_api_error(mocker):
    """Tests that a generic LLMClientError is raised on API failure."""
    mocker.patch("data_scribe.components.llm_clients.openai_client.settings").openai_api_key = "fake_key"
    
    # Make the create call itself raise the error
    mock_completions = MagicMock()
    mock_completions.create.side_effect = Exception("API is down")
    
    mock_openai_instance = MagicMock()
    mock_openai_instance.chat.completions = mock_completions
    
    mocker.patch("data_scribe.components.llm_clients.openai_client.OpenAI", return_value=mock_openai_instance)

    client = OpenAIClient(model="gpt-test")
    
    with pytest.raises(LLMClientError, match="OpenAI API call failed: API is down"):
        client.get_description("prompt", 100)