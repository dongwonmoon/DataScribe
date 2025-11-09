"""
Unit tests for the OllamaClient.
"""

from unittest.mock import patch

from data_scribe.components.llm_clients import OllamaClient


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
    mock_instance.chat.return_value = {
        "message": {"content": "Ollama response"}
    }

    client = OllamaClient(model="llama-test", host="http://ollama:11434")
    description = client.get_description("test prompt", 200)

    assert description == "Ollama response"
    mock_instance.chat.assert_called_once_with(
        model="llama-test",
        messages=[{"role": "system", "content": "test prompt"}],
        options={"num_predict": 200},
    )
