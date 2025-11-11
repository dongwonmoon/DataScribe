"""
This module provides `OllamaClient`, a concrete implementation of the
`BaseLLMClient` interface for the Ollama API.

Design Rationale:
The `OllamaClient` is a key component for users who wish to leverage locally-run
language models instead of relying on cloud-based services. This offers several
advantages:
- **Privacy**: Data never leaves the user's machine.
- **Cost**: No API fees for model usage.
- **Offline Capability**: Can function without an internet connection once models
  are downloaded.

The client is designed to be robust, automatically pulling the specified model
on initialization to ensure it is available for use.
"""

import ollama
from typing import Dict, Any

from schema_scribe.core.interfaces import BaseLLMClient
from schema_scribe.core.exceptions import LLMClientError, ConfigError
from schema_scribe.utils.logger import get_logger

# Initialize a logger for this module
logger = get_logger(__name__)


class OllamaClient(BaseLLMClient):
    """
    A client for interacting with a local Ollama API.

    This class implements the `BaseLLMClient` interface to provide a standardized
    way to generate text using models hosted via Ollama. Its responsibilities are:
    1.  Connect to a local Ollama instance at a specified host URL.
    2.  Proactively `pull` the specified model on initialization to ensure it is
        available, simplifying the user experience.
    3.  Wrap the `chat` API call to provide a consistent `get_description` method.
    """

    def __init__(
        self, model: str = "llama3", host: str = "http://localhost:11434"
    ):
        """
        Initializes the OllamaClient.

        This method creates a client for the specified Ollama host and performs
        a `pull` operation to ensure the requested model is available locally.
        Note that this initial pull may take some time on the first run for a
        given model.

        Args:
            model: The name of the Ollama model to use (e.g., "llama3").
            host: The host URL of the Ollama API.

        Raises:
            ConfigError: If the client fails to initialize or pull the model,
                         often due to an unreachable host.
        """
        try:
            logger.info(
                f"Initializing Ollama client with model: {model} and host: {host}"
            )
            self.client = ollama.Client(host=host)
            self.model = model
            logger.info(f"Pulling model '{model}' to ensure it is available...")
            self.client.pull(model)
            logger.info("Ollama client initialized successfully.")
        except Exception as e:
            logger.error(
                f"Failed to initialize Ollama client: {e}", exc_info=True
            )
            raise ConfigError(f"Failed to initialize Ollama client: {e}") from e

    def get_description(self, prompt: str, max_tokens: int) -> str:
        """
        Generates a description for a given prompt using the Ollama API.

        Args:
            prompt: The prompt to send to the language model.
            max_tokens: The maximum number of tokens to generate. This is passed
                        to the Ollama API via the `num_predict` option.

        Returns:
            The AI-generated description as a string.

        Raises:
            LLMClientError: If the API call to Ollama fails.
        """
        try:
            logger.info(f"Sending prompt to Ollama model '{self.model}'...")
            response = self.client.chat(
                model=self.model,
                messages=[{"role": "system", "content": prompt}],
                options={"num_predict": max_tokens},
            )
            description = response["message"]["content"].strip()
            logger.info("Successfully received description from Ollama.")
            return description
        except Exception as e:
            logger.error(
                f"Failed to generate AI description with Ollama: {e}",
                exc_info=True,
            )
            raise LLMClientError(f"Ollama API call failed: {e}") from e
