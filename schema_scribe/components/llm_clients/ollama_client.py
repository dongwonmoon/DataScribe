"""
This module provides a concrete implementation of the `BaseLLMClient` for
the Ollama API, allowing interaction with locally-run language models.
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
    way to generate text using models hosted via Ollama.
    """

    def __init__(
        self, model: str = "llama3", host: str = "http://localhost:11434"
    ):
        """
        Initializes the OllamaClient.

        Args:
            model: The name of the Ollama model to use (e.g., "llama3").
            host: The host URL of the Ollama API.

        Raises:
            ConfigError: If the client fails to initialize or pull the model.
        """
        try:
            logger.info(
                f"Initializing Ollama client with model: {model} and host: {host}"
            )
            self.client = ollama.Client(host=host)
            self.model = model
            logger.info(f"Pulling model '{model}'...")
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
            max_tokens: The maximum number of tokens to generate in the response.

        Returns:
            The AI-generated description as a string.

        Raises:
            LLMClientError: If the API call to Ollama fails.
        """
        try:
            logger.info(f"Sending prompt to Ollama model '{self.model}'.")
            logger.debug(f"Prompt: {prompt}")
            response = self.client.chat(
                model=self.model,
                messages=[{"role": "system", "content": prompt}],
                options={"num_predict": max_tokens},
            )
            description = response["message"]["content"].strip()
            logger.info("Successfully received description from Ollama.")
            logger.debug(f"Generated description: {description}")
            return description
        except Exception as e:
            logger.error(
                f"Failed to generate AI description with Ollama: {e}",
                exc_info=True,
            )
            raise LLMClientError(f"Ollama API call failed: {e}") from e
