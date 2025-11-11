"""
This module provides a concrete implementation of the `BaseLLMClient` for
Google's Generative AI (Gemini) API.
"""

import google.generativeai as genai
from schema_scribe.core.interfaces import BaseLLMClient
from schema_scribe.core.exceptions import LLMClientError, ConfigError
from schema_scribe.utils.config import settings
from schema_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class GoogleGenAIClient(BaseLLMClient):
    """
    A client for interacting with Google's Generative AI (Gemini) models.

    This class handles the configuration and API calls to the Google Gemini API,
    using the `google-generativeai` library.
    """

    def __init__(self, model: str = "gemini-pro"):
        """
        Initializes the Google GenAI (Gemini) client.

        This method configures the `google.generativeai` library with the API key
        from the application settings and instantiates the specified model.

        Args:
            model: The name of the Gemini model to use, as specified in the
                   `config.yaml` file (e.g., 'gemini-pro').

        Raises:
            ConfigError: If the `GOOGLE_API_KEY` is not found in the environment
                         or if the client fails to initialize.
        """
        api_key = settings.google_api_key
        if not api_key:
            logger.error("GOOGLE_API_KEY environment variable not set.")
            raise ConfigError(
                "GOOGLE_API_KEY must be set in the .env file to use GoogleGenAIClient."
            )

        try:
            logger.info(f"Initializing Google GenAI client with model: {model}")
            genai.configure(api_key=api_key)
            self.model = genai.GenerativeModel(model)
            logger.info("Google GenAI client initialized successfully.")
        except Exception as e:
            logger.error(
                f"Failed to initialize Google GenAI client: {e}", exc_info=True
            )
            raise ConfigError(f"Failed to initialize Google GenAI client: {e}")

    def get_description(self, prompt: str, max_tokens: int) -> str:
        """
        Generates a description using the configured Google Gemini model.

        Note on `max_tokens`:
        The `google-generativeai` library does not use a direct `max_tokens`
        parameter in the `generate_content` method. Instead, output length is
        controlled via a `generation_config` object. For simplicity, this
        implementation does not use it, and the `max_tokens` argument is ignored.

        Args:
            prompt: The prompt to send to the language model.
            max_tokens: The maximum number of tokens to generate (currently ignored).

        Returns:
            The AI-generated description as a string.

        Raises:
            LLMClientError: If the API call to Google GenAI fails.
        """
        try:
            logger.info(
                f"Sending prompt to Google GenAI '{self.model.model_name}' model..."
            )

            response = self.model.generate_content(prompt)

            description = response.text.strip()
            logger.info("Response received from Google GenAI.")
            return description
        except Exception as e:
            logger.error(
                f"Failed to generate description with Google GenAI: {e}",
                exc_info=True,
            )
            raise LLMClientError(f"Google GenAI API call failed: {e}") from e
