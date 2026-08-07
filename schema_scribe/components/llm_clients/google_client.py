"""
This module provides `GoogleGenAIClient`, a concrete implementation of the
`BaseLLMClient` interface for Google's Generative AI (Gemini) API.

Design Rationale:
This client wraps the official `google-genai` SDK. It integrates with the
application's centralized settings management (`schema_scribe.utils.config.settings`),
which securely loads the `GOOGLE_API_KEY` from environment variables (e.g., a
`.env` file). This approach avoids hardcoding secrets and keeps API key handling
consistent and secure.

The `google-genai` SDK (not the deprecated `google-generativeai`) is required
because reasoning models (e.g. gemini-3.5-flash) return responses with a
`thoughtSignature` part that the legacy SDK cannot parse — it returned empty
responses with finish_reason MAX_TOKENS. The new SDK extracts `response.text`
correctly (verified live on 2026-08-07).
"""

from google import genai
from schema_scribe.core.interfaces import BaseLLMClient
from schema_scribe.core.exceptions import LLMClientError, ConfigError
from schema_scribe.utils.config import settings
from schema_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class GoogleGenAIClient(BaseLLMClient):
    """
    A client for interacting with Google's Generative AI (Gemini) models.

    This class implements the `BaseLLMClient` interface. Its primary
    responsibilities are to:
    1.  Fetch the `GOOGLE_API_KEY` from the application's central settings.
    2.  Build a `google.genai.Client` with the API key.
    3.  Wrap the `generate_content` API call to provide a consistent
        `get_description` method.
    """

    def __init__(self, model: str = "gemini-2.5-flash"):
        """
        Initializes the Google GenAI (Gemini) client.

        This method builds a `google.genai.Client` with the API key retrieved
        from the application's settings.

        Args:
            model: The name of the Gemini model to use, as specified in the
                   `config.yaml` file (e.g., 'gemini-3.5-flash').

        Raises:
            ConfigError: If the `GOOGLE_API_KEY` is not found in the environment
                         or if the client fails to initialize.
        """
        api_key = settings.google_api_key
        if not api_key:
            raise ConfigError(
                "GOOGLE_API_KEY must be set in your environment (e.g., in a .env file) "
                "to use GoogleGenAIClient."
            )

        try:
            logger.info(f"Initializing Google GenAI client with model: {model}")
            self.client = genai.Client(api_key=api_key)
            self.model = model
            logger.info("Google GenAI client initialized successfully.")
        except Exception as e:
            logger.error(
                f"Failed to initialize Google GenAI client: {e}", exc_info=True
            )
            raise ConfigError(
                f"Failed to initialize Google GenAI client: {e}"
            ) from e

    def get_description(self, prompt: str, max_tokens: int) -> str:
        """
        Generates a description using the configured Google Gemini model.

        Args:
            prompt: The prompt to send to the language model.
            max_tokens: The maximum number of tokens to generate.

        Returns:
            The AI-generated description as a string.

        Raises:
            LLMClientError: If the API call to Google GenAI fails.
        """
        try:
            logger.info(
                f"Sending prompt to Google GenAI '{self.model}' model..."
            )
            response = self.client.models.generate_content(
                model=self.model,
                contents=prompt,
                config={"max_output_tokens": max_tokens},
            )
            if response.text is None:
                raise LLMClientError(
                    "Google GenAI returned no text for this request "
                    "(the model may have exhausted its output budget on "
                    "thinking tokens)."
                )
            description = response.text.strip()
            logger.info("Response received from Google GenAI.")
            return description
        except Exception as e:
            logger.error(
                f"Failed to generate description with Google GenAI: {e}",
                exc_info=True,
            )
            raise LLMClientError(f"Google GenAI API call failed: {e}") from e
