"""
This module provides `OpenAIClient`, a concrete implementation of the
`BaseLLMClient` interface for the OpenAI API.

Design Rationale:
This client is designed to be a straightforward and secure wrapper around the
official `openai` Python library. It integrates with the application's
centralized settings management (`schema_scribe.utils.config.settings`), which
securely loads the `OPENAI_API_KEY` from environment variables (e.g., a `.env`
file). This approach avoids hardcoding secrets and keeps API key handling
consistent and secure across the application.
"""

from openai import OpenAI
from schema_scribe.core.interfaces import BaseLLMClient
from schema_scribe.core.exceptions import LLMClientError, ConfigError
from schema_scribe.utils.config import settings
from schema_scribe.utils.logger import get_logger

# Initialize a logger for this module
logger = get_logger(__name__)


class OpenAIClient(BaseLLMClient):
    """
    A client for interacting with the OpenAI API.

    This class implements the `BaseLLMClient` interface. Its primary
    responsibilities are to:
    1.  Fetch the `OPENAI_API_KEY` from the application's central settings.
    2.  Initialize the `openai` library's client.
    3.  Wrap the `chat.completions.create` API call to provide a consistent
        `get_description` method.
    """

    def __init__(self, model: str = "gpt-3.5-turbo"):
        """
        Initializes the OpenAIClient.

        This constructor retrieves the API key from the application's settings
        (which are loaded from environment variables) and initializes the client.

        Args:
            model: The name of the OpenAI model to use (e.g., "gpt-3.5-turbo"),
                   as specified in the `config.yaml` file.

        Raises:
            ConfigError: If the `OPENAI_API_KEY` environment variable is not set.
        """
        api_key = settings.openai_api_key
        if not api_key:
            raise ConfigError(
                "OPENAI_API_KEY must be set in your environment (e.g., in a .env file) "
                "to use OpenAIClient."
            )

        logger.info(f"Initializing OpenAI client with model: {model}")
        self.client = OpenAI(api_key=api_key)
        self.model = model
        logger.info("OpenAI client initialized successfully.")

    def get_description(self, prompt: str, max_tokens: int) -> str:
        """
        Generates a description for a given prompt using the OpenAI API.

        Args:
            prompt: The prompt to send to the language model.
            max_tokens: The maximum number of tokens to generate in the response.

        Returns:
            The AI-generated description as a string.

        Raises:
            LLMClientError: If the API call to OpenAI fails.
        """
        try:
            logger.info(f"Sending prompt to OpenAI model '{self.model}'...")
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "system", "content": prompt}],
                max_tokens=max_tokens,
            )
            description = response.choices[0].message.content.strip()
            logger.info("Successfully received description from OpenAI.")
            return description
        except Exception as e:
            logger.error(
                f"Failed to generate AI description with OpenAI: {e}",
                exc_info=True,
            )
            raise LLMClientError(f"OpenAI API call failed: {e}") from e
