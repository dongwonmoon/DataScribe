"""
This module provides a concrete implementation of the `BaseLLMClient` for
the OpenAI API.
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

    This class implements the `BaseLLMClient` interface to provide a standardized
    way to generate text using models from OpenAI.
    """

    def __init__(self, model: str = "gpt-3.5-turbo"):
        """
        Initializes the OpenAIClient.

        Args:
            model: The name of the OpenAI model to use (e.g., "gpt-3.5-turbo").

        Raises:
            ConfigError: If the `OPENAI_API_KEY` environment variable is not set.
        """
        # Retrieve the API key from the application settings
        api_key = settings.openai_api_key
        if not api_key:
            logger.error("OPENAI_API_KEY environment variable not set.")
            raise ConfigError("OPENAI_API_KEY environment variable not set.")

        logger.info(f"Initializing OpenAI client with model: {model}")
        # Instantiate the OpenAI client with the API key
        self.client = OpenAI(api_key=api_key)
        # Store the model name for later use
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
            logger.info(f"Sending prompt to OpenAI model '{self.model}'.")
            # Use the chat completions endpoint to generate a response
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "system", "content": prompt}],
                max_tokens=max_tokens,
            )
            # Extract the content of the message from the first choice
            description = response.choices[0].message.content.strip()
            logger.info("Successfully received description from OpenAI.")
            return description
        except Exception as e:
            logger.error(
                f"Failed to generate AI description: {e}", exc_info=True
            )
            # Return a fallback message if the API call fails
            raise LLMClientError(f"OpenAI API call failed: {e}") from e
