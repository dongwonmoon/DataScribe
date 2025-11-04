import google.generativeai as genai
from data_scribe.core.interfaces import BaseLLMClient
from data_scribe.utils.config import settings
from data_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class GoogleGenAIClient(BaseLLMClient):
    """A client for interacting with Google's Generative AI (Gemini) models."""

    def __init__(self, model: str = "gemini-pro"):
        """
        Initializes the Google GenAI (Gemini) client.

        This method configures the `google.generativeai` library with the API key
        from the application settings and instantiates the specified model.

        Args:
            model: The name of the Gemini model to use, as specified in the
                   `config.yaml` file (e.g., 'gemini-pro').

        Raises:
            ValueError: If the `GOOGLE_API_KEY` is not found in the environment
                        variables or .env file.
        """
        api_key = settings.google_api_key
        if not api_key:
            logger.error("GOOGLE_API_KEY environment variable not set.")
            raise ValueError(
                "GOOGLE_API_KEY must be set in the .env file to use GoogleGenAIClient."
            )

        try:
            logger.info(f"Initializing Google GenAI client with model: {model}")
            genai.configure(api_key=api_key)
            self.model = genai.GenerativeModel(model)
            logger.info("Google GenAI client initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize Google GenAI client: {e}", exc_info=True)
            raise ValueError(f"Failed to initialize Google GenAI client: {e}")

    def get_description(self, prompt: str, max_tokens: int) -> str:
        """
        Generates a description using the configured Google Gemini model.

        Note on `max_tokens`:
        The `google-generativeai` library does not use a direct `max_tokens`
        parameter in the `generate_content` method. Instead, output length is
        controlled via a `generation_config` object, which can be passed during
        model instantiation or in the generation call. For simplicity, this
        implementation does not use it, and the `max_tokens` argument is ignored.

        Args:
            prompt: The prompt to send to the language model.
            max_tokens: The maximum number of tokens to generate (currently ignored).

        Returns:
            The AI-generated description as a string, or a failure message if an
            error occurs.
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
            logger.error(f"Failed to generate description with Google GenAI: {e}", exc_info=True)
            return "(Google AI generation failed)"
