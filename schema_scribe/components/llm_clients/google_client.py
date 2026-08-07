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

import re
import time

from google import genai
from google.genai import types
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

    def __init__(self, model: str = "gemma-4-26b-a4b-it"):
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
            description = self._generate_with_budget_ladder(prompt, max_tokens)
            logger.info("Response received from Google GenAI.")
            return description
        except Exception as e:
            logger.error(
                f"Failed to generate description with Google GenAI: {e}",
                exc_info=True,
            )
            raise LLMClientError(f"Google GenAI API call failed: {e}") from e

    def _generate_with_budget_ladder(self, prompt: str, max_tokens: int) -> str:
        """Generates a description, escalating the output budget on failure.

        Reasoning models (e.g. gemma-4-26b) emit a thought part whose length
        varies per prompt. Two failure modes were measured live on
        2026-08-08 (10-column sweep): thought-only responses (text None) at
        512 tokens, and TRUNCATED answers (text present, finish_reason
        MAX_TOKENS — e.g. "A" for "A unique identifier...") which a text-only
        check would silently accept. Both escalate through [base, 2x, 4x];
        anything else with text is accepted, and a STOP finish with empty
        text raises.
        """
        budgets = [max_tokens, max_tokens * 2, max_tokens * 4]
        for budget in budgets:
            response = self._generate_with_quota_retry(prompt, budget)
            text = response.text
            truncated = (
                response.candidates[0].finish_reason
                == types.FinishReason.MAX_TOKENS
            )
            if text is None or truncated:
                logger.warning(
                    f"Google GenAI incomplete response "
                    f"(text={'none' if text is None else 'truncated'}); "
                    f"retrying with budget {budget * 2}."
                )
                continue
            description = text.strip()
            if not description:
                raise LLMClientError(
                    "Google GenAI returned an empty description for this "
                    "request."
                )
            return description
        raise LLMClientError(
            "Google GenAI returned no complete text for this request "
            "after escalating the output budget "
            f"({budgets[-1]} tokens max)."
        )

    @staticmethod
    def _parse_retry_delay(message: str) -> float:
        """Extracts the server-advised retry delay from a 429 message."""
        match = re.search(r"retry in ([\d.]+)s", message)
        return float(match.group(1)) if match else 30.0

    def _generate_with_quota_retry(self, prompt: str, max_tokens: int):
        """Calls generate_content, retrying up to 3 times on 429 with the
        server-advised delay. Free tiers (5-15 RPM per model) make quota
        exhaustion a normal condition for multi-call runs (verified live
        2026-08-07); without this the run aborts mid-way.
        """
        last_error = None
        for attempt in range(4):
            try:
                return self.client.models.generate_content(
                    model=self.model,
                    contents=prompt,
                    config={"max_output_tokens": max_tokens},
                )
            except Exception as e:  # noqa: BLE001 - SDK raises broad errors
                last_error = e
                if getattr(e, "code", None) == 429 and attempt < 3:
                    delay = self._parse_retry_delay(str(e))
                    logger.warning(
                        f"Google GenAI quota exceeded; retrying in "
                        f"{delay:.0f}s (attempt {attempt + 1}/3)."
                    )
                    time.sleep(delay)
                    continue
                raise
        raise last_error
