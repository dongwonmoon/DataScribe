import ollama
from typing import Dict, Any

from data_scribe.core.interfaces import BaseLLMClient
from data_scribe.utils.logger import get_logger

# Initialize a logger for this module
logger = get_logger(__name__)


class OllamaClient(BaseLLMClient):

    def __init__(self, model: str = "llama3", host: str = "http://localhost:11434"):
        try:
            logger.info(f"Initializing Ollama client with model: {model}")
            self.client = ollama.Client(host=host)
            self.model = model
            self.client.pull(model)
            logger.info("Ollama client initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize Ollama client: {e}", exc_info=True)
            raise

    def get_description(self, prompt: str, max_tokens: int) -> str:
        try:
            logger.info(f"Sending prompt to Ollama model '{self.model}'.")
            response = self.client.chat(
                model=self.model,
                messages=[{"role": "system", "content": prompt}],
                options={"num_predict": max_tokens},
            )
            description = response["message"]["content"].strip()
            logger.info("Successfully received description from Ollama.")
            return description
        except Exception as e:
            logger.error(f"Failed to generate AI description: {e}", exc_info=True)
            return "(AI description generation failed)"
