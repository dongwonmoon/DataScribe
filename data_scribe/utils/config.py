"""
This module manages the application's configuration settings, primarily by loading
them from environment variables defined in a `.env` file.

It uses the `pydantic-settings` library (implicitly via `python-dotenv` in this setup)
and a singleton pattern to provide a globally accessible, consistent configuration object.
"""

import os
from dotenv import load_dotenv

# Load environment variables from a .env file into the application's environment.
# This is called at the module level to ensure that environment variables are
# available as soon as the `settings` object is imported elsewhere.
load_dotenv()


class Settings:
    """
    A centralized class for managing application settings from environment variables.

    This class acts as a single source of truth for all configuration variables.
    Attributes are defined on this class and are automatically populated from
    environment variables with the same name (case-insensitive).

    Usage:
        from data_scribe.utils.config import settings
        api_key = settings.openai_api_key
    """

    def __init__(self):
        """
        Initializes the Settings object by loading values from the environment.

        To add a new setting, declare it as a class attribute with a type hint.
        The value will be automatically loaded from the corresponding environment
        variable. For example, to add a setting for a new service, you would add
        the following line to this class:

        `self.new_service_api_key: str | None = os.getenv("NEW_SERVICE_API_KEY")`

        Then, you can access it anywhere in the application via:
        `from data_scribe.utils.config import settings`
        `key = settings.new_service_api_key`
        """
        # Load the OpenAI API key from the `OPENAI_API_KEY` environment variable.
        self.openai_api_key: str | None = os.getenv("OPENAI_API_KEY")

        # Load the Google API key from the `GOOGLE_API_KEY` environment variable.
        self.google_api_key: str | None = os.getenv("GOOGLE_API_KEY")


# Create a single, globally accessible instance of the Settings class.
# This singleton pattern ensures that settings are loaded only once and are
# consistently available throughout the application.
settings = Settings()
