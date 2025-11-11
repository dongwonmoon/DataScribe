"""
This module manages the application's configuration settings by loading them
from environment variables.

Design Rationale:
This module implements a robust and centralized configuration management strategy.
- **`python-dotenv`**: Automatically loads variables from a `.env` file,
  which is ideal for local development as it keeps sensitive information
  out of version control and simplifies environment setup.
- **Singleton `Settings` object**: Provides a globally consistent and
  type-hinted access to all configuration values. This ensures that settings
  are loaded only once and are uniformly available throughout the application,
  preventing discrepancies and promoting maintainability.
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

    This class provides a single, typed interface for all configuration variables
    that are loaded from the environment. It acts as a single source of truth
    for application settings, promoting consistency and type safety across the codebase.
    """

    def __init__(self):
        """
        Initializes the Settings object by loading values from the environment.

        To add a new setting, declare it as a class attribute and load it from
        the environment using `os.getenv`. For example:
        `self.new_key: str | None = os.getenv("NEW_KEY")`
        Ensure that corresponding environment variables are set in your
        deployment environment or in a `.env` file for local development.
        """
        # Load the OpenAI API key from the `OPENAI_API_KEY` environment variable.
        self.openai_api_key: str | None = os.getenv("OPENAI_API_KEY")

        # Load the Google API key from the `GOOGLE_API_KEY` environment variable.
        self.google_api_key: str | None = os.getenv("GOOGLE_API_KEY")


# Create a single, globally accessible instance of the Settings class.
# This singleton pattern ensures that settings are loaded only once and are
# consistently available throughout the application, preventing discrepancies
# and simplifying access to configuration values.
settings = Settings()
