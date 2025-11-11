"""
This module provides a simple, reusable logger configuration for the application.

The `get_logger` function configures and returns a standard Python logger that
outputs formatted messages to the console.
"""

import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """
    Configures and returns a logger with a specified name.

    The logger is configured to output messages of level INFO and above to the
    console (stdout). The format includes a timestamp, logger name, level, and
    message. If a logger with the given name has already been configured, this
    function returns the existing instance without re-configuring it.

    Args:
        name: The name for the logger, typically the module's `__name__`.

    Returns:
        A configured `logging.Logger` instance.
    """
    # Get a logger instance for the specified name
    logger = logging.getLogger(name)

    # Configure the logger only if it hasn't been configured already
    if not logger.handlers:
        logger.setLevel(logging.INFO)

        # Create a handler to stream log messages to standard output
        handler = logging.StreamHandler(sys.stdout)

        # Define the format for the log messages
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)

        # Add the configured handler to the logger
        logger.addHandler(handler)

    return logger
