"""
This module provides a simple, reusable logger configuration for the application.

Design Rationale:
A centralized logging utility is crucial for maintaining consistent and
actionable logs across an application. This module ensures that:
- **Consistency**: All log messages adhere to a predefined format, making them
  easier to read and parse.
- **Ease of Configuration**: Loggers can be obtained and used throughout the
  application without repetitive setup code.
- **Single Point of Control**: Future changes to logging behavior (e.g.,
  changing output destination, log level, or format) can be managed from
  a single location.
"""

import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """
    Configures and returns a logger with a specified name.

    Design Rationale:
    This function implements a "singleton-like" pattern for loggers. If a
    logger with the given name has already been configured, it returns the
    existing instance without re-configuring it. This prevents duplicate
    handlers and ensures consistent logging behavior across different parts
    of the application that request the same named logger.

    The logger is configured to:
    - Output messages of level INFO and above.
    - Stream log messages to standard output (stdout).
    - Format messages to include a timestamp, logger name, level, and the message itself.

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
