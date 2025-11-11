"""
This module serves as the primary entry point for the Schema Scribe
command-line interface (CLI).

Design Rationale:
The purpose of this file is to provide a clean, executable entry point for the
console script defined in `pyproject.toml`. It imports the main Typer `app`
object from `schema_scribe.app` and executes it. This separation keeps the
entry point logic minimal and distinct from the CLI command definitions found
in the `app` module.
"""

from schema_scribe.app import app

if __name__ == "__main__":
    # If this script is run directly, execute the Typer application.
    # This allows for running the CLI via `python -m schema_scribe.main`.
    app()
