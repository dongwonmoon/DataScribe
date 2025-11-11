"""
This module defines the main FastAPI application for the Schema Scribe server.

Design Rationale:
The server provides a RESTful API wrapper around the core workflows, enabling
programmatic or UI-driven execution. It mirrors the dependency injection pattern
used by the CLI (`app.py`), using the `ConfigManager` to build and inject
components into the workflows. This ensures consistent behavior between the
CLI and the server. Error handling is managed at the API boundary, translating
internal application errors into appropriate HTTP status codes.
"""

import os
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List, Optional

from schema_scribe.config.manager import ConfigManager
from schema_scribe.workflows.db_workflow import DbWorkflow
from schema_scribe.workflows.dbt_workflow import DbtWorkflow
from schema_scribe.core.exceptions import DataScribeError, CIError
from schema_scribe.utils.logger import get_logger

logger = get_logger(__name__)


app = FastAPI(
    title="Schema Scribe Server",
    description="API for running Schema Scribe documentation workflows.",
    version="1.0.0",
)

# --- Pydantic Models for API Request/Response Validation ---


class ProfileInfo(BaseModel):
    """Defines the response structure for the profile discovery endpoint."""

    db_connections: List[str]
    llm_providers: List[str]
    output_profiles: List[str]


class RunDbWorkflowRequest(BaseModel):
    """Defines the request body for triggering the 'db' workflow."""

    db_profile: str
    llm_profile: str
    output_profile: str


class RunDbtWorkflowRequest(BaseModel):
    """
    Defines the request body for triggering the 'dbt' workflow.
    The mode flags (`update_yaml`, `check`, `drift`) are mutually exclusive.
    """

    dbt_project_dir: str
    llm_profile: Optional[str] = None
    db_profile: Optional[str] = None  # Required only for drift mode
    output_profile: Optional[str] = None
    # Mode flags
    update_yaml: bool = False
    check: bool = False
    drift: bool = False


# --- API Endpoints ---


@app.get("/api/profiles", response_model=ProfileInfo)
def get_profiles():
    """
    A discovery endpoint that returns available profiles from `config.yaml`.

    This is useful for UIs that need to populate dropdown menus with available
    connection, LLM, and output options.

    Raises:
        HTTPException(404): If `config.yaml` is not found.
        HTTPException(500): For other configuration loading errors.
    """
    try:
        # Use ConfigManager to safely load and access the config
        cfg_manager = ConfigManager("config.yaml")
        config = cfg_manager.config
        return {
            "db_connections": list(config.get("db_connections", {}).keys()),
            "llm_providers": list(config.get("llm_providers", {}).keys()),
            "output_profiles": list(config.get("output_profiles", {}).keys()),
        }
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail="config.yaml not found in the current directory.",
        )
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to load config: {e}"
        )


@app.post("/api/run/db", status_code=200)
def run_db_workflow(request: RunDbWorkflowRequest):
    """
    Runs the 'db' documentation workflow.

    This endpoint triggers a synchronous run of the `DbWorkflow`. It uses the
    `ConfigManager` to build the necessary components based on the profile names
    provided in the request, then injects them into the workflow.

    Args:
        request: A `RunDbWorkflowRequest` with the db, llm, and output profiles.

    Returns:
        A success message if the workflow completes.
    """
    try:
        logger.info(
            f"Received request to run 'db' workflow with profile: {request.db_profile}"
        )
        cfg_manager = ConfigManager("config.yaml")

        # Build components using ConfigManager
        db_connector, db_name = cfg_manager.get_db_connector(request.db_profile)
        llm_client, _ = cfg_manager.get_llm_client(request.llm_profile)
        writer, out_name, writer_params = cfg_manager.get_writer(
            request.output_profile
        )

        # Inject components into the workflow
        workflow = DbWorkflow(
            db_connector=db_connector,
            llm_client=llm_client,
            writer=writer,
            db_profile_name=db_name,
            output_profile_name=out_name,
            writer_params=writer_params,
        )
        workflow.run()
        return {
            "status": "success",
            "message": f"DB workflow completed for {request.db_profile}.",
        }
    except DataScribeError as e:
        logger.error(
            f"Schema Scribe error running workflow: {e}", exc_info=True
        )
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error running workflow: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"An unexpected error occurred: {e}"
        )


@app.post("/api/run/dbt", status_code=200)
def run_dbt_workflow(request: RunDbtWorkflowRequest):
    """
    Runs the 'dbt' documentation workflow with various modes.

    This endpoint triggers a synchronous run of the `DbtWorkflow`, using the
    `ConfigManager` to build and inject dependencies.

    - **CI Failures**: If `check` or `drift` mode is used and a failure is
      detected, this endpoint returns an **HTTP 409 Conflict** status, which
      can be used to fail a CI job.
    - **Interactive Mode**: The `interactive` mode is not supported via the API
      and is always disabled.

    Args:
        request: A `RunDbtWorkflowRequest` defining the project and execution mode.

    Returns:
        A success message if the workflow completes without a CI failure.
    """
    try:
        logger.info(
            f"Received request to run 'dbt' workflow for dir: {request.dbt_project_dir}"
        )
        if sum([request.update_yaml, request.check, request.drift]) > 1:
            raise HTTPException(
                status_code=400, detail="Modes are mutually exclusive."
            )
        if request.drift and not request.db_profile:
            raise HTTPException(
                status_code=400, detail="Drift mode requires a db_profile."
            )

        cfg_manager = ConfigManager("config.yaml")

        # Build components
        llm_client, _ = cfg_manager.get_llm_client(request.llm_profile)
        db_connector = None
        if request.db_profile:
            db_connector, _ = cfg_manager.get_db_connector(request.db_profile)
        writer, out_name, writer_params = cfg_manager.get_writer(
            request.output_profile
        )

        # Inject components into the workflow
        workflow = DbtWorkflow(
            llm_client=llm_client,
            dbt_project_dir=request.dbt_project_dir,
            update_yaml=request.update_yaml,
            check=request.check,
            interactive=False,  # Interactive mode is CLI-only
            drift=request.drift,
            db_connector=db_connector,
            writer=writer,
            writer_params=writer_params,
            output_profile_name=out_name,
        )
        workflow.run()
        return {
            "status": "success",
            "message": f"dbt workflow completed for {request.dbt_project_dir}.",
        }
    except CIError as e:
        logger.warning(f"CI check failed during API call: {e}")
        raise HTTPException(status_code=409, detail=str(e))
    except DataScribeError as e:
        logger.error(
            f"Schema Scribe error running workflow: {e}", exc_info=True
        )
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error running workflow: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"An unexpected error occurred: {e}"
        )


# --- Static File Serving ---

# Get the directory where this server file is located. This is necessary
# to reliably find the 'static' folder regardless of where the application
# is run from.
SERVER_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(SERVER_DIR, "static")


# Serve the main index.html file from the root path. This allows the app
# to be served at the base URL (e.g., http://localhost:8000).
@app.get("/", include_in_schema=False)
async def read_index():
    """Serves the main index.html file for the frontend."""
    index_path = os.path.join(STATIC_DIR, "index.html")
    if not os.path.exists(index_path):
        return {
            "message": "Schema Scribe Server is running. Frontend 'index.html' not found."
        }
    return FileResponse(index_path)


# Mount the 'static' directory to serve all other static files (CSS, JS, etc.).
# This must come after the root endpoint to ensure the root is served correctly.
# The `html=True` argument enables it to serve 'index.html' for sub-paths,
# which is useful for single-page applications (SPAs).
app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")
