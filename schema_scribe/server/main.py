"""
This module defines the main FastAPI application for the Schema Scribe server.

Design Rationale:
The server provides a RESTful API wrapper around the core workflows, enabling
programmatic or UI-driven execution. It mirrors the dependency injection (DI)
pattern used by the CLI (`app.py`), using the `ConfigManager` to build and
inject components into the workflows. This ensures consistent behavior
between the CLI and the server.
"""

import os
import json
from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List, Optional, Any

# Adheres to Phase 1 Refactoring: Import from new locations
from schema_scribe.config.manager import ConfigManager
from schema_scribe.workflows.db_workflow import DbWorkflow
from schema_scribe.workflows.dbt_workflow import DbtWorkflow
from schema_scribe.workflows.lineage_workflow import LineageWorkflow
from schema_scribe.core.exceptions import DataScribeError, CIError
from schema_scribe.utils.logger import get_logger

logger = get_logger(__name__)


app = FastAPI(
    title="Schema Scribe Server",
    description="API for running Schema Scribe documentation workflows.",
    version="1.0.0",
)

CATALOG_CACHE_FILE = "server_catalog.json"

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
    output_profile: str  # Note: Will be required for this endpoint


class RunDbtWorkflowRequest(BaseModel):
    """
    Defines the request body for triggering the 'dbt' workflow.
    Mode flags are mutually exclusive.
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
    Discovery endpoint that returns available profiles from `config.yaml`.
    Useful for populating UI dropdowns.
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

    This endpoint uses the `ConfigManager` to build and inject
    components (DB, LLM, Writer) into the DbWorkflow based on
    the profile names provided in the request.
    """
    db_connector = None
    try:
        logger.info(
            f"Received request to run 'db' workflow with profile: {request.db_profile}"
        )
        cfg_manager = ConfigManager("config.yaml")

        # 1. Get dependencies
        db_connector, db_name = cfg_manager.get_db_connector(request.db_profile)
        llm_client, _ = cfg_manager.get_llm_client(request.llm_profile)
        
        # Get output writer *only if specified*, 
        # but the primary goal is to update the cache.
        writer, out_name, writer_params = cfg_manager.get_writer(request.output_profile)

        workflow = DbWorkflow(
            db_connector=db_connector,
            llm_client=llm_client,
            writer=writer, # Pass writer if it exists
            db_profile_name=db_name,
            output_profile_name=out_name,
            writer_params=writer_params,
        )
        
        # 2. Generate catalog
        # This executes the logic and closes the connection.
        catalog_data = workflow.generate_catalog()

        # 3. Update the central cache
        try:
            with open(CATALOG_CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(catalog_data, f, indent=2)
            logger.info(f"Updated catalog cache file: {CATALOG_CACHE_FILE}")
        except Exception as e:
            logger.error(f"Failed to write catalog cache: {e}")
            # This isn't fatal, but we should log it.

        # 4. If a writer was requested, also run the write logic
        if writer:
            logger.info(f"Running writer for output profile: {out_name}")
            writer_kwargs = {
                "db_profile_name": db_name,
                "db_connector": db_connector, # Note: connection is closed, only for metadata
                **writer_params,
            }
            writer.write(catalog_data, **writer_kwargs)

        # 5. Return the new catalog data
        return catalog_data
    except DataScribeError as e:
        logger.error(f"Schema Scribe error running workflow: {e}", exc_info=True)
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

    Uses `ConfigManager` to build and inject dependencies.
    - CI Failures (check/drift) return HTTP 409 Conflict.
    - Interactive mode is disabled via API.
    """
    db_connector = None  # Ensure db_connector is defined in the outer scope
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
        llm_client, llm_name = cfg_manager.get_llm_client(request.llm_profile)
        db_name = None
        if request.db_profile:
            db_connector, db_name = cfg_manager.get_db_connector(
                request.db_profile
            )

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
            db_profile_name=db_name,  # Pass name for logging
            output_profile_name=out_name,
        )
        
        catalog_data = workflow.generate_catalog()
        
        action_mode = None
        if request.drift: action_mode = "drift"
        elif request.check: action_mode = "check"
        elif request.update_yaml: action_mode = "update"
        
        if action_mode:
            workflow._handle_yaml_update(action_mode, catalog_data)
            return {"status": "success", "mode": action_mode, "message": f"dbt {action_mode} complete."}
        try:
            with open(CATALOG_CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(catalog_data, f, indent=2)
            logger.info(f"Updated catalog cache file: {CATALOG_CACHE_FILE}")
        except Exception as e:
            logger.error(f"Failed to write catalog cache: {e}")
            
        if writer:
            logger.info(f"Running writer for output profile: {out_name}")
            workflow._handle_file_output(catalog_data)
            
        return catalog_data
    
    except CIError as e:
        logger.warning(f"CI check failed during API call: {e}")
        raise HTTPException(status_code=409, detail=str(e))
    except DataScribeError as e:
        logger.error(f"Schema Scribe error running workflow: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error running workflow: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"An unexpected error occurred: {e}"
        )


@app.get("/api/lineage/graph")
def get_global_lineage_graph(
    db_profile: str = Query(
        ..., description="DB profile to scan for physical FKs."
    ),
    dbt_project_dir: str = Query(
        ..., description="Path to the dbt project directory."
    ),
) -> Any:
    """
    Returns a JSON object of the global lineage graph (nodes and edges)
    for use in interactive UIs (e.g., react-flow).

    Combines physical (DB FKs) and logical (dbt) lineage.
    """
    db_connector = None
    try:
        # Adheres to Phase 1 DI pattern
        cfg_manager = ConfigManager("config.yaml")

        # 1. Build dependencies using ConfigManager
        db_connector, db_name = cfg_manager.get_db_connector(db_profile)

        # 2. Instantiate workflow, injecting dependencies (no writer)
        workflow = LineageWorkflow(
            db_connector=db_connector,
            writer=None,  # The API's goal is to return data, not save files
            dbt_project_dir=dbt_project_dir,
            db_profile_name=db_name,
            output_profile_name=None,
            writer_params={},
        )

        # 3. Call generate_catalog() instead of run()
        catalog_data = workflow.generate_catalog()

        # 4. Return the 'graph_json' payload for the UI
        return catalog_data["graph_json"]

    except DataScribeError as e:
        logger.error(
            f"Schema Scribe error generating lineage: {e}", exc_info=True
        )
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error generating lineage: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"An unexpected error occurred: {e}"
        )
    finally:
        # 5. Manually close resources
        # Since we didn't call workflow.run(), we must close the connection
        if db_connector:
            logger.info(f"Closing DB connection for {db_profile}...")
            db_connector.close()

@app.get("/api/catalog")
def get_cached_catalog() -> Any:
    """
    (NEW) Reads and returns the master catalog JSON file.
    This file is the central cache used by the UI.
    """
    if not os.path.exists(CATALOG_CACHE_FILE):
        return {"error": "Catalog cache not found. Please run a workflow first."}
    
    try:
        with open(CATALOG_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error reading cache file {CATALOG_CACHE_FILE}: {e}")
        raise HTTPException(status_code=500, detail="Could not read catalog cache.")

# --- Static File Serving ---

SERVER_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(SERVER_DIR, "static")


@app.get("/", include_in_schema=False)
async def read_index():
    """Serves the main index.html file for the frontend."""
    index_path = os.path.join(STATIC_DIR, "index.html")
    if not os.path.exists(index_path):
        return {
            "message": "Schema Scribe Server is running. Frontend 'index.html' not found."
        }
    return FileResponse(index_path)


app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")
