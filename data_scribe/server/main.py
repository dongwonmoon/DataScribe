"""
Main FastAPI application for the Data Scribe server.

This module defines the FastAPI application and its API endpoints. It provides
a web-based interface to run Data Scribe's core documentation workflows,
reusing the existing workflow classes for consistency.
"""
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Optional

from data_scribe.core.workflow_helpers import load_and_validate_config
from data_scribe.core.db_workflow import DbWorkflow
from data_scribe.core.dbt_workflow import DbtWorkflow
from data_scribe.core.exceptions import DataScribeError, CIError
from data_scribe.utils.logger import get_logger

logger = get_logger(__name__)


app = FastAPI(
    title="Data Scribe Server",
    description="API for running Data Scribe documentation workflows."
)

# --- API Request/Response Models ---

class ProfileInfo(BaseModel):
    """Defines the structure for returning available profile names."""
    db_connections: List[str]
    llm_providers: List[str]
    output_profiles: List[str]

class RunDbWorkflowRequest(BaseModel):
    """Defines the request body for running the 'db' workflow."""
    db_profile: str
    llm_profile: str
    output_profile: str
    
class RunDbtWorkflowRequest(BaseModel):
    """Request body for running the 'dbt' workflow."""
    dbt_project_dir: str
    llm_profile: Optional[str] = None
    
    # Modes (mutually exclusive)
    update_yaml: bool = False
    check: bool = False
    drift: bool = False
    
    # Other args
    db_profile: Optional[str] = None # Required for drift
    output_profile: Optional[str] = None

# --- API Endpoints ---

@app.get("/api/profiles", response_model=ProfileInfo)
def get_profiles():
    """
    Loads and returns the available profiles from the `config.yaml` file.

    This is useful for UIs that need to populate dropdown menus with available
    connection, LLM, and output options.

    Raises:
        HTTPException(404): If the `config.yaml` file is not found in the
                            directory where the server is running.
        HTTPException(500): If there is a general error loading or parsing
                            the configuration file.
    """
    try:
        # We assume config.yaml is in the directory where the server is run
        config = load_and_validate_config("config.yaml") 
        return {
            "db_connections": list(config.get("db_connections", {}).keys()),
            "llm_providers": list(config.get("llm_providers", {}).keys()),
            "output_profiles": list(config.get("output_profiles", {}).keys()),
        }
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="config.yaml not found.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load config: {e}")

@app.post("/api/run/db")
def run_db_workflow(request: RunDbWorkflowRequest):
    """
    Runs the 'db' documentation workflow based on the provided profiles.

    This endpoint triggers a synchronous run of the `DbWorkflow`, which connects
    to a database, generates documentation, and writes it to the specified output.

    Args:
        request: A `RunDbWorkflowRequest` containing the names of the db, llm,
                 and output profiles to use.

    Returns:
        A success message if the workflow completes without errors.

    Raises:
        HTTPException(400): If there is a configuration or operational error
                            within the Data Scribe workflow (e.g., bad profile name).
        HTTPException(500): For any other unexpected errors during the workflow.
    """
    try:
        logger.info(f"Received request to run 'db' workflow with profile: {request.db_profile}")
        
        # --- REUSE THE WORKFLOW ---
        # We instantiate the workflow class just like the CLI does.
        workflow = DbWorkflow(
            config_path="config.yaml",
            db_profile=request.db_profile,
            llm_profile=request.llm_profile,
            output_profile=request.output_profile
        )
        
        # Run the workflow (this is a synchronous call)
        workflow.run()
        
        return {"status": "success", "message": f"DB workflow completed for {request.db_profile}."}

    except DataScribeError as e:
        # Catch our custom exceptions and return a 400 Bad Request
        logger.error(f"Data Scribe error running workflow: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # Catch any other unexpected errors
        logger.error(f"Unexpected error running workflow: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")

@app.post("/api/run/dbt")
def run_dbt_workflow(request: RunDbtWorkflowRequest):
    """
    Runs the 'dbt' documentation workflow.
    Note: 'interactive' mode is not supported via API.
    """
    try:
        logger.info(f"Received request to run 'dbt' workflow for dir: {request.dbt_project_dir}")

        # --- Validate Inputs ---
        mode_flags = sum([request.update_yaml, request.check, request.drift])
        if mode_flags > 1:
            raise HTTPException(
                status_code=400, 
                detail="--update, --check, and --drift are mutually exclusive."
            )
        
        if request.drift and not request.db_profile:
            raise HTTPException(
                status_code=400, 
                detail="--drift mode requires --db_profile."
            )

        # --- REUSE THE WORKFLOW ---
        workflow = DbtWorkflow(
            dbt_project_dir=request.dbt_project_dir,
            db_profile=request.db_profile,
            llm_profile=request.llm_profile,
            config_path="config.yaml", # Assume config is in the root
            output_profile=request.output_profile,
            update_yaml=request.update_yaml,
            check=request.check,
            interactive=False, # <-- Interactive mode is CLI-only
            drift=request.drift
        )
        
        workflow.run()
        
        return {"status": "success", "message": f"dbt workflow completed for {request.dbt_project_dir}."}
    
    except CIError as e:
        logger.warning(f"CI check failed during API call: {e}")
        raise HTTPException(status_code=409, detail=str(e))
    except DataScribeError as e:
        logger.error(f"Data Scribe error running workflow: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error running workflow: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")

@app.get("/")
def read_root():
    """
    A simple root endpoint to confirm the server is running.
    """
    return {"message": "Data Scribe Server is running. Go to /docs for API."}