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
from fastapi import FastAPI, HTTPException, Query, Request
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
from schema_scribe.server.jobs import JobManager, QueueFullError
from schema_scribe.utils.counters import CountingCursor, CountingLLM
from schema_scribe.utils.logger import get_logger
from contextlib import asynccontextmanager
import time

logger = get_logger(__name__)


app = FastAPI(
    title="Schema Scribe Server",
    description="API for running Schema Scribe documentation workflows.",
    version="1.0.0",
)

CATALOG_CACHE_FILE = "server_catalog.json"
CONFIG_PATH = "config.yaml"  # set by `schema-scribe serve --config`

@asynccontextmanager
async def lifespan(app):
    # Fresh manager per app run: asyncio.Queue binds to the running event
    # loop, so a module-level instance breaks across TestClient lifespans.
    app.state.job_manager = JobManager(max_concurrent=2, max_queue=10)
    app.state.job_manager.start()
    try:
        yield
    finally:
        await app.state.job_manager.stop()


app = FastAPI(
    title="Schema Scribe Server",
    description="API for running Schema Scribe documentation workflows.",
    version="1.0.0",
    lifespan=lifespan,
)

# --- Pydantic Models for API Request/Response Validation ---


class ProfileInfo(BaseModel):
    """Defines the response structure for the profile discovery endpoint."""

    db_connections: List[str]
    llm_providers: List[str]
    output_profiles: List[str]


class JobRequest(BaseModel):
    """Body for POST /api/jobs: the db profile to scan."""

    db_profile: str


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
        cfg_manager = ConfigManager(CONFIG_PATH)
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


def _error(code: str, message: str, status: int):
    """Consistent error shape (Phase 3a)."""
    return HTTPException(status_code=status, detail={"error": {"code": code, "message": message}})


def _run_db_scan_job(config_path: str, db_profile: str) -> dict:
    """Runs a db scan synchronously (called from the job manager's thread
    via asyncio.to_thread) and returns the catalog + engine metrics."""
    cfg_manager = ConfigManager(config_path)
    connector, db_name = cfg_manager.get_db_connector(db_profile)
    llm_client, _ = cfg_manager.get_llm_client(None)

    counting_cursor = CountingCursor(connector.cursor)
    connector.cursor = counting_cursor
    counting_llm = CountingLLM(llm_client)

    workflow = DbWorkflow(
        db_connector=connector,
        llm_client=counting_llm,
        writer=None,
        db_profile_name=db_name,
    )
    t0 = time.perf_counter()
    catalog = workflow.generate_catalog()
    engine_ms = round((time.perf_counter() - t0) * 1000, 1)

    metrics = {
        "db_queries": counting_cursor.count,
        "llm_calls": counting_llm.count,
        "engine_ms": engine_ms,
    }
    # Update the catalog cache (read surface for /api/catalog and the UI).
    try:
        with open(CATALOG_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(catalog, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Failed to write catalog cache: {e}")
    return {"catalog": catalog, "metrics": metrics}


def _manager(request):
    return request.app.state.job_manager


@app.post("/api/jobs", status_code=202)
def submit_job(request: JobRequest, fastapi_request: Request):
    """Schedules a db documentation scan; returns a job id immediately.

    The engine runs off the event loop (asyncio.to_thread), so polling
    stays responsive during long runs. Queue overflow -> 429.
    """
    # Validate the profile exists BEFORE enqueuing (400, cheap).
    try:
        cfg = ConfigManager(CONFIG_PATH)
        cfg.get_db_connector(request.db_profile)
    except Exception:
        raise _error("unknown_profile", f"db profile '{request.db_profile}' not found", 400)

    try:
        job_id = _manager(fastapi_request).submit(
            lambda: _run_db_scan_job(CONFIG_PATH, request.db_profile)
        )
    except QueueFullError as e:
        raise _error("queue_full", str(e), 429)
    return {"job_id": job_id}


@app.get("/api/jobs")
def list_jobs(fastapi_request: Request):
    """Job board: newest first."""
    manager = _manager(fastapi_request)
    jobs = []
    for jid in manager.job_ids():
        jobs.append(_job_state(manager.get(jid)))
    return {"jobs": jobs}


def _job_state(job) -> dict:
    return {
        "job_id": job.job_id,
        "status": job.status,
        "error": job.error,
        "submitted_at": job.submitted_at,
        "started_at": job.started_at,
        "finished_at": job.finished_at,
        "metrics": job.result.get("metrics") if job.result else None,
        "has_catalog": bool(job.result and job.result.get("catalog")),
    }


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str, fastapi_request: Request):
    job = _manager(fastapi_request).get(job_id)
    if job is None:
        raise _error("unknown_job", f"job '{job_id}' not found", 404)
    state = _job_state(job)
    if state["has_catalog"]:
        state["catalog"] = job.result["catalog"]
    return state


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

        cfg_manager = ConfigManager(CONFIG_PATH)

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
        cfg_manager = ConfigManager(CONFIG_PATH)

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
