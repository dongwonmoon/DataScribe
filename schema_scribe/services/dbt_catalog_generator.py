"""
This module provides the `DbtCatalogGenerator`, a specialized engine for
creating a data catalog directly from a dbt project's compiled artifacts.

Unlike the database-centric `CatalogGenerator`, this class operates on the
`manifest.json` file, allowing it to understand a project's models, columns,
tests, and dependencies without needing full database access, except for
optional "drift detection" features.
"""

from typing import Dict, Any
from ruamel.yaml import YAML

from schema_scribe.core.interfaces import BaseLLMClient, BaseConnector
from schema_scribe.services.dbt_parser import DbtManifestParser
from schema_scribe.prompts import (
    DBT_MODEL_PROMPT,
    DBT_COLUMN_PROMPT,
    DBT_MODEL_LINEAGE_PROMPT,
    DBT_DRIFT_CHECK_PROMPT,
)
from schema_scribe.utils.logger import get_logger

# Initialize a logger for this module
logger = get_logger(__name__)


class DbtCatalogGenerator:
    """
    Generates an AI-powered data catalog by parsing a dbt project's manifest.

    This class is the core of the `dbt` workflow. It reads the `manifest.json`
    file to extract a project's models, columns, and dependencies. It then uses
    a `BaseLLMClient` to generate multiple AI artifacts:
    - High-level model descriptions.
    - Structured column metadata (tags, tests, PII status) in YAML format.
    - Mermaid.js lineage graphs for model dependencies.

    A key feature is its ability to perform "drift detection" by connecting to a
    live database (`db_connector`) to compare existing documentation against
    live data profiles, identifying outdated or inconsistent descriptions.
    """

    def __init__(
        self,
        llm_client: BaseLLMClient,
        db_connector: BaseConnector | None = None,
    ):
        """
        Initializes the DbtCatalogGenerator.

        Args:
            llm_client: An initialized client for the desired LLM provider.
            db_connector: (Optional) An initialized database connector. This is
                          only required for running the `--drift` detection check,
                          which needs to query the live database.
        """
        self.llm_client = llm_client
        self.db_connector = db_connector
        self.yaml_parser = YAML()
        logger.info("DbtCatalogGenerator initialized.")

    def _format_profile_stats(self, profile_stats: Dict[str, Any]) -> str:
        """(Helper) Formats profile stats into a string for an LLM prompt."""
        context_lines = [
            f"- Null Ratio: {profile_stats.get('null_ratio', 'N/A')}",
            f"- Is Unique: {profile_stats.get('is_unique', 'N/A')}",
            f"- Distinct Count: {profile_stats.get('distinct_count', 'N/A')}",
        ]
        return "\n".join(context_lines)

    def generate_catalog(
        self,
        dbt_project_dir: str,
        run_drift_check: bool = False,
    ) -> Dict[str, Any]:
        """
        Orchestrates the generation of a complete dbt data catalog.

        This method executes the main logic in a series of steps for each model:
        1.  **Parse Manifest**: Uses `DbtManifestParser` to load all models.
        2.  **Generate Model Description**: Creates a high-level summary for the
            model based on its raw SQL.
        3.  **Generate Model Lineage**: Creates a Mermaid.js graph showing the
            model's direct parents.
        4.  **Process Columns**: For each column, it follows one of two paths:
            a.  **Drift Check**: If `run_drift_check` is True and a description
                already exists, it profiles the live data and asks an AI to
                check for inconsistencies.
            b.  **New Description**: If no description exists, it asks an AI to
                generate a structured YAML block containing a description, tags,
                and tests. It includes a robust fallback to use the raw AI
                response if the YAML is malformed.

        Args:
            dbt_project_dir: The absolute path to the root of the dbt project.
            run_drift_check: If True, perform drift detection for columns that
                             already have descriptions. Requires `db_connector`.

        Returns:
            A dictionary representing the data catalog, keyed by model name.
        """
        logger.info(f"Dbt catalog generation started for {dbt_project_dir}")
        parser = DbtManifestParser(dbt_project_dir)
        models = parser.models
        catalog_data = {}

        for model in models:
            model_name = model["name"]
            logger.info(f"Processing dbt model: '{model_name}'")

            # 1. Generate a high-level description for the dbt model.
            model_description = self._generate_model_description(model)

            # 2. Generate a Mermaid.js lineage chart.
            mermaid_chart_block = self._generate_model_lineage(model)

            # 3. Process each column for descriptions or drift detection.
            enriched_columns = self._process_columns(
                model, run_drift_check=run_drift_check
            )

            # 4. Assemble all generated content for the model into the catalog.
            catalog_data[model_name] = {
                "model_description": model_description,
                "model_lineage_chart": mermaid_chart_block,
                "columns": enriched_columns,
                "original_file_path": model["original_file_path"],
            }

        logger.info("Dbt catalog generation finished.")
        return catalog_data

    def _generate_model_description(self, model: Dict[str, Any]) -> str:
        """Generates a high-level description for a dbt model."""
        model_prompt = DBT_MODEL_PROMPT.format(
            model_name=model["name"], raw_sql=model["raw_sql"]
        )
        return self.llm_client.get_description(model_prompt, max_tokens=200)

    def _generate_model_lineage(self, model: Dict[str, Any]) -> str:
        """Generates a Mermaid.js lineage chart for a model's parents."""
        logger.info(f"  - Generating Mermaid lineage for: '{model['name']}'")
        lineage_prompt = DBT_MODEL_LINEAGE_PROMPT.format(
            model_name=model["name"], raw_sql=model["raw_sql"]
        )
        return self.llm_client.get_description(lineage_prompt, max_tokens=1000)

    def _process_columns(
        self, model: Dict[str, Any], run_drift_check: bool
    ) -> list:
        """
        Processes all columns for a given model, either generating new
        descriptions or performing drift detection.
        """
        enriched_columns = []
        for column in model["columns"]:
            col_name = column["name"]
            existing_desc = column["description"]
            ai_data_dict = {}
            drift_status = "N/A"

            if run_drift_check and self.db_connector and existing_desc:
                drift_status = self._run_drift_check(
                    model["name"], col_name, existing_desc
                )
            elif not existing_desc:
                ai_data_dict = self._generate_column_yaml(model, column)

            enriched_columns.append(
                {
                    "name": col_name,
                    "type": column["type"],
                    "ai_generated": ai_data_dict,
                    "drift_status": drift_status,
                }
            )
        return enriched_columns

    def _run_drift_check(
        self, model_name: str, col_name: str, existing_desc: str
    ) -> str:
        """
        Checks a single column for documentation drift against the live database.
        """
        logger.info(f"  - Running drift check for: {model_name}.{col_name}")
        # Get live profile stats from the database.
        profile_stats = self.db_connector.get_column_profile(
            model_name, col_name
        )
        profile_context = self._format_profile_stats(profile_stats)

        # Ask the AI to judge if the description still matches the data profile.
        drift_prompt = DBT_DRIFT_CHECK_PROMPT.format(
            node_name=model_name,
            column_name=col_name,
            existing_description=existing_desc,
            profile_context=profile_context,
        )
        ai_judgement = self.llm_client.get_description(
            drift_prompt, max_tokens=10
        ).upper()

        if "DRIFT" in ai_judgement:
            logger.warning(f"  - DRIFT DETECTED for {model_name}.{col_name}!")
            return "DRIFT"
        return "MATCH"

    def _generate_column_yaml(
        self, model: Dict[str, Any], column: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generates a structured YAML dictionary for a single column using an AI.
        """
        col_prompt = DBT_COLUMN_PROMPT.format(
            model_name=model["name"],
            col_name=column["name"],
            col_type=column["type"],
            raw_sql=model["raw_sql"],
        )
        # The prompt asks the LLM to return a YAML snippet.
        yaml_snippet_str = self.llm_client.get_description(
            col_prompt, max_tokens=250
        )

        # Try to parse the LLM's response as YAML. If parsing fails, the raw
        # response is used as the description for robustness.
        try:
            ai_data_dict = self.yaml_parser.load(yaml_snippet_str)
            if not isinstance(ai_data_dict, dict):
                raise ValueError("AI did not return a valid YAML mapping.")
            return ai_data_dict
        except Exception as e:
            logger.error(
                f"AI YAML snippet parsing failed for {model['name']}.{column['name']}: {e}"
            )
            logger.debug(f"Failed snippet:\n{yaml_snippet_str}")
            return {"description": yaml_snippet_str.strip()}
