"""
This module contains the logic for generating a data catalog from a dbt project.

It defines the DbtCatalogGenerator class, which parses a dbt manifest file,
extracts model and column information, and uses an LLM to generate descriptions.
"""

from typing import Dict, Any
from ruamel.yaml import YAML

from schema_scribe.core.interfaces import BaseLLMClient, BaseConnector
from schema_scribe.core.dbt_parser import DbtManifestParser
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

    This class reads the `manifest.json` file from a dbt project to understand
    its models, columns, and dependencies. It then uses a `BaseLLMClient` to
    generate model descriptions, structured column metadata (including tags,
    tests, and PII status), and Mermaid.js lineage graphs.

    It can also connect to a live database to perform "drift detection,"
    comparing existing documentation against live data profiles.
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
            db_connector: (Optional) An initialized database connector, required
                          only for running drift detection checks.
        """
        self.llm_client = llm_client
        self.db_connector = db_connector
        self.yaml_parser = YAML()
        logger.info("DbtCatalogGenerator initialized.")

    def _format_profile_stats(self, profile_stats: Dict[str, Any]) -> str:
        """(Helper) Formats profile stats for the prompt."""
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
        Orchestrates the generation of a dbt data catalog.

        This method parses the dbt manifest, iterates through all discovered
        models, and generates AI-based documentation for each model and its
        columns. If `run_drift_check` is True, it will also compare existing
        documentation against the live database.

        Args:
            dbt_project_dir: The absolute path to the root of the dbt project.
            run_drift_check: If True, perform drift detection for columns that
                             already have descriptions. Requires `db_connector`
                             to be initialized.

        Returns:
            A dictionary representing the data catalog, keyed by model name.
            The structure is as follows:
            ```
            {
                "model_name": {
                    "model_description": "...",
                    "model_lineage_chart": "...",
                    "columns": [
                        {
                            "name": "column_name",
                            "type": "column_type",
                            "ai_generated": { ... },
                            "drift_status": "DRIFT" | "MATCH" | "N/A"
                        },
                        ...
                    ],
                    "original_file_path": "..."
                },
                ...
            }
            ```
        """
        logger.info(f"Dbt catalog generation started for {dbt_project_dir}")
        # Initialize the manifest parser and extract dbt models.
        parser = DbtManifestParser(dbt_project_dir)
        models = parser.models

        catalog_data = {}

        # Iterate over each parsed dbt model to generate documentation.
        for model in models:
            model_name = model["name"]
            raw_sql = model["raw_sql"]
            table_name = model_name
            logger.info(f"Processing dbt model: '{model_name}'")

            # 1. Generate a high-level description for the dbt model.
            model_prompt = DBT_MODEL_PROMPT.format(
                model_name=model_name, raw_sql=raw_sql
            )
            model_description = self.llm_client.get_description(
                model_prompt, max_tokens=200
            )

            # 2. Generate a Mermaid.js lineage chart for the model's direct parents.
            logger.info(f"  - Generating Mermaid lineage for: '{model_name}'")
            lineage_prompt = DBT_MODEL_LINEAGE_PROMPT.format(
                model_name=model_name, raw_sql=raw_sql
            )
            mermaid_chart_block = self.llm_client.get_description(
                lineage_prompt, max_tokens=1000
            )

            enriched_columns = []
            # 3. For each column, generate a structured YAML block of metadata.
            for column in model["columns"]:
                col_name = column["name"]
                col_type = column["type"]
                existing_desc = column["description"]

                ai_data_dict = {}
                drift_status = "N/A"

                if run_drift_check and self.db_connector and existing_desc:
                    logger.info(
                        f"  - Running drift check for: {model_name}.{col_name}"
                    )

                    # 1. Get live profile stats
                    profile_stats = self.db_connector.get_column_profile(
                        table_name, col_name
                    )
                    profile_context = self._format_profile_stats(profile_stats)

                    # 2. Ask AI to check for drift
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
                        drift_status = "DRIFT"
                        logger.warning(
                            f"  - DRIFT DETECTED for {model_name}.{col_name}!"
                        )
                    else:
                        drift_status = "MATCH"

                if not existing_desc:
                    col_prompt = DBT_COLUMN_PROMPT.format(
                        model_name=model_name,
                        col_name=col_name,
                        col_type=col_type,
                        raw_sql=raw_sql,
                    )
                    # The prompt asks the LLM to return a YAML snippet.
                    yaml_snippet_str = self.llm_client.get_description(
                        col_prompt, max_tokens=250
                    )

                    # Try to parse the LLM's response as YAML to get structured data.
                    # If parsing fails, the raw response is used as the description,
                    # ensuring robustness against malformed AI outputs.
                    try:
                        ai_data_dict = self.yaml_parser.load(yaml_snippet_str)
                        if not isinstance(ai_data_dict, dict):
                            raise ValueError(
                                "AI did not return a valid YAML mapping."
                            )
                    except Exception as e:
                        logger.error(
                            f"AI YAML snippet parsing failed for {model_name}.{col_name}: {e}"
                        )
                        logger.debug(f"Failed snippet:\n{yaml_snippet_str}")
                        ai_data_dict = {"description": yaml_snippet_str.strip()}

                enriched_columns.append(
                    {
                        "name": col_name,
                        "type": col_type,
                        "ai_generated": ai_data_dict,
                        "drift_status": drift_status,
                    }
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
