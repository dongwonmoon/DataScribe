"""
This refactored `LineageWorkflow` module is responsible for orchestrating
the global lineage generation process using injected components.

Design Rationale:
The `LineageWorkflow` is designed to provide a comprehensive view of data
lineage by combining information from both database foreign keys and dbt
model dependencies. Adhering to dependency injection principles, it receives
pre-initialized `db_connector` and `writer` instances, making it highly
modular, testable, and flexible. This separation of concerns allows the
workflow to focus purely on the logic of fetching, merging, and outputting
lineage data, while component creation and configuration are handled externally.
"""

import typer
from typing import List, Dict, Any

from schema_scribe.core.interfaces import BaseConnector, BaseWriter
from schema_scribe.services.dbt_parser import DbtManifestParser
from schema_scribe.services.lineage_generator import GlobalLineageGenerator
from schema_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class LineageWorkflow:
    """
    Orchestrates the global lineage generation workflow.

    This class relies on injected component instances rather than loading
    configuration files directly. It coordinates the fetching of physical
    (database foreign keys) and logical (dbt model dependencies) lineage,
    merges them into a unified graph, and then uses an injected writer
    to output the result.
    """

    def __init__(
        self,
        db_connector: BaseConnector,
        writer: BaseWriter,
        dbt_project_dir: str,
        db_profile_name: str,
        output_profile_name: str,
        writer_params: Dict[str, Any],
    ):
        """
        Initializes the LineageWorkflow with all necessary dependencies.

        Args:
            db_connector: An initialized and connected database connector instance.
            writer: An initialized writer instance for outputting the lineage graph.
            dbt_project_dir: The absolute path to the dbt project directory.
            db_profile_name: The name of the database profile used, for logging.
            output_profile_name: The name of the output profile used, for logging.
            writer_params: Additional parameters to pass to the writer's `write` method.
        """
        self.db_connector = db_connector
        self.writer = writer
        self.dbt_project_dir = dbt_project_dir
        self.db_profile_name = db_profile_name
        self.output_profile_name = output_profile_name
        self.writer_params = writer_params

    def run(self):
        """
        Executes the global lineage generation and writing process using injected components.

        The workflow proceeds through the following stages:
        1.  **Fetch Physical Lineage**: Retrieves foreign key relationships from the database
            using the injected `db_connector`.
        2.  **Fetch Logical Lineage**: Parses dbt models and their dependencies from the
            dbt project manifest.
        3.  **Generate Unified Graph**: Combines physical and logical lineage into a single
            Mermaid graph using `GlobalLineageGenerator`.
        4.  **Write Output**: Uses the injected `writer` to save the generated Mermaid graph
            to the specified output.
        5.  **Resource Cleanup**: Closes the database connection.

        Raises:
            typer.Exit: If any error occurs during the lineage generation or writing process.
        """
        try:
            # 1. Fetch physical lineage (using injected connector)
            logger.info(
                f"Connecting to DB '{self.db_profile_name}' for FK scan..."
            )
            db_fks = self.db_connector.get_foreign_keys()
            logger.info(f"Found {len(db_fks)} foreign key relationships.")

            # 2. Fetch logical lineage (internal service call)
            dbt_models = self._parse_dbt_models()

            # 3. Generate unified graph (service call)
            generator = GlobalLineageGenerator(db_fks, dbt_models)
            mermaid_graph = generator.generate_graph()
            catalog_data = {"mermaid_graph": mermaid_graph}

            # 4. Write output (using injected writer)
            logger.info(
                f"Writing lineage graph using output profile: '{self.output_profile_name}'."
            )
            self.writer.write(catalog_data, **self.writer_params)

            logger.info("Global lineage graph written successfully.")

        except Exception as e:
            logger.error(
                f"Failed to write lineage graph using profile '{self.output_profile_name}': {e}"
            )
            raise typer.Exit(code=1)
        finally:
            # 5. Resource cleanup
            if self.db_connector:
                logger.info(
                    f"Closing DB connection for {self.db_profile_name}..."
                )
                self.db_connector.close()

    def _parse_dbt_models(self) -> List[Dict[str, Any]]:
        """
        Parses the dbt manifest to extract model information and dependencies.

        This is an internal helper method that encapsulates the logic for
        interacting with the `DbtManifestParser` service.

        Returns:
            A list of dictionaries, where each dictionary represents a dbt model
            with its name and dependencies.
        """
        logger.info(
            f"Parsing dbt project at '{self.dbt_project_dir}' for dependencies..."
        )
        parser = DbtManifestParser(self.dbt_project_dir)
        models = parser.models
        logger.info(f"Parsed {len(models)} dbt models.")
        return models
