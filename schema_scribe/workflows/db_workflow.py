"""
This module defines the `DbWorkflow`, the orchestrator for the `db` command.

Design Rationale:
This class is designed to be a "pure" workflow coordinator. It does not
load configuration or create its own dependencies. Instead, it receives
fully initialized components (like a database connector and an LLM client)
via dependency injection. This separation of concerns makes the workflow
easier to test, as its dependencies can be mocked, and decouples it from the
details of configuration management, which is handled by the `ConfigManager`.
"""

from typing import Optional, Dict, Any
import typer
from schema_scribe.core.interfaces import (
    BaseConnector,
    BaseLLMClient,
    BaseWriter,
)
from schema_scribe.services.catalog_generator import CatalogGenerator
from schema_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class DbWorkflow:
    """
    Orchestrates the database scanning workflow.

    This class depends on injected component instances rather than configuration
    files, allowing it to focus solely on the business logic of generating a
    data catalog.
    """

    def __init__(
        self,
        db_connector: BaseConnector,
        llm_client: Optional[BaseLLMClient] = None,
        writer: Optional[BaseWriter] = None,
        db_profile_name: str = "unknown_db",
        output_profile_name: Optional[str] = None,
        writer_params: Optional[dict] = None,
        provider_name: Optional[str] = None,
    ):
        """
        Initializes the workflow with all required dependencies.

        Args:
            db_connector: An initialized connector instance for database interaction.
            llm_client: An initialized client instance for LLM communication.
            writer: An optional writer instance for saving the output.
            db_profile_name: The name of the DB profile for logging and context.
            output_profile_name: The name of the output profile for logging.
            writer_params: Additional parameters to pass to the writer's `write()` method.
            provider_name: The name of the LLM provider, disclosed in the dry-run
                manifest. Not read from the client because a dry run never
                constructs one.
        """
        self.db_connector = db_connector
        self.llm_client = llm_client
        self.writer = writer
        self.db_profile_name = db_profile_name
        self.output_profile_name = output_profile_name
        self.writer_params = writer_params or {}
        self.provider_name = provider_name

    @staticmethod
    def _format_profile_value(value: Any) -> str:
        """
        Formats a profile stat value the way the prompt formatter would
        show it: raw value, "N/A" when None, lowercase for booleans.
        """
        if value is None:
            return "N/A"
        if isinstance(value, bool):
            return str(value).lower()
        return str(value)

    def dry_run(self, full: bool = False) -> None:
        """
        Prints a disclosure manifest of everything a real run would send to
        the LLM, without constructing the LLM client or writing any output.

        By default (compact) no profiling is performed: the manifest lists
        the profile, provider, per-table lines (table name, column count,
        column name: type), verbatim view SQL, foreign key count, and a note
        that per-column aggregate stats are computed at run time. With
        ``full=True`` the per-column profile stat values are computed and
        printed as one line per column (grouped by table).
        """
        try:
            tables = self.db_connector.get_tables()
            views = self.db_connector.get_views()
            foreign_keys = self.db_connector.get_foreign_keys()
            columns_by_table = {
                table: self.db_connector.get_columns(table) for table in tables
            }

            print("=" * 72)
            print("DRY RUN — Disclosure manifest (no LLM call will be made)")
            print("=" * 72)
            print(f"DB profile   : {self.db_profile_name}")
            print(f"LLM provider : {self.provider_name or 'unknown'}")
            print(f"Tables ({len(tables)}):")
            for table in tables:
                columns = columns_by_table[table]
                print(f"  {table} ({len(columns)} columns)")
                for column in columns:
                    print(f"    {column['name']}: {column['type']}")
            print(f"Views ({len(views)}):")
            for view in views:
                print(f"  {view['name']}")
                print(f"    {view['definition']}")
            print(f"Foreign keys: {len(foreign_keys)}")
            if full:
                print(
                    "Per-column aggregate stats "
                    "(null_ratio, distinct_count, is_unique):"
                )
                for table in tables:
                    for column in columns_by_table[table]:
                        profile = self.db_connector.get_column_profile(
                            table, column["name"]
                        )
                        print(
                            f"  {table}.{column['name']}: "
                            f"null_ratio={self._format_profile_value(profile.get('null_ratio'))}  "
                            f"distinct={self._format_profile_value(profile.get('distinct_count'))}  "
                            f"unique={self._format_profile_value(profile.get('is_unique'))}"
                        )
            else:
                print(
                    "Per-column aggregate stats (null_ratio, distinct_count, "
                    "is_unique) computed at run time"
                )
        finally:
            logger.info(f"Closing DB connection for {self.db_profile_name}...")
            self.db_connector.close()
        
    def generate_catalog(self) -> Dict[str, Any]:
        """
        Runs the core business logic to generate the catalog dictionary.
        This can be called by the API server without triggering a file write.
        """
        try:
            # 1. Call the pure business logic (service)
            logger.info(f"Generating data catalog for: {self.db_profile_name}")
            catalog_gen = CatalogGenerator(
                self.db_connector,
                self.llm_client,
                provider_name=self.provider_name,
            )
            catalog = catalog_gen.generate_catalog(self.db_profile_name)
            return catalog
        finally:
            # Ensure the connection is closed even if only generating data
            logger.info(f"Closing DB connection for {self.db_profile_name}...")
            self.db_connector.close()

    def run(self):
        """
        Executes the database catalog generation workflow using the injected components.

        The process is as follows:
        1.  Invoke the `CatalogGenerator` service to perform the core business logic
            of fetching metadata, profiling data, and generating AI descriptions.
        2.  If a writer component was provided, pass the generated catalog to it
            to be written to the target output (e.g., a file or API).
        3.  Ensure that the database connection is closed in a `finally` block
            to release resources, regardless of success or failure.
        """
        catalog = None
        try:
            # 1. Generate the catalog data
            # (Now closes connection internally)
            catalog = self.generate_catalog()

            # 2. Execute writer (if injected)
            if not self.writer:
                logger.info(
                    "Catalog generated. No --output profile specified, not writing."
                )
                return

            logger.info(
                f"Writing catalog using output profile: '{self.output_profile_name}'"
            )
            
            writer_kwargs = {
                "db_profile_name": self.db_profile_name,
                "db_connector": self.db_connector,
                **self.writer_params,
            }
            
            self.writer.write(catalog, **writer_kwargs)
            logger.info("Catalog written successfully.")

        except (KeyError, ValueError, IOError) as e:
            logger.error(
                f"Failed to write catalog using profile '{self.output_profile_name}': {e}"
            )
            raise typer.Exit(code=1)
