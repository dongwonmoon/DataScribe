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
import difflib
import os
import tempfile
import typer
from schema_scribe.core.interfaces import (
    BaseConnector,
    BaseLLMClient,
    BaseWriter,
)
from schema_scribe.core.exceptions import ConfigError
from schema_scribe.services.catalog_generator import CatalogGenerator
from schema_scribe.services.landscape import build_landscape
from schema_scribe.services.schema_state import SchemaState
from schema_scribe.components.writers.markdown_writer import MarkdownWriter
from schema_scribe.prompts import (
    LANDSCAPE_CLUSTER_HINT_PROMPT,
    LANDSCAPE_TABLE_HINT_PROMPT,
)
from schema_scribe.utils.logger import get_logger

logger = get_logger(__name__)

# Hints cap (documented decision, Task 8.2): one prompt per cluster for the
# top clusters by member count, and one prompt per core table (build_landscape
# already caps core tables at 10) — at most 20 LLM calls on legacy-rich.
_HINT_CLUSTER_CAP = 10
_HINT_MAX_TOKENS = 60


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

    def render(self) -> str:
        """
        Renders the catalog to the configured writer's output string
        without writing anything to disk or external services.

        File-based writers (Markdown/JSON) expose an in-memory `render()`
        method (Task 6.1 extraction), which is used directly. Writers that
        only expose `write()` fall back to writing into a temporary file
        (with `output_filename` redirected) and reading it back.
        """
        return self._render_catalog(self.generate_catalog())

    def _render_catalog(self, catalog: Dict[str, Any]) -> str:
        """
        Renders an already-generated catalog to the configured writer's
        output string without writing to the real output target.
        """
        if not self.writer:
            raise ValueError(
                "Cannot render the catalog without a writer "
                "(--output profile)."
            )
        render_fn = getattr(self.writer, "render", None)
        if callable(render_fn):
            return render_fn(catalog, db_profile_name=self.db_profile_name)
        writer_kwargs = {
            "db_profile_name": self.db_profile_name,
            "db_connector": self.db_connector,
            **self.writer_params,
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_out = os.path.join(tmp_dir, "render.tmp")
            self.writer.write(catalog, **writer_kwargs, output_filename=tmp_out)
            with open(tmp_out, "r", encoding="utf-8") as f:
                return f.read()

    def check(self) -> bool:
        """
        Compares a fresh generation against the persisted schema state
        sidecar and the existing output file; returns True iff anything
        would change. Never writes to the output target.

        Semantics (defined by Slice 6.1):
        - No sidecar and no existing output file → False ("no existing
          documentation to compare").
        - Sidecar present → SchemaState.classify drives the result; one
          status line per table (unchanged/added/removed/
          structurally-changed) is printed.
        - Output file present → a unified diff of the new render vs the
          existing content is also printed. An existing output file
          without a sidecar fails closed: every table classifies as added.

        The DB connection is closed by generate_catalog().
        """
        catalog = self.generate_catalog()
        curr = SchemaState.snapshot(catalog)
        output_filename = self.writer_params.get("output_filename")
        sidecar_path = (
            f"{output_filename}.schema-state.json" if output_filename else None
        )
        prev = SchemaState.load(sidecar_path) if sidecar_path else None
        output_exists = bool(output_filename) and os.path.exists(
            output_filename
        )

        if prev is None and not output_exists:
            print("no existing documentation to compare")
            return False

        result = SchemaState.classify(prev, curr)
        unchanged = sorted(
            set(curr["tables"])
            - set(result["added"])
            - set(result["structurally_changed"])
        )

        print("Schema state check:")
        for name in unchanged:
            print(f"  [unchanged]            {name}")
        for name in result["added"]:
            print(f"  [added]                {name}")
        for name in result["removed"]:
            print(f"  [removed]              {name}")
        for name in result["structurally_changed"]:
            print(f"  [structurally-changed] {name}")

        changed = bool(
            result["added"] or result["removed"] or result["structurally_changed"]
        )

        if output_exists:
            try:
                new_render = self._render_catalog(catalog)
            except ValueError as e:
                print(f"  (cannot render diff: {e})")
            else:
                with open(output_filename, "r", encoding="utf-8") as f:
                    existing = f.read()
                diff = difflib.unified_diff(
                    existing.splitlines(),
                    new_render.splitlines(),
                    fromfile=str(output_filename),
                    tofile="<new render>",
                    lineterm="",
                )
                print("\n".join(diff))

        return changed

    def landscape(self, hints: bool = False) -> None:
        """
        Renders the deterministic landscape report (Slice 8.2).

        Collects metadata through the connector, builds the landscape with
        the pure ``build_landscape`` service, optionally enriches it with
        LLM name-decoding hints (disclosure logged before the first
        transmission, Slice 3 rule), and then either prints the rendered
        Markdown to stdout (no writer injected, like dry-run) or writes it
        through the injected writer's landscape path.

        The deterministic path never needs an LLM client: the caller builds
        the workflow with ``llm_client=None`` unless ``hints=True``.

        Raises:
            ValueError: If ``hints=True`` without an LLM client, or the
                injected writer has no landscape path (the landscape is a
                Markdown artifact; only MarkdownWriter supports it).
            ConfigError: If the injected writer's output profile lacks
                ``output_filename`` (a file writer cannot produce the
                report without a target path).
        """
        try:
            # Validate the writer's landscape path BEFORE any hint is
            # transmitted: a writer that cannot render the landscape (or a
            # profile missing its output filename) would otherwise receive
            # table names over the wire, then fail after up to 20 LLM calls
            # (Slice 8 final review finding 1). Fail fast instead.
            write_landscape = None
            if self.writer is not None:
                write_landscape = getattr(self.writer, "write_landscape", None)
                if not callable(write_landscape):
                    raise ValueError(
                        "Landscape output requires a MarkdownWriter output "
                        f"profile (got {type(self.writer).__name__})."
                    )
                if not self.writer_params.get("output_filename"):
                    raise ConfigError(
                        "Landscape output requires 'output_filename' in the "
                        "output profile parameters."
                    )

            tables = self.db_connector.get_tables()
            columns_by_table = {
                table: self.db_connector.get_columns(table) for table in tables
            }
            foreign_keys = self.db_connector.get_foreign_keys()
            landscape = build_landscape(tables, columns_by_table, foreign_keys)
            hint_map = self._landscape_hints(landscape) if hints else None

            if self.writer is not None:
                write_landscape(
                    landscape,
                    hints=hint_map,
                    output_filename=self.writer_params.get("output_filename"),
                    db_profile_name=self.db_profile_name,
                )
            else:
                rendered = MarkdownWriter().render_landscape(
                    landscape,
                    hints=hint_map,
                    db_profile_name=self.db_profile_name,
                )
                print(rendered)
        finally:
            logger.info(f"Closing DB connection for {self.db_profile_name}...")
            self.db_connector.close()

    def _landscape_hints(self, landscape: Dict[str, Any]) -> Dict[str, Any]:
        """
        Adds LLM name-decoding hints to the landscape: one prompt per
        cluster (top ``_HINT_CLUSTER_CAP`` by member count, then name) and
        one prompt per core table. Discloses payload counts and provider
        before the first transmission, mirroring the Slice 3 disclosure.
        """
        if self.llm_client is None:
            raise ValueError("--landscape-hints requires an LLM client.")

        provider = self.provider_name or "unknown"
        clusters = sorted(
            landscape["clusters"].items(),
            key=lambda item: (-len(item[1]), item[0]),
        )[:_HINT_CLUSTER_CAP]
        core = landscape["core_tables"]

        logger.info(
            f"Sending {len(clusters)} cluster hints and {len(core)} "
            f"core-table hints to provider '{provider}'. Metadata: "
            f"clusters={len(landscape['clusters'])}, "
            f"core_tables={len(core)}."
        )

        hint_map: Dict[str, Any] = {"clusters": {}, "core_tables": {}}
        for name, members in clusters:
            prompt = LANDSCAPE_CLUSTER_HINT_PROMPT.format(
                cluster_name=name,
                member_count=len(members),
                member_tables=", ".join(members[:_HINT_CLUSTER_CAP]),
            )
            hint_map["clusters"][name] = self.llm_client.get_description(
                prompt, max_tokens=_HINT_MAX_TOKENS
            )
        for entry in core:
            prompt = LANDSCAPE_TABLE_HINT_PROMPT.format(
                table_name=entry["table"]
            )
            hint_map["core_tables"][entry["table"]] = (
                self.llm_client.get_description(prompt, max_tokens=_HINT_MAX_TOKENS)
            )
        return hint_map

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

            self._save_sidecar(catalog)

        except (KeyError, ValueError, IOError) as e:
            logger.error(
                f"Failed to write catalog using profile '{self.output_profile_name}': {e}"
            )
            raise typer.Exit(code=1)

    def _save_sidecar(self, catalog: Dict[str, Any]) -> None:
        """
        Persists the schema state snapshot next to the output file after a
        successful write. Bookkeeping: a failure logs a warning and never
        fails the run.
        """
        output_filename = self.writer_params.get("output_filename")
        if output_filename:
            try:
                sidecar_path = f"{output_filename}.schema-state.json"
                SchemaState.save(
                    SchemaState.snapshot(catalog), sidecar_path
                )
                logger.info(
                    f"Schema state sidecar saved to '{sidecar_path}'."
                )
            except Exception as e:
                logger.warning(
                    f"Failed to save schema state sidecar: {e}"
                )

    def _prompt_review(
        self, key: str, node_label: str, ai_value: str
    ) -> str:
        """
        Prompts the user to accept, edit, or reject an AI-generated value
        (a table summary or a column description).

        Commands: 'accept' keeps the draft, 'edit:<text>' replaces it,
        'reject' empties it. Any other input re-prompts.
        """
        while True:
            typer.echo(
                typer.style(
                    f"Suggestion for '{key}' on {node_label}:",
                    fg=typer.colors.CYAN,
                )
            )
            typer.echo(
                typer.style(f'  AI: "{ai_value}"', fg=typer.colors.GREEN)
            )
            answer = typer.prompt(
                "  [accept] to keep, [edit:NEW TEXT] to replace, "
                "[reject] to drop"
            )
            command = answer.strip().lower()
            if command == "accept":
                return ai_value
            if command == "reject":
                return ""
            if command.startswith("edit:"):
                return answer.strip()[5:].lstrip()
            logger.warning(
                f"Unrecognized review input '{answer}' — expected "
                "accept, edit:<text>, or reject."
            )

    def run_interactive(self):
        """
        Generates the catalog, then prompts the user to accept, edit, or
        reject each table summary and each column description before
        writing the reviewed catalog via the configured writer.

        Reject semantics (plan-mandated): a rejected value becomes an
        empty string — the 'description' key is never removed, because
        MarkdownWriter hard-indexes `column['description']` and removal
        would raise KeyError mid-write. View summaries are not reviewed:
        the review loop covers table summaries and column descriptions
        only. The schema state sidecar is saved like a regular run so the
        next --check compares against the reviewed baseline.
        """
        catalog = None
        try:
            # 1. Generate the catalog data (closes connection internally)
            catalog = self.generate_catalog()

            # 2. Review each table summary, then its column descriptions
            for table in catalog["tables"]:
                table["ai_summary"] = self._prompt_review(
                    "ai_summary",
                    f"table '{table['name']}'",
                    table["ai_summary"],
                )
                for column in table["columns"]:
                    column["description"] = self._prompt_review(
                        "description",
                        f"column '{table['name']}.{column['name']}'",
                        column["description"],
                    )

            # 3. Execute writer (if injected)
            if not self.writer:
                logger.info(
                    "Catalog generated. No --output profile specified, "
                    "not writing."
                )
                return

            logger.info(
                f"Writing catalog using output profile: "
                f"'{self.output_profile_name}'"
            )

            writer_kwargs = {
                "db_profile_name": self.db_profile_name,
                "db_connector": self.db_connector,
                **self.writer_params,
            }

            self.writer.write(catalog, **writer_kwargs)
            logger.info("Catalog written successfully.")

            self._save_sidecar(catalog)

        except (KeyError, ValueError, IOError) as e:
            logger.error(
                f"Failed to write catalog using profile "
                f"'{self.output_profile_name}': {e}"
            )
            raise typer.Exit(code=1)
