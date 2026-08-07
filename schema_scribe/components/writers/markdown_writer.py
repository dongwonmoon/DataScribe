"""
This module provides `MarkdownWriter`, an implementation of `BaseWriter` for
generating a data catalog in Markdown format.

Design Rationale:
The `MarkdownWriter` is designed to produce a human-readable and easily shareable
representation of a database's schema. It translates the structured output of
the `CatalogGenerator` into a Markdown document, including:
- A high-level overview.
- An Entity Relationship Diagram (ERD) generated using Mermaid.js syntax.
- Detailed sections for database views and tables, complete with AI-generated
  summaries and column descriptions.
This format is ideal for documentation that can be version-controlled and
rendered in various platforms (e.g., GitHub, Confluence).
"""

from typing import Dict, List, Any
import os
import stat
import tempfile

from schema_scribe.utils.logger import get_logger
from schema_scribe.core.interfaces import BaseWriter
from schema_scribe.core.exceptions import WriterError, ConfigError


# Initialize a logger for this module
logger = get_logger(__name__)


class MarkdownWriter(BaseWriter):
    """
    Implements `BaseWriter` to write a database catalog to a Markdown file.

    This class transforms the abstract catalog dictionary into a rich,
    human-readable Markdown document, creating dedicated sections for ERD,
    views, and tables.
    """

    def _generate_erd_mermaid(self, foreign_keys: List[Dict[str, str]]) -> str:
        """
        Generates a Mermaid.js ERD chart from foreign key data.

        This helper function takes a list of foreign key relationships and
        constructs a string containing Mermaid graph syntax. It uses the
        `source_table` and `target_table` keys from the foreign key dictionaries.

        Args:
            foreign_keys: A list of dictionaries, each representing a
                          foreign key relationship.

        Returns:
            A string containing the Mermaid ERD code block, or a message
            if no foreign keys were provided.
        """
        if not foreign_keys:
            return "No foreign key relationships found to generate a diagram."

        code = ["```mermaid", "erDiagram"]
        for fk in foreign_keys:
            # Mermaid syntax: "users" ||--o{ "orders" : "has"
            source_table = fk["source_table"]
            target_table = fk["target_table"]
            source_column = fk["source_column"]
            target_column = fk["target_column"]
            code.append(
                f'    "{source_table}" ||--o{{ "{target_table}" : "{source_column} to {target_column}"'
            )
        code.append("```")
        return "\n".join(code)

    def _atomic_write(self, output_filename: str, content: str) -> None:
        """
        Writes `content` to `output_filename` atomically.

        The full content is written to a temp file in the target directory and
        then moved onto the target with `os.replace`, so a mid-write failure
        never leaves a truncated or partially written target file. The temp
        file is removed on any failure.

        The temp file is created mode 0600 by `tempfile.mkstemp`; the target's
        existing permission bits are preserved when it already exists,
        otherwise the umask-derived default for a new file is applied.
        """
        target_dir = os.path.dirname(os.path.abspath(output_filename))
        fd, tmp_name = tempfile.mkstemp(dir=target_dir, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(content)
            if os.path.exists(output_filename):
                os.chmod(tmp_name, stat.S_IMODE(os.stat(output_filename).st_mode))
            else:
                current_umask = os.umask(0)
                os.umask(current_umask)
                os.chmod(tmp_name, 0o666 & ~current_umask)
            os.replace(tmp_name, output_filename)
        except BaseException:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
            raise

    def render(self, catalog_data: Dict[str, Any], **kwargs) -> str:
        """
        Builds the Markdown document string for a catalog without writing.

        This is the pure, in-memory half of `write()`: the same line
        composition `write()` uses, returned as a string so callers like
        `DbWorkflow.check()` can diff a fresh render against the existing
        output file without touching disk.

        Args:
            catalog_data: A dictionary containing the structured catalog data.
            **kwargs: Must contain `db_profile_name`.

        Raises:
            ConfigError: If `db_profile_name` is not provided.
        """
        db_profile_name = kwargs.get("db_profile_name")
        if not db_profile_name:
            raise ConfigError(
                "MarkdownWriter.render requires 'db_profile_name' in kwargs."
            )

        lines = []

        # 1. Main Title
        lines.append(f"# 📁 Data Catalog for {db_profile_name}\n")

        # 2. ERD Section
        lines.append("\n## 🚀 Entity Relationship Diagram (ERD)\n\n")
        foreign_keys = catalog_data.get("foreign_keys", [])
        mermaid_code = self._generate_erd_mermaid(foreign_keys)
        lines.append(mermaid_code + "\n")

        # 3. Views Section
        lines.append("\n## 🔎 Views\n\n")
        views = catalog_data.get("views", [])
        if not views:
            lines.append("No views found in this database.\n")
        else:
            for view in views:
                lines.append(f"### 📄 View: `{view['name']}`\n\n")
                lines.append("**AI-Generated Summary:**\n")
                lines.append(
                    f"> {view.get('ai_summary', '(No summary available)')}\n\n"
                )
                lines.append("**SQL Definition:**\n")
                lines.append(
                    f"```sql\n{view.get('definition', 'N/A')}\n```\n\n"
                )

        # 4. Tables Section
        lines.append("\n## 🗂️ Tables\n\n")
        tables = catalog_data.get("tables", [])
        if not tables:
            lines.append("No tables found in this database.\n")
        else:
            for table in tables:
                lines.append(f"### 📄 Table: `{table['name']}`\n\n")
                lines.append("**AI-Generated Summary:**\n")
                lines.append(
                    f"> {table.get('ai_summary', '(No summary available)')}\n\n"
                )
                lines.append(
                    "| Column Name | Data Type | AI-Generated Description |\n"
                )
                lines.append("| :--- | :--- | :--- |\n")
                for column in table.get("columns", []):
                    name_cell = (
                        f"🔑 `{column['name']}`"
                        if column.get("is_pk", False)
                        else f"`{column['name']}`"
                    )
                    lines.append(
                        f"| {name_cell} | `{column['type']}` | {column['description']} |\n"
                    )
                lines.append("\n")
        return "".join(lines)

    def render_landscape(
        self, landscape: Dict[str, Any], hints: Dict[str, Any] = None, **kwargs
    ) -> str:
        """
        Builds the Markdown landscape report string without writing.

        The landscape is a distinct artifact from the data catalog (Slice
        8.1/8.2): a deterministic, LLM-free orientation document answering
        "how big is this database, where do I start?". It renders the four
        ``build_landscape`` sections — Scale, Clusters, Core tables, and
        Relationship map — and optionally inlines LLM name-decoding hints
        (``hints={"clusters": {...}, "core_tables": {...}}``).

        Args:
            landscape: A dict from ``build_landscape``.
            hints: Optional ``{"clusters": {name: hint},
                "core_tables": {table: hint}}``.
            **kwargs: Must contain `db_profile_name`.

        Raises:
            ConfigError: If `db_profile_name` is not provided.
        """
        db_profile_name = kwargs.get("db_profile_name")
        if not db_profile_name:
            raise ConfigError(
                "MarkdownWriter.render_landscape requires 'db_profile_name' in kwargs."
            )
        hints = hints or {}

        lines = [f"# 🏞️ Landscape Report for {db_profile_name}\n"]

        # 1. Scale
        lines.append("\n## Scale\n\n")
        scale = landscape["scale"]
        column_types = (
            ", ".join(
                f"{column_type} ({count})"
                for column_type, count in scale["column_types"].items()
            )
            or "-"
        )
        lines.append("| Metric | Value |\n| --- | --- |\n")
        lines.append(f"| Tables | {scale['tables']} |\n")
        lines.append(f"| Columns | {scale['columns']} |\n")
        lines.append(f"| Column types | {column_types} |\n")

        # 2. Clusters
        lines.append("\n## Clusters\n\n")
        clusters = landscape["clusters"]
        cluster_hints = hints.get("clusters", {})
        non_empty = [name for name, members in clusters.items() if members]
        if not non_empty:
            lines.append("No tables found in this database.\n")
        else:
            for name in sorted(non_empty):
                members = clusters[name]
                lines.append(f"### `{name}` ({len(members)} tables)\n\n")
                hint = cluster_hints.get(name)
                if hint:
                    lines.append(f"> **Hint:** {hint}\n\n")
                for member in members:
                    lines.append(f"- `{member}`\n")
                lines.append("\n")

        # 3. Core tables
        lines.append("## Core tables\n\n")
        core = landscape["core_tables"]
        if not core:
            lines.append("No core tables found (no foreign key hubs).\n")
        else:
            lines.append("| Rank | Table | Degree |\n| --- | --- | --- |\n")
            for rank, entry in enumerate(core, 1):
                lines.append(
                    f"| {rank} | `{entry['table']}` | {entry['degree']} |\n"
                )
            core_hints = hints.get("core_tables", {})
            if core_hints:
                lines.append("\nHints:\n\n")
                for entry in core:
                    hint = core_hints.get(entry["table"])
                    if hint:
                        lines.append(f"- `{entry['table']}` — {hint}\n")

        # 4. Relationship map
        lines.append("\n## Relationship map\n\n")
        relationship = landscape["relationship_map"]
        nodes = relationship["nodes"]
        if nodes:
            lines.append(
                f"Nodes ({len(nodes)}): "
                + ", ".join(f"`{node}`" for node in nodes)
                + "\n"
            )
        else:
            lines.append("No nodes (no tables).\n")
        edges = relationship["edges"]
        if edges:
            lines.append("\nEdges:\n\n")
            lines.append(
                "| Source | Source column | Target | Target column |\n"
                "| --- | --- | --- | --- |\n"
            )
            for edge in edges:
                lines.append(
                    f"| `{edge['source']}` | `{edge['source_column']}` | "
                    f"`{edge['target']}` | `{edge['target_column']}` |\n"
                )
        else:
            lines.append("\nNo edges (no foreign keys).\n")

        return "".join(lines)

    def write_landscape(self, landscape: Dict[str, Any], **kwargs) -> None:
        """
        Writes the landscape report to a Markdown file atomically.

        Args:
            landscape: A dict from ``build_landscape``.
            **kwargs: Must contain `output_filename`, `db_profile_name`;
                may contain `hints` (same shape as ``render_landscape``).

        Raises:
            ConfigError: If required `kwargs` are missing.
            WriterError: If an error occurs during file writing.
        """
        output_filename = kwargs.get("output_filename")
        db_profile_name = kwargs.get("db_profile_name")
        if not output_filename or not db_profile_name:
            raise ConfigError(
                "MarkdownWriter.write_landscape requires 'output_filename' "
                "and 'db_profile_name' in kwargs."
            )
        try:
            logger.info(
                f"Writing landscape report for '{db_profile_name}' "
                f"to '{output_filename}'."
            )
            content = self.render_landscape(
                landscape,
                hints=kwargs.get("hints"),
                db_profile_name=db_profile_name,
            )
            self._atomic_write(output_filename, content)
            logger.info(
                f"Successfully wrote landscape report to '{output_filename}'."
            )
        except IOError as e:
            raise WriterError(
                f"Error writing landscape report to file "
                f"'{output_filename}': {e}"
            ) from e

    def write(self, catalog_data: Dict[str, List[Dict[str, Any]]], **kwargs):
        """
        Writes the catalog data to a Markdown file.

        The generated file has the following structure:
        1.  A main title for the database profile.
        2.  An Entity Relationship Diagram (ERD) generated with Mermaid.js.
        3.  A section for all database views with their AI-generated summaries and SQL code.
        4.  A section for all database tables with their AI-generated summaries and column details.

        Args:
            catalog_data: A dictionary containing the structured catalog data.
            **kwargs: Must contain `output_filename` and `db_profile_name`.

        Raises:
            ConfigError: If required `kwargs` are missing.
            WriterError: If an error occurs during file writing.
        """
        output_filename = kwargs.get("output_filename")
        db_profile_name = kwargs.get("db_profile_name")
        if not output_filename or not db_profile_name:
            raise ConfigError(
                "MarkdownWriter requires 'output_filename' and 'db_profile_name' in kwargs."
            )

        try:
            logger.info(
                f"Writing data catalog for '{db_profile_name}' to '{output_filename}'."
            )
            content = self.render(catalog_data, db_profile_name=db_profile_name)
            self._atomic_write(output_filename, content)
            logger.info(f"Successfully wrote catalog to '{output_filename}'.")
        except IOError as e:
            raise WriterError(
                f"Error writing to file '{output_filename}': {e}"
            ) from e
