"""
This module defines the core business logic for generating a unified global
lineage graph (Mermaid) by combining information from both dbt projects
and database foreign keys (FKs).

Design Rationale:
Data lineage is often fragmented across different systems (e.g., explicit
foreign keys in a database, implicit dependencies in dbt models). This module
aims to consolidate these disparate sources into a single, coherent view.
By generating a Mermaid graph, it provides a human-readable and easily
renderable visualization of data flow, which is crucial for understanding
data transformations and impacts. The generator prioritizes dbt model
lineage over generic database table lineage to reflect the higher-level
business logic often encapsulated in dbt.
"""

from typing import List, Dict, Any, Set

from schema_scribe.utils.logger import get_logger

logger = get_logger(__name__)


class GlobalLineageGenerator:
    """
    Merges physical (FK) and logical (dbt) lineage to create a single,
    unified Mermaid graph string.

    This class processes foreign key relationships from a database and
    dependencies from dbt models, resolving conflicts and styling nodes
    appropriately to produce a comprehensive data lineage visualization.
    """

    def __init__(
        self, db_fks: List[Dict[str, str]], dbt_models: List[Dict[str, Any]]
    ):
        """
        Initializes the GlobalLineageGenerator.

        Args:
            db_fks: A list of foreign key relationships returned from a BaseConnector.
                    Each dictionary is expected to contain 'source_table',
                    'target_table', 'source_column', 'target_column'.
            dbt_models: A list of dbt models parsed from a DbtManifestParser.
                        Each dictionary is expected to contain 'name' and
                        'dependencies'.
        """
        self.db_fks = db_fks
        self.dbt_models = dbt_models
        self.nodes: Dict[str, str] = {}  # Stores node names and their styles
        self.edges: Set[str] = (
            set()
        )  # Stores unique edges to prevent duplicates

    def _get_style_priority(self, style: str) -> int:
        """
        Assigns a priority to node styles. Higher numbers indicate higher priority.

        Design Rationale:
        This priority system ensures that when a node (e.g., a table) appears
        in multiple contexts (e.g., as a raw DB table and as a dbt model),
        its visual representation reflects the most significant role. Dbt models
        are given the highest priority as they often represent transformed,
        business-logic-driven entities.
        """
        if style == "box":
            return 3  # dbt model (highest priority)
        if style == "source":
            return 2  # dbt source
        if style == "db":
            return 1  # db table (lowest priority)
        return 0

    def _add_node(self, name: str, style: str):
        """
        Adds a node to the graph or updates its style based on priority.

        If a node with the given `name` already exists, its style is updated
        only if the `new_style` has a higher priority than the `current_style`.
        This ensures that, for instance, a dbt model style always overrides
        a generic database table style for the same entity.
        """
        current_style = self.nodes.get(name)
        current_priority = (
            self._get_style_priority(current_style) if current_style else -1
        )
        new_priority = self._get_style_priority(style)

        if new_priority > current_priority:
            self.nodes[name] = style

    def _add_edge(self, from_node: str, to_node: str, label: str = ""):
        """
        Adds a unique edge to the graph.

        Edges are stored in a set to automatically handle duplicates, ensuring
        that the final Mermaid graph does not contain redundant connections.
        """
        if label:
            self.edges.add(f'    {from_node} -- "{label}" --> {to_node}')
        else:
            self.edges.add(f"    {from_node} --> {to_node}")

    def generate_graph(self) -> str:
        """
        Generates a complete Mermaid.js graph string by merging physical and
        logical lineage information.

        The process involves:
        1.  Processing physical lineage from database foreign keys, adding
            nodes and edges with 'db' style.
        2.  Processing logical lineage from dbt model dependencies, adding
            nodes and edges with 'box' (for models) or 'source' (for sources)
            styles, respecting style priorities.
        3.  Combining all collected nodes and edges into a Mermaid 'graph TD'
            syntax string, applying appropriate Mermaid node shapes based on style.

        Returns:
            A string representing the Mermaid.js graph definition.
        """
        logger.info("Generating global lineage graph...")

        # 1. Process physical lineage (DB Foreign Keys)
        for fk in self.db_fks:
            from_table = fk["source_table"]
            to_table = fk["target_table"]
            self._add_node(from_table, "db")
            self._add_node(to_table, "db")
            self._add_edge(from_table, to_table, "FK")

        # 2. Process logical lineage (dbt Model Dependencies)
        for model in self.dbt_models:
            model_name = model["name"]
            self._add_node(model_name, "box")  # dbt model has highest priority

            for dep in model.get("dependencies", []):
                if (
                    "." in dep
                ):  # Heuristic for dbt sources (e.g., 'source_name.table_name')
                    self._add_node(dep, "source")
                    self._add_edge(dep, model_name)
                else:  # Assume it's another dbt model
                    self._add_node(dep, "box")
                    self._add_edge(dep, model_name)

        # 3. Combine into Mermaid string
        graph_lines = ["graph TD;"]
        node_definitions = []
        for name, style in self.nodes.items():
            if style == "box":
                node_definitions.append(
                    f'    {name}["{name}"]'
                )  # dbt model (rectangular box)
            elif style == "db":
                node_definitions.append(
                    f'    {name}[("{name}")]'
                )  # DB table (rounded box)
            elif style == "source":
                node_definitions.append(
                    f'    {name}(("{name}"))'
                )  # dbt source (stadium shape)

        graph_lines.extend(sorted(node_definitions))
        graph_lines.append("")
        graph_lines.extend(sorted(list(self.edges)))

        return "\n".join(graph_lines)
