"""
This module defines the workflow for the 'lineage' command.

It combines physical database lineage (from foreign keys) with logical dbt
project lineage (from refs and sources) to generate a single, comprehensive
end-to-end data lineage graph.
"""
import typer
from typing import List, Dict, Any, Set

from data_scribe.core.factory import get_db_connector, get_writer
from data_scribe.core.dbt_parser import DbtManifestParser
from data_scribe.core.workflow_helpers import load_config
from data_scribe.utils.logger import get_logger

logger = get_logger(__name__)

class GlobalLineageGenerator:
    """
    Builds a global lineage graph from multiple sources.

    This class merges physical foreign key relationships from a database with
    logical dependencies from a dbt project (`ref` and `source` calls) into a
    single graph structure. It intelligently assigns and prioritizes styles to
    nodes to ensure, for example, that a dbt model is always styled as a model,
    even if it's also a plain database table.
    """
    def __init__(self, db_fks: List[Dict[str, str]], dbt_models: List[Dict[str, Any]]):
        """
        Initializes the GlobalLineageGenerator.

        Args:
            db_fks: A list of foreign key relationships from the database.
            dbt_models: A list of parsed dbt models, including their dependencies.
        """
        self.db_fks = db_fks
        self.dbt_models = dbt_models
        
        # Stores nodes and their assigned style, e.g., {"stg_orders": "box"}
        self.nodes: Dict[str, str] = {}
        # Stores unique edges to prevent duplicates in the graph
        self.edges: Set[str] = set()

    def _get_style_priority(self, style: str) -> int:
        """Assigns a priority to a node style. Higher numbers win."""
        if style == "box": return 3    # dbt model (highest priority)
        if style == "source": return 2 # dbt source
        if style == "db": return 1    # db table (lowest priority)
        return 0

    def _add_node(self, name: str, style: str = "box"):
        """
        Adds a node to the graph, applying style based on priority.

        If the node already exists, its style is only updated if the new
        style has a higher priority than the current one. This ensures a
        dbt model is always styled as a model, not as a generic DB table.
        """
        current_style = self.nodes.get(name)
        current_priority = self._get_style_priority(current_style) if current_style else -1
        new_priority = self._get_style_priority(style)
        
        if new_priority > current_priority:
            self.nodes[name] = style

    def _add_edge(self, from_node: str, to_node: str, label: str = ""):
        """Adds a unique, formatted edge to the graph's edge set."""
        if label:
            self.edges.add(f'    {from_node} -- "{label}" --> {to_node}')
        else:
            self.edges.add(f'    {from_node} --> {to_node}')

    def generate_graph(self) -> str:
        """
        Generates the complete Mermaid.js graph string.

        It processes database foreign keys first, then dbt dependencies,
        allowing the style prioritization logic in `_add_node` to work
        correctly. Finally, it assembles the unique nodes and edges into a
        single string.

        Returns:
            A string containing the full Mermaid.js graph definition.
        """
        logger.info("Generating global lineage graph...")
        
        # 1. Process DB Foreign Keys (Physical Lineage)
        for fk in self.db_fks:
            from_table = fk["from_table"]
            to_table = fk["to_table"]
            
            # Add nodes with 'db' style (lowest priority)
            self._add_node(from_table, "db")
            self._add_node(to_table, "db")
            self._add_edge(from_table, to_table, "FK")

        # 2. Process dbt Model Dependencies (Logical Lineage)
        for model in self.dbt_models:
            model_name = model["name"]
            self._add_node(model_name, "box") # Style dbt models (highest priority)
            
            for dep in model.get("dependencies", []):
                # A dependency with a dot is a source (e.g., 'jaffle_shop.customers')
                if "." in dep:
                    self._add_node(dep, "source")
                    self._add_edge(dep, model_name)
                else: # Otherwise, it's another dbt model (a ref)
                    self._add_node(dep, "box")
                    self._add_edge(dep, model_name)

        # 3. Combine into a Mermaid string
        graph_lines = ["graph TD;"]
        
        # Define all nodes with their final, prioritized styles
        node_definitions = []
        for name, style in self.nodes.items():
            if style == "box":
                node_definitions.append(f'    {name}["{name}"]') # dbt model
            elif style == "db":
                node_definitions.append(f'    {name}[("{name}")]') # DB table
            elif style == "source":
                node_definitions.append(f'    {name}(("{name}"))') # dbt source
        
        graph_lines.extend(sorted(node_definitions))
        graph_lines.append("") # Spacer for readability
        graph_lines.extend(sorted(list(self.edges)))
        
        return "\n".join(graph_lines)


class LineageWorkflow:
    """
    Manages the end-to-end workflow for the `data-scribe lineage` command.
    """
    def __init__(
        self,
        config_path: str,
        db_profile: str,
        dbt_project_dir: str,
        output_profile: str,
    ):
        """
        Initializes the LineageWorkflow with parameters from the CLI.
        """
        self.config_path = config_path
        self.db_profile_name = db_profile
        self.dbt_project_dir = dbt_project_dir
        self.output_profile_name = output_profile
        self.config = load_config(config_path)

    def run(self):
        """
        Executes the full lineage generation and writing workflow.
        """
        
        # 1. Get Physical Lineage (FKs) from DB
        db_connector = None
        db_fks = []
        try:
            logger.info(f"Connecting to DB '{self.db_profile_name}' for FK scan...")
            db_params = self.config["db_connections"][self.db_profile_name]
            db_type = db_params.pop("type")
            db_connector = get_db_connector(db_type, db_params)
            db_fks = db_connector.get_foreign_keys()
            logger.info(f"Found {len(db_fks)} foreign key relationships.")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise typer.Exit(code=1)
        finally:
            if db_connector:
                db_connector.close()
                
        # 2. Get Logical Lineage (refs) from dbt
        logger.info(f"Parsing dbt project at '{self.dbt_project_dir}' for dependencies...")
        parser = DbtManifestParser(self.dbt_project_dir)
        dbt_models = parser.models
        logger.info(f"Parsed {len(dbt_models)} dbt models.")
        
        # 3. Generate Graph
        generator = GlobalLineageGenerator(db_fks, dbt_models)
        mermaid_graph = generator.generate_graph()
        
        catalog_data = {"mermaid_graph": mermaid_graph}
        
        # 4. Write to file
        try:
            writer_params = self.config["output_profiles"][self.output_profile_name]
            writer_type = writer_params.pop("type")
            # The workflow requires a 'mermaid' writer type.
            if writer_type != "mermaid":
                 logger.warning(f"Output profile '{self.output_profile_name}' is not type 'mermaid'. Using MermaidWriter anyway.")
            
            writer = get_writer("mermaid") # Force MermaidWriter
            
            writer.write(catalog_data, **writer_params)
            logger.info(
                f"Global lineage graph written successfully using output profile: '{self.output_profile_name}'."
            )
        except Exception as e:
            logger.error(
                f"Failed to write lineage graph using profile '{self.output_profile_name}': {e}"
            )
            raise typer.Exit(code=1)