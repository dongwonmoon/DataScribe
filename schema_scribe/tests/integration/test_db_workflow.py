"""
Integration tests for the 'db' command workflow.

This test suite verifies the end-to-end functionality of the DbWorkflow,
ensuring that it correctly uses the components (connector, generator, writer)
to produce a data catalog from a database.
"""

import json
import logging
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from schema_scribe.workflows.db_workflow import DbWorkflow
from schema_scribe.components.writers import MarkdownWriter
from schema_scribe.core.exceptions import LLMClientError
from schema_scribe.core.interfaces import (
    BaseConnector,
    BaseLLMClient,
    BaseWriter,
)
from schema_scribe.services.schema_state import SchemaState
from schema_scribe.utils.logger import get_logger


@pytest.fixture
def mock_db_connector(sqlite_db: str):
    """
    Provides a mock DbConnector that simulates a SQLite connection.
    """
    mock_connector = MagicMock(spec=BaseConnector)
    mock_connector.db_profile_name = "test_db"
    mock_connector.get_tables.return_value = [
        {"name": "users", "comment": None},
        {"name": "products", "comment": None},
        {"name": "orders", "comment": None},
    ]
    mock_connector.get_views.return_value = [
        {
            "name": "user_orders",
            "definition": "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
        }
    ]
    mock_connector.get_columns.side_effect = [
        # users table columns
        [
            {
                "name": "id",
                "type": "INTEGER",
                "is_nullable": False,
                "is_pk": True,
                "comment": None,
            },
            {
                "name": "email",
                "type": "TEXT",
                "is_nullable": False,
                "is_pk": False,
                "comment": None,
            },
        ],
        # products table columns
        [
            {
                "name": "id",
                "type": "INTEGER",
                "is_nullable": False,
                "is_pk": True,
                "comment": None,
            },
            {
                "name": "name",
                "type": "TEXT",
                "is_nullable": False,
                "is_pk": False,
                "comment": None,
            },
            {
                "name": "price",
                "type": "REAL",
                "is_nullable": False,
                "is_pk": False,
                "comment": None,
            },
        ],
        # orders table columns
        [
            {
                "name": "id",
                "type": "INTEGER",
                "is_nullable": False,
                "is_pk": True,
                "comment": None,
            },
            {
                "name": "user_id",
                "type": "INTEGER",
                "is_nullable": False,
                "is_pk": False,
                "comment": None,
            },
            {
                "name": "product_id",
                "type": "INTEGER",
                "is_nullable": False,
                "is_pk": False,
                "comment": None,
            },
        ],
    ]
    mock_connector.get_foreign_keys.return_value = [
        {
            "source_table": "orders",
            "source_column": "user_id",
            "target_table": "users",
            "target_column": "id",
        },
        {
            "source_table": "orders",
            "source_column": "product_id",
            "target_table": "products",
            "target_column": "id",
        },
    ]
    mock_connector.get_column_profile.return_value = {
        "total_count": 0,
        "null_count": 0,
        "distinct_count": 0,
        "is_unique": True,
    }
    mock_connector.close.return_value = None
    return mock_connector


@pytest.fixture
def mock_llm_client():
    """
    Provides a mock LLMClient that returns a predictable description.
    """
    mock_client = MagicMock(spec=BaseLLMClient)
    mock_client.llm_profile_name = "test_llm"
    mock_client.get_description.return_value = (
        "This is an AI-generated description."
    )
    return mock_client


@pytest.fixture
def mock_writer():
    """
    Provides a mock Writer.
    """
    mock_writer_instance = MagicMock(spec=BaseWriter)
    mock_writer_instance.write.return_value = None
    return mock_writer_instance


def test_db_workflow_end_to_end(
    tmp_path: Path, mock_db_connector, mock_llm_client, mock_writer
):
    """
    Tests the full DbWorkflow from configuration to final output.

    This test uses fixtures to:
    1. Provide a mock DbConnector (`mock_db_connector`).
    2. Provide a mock LLM client (`mock_llm_client`).
    3. Provide a mock Writer (`mock_writer`).

    It then runs the workflow and asserts that the writer's `write` method
    is called with the expected catalog data.
    """
    # Arrange
    output_md_path = tmp_path / "db_catalog.md"
    writer_params = {"output_filename": str(output_md_path)}

    # Act
    workflow = DbWorkflow(
        llm_client=mock_llm_client,
        db_connector=mock_db_connector,
        writer=mock_writer,
        db_profile_name="test_db",
        output_profile_name="test_markdown_output",
        writer_params=writer_params,
    )
    workflow.run()

    # Assert
    # 1. Verify components were called
    mock_db_connector.get_tables.assert_called_once()
    mock_db_connector.get_views.assert_called_once()
    assert (
        mock_db_connector.get_columns.call_count == 3
    )  # Only called for tables
    mock_db_connector.get_foreign_keys.assert_called_once()
    mock_llm_client.get_description.assert_called()  # Called for tables, views, columns
    mock_writer.write.assert_called_once()
    mock_db_connector.close.assert_called_once()

    # 2. Assert the catalog data passed to the writer
    captured_catalog_data = mock_writer.write.call_args[0][0]
    captured_writer_params = mock_writer.write.call_args[1]

    assert "tables" in captured_catalog_data
    assert "views" in captured_catalog_data
    assert "foreign_keys" in captured_catalog_data

    assert captured_writer_params == {
        "db_profile_name": "test_db",
        "db_connector": mock_db_connector,  # Added mock_db_connector to expected params
        "output_filename": str(output_md_path),
    }

    # Check for mocked AI description in tables/views/columns
    for table in captured_catalog_data["tables"]:
        assert table["ai_summary"] == "This is an AI-generated description."
        for col in table["columns"]:
            assert col["description"] == "This is an AI-generated description."
    for view in captured_catalog_data["views"]:
        assert view["ai_summary"] == "This is an AI-generated description."


def test_db_workflow_end_to_end_with_profiling(
    tmp_path: Path, mock_db_connector, mock_llm_client, mock_writer
):
    """
    Tests the full DbWorkflow with profiling enabled.

    This test verifies that:
    1. The workflow runs.
    2. The CatalogGenerator *calls* the get_column_profile method.
    3. The profile stats are *included* in the prompt sent to the LLM.
    """
    # Arrange
    output_md_path = tmp_path / "db_catalog_profile.md"
    writer_params = {"output_filename": str(output_md_path)}

    # Act
    workflow = DbWorkflow(
        llm_client=mock_llm_client,
        db_connector=mock_db_connector,
        writer=mock_writer,
        db_profile_name="test_db",
        output_profile_name="test_markdown_output",
        writer_params=writer_params,
    )
    workflow.run()

    # Assert
    # 1. Verify components were called
    mock_db_connector.get_column_profile.assert_called()
    mock_llm_client.get_description.assert_called()

    # 2. Verify the LLM was called with the *correct profiling context*
    # Get the list of all calls made to the mock LLM
    calls = mock_llm_client.get_description.call_args_list

    # Find the prompt for a specific column (e.g., 'users.id')
    users_id_prompt = None
    for call in calls:
        prompt_text = call[0][0]
        # Make the search more flexible
        if (
            "Table: {'name': 'users', 'comment': None}" in prompt_text
            and "Column: id" in prompt_text
            and "Data Profile Context:" in prompt_text
        ):
            users_id_prompt = prompt_text
            break

    assert users_id_prompt is not None, "Prompt for 'users.id' was not found"

    # Check that the prompt contains the mocked profile stats
    assert "Data Profile Context:" in users_id_prompt
    assert "- Null Ratio: N/A" in users_id_prompt
    assert "- Is Unique: True" in users_id_prompt
    assert "- Distinct Count: 0" in users_id_prompt

    # 3. Verify the total number of LLM calls
    # 3 tables + 1 view = 4 summary calls
    # 2 (users) + 3 (products) + 3 (orders) = 8 column calls
    # Total = 12 calls (4 summaries + 8 columns)
    assert mock_llm_client.get_description.call_count == 12


def test_dry_run_prints_manifest_without_llm_or_write(capsys):
    """
    A dry run must disclose exactly what would be sent to the LLM, without
    constructing the LLM client, profiling columns, or writing anything.
    """
    connector = MagicMock(spec=BaseConnector)
    connector.get_tables.return_value = ["users"]
    connector.get_columns.return_value = [{"name": "id", "type": "INTEGER"}]
    connector.get_views.return_value = [
        {
            "name": "v_active",
            "definition": (
                "CREATE VIEW v_active AS SELECT id FROM users WHERE status = 'paid'"
            ),
        }
    ]
    connector.get_foreign_keys.return_value = []

    wf = DbWorkflow(
        connector,
        llm_client=None,
        writer=None,
        db_profile_name="mydb",
        provider_name="ollama",
    )
    wf.dry_run()

    out = capsys.readouterr().out
    assert "mydb" in out
    assert "users" in out and "id" in out
    assert "v_active" in out and "status = 'paid'" in out  # view SQL disclosed verbatim
    assert "ollama" in out
    assert "0" in out  # FK count disclosed
    assert (
        "Per-column aggregate stats (null_ratio, distinct_count, is_unique) "
        "computed at run time" in out
    )
    connector.get_column_profile.assert_not_called()
    connector.close.assert_called_once()


def test_dry_run_full_prints_profile_values(capsys):
    """
    With full=True a dry run must ALSO compute and print per-column profile
    stat values (one line per column, N/A for None), while still disclosing
    the view SQL verbatim, writing nothing, and calling no LLM.
    """
    connector = MagicMock(spec=BaseConnector)
    connector.get_tables.return_value = ["users"]
    connector.get_columns.return_value = [
        {"name": "id", "type": "INTEGER"},
        {"name": "email", "type": "TEXT"},
    ]
    connector.get_views.return_value = [
        {
            "name": "v_active",
            "definition": (
                "CREATE VIEW v_active AS SELECT id FROM users WHERE status = 'paid'"
            ),
        }
    ]
    connector.get_foreign_keys.return_value = []
    connector.get_column_profile.side_effect = [
        {"null_ratio": 0.0, "distinct_count": 1044, "is_unique": True},
        {"null_ratio": None, "distinct_count": None, "is_unique": None},
    ]

    wf = DbWorkflow(
        connector,
        llm_client=None,
        writer=None,
        db_profile_name="mydb",
        provider_name="ollama",
    )
    wf.dry_run(full=True)

    out = capsys.readouterr().out
    assert "users.id: null_ratio=0.0  distinct=1044  unique=true" in out
    assert "users.email: null_ratio=N/A  distinct=N/A  unique=N/A" in out
    assert "status = 'paid'" in out  # view SQL still disclosed verbatim
    assert connector.get_column_profile.call_count == 2  # once per column
    connector.close.assert_called_once()


def test_db_command_full_without_dry_run_errors():
    """
    The --full flag is meaningless without --dry-run and must be rejected
    before any configuration or connection work happens.
    """
    from typer.testing import CliRunner

    from schema_scribe.app import app

    result = CliRunner().invoke(app, ["db", "--full"])
    assert result.exit_code == 1
    assert "--full requires --dry-run" in result.output


def test_run_discloses_payload_before_first_llm_call(caplog):
    """
    The real run must log what will be sent to the LLM BEFORE the first
    get_description call: summary/description counts, provider name, and
    the collected metadata counts (tables, columns, views).
    """
    connector = MagicMock(spec=BaseConnector)
    connector.get_tables.return_value = ["t"]
    connector.get_columns.return_value = [
        {
            "name": "c",
            "type": "INTEGER",
            "description": "",
            "is_nullable": True,
            "is_pk": False,
        }
    ]
    connector.get_views.return_value = [{"name": "v", "definition": "SELECT * FROM t"}]
    connector.get_foreign_keys.return_value = []
    connector.get_column_profile.return_value = {
        "total_count": 0,
        "null_count": 0,
        "distinct_count": 0,
        "is_unique": True,
    }
    mock_llm_client = MagicMock(spec=BaseLLMClient)

    def mark_llm_call(*args, **kwargs):
        get_logger("schema_scribe.services.catalog_generator").info(
            "LLM get_description called"
        )
        return "This is an AI-generated description."

    mock_llm_client.get_description.side_effect = mark_llm_call

    wf = DbWorkflow(
        connector,
        mock_llm_client,
        writer=None,
        db_profile_name="d",
        provider_name="openai",
    )
    with caplog.at_level(logging.INFO):
        wf.run()

    assert (
        "Sending 1 table summaries and 1 column descriptions to provider "
        "'openai'. Metadata: tables=1, columns=1, views=1."
    ) in caplog.text

    messages = [r.getMessage() for r in caplog.records]
    disclosure_index = next(
        i for i, m in enumerate(messages) if "table summaries" in m
    )
    first_llm_index = next(
        i for i, m in enumerate(messages) if m == "LLM get_description called"
    )
    assert disclosure_index < first_llm_index


def test_run_disclosure_defaults_provider_to_unknown(caplog):
    """
    When no provider name is available, the disclosure logs 'unknown'
    instead of failing or guessing.
    """
    connector = MagicMock(spec=BaseConnector)
    connector.get_tables.return_value = []
    connector.get_views.return_value = []
    connector.get_foreign_keys.return_value = []
    mock_llm_client = MagicMock(spec=BaseLLMClient)

    wf = DbWorkflow(connector, mock_llm_client, writer=None, db_profile_name="d")
    with caplog.at_level(logging.INFO):
        wf.run()

    assert "to provider 'unknown'." in caplog.text
    mock_llm_client.get_description.assert_not_called()


def test_generation_failure_preserves_output(tmp_path):
    """
    Regression lock: an LLM failure mid-run must leave the previous output
    file untouched — the writer never runs because generation completes
    before the write phase (db_workflow.py run() ordering).
    """
    connector = MagicMock(spec=BaseConnector)
    connector.get_tables.return_value = ["t"]
    connector.get_columns.return_value = [
        {
            "name": "c",
            "type": "INTEGER",
            "description": "",
            "is_nullable": True,
            "is_pk": False,
        }
    ]
    connector.get_views.return_value = []
    connector.get_foreign_keys.return_value = []
    connector.get_column_profile.return_value = {
        "total_count": 0,
        "null_count": 0,
        "distinct_count": 0,
        "is_unique": True,
    }
    llm = MagicMock(spec=BaseLLMClient)
    llm.get_description.side_effect = LLMClientError("boom")

    target = tmp_path / "catalog.md"
    target.write_text("PREVIOUS")

    writer = MarkdownWriter()
    wf = DbWorkflow(
        connector,
        llm,
        writer,
        writer_params={"output_filename": str(target)},
    )
    with pytest.raises(LLMClientError):
        wf.run()

    assert target.read_text() == "PREVIOUS"
    connector.close.assert_called_once()


def test_run_writes_schema_state_sidecar(
    tmp_path: Path, mock_llm_client, mock_writer
):
    """
    After a successful writer.write, run() must persist the schema state
    snapshot to <output_filename>.schema-state.json: per-table column
    types and PKs, FK (source, target) pairs, and view names.

    The connector mock returns string table names per the BaseConnector
    interface (the shared fixture's dict names are out-of-contract).
    """
    connector = MagicMock(spec=BaseConnector)
    connector.db_profile_name = "test_db"
    connector.get_tables.return_value = ["users", "orders"]
    connector.get_columns.side_effect = [
        [
            {
                "name": "id",
                "type": "INTEGER",
                "is_nullable": False,
                "is_pk": True,
                "comment": None,
            },
            {
                "name": "email",
                "type": "TEXT",
                "is_nullable": False,
                "is_pk": False,
                "comment": None,
            },
        ],
        [
            {
                "name": "id",
                "type": "INTEGER",
                "is_nullable": False,
                "is_pk": True,
                "comment": None,
            },
            {
                "name": "user_id",
                "type": "INTEGER",
                "is_nullable": False,
                "is_pk": False,
                "comment": None,
            },
        ],
    ]
    connector.get_views.return_value = [
        {"name": "user_orders", "definition": "SELECT * FROM users JOIN orders"}
    ]
    connector.get_foreign_keys.return_value = [
        {
            "source_table": "orders",
            "source_column": "user_id",
            "target_table": "users",
            "target_column": "id",
        }
    ]
    connector.get_column_profile.return_value = {
        "total_count": 0,
        "null_count": 0,
        "distinct_count": 0,
        "is_unique": True,
    }
    connector.close.return_value = None

    output_md_path = tmp_path / "db_catalog.md"
    workflow = DbWorkflow(
        llm_client=mock_llm_client,
        db_connector=connector,
        writer=mock_writer,
        db_profile_name="test_db",
        output_profile_name="test_markdown_output",
        writer_params={"output_filename": str(output_md_path)},
    )
    workflow.run()

    sidecar = tmp_path / "db_catalog.md.schema-state.json"
    assert sidecar.exists()
    state = json.loads(sidecar.read_text())
    assert set(state["tables"]) == {"users", "orders"}
    assert state["tables"]["users"]["columns"] == {
        "id": "INTEGER",
        "email": "TEXT",
    }
    assert state["tables"]["users"]["pk"] == ["id"]
    assert state["tables"]["orders"]["fks"] == [
        {"source": "user_id", "target": "users.id"},
    ]
    assert state["views"] == ["user_orders"]
    assert "generated_at" in state


def test_run_without_output_filename_writes_no_sidecar(
    tmp_path: Path, mock_db_connector, mock_llm_client, mock_writer
):
    """
    A run without output_filename in writer_params must not write any
    sidecar file — the sidecar is only meaningful next to a file output.
    """
    workflow = DbWorkflow(
        llm_client=mock_llm_client,
        db_connector=mock_db_connector,
        writer=mock_writer,
        db_profile_name="test_db",
        output_profile_name="test_markdown_output",
    )
    workflow.run()

    assert list(tmp_path.glob("*.schema-state.json")) == []
    mock_writer.write.assert_called_once()


def test_sidecar_write_failure_does_not_fail_run(
    tmp_path: Path, mock_db_connector, mock_llm_client, mock_writer, caplog
):
    """
    The documentation output is the product and the sidecar is
    bookkeeping: a sidecar write failure must log a warning and leave the
    run's exit status unaffected.
    """
    output_md_path = tmp_path / "db_catalog.md"
    workflow = DbWorkflow(
        llm_client=mock_llm_client,
        db_connector=mock_db_connector,
        writer=mock_writer,
        db_profile_name="test_db",
        output_profile_name="test_markdown_output",
        writer_params={"output_filename": str(output_md_path)},
    )
    with caplog.at_level(logging.WARNING):
        with patch(
            "schema_scribe.workflows.db_workflow.SchemaState.save",
            side_effect=IOError("disk full"),
        ):
            workflow.run()

    mock_writer.write.assert_called_once()
    assert "Failed to save schema state sidecar" in caplog.text
    assert not (tmp_path / "db_catalog.md.schema-state.json").exists()


def _check_connector():
    """
    Minimal connector mock for check() tests: one table 'users' with a
    single PK column 'id', no views, no foreign keys.
    """
    connector = MagicMock(spec=BaseConnector)
    connector.get_tables.return_value = ["users"]
    connector.get_columns.return_value = [
        {"name": "id", "type": "INTEGER", "is_pk": True}
    ]
    connector.get_views.return_value = []
    connector.get_foreign_keys.return_value = []
    connector.get_column_profile.return_value = {
        "total_count": 0,
        "null_count": 0,
        "distinct_count": 0,
        "is_unique": True,
    }
    connector.close.return_value = None
    return connector


def _check_llm():
    """
    Minimal LLM mock for check() tests: every description is a fixed draft.
    """
    llm = MagicMock(spec=BaseLLMClient)
    llm.get_description.return_value = "draft"
    return llm


def test_check_no_sidecar_no_output_returns_false(capsys):
    """
    No sidecar and no existing output file → check() returns False and
    prints 'no existing documentation to compare' (exit-0 path).
    """
    wf = DbWorkflow(
        _check_connector(), _check_llm(), writer=None, db_profile_name="d"
    )
    assert wf.check() is False
    assert "no existing documentation to compare" in capsys.readouterr().out
    wf.db_connector.close.assert_called_once()


def test_check_stale_sidecar_reports_added_table(capsys, tmp_path):
    """
    A stale sidecar (baseline with zero tables) classifies the fresh
    generation's 'users' table as added → check() returns True and prints
    a per-object status line.
    """
    output = tmp_path / "out.md"
    SchemaState.save(
        {"tables": {}, "views": [], "generated_at": "x"},
        str(output) + ".schema-state.json",
    )
    wf = DbWorkflow(
        _check_connector(),
        _check_llm(),
        writer=None,
        db_profile_name="d",
        writer_params={"output_filename": str(output)},
    )
    assert wf.check() is True
    out = capsys.readouterr().out
    assert "[added]                users" in out


def test_check_unchanged_sidecar_returns_false(capsys, tmp_path):
    """
    A sidecar matching the fresh generation classifies 'users' as
    unchanged → check() returns False.
    """
    output = tmp_path / "out.md"
    SchemaState.save(
        {
            "tables": {
                "users": {"columns": {"id": "INTEGER"}, "pk": ["id"], "fks": []}
            },
            "views": [],
            "generated_at": "x",
        },
        str(output) + ".schema-state.json",
    )
    wf = DbWorkflow(
        _check_connector(),
        _check_llm(),
        writer=None,
        db_profile_name="d",
        writer_params={"output_filename": str(output)},
    )
    assert wf.check() is False
    out = capsys.readouterr().out
    assert "[unchanged]            users" in out


def test_check_prints_unified_diff_and_never_writes(tmp_path, capsys):
    """
    When the output file exists, check() prints a unified diff of the new
    render vs the existing content and leaves the file untouched.
    """
    output = tmp_path / "out.md"
    output.write_text("# OLD DOCUMENT\n", encoding="utf-8")
    SchemaState.save(
        {"tables": {}, "views": [], "generated_at": "x"},
        str(output) + ".schema-state.json",
    )
    wf = DbWorkflow(
        _check_connector(),
        _check_llm(),
        writer=MarkdownWriter(),
        db_profile_name="d",
        writer_params={"output_filename": str(output)},
    )
    assert wf.check() is True
    out = capsys.readouterr().out
    assert "# OLD DOCUMENT" in out  # existing content in the diff
    assert "Data Catalog for d" in out  # new render in the diff
    assert "+++ <new render>" in out
    assert output.read_text(encoding="utf-8") == "# OLD DOCUMENT\n"


def test_check_missing_sidecar_with_existing_output_fails_closed(tmp_path):
    """
    An existing output file without a sidecar has no baseline to prove
    currency → every table classifies as added, check() returns True.
    """
    output = tmp_path / "out.md"
    output.write_text("# OLD DOCUMENT\n", encoding="utf-8")
    wf = DbWorkflow(
        _check_connector(),
        _check_llm(),
        writer=MarkdownWriter(),
        db_profile_name="d",
        writer_params={"output_filename": str(output)},
    )
    assert wf.check() is True


def test_db_command_check_and_dry_run_mutually_exclusive():
    """
    --check and --dry-run are mutually exclusive and rejected before any
    configuration work happens.
    """
    from typer.testing import CliRunner

    from schema_scribe.app import app

    result = CliRunner().invoke(app, ["db", "--check", "--dry-run"])
    assert result.exit_code == 1
    assert "--check and --dry-run are mutually exclusive" in result.output


def _check_cli_config_manager(output_path):
    """
    Builds a fake ConfigManager for CLI check tests: sqlite-shaped mocks
    backed by the configured markdown output path.
    """

    class FakeConfigManager:
        def __init__(self, config_path):
            self.config_path = config_path

        def get_db_connector(self, cli_profile):
            connector = MagicMock(spec=BaseConnector)
            connector.get_tables.return_value = ["users"]
            connector.get_columns.return_value = [
                {"name": "id", "type": "INTEGER", "is_pk": True}
            ]
            connector.get_views.return_value = []
            connector.get_foreign_keys.return_value = []
            connector.get_column_profile.return_value = {
                "total_count": 0,
                "null_count": 0,
                "distinct_count": 0,
                "is_unique": True,
            }
            connector.close.return_value = None
            return connector, "d"

        def get_llm_client(self, cli_profile):
            return _check_llm(), "test_llm"

        def get_writer(self, cli_profile):
            return (
                MarkdownWriter(),
                "test_markdown_output",
                {"output_filename": str(output_path)},
            )

    return FakeConfigManager


def test_db_command_check_exits_1_when_schema_changed(monkeypatch, tmp_path):
    """
    `db --check` exits 1 when check() reports a change (stale sidecar).
    """
    from typer.testing import CliRunner

    from schema_scribe.app import app

    output_path = tmp_path / "catalog.md"
    SchemaState.save(
        {"tables": {}, "views": [], "generated_at": "x"},
        str(output_path) + ".schema-state.json",
    )
    monkeypatch.setattr(
        "schema_scribe.app.ConfigManager",
        _check_cli_config_manager(output_path),
    )
    result = CliRunner().invoke(app, ["db", "--check", "--config", "fake.yaml"])
    assert result.exit_code == 1


def test_db_command_check_exits_0_when_up_to_date(monkeypatch, tmp_path):
    """
    `db --check` exits 0 when the fresh generation matches the sidecar.
    """
    from typer.testing import CliRunner

    from schema_scribe.app import app

    output_path = tmp_path / "catalog.md"
    SchemaState.save(
        {
            "tables": {
                "users": {"columns": {"id": "INTEGER"}, "pk": ["id"], "fks": []}
            },
            "views": [],
            "generated_at": "x",
        },
        str(output_path) + ".schema-state.json",
    )
    monkeypatch.setattr(
        "schema_scribe.app.ConfigManager",
        _check_cli_config_manager(output_path),
    )
    result = CliRunner().invoke(app, ["db", "--check", "--config", "fake.yaml"])
    assert result.exit_code == 0
