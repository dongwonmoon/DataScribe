# Data Scribe

Data Scribe is a command-line tool that automatically generates a data catalog from your database schema or dbt project. It inspects your database or dbt project, extracts table and column information, and uses a Large Language Model (LLM) to generate business-friendly descriptions for each column, outputting a clean, easy-to-read Markdown file.

This tool is designed to help data teams document their data assets efficiently, making it easier for analysts, data scientists, and other stakeholders to discover and understand the data available to them.

## Features

- **Automated Data Cataloging**: Automatically generates a data catalog from your database schema or dbt project.
- **AI-Powered Descriptions**: Uses an LLM to generate meaningful, business-focused descriptions for your database columns.
- **Extensible Architecture**: Easily extendable to support different databases and LLM providers through a simple, interface-based plugin system.
- **Configuration-Driven**: Simple YAML-based configuration for managing database connections and LLM settings.
- **Markdown Output**: Generates a clean and readable Markdown file, perfect for documentation and sharing.

## Installation

You can install Data Scribe using pip:

```bash
pip install .
```

## Usage

Data Scribe provides two main commands: `db` and `dbt`.

### `db`

The `db` command scans a database schema, generates a data catalog using an LLM, and writes it to a Markdown file.

```bash
data-scribe db [OPTIONS]
```

**Options:**

*   `--db TEXT`: The name of the database profile to use from `config.yaml`.
*   `--llm TEXT`: The name of the LLM profile to use from `config.yaml`.
*   `--config TEXT`: The path to the configuration file. Defaults to `config.yaml`.
*   `--output TEXT`: The name of the output file. Defaults to `db_catalog.md`.

### `dbt`

The `dbt` command scans a dbt project, generates a data catalog using an LLM, and writes it to a Markdown file.

```bash
data-scribe dbt [OPTIONS]
```

**Options:**

*   `--project-dir TEXT`: The path to the dbt project directory.
*   `--llm TEXT`: The name of the LLM profile to use from `config.yaml`.
*   `--config TEXT`: The path to the configuration file. Defaults to `config.yaml`.
*   `--output TEXT`: The name of the output file. Defaults to `dbt_catalog.md`.

## Configuration

Data Scribe uses a `config.yaml` file to configure database connections and LLM providers.

### `.env` file

Before configuring `config.yaml`, you need to create a `.env` file in the root of the project and add your API keys. For example:

```
OPENAI_API_KEY="your-api-key-here"
```

### `config.yaml`

Here is an example of a `config.yaml` file:

```yaml
# config.yaml

default:
  db: dev_sqlite
  llm: openai_dev

db_connections:
  dev_sqlite:
    type: "sqlite"
    path: "test.db" # Path to your SQLite database

  # Example for PostgreSQL
  # prod_postgres:
  #   type: "postgres"
  #   host: "prod.db.example.com"
  #   user: "admin"
  #   password: "${PROD_DB_PASSWORD}" # Example of using an env var

llm_providers:
  openai_dev:
    provider: "openai"
    model: "gpt-3.5-turbo"
```

## Extensibility

Data Scribe is designed to be easily extensible. To add support for a new database or LLM provider, you need to:

1.  **Create a new connector/client class** in the `components` directory that implements the corresponding interface (`BaseConnector` or `BaseLLMClient`).
2.  **Register your new class** in the appropriate registry (`DB_CONNECTOR_REGISTRY` or `LLM_CLIENT_REGISTRY`) in `data_scribe/core/factory.py`.

That's it! The factory will then be able to instantiate your new component based on the configuration in `config.yaml`.

## Contributing

Contributions are welcome! Please feel free to open an issue or submit a pull request.