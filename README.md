# Data Scribe: AI-Powered Data Documentation

Data Scribe is a command-line tool that automatically generates a data catalog from your database schema or dbt project. It inspects your database or dbt project, extracts table and column information, and uses a Large Language Model (LLM) to generate business-friendly descriptions for each column, outputting a clean, easy-to-read Markdown file.

This tool is designed to help data teams document their data assets efficiently, making it easier for analysts, data scientists, and other stakeholders to discover and understand the data available to them.

## Table of Contents

- [Data Scribe: AI-Powered Data Documentation](#data-scribe-ai-powered-data-documentation)
  - [Table of Contents](#table-of-contents)
  - [Getting Started](#getting-started)
    - [Installation](#installation)
    - [Usage](#usage)
      - [`db`](#db)
      - [`dbt`](#dbt)
  - [Configuration](#configuration)
    - [`.env` file](#env-file)
    - [`config.yaml`](#configyaml)
  - [Supported Databases and LLMs](#supported-databases-and-llms)
  - [Extensibility](#extensibility)
    - [Adding a New Database Connector](#adding-a-new-database-connector)
    - [Adding a New LLM Client](#adding-a-new-llm-client)
  - [Contributing](#contributing)

## Getting Started

### Installation

You can install Data Scribe using pip:

```bash
pip install .
```

### Usage

Data Scribe provides two main commands: `db` and `dbt`.

#### `db`

The `db` command scans a database schema, generates a data catalog using an LLM, and writes it to a Markdown file.

```bash
data-scribe db [OPTIONS]
```

**Options:**

*   `--db TEXT`: The name of the database profile to use from `config.yaml`.
*   `--llm TEXT`: The name of the LLM profile to use from `config.yaml`.
*   `--config TEXT`: The path to the configuration file. Defaults to `config.yaml`.
*   `--output TEXT`: The name of the output file. Defaults to `db_catalog.md`.

#### `dbt`

The `dbt` command scans a dbt project, generates a data catalog using an LLM, and writes it to a Markdown file.

```bash
data-scribe dbt [OPTIONS]
```

**Options:**

*   `--project-dir TEXT`: The path to the dbt project directory.
*   `--llm TEXT`: The name of the LLM profile to use from `config.yaml`.
*   `--config TEXT`: The path to the configuration file. Defaults to `config.yaml`.
*   `--output TEXT`: The name of the output file. Defaults to `dbt_catalog.md`.
*   `--update-yaml`: Update the dbt schema.yml file directly with the AI description.

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
  db: local_sqlite
  llm: local_ollama

db_connections:
  local_sqlite:
    type: "sqlite"     
    path: "test.db"    

  dev_postgres:
    type: "postgres"
    host: "localhost"
    port: 5432
    user: "admin"
    password: "password"
    dbname: "analytics_db"

llm_providers:
  openai_prod:
    provider: "openai"  
    model: "gpt-4o-mini"

  local_ollama:
    provider: "ollama"
    model: "granite3.3:2b"
    host: "http://localhost:11434"
```

## Supported Databases and LLMs

Data Scribe currently supports the following:

**Databases:**

*   SQLite
*   PostgreSQL

**LLMs:**

*   OpenAI
*   Ollama

## Extensibility

Data Scribe is designed to be easily extensible. To add support for a new database or LLM provider, you need to:

### Adding a New Database Connector

1.  **Create a new connector class** in the `data_scribe/components/db_connectors` directory that implements the `BaseConnector` interface from `data_scribe/core/interfaces.py`.
2.  **Register your new class** in the `DB_CONNECTOR_REGISTRY` in `data_scribe/core/factory.py`.

### Adding a New LLM Client

1.  **Create a new client class** in the `data_scribe/components/llm_clients` directory that implements the `BaseLLMClient` interface from `data_scribe/core/interfaces.py`.
2.  **Register your new class** in the `LLM_CLIENT_REGISTRY` in `data_scribe/core/factory.py`.

## Contributing

Contributions are welcome! Please feel free to open an issue or submit a pull request.