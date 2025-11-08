# Data Scribe: AI-Powered Data Documentation

Data Scribe is a command-line tool that automates the generation of data catalogs for your database schemas and dbt projects. It inspects your data sources, extracts metadata, and leverages Large Language Models (LLMs) to produce clear, business-friendly documentation.

This tool streamlines the data documentation process, making it easier for analysts, data scientists, and business stakeholders to discover, understand, and trust their data assets.

## Key Features

- **Automated Catalog Generation**: Scan databases or dbt projects to automatically generate documentation.
- **LLM-Powered Descriptions**: Uses AI to create meaningful business descriptions for your models and columns.
- **dbt Integration**:
    - **Directly Update YAML**: Seamlessly update your dbt `schema.yml` files with AI-generated descriptions and tests.
    - **CI/CD Validation**: Use the `--check` flag in your CI pipeline to ensure documentation is always up-to-date and fails builds if it's not.
- **Broad Compatibility**:
    - **Supported Databases**: PostgreSQL, SQLite, MariaDB, and MySQL.
    - **Supported LLMs**: OpenAI (GPT series), Ollama (for local models), and Google Gemini.
- **Extensible by Design**: Easily add new database connectors or LLM clients to fit your stack.
- **Markdown Output**: Generates clean, readable Markdown files for easy sharing and version control.

## Getting Started

### Prerequisites

- Python 3.8+
- Git

### 1. Clone the Repository

```bash
git clone https://github.com/dongwonmoon/DataScribe.git
cd DataScribe
```

### 2. Install Dependencies

Install the required Python packages:

```bash
pip install -r requirements.txt
```

### 3. Configure API Keys (`.env`)

Create a `.env` file in the root of the project to store your secret API keys.

**Example:**
```env
# .env
OPENAI_API_KEY="sk-..."
GOOGLE_API_KEY="AIza..."
```

### 4. Configure Connections (`config.yaml`)

Create a `config.yaml` file to define your database connections and LLM providers. You can set default providers to use when no command-line options are specified.

**Example `config.yaml`:**
```yaml
# Set the default profiles to use if --db or --llm flags are not provided
default:
  db: dev_postgres
  llm: openai_prod

# Define all your database connection profiles
db_connections:
  local_sqlite:
    type: "sqlite"
    path: "jaffle_shop.db" # Path to your SQLite file

  dev_postgres:
    type: "postgres"
    host: "localhost"
    port: 5432
    user: "admin"
    password: "password"
    dbname: "analytics_db"

  prod_mariadb:
    type: "mariadb"
    host: "prod.db.example.com"
    port: 3306
    user: "readonly_user"
    password: "db_password"
    dbname: "production"

# Define all your LLM provider profiles
llm_providers:
  openai_prod:
    provider: "openai"
    model: "gpt-4o-mini"

  google_gemini:
    provider: "google"
    model: "gemini-1.5-flash"

  local_ollama:
    provider: "ollama"
    model: "llama3"
    host: "http://localhost:11434"
```

---
**Note on Secrets**: For sensitive values like passwords and API tokens, you can use environment variables. Data Scribe will automatically expand any value with the format `${VAR_NAME}`.

**Example:**
```yaml
  prod_mariadb:
    type: "mariadb"
    host: "prod.mariadb.com"
    user: "admin"
    password: "${MARIADB_PASSWORD}" # Loaded from environment variable
    dbname: "production_db"
```

## Usage

Data Scribe offers two main commands: `db` for databases and `dbt` for dbt projects.

### Scanning a Database (`db`)

This command scans a database schema and generates a Markdown data catalog.

```bash
python -m data_scribe.main db [OPTIONS]
```

**Options:**

- `--db TEXT`: The database profile from `config.yaml` to use. (Overrides default)
- `--llm TEXT`: The LLM profile from `config.yaml` to use. (Overrides default)
- `--config TEXT`: Path to the configuration file. (Default: `config.yaml`)
- `--output TEXT`: Name of the output Markdown file. (Default: `db_catalog.md`)

**Example:**
```bash
# Use the default db and llm profiles from config.yaml
python -m data_scribe.main db

# Specify a different database and output file
python -m data_scribe.main db --db prod_mariadb --output prod_catalog.md
```

### Scanning a dbt Project (`dbt`)

This command scans a dbt project, generates documentation, and can either create a Markdown file or update your `schema.yml` files directly.

```bash
python -m data_scribe.main dbt [OPTIONS]
```

**Options:**

- `--project-dir TEXT`: **(Required)** Path to the dbt project directory.
- `--llm TEXT`: The LLM profile to use. (Overrides default)
- `--output TEXT`: Name of the output Markdown file. (Default: `dbt_catalog.md`)
- `--update`: A flag to directly update dbt `schema.yml` files with AI-generated content.
- `--check`: A flag for CI/CD. The command fails if documentation is missing or outdated, preventing merges.

**Examples:**

1.  **Generate a Markdown catalog:**
    ```bash
    python -m data_scribe.main dbt --project-dir ./path/to/dbt/project
    ```

2.  **Directly update `schema.yml` files:**
    ```bash
    python -m data_scribe.main dbt --project-dir ./path/to/dbt/project --update
    ```

3.  **Run a CI check to enforce documentation:**
    ```bash
    python -m data_scribe.main dbt --project-dir ./path/to/dbt/project --check
    ```

## Extensibility

Data Scribe is designed for easy extension.

### Adding a New Database Connector

1.  Create a new connector class in `data_scribe/components/db_connectors` that implements the `BaseConnector` interface.
2.  Register the new class in the `DB_CONNECTOR_REGISTRY` in `data_scribe/core/factory.py`.

### Adding a New LLM Client

1.  Create a new client class in `data_scribe/components/llm_clients` that implements the `BaseLLMClient` interface.
2.  Register the new class in the `LLM_CLIENT_REGISTRY` in `data_scribe/core/factory.py`.

## Contributing

Contributions are welcome! Please feel free to open an issue or submit a pull request.
