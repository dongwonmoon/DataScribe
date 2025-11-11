# ✍️ Data Scribe: AI-Powered Data Documentation

**Tired of writing data documentation? Let AI do it for you.**

Data Scribe is a CLI tool that scans your databases and dbt projects, uses AI to generate descriptions, and automatically updates your documentation.

---

## ✨ See it in Action

Stop manually updating YAML files or writing Markdown tables. Let `data-scribe` do the work in seconds.

| **Magically update dbt `schema.yml`** | **Instantly generate DB catalogs (w/ ERD)** |
| :---: | :---: |
| Run `data-scribe dbt --update` and watch AI fill in your missing descriptions, tags, and tests. | Point `data-scribe db` at a database and get a full Markdown catalog, complete with a Mermaid ERD. |
| ![dbt Workflow Demo](asset/dbt_demo.gif) | ![Database Scan Demo](asset/markdown_demo.gif) |

## 🚀 Quick Start (60 Seconds)

Get your first AI-generated catalog in less than a minute.

### 1. Install

Clone the repo and install dependencies.

```bash
git clone https://github.com/dongwonmoon/DataScribe.git
cd DataScribe
pip install -r requirements.txt
```

*(Note: For specific databases, install optional dependencies: `pip install -e ".[postgres, snowflake]"`)*
*(Note: To use the web server, also install server dependencies: `pip install "data-scribe[server]"`)*

### 2. Initialize

Run the interactive wizard. It will guide you through setting up your database and LLM, automatically creating `config.yaml` and a secure `.env` file for your API keys.

```bash
data-scribe init
```

### 3. Run!

You're all set.

**For a dbt project:**
(Make sure `dbt compile` has been run to create `manifest.json`)
```bash
# See what's missing (CI check)
data-scribe dbt --project-dir /path/to/your/dbt/project --check

# Let AI fix it
data-scribe dbt --project-dir /path/to/your/dbt/project --update

# Check for documentation drift against the live database
data-scribe dbt --project-dir /path/to/your/dbt/project --db your_db_profile --drift
```

**For a database:**
(Assuming you created an output profile named `my_markdown` during `init`)
```bash
data-scribe db --output my_markdown
```

---

## ✅ Key Features

-   **🤖 Automated Catalog Generation**: Scans live databases or dbt projects to generate documentation. Includes AI-generated table summaries for databases.
-   **✍️ LLM-Powered Descriptions**: Uses AI (OpenAI, Google, Ollama) to create meaningful business descriptions for tables, views, models, and columns.
-   **🧬 Deep dbt Integration**:
    -   **Direct YAML Updates**: Seamlessly updates your dbt `schema.yml` files with AI-generated content.
    -   **CI/CD Validation**: Use the `--check` flag in your CI pipeline to fail builds if documentation is outdated.
    -   **Interactive Updates**: Use the `--interactive` flag to review and approve AI-generated changes one by one.
    -   **Documentation Drift Detection**: Use the `--drift` flag to compare your existing documentation against the live database, catching descriptions that have become inconsistent with reality.
-   **🔒 Security-Aware**: The `init` wizard helps you store sensitive keys (passwords, API tokens) in a `.env` file, not in `config.yaml`.
-   **🔌 Extensible by Design**: A pluggable architecture supports multiple backends.
-   **🚀 Web API Server**: Launch a FastAPI server to trigger documentation workflows programmatically. Includes built-in API documentation via Swagger/ReDoc.

---

## 🛠️ Supported Backends

| Type | Supported Providers |
| :--- | :--- |
| **Databases** | `sqlite`, `postgres`, `mariadb`, `mysql`, `duckdb` (files, directories, S3), `snowflake` |
| **LLMs** | `openai`, `ollama`, `google` |
| **Outputs** | `markdown`, `dbt-markdown`, `json`, `confluence`, `notion`, `postgres-comment` |

---

## Command Reference

### `data-scribe init`

Runs the interactive wizard to create `config.yaml` and `.env` files. This is the recommended first step.

### `data-scribe db`

Scans a live database and generates a catalog.

-   `--db TEXT`: (Optional) The database profile from `config.yaml` to use. Overrides default.
-   `--llm TEXT`: (Optional) The LLM profile from `config.yaml` to use. Overrides default.
-   `--output TEXT`: (Required) The output profile from `config.yaml` to use.

### `data-scribe dbt`

Scans a dbt project's `manifest.json` file.

-   `--project-dir TEXT`: **(Required)** Path to the dbt project directory.
-   `--update`: (Flag) Directly update dbt `schema.yml` files.
-   `--check`: (Flag) Run in CI mode. Fails if documentation is outdated.
-   `--interactive`: (Flag) Run in interactive mode. Prompts user for each AI-generated change.
-   `--drift`: (Flag) Run in drift detection mode. Fails if existing documentation conflicts with the live database schema. Requires a `--db` profile.
-   `--llm TEXT`: (Optional) The LLM profile to use.
-   `--output TEXT`: (Optional) The output profile to use (if not using `--update`, `--check`, or `--interactive`).

**Note:** `--update`, `--check`, `--interactive`, and `--drift` flags are mutually exclusive. Choose only one.

### `data-scribe serve`

Launches the FastAPI web server.

-   `--host TEXT`: (Optional) The host to bind the server to. Defaults to `127.0.0.1`.
-   `--port INTEGER`: (Optional) The port to run the server on. Defaults to `8000`.

---

## 🚀 Web API Server

Data Scribe includes a built-in FastAPI web server that exposes the core workflows via a REST API. This is perfect for programmatic integration or for building a custom web UI.

**1. Launch the server:**
(Make sure you have installed the server dependencies: `pip install "data-scribe[server]"`)
```bash
data-scribe serve --host 0.0.0.0 --port 8000
```

**2. Explore the API:**
Once the server is running, you can access the interactive API documentation (powered by Swagger UI) at:
[http://localhost:8000/docs](http://localhost:8000/docs)

**3. Example: Get available profiles**
You can interact with the API using any HTTP client, like `curl`.
```bash
curl -X GET "http://localhost:8000/api/profiles" -H "accept: application/json"
```

This will return a JSON object listing all the database, LLM, and output profiles defined in your `config.yaml`.

**4. Example: Trigger a dbt workflow**
You can also trigger core workflows. For example, to run a `dbt --check` on a project:
```bash
curl -X POST "http://localhost:8000/api/run/dbt" \
-H "Content-Type: application/json" \
-d '{
  "dbt_project_dir": "/path/to/your/dbt/project",
  "check": true
}'
```

If the documentation is outdated, the API will return a `409 Conflict` status code, making it easy to integrate with CI/CD pipelines.

---

## 💡 Extensibility

Adding a new database, LLM, or writer is easy:

1.  Create a new class in the appropriate directory (e.g., `data_scribe/components/db_connectors`).
2.  Implement the base interface (e.g., `BaseConnector`).
3.  Register your new class in `data_scribe/core/factory.py`.

The `init` command and core logic will automatically pick up your new component.

## 🤝 Contributing

Contributions are welcome! Please feel free to open an issue or submit a pull request.
