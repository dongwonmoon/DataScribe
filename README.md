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
```

**For a database:**
(Assuming you created an output profile named `my_markdown` during `init`)
```bash
data-scribe db --output my_markdown
```

---

## ✅ Key Features

-   **🤖 Automated Catalog Generation**: Scans live databases or dbt projects to generate documentation.
-   **✍️ LLM-Powered Descriptions**: Uses AI (OpenAI, Google, Ollama) to create meaningful business descriptions for tables, views, models, and columns.
-   **🧬 Deep dbt Integration**:
    -   **Direct YAML Updates**: Seamlessly updates your dbt `schema.yml` files with AI-generated content.
    -   **CI/CD Validation**: Use the `--check` flag in your CI pipeline to fail builds if documentation is outdated.
-   **🔒 Security-Aware**: The `init` wizard helps you store sensitive keys (passwords, API tokens) in a `.env` file, not in `config.yaml`.
-   **🔌 Extensible by Design**: A pluggable architecture supports multiple backends.

---

## 🛠️ Supported Backends

| Type | Supported Providers |
| :--- | :--- |
| **Databases** | `sqlite`, `postgres`, `mariadb`, `mysql`, `duckdb`, `snowflake` |
| **LLMs** | `openai`, `ollama`, `google` |
| **Outputs** | `markdown`, `dbt-markdown`, `json`, `confluence` |

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
-   `--llm TEXT`: (Optional) The LLM profile to use.
-   `--output TEXT`: (Optional) The output profile to use (if not using `--update`).

---

## 💡 Extensibility

Adding a new database, LLM, or writer is easy:

1.  Create a new class in the appropriate directory (e.g., `data_scribe/components/db_connectors`).
2.  Implement the base interface (e.g., `BaseConnector`).
3.  Register your new class in `data_scribe/core/factory.py`.

The `init` command and core logic will automatically pick up your new component.

## 🤝 Contributing

Contributions are welcome! Please feel free to open an issue or submit a pull request.
