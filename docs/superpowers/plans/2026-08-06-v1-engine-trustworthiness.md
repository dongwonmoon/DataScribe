# V1 Engine Trustworthiness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the local database documentation engine (the `db` command) trustworthy and measurable enough to be SchemaScribe v1, per `docs/PRODUCT.md`.

**Architecture:** This plan is product-sequence step 1 ("local engine trustworthy and measurable"). It hardens trust boundaries (read-only enforcement, disclosure), fixes the profiling contract, makes output writes atomic, adds baseline-based change classification and a review loop, then qualifies connectors through a shared acceptance suite. No new dependencies; every change starts with a failing test that proves the current defect.

**Tech Stack:** Python 3.11/3.12, pytest (+ pytest-mock), Typer CLI, stdlib `sqlite3`/`os`/`tempfile`. Drivers already in extras: `psycopg2`, `mysql-connector-python`, `duckdb`, `snowflake-connector-python`.

## Global Constraints

- Verification gate: `./scripts/verify.sh` must pass after every slice (72 tests today + new tests).
- Red test first: each task's first step is a test that fails on the current code — that test is the permanent regression guard.
- No raw-row or sample-value collection or transmission (PRODUCT.md:62-64). Aggregate stats only.
- No new runtime dependencies; stdlib only for atomic writes (`tempfile`, `os.replace`).
- Read-only core path: writers that mutate a database (PostgresCommentWriter) are out of v1 scope (PRODUCT.md:69-70).
- The `db` workflow's data boundary: tables, columns, types, PKs, FKs, and exactly three aggregate stats (`null_ratio`, `distinct_count`, `is_unique`).
- Commit style: conventional title + body "문제 상황 / 증거 / 해결 / 기각한 대안" (see AGENTS.md).
- Narrow topic branches; merge slice to parent only after its gate passes.

---

# Gap Audit (Evidence)

Audit of contract `docs/PRODUCT.md` vs current source on `dev` (4 independent source audits, all claims verified against source):

| # | Contract clause | State | Evidence |
|---|---|---|---|
| G1 | Read-only connection on core path (PRODUCT.md:43,61) | **Missing on 4/5 connectors** | `sqlite3.connect(db_path)` no `mode=ro` (sqlite_connector.py:57); postgres no `default_transaction_read_only` (postgres_connector.py:61-67); mariadb no `SET SESSION TRANSACTION READ ONLY` (mariadb_connector.py:72-78); snowflake no session hardening (snowflake_connector.py:69-76). Only DuckDB file mode enforces it (duckdb_connector.py:76-89). |
| G2 | Aggregate profiling without source rows (PRODUCT.md:44-45,62-64) | **Implemented** | All queries hit catalog/PRAGMA tables; profile = `COUNT(*)`, `SUM(CASE IS NULL)`, `COUNT(DISTINCT)` only (sql_base_connector.py:261-267, sqlite_connector.py:214-220, duckdb_connector.py:223-229). No `SELECT` of source rows anywhere. |
| G3 | Disclosure before external LLM (PRODUCT.md:46-47,66-67) | **Missing** | No `--dry-run`/preview/confirm anywhere; `db` command has no such flag (app.py:301-346). What leaves the machine: identifiers, types, view SQL verbatim, 3 stats (catalog_generator.py:134-170,199-203; prompts.py:12-38,129-137). |
| G4 | Interface vs behavior drift | **Partial** | `get_column_profile` docstring documents min/max/avg (interfaces.py:139-146) that no connector returns; connectors return `"N/A"` strings where float/bool documented (sqlite_connector.py:257-261, sql_base_connector.py:305-309). |
| G5 | PK accuracy | **Partial** | DuckDB `is_pk` always False (`row[3] == "PRI"` on DESCRIBE that yields NULL, duckdb_connector.py:186); SQLite marks only first column of composite PK (`row[5] == 1`, sqlite_connector.py:122); SqlBase returns int not bool (sql_base_connector.py:150). |
| G6 | Atomic writes / failure preserves last output (PRODUCT.md:53) | **Missing** | All file writers `open(path, "w")` truncate-then-stream (markdown_writer.py:95, json_writer.py:57, dbt_markdown_writer.py:62, mermaid_writer.py:66). Generation failure preserves old file only incidentally (writer runs after full generation, db_workflow.py:94-113); write-phase failure destroys it. No `os.replace` anywhere. |
| G7 | Change classification unchanged/added/removed/structurally-changed (PRODUCT.md:51-52) | **Missing** | No persisted baseline, no comparison in `db` workflow. dbt `--check` detects added only, never removed (dbt_yaml_writer.py:100-104). |
| G8 | Review before replacement (PRODUCT.md:48-49) | **Missing for `db`** | Only dbt `--interactive` has accept/edit/skip (dbt_yaml_writer.py:398-447). `db` has no `--check`/`--interactive`. |
| G9 | Shared acceptance suite per adapter (PRODUCT.md:55-57) | **Absent** | No parametrized suite; only SQLite has real-engine tests; postgres/mariadb/snowflake mock connect params only; duckdb mocks the library (tests/unit/db_connectors/). No connector can claim v1 support today. |
| G10 | Test hygiene | **Defective** | Dead conftest fixture patches nonexistent modules (`schema_scribe.core.db_workflow.init_llm`, tests/conftest.py:42-45); workflow tests mock `get_tables` returning dicts while the real connector returns `List[str]` (test_db_workflow.py:28-32 vs sqlite_connector.py:68) — real composition never exercised. |

**In-scope adapter defect noted, deferred:** PostgresCommentWriter is dead-on-arrival because `generate_catalog()` closes the connection before `writer.write()` runs (db_workflow.py:71-76,113; postgres_comment_writer.py:73-76). It is a mutating writer, out of v1 scope; reworked when adapters are qualified.

**Deferred to product-evaluation work (tracked, not in this plan):** eval fixture + human review rubric, benchmark harness (query count / elapsed / LLM calls), drift-in-CI support, hosted demo. See docs/TESTING.md:30-34 and CURRENT_FOCUS.md.

---

## Slice 1: Read-Only Enforcement (G1)

### Task 1.1: SQLite read-only connection

**Files:**
- Modify: `schema_scribe/components/db_connectors/sqlite_connector.py:57`
- Test: `schema_scribe/tests/unit/db_connectors/test_sqlite_connector.py`

**Interfaces:**
- Consumes: `connect(self, db_params: Dict[str, Any])` — unchanged signature.
- Produces: no external contract change; behavior: connection opened with `mode=ro` URI.

- [ ] **Step 1: Write the failing test**

Append to `test_sqlite_connector.py` (pattern: existing tests use `tmp_path` and a seeded fixture DB — reuse `create_sqlite_db` helper if present, otherwise seed inline):

```python
def test_connection_is_read_only(tmp_path):
    db_path = tmp_path / "ro.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    conn.commit()
    conn.close()
    connector = SQLiteConnector()
    connector.connect({"db_path": str(db_path)})
    with pytest.raises(sqlite3.OperationalError, match="readonly"):
        connector.connection.execute("CREATE TABLE t2 (id INTEGER)")
    connector.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest schema_scribe/tests/unit/db_connectors/test_sqlite_connector.py::test_connection_is_read_only -v`
Expected: FAIL — write succeeds (no `readonly` error raised).

- [ ] **Step 3: Minimal implementation**

In `sqlite_connector.py`, replace the connect call:

```python
self.connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
```

Keep the existing `finally`/error handling. Note: a missing file now raises `OperationalError: unable to open database file` — add `assert os.path.exists(db_path)` (or a clear error message) before connecting, wrapped in the existing error-handling path.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest schema_scribe/tests/unit/db_connectors/test_sqlite_connector.py -v`
Expected: PASS (all sqlite connector tests, old + new).

- [ ] **Step 5: Commit**

```bash
git add schema_scribe/components/db_connectors/sqlite_connector.py schema_scribe/tests/unit/db_connectors/test_sqlite_connector.py
git commit -m "fix: enforce read-only SQLite connections

문제 상황: v1 계약은 코어 경로 읽기 전용을 요구하나 sqlite 커넥터가 쓰기 가능 연결을 열었음.
증거: gap audit G1 — sqlite3.connect(db_path)에 mode=ro 없음 (sqlite_connector.py:57); 쓰기 시도가 성공하는 red 테스트로 재현.
해결: mode=ro URI + 파일 존재 검증.
기각한 대안: PRAGMA query_only(연결 후 설정이라 오픈 순간 쓰기 위험이 남음)."
```

### Task 1.2: PostgreSQL read-only connection

**Files:**
- Modify: `schema_scribe/components/db_connectors/postgres_connector.py:61-67`
- Test: `schema_scribe/tests/unit/db_connectors/test_postgres_connector.py`

**Interfaces:**
- Consumes: `connect(self, db_params)`; existing test asserts `psycopg2.connect` called with expected kwargs.
- Produces: connect kwargs now include `options="-c default_transaction_read_only=on"`.

- [ ] **Step 1: Write the failing test**

In `test_postgres_connector.py`, extend the existing connect-params test (assert on the mocked `psycopg2.connect` call):

```python
def test_connect_enforces_read_only(mocker):
    mock_connect = mocker.patch("psycopg2.connect")
    connector = PostgresConnector()
    connector.connect({"host": "h", "user": "u", "password": "p", "dbname": "d"})
    kwargs = mock_connect.call_args.kwargs
    assert kwargs["options"] == "-c default_transaction_read_only=on"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest schema_scribe/tests/unit/db_connectors/test_postgres_connector.py::test_connect_enforces_read_only -v`
Expected: FAIL — `options` key missing.

- [ ] **Step 3: Minimal implementation**

Add to the connect kwargs dict in `postgres_connector.py`:

```python
"options": "-c default_transaction_read_only=on",
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest schema_scribe/tests/unit/db_connectors/test_postgres_connector.py -v`
Expected: PASS.

- [ ] **Step 5: Commit** (same body format as Task 1.1; title `fix: enforce read-only PostgreSQL connections`)

### Task 1.3: MariaDB read-only connection

**Files:**
- Modify: `schema_scribe/components/db_connectors/mariadb_connector.py:72-78`
- Test: `schema_scribe/tests/unit/db_connectors/test_mariadb_connector.py`

**Interfaces:**
- Consumes: `connect(self, db_params)`.
- Produces: after connect, issues `SET SESSION TRANSACTION READ ONLY`.

- [ ] **Step 1: Write the failing test**

```python
def test_connect_enforces_read_only(mocker):
    mock_cursor = mocker.MagicMock()
    mock_conn = mocker.MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mocker.patch("mysql.connector.connect", return_value=mock_conn)
    connector = MariaDBConnector()
    connector.connect({"host": "h", "user": "u", "password": "p", "database": "d"})
    mock_cursor.execute.assert_called_once_with("SET SESSION TRANSACTION READ ONLY")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest schema_scribe/tests/unit/db_connectors/test_mariadb_connector.py::test_connect_enforces_read_only -v`
Expected: FAIL — cursor never called.

- [ ] **Step 3: Minimal implementation**

After the `mysql.connector.connect(...)` call in `mariadb_connector.py`, add:

```python
cursor = self.connection.cursor()
cursor.execute("SET SESSION TRANSACTION READ ONLY")
cursor.close()
```

(If the connector already creates a cursor elsewhere, reuse that path.)

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest schema_scribe/tests/unit/db_connectors/test_mariadb_connector.py -v`
Expected: PASS.

- [ ] **Step 5: Commit** (title `fix: enforce read-only MariaDB connections`)

### Task 1.4: DuckDB read-only coverage + Snowflake documented exception

**Files:**
- Modify: `schema_scribe/components/db_connectors/snowflake_connector.py:69-76` (docstring only)
- Test: `schema_scribe/tests/unit/db_connectors/test_duckdb_connector.py` (add), `schema_scribe/tests/unit/db_connectors/test_snowflake_connector.py` (add)

**Interfaces:**
- Produces: DuckDB `.db` connect keeps `read_only=True` (existing); a regression test locks it. Snowflake: read-only is enforced by account role grants (no `SET TRANSACTION READ ONLY` support) — recorded as a documented exception in the connector docstring.

- [ ] **Step 1: Write the failing test (duckdb regression lock)**

```python
def test_file_connection_is_read_only(mocker):
    mock_connect = mocker.patch("duckdb.connect")
    connector = DuckDBConnector()
    connector.connect({"database": "/tmp/x.duckdb"})
    assert mock_connect.call_args.kwargs.get("read_only") is True
```

Run it — if it passes immediately, add a write-attempt assertion instead if the real duckdb is importable in the test env; otherwise keep the connect-params lock and mark the step as a regression guard (record the result in the commit body).

- [ ] **Step 2: Add Snowflake exception documentation**

In `snowflake_connector.py` docstring for `connect`, add: "Read-only enforcement relies on the account role's grants; Snowflake sessions do not support a server-side read-only transaction mode."

- [ ] **Step 3: Run the connector test suites**

Run: `pytest schema_scribe/tests/unit/db_connectors/ -v`
Expected: PASS.

- [ ] **Step 4: Commit** (title `test: lock DuckDB read-only; document Snowflake exception`)

### Slice 1 gate

- [ ] Run `./scripts/verify.sh` — expected: PASS (72 + new tests).
- [ ] Merge to parent branch (ff) with `git log` review for missed commits.

---

## Slice 2: Profiling Contract Fixes (G4, G5)

### Task 2.1: Align `get_column_profile` interface with privacy boundary

**Files:**
- Modify: `schema_scribe/core/interfaces.py:137-146`
- Modify: `schema_scribe/components/db_connectors/sql_base_connector.py:292-309`, `sqlite_connector.py:244-261`, `duckdb_connector.py:253-266`
- Modify: `schema_scribe/services/catalog_generator.py:69-74`
- Test: `schema_scribe/tests/unit/db_connectors/test_sql_base_connector.py` (add), `schema_scribe/tests/unit/db_connectors/test_sqlite_connector.py` (extend)

**Interfaces:**
- Consumes: current `get_column_profile` returning `{"null_ratio": ..., "distinct_count": ..., "is_unique": ..., "N/A" strings on failure}`.
- Produces: interface documents exactly `null_ratio: float | None`, `distinct_count: int | None`, `is_unique: bool | None`; failure returns `None` values instead of `"N/A"` strings.

- [ ] **Step 1: Write the failing test**

```python
def test_profile_returns_none_on_query_failure(mocker):
    connector = SQLiteConnector()
    connector.connection = mocker.MagicMock()
    connector.connection.cursor.return_value.execute.side_effect = sqlite3.Error("boom")
    profile = connector.get_column_profile("t", "c")
    assert profile["null_ratio"] is None
    assert profile["distinct_count"] is None
    assert profile["is_unique"] is None
```

- [ ] **Step 2: Run test to verify it fails**

Expected: FAIL — current code returns `"N/A"` strings.

- [ ] **Step 3: Minimal implementation**

In each connector's `get_column_profile` failure branch, return `None` for each stat instead of `"N/A"`; in `sql_base_connector.py`/`duckdb_connector.py` likewise. Update `interfaces.py:139-146` docstring: drop `min`/`max`/`avg`, document `float | None` etc. In `catalog_generator.py:_format_profile_stats`, format `None` as `"N/A"` (the current default already does — verify and keep).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest schema_scribe/tests/unit/db_connectors/ -v`
Expected: PASS.

- [ ] **Step 5: Commit** (title `fix: align profile stats contract with privacy boundary`)

### Task 2.2: DuckDB primary-key detection

**Files:**
- Modify: `schema_scribe/components/db_connectors/duckdb_connector.py:180-189`
- Test: `schema_scribe/tests/unit/db_connectors/test_duckdb_connector.py` (add)

**Interfaces:**
- Consumes: `get_columns(table_name)` returning per-column dicts with `is_pk: bool`.
- Produces: `is_pk` computed from `duckdb_constraints()` instead of `DESCRIBE`.

- [ ] **Step 1: Write the failing test**

Real-duckdb test (duckdb is a real dependency; if the test env imports it, use it):

```python
def test_is_pk_detected_from_constraints(tmp_path):
    db = duckdb.connect(str(tmp_path / "pk.db"))
    db.execute("CREATE TABLE t (a INTEGER, b INTEGER, PRIMARY KEY (a, b))")
    db.close()
    connector = DuckDBConnector()
    connector.connect({"database": str(tmp_path / "pk.db")})
    cols = {c["name"]: c for c in connector.get_columns("t")}
    assert cols["a"]["is_pk"] is True
    assert cols["b"]["is_pk"] is True
    connector.close()
```

If the suite cannot import duckdb in CI (verify first), fall back to a mocked test asserting `duckdb_constraints()` is queried and its result drives `is_pk` — choose per environment evidence, and say which in the commit body.

- [ ] **Step 2: Run test to verify it fails**

Expected: FAIL — `is_pk` is False for both.

- [ ] **Step 3: Minimal implementation**

Replace the DESCRIBE-based PK check: query `duckdb_constraints()` (matching `constraint_type = 'PRIMARY KEY'`), collect the constrained column names, set `is_pk = name in pk_columns`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest schema_scribe/tests/unit/db_connectors/test_duckdb_connector.py -v`
Expected: PASS.

- [ ] **Step 5: Commit** (title `fix: detect DuckDB primary keys via constraints`)

### Task 2.3: SQLite composite-PK and SqlBase boolean coercion

**Files:**
- Modify: `schema_scribe/components/db_connectors/sqlite_connector.py:122`
- Modify: `schema_scribe/components/db_connectors/sql_base_connector.py:150`
- Test: `schema_scribe/tests/unit/db_connectors/test_sqlite_connector.py` (extend), `schema_scribe/tests/unit/db_connectors/test_sql_base_connector.py` (extend)

**Interfaces:**
- Produces: SQLite marks every composite-PK column (`row[5] > 0`); SqlBase returns bool (`row[3] is truthy and not "0"`), not int.

- [ ] **Step 1: Write the failing test**

```python
def test_composite_pk_all_columns_marked(tmp_path):
    db_path = tmp_path / "pk.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE t (a INTEGER, b INTEGER, PRIMARY KEY (a, b))")
    conn.commit()
    conn.close()
    connector = SQLiteConnector()
    connector.connect({"db_path": str(db_path)})
    cols = {c["name"]: c for c in connector.get_columns("t")}
    assert cols["a"]["is_pk"] is True
    assert cols["b"]["is_pk"] is True
    connector.close()
```

- [ ] **Step 2: Run test to verify it fails**

Expected: FAIL — `is_pk` True only for `a`.

- [ ] **Step 3: Minimal implementation**

`sqlite_connector.py`: change `row[5] == 1` to `row[5] > 0`. `sql_base_connector.py`: coerce the PK flag to `bool(row[3])` (verify the MariaDB CASE TRUE behavior and use `bool(...)`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest schema_scribe/tests/unit/db_connectors/test_sqlite_connector.py schema_scribe/tests/unit/db_connectors/test_sql_base_connector.py -v`
Expected: PASS.

- [ ] **Step 5: Commit** (title `fix: correct composite PK marking and bool coercion`)

### Slice 2 gate

- [ ] Run `./scripts/verify.sh` — expected: PASS.
- [ ] Merge to parent branch (ff).

---

## Slice 3: Disclosure Before External LLM (G3)

### Task 3.1: `db --dry-run` payload manifest

**Files:**
- Modify: `schema_scribe/app.py:301-346` (db command)
- Modify: `schema_scribe/workflows/db_workflow.py` (new method)
- Test: `schema_scribe/tests/integration/test_db_workflow.py` (add), `schema_scribe/tests/unit/test_app_cli.py` if a CLI test harness exists (check; otherwise integration only)

**Interfaces:**
- Consumes: `DbWorkflow(db_connector, llm_client, writer, ...)`.
- Produces: `DbWorkflow.dry_run() -> None` — collects schema metadata + profile stats, prints a manifest (db profile, table/column/view counts, stat counts, target provider), calls NO LLM, writes nothing, closes the connection. New `--dry-run` flag on `db`.

- [ ] **Step 1: Write the failing test**

```python
def test_dry_run_prints_manifest_without_llm_or_write(capsys):
    connector = MagicMock(spec=BaseConnector)
    connector.get_tables.return_value = ["users"]
    connector.get_columns.return_value = [{"name": "id", "type": "INTEGER"}]
    connector.get_views.return_value = []
    connector.get_foreign_keys.return_value = []
    connector.get_column_profile.return_value = {"null_ratio": 0.0, "distinct_count": 10, "is_unique": True}
    llm = MagicMock(spec=BaseLLMClient)
    writer = MagicMock(spec=BaseWriter)
    wf = DbWorkflow(connector, llm, writer, db_profile_name="mydb")
    wf.dry_run()
    assert llm.get_description.call_count == 0
    writer.write.assert_not_called()
    out = capsys.readouterr().out
    assert "mydb" in out and "users" in out and "id" in out
    connector.close.assert_called_once()
```

- [ ] **Step 2: Run test to verify it fails**

Expected: FAIL — `DbWorkflow.dry_run` does not exist.

- [ ] **Step 3: Minimal implementation**

In `db_workflow.py`, add `dry_run()` that reuses the same metadata collection as `CatalogGenerator` but only prints counts + names (table names, column name:type lists, view count) and the profile stat keys, then closes the connection. Wire `--dry-run` in `app.py` db command: when set, build workflow without writer and call `dry_run()` (mirroring the existing writer-null path at app.py:301-346).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest schema_scribe/tests/integration/test_db_workflow.py -v`
Expected: PASS.

- [ ] **Step 5: Commit** (title `feat: add db --dry-run disclosure manifest`)

### Task 3.2: Run-start transmission disclosure line

**Files:**
- Modify: `schema_scribe/services/catalog_generator.py` (before first `get_description` call, ~line 139)
- Test: `schema_scribe/tests/integration/test_db_workflow.py` (extend) or unit test on CatalogGenerator

**Interfaces:**
- Consumes: `CatalogGenerator(db_connector, llm_client)`.
- Produces: before the first LLM call, a log/info line: `Sending <n> table summaries and <m> column descriptions to provider '<provider>'. Metadata: tables=<t>, columns=<c>, views=<v>.`

- [ ] **Step 1: Write the failing test**

Extend the existing `test_db_workflow.py` orchestration test (it asserts `call_count == 12`): assert the logger (or a returned manifest) records the disclosure line before any `get_description` call. Prefer asserting on a structured return value over log capture if simpler: have `CatalogGenerator.generate_catalog` accept a `disclosure_callback` or return a `payload_summary` alongside the catalog — pick the cheapest that keeps the existing call-count test green.

- [ ] **Step 2: Run test to verify it fails**

Expected: FAIL — no disclosure emitted.

- [ ] **Step 3: Minimal implementation**

In `catalog_generator.py`, before the description loop, compute counts and emit the disclosure (log at INFO via `get_logger`, plus include the summary in the returned catalog dict under `"payload_summary"` if the structured approach was chosen).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest schema_scribe/tests/integration/test_db_workflow.py -v`
Expected: PASS.

- [ ] **Step 5: Commit** (title `feat: disclose external LLM payload before execution`)

### Slice 3 gate

- [ ] Run `./scripts/verify.sh` — expected: PASS.
- [ ] Merge to parent branch (ff).

---

## Slice 4: Output Safety (G6)

### Task 4.1: Atomic file writes for Markdown and JSON writers

**Files:**
- Modify: `schema_scribe/components/writers/markdown_writer.py:90-150`
- Modify: `schema_scribe/components/writers/json_writer.py:53-68`
- Test: `schema_scribe/tests/unit/writers/test_markdown_writer.py`, `schema_scribe/tests/unit/writers/test_json_writer.py`

**Interfaces:**
- Produces: both writers render the complete output in memory, write to a temp file in the target directory (`tempfile.NamedTemporaryFile(mode="w", dir=target_dir, delete=False)`), then `os.replace(tmp, target)`; `IOError` handling unchanged.

- [ ] **Step 1: Write the failing test**

```python
def test_failed_write_preserves_previous_output(tmp_path, monkeypatch):
    target = tmp_path / "catalog.md"
    target.write_text("PREVIOUS CONTENT")
    writer = MarkdownWriter()
    def boom(*args, **kwargs):
        raise IOError("disk full")
    monkeypatch.setattr(writer, "write", boom)  # or patch the underlying f.write
    with pytest.raises(IOError):
        writer.write({...}, output_filename=str(target), ...)
    assert target.read_text() == "PREVIOUS CONTENT"
```

(The exact catalog-dict shape comes from the existing markdown writer test — reuse its fixture. If patching `write` masks the mechanics, patch `json.dump`/`f.write` at the streaming call site instead; the assertion is what matters: old content intact after failure.)

- [ ] **Step 2: Run test to verify it fails**

Expected: FAIL — `"PREVIOUS CONTENT"` lost (file truncated).

- [ ] **Step 3: Minimal implementation**

Both writers: build the full string (markdown already streams — accumulate into a `StringIO` or list then join; json uses `json.dumps(catalog_data, indent=2)`), then atomic write:

```python
import os, tempfile
fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(os.path.abspath(output_filename)), suffix=".tmp")
with os.fdopen(fd, "w", encoding="utf-8") as f:
    f.write(content)
os.replace(tmp_path, output_filename)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest schema_scribe/tests/unit/writers/test_markdown_writer.py schema_scribe/tests/unit/writers/test_json_writer.py -v`
Expected: PASS.

- [ ] **Step 5: Commit** (title `fix: make file writers atomic`)

### Task 4.2: Failure-preservation integration tests

**Files:**
- Test: `schema_scribe/tests/integration/test_db_workflow.py` (add)

**Interfaces:**
- Consumes: `DbWorkflow.run()` with injected fakes.

- [ ] **Step 1: Write the failing test**

```python
def test_generation_failure_preserves_output(tmp_path):
    connector = MagicMock(spec=BaseConnector)
    connector.get_tables.return_value = ["t"]
    connector.get_columns.return_value = [{"name": "c", "type": "INTEGER", "description": "", "is_nullable": True, "is_pk": False}]
    llm = MagicMock(spec=BaseLLMClient)
    llm.get_description.side_effect = LLMClientError("boom")
    target = tmp_path / "catalog.md"
    target.write_text("PREVIOUS")
    writer = MarkdownWriter()
    wf = DbWorkflow(connector, llm, writer, writer_params={"output_filename": str(target)})
    with pytest.raises(LLMClientError):
        wf.run()
    assert target.read_text() == "PREVIOUS"
```

- [ ] **Step 2: Run test to verify it fails**

Expected: FAIL — `LLMClientError` escapes `DbWorkflow.run` (it catches only KeyError/ValueError/IOError); the file is untouched, so this test may pass immediately. If it passes, keep it as the regression lock and note in the commit body that ordering already protected generation failures; the destructive case is covered by Task 4.1's mid-write test.

- [ ] **Step 3: Minimal implementation**

None expected — verification task. If the test fails, fix the failure path (writer invocation must not run before generation completes; it already doesn't, db_workflow.py:94-113).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest schema_scribe/tests/integration/test_db_workflow.py -v`
Expected: PASS.

- [ ] **Step 5: Commit** (title `test: lock failure-preserves-output semantics`)

### Slice 4 gate

- [ ] Run `./scripts/verify.sh` — expected: PASS.
- [ ] Merge to parent branch (ff).

---

## Slice 5: Baseline And Change Classification (G7)

### Task 5.1: Schema state sidecar file

**Files:**
- Create: `schema_scribe/services/schema_state.py`
- Modify: `schema_scribe/workflows/db_workflow.py:78-113`
- Test: `schema_scribe/tests/unit/test_schema_state.py` (new)

**Interfaces:**
- Consumes: catalog dict from `CatalogGenerator`.
- Produces: `class SchemaState` with `load(path) -> dict | None`, `save(catalog, path)`, `snapshot(catalog) -> dict` where snapshot = `{"tables": {name: {"columns": {col: type}, "pk": [cols], "fks": [...]}}, "views": [names], "generated_at": iso}`. Sidecar path convention: `<output_filename>.schema-state.json`.

- [ ] **Step 1: Write the failing test**

```python
def test_roundtrip_snapshot(tmp_path):
    path = tmp_path / "catalog.md.schema-state.json"
    catalog = {"tables": [{"name": "t", "columns": [{"name": "c", "type": "INTEGER", "is_pk": True}]}], "views": []}
    SchemaState.save(SchemaState.snapshot(catalog), str(path))
    loaded = SchemaState.load(str(path))
    assert loaded["tables"]["t"]["columns"] == {"c": "INTEGER"}
    assert loaded["tables"]["t"]["pk"] == ["c"]
```

- [ ] **Step 2: Run test to verify it fails**

Expected: FAIL — module does not exist.

- [ ] **Step 3: Minimal implementation**

`schema_state.py` per the Interfaces block (pure functions; `json` stdlib). In `db_workflow.run()`, after a successful `writer.write(...)`, save the snapshot next to the output file (requires the workflow to know the output filename — derive from `writer_params`; if absent, skip state save).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest schema_scribe/tests/unit/test_schema_state.py -v`
Expected: PASS.

- [ ] **Step 5: Commit** (title `feat: persist schema state sidecar after successful run`)

### Task 5.2: Change classifier

**Files:**
- Modify: `schema_scribe/services/schema_state.py`
- Test: `schema_scribe/tests/unit/test_schema_state.py` (extend)

**Interfaces:**
- Consumes: previous snapshot (Task 5.1) + current snapshot.
- Produces: `classify(prev: dict | None, curr: dict) -> {"added": [...], "removed": [...], "structurally_changed": [...]}` where structural change = column type change or PK/FK set change or column added/removed within an existing table; unchanged tables appear in neither list.

- [ ] **Step 1: Write the failing test**

```python
def test_classify_categories():
    prev = SchemaState.snapshot({"tables": [
        {"name": "keep", "columns": [{"name": "c", "type": "INTEGER", "is_pk": True}], "views": []},
        {"name": "drop", "columns": [], "views": []},
    ]})
    curr = SchemaState.snapshot({"tables": [
        {"name": "keep", "columns": [{"name": "c", "type": "TEXT", "is_pk": True}], "views": []},
        {"name": "new", "columns": [], "views": []},
    ]})
    r = SchemaState.classify(prev, curr)
    assert r["added"] == ["new"]
    assert r["removed"] == ["drop"]
    assert r["structurally_changed"] == ["keep"]
```

- [ ] **Step 2: Run test to verify it fails**

Expected: FAIL — `classify` does not exist.

- [ ] **Step 3: Minimal implementation**

Add `classify(prev, curr)` to `schema_state.py` per the Interfaces block.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest schema_scribe/tests/unit/test_schema_state.py -v`
Expected: PASS.

- [ ] **Step 5: Commit** (title `feat: classify schema changes across runs`)

### Slice 5 gate

- [ ] Run `./scripts/verify.sh` — expected: PASS.
- [ ] Merge to parent branch (ff).

---

## Slice 6: Review Loop (G8)

### Task 6.1: `db --check` diff gate

**Files:**
- Modify: `schema_scribe/app.py:301-346` (db command flag)
- Modify: `schema_scribe/workflows/db_workflow.py`
- Test: `schema_scribe/tests/integration/test_db_workflow.py` (add)

**Interfaces:**
- Consumes: `SchemaState` (Slice 5), rendered output string.
- Produces: `DbWorkflow.check() -> bool` — runs generation (no write), classifies against the sidecar if present, prints per-object status (`unchanged`/`added`/`removed`/`structurally-changed`), prints a unified diff of new render vs existing output file, returns True if anything would change. `db --check` exits 1 on changes (mirror dbt `--check` semantics, dbt_workflow.py:187-195).

- [ ] **Step 1: Write the failing test**

```python
def test_check_reports_added_table_and_exits_changed():
    connector = MagicMock(spec=BaseConnector)
    connector.get_tables.return_value = ["t"]
    ...  # same metadata stubs as existing workflow tests
    wf = DbWorkflow(connector, llm, writer=None, db_profile_name="mydb")
    wf.render_catalog = lambda: {"tables": [{"name": "t", ...}]}
    assert wf.check() is False  # with no prior state, nothing to compare → clean? define below
```

Choose semantics and assert them: with no sidecar and no existing file, `check()` returns True ("no existing documentation") and exits 1 — match this in the test; with a sidecar, classification drives the result.

- [ ] **Step 2: Run test to verify it fails**

Expected: FAIL — `check` does not exist.

- [ ] **Step 3: Minimal implementation**

`db_workflow.check()` per Interfaces block; `--check` flag in app.py that builds the workflow with no writer and calls `check()`, exiting 1 when it returns True.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest schema_scribe/tests/integration/test_db_workflow.py -v`
Expected: PASS.

- [ ] **Step 5: Commit** (title `feat: add db --check diff gate`)

### Task 6.2: `db --interactive` accept/edit/reject review

**Files:**
- Modify: `schema_scribe/workflows/db_workflow.py`
- Test: `schema_scribe/tests/integration/test_db_workflow.py` (add)

**Interfaces:**
- Consumes: catalog dict from `CatalogGenerator`.
- Produces: `DbWorkflow.run_interactive()` — after generation, for each table summary and each column description, prompts the user (typer.prompt) to accept / edit / reject; rejected descriptions are removed from the catalog; edited values replace them; then writes. Mirrors dbt `_prompt_user_for_change` (dbt_yaml_writer.py:412-447).

- [ ] **Step 1: Write the failing test**

```python
def test_interactive_applies_edits_and_drops_rejects(monkeypatch):
    connector = MagicMock(spec=BaseConnector)
    ...
    writer = MagicMock(spec=BaseWriter)
    wf = DbWorkflow(connector, llm, writer, ...)
    answers = iter(["accept", "edit:NEW DESC", "reject"])
    monkeypatch.setattr(typer, "prompt", lambda *a, **k: next(answers))
    wf.run_interactive()
    written = writer.write.call_args.args[0]
    ...  # assert the edited description present, rejected one absent
```

- [ ] **Step 2: Run test to verify it fails**

Expected: FAIL — `run_interactive` does not exist.

- [ ] **Step 3: Minimal implementation**

`run_interactive()` per Interfaces block; `--interactive` flag in app.py (mutually exclusive with `--check`, matching the dbt flag exclusivity at app.py:373-392).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest schema_scribe/tests/integration/test_db_workflow.py -v`
Expected: PASS.

- [ ] **Step 5: Commit** (title `feat: add db --interactive review loop`)

### Slice 6 gate

- [ ] Run `./scripts/verify.sh` — expected: PASS.
- [ ] Merge to parent branch (ff).

---

## Slice 7: Shared Acceptance Suite And Composition (G9, G10)

### Task 7.1: Shared connector acceptance suite

**Files:**
- Create: `schema_scribe/tests/acceptance/conftest.py`
- Create: `schema_scribe/tests/acceptance/test_shared_acceptance.py`
- Test: the new suite itself

**Interfaces:**
- Consumes: existing connector implementations.
- Produces: `pytest.mark.parametrize("connector_id", ["sqlite", "duckdb"])` real-engine suite (in-memory/temp-file fixtures) asserting the full lifecycle identically: connect → get_tables → get_columns (types, `is_pk`) → get_views → get_foreign_keys → profile (3 stats, no values) → read-only enforcement → close. Postgres/MariaDB/Snowflake get driver-mocked variants of the same assertions (params-check depth), marked so their v1 qualification is explicit: a connector is v1-supported only when it passes the real-engine suite (PRODUCT.md:55-57).

- [ ] **Step 1: Write the acceptance tests**

Same fixture DB schema (e.g., two tables with a PK + FK, one view) built per engine; identical assertions per connector.

- [ ] **Step 2: Run to verify sqlite + duckdb pass, others are driver-mocked**

Run: `pytest schema_scribe/tests/acceptance -v`
Expected: sqlite/duckdb PASS against real engines; mocked variants PASS.

- [ ] **Step 3: Minimal implementation**

Fixture factory in `conftest.py` keyed by connector id; parametrize over `["sqlite", "duckdb"]` for the real-engine tier.

- [ ] **Step 4: Record v1 qualification state**

Document in the acceptance test docstring: v1-supported today = sqlite, duckdb (real-engine suite passing). Postgres/MariaDB/Snowflake remain unqualified until a real-engine acceptance run is executed and recorded.

- [ ] **Step 5: Commit** (title `test: add shared connector acceptance suite`)

### Task 7.2: Test hygiene — dead fixture, workflow mock, composition smoke

**Files:**
- Modify: `schema_scribe/tests/conftest.py:15-48` (remove dead `mock_llm_client` or repoint it)
- Modify: `schema_scribe/tests/integration/test_db_workflow.py:26-32` (mock `get_tables` to return `List[str]`)
- Add: `schema_scribe/tests/integration/test_db_workflow_composition.py` (new)

**Interfaces:**
- Produces: real `SQLiteConnector` + mocked LLM + real `MarkdownWriter` through `DbWorkflow` produces a valid catalog file (the audit's open question: real composition never exercised).

- [ ] **Step 1: Write the composition smoke test**

```python
def test_real_sqlite_composition(tmp_path):
    db_path = tmp_path / "fixture.db"
    ...  # seed two tables + FK using sqlite3 directly (reuse acceptance fixture pattern)
    connector = SQLiteConnector()
    connector.connect({"db_path": str(db_path)})
    llm = MagicMock(spec=BaseLLMClient)
    llm.get_description.return_value = "draft description"
    out = tmp_path / "catalog.md"
    writer = MarkdownWriter()
    wf = DbWorkflow(connector, llm, writer, db_profile_name="fixture",
                    writer_params={"output_filename": str(out)})
    wf.run()
    assert out.exists() and "draft description" in out.read_text()
```

- [ ] **Step 2: Run test to verify it fails**

Expected: FAIL or PASS — record actual result. If it fails, that failure is the composition bug to fix in this task (e.g., `get_tables` shape mismatch).

- [ ] **Step 3: Minimal implementation**

Remove the dead conftest fixture; align the workflow test mock with the real `List[str]` contract; fix whatever the smoke test exposes.

- [ ] **Step 4: Run the full suite**

Run: `./scripts/verify.sh`
Expected: PASS.

- [ ] **Step 5: Commit** (title `test: exercise real connector composition; remove dead fixtures`)

### Slice 7 gate

- [ ] Run `./scripts/verify.sh` — expected: PASS.
- [ ] Merge to parent branch (ff) after a review pass.

---

## Out Of Scope (tracked elsewhere)

- Evaluation fixture + human review rubric (PRODUCT.md:114-116) — deferred, docs/TESTING.md:30-34.
- Benchmark harness: query count / elapsed / LLM calls (PRODUCT.md:117) — deferred; the existing `call_count == 12` assertion (test_db_workflow.py:277) is the seed.
- Drift checks in CI, dbt removed-model detection (`dbt --check` reverse set difference, dbt_yaml_writer.py:100-104) — product-sequence step 2.
- PostgresCommentWriter connection-lifecycle bug (db_workflow.py:71-76 vs postgres_comment_writer.py:73-76) — adapter qualification.
- Snowflake read-only enforcement and real-engine qualification — adapter qualification.
- Hosted demo — product-sequence step 3.
