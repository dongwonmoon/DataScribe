# V1 Engine Trustworthiness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the local database documentation engine (the `db` command) trustworthy and measurable enough to be SchemaScribe v1, per `docs/PRODUCT.md`.

**Architecture:** This plan is product-sequence step 1 ("local engine trustworthy and measurable"). It hardens trust boundaries (read-only enforcement, disclosure), fixes the profiling contract, makes output writes atomic, adds baseline-based change classification and a review loop, then qualifies connectors through a shared acceptance suite. No new dependencies; every change starts with a failing test that proves the current defect. A real-composition probe (Slice 0) runs first because several later tasks depend on the actual catalog dict shape.

**Tech Stack:** Python 3.11/3.12, pytest (+ pytest-mock), Typer CLI, stdlib `sqlite3`/`os`/`tempfile`/`json`. Drivers already in extras: `psycopg2`, `mysql-connector-python`, `duckdb`, `snowflake-connector-python`.

## Global Constraints

- Verification gate: `./scripts/verify.sh` must pass after every slice (72 tests today + new tests).
- Red test first: each task's first step is a test that fails on the current code — that test is the permanent regression guard. A test that fails for the wrong reason (e.g., `ValueError` on a wrong parameter key) is not a valid red test; fix the test until it fails on the defect it claims to prove.
- No raw-row or sample-value collection or transmission (PRODUCT.md:62-64). Aggregate stats only.
- No new runtime dependencies; stdlib only for atomic writes (`tempfile`, `os.replace`).
- Read-only core path: writers that mutate a database (PostgresCommentWriter) are out of v1 scope (PRODUCT.md:69-70).
- The `db` workflow's data boundary: tables, columns, types, PKs, FKs, and exactly three aggregate stats (`null_ratio`, `distinct_count`, `is_unique`).
- Connectors read `db_params` key `"path"` for sqlite/duckdb (verify per connector before writing tests — the audit found tests in this plan originally used wrong keys).
- When a task changes behavior that an existing test pins, that existing test must be updated **in the same task**, and the update named explicitly.
- Commit style: conventional title + body "문제 상황 / 증거 / 해결 / 기각한 대안" (see AGENTS.md).
- Narrow topic branches; merge slice to parent only after its gate passes.

---

# Gap Audit (Evidence)

Audit of contract `docs/PRODUCT.md` vs current source on `dev` (4 independent source audits; claims verified against source on 2026-08-06):

| # | Contract clause | State | Evidence |
|---|---|---|---|
| G1 | Read-only connection on core path (PRODUCT.md:43,61) | **Missing on 4/5 connectors** | `sqlite3.connect(db_path)` no `mode=ro` (sqlite_connector.py:57); postgres no `default_transaction_read_only` (postgres_connector.py:61-67); mariadb no `SET SESSION TRANSACTION READ ONLY` (mariadb_connector.py:72-78); snowflake no session hardening (snowflake_connector.py:69-76). Only DuckDB file mode enforces it (duckdb_connector.py:76-89). |
| G2 | Aggregate profiling without source rows (PRODUCT.md:44-45,62-64) | **Implemented** | All queries hit catalog/PRAGMA tables; profile = `COUNT(*)`, `SUM(CASE IS NULL)`, `COUNT(DISTINCT)` only (sql_base_connector.py:261-267, sqlite_connector.py:214-220, duckdb_connector.py:223-229). No `SELECT` of source rows anywhere. |
| G3 | Disclosure before external LLM (PRODUCT.md:46-47,66-67) | **Missing** | No `--dry-run`/preview/confirm anywhere; `db` command has no such flag (app.py:301-346). What leaves the machine: identifiers, types, **view SQL verbatim** (catalog_generator.py:194-203), 3 stats (catalog_generator.py:134-170; prompts.py:12-38,129-137). |
| G4 | Interface vs behavior drift | **Partial** | `get_column_profile` docstring documents min/max/avg (interfaces.py:139-146) that no connector returns; connectors return `"N/A"` strings where float/bool documented (sqlite_connector.py:257-261, sql_base_connector.py:305-309). |
| G5 | PK accuracy + propagation | **Partial + broken** | DuckDB `is_pk` from `row[3] == "PRI"` (duckdb_connector.py:186); SQLite marks only first composite-PK column (`row[5] == 1`, sqlite_connector.py:122); SqlBase returns int not bool (sql_base_connector.py:150). **`CatalogGenerator` drops `is_pk` entirely** — enriched columns carry only name/type/description/profile_stats (catalog_generator.py:172-179), so PKs never reach writers or any downstream consumer. |
| G6 | Atomic writes / failure preserves last output (PRODUCT.md:53) | **Missing** | File writers `open(path, "w")` truncate-then-stream (markdown_writer.py:95, json_writer.py:57, dbt_markdown_writer.py:62, mermaid_writer.py:66). Generation failure preserves old file only incidentally (writer runs after full generation, db_workflow.py:94-113); write-phase failure destroys it. No `os.replace` anywhere. |
| G7 | Change classification unchanged/added/removed/structurally-changed (PRODUCT.md:51-52) | **Missing** | No persisted baseline, no comparison in `db` workflow. dbt `--check` detects added only, never removed (dbt_yaml_writer.py:100-104). |
| G8 | Review before replacement (PRODUCT.md:48-49) | **Missing for `db`** | Only dbt `--interactive` has accept/edit/skip (dbt_yaml_writer.py:398-447). `db` has no `--check`/`--interactive`. |
| G9 | Shared acceptance suite per adapter (PRODUCT.md:55-57) | **Absent** | No parametrized suite; only SQLite has real-engine tests; postgres/mariadb/snowflake mock connect params only; duckdb mocks the library (tests/unit/db_connectors/). No connector can claim v1 support today. |
| G10 | Test hygiene | **Defective** | Dead conftest fixture patches nonexistent modules (`schema_scribe.core.db_workflow.init_llm`, tests/conftest.py:42-45); workflow tests mock `get_tables` returning dicts while the real connector returns `List[str]` (test_db_workflow.py:28-32 vs sqlite_connector.py:68); prompt assertion pins the dict shape (`"Table: {'name': 'users', 'comment': None}"`, test_db_workflow.py:258) — real composition never exercised. |

**In-scope adapter defect noted, deferred:** PostgresCommentWriter is dead-on-arrival because `generate_catalog()` closes the connection before `writer.write()` runs (db_workflow.py:71-76,113; postgres_comment_writer.py:73-76). It is a mutating writer, out of v1 scope; reworked when adapters are qualified.

**Deferred to product-evaluation work (tracked, not in this plan):** eval fixture + human review rubric, benchmark harness (query count / elapsed / LLM calls), drift-in-CI support, hosted demo. See docs/TESTING.md:30-34 and CURRENT_FOCUS.md.

---

## Slice 0: Composition Probe (falsifies the catalog-shape assumptions Slices 2-6 rely on)

### Task 0.1: Real-composition smoke test

The cheapest experiment that can falsify the plan's core assumption: `DbWorkflow` + **real** `SQLiteConnector` + mocked LLM + real `MarkdownWriter` produces a valid catalog with a known shape. Every later slice (profile contract, disclosure, state snapshot, classification) is written against this shape, so it must be pinned first.

**Files:**
- Create: `schema_scribe/tests/integration/test_db_workflow_composition.py`
- Test: the new file itself (this task is test-only until it exposes defects, which then get fixed here)

**Interfaces:**
- Consumes: `SQLiteConnector.connect(db_params)` with key `"path"`; `DbWorkflow(db_connector, llm_client, writer, db_profile_name=..., writer_params=...)`.
- Produces: a pinned catalog dict shape: `catalog["tables"][i]` = `{"name", "columns": [{"name", "type", "description", "profile_stats", "is_pk"}], "views": [...], "foreign_keys": [...]}` (exact keys as produced by `CatalogGenerator` — record deviations, do not assume).

- [ ] **Step 1: Write the probe test**

```python
import json
import sqlite3
from unittest.mock import MagicMock

from schema_scribe.components.db_connectors.sqlite_connector import SQLiteConnector
from schema_scribe.components.writers.markdown_writer import MarkdownWriter
from schema_scribe.core.interfaces import BaseLLMClient
from schema_scribe.workflows.db_workflow import DbWorkflow


def test_real_sqlite_composition(tmp_path):
    db_path = tmp_path / "fixture.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email VARCHAR NOT NULL)")
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id))")
    conn.commit()
    conn.close()

    connector = SQLiteConnector()
    connector.connect({"path": str(db_path)})
    llm = MagicMock(spec=BaseLLMClient)
    llm.get_description.return_value = "draft description"
    out = tmp_path / "catalog.md"
    writer = MarkdownWriter()
    wf = DbWorkflow(connector, llm, writer, db_profile_name="fixture",
                    writer_params={"output_filename": str(out)})
    wf.run()
    assert out.exists()
    assert "draft description" in out.read_text()
```

- [ ] **Step 2: Run to observe the actual result**

Run: `pytest schema_scribe/tests/integration/test_db_workflow_composition.py -v`
Expected: FAIL or PASS — record which, and record the exact catalog dict shape by printing it (add a temporary `print(json.dumps(catalog))` if needed; remove before commit). This observation is the input to every later task.

- [ ] **Step 3: Fix composition defects surfaced here**

Known candidates (from the audit, to verify): `get_tables` returns `List[str]` while `CatalogGenerator` iterates objects (verify it handles strings — sqlite_connector.py:68 vs catalog_generator.py:125-133); FK/profile queries fail on the fixture (e.g., `SUM(CASE WHEN "email" IS NULL ...)` on VARCHAR is fine, but record real behavior); `is_pk` is dropped from enriched columns (catalog_generator.py:172-179). Fix only what blocks the test; the catalog-shape observation is recorded in the commit body.

- [ ] **Step 4: Run full suite to confirm no regressions**

Run: `./scripts/verify.sh`
Expected: PASS (or only the newly-recorded deviation, documented).

- [ ] **Step 5: Commit**

```bash
git add schema_scribe/tests/integration/test_db_workflow_composition.py
git commit -m "test: probe real connector-workflow-writer composition

문제 상황: workflow 테스트가 dict 반환 get_tables 목(mock)으로 실제 커넥터와
다른 계약을 가정해, 카탈로그 실제 형태가 검증된 적 없음.
증거: gap audit G10/G5 — test_db_workflow.py:28-32 vs sqlite_connector.py:68;
is_pk가 catalog에 전달되지 않음(catalog_generator.py:172-179).
해결: 실제 SQLiteConnector+MarkdownWriter 조합 스모크 테스트와 관찰된
카탈로그 형태 기록.
기각한 대안: 형태 검증 없이 슬라이스 진행(이후 모든 슬라이스의 입력
가정이 무너질 위험)."
```

### Slice 0 gate

- [ ] Run `./scripts/verify.sh` — expected: PASS.
- [ ] Merge to parent branch (ff). Record the observed catalog shape in the commit body; Slice 2+ tasks were written against it.

---

## Slice 0.5: Fixture Workshop (evidence for the gated Landscape slice)

User research (docs/superpowers/specs/2026-08-07-db-orientation-problem-research.md) established the author's real problem: "opening the database is scary" — (b) overwhelmed by scale, (c) meaningless names. The Landscape Report (Slice 8) is gated on evidence; this slice builds the synthetic fixtures that produce it, and those fixtures also serve the acceptance suite (Slice 7) and change-classification tests (Slice 5). One asset, three consumers.

### Task 0.5.1: Legacy fixture builder

**Files:**
- Create: `schema_scribe/tests/fixtures/db_fixtures.py`
- Create: `schema_scribe/tests/fixtures/__init__.py`
- Create: `scripts/build_fixture_dbs.py`
- Test: `schema_scribe/tests/unit/test_db_fixtures.py` (new)

**Interfaces:**
- Consumes: nothing (stdlib `sqlite3` only).
- Produces: `build_sqlite(path: str, variant: str) -> None` with variants:
  - `"clean"` — the existing demo shape: a few tables, meaningful names, declared FKs.
  - `"legacy-rich"` — 100+ tables, cryptic names (e.g., `TBL_CUST_MST_2021`, `T_SLS_ORD_HDR`), dense declared FKs.
  - `"legacy-poor"` — same cryptic scale but almost no declared FKs (the FK-centrality worst case).
  - `"legacy-conventions"` — meaningful-ish names but organized by convention (`dim_`, `fact_`, `stg_`, `_audit`).
  Also `VARIANT_NAMES = ("clean", "legacy-rich", "legacy-poor", "legacy-conventions")` and a deterministic table generator (seeded, no randomness) so fixtures are reproducible.

- [ ] **Step 1: Write the failing test**

```python
@pytest.mark.parametrize("variant", db_fixtures.VARIANT_NAMES)
def test_fixture_builds_deterministically(tmp_path, variant):
    p1 = tmp_path / f"{variant}-1.db"
    p2 = tmp_path / f"{variant}-2.db"
    db_fixtures.build_sqlite(str(p1), variant)
    db_fixtures.build_sqlite(str(p2), variant)
    assert p1.read_bytes() == p2.read_bytes()  # deterministic
    conn = sqlite3.connect(str(p1))
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    conn.close()
    if variant.startswith("legacy"):
        assert len(tables) >= 100
    else:
        assert len(tables) >= 3
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest schema_scribe/tests/unit/test_db_fixtures.py -v`
Expected: FAIL — module does not exist.

- [ ] **Step 3: Minimal implementation**

`db_fixtures.py` per the Interfaces block. Deterministic generation: seed a PRNG with a fixed constant per variant, or generate names algorithmically (e.g., `f"TBL_{prefix}_{year}"` over a fixed prefix list). `scripts/build_fixture_dbs.py` is a thin CLI (`--out-dir`, `--variant`) so the author can materialize a legacy DB to "play with the tool" manually.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest schema_scribe/tests/unit/test_db_fixtures.py -v`
Expected: PASS.

- [ ] **Step 5: Commit** (title `test: add deterministic legacy DB fixture builder`)

### Task 0.5.2: Landscape probe protocol (counterfactual run)

**Files:**
- Create: `docs/superpowers/specs/2026-08-07-landscape-probe-results.md` (filled in by this task)
- Test: none — this is a manual evidence task with a written protocol

**Interfaces:**
- Consumes: fixture DBs from Task 0.5.1, the existing `db` command (pre- or post-Slice 1 read-only — run it against the fixture).

- [ ] **Step 1: Materialize the legacy fixtures**

Run: `python scripts/build_fixture_dbs.py --out-dir /tmp/schema-fixtures --variant legacy-rich` (and `legacy-poor`).

- [ ] **Step 2: Counterfactual run**

Run the existing `db` command against `legacy-rich` (mocked or local LLM; the point is the deterministic output — ERD, table list, summaries):

```bash
schema-scribe db --db <fixture-profile> --output <markdown-profile> 2>&1 | head -100
```

- [ ] **Step 3: The author judges (recorded, one question set fixed in advance)**

Questions (fixed before looking at output — do not score after the fact):
1. Does the output tell you where to start? (scale cure / (b))
2. Does the output decode the table names? (name cure / (c))
3. What single feature would have cured the fear you remember?
Record the answers verbatim in the probe-results spec. The FK density of `legacy-rich` vs `legacy-poor` (tables with declared FKs / total tables) is recorded too — it decides whether FK-centrality survives (Slice 8 gate).

- [ ] **Step 4: Commit** (title `docs: record landscape probe results`)

### Slice 0.5 gate

- [ ] Probe results recorded; the Slice 8 gate decision (implement / defer) is written in the probe-results spec.
- [ ] Merge to parent branch (ff).

---

## Slice 1: Read-Only Enforcement (G1)

### Task 1.1: SQLite read-only connection

**Files:**
- Modify: `schema_scribe/components/db_connectors/sqlite_connector.py:51-60`
- Test: `schema_scribe/tests/unit/db_connectors/test_sqlite_connector.py`

**Interfaces:**
- Consumes: `connect(self, db_params)` where db_params key is `"path"` (sqlite_connector.py:51-53).
- Produces: behavior change — connection opened with `mode=ro` URI; connecting to a nonexistent path now raises instead of silently creating the file.

- [ ] **Step 1: Write the failing test**

Append to `test_sqlite_connector.py` (existing tests seed temp DBs with `sqlite3.connect` first and pass `{"path": ...}` — follow that pattern):

```python
def test_connection_is_read_only(tmp_path):
    db_path = tmp_path / "ro.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    conn.commit()
    conn.close()
    connector = SQLiteConnector()
    connector.connect({"path": str(db_path)})
    with pytest.raises(sqlite3.OperationalError, match="readonly"):
        connector.connection.execute("CREATE TABLE t2 (id INTEGER)")
    connector.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest schema_scribe/tests/unit/db_connectors/test_sqlite_connector.py::test_connection_is_read_only -v`
Expected: FAIL — write succeeds (no `readonly` error raised).

- [ ] **Step 3: Minimal implementation**

In `sqlite_connector.py`, replace the connect call (keep the existing `except sqlite3.Error` wrapper and the missing-`path` ValueError):

```python
self.connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
```

Note: with `mode=ro`, a nonexistent file raises `sqlite3.OperationalError` (unable to open database file) instead of creating it — this is the intended read-only contract; the existing `ConnectorError` wrapper converts it. No `assert os.path.exists(...)` needed; do not add one. (If a future user needs a "create if missing" mode it must be explicit opt-in, out of v1 scope.)

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest schema_scribe/tests/unit/db_connectors/test_sqlite_connector.py -v`
Expected: PASS (all sqlite connector tests, old + new).

- [ ] **Step 5: Commit**

```bash
git add schema_scribe/components/db_connectors/sqlite_connector.py schema_scribe/tests/unit/db_connectors/test_sqlite_connector.py
git commit -m "fix: enforce read-only SQLite connections

문제 상황: v1 계약은 코어 경로 읽기 전용을 요구하나 sqlite 커넥터가 쓰기 가능 연결을 열었음.
증거: gap audit G1 — sqlite3.connect(db_path)에 mode=ro 없음 (sqlite_connector.py:57); 쓰기 시도가 성공하는 red 테스트로 재현.
해결: mode=ro URI로 전환.
기각한 대안: PRAGMA query_only(연결 후 설정이라 오픈 순간 쓰기 위험이 남음), 파일 존재 assert(중복 방어 코드, wrapper가 처리)."
```

### Task 1.2: PostgreSQL read-only connection

**Files:**
- Modify: `schema_scribe/components/db_connectors/postgres_connector.py:61-67`
- Modify: `schema_scribe/tests/unit/db_connectors/test_postgres_connector.py:14-32` — **existing test update required**
- Test: extend `test_postgres_connector.py`

**Interfaces:**
- Consumes: `connect(self, db_params)`.
- Produces: connect kwargs now include `options="-c default_transaction_read_only=on"`.

- [ ] **Step 1: Write the failing test and update the pinned existing test**

The existing `test_postgres_connector_connect` asserts `mock_psycopg2.connect.assert_called_once_with(host=..., port=..., user=..., password=..., dbname=...)` (test_postgres_connector.py:26-32) — it **must** be extended to include the new kwarg, in this same task:

```python
    mock_psycopg2.connect.assert_called_once_with(
        host="localhost",
        port=5432,
        user="admin",
        password="pw",
        dbname="testdb",
        options="-c default_transaction_read_only=on",
    )
```

Add a dedicated red test first:

```python
def test_connect_enforces_read_only(mocker):
    mock_connect = mocker.patch("psycopg2.connect")
    connector = PostgresConnector()
    connector.connect({"host": "h", "user": "u", "password": "p", "dbname": "d"})
    kwargs = mock_connect.call_args.kwargs
    assert kwargs["options"] == "-c default_transaction_read_only=on"
```

- [ ] **Step 2: Run tests to verify the new one fails**

Run: `pytest schema_scribe/tests/unit/db_connectors/test_postgres_connector.py::test_connect_enforces_read_only -v`
Expected: FAIL — `options` key missing.

- [ ] **Step 3: Minimal implementation**

Add to the connect kwargs dict in `postgres_connector.py`:

```python
"options": "-c default_transaction_read_only=on",
```

- [ ] **Step 4: Run the whole file**

Run: `pytest schema_scribe/tests/unit/db_connectors/test_postgres_connector.py -v`
Expected: PASS — both the new test and the updated pinned test.

- [ ] **Step 5: Commit** (title `fix: enforce read-only PostgreSQL connections`; body notes the pinned-test update)

### Task 1.3: MariaDB read-only connection

**Files:**
- Modify: `schema_scribe/components/db_connectors/mariadb_connector.py:72-80`
- Test: `schema_scribe/tests/unit/db_connectors/test_mariadb_connector.py`

**Interfaces:**
- Consumes: `connect(self, db_params)`; the connector keeps a live `self.cursor` created in `connect` (mariadb_connector.py:79) — **reuse it, do not create or close a second cursor**.

- [ ] **Step 1: Write the failing test**

```python
def test_connect_enforces_read_only(mocker):
    mock_cursor = mocker.MagicMock()
    mock_conn = mocker.MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mocker.patch("mysql.connector.connect", return_value=mock_conn)
    connector = MariaDBConnector()
    connector.connect({"host": "h", "user": "u", "password": "p", "database": "d"})
    mock_cursor.execute.assert_any_call("SET SESSION TRANSACTION READ ONLY")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest schema_scribe/tests/unit/db_connectors/test_mariadb_connector.py::test_connect_enforces_read_only -v`
Expected: FAIL — cursor never called with that statement.

- [ ] **Step 3: Minimal implementation**

After `self.cursor = self.connection.cursor()` (mariadb_connector.py:79), add:

```python
self.cursor.execute("SET SESSION TRANSACTION READ ONLY")
```

Do **not** close the cursor — it is the connector's live metadata cursor. Do **not** create a second cursor.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest schema_scribe/tests/unit/db_connectors/test_mariadb_connector.py -v`
Expected: PASS.

- [ ] **Step 5: Commit** (title `fix: enforce read-only MariaDB connections`)

### Task 1.4: DuckDB read-only regression lock

**Files:**
- Test: `schema_scribe/tests/unit/db_connectors/test_duckdb_connector.py`

**Interfaces:**
- Produces: no code change expected — the existing test `test_duckdb_connector` already asserts `duckdb.connect(database=..., read_only=True)` for `.db` files (test_duckdb_connector.py:34-42). This task makes the read-only guarantee an explicit, named regression lock with a write-attempt assertion.

- [ ] **Step 1: Add the regression lock**

```python
def test_file_connection_is_read_only(mock_duckdb_lib):
    connector = DuckDBConnector()
    connector.connect({"path": "/tmp/fixture.duckdb"})
    kwargs = mock_duckdb_lib.mock_connect.call_args.kwargs
    assert kwargs.get("read_only") is True
```

- [ ] **Step 2: Run test**

Run: `pytest schema_scribe/tests/unit/db_connectors/test_duckdb_connector.py::test_file_connection_is_read_only -v`
Expected: PASS immediately (existing behavior). This is a regression lock, not a red test — state that explicitly in the commit body so reviewers know the assertion already holds.

- [ ] **Step 3: Declare Snowflake read-only status**

In `snowflake_connector.py` `connect` docstring, replace any privacy-adjacent claims with: "Read-only enforcement relies on the account role's grants; Snowflake does not support a server-side read-only session mode. Snowflake is NOT v1-qualified (see docs/PRODUCT.md:55-57 and the acceptance suite)."

- [ ] **Step 4: Commit** (title `test: lock DuckDB read-only; declare Snowflake unqualified`)

### Slice 1 gate

- [ ] Run `./scripts/verify.sh` — expected: PASS (72 + new tests; the updated postgres pinned test included).
- [ ] Merge to parent branch (ff) with `git log` review for missed commits.

---

## Slice 2: Profiling Contract Fixes (G4, G5)

### Task 2.1: Align `get_column_profile` interface with privacy boundary

**Files:**
- Modify: `schema_scribe/core/interfaces.py:137-146`
- Modify: `schema_scribe/components/db_connectors/sql_base_connector.py:292-309`, `sqlite_connector.py:244-261`, `duckdb_connector.py:253-266`
- Modify: `schema_scribe/services/catalog_generator.py:69-74`
- Test: `schema_scribe/tests/unit/db_connectors/test_sqlite_connector.py` (extend)

**Interfaces:**
- Consumes: `get_column_profile` — note the guard is on `self.cursor` (sqlite_connector.py:209-212), not `self.connection`.
- Produces: interface documents exactly `null_ratio: float | None`, `distinct_count: int | None`, `is_unique: bool | None`; failure returns `None` values instead of `"N/A"` strings; `_format_profile_stats` renders `None` as `"N/A"` in prompts.

- [ ] **Step 1: Write the failing test**

```python
def test_profile_returns_none_on_query_failure(mocker):
    connector = SQLiteConnector()
    connector.cursor = mocker.MagicMock()
    connector.cursor.execute.side_effect = sqlite3.Error("boom")
    profile = connector.get_column_profile("t", "c")
    assert profile["null_ratio"] is None
    assert profile["distinct_count"] is None
    assert profile["is_unique"] is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest schema_scribe/tests/unit/db_connectors/test_sqlite_connector.py::test_profile_returns_none_on_query_failure -v`
Expected: FAIL — current code returns `"N/A"` strings (and note: the test must stub `connector.cursor`, not `connector.connection.cursor`, or the `if not self.cursor` guard at sqlite_connector.py:209-212 fires first).

- [ ] **Step 3: Minimal implementation**

In each connector's `get_column_profile` failure branch, return `None` for each stat instead of `"N/A"` (sqlite_connector.py:257-261, sql_base_connector.py:305-309, duckdb_connector.py:262-266). Update `interfaces.py:139-146` docstring: drop `min`/`max`/`avg`, document `float | None` / `int | None` / `bool | None`. In `catalog_generator.py:_format_profile_stats` (catalog_generator.py:69-74), the current `.get(key, "N/A")` default does **not** apply when the key exists with value `None` — change to:

```python
def _format_profile_stats(self, profile_stats):
    null_ratio = profile_stats.get("null_ratio")
    distinct_count = profile_stats.get("distinct_count")
    is_unique = profile_stats.get("is_unique")
    return (
        f"Null Ratio: {null_ratio if null_ratio is not None else 'N/A'} | "
        f"Distinct Count: {distinct_count if distinct_count is not None else 'N/A'} | "
        f"Is Unique: {is_unique if is_unique is not None else 'N/A'}"
    )
```

(Adjust to the exact current formatting at catalog_generator.py:69-74 — the requirement is: `None` renders as `"N/A"`, never as the string `"None"`.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest schema_scribe/tests/unit/db_connectors/test_sqlite_connector.py schema_scribe/tests/integration/test_db_workflow.py -v`
Expected: PASS.

- [ ] **Step 5: Commit** (title `fix: align profile stats contract with privacy boundary`)

### Task 2.2: DuckDB primary-key detection (decision gate on real behavior)

The repo's own mocked test models DuckDB `DESCRIBE` as returning `"PRI"` in the key slot (test_duckdb_connector.py:131), which would make `is_pk` work today and make a rewrite unnecessary. The audit claims real DuckDB returns NULL there. This task starts with an observation that decides the rest.

**Files:**
- Modify: `schema_scribe/components/db_connectors/duckdb_connector.py:180-189` (only if the probe shows a real defect)
- Test: `schema_scribe/tests/unit/db_connectors/test_duckdb_connector.py` (extend)

**Interfaces:**
- Consumes: `get_columns(table_name)` returning per-column dicts with `is_pk: bool`.
- Produces: either (a) proof that `is_pk` works and a regression lock, or (b) `is_pk` computed from `duckdb_constraints()` (same API already used for FKs at duckdb_connector.py:307-318, proven available in DuckDB v0.9.0+).

- [ ] **Step 1: Observe real behavior (5 minutes)**

Run a real DuckDB probe (from the repo's `.venv`):

```python
import duckdb, tempfile, os
p = os.path.join(tempfile.mkdtemp(), "pk.db")
con = duckdb.connect(p)
con.execute("CREATE TABLE t (a INTEGER, b INTEGER, PRIMARY KEY (a, b))")
print(con.execute("DESCRIBE t").fetchall())
con.close()
```

Record the actual `key` slot value (and whether it is NULL or `"PRI"`).

- [ ] **Step 2: Decide the branch**

- If `DESCRIBE` returns `"PRI"`: write a regression lock test asserting `is_pk is True` for both composite-PK columns (it may pass immediately — say so in the commit body), and stop. No code change.
- If `DESCRIBE` returns NULL/empty: write the failing test below, then implement (b).

Failing test (real duckdb, if importable in the test env; otherwise mocked with the real `duckdb_constraints()` row shape seen in the probe):

```python
def test_is_pk_detected_from_constraints(tmp_path):
    db = duckdb.connect(str(tmp_path / "pk.db"))
    db.execute("CREATE TABLE t (a INTEGER, b INTEGER, PRIMARY KEY (a, b))")
    db.close()
    connector = DuckDBConnector()
    connector.connect({"path": str(tmp_path / "pk.db")})
    cols = {c["name"]: c for c in connector.get_columns("t")}
    assert cols["a"]["is_pk"] is True
    assert cols["b"]["is_pk"] is True
    connector.close()
```

- [ ] **Step 3: Implementation (only in the second branch)**

Keep the file-scan path's DESCRIBE-based inference intact (test_duckdb_connector.py:128-161 pins it). For persistent `.db`/`.duckdb` connections, query `duckdb_constraints()` exactly like the existing FK code (duckdb_connector.py:307-318), filtering `WHERE constraint_type = 'PRIMARY KEY'` **and `table_name = ?`**, and flattening `column_names` (a list — each element is a PK column). Build a `set` of PK column names and set `is_pk = name in pk_columns`.

- [ ] **Step 4: Run the duckdb tests**

Run: `pytest schema_scribe/tests/unit/db_connectors/test_duckdb_connector.py -v`
Expected: PASS (both branches).

- [ ] **Step 5: Commit** (title: `fix: detect DuckDB primary keys via constraints` or `test: lock DuckDB primary-key detection` — whichever the probe decides)

### Task 2.3: SQLite composite-PK marking and SqlBase boolean coercion

**Files:**
- Modify: `schema_scribe/components/db_connectors/sqlite_connector.py:122`
- Modify: `schema_scribe/components/db_connectors/sql_base_connector.py:150`
- Test: `schema_scribe/tests/unit/db_connectors/test_sqlite_connector.py` (extend), `schema_scribe/tests/unit/db_connectors/test_sql_base_connector.py` (extend)

**Interfaces:**
- Produces: SQLite marks every composite-PK column (`row[5] > 0`); SqlBase returns `bool(row[3])`.

- [ ] **Step 1: Write the failing test**

```python
def test_composite_pk_all_columns_marked(tmp_path):
    db_path = tmp_path / "pk.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE t (a INTEGER, b INTEGER, PRIMARY KEY (a, b))")
    conn.commit()
    conn.close()
    connector = SQLiteConnector()
    connector.connect({"path": str(db_path)})
    cols = {c["name"]: c for c in connector.get_columns("t")}
    assert cols["a"]["is_pk"] is True
    assert cols["b"]["is_pk"] is True
    connector.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest schema_scribe/tests/unit/db_connectors/test_sqlite_connector.py::test_composite_pk_all_columns_marked -v`
Expected: FAIL — `is_pk` True only for `a`.

- [ ] **Step 3: Minimal implementation**

`sqlite_connector.py`: change `row[5] == 1` to `row[5] > 0`. `sql_base_connector.py`: change `row[3] or False` to `bool(row[3])` (psycopg2 already yields bool; MariaDB's CASE TRUE yields int 1 — `bool()` normalizes both).

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest schema_scribe/tests/unit/db_connectors/test_sqlite_connector.py schema_scribe/tests/unit/db_connectors/test_sql_base_connector.py -v`
Expected: PASS.

- [ ] **Step 5: Commit** (title `fix: correct composite PK marking and bool coercion`)

### Task 2.4: Propagate `is_pk` into the catalog dict (unblocks Slice 5)

The audit found `CatalogGenerator` drops `is_pk` (catalog_generator.py:172-179). Without this, PK fixes have zero user-visible effect and the Slice 5 state snapshot cannot compute PK-based structural change.

**Files:**
- Modify: `schema_scribe/services/catalog_generator.py:172-179`
- Modify: `schema_scribe/components/writers/markdown_writer.py:137-144` (render PK marker)
- Test: `schema_scribe/tests/integration/test_db_workflow_composition.py` (extend), `schema_scribe/tests/unit/writers/test_markdown_writer.py` (extend)

**Interfaces:**
- Consumes: column dicts from `get_columns` (have `is_pk`).
- Produces: enriched column dicts include `"is_pk": bool`; MarkdownWriter renders a PK marker (e.g., a `🔑` or `[PK]` prefix on the column name — choose the same style as existing markdown); JsonWriter already dumps the catalog dict unchanged, so it inherits `is_pk`.

- [ ] **Step 1: Write the failing test**

Extend the Slice 0 composition test (the real connector path):

```python
def test_catalog_carries_is_pk(tmp_path):
    db_path = tmp_path / "pk.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    conn.commit()
    conn.close()
    connector = SQLiteConnector()
    connector.connect({"path": str(db_path)})
    llm = MagicMock(spec=BaseLLMClient)
    llm.get_description.return_value = "draft"
    wf = DbWorkflow(connector, llm, writer=None, db_profile_name="fixture")
    catalog = wf.generate_catalog()
    col = catalog["tables"][0]["columns"][0]
    assert col["name"] == "id" and col["is_pk"] is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest schema_scribe/tests/integration/test_db_workflow_composition.py::test_catalog_carries_is_pk -v`
Expected: FAIL — `KeyError: 'is_pk'` (key absent).

- [ ] **Step 3: Minimal implementation**

In `catalog_generator.py`'s enriched-column dict (catalog_generator.py:172-179), add `"is_pk": col_info.get("is_pk", False)` (the connector returns it; check the exact key name from `get_columns`). In `markdown_writer.py`, render the PK marker in the column row using the existing column-name cell style.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest schema_scribe/tests/integration/test_db_workflow_composition.py schema_scribe/tests/unit/writers/test_markdown_writer.py -v`
Expected: PASS.

- [ ] **Step 5: Commit** (title `feat: propagate primary-key markers into catalog and markdown`)

### Task 2.5: Composite foreign-key pairing (panel finding, C2)

The panel found FK extraction is broken for composite FKs: SQLBase joins FK columns without ordinal pairing (cross-products composite columns), DuckDB returns only the first column of a composite FK (`column_names[1]`). FK quality matters for both the catalog (A) and FK-centrality (Slice 8).

**Files:**
- Modify: `schema_scribe/components/db_connectors/sql_base_connector.py:203-235`
- Modify: `schema_scribe/components/db_connectors/duckdb_connector.py:305-327`
- Test: `schema_scribe/tests/unit/db_connectors/test_sql_base_connector.py` (extend), `schema_scribe/tests/unit/db_connectors/test_duckdb_connector.py` (extend)

**Interfaces:**
- Consumes: `get_foreign_keys()` returning per-FK dicts with `source_table`, `source_column`, `target_table`, `target_column`.
- Produces: for a composite FK on `(a, b) -> (x, y)`, exactly two dicts with correctly paired columns — no cross-product, no dropped columns.

- [ ] **Step 1: Write the failing test**

```python
def test_composite_fk_pairs_columns_by_ordinal():
    # mock cursor returns PRAGMA/information_schema rows for a composite FK
    # on (a, b) -> (x, y); assert exactly two dicts with
    # (source_column="a", target_column="x") and (source_column="b", target_column="y")
```

(Adapt to each connector's actual row shape: SQLBase joins FK rows to PK rows via `constraint_name` — pair on the ordinal/seq field instead; DuckDB already reads `duckdb_constraints()` — use `column_names`/`referenced_column_names` element-wise instead of `[1]`.)

- [ ] **Step 2: Run tests to verify they fail**

Expected: FAIL — cross-product (SQLBase) / single column (DuckDB).

- [ ] **Step 3: Minimal implementation**

Pair composite FK columns by their ordinal position within the constraint, element-wise, in both connectors.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest schema_scribe/tests/unit/db_connectors/test_sql_base_connector.py schema_scribe/tests/unit/db_connectors/test_duckdb_connector.py -v`
Expected: PASS.

- [ ] **Step 5: Commit** (title `fix: pair composite foreign-key columns by ordinal`)

### Slice 2 gate

- [ ] Run `./scripts/verify.sh` — expected: PASS.
- [ ] Merge to parent branch (ff).

---

## Slice 3: Disclosure Before External LLM (G3)

### Task 3.1: `db --dry-run` — real disclosure, no LLM client construction

Design decisions from the panel review: (1) `--dry-run` must **not** construct the LLM client — `OllamaClient.__init__` performs a model pull (ollama_client.py:64-67), which would execute provider-side work in a "dry" run; (2) the manifest must include the **verbatim view SQL** (the riskiest payload item, G3) and state the profile stats that will be sent; (3) it must not run the full profiling workload twice — it discloses what WILL be sent without paying the profiling cost twice (stat keys + a note that per-column aggregate stats are included).

**Files:**
- Modify: `schema_scribe/app.py:301-346` (db command: build workflow with no LLM client when `--dry-run`)
- Modify: `schema_scribe/workflows/db_workflow.py` (new method)
- Test: `schema_scribe/tests/integration/test_db_workflow.py` (add), `schema_scribe/tests/unit/db_connectors/test_sqlite_connector.py` if a dry-run-specific unit is cheaper

**Interfaces:**
- Consumes: `DbWorkflow(db_connector, llm_client=None, writer=None, db_profile_name=...)`.
- Produces: `DbWorkflow.dry_run() -> None` — collects schema metadata (tables, columns with types, **view SQL verbatim**, FK count), prints the manifest, calls NO LLM, writes nothing, closes the connection. Provider name is a constructor param `provider_name: str | None = None`, not read from the client.

- [ ] **Step 1: Write the failing test**

```python
def test_dry_run_prints_manifest_without_llm_or_write(capsys):
    connector = MagicMock(spec=BaseConnector)
    connector.get_tables.return_value = ["users"]
    connector.get_columns.return_value = [{"name": "id", "type": "INTEGER"}]
    connector.get_views.return_value = [{"name": "v_active", "definition": "CREATE VIEW v_active AS SELECT id FROM users WHERE status = 'paid'"}]
    connector.get_foreign_keys.return_value = []
    wf = DbWorkflow(connector, llm_client=None, writer=None, db_profile_name="mydb",
                    provider_name="ollama")
    wf.dry_run()
    out = capsys.readouterr().out
    assert "mydb" in out
    assert "users" in out and "id" in out
    assert "v_active" in out and "status = 'paid'" in out  # view SQL disclosed verbatim
    assert "ollama" in out
    connector.get_column_profile.assert_not_called()
    connector.close.assert_called_once()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest schema_scribe/tests/integration/test_db_workflow.py::test_dry_run_prints_manifest_without_llm_or_write -v`
Expected: FAIL — `DbWorkflow.dry_run` does not exist.

- [ ] **Step 3: Minimal implementation**

In `db_workflow.py`, add `dry_run()`: fetch tables/columns/views/FKs via the connector, print the manifest (profile name, provider name, table list, per-table column `name: type` lines, view count + verbatim view SQL lines, FK count, and the note "Per-column aggregate stats (null_ratio, distinct_count, is_unique) will be included"), then `close()`. In `app.py`, the `--dry-run` flag builds the workflow with `llm_client=None` and `writer=None` and calls `dry_run()` — the LLM client is never constructed.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest schema_scribe/tests/integration/test_db_workflow.py -v`
Expected: PASS.

- [ ] **Step 5: Commit** (title `feat: add db --dry-run disclosure manifest`)

### Task 3.2: Run-start transmission disclosure line

**Files:**
- Modify: `schema_scribe/workflows/db_workflow.py` (pass `provider_name` through to the run log)
- Modify: `schema_scribe/services/catalog_generator.py` (emit disclosure before first LLM call)
- Test: `schema_scribe/tests/integration/test_db_workflow.py` (extend, using `caplog` — the logger binds to `sys.stdout` at import (utils/logger.py:46-59), so `capsys` will not capture log output)

**Interfaces:**
- Consumes: `CatalogGenerator(db_connector, llm_client)`; `DbWorkflow(..., provider_name=...)`.
- Produces: before the first `get_description` call, log at INFO: `Sending <n> table summaries and <m> column descriptions to provider '<provider_name>'. Metadata: tables=<t>, columns=<c>, views=<v>.` The counts are computed from the already-collected metadata in the run's collection phase — do not add a second collection pass.

- [ ] **Step 1: Write the failing test**

```python
def test_run_discloses_payload_before_first_llm_call(caplog, mock_llm_client, ...):
    connector = MagicMock(spec=BaseConnector)
    connector.get_tables.return_value = ["t"]
    connector.get_columns.return_value = [{"name": "c", "type": "INTEGER", "description": "", "is_nullable": True, "is_pk": False}]
    wf = DbWorkflow(connector, mock_llm_client, writer=None, db_profile_name="d", provider_name="openai")
    with caplog.at_level(logging.INFO):
        wf.run()
    assert "provider 'openai'" in caplog.text
    assert "1 table summaries" in caplog.text
```

Note: `mock_llm_client` is `MagicMock(spec=BaseLLMClient)` — do **not** access any attribute on it except `get_description`, because `BaseLLMClient` has no provider attribute.

- [ ] **Step 2: Run test to verify it fails**

Expected: FAIL — no disclosure line logged.

- [ ] **Step 3: Minimal implementation**

In `catalog_generator.py`, at the start of the table-description loop (before the first `get_description`), log the disclosure line with the collected counts. The counts must come from the collection already done in this run — if `CatalogGenerator` collects views only later (catalog_generator.py:189-203), move view collection before the description loop or log the view count as "pending" — prefer the cheap fix: collect metadata first, then describe (one reorder, no duplicate work).

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
- Produces: both writers render the complete output in memory, write to a temp file in the target directory, then `os.replace` onto the target; temp file removed on failure (`try/finally`); existing `IOError` handling preserved.

- [ ] **Step 1: Write the failing test — inject failure INSIDE the write path**

The test must exercise the truncation defect, so it patches a stream write, not the writer method itself:

```python
def test_failed_write_preserves_previous_output(tmp_path, monkeypatch):
    target = tmp_path / "catalog.md"
    target.write_text("PREVIOUS CONTENT")
    writer = MarkdownWriter()
    import io
    def failing_io(*a, **k):
        raise IOError("disk full")
    monkeypatch.setattr(io, "open", failing_io)  # or patch the exact open the writer calls
    with pytest.raises(IOError):
        writer.write({...same dict as existing markdown writer tests...},
                     output_filename=str(target))
    assert target.read_text() == "PREVIOUS CONTENT"
```

(Reuse the existing markdown writer test's catalog dict. If patching `io.open` is too broad, patch `tempfile.mkstemp`/`os.replace` failure instead and assert the temp file is cleaned up and the target untouched — the assertion that matters: old content intact after failure.)

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest schema_scribe/tests/unit/writers/test_markdown_writer.py::test_failed_write_preserves_previous_output -v`
Expected: FAIL — `"PREVIOUS CONTENT"` lost (file truncated at open).

- [ ] **Step 3: Minimal implementation**

Both writers: accumulate the full output (markdown: build the string in memory instead of streaming to `f`; json: `json.dumps(catalog_data, indent=2)`), then:

```python
import os
import tempfile

content = ...  # full rendered string
fd, tmp_name = tempfile.mkstemp(dir=os.path.dirname(os.path.abspath(output_filename)), suffix=".tmp")
try:
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(content)
    os.replace(tmp_name, output_filename)
except BaseException:
    if os.path.exists(tmp_name):
        os.unlink(tmp_name)
    raise
```

Note: `mkstemp` creates the file mode 0600 — `os.replace` keeps that mode, changing output file permissions from umask-default. If the existing tests or product behavior require umask-default modes, copy the mode of an existing target with `os.chmod(tmp_name, ...)` before replace (or use `os.open` with the umask default) — decide by checking existing writer tests; record the choice in the commit body.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest schema_scribe/tests/unit/writers/test_markdown_writer.py schema_scribe/tests/unit/writers/test_json_writer.py -v`
Expected: PASS.

- [ ] **Step 5: Commit** (title `fix: make file writers atomic`)

### Task 4.2: Failure-preservation integration test (regression lock)

**Files:**
- Test: `schema_scribe/tests/integration/test_db_workflow.py` (add)

**Interfaces:**
- Consumes: `DbWorkflow.run()` with injected fakes.

- [ ] **Step 1: Write the regression lock**

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

- [ ] **Step 2: Run test**

Expected: PASS immediately (generation completes before the writer runs, db_workflow.py:94-113). This locks the generation-failure path; the destructive write-phase path is covered by Task 4.1's mid-write test. State this honestly in the commit body — do not claim this test proves write-phase preservation.

- [ ] **Step 3: Commit** (title `test: lock failure-preserves-output semantics`)

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
- Consumes: catalog dict from `CatalogGenerator` (shape pinned by Slice 0: tables with `columns` incl. `is_pk`, top-level `views`, `foreign_keys`).
- Produces: `SchemaState.snapshot(catalog) -> dict` (pure), `SchemaState.save(snapshot: dict, path: str) -> None` (atomic, reusing the Task 4.1 pattern), `SchemaState.load(path: str) -> dict | None` (`None` on missing/corrupt JSON — never raise). Snapshot shape: `{"tables": {name: {"columns": {col: type}, "pk": [col, ...], "fks": [{"source": col, "target": "table.col"}, ...]}}, "views": [names], "generated_at": iso}`. The `fks` entries carry **both source and target** so the classifier (Task 5.2) can detect an FK target change, not only FK presence (panel finding C10). Sidecar path convention: `<output_filename>.schema-state.json`. Sidecar written only for file writers that receive `output_filename`, and only after a successful `writer.write`.

- [ ] **Step 1: Write the failing test**

```python
def test_roundtrip_snapshot(tmp_path):
    path = tmp_path / "catalog.md.schema-state.json"
    catalog = {"tables": [{"name": "t", "columns": [
        {"name": "id", "type": "INTEGER", "is_pk": True},
        {"name": "v", "type": "TEXT", "is_pk": False},
    ]}], "views": [], "foreign_keys": []}
    snap = SchemaState.snapshot(catalog)
    SchemaState.save(snap, str(path))
    loaded = SchemaState.load(str(path))
    assert loaded["tables"]["t"]["columns"] == {"id": "INTEGER", "v": "TEXT"}
    assert loaded["tables"]["t"]["pk"] == ["id"]


def test_load_missing_or_corrupt_returns_none(tmp_path):
    assert SchemaState.load(str(tmp_path / "nope.json")) is None
    bad = tmp_path / "bad.json"
    bad.write_text("{not json")
    assert SchemaState.load(str(bad)) is None
```

- [ ] **Step 2: Run test to verify it fails**

Expected: FAIL — module does not exist.

- [ ] **Step 3: Minimal implementation**

`schema_state.py` per the Interfaces block. In `db_workflow.run()`, after a successful `writer.write(...)`: if `writer_params` contains `output_filename`, save the snapshot to `<output_filename>.schema-state.json`; a sidecar write failure must **not** fail the run (log a warning and continue) — the documentation output is the product, the sidecar is bookkeeping.

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
- Produces: `SchemaState.classify(prev: dict | None, curr: dict) -> {"added": [...], "removed": [...], "structurally_changed": [...]}` — structural change = column type change, PK set change, FK set change (compare `fks` as (source, target) pairs — a changed FK target counts), or column added/removed within an existing table; unchanged tables appear in neither list. `prev=None` → everything is `added`.

- [ ] **Step 1: Write the failing test**

```python
def test_classify_categories():
    prev = SchemaState.snapshot({"tables": [
        {"name": "keep", "columns": [{"name": "c", "type": "INTEGER", "is_pk": True}]},
        {"name": "drop", "columns": []},
    ], "views": [], "foreign_keys": []})
    curr = SchemaState.snapshot({"tables": [
        {"name": "keep", "columns": [{"name": "c", "type": "TEXT", "is_pk": True}]},
        {"name": "new", "columns": []},
    ], "views": [], "foreign_keys": []})
    r = SchemaState.classify(prev, curr)
    assert r["added"] == ["new"]
    assert r["removed"] == ["drop"]
    assert r["structurally_changed"] == ["keep"]


def test_classify_without_previous_baseline_marks_all_added():
    curr = SchemaState.snapshot({"tables": [{"name": "t", "columns": []}], "views": [], "foreign_keys": []})
    r = SchemaState.classify(None, curr)
    assert r["added"] == ["t"]
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

Semantics are defined here, not left open: no sidecar and no existing output file → `check()` returns False (exit 0, "no existing documentation to compare"); sidecar present → classification (Task 5.2) drives the result; output file present → also print a unified diff of new render vs existing file. `check()` returns True iff anything would change. `db --check` exits 1 on True.

**Files:**
- Modify: `schema_scribe/app.py:301-346` (db command: `--check` flag; the mutual-exclusion check lives at app.py:402-407 for dbt — mirror it for `db`)
- Modify: `schema_scribe/workflows/db_workflow.py`
- Test: `schema_scribe/tests/integration/test_db_workflow.py` (add)

**Interfaces:**
- Consumes: `SchemaState` (Slice 5), a render function.
- Produces: `DbWorkflow.render() -> str` — renders the catalog to the configured writer's output string without writing (for Markdown/JSON writers; extract the render logic from the writer or reuse `io.StringIO`-style accumulation). `DbWorkflow.check() -> bool` per the semantics above.

- [ ] **Step 1: Write the failing test**

```python
def test_check_reports_added_table(capsys):
    connector = MagicMock(spec=BaseConnector)
    connector.get_tables.return_value = ["users"]
    connector.get_columns.return_value = [{"name": "id", "type": "INTEGER", "is_pk": True}]
    connector.get_views.return_value = []
    connector.get_foreign_keys.return_value = []
    llm = MagicMock(spec=BaseLLMClient)
    llm.get_description.return_value = "draft"
    wf = DbWorkflow(connector, llm, writer=None, db_profile_name="d")
    assert wf.check() is False  # no sidecar, no output → clean
    # with a stale sidecar → changed
    prev = {"tables": {}, "views": [], "generated_at": "x"}
    SchemaState.save(prev, str(tmp_path / "out.md.schema-state.json"))
    # point wf at the sidecar path and assert check() is True + exit code path
```

(The test needs the workflow to know the sidecar/output path — pass it via `writer_params={"output_filename": ...}` and `check()` reads the sidecar next to it.)

- [ ] **Step 2: Run test to verify it fails**

Expected: FAIL — `check` and `render` do not exist.

- [ ] **Step 3: Minimal implementation**

`DbWorkflow.render()` + `check()` per the Interfaces block; `--check` flag in app.py (mutually exclusive with `--interactive`, mirroring dbt at app.py:402-407), building the workflow with no writer and exiting 1 when `check()` returns True.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest schema_scribe/tests/integration/test_db_workflow.py -v`
Expected: PASS.

- [ ] **Step 5: Commit** (title `feat: add db --check diff gate`)

### Task 6.2: `db --interactive` accept/edit/reject review

Reject semantics: a rejected description is replaced with an empty string (and a `[rejected]` marker where the writer supports it) — the `description` key is **never removed**, because MarkdownWriter hard-indexes `column['description']` (markdown_writer.py:143) and removal would raise `KeyError` mid-write.

**Files:**
- Modify: `schema_scribe/workflows/db_workflow.py`
- Test: `schema_scribe/tests/integration/test_db_workflow.py` (add)

**Interfaces:**
- Consumes: catalog dict from `CatalogGenerator`.
- Produces: `DbWorkflow.run_interactive()` — after generation, for each table summary and each column description, prompts (typer.prompt) accept / edit / reject; rejects → `description = ""`; edits → replace value; then writes via the configured writer. Mirrors dbt `_prompt_user_for_change` (dbt_yaml_writer.py:412-447).

- [ ] **Step 1: Write the failing test**

```python
def test_interactive_applies_edits_and_drops_rejects(monkeypatch):
    connector = MagicMock(spec=BaseConnector)
    connector.get_tables.return_value = ["users"]
    connector.get_columns.return_value = [
        {"name": "id", "type": "INTEGER", "is_pk": True},
        {"name": "email", "type": "VARCHAR", "is_pk": False},
    ]
    llm = MagicMock(spec=BaseLLMClient)
    llm.get_description.side_effect = ["sum draft", "col draft", "col2 draft"]
    writer = MagicMock(spec=BaseWriter)
    wf = DbWorkflow(connector, llm, writer, db_profile_name="d")
    answers = iter(["accept", "edit:NEW DESC", "reject"])
    monkeypatch.setattr(typer, "prompt", lambda *a, **k: next(answers))
    wf.run_interactive()
    catalog = writer.write.call_args.args[0]
    cols = catalog["tables"][0]["columns"]
    assert cols[0]["description"] == "col draft"
    assert cols[1]["description"] == "NEW DESC"
    assert cols[2]["description"] == ""
    assert "description" in cols[2]  # key never removed
```

(Adjust to the actual per-asset prompt order — table summaries and columns each get one prompt; match the loop order in `run_interactive`.)

- [ ] **Step 2: Run test to verify it fails**

Expected: FAIL — `run_interactive` does not exist.

- [ ] **Step 3: Minimal implementation**

`run_interactive()` per the Interfaces block; `--interactive` flag in app.py (mutually exclusive with `--check`).

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
- Produces: `pytest.mark.parametrize("connector_id", ["sqlite", "duckdb"])` real-engine tier — identical fixture schema (two tables with PK + FK, one view) and identical assertions per connector: connect → get_tables → get_columns (types, `is_pk`) → get_views → get_foreign_keys → profile (3 stats, no values) → read-only write-attempt rejection → close. Postgres/MariaDB/Snowflake get driver-mocked variants of the same assertions but are **explicitly not v1-qualified** — qualification requires a real-engine acceptance run recorded in the repo (PRODUCT.md:55-57). The duckdb tier entries are marked `xfail(strict=True)` for the steps that fail on duckdb 1.5.5 (see the suite's `DUCKDB_XFAIL_REASONS`): DuckDB is **not** v1-qualified; only sqlite is.

- [ ] **Step 1: Write the acceptance tests**

Fixture factory in `conftest.py` keyed by connector id; identical assertions per connector; parametrize the real-engine tier over `["sqlite", "duckdb"]` with the duckdb entries marked `xfail(strict=True)` so the suite stays red-forcing until each failing step is fixed.

- [ ] **Step 2: Run to verify sqlite passes; duckdb records expected failures**

Run: `pytest schema_scribe/tests/acceptance -v`
Expected: sqlite PASS against the real engine; duckdb recorded as expected failures (xfail, strict — `get_tables`, `get_views`, `get_foreign_keys` against duckdb 1.5.5); mocked variants PASS.

- [ ] **Step 3: Minimal implementation**

Per the Interfaces block. Record in the suite docstring: **v1-qualified = sqlite only**; duckdb deferred — its real-engine tier currently records expected failures (see the acceptance suite xfails: `SHOW ALL TABLES` row shape, unfiltered internal `duckdb_views()`, sqlite-extension FK visibility); postgres/mariadb/snowflake unqualified until a real-engine acceptance run is executed and recorded (needs CI or a documented manual run).

- [ ] **Step 4: Commit** (title `test: add shared connector acceptance suite`)

### Task 7.2: Test hygiene — dead fixture, workflow mock, prompt assertion

**Files:**
- Modify: `schema_scribe/tests/conftest.py:15-48` (delete the dead `mock_llm_client` fixture — it patches nonexistent modules `schema_scribe.core.db_workflow.init_llm` / `schema_scribe.core.dbt_workflow.init_llm`)
- Modify: `schema_scribe/tests/integration/test_db_workflow.py:28-32` (mock `get_tables` to return `List[str]` to match the real contract)
- Modify: `schema_scribe/tests/integration/test_db_workflow.py:252-265` — **existing prompt assertion update required**: the assertion searches for `"Table: {'name': 'users', 'comment': None}"`, which pins the dict shape; after the mock change the prompt contains the string table name — update the search term accordingly (the prompt for a column of table "users" will contain the table name directly, e.g., `Table: users` — adapt the assertion, keep its intent: find the users.id prompt)

**Interfaces:**
- Produces: no dead fixtures; workflow tests mock the same shapes the real connector produces; the full suite passes.

- [ ] **Step 1: Make the changes per the Files block**

- [ ] **Step 2: Run the full suite**

Run: `./scripts/verify.sh`
Expected: PASS.

- [ ] **Step 3: Commit** (title `test: align workflow mocks with real connector contracts`)

### Slice 7 gate

- [ ] Run `./scripts/verify.sh` — expected: PASS.
- [ ] Merge to parent branch (ff) after a review pass.

---

## Slice 8: Landscape Report (GATED — evidence decides)

User research (docs/superpowers/specs/2026-08-07-db-orientation-problem-research.md) found the author's real problem is "opening the database is scary" — scale + meaningless names. The 3-panel review verdict: direction B (orientation tool) is not a replacement north star — the ERD already ships in `MarkdownWriter`, the real delta is LLM hints, and "understand" is not measurable by deterministic gates — so B is implemented here as a **gated feature**, not a promise change. PRODUCT.md is unchanged.

**GATE (from Slice 0.5 probe):** this slice is implemented only if the probe results show the deterministic pillars work on the legacy fixtures — specifically FK-centrality must produce non-empty results on `legacy-rich` (FK density recorded), and the author's counterfactual judgment must identify a concrete, buildable missing feature. If the probe fails, this slice is deferred and the findings go back to the research spec.

### Task 8.1: Deterministic landscape service

**Files:**
- Create: `schema_scribe/services/landscape.py`
- Test: `schema_scribe/tests/unit/test_landscape.py` (new, using the Slice 0.5 fixtures)

Note: the `db_workflow.py` landscape path is wired in Task 8.2, where the renderer defines the command contract.

**Interfaces:**
- Consumes: `get_tables()` names, `get_columns()` per table, `get_foreign_keys()` pairs (pure function inputs — no DB connection needed beyond collection).
- Produces: `build_landscape(tables, columns_by_table, foreign_keys) -> dict`:
  - `scale`: table count, column count, per-type distribution.
  - `clusters`: tables grouped by naming convention (prefix/suffix regexes: `^dim_`, `^fact_`, `^stg_`, `_audit$`, `^TBL_`, `^T_SLS_`-style, else `"other"`).
  - `core_tables`: FK-centrality ranking (in-degree + out-degree on the FK graph), top N.
  - `relationship_map`: FK graph nodes/edges (isolated tables included as single nodes).
  Deterministic and testable — no LLM in this task.

- [ ] **Step 1: Write the failing test**

```python
def test_landscape_on_legacy_fixture(tmp_path):
    db_path = tmp_path / "legacy.db"
    db_fixtures.build_sqlite(str(db_path), "legacy-rich")
    connector = SQLiteConnector()
    connector.connect({"path": str(db_path)})
    tables = connector.get_tables()
    cols = {t: connector.get_columns(t) for t in tables}
    fks = connector.get_foreign_keys()
    connector.close()
    landscape = build_landscape(tables, cols, fks)
    assert landscape["scale"]["tables"] == len(tables)
    assert landscape["core_tables"]  # non-empty on legacy-rich
    assert set(landscape["relationship_map"]["nodes"]) == set(tables)  # isolated tables included
```

- [ ] **Step 2: Run test to verify it fails**

Expected: FAIL — module does not exist.

- [ ] **Step 3: Minimal implementation**

`landscape.py` per the Interfaces block — pure functions over the collected metadata. Ordering must be deterministic (sort all outputs).

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest schema_scribe/tests/unit/test_landscape.py -v`
Expected: PASS.

- [ ] **Step 5: Commit** (title `feat: add deterministic database landscape analysis`)

### Task 8.2: Landscape output (Markdown) + optional LLM hints

**Files:**
- Modify: `schema_scribe/components/writers/markdown_writer.py` (landscape section) or `schema_scribe/workflows/db_workflow.py` (new `landscape` command path)
- Test: `schema_scribe/tests/unit/writers/test_markdown_writer.py` (extend), `schema_scribe/tests/unit/test_landscape.py` (extend)

**Interfaces:**
- Consumes: `build_landscape` (Task 8.1).
- Produces: a `db --landscape` command path that renders the landscape as Markdown (scale summary, clusters, core tables, relationship map) WITHOUT constructing the LLM client; an optional `--landscape-hints` flag adds LLM name-decoding hints per cluster/top table, subject to the same disclosure rules as Slice 3 (payload disclosed before transmission; provider chosen by the user).

- [ ] **Step 1: Write the failing test**

```python
def test_landscape_render_includes_clusters_and_core(tmp_path):
    ...  # build landscape dict from the legacy fixture, render via MarkdownWriter landscape path
    out = ...  # rendered markdown
    assert "Clusters" in out and "Core tables" in out
```

- [ ] **Step 2: Run test to verify it fails**

Expected: FAIL — no landscape render path.

- [ ] **Step 3: Minimal implementation**

Per the Interfaces block. The hints path reuses the existing prompt/disclosure machinery (Slice 3) — no new egress channels.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest schema_scribe/tests/unit/test_landscape.py schema_scribe/tests/unit/writers/test_markdown_writer.py -v`
Expected: PASS.

- [ ] **Step 5: Commit** (title `feat: render database landscape with optional LLM hints`)

### Slice 8 gate

- [ ] `./scripts/verify.sh` — expected: PASS.
- [ ] Probe results and this slice's existence both recorded; merge to parent (ff).
- [ ] If the gate probe rejected the slice, record the decision in the research spec and remove Slice 8 from the active plan instead of shipping a hedged feature.

---

## Out Of Scope (tracked elsewhere)

- Direction B as a product promise (PRODUCT.md stays A) — the Landscape Report is a gated feature (Slice 8), not a contract change.
- Evaluation fixture + human review rubric (PRODUCT.md:114-116) — deferred, docs/TESTING.md:30-34.
- Benchmark harness: query count / elapsed / LLM calls (PRODUCT.md:117) — deferred; the existing `call_count == 12` assertion (test_db_workflow.py:277) is the seed.
- Drift checks in CI, dbt removed-model detection (`dbt --check` reverse set difference, dbt_yaml_writer.py:100-104) — product-sequence step 2.
- PostgresCommentWriter connection-lifecycle bug (db_workflow.py:71-76 vs postgres_comment_writer.py:73-76) — adapter qualification.
- DuckDB connector real-engine qualification (the acceptance suite's real-engine tier records expected failures on duckdb 1.5.5: `get_tables`, `get_views`, `get_foreign_keys` — see `DUCKDB_XFAIL_REASONS`) — adapter qualification; only sqlite is v1-qualified.
- Snowflake real-engine qualification and read-only enforcement (declared not v1-supported; docstring updated in Task 1.4) — adapter qualification.
- dbt_markdown_writer and mermaid_writer atomicity (they remain truncate-on-open; PRODUCT.md:50 names only Markdown/JSON for v1) — adapter qualification.
- Hosted demo — product-sequence step 3.
