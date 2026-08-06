# Project Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give SchemaScribe a portable Python setup, one deterministic verification command, indexed active documentation, and tool-neutral AI instructions with a narrow OpenCode supplement.

**Architecture:** Keep product truth and repository workflow in root `AGENTS.md` and active documents under `docs/`. Put OpenCode-only behavior behind `opencode.json`, and make `./scripts/verify.sh` the common executable contract for humans, agents, and GitHub Actions. Do not introduce a harness service, custom agent, Skill, hook, or product/runtime refactor.

**Tech Stack:** Python 3.12, setuptools, uv-compatible virtual environments, pytest, PyPA build, Bash, GitHub Actions, OpenCode JSON configuration.

## Global Constraints

- Product planning is out of scope; do not create `docs/PRODUCT.md` in this plan.
- Preserve the current runtime and the 72-test green baseline.
- Keep `main` stable and implement only on `chore/project-foundation`.
- The canonical local and CI command is `./scripts/verify.sh`.
- Real Ollama, Google, and OpenAI calls must not run in deterministic verification.
- Root `AGENTS.md` must not name model tiers or require subagents.
- Do not add custom agents, project Skills, hooks, checkpoint, or queue behavior.
- Keep credentials, provider selection, and personal configuration out of Git.
- Repository transfer to `schemascribe/SchemaScribe` happens only after the branch is verified.

---

## File Structure

### Create

- `AGENTS.md`: tool-neutral recovery, scope, safety, workflow, and verification contract.
- `docs/README.md`: active document index and capture rules.
- `docs/CURRENT_FOCUS.md`: current foundation workstream and completion gate.
- `docs/ARCHITECTURE.md`: current connector → service → writer boundaries established by source.
- `docs/TESTING.md`: setup, deterministic verification, and deferred real-model evaluation.
- `scripts/verify.sh`: canonical deterministic verification entry point.
- `opencode.json`: loads the OpenCode-specific instruction supplement.
- `agent-instructions/opencode.md`: OpenCode-only scope and credential rules.

### Modify

- `.python-version`: replace the local pyenv environment name with portable `3.12`.
- `pyproject.toml`: add `build>=1.2.2` to the `test` and `all` extras.
- `.github/workflows/run_tests.yml`: install declared extras and call the canonical verification script.
- `README.md`: add contributor setup and link to the active document index; keep product copy otherwise unchanged.
- `scripts/verify.sh`: extend it in Task 3 to validate `opencode.json` and its instruction target.

### Intentionally Unchanged

- `schema_scribe/**`: engine and optional-import defects are separate runtime work.
- `.codex/config.toml`: no project-specific Codex setting has been established.
- `.agents/**`: no repeated need justifies a project Skill or custom agent.
- `requirements.txt`: dependency-source cleanup needs a separate decision; this plan uses `pyproject.toml` as the declared package contract.

---

### Task 1: Portable Environment And Canonical Verification

**Files:**
- Create: `scripts/verify.sh`
- Modify: `.python-version`
- Modify: `pyproject.toml`

**Interfaces:**
- Consumes: the existing `schema_scribe/tests` suite and `schema_scribe.main` CLI module.
- Produces: executable `./scripts/verify.sh`; optional `SCHEMA_SCRIBE_PYTHON` override containing a Python command or executable path.

- [ ] **Step 1: Capture the current failing portable-environment behavior**

Run:

```bash
pyenv exec python --version
```

Expected before the change: failure stating that pyenv version `schema-scribe` is not installed.

- [ ] **Step 2: Capture the missing verification entry point**

Run:

```bash
./scripts/verify.sh
```

Expected before the change: exit 127 because the file does not exist.

- [ ] **Step 3: Make the Python version portable**

Replace the complete contents of `.python-version` with:

```text
3.12
```

- [ ] **Step 4: Add the build dependency to both development extras**

In `pyproject.toml`, add the same dependency to `[project.optional-dependencies].test` and `.all`:

```toml
"build>=1.2.2",
```

Do not reorder or broaden any existing runtime dependency.

- [ ] **Step 5: Create the canonical verification script**

Create `scripts/verify.sh` with exactly this behavior:

```bash
#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python_bin="${SCHEMA_SCRIBE_PYTHON:-${repo_root}/.venv/bin/python}"

if ! command -v "${python_bin}" >/dev/null 2>&1 && [[ ! -x "${python_bin}" ]]; then
  echo "SchemaScribe Python not found: ${python_bin}" >&2
  echo "Run: uv venv --python 3.12 .venv && uv pip install --python .venv/bin/python -e '.[all]'" >&2
  exit 2
fi

cd "${repo_root}"
"${python_bin}" -m pytest schema_scribe/tests
"${python_bin}" -m build
"${python_bin}" -m schema_scribe.main --help >/dev/null
```

- [ ] **Step 6: Make the script executable**

Run:

```bash
chmod +x scripts/verify.sh
```

- [ ] **Step 7: Refresh the editable environment from declared dependencies**

Run:

```bash
uv pip install --python .venv/bin/python -e '.[all]'
```

Expected: `build` is installed and SchemaScribe remains editable.

- [ ] **Step 8: Run the canonical verification**

Run:

```bash
./scripts/verify.sh
```

Expected: 72 tests pass, sdist and wheel are created under ignored `dist/`, and CLI help exits zero without a network call.

- [ ] **Step 9: Confirm generated files are ignored and the diff is scoped**

Run:

```bash
git status --short
git diff --check
```

Expected: only `.python-version`, `pyproject.toml`, and `scripts/verify.sh` are tracked changes; `.venv`, `build`, `dist`, and egg-info are absent.

- [ ] **Step 10: Commit the environment contract**

```bash
git add .python-version pyproject.toml scripts/verify.sh
git commit -m "build: add portable verification entry point"
```

---

### Task 2: Tool-Neutral Instructions And Active Documents

**Files:**
- Create: `AGENTS.md`
- Create: `docs/README.md`
- Create: `docs/CURRENT_FOCUS.md`
- Create: `docs/ARCHITECTURE.md`
- Create: `docs/TESTING.md`

**Interfaces:**
- Consumes: `./scripts/verify.sh` from Task 1 and the existing connector, service, workflow, writer, CLI, and FastAPI source boundaries.
- Produces: a recovery path of `AGENTS.md` → `docs/README.md` → `docs/CURRENT_FOCUS.md`, with one owner document for architecture and testing truth.

- [ ] **Step 1: Write a documentation-contract check that must initially fail**

Run:

```bash
for path in AGENTS.md docs/README.md docs/CURRENT_FOCUS.md docs/ARCHITECTURE.md docs/TESTING.md; do test -f "$path" || exit 1; done
```

Expected before creation: exit 1.

- [ ] **Step 2: Create root `AGENTS.md`**

The file must state these exact contracts in concise prose:

```text
Recovery order: inspect Git state, then read README.md, docs/README.md, and docs/CURRENT_FOCUS.md.
Current product scope is not inferred from dormant code or README marketing copy.
Database access is read-only by default; do not add raw-value sampling or external transmission without an approved product and privacy decision.
Use narrow topic branches; preserve unrelated changes; do not commit directly to main.
Run ./scripts/verify.sh before claiming completion.
Deterministic verification uses fixtures and mocked LLMs; real-model evaluation is separate.
Update an existing owner document when its contract changes; do not create duplicate inventories.
Do not add a Skill, custom agent, hook, dependency, or abstraction without an observed project need.
```

It must point to `docs/README.md` for document ownership. It must not mention Sol, Luna, DeepSeek, Best Model, Cost Effective Model, or mandatory subagent delegation.

- [ ] **Step 3: Create `docs/README.md`**

Include a table with these owners and no additional active document:

```text
CURRENT_FOCUS.md — current workstream, gate, deferred work
ARCHITECTURE.md — established runtime boundaries
TESTING.md — setup, deterministic verification, evaluation policy
superpowers/specs/... — approved design history, not current product truth
superpowers/plans/... — execution plans, not current product truth
```

Add a capture rule: update an owner only when current scope, procedure, architecture, risk, or evidence changes; keep transient execution details in Git history.

- [ ] **Step 4: Create `docs/CURRENT_FOCUS.md`**

Record only the active foundation work:

```text
Branch: chore/project-foundation
Goal: portable setup, canonical verification, indexed docs, tool-neutral AI instructions
Gate: clean-clone setup documented; ./scripts/verify.sh passes locally and in CI
Deferred: product brief, engine hardening, evaluation fixture, hosted service, repository transfer
Known baseline: 72 tests pass with all extras; test-only install cannot collect because optional integrations are eagerly imported; google.generativeai is deprecated
```

- [ ] **Step 5: Create `docs/ARCHITECTURE.md` from current source**

Document this current data flow without proposing a refactor:

```text
Typer CLI or FastAPI entry point
→ workflow selects configured connector, LLM client, and writer through factory registries
→ connector returns schema metadata and column profile statistics
→ catalog service builds prompts and obtains LLM descriptions
→ writer renders or applies the catalog
```

State that `schema_scribe/core/interfaces.py` owns component contracts,
`schema_scribe/core/factory.py` owns registry selection, and
`schema_scribe/workflows/` owns orchestration. Record the eager-import optional dependency problem as a known boundary defect, not intended architecture.

- [ ] **Step 6: Create `docs/TESTING.md`**

Include these executable setup commands:

```bash
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -e '.[all]'
./scripts/verify.sh
```

Define deterministic verification as pytest, package build, and CLI help with no real provider calls. Define real-model quality, latency, tokens, cost, query count, and human acceptance rate as deferred evaluation work that does not run in CI.

- [ ] **Step 7: Run document checks**

Run:

```bash
for path in AGENTS.md docs/README.md docs/CURRENT_FOCUS.md docs/ARCHITECTURE.md docs/TESTING.md; do test -s "$path" || exit 1; done
! rg -n "Sol|Luna|DeepSeek|Best Model|Cost Effective Model|must use subagent" AGENTS.md
git diff --check
./scripts/verify.sh
```

Expected: all commands exit zero and the existing runtime suite remains green.

- [ ] **Step 8: Commit the shared project truth**

```bash
git add AGENTS.md docs/README.md docs/CURRENT_FOCUS.md docs/ARCHITECTURE.md docs/TESTING.md
git commit -m "docs: add shared project operating contract"
```

---

### Task 3: OpenCode-Specific Boundary

**Files:**
- Create: `opencode.json`
- Create: `agent-instructions/opencode.md`
- Modify: `scripts/verify.sh`

**Interfaces:**
- Consumes: root `AGENTS.md` as shared truth.
- Produces: OpenCode config whose `instructions` array contains only `agent-instructions/opencode.md`; deterministic validation that the JSON and referenced file exist.

- [ ] **Step 1: Capture the absent OpenCode configuration**

Run:

```bash
test -f opencode.json && test -f agent-instructions/opencode.md
```

Expected before creation: exit 1.

- [ ] **Step 2: Create `opencode.json`**

Use the already-proven Morrow configuration shape:

```json
{
  "$schema": "https://opencode.ai/config.json",
  "instructions": ["agent-instructions/opencode.md"]
}
```

- [ ] **Step 3: Create `agent-instructions/opencode.md`**

The supplement must say:

```text
Root AGENTS.md and active owner documents remain authoritative.
Default work to bounded implementation, testing, performance, efficiency, maintainability, and code mapping.
Do not decide product scope, privacy, raw-data sampling, hosted credential handling, or architecture policy without explicit authorization.
Use OpenCode-native capabilities only; do not claim Codex Skills, hooks, agents, or tool results.
Keep provider credentials, personal model selection, and authentication out of committed configuration.
Keep changes small and explain material algorithm or architecture choices so the user can learn from them.
```

Do not repeat verification commands or product facts already owned by shared documents.

- [ ] **Step 4: Extend `scripts/verify.sh` with configuration validation**

Append these checks after CLI smoke verification:

```bash
"${python_bin}" -m json.tool opencode.json >/dev/null
"${python_bin}" - <<'PY'
import json
from pathlib import Path

config = json.loads(Path("opencode.json").read_text())
instructions = config.get("instructions")
assert instructions == ["agent-instructions/opencode.md"]
assert Path(instructions[0]).is_file()
PY
```

- [ ] **Step 5: Run focused and full validation**

Run:

```bash
.venv/bin/python -m json.tool opencode.json >/dev/null
test -s agent-instructions/opencode.md
./scripts/verify.sh
git diff --check
```

Expected: all commands exit zero.

- [ ] **Step 6: Commit the OpenCode boundary**

```bash
git add opencode.json agent-instructions/opencode.md scripts/verify.sh
git commit -m "chore: separate OpenCode instructions"
```

---

### Task 4: CI And Contributor Onboarding

**Files:**
- Modify: `.github/workflows/run_tests.yml`
- Modify: `README.md`

**Interfaces:**
- Consumes: `./scripts/verify.sh` and the setup contract from `docs/TESTING.md`.
- Produces: GitHub Actions invoking the same verification command as local development; README contributor path into active docs.

- [ ] **Step 1: Demonstrate current CI drift from the canonical command**

Run:

```bash
rg -n "scripts/verify.sh" .github/workflows/run_tests.yml
```

Expected before modification: no match and non-zero exit.

- [ ] **Step 2: Align GitHub Actions with declared dependencies and verification**

Keep the Python 3.11 and 3.12 matrix. Replace the install and test steps with:

```yaml
      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          python -m pip install -e ".[all]"

      - name: Verify
        env:
          SCHEMA_SCRIBE_PYTHON: python
          OPENAI_API_KEY: ci_test_key
          GOOGLE_API_KEY: ci_test_key
          SNOWFLAKE_PASSWORD: ci_test_key
          MARIADB_PASSWORD: ci_test_key
          CONFLUENCE_API_TOKEN: ci_test_key
        run: ./scripts/verify.sh
```

Do not add a real provider credential or network integration test.

- [ ] **Step 3: Add a contributor setup section to `README.md`**

After the existing source-install instructions, add:

````markdown
### Contributor setup

```bash
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -e '.[all]'
./scripts/verify.sh
```

Project scope, architecture, and testing ownership are indexed in
[`docs/README.md`](docs/README.md).
````

Do not rewrite the product headline or feature list in this task.

- [ ] **Step 4: Validate the workflow text and local gate**

Run:

```bash
rg -n "scripts/verify.sh|\[all\]" .github/workflows/run_tests.yml README.md
./scripts/verify.sh
git diff --check
```

Expected: workflow and README both reference the canonical setup, and verification passes.

- [ ] **Step 5: Commit CI and onboarding alignment**

```bash
git add .github/workflows/run_tests.yml README.md
git commit -m "ci: align automation with local verification"
```

---

### Task 5: Close The Foundation Branch And Prepare Transfer

**Files:**
- Modify: `docs/CURRENT_FOCUS.md`
- Modify after transfer: `README.md`
- Modify local Git config after transfer: `origin` URL

**Interfaces:**
- Consumes: all previous task deliverables.
- Produces: a verified branch ready for review and exact user-owned transfer commands.

- [ ] **Step 1: Run final local verification**

Run:

```bash
./scripts/verify.sh
git diff --check
git status --short
git log --oneline --decorate -5
```

Expected: verification passes, no unstaged changes exist before the focus update, and the branch contains one coherent commit per task.

- [ ] **Step 2: Update the active gate evidence**

In `docs/CURRENT_FOCUS.md`, mark the local foundation gate complete only with the actual test count and command result. Keep GitHub Actions and Organization transfer pending until observed.

- [ ] **Step 3: Commit the evidence update**

```bash
git add docs/CURRENT_FOCUS.md
git commit -m "docs: record project foundation evidence"
```

- [ ] **Step 4: Re-run final verification after the documentation commit**

Run:

```bash
./scripts/verify.sh
git status --short
```

Expected: verification passes and the working tree is clean.

- [ ] **Step 5: Give the user the authenticated transfer command**

GitHub CLI has no `gh repo transfer` command. After the user authenticates with `gh auth login -h github.com`, the supported REST call is:

```bash
gh api \
  --method POST \
  -H "Accept: application/vnd.github+json" \
  -H "X-GitHub-Api-Version: 2026-03-10" \
  /repos/dongwonmoon/SchemaScribe/transfer \
  -f new_owner=schemascribe
```

This is an external ownership change and must be executed by the user.

- [ ] **Step 6: Verify transfer and update local origin**

After GitHub reports the transfer complete, run:

```bash
gh repo view schemascribe/SchemaScribe --json nameWithOwner,url,defaultBranchRef
git remote set-url origin https://github.com/schemascribe/SchemaScribe.git
git remote -v
```

Expected: GitHub reports `schemascribe/SchemaScribe` and both fetch/push URLs use the Organization.

- [ ] **Step 7: Update the README clone URL after transfer**

Change only:

```text
https://github.com/dongwonmoon/SchemaScribe.git
```

to:

```text
https://github.com/schemascribe/SchemaScribe.git
```

Then run `./scripts/verify.sh`, commit with `docs: update repository owner links`, and push only when the user explicitly requests it.

---

## Completion Evidence

- `./scripts/verify.sh` passes locally from the documented `.venv`.
- GitHub Actions invokes the same script on Python 3.11 and 3.12.
- Root instructions are tool-neutral and indexed active documents own current truth.
- OpenCode loads only its narrow supplement.
- No product plan, engine refactor, custom Skill, custom agent, hook, or harness subsystem was added.
- Repository transfer and new remote are observed rather than assumed.
