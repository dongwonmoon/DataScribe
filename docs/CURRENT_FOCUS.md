# Current Focus

## Active Workstream

- Branch: `dev` (integration). The v1 engine trustworthiness plan
  (`docs/superpowers/plans/2026-08-06-v1-engine-trustworthiness.md`) is
  COMPLETE — all slices 0-8 executed and merged (33 commits).
- Live-demo round (2026-08-07) verified the real-LLM path end to end and
  shipped the Google client migration (see Evidence).
- Next: user decision on the following phase — product-sequence step 2
  (bounded schema-change/drift checks) vs portfolio/marketing work vs
  something else.
- Gate: `./scripts/verify.sh` (238 passed + 3 xfails on `dev`).

## Evidence

- Live real-LLM demo (2026-08-07) against the Google free tier produced the
  first clean end-to-end catalog (gemini-3.1-flash-lite): read-only → profile
  → disclosure → LLM → atomic write → sidecar → ERD. Findings shipped as
  fixes on `dev`:
  - Deprecated `google.generativeai` cannot parse reasoning-model responses
    (thoughtSignature); migrated to `google.genai` (verified: gemini-3.5-flash
    works via REST/SDK; free tier 5 RPM for it, flash-lite tolerates rapid
    calls).
  - gemma-4-26b/31b responses have a separate `thought` part; the old SDK
    read it and every description looked like a prompt echo. The new SDK
    returns the answer part only — the model was answering correctly all
    along (user suspicion confirmed).
  - Reasoning models emit verbose thoughts: 200-token budgets yield
    thought-only responses; budgets raised to 512 with a retry-once at
    double budget when `text` is None.
  - Free-tier quotas (5-15 RPM per model) abort multi-call runs; the Google
    client now retries 429 with the server-advised delay (up to 3).
  - `requirements.txt` regenerated via `uv pip compile` (pip-compile is
    broken against current pip); clean-install smoke test passed.
  - First real-model evaluation data recorded (re-verified after the SDK
    migration): gemma-4-26b-a4b-it and gemma-4-31b-it answer correctly via
    `google.genai` — earlier "prompt echo"/noise observations were the old
    SDK reading the separate `thought` part, not model behavior. Their
    verbose thinking still consumes token budgets (handled by the
    doubled-budget retry) and free-tier quota. gemini-3.1-flash-lite = clean,
    fast, quota-tolerant. Per-model prompt variants rejected by user decision
    (single model-agnostic prompt set).
- The full plan executed with subagent-driven development: every task passed
  a task review; two final whole-branch reviews (slices 0-7, slice 8) both
  passed after fix waves. The shared acceptance suite found DuckDB fails the
  real-engine tier on 1.5.5 — v1-supported connectors = sqlite only
  (PRODUCT.md:55-57); DuckDB qualification tracked as deferred.
- The Landscape Report (Slice 8) shipped as a gated feature: deterministic
  grouping (192 tables → 48 groups on the legacy fixture), FK-centrality
  hubs, relationship map; optional `--landscape-hints` with disclosure and
  draft framing.
- The user approved the v1 engine plan on 2026-08-07, including the added
  Slice 0.5 (Fixture Workshop) and gated Slice 8 (Landscape Report).
- User research (docs/superpowers/specs/2026-08-07-db-orientation-problem-research.md)
  established the author's lived problem: "opening the database is scary"
  (scale + meaningless names). A 3-panel blind review verified the Landscape
  direction is computationally feasible and trust-boundary-safe but rejected
  it as a north-star replacement; PRODUCT.md's promise (A) is unchanged.
- The user approved `docs/PRODUCT.md` as the active product contract on
  2026-08-06, completing the `docs/v1-product-contract` workstream.
- Local foundation gate passed on 2026-08-06 with Python 3.12.13.
- GitHub Actions remains unverified until the branch is pushed.
- Repository transfer remains pending and user-owned.

## Known Baseline

- 72 tests pass when all optional dependencies are installed.
- Installing only the `test` extra cannot collect the full suite because
  component packages eagerly import optional integrations.
- `google.generativeai` is deprecated and emits a warning.
- Package build currently includes the test modules in the wheel.

These are recorded observations, not commitments to fix them in the foundation
workstream.

## Deferred

- API server security (GitHub issue #3 #2/#4/#6): authentication, dbt path
  confinement, cache-file permissions, 500 error detail — PRODUCT.md:84,101-103
  exclude auth/hosted work from v1; revisit with Phase 3 hosted work. README
  now binds the serve example to 127.0.0.1 with an unauthenticated warning.
- Prompt-injection via poisoned metadata (issue #3 note, panel C10): LLM text
  and DB metadata flow into generated docs/YAML; the review loop (draft
  framing) mitigates; document explicitly when the eval work lands.
- Snowflake + PostgresCommentWriter identifier quoting (issue #3 #1 remainder):
  deferred with adapter qualification (neither is v1-qualified).
- DuckDB connector real-engine qualification (get_tables/get_views/get_foreign_keys against duckdb 1.5.5; FK path redesign) — acceptance suite pins it with xfail(strict)
- Engine hardening and optional-dependency boundaries
- Real-model evaluation fixture and human-review rubric
- Benchmark harness for query count, elapsed time, and LLM call count
- Hosted service, authentication, tenancy, and credential handling
- Repository transfer to the `schemascribe` GitHub Organization
