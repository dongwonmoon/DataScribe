# Current Focus

## Active Workstream

- Branch: `dev` (integration). The v1 engine trustworthiness plan
  (`docs/superpowers/plans/2026-08-06-v1-engine-trustworthiness.md`) is
  COMPLETE — all slices 0-8 executed and merged (33 commits).
- Next: user decision on the following phase — product-sequence step 2
  (bounded schema-change/drift checks) vs portfolio/marketing work vs
  something else.
- Gate: `./scripts/verify.sh` (230 passed + 3 xfails on `dev`).

## Evidence

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

- DuckDB connector real-engine qualification (get_tables/get_views/get_foreign_keys against duckdb 1.5.5; FK path redesign) — acceptance suite pins it with xfail(strict)
- Engine hardening and optional-dependency boundaries
- Real-model evaluation fixture and human-review rubric
- Benchmark harness for query count, elapsed time, and LLM call count
- Hosted service, authentication, tenancy, and credential handling
- Repository transfer to the `schemascribe` GitHub Organization
