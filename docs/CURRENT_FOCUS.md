# Current Focus

## Active Workstream

- Branch: per-slice topic branches off `dev`, executing
  `docs/superpowers/plans/2026-08-06-v1-engine-trustworthiness.md`.
- Goal: execute the v1 engine trustworthiness plan slice by slice (Slices 0,
  0.5, 1-8). Slice 8 (Landscape Report) is gated on the Slice 0.5 fixture
  probe; PRODUCT.md stays direction A.
- Gate: `./scripts/verify.sh` after every slice; merge each slice to `dev`
  (ff) with a git-log check for missed commits.

## Evidence

- The user approved the v1 engine plan on 2026-08-07, including the added
  Slice 0.5 (Fixture Workshop) and gated Slice 8 (Landscape Report).
- User research (docs/superpowers/specs/2026-08-07-db-orientation-problem-research.md)
  established the author's lived problem: "opening the database is scary"
  (scale + meaningless names). A 3-panel blind review verified the Landscape
  direction is computationally feasible and trust-boundary-safe but rejected
  it as a north-star replacement; PRODUCT.md's promise (A) is unchanged.
- The user approved `docs/PRODUCT.md` as the active product contract on
  2026-08-06, completing the `docs/v1-product-contract` workstream.
- A four-track source audit against the contract found: read-only enforcement
  missing on 4/5 connectors, no pre-LLM disclosure, non-atomic writer writes,
  no change classification, no review loop for `db`, and no shared connector
  acceptance suite. Audit evidence is embedded in the plan document.
- Local foundation gate passed on 2026-08-06 with Python 3.12.13.
- `./scripts/verify.sh` passed 72 tests, built the sdist and wheel, exercised
  CLI help without provider calls, and validated the OpenCode instruction link.
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

- Engine hardening and optional-dependency boundaries
- Real-model evaluation fixture and human-review rubric
- Benchmark harness for query count, elapsed time, and LLM call count
- Hosted service, authentication, tenancy, and credential handling
- Repository transfer to the `schemascribe` GitHub Organization
