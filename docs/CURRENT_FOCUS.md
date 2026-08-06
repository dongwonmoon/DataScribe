# Current Focus

## Active Workstream

- Branch: `plan/v1-engine-trustworthiness`
- Goal: review and approve the v1 engine trustworthiness plan
  (`docs/superpowers/plans/2026-08-06-v1-engine-trustworthiness.md`), the first
  implementation plan derived from the product contract gap audit.
- Gate: the user approves the plan; implementation begins slice by slice with
  `./scripts/verify.sh` as the per-slice gate.

## Evidence

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
