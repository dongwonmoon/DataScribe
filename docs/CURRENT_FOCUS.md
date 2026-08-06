# Current Focus

## Active Workstream

- Branch: `docs/v1-product-contract`
- Goal: establish the first user, v1 product promise, trust boundaries,
  non-goals, delivery sequence, and measurable success criteria.
- Gate: the user reviews and approves `docs/PRODUCT.md` as the active product
  contract.

## Evidence

- The user selected a local database documentation engine before bounded
  schema-change checks and a thin hosted demonstration.
- The proposed contract is recorded in `docs/PRODUCT.md`; implementation has
  not been authorized by this documentation step.
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
- Hosted service, authentication, tenancy, and credential handling
- Repository transfer to the `schemascribe` GitHub Organization
