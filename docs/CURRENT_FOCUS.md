# Current Focus

## Active Workstream

- Branch: `chore/project-foundation`
- Goal: portable setup, canonical verification, indexed active documents, and
  tool-neutral AI instructions.
- Gate: clean-clone setup is documented and `./scripts/verify.sh` passes locally
  and in GitHub Actions.

## Evidence

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

- Product brief and release scope
- Engine hardening and optional-dependency boundaries
- Real-model evaluation fixture and human-review rubric
- Hosted service, authentication, tenancy, and credential handling
- Repository transfer to the `schemascribe` GitHub Organization
