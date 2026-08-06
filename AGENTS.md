# SchemaScribe Repository Instructions

## Recover Context

Before repository work:

1. Inspect Git status, branch, HEAD, and preserve unrelated changes.
2. Read `README.md`, `docs/README.md`, and `docs/CURRENT_FOCUS.md`.
3. Use the document index to select only the owner document for the question.
4. Inspect relevant source and tests before proposing a change.

Do not infer current product scope from dormant code or README marketing copy.
Historical designs and plans are evidence, not active product truth.

## Protect Data Boundaries

- Database access is read-only by default.
- Do not add raw-value sampling or transmit database values to an external model
  without an approved product and privacy decision.
- Keep credentials, provider selection, and personal configuration out of Git.
- Treat generated descriptions as reviewable drafts, not verified business facts.

## Work Narrowly

- Use a narrow topic branch and do not commit scoped work directly to `main`.
- Preserve unrelated user changes and avoid drive-by refactors.
- Prefer existing interfaces and dependencies over speculative abstractions.
- Stop when the approved behavior and verification gate are satisfied.
- Do not add a Skill, custom agent, hook, dependency, or harness subsystem
  without an observed project need.

## Verify And Document

- Run `./scripts/verify.sh` before claiming completion.
- Deterministic verification uses fixture databases and mocked LLMs. Real-model
  quality, latency, cost, and token evaluation are separate explicit work.
- Use `docs/README.md` to find the document that owns a changed contract.
- Update an existing owner document when current scope, procedure,
  architecture, risk, or evidence changes. Do not create duplicate inventories.
- Keep transient execution details in Git history rather than active documents.

## Tool Neutrality

This file contains shared project truth for humans and all coding assistants.
Tool-specific instructions may supplement it but must not override it. Do not
commit personal model choices, authentication, or provider credentials.
