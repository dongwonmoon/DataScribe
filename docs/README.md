# Documentation Index

Use active documents for current project truth. Designs and plans explain how a
decision or change was developed, but do not override active owners or source.

| Document | Owns |
| --- | --- |
| `PRODUCT.md` | First user, product promise, v1 scope, trust boundaries, and success criteria |
| `CURRENT_FOCUS.md` | Current workstream, completion gate, and deferred work |
| `ARCHITECTURE.md` | Runtime boundaries established by current source |
| `TESTING.md` | Setup, deterministic verification, and evaluation policy |
| `superpowers/specs/` | Approved design history; not current product truth |
| `superpowers/plans/` | Execution plans; not current product truth |

## Capture Rule

Update an owner only when current scope, procedure, architecture, risk, or
evidence changes. Keep transient branch state and command traces in Git history.
Create a new active document only when no existing owner can hold the contract
without mixing responsibilities.
