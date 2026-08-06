# SchemaScribe Project Foundation Design

## Status

Approved direction, pending written-spec review. This document covers repository
workflow and AI-assisted development foundations only. It does not finalize the
SchemaScribe product plan.

## Context

SchemaScribe already ships a Python package, CLI, FastAPI entry point, database
connectors, LLM adapters, writers, and 72 passing tests when every optional
dependency is installed. A clean clone is not yet self-explanatory or fully
reproducible:

- `.python-version` names a developer-local pyenv virtual environment rather
  than a portable Python version;
- the declared `test` extra cannot collect the full suite because component
  packages eagerly import optional integrations;
- CI runs pytest but has no single local verification entry point covering
  formatting, types, packaging, and CLI smoke behavior;
- the repository has no indexed source-of-truth documents or shared,
  tool-neutral instructions for AI assistants.

The repository is moving from `dongwonmoon/SchemaScribe` to the dedicated
`schemascribe` GitHub Organization. The transfer itself is user-owned because
the current GitHub CLI credential is expired.

## Goal

Create the smallest shared development foundation that lets a human, Codex, or
OpenCode-based agent recover project context, make a narrow change, and run the
same deterministic verification command.

## Non-goals

- Finalize the product brief or roadmap.
- Copy Morrow-specific mobile, backup, checkpoint, queue, or agent policies.
- Add custom agents, project Skills, or hooks without a repeated failure that
  justifies them.
- Split the repository into core, server, documentation, or website repos.
- Redesign the runtime or fix every issue discovered during the baseline audit.
- Add a hosted multi-user service.

## Repository Instructions

The root `AGENTS.md` will contain only tool-neutral project truth:

- the current user and product promise once approved;
- privacy and read-only database access constraints;
- repository workflow and narrow-change rules;
- the canonical verification command;
- the active documentation index and ownership rules;
- the distinction between deterministic tests and real-LLM evaluation.

It will not name particular model tiers or require subagents. Tool-specific
behavior will stay outside the shared file:

- `.codex/config.toml` for Codex configuration when the project has an actual
  setting to own;
- `opencode.json` for OpenCode configuration;
- `agent-instructions/opencode.md` for OpenCode-specific collaboration rules.

No empty `.codex/config.toml` or `agent-instructions/codex.md` will be added
initially. Codex can use the shared `AGENTS.md`; another file would duplicate
instructions without an observed need.

## Active Documentation

The first documentation set will remain deliberately small:

- `docs/README.md`: document index and capture rules;
- `docs/CURRENT_FOCUS.md`: immediate workstream, gate, and deferred work;
- `docs/ARCHITECTURE.md`: current runtime boundaries established by code;
- `docs/TESTING.md`: deterministic verification and occasional evaluation;
- this design document: rationale for the initial project foundation.

`docs/PRODUCT.md` will be created only after the user reviews and approves the
product brief. An ADR directory, roadmap, test matrix, and release guide will
be added only when the project produces decisions or procedures that need
those distinct owners.

## Deterministic Verification

The repository will expose `./scripts/verify.sh` as its canonical command,
implemented without a new task-runner dependency. The first version will run
the checks the repository can support honestly:

1. unit and integration tests with fixture databases and mocked LLMs;
2. an sdist and wheel build with `python -m build`;
3. a non-network `schema-scribe --help` smoke check.

Formatting, linting, and static type checking will enter the command only after
their tools and configuration are selected and the existing codebase has a
known baseline. The foundation change must not disguise a repository-wide
format or type migration as setup work.

Real Ollama, Google, and OpenAI calls will not run in the deterministic gate.
Quality, latency, token usage, and database-query cost will be measured by a
separate explicit evaluation command once its fixture and rubric are designed.

## Environment Contract

The portable Python contract will be Python 3.12, which is already supported by
the package and CI. Setup instructions will create a repository-local `.venv`
and install the declared development dependencies. The personal pyenv virtual
environment name in `.python-version` will be replaced by a portable version
contract.

The optional-dependency collection failure is recorded as an engine-boundary
defect, not silently solved by declaring every integration mandatory. The
initial verification command may use the full extra to preserve the current
green baseline, while a later narrow change makes optional integrations truly
optional and allows the test extra to stand alone.

## Git And Organization Workflow

- `main` remains the stable branch.
- Product and runtime changes use narrow topic branches.
- The project-foundation change stays on `chore/project-foundation`.
- Pushes and repository transfer remain user-approved external actions.
- After transfer, local `origin`, README clone links, CI, releases, and PyPI
  metadata are checked against `schemascribe/SchemaScribe`.

## Validation

The foundation is complete when:

- a clean clone has documented, reproducible setup steps;
- the canonical verification command passes locally and in GitHub Actions;
- shared instructions contain no Codex-only or OpenCode-only behavior;
- tool-specific files do not repeat product truth;
- active documents are indexed and do not claim an unapproved product plan;
- no custom Skill, agent, hook, or harness subsystem was added without current
  evidence of need.

## Deferred Decisions

- Exact product promise and first release scope;
- evaluation fixture and human-review rubric;
- optional raw-value sampling and redaction policy;
- hosted job execution, authentication, tenancy, and credential handling;
- custom project agents or Skills;
- repository split and dedicated documentation website.
