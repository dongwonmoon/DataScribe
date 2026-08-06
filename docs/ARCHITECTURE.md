# Architecture

This document records current runtime boundaries established by source. It does
not make the dormant FastAPI surface an approved hosted product.

## Data Flow

```text
Typer CLI or FastAPI entry point
  → workflow selects a connector, LLM client, and writer through registries
  → connector returns schema metadata and column profile statistics
  → catalog service builds prompts and obtains LLM descriptions
  → writer renders or applies the resulting catalog
```

## Ownership

- `schema_scribe/core/interfaces.py` defines connector, LLM client, and writer
  contracts.
- `schema_scribe/core/factory.py` owns registry-based component selection.
- `schema_scribe/workflows/` owns orchestration for database, dbt, and lineage
  commands.
- `schema_scribe/services/` transforms parsed metadata into catalog and lineage
  results.
- `schema_scribe/components/` contains database, LLM, and output adapters.
- `schema_scribe/app.py` and `schema_scribe/server/main.py` expose CLI and HTTP
  entry points.

## Known Boundary Defect

Component package initializers eagerly import optional integrations. As a
result, an unused connector or writer dependency can prevent unrelated modules
and tests from importing. This is an observed defect, not intended architecture,
and is outside the project-foundation workstream.
