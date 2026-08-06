# Product Contract

## Status

This is the proposed SchemaScribe v1 product contract, written for user review.
It defines the product target; it does not claim that every item is already
implemented or authorize implementation by itself.

## First User

SchemaScribe is for a backend developer on a team too small to operate a
dedicated data catalog, but still responsible for understanding an existing
database and keeping its documentation useful.

That developer commonly joins a legacy database with little documentation,
has to infer meaning from table and column names, watches hand-written docs
drift after schema changes, finds full catalog platforms too expensive to
operate, and cannot casually upload production schemas or values to an external
service.

## Product Promise

SchemaScribe is a local-first, review-driven tool that produces and maintains
documentation drafts for an existing SQL database. It reduces the time needed
to understand and document a database without pretending that an AI can verify
its business meaning.

The product promise is **a safer first draft and a shorter review loop**, not
automatic documentation truth.

## V1 Core Workflow

```text
read-only database connection
  -> schema and bounded aggregate profile collection
  -> AI-generated documentation draft
  -> human preview, diff, and review
  -> Markdown or JSON output
```

V1 must:

- connect to a declared supported SQL database in read-only mode;
- collect tables, columns, types, primary keys, foreign keys, and bounded
  aggregate statistics without returning source rows;
- show what schema metadata and statistics will be sent before an external LLM
  is used;
- generate draft descriptions for tables and columns;
- let the user review a preview or diff before replacing existing output;
- export Markdown and machine-readable JSON;
- distinguish unchanged, added, removed, and structurally changed objects on a
  later run against the same database;
- fail without corrupting the last successful documentation output.

An adapter is supported by v1 only after it passes the shared acceptance suite.
The presence of an existing connector in source code does not make it part of
the supported release set.

## Data And Trust Boundaries

- The source database is read-only on the core v1 path.
- Raw rows and individual field values are not collected or sent to an LLM by
  default. Aggregate profiling must not expose representative samples or value
  lists.
- Credentials remain local and are never written to generated documentation.
- The user explicitly chooses a local or external model provider. External
  transmission is disclosed before execution.
- Generated descriptions remain drafts until a human accepts them.
- Writers that mutate a database or a remote documentation system are optional
  adapters outside the core v1 promise and require explicit opt-in.
- SchemaScribe does not claim that local execution alone makes arbitrary model,
  plugin, or output configurations safe.

## V1 Non-goals

V1 does not promise:

- automatic confirmation that an AI description is factually correct;
- a complete data catalog, governance, or discovery platform;
- users, organizations, permissions, or multi-tenancy;
- continuous or real-time synchronization;
- raw-data sampling in the default workflow;
- support for every database or SQL dialect;
- a hosted production service;
- dbt, Notion, Confluence, lineage, or database-comment workflows as part of
  the primary onboarding and product message.

Existing integrations may remain available, but they do not set the v1 scope
or block completion of the core workflow.

## Product Sequence

Development follows this order:

1. Make the local database documentation engine trustworthy and measurable.
2. Add a bounded subset of schema-change and documentation-drift checks where
   they reuse the same engine.
3. Build a thin hosted demonstration only after the engine has credible output
   and failure behavior.

The second step does not imply broad migration-tool support. The third step is
not approval for credential storage, background jobs, authentication, or a
multi-user catalog.

## Success Criteria

V1 is successful when:

- a backend developer can install and configure a representative supported
  database and generate the first document within ten minutes;
- every generated table and column description can be reviewed as accepted,
  edited, or rejected;
- a failed generation leaves the previous successful output intact;
- a fixed evaluation fixture reports acceptance, edit, and rejection rates for
  generated descriptions;
- a reproducible benchmark records database query count, elapsed time, and LLM
  call count for the same fixture;
- the documentation clearly identifies which metadata leaves the machine for
  each provider configuration.

These measures are release evidence, not targets to optimize without a known
baseline. Quality claims must name the fixture, model, prompt version, and
evaluation method that produced them.

## Revisit Triggers

Reconsider the scope only when evidence shows that:

- users cannot complete the core workflow without a specific integration;
- schema-only drafts are consistently too weak and a privacy-reviewed sampling
  mode may justify its risk;
- repeated users need automated drift checks often enough to justify CI support;
- demand for shared access is strong enough to justify hosted credential,
  tenancy, and authorization work.
