# SchemaScribe V1 Product Contract Design

## Status

Approved product direction, written for final user review. This design records
why the active contract chooses a local documentation engine before change
review or a hosted catalog.

## Decision

The first user is a backend developer on a team too small to operate a dedicated
data catalog, but responsible for understanding an existing database and
keeping its documentation useful.

SchemaScribe will first become a trustworthy local database documentation
engine. A bounded schema-change and documentation-drift capability may follow,
then a thin hosted demonstration. The hosted surface does not become the
product's authority; it reuses an already credible engine.

## User Problem

The target user encounters legacy databases without trustworthy documentation,
infers meaning from names, and sees README or internal documentation become
stale after schema changes. Full catalog platforms impose installation and
operational costs that a small team cannot justify. Uploading production values
or sensitive schemas to a third-party service may also be unacceptable.

The useful job is therefore not “let AI document everything automatically.” It
is “give me a safe, reviewable draft that makes an unfamiliar database faster
to understand and cheaper to keep documented.”

## Considered Directions

### A. Local database documentation tool — selected

```text
database -> schema/profile collection -> AI draft -> human review -> Markdown/JSON
```

This direction is closest to the existing engine, works without dbt, addresses
a specific small-team problem, and makes local execution and Ollama meaningful
rather than decorative privacy features. Its limitation is explicit: schema
metadata cannot prove business meaning, so descriptions remain drafts.

### B. Schema-change review tool — later, in part

Migration or schema diffs could produce impact explanations and detect missing
documentation updates in CI. This has strong backend-engineering value, but it
introduces migration-tool compatibility, before/after schema ownership, and
breaking-change policy. Only the portion that naturally reuses the v1 engine
should follow after the local workflow is reliable.

### C. Lightweight catalog web service — deferred

A shared web catalog is easy to demonstrate but immediately creates credential
storage, authentication, authorization, job execution, multi-tenancy, search,
and deployment obligations. Starting there would make surrounding systems more
expensive than the unproven documentation engine.

## Product Boundary

The active contract is owned by `docs/PRODUCT.md`. It defines a read-only core
path, bounded aggregate profiling without raw-row sampling, explicit disclosure
for external LLM payloads, review before replacement, Markdown and JSON output,
incremental change classification, and preservation of the last successful
output on failure.

Existing dbt, remote writer, lineage, and FastAPI code is repository capability,
not approved v1 scope. Those surfaces may remain, but README visibility or code
presence must not silently promote them into release commitments.

## Delivery And Evidence

The product contract deliberately separates three kinds of evidence:

1. deterministic correctness through fixtures and mocked providers;
2. output usefulness through accepted, edited, and rejected descriptions on a
   fixed evaluation fixture;
3. efficiency through reproducible query-count, elapsed-time, and LLM-call
   benchmarks.

The first implementation plan should begin with a gap audit between this
contract and current behavior. It must not assume that existing connectors,
writers, preview behavior, or failure semantics satisfy the contract merely
because related code exists.

## Non-goals

- Designing the hosted service;
- selecting authentication, job queue, or tenancy technology;
- supporting every existing adapter in the first release;
- enabling raw-value sampling;
- rewriting the README or implementation before the written contract is
  reviewed;
- creating project-specific agents, Skills, or harness machinery.

## Completion Gate

This design is complete when the user confirms that `docs/PRODUCT.md` accurately
captures the intended first user, v1 promise, trust boundary, non-goals,
delivery sequence, and measurable success criteria. Implementation planning
begins only after that review.
