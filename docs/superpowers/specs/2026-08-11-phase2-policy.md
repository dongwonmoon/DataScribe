# Phase 2 Policy Decisions

## Status

Decision record from the Phase 2 plan revision (2026-08-11), following the
3-panel attack round. Answers the open policy questions; not a product
contract.

## 1. Destructive changes and the CI gate

- A documented object absent from the current catalog (dbt model removal,
  table drop) makes `--check` FAIL in CI. Already true for `db` (structural
  gate, 2026-08-11); now true for dbt via the removed-model detection.
- **No automatic deletion anywhere.** Not in dbt YAML, not in outputs.
- **Resolution path (not a dead-end):** the check report NAMES the missing
  objects; the team resolves deliberately — removing the node from the
  documented YAML is the acknowledged resolution, and the next check goes
  green. "Never delete" is a safety rule with an explicit escape hatch,
  not a permanent red.

## 2. Profile-data drift gate — DEFERRED candidate

The original Phase 2 plan proposed `db --drift` comparing per-column
profile stats (is_unique flip / null_ratio crossing 0.5 / distinct_count
halving) against a stats-bearing sidecar. The panel rejected it:

- **Category error:** profile stats are never rendered into any output
  (markdown/json write descriptions only), so a stats change cannot
  invalidate documented content — this would detect data volatility, not
  documentation drift (PRODUCT.md:96-97).
- **Small-N false positives:** legacy fixture variants are DDL-only, so
  every column baselines to `is_unique=True, distinct=0`; the first real
  data load fires the gate. On the seeded clean fixture, 5 of 7 unique
  columns are unique only by seed accident; one duplicate insert fails CI.
- **Trigger discipline:** PRODUCT.md:132 authorizes drift/CI work only
  when evidence shows repeated demand — no such evidence exists.

**If revived, the design must be:** report-only by default, severity as a
threaded parameter (not a doc note), and an LLM-free baseline refresh
path (the current sidecar is only written by an LLM generation run).

## 3. Gates are deterministic

Phase 2 adds no LLM calls to any gate. `db --check` is a pure function of
connector metadata; the dbt removed-model check is a set difference. The
benchmark (PRODUCT.md:116-117) records the query cost of each gate.
