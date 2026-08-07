# Landscape Probe Results

## Status

Evidence record for the gated Slice 8 (Landscape Report) of the v1 engine
plan. The probe ran against the Slice 0.5 legacy fixtures on 2026-08-07.

## Setup

- Fixtures: `legacy-rich.db` (192 tables, cryptic names `TBL_<DOMAIN>_<TYPE>_<YEAR>`,
  276 declared FKs, 100% resolvable) and `legacy-poor.db` (same 192 tables, 2 FKs).
- Counterfactual run: existing `db` workflow (real SQLiteConnector + real
  MarkdownWriter, stub LLM) against `legacy-rich` → `catalog.md` (2,870 lines:
  276-edge Mermaid ERD, views section, per-table sections).
- Caveat recorded: descriptions were stub text (no LLM); the deterministic
  structure (ERD, table list, column tables) is identical to a real run.

## Measurements

| Metric | Value |
| --- | --- |
| legacy-rich declared FKs | 276 (100% resolvable against `sqlite_master`) |
| legacy-poor declared FKs | 2 |
| Deterministic grouping (pure regex, no LLM) | 192 tables → 48 groups (12 domains × 4 types), 0 uncategorized |
| FK-centrality top-10 | all `*_MST_2021` master tables (hub signal: "start with masters") |

## Author Judgment (verbatim findings)

- The Mermaid ERD idea is excellent, but a single page with that many tables
  remains burdensome — the scale problem (b) is confirmed on the real output.
- Proposed the missing feature himself: deterministic table grouping, asking
  whether it is feasible without AI. The demonstration above answers yes.
- The author did not answer the name-decoding question (Q2) directly; the
  grouping result implies decoding through convention (`MST`=master, `TRN`=
  transaction) rather than through prose.

## Slice 8 Gate Decision

**IMPLEMENT.** Both gate conditions are met:

1. FK-centrality produces non-empty, meaningful results on `legacy-rich`
   (hub = master tables).
2. The author identified a concrete, buildable missing feature (deterministic
   grouping / scale reduction).

Implementation notes carried into Slice 8: grouping must handle the
uncategorized bucket explicitly; the fixture pattern demonstrates
domain::type::year conventions, but real schemas will be messier — the
`"other"` cluster is a first-class output, not an error state.
