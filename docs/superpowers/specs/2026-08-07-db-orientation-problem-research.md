# Database Orientation Problem Research

## Status

User-research evidence record from the 2026-08-07 grill session. Records the
problem-definition shift and the direction decision that followed. This is
evidence history, not current product truth: `docs/PRODUCT.md` remains the
active owner of the product promise.

## Interview Findings (n=1, the author)

- Original assumption: "data documentation is hard".
- Actual lived problem: **"opening the database is scary"** — the author, a
  junior backend developer who has never formally done data documentation,
  reported repeated memories of avoiding opening databases entirely.
- Selected causes (from four candidates — breaking things, scale, meaningless
  names, tooling): **(b) overwhelmed by scale** (hundreds of tables, no
  obvious starting point) and **(c) meaningless names** (table/column names
  carry no decodable meaning).
- Context example: a legacy SQLite database in an earlier app project that he
  was afraid to open.

## Direction Analysis

Two candidate promises:

- **A — documentation engine** (current PRODUCT.md): "safer first draft and
  shorter review loop".
- **B — orientation tool**: "understand an unfamiliar database without fear".
  Pillars: table clustering by naming conventions, FK-centrality to surface
  core tables, relationship map, scale summary, plus optional LLM name-decoding
  hints. The existing catalog/docs become a byproduct.

The engine is shared: both directions consume read-only metadata, tables,
columns, FKs, and bounded aggregate profiles.

## Panel Verdict (3 blind panelists, 2026-08-07)

Verified affirmatively:

- The "mostly deterministic, LLM-free" claim is true **computationally**:
  `get_tables()` returns names and `get_foreign_keys()` returns
  source/target pairs, so clustering and centrality are pure functions of
  already-returned metadata.
- Direction B does not violate the no-sampling or disclosure trust boundaries.

Rejected as a replacement north star (fatal / must-fix consensus):

- The relationship-map pillar already ships in A's output
  (`markdown_writer.py:37-67`); B's real delta is the optional LLM hints.
- "Understand in 10 minutes" is not verifiable by deterministic gates, unlike
  A's measurable criteria (first doc time, accept/edit/reject rates).
- FK-centrality reads only declared constraints; on FK-less legacy/ORM
  schemas the headline feature returns nothing — must be probed before being
  promised.
- Documentation/drift is recurring work; a landscape report is consumed once
  per database per person — weaker as a service thesis.
- Evidence was n=1 and unrecorded; PRODUCT.md's own revisit triggers do not
  include this scenario.

Panel also surfaced two defects in the existing engine plan (fixed in the
plan revision): the schema-state snapshot stores only FK source columns, and
composite-FK pairing is broken per connector.

## Decision

- **PRODUCT.md is unchanged.** A remains the v1 promise; the contract's
  revisit procedure is not bypassed.
- The Landscape Report is added to the engine plan as a **gated Slice 8**,
  not a promise change.
- Evidence gates before Slice 8 implementation:
  1. Legacy fixture workshop — 100+ table SQLite fixtures with cryptic
     names, FK-rich and FK-poor variants (committed builder, deterministic).
  2. Counterfactual run — execute the existing `db` command against the
     legacy fixture; the author judges whether the output cures (b)+(c).
  3. Optional real-world check — the author's own legacy SQLite database
     (read-only, safe after the read-only enforcement slice).
