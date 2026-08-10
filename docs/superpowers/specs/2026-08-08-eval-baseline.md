# Evaluation Baseline And Lever 1 Results

## Status

Evidence record: first valid fixed-fixture evaluation (2026-08-08). Baseline
(variant A) and lever-1 (sibling-column context, variant E1) measured on the
seeded clean fixture with gemma-4-26b-a4b-it. Not a product contract.

## Method

- Fixture: `clean` (users/orders/products) — **seeded** with 12 users / 8
  products / 20 orders so profile statistics are non-degenerate (empty
  tables hardcode `{null_ratio: 0.0, distinct_count: 0, is_unique: True}`
  for every column — the earlier "descriptions too obvious" measurement was
  invalid for this reason, verified at sqlite_connector.py:241-246).
- Model: gemma-4-26b-a4b-it via `google.genai` (thought parts excluded by
  the SDK; output-budget ladder [1024, 2048, 4096] with finish_reason
  MAX_TOKENS truncation detection).
- Prompt: single shared prompt set (no per-model variants — user decision);
  variant A = current prompt; variant E1 = A + sibling-column context
  (other columns in the same table, zero-cost: already fetched).
- Review: 13 items (3 table summaries + 10 columns). Sheet generator/parser:
  scripts/run_evaluation.py; rubric in docs/TESTING.md.

## Baseline A Results (13 items)

Acceptable (10/13): 1, 2, 4, 5, 6, 7, 9, 10, 11, 13.
Hallucinated qualifiers (3/13, ~23% edit rate): 3 (`users.name` →
"category name"), 8 (`products.price` → "unique price category"),
12 (`orders.product_id` → "product type identifier").

Pattern: with distinct_count < 10 AND is_unique True, prompt rules 2+3
(prompts.py:26-28) contradict ("mention unique" vs "likely a category") and
the model invents a qualifier.

## Lever 1 Results (E1: sibling columns, 13 items)

Hallucinations: 3 → 1 (`orders.product_id` → "product category" persists).
Fixed: `users.name` → "The full name of the user."; `products.price` →
"Unique price for each product."; `users.email` stays correct.
Estimated edit rate ~8%.

**Author verdict (2026-08-09, the official review):** "좀 나아졌다,
(그러나 실제 데이터 문서를 보고난 뒤) 아직 모자라다" — improved, but
still short of real-world documentation level. Reference for "real docs":
dbt-labs/jaffle_shop schema.yml conventions — descriptions carry units
(AUD/UTC), PII classification, and relationship naming ("Foreign key to
the customers table"); our prompts encode none of these in the db path
(the dbt path already asks for PII meta).

## Findings

1. The empty-fixture artifact invalidated the earlier "too obvious" finding;
   with real rows, name-echo largely disappears (this is the reason fixture
   seeding matters — evaluation must measure a populated database).
2. Sibling-column context is a zero-cost quality lever (3 → 1 hallucination).
3. Remaining hallucination class: FK columns (`product_id`) still get
   invented qualifiers ("category") — candidate for lever 2 (FK context).
4. `order_date` described as "unique date" is factually correct here (dates
   are unique in the seed) but phrasing is odd — minor.

## Pending

- Human grading of the E1 sheet (the author) for the official accept rate.
- Pre-registered legacy counterfactual (orientation research gate #2):
  `db` against legacy-rich — the product-level test.
- Lever 2 (FK context) measured independently before any combination.

## Lever 3 / Rule-Conflict Fix (E3, 2026-08-09)

Root cause found via the legacy counterfactual (gate #2, legacy_sample):
prompt rules 2 (unique) and 3 (distinct<10 → category) fire together on
small tables, producing systematic "category" pollution (5/5 columns wrong
on the sample, e.g. `reg_dt` → "categorical registration period indicator").

Fix: rule 3 now requires "AND the column is NOT unique"; unique columns with
low distinct counts are explicitly normal.

Results:
- E3 (seeded clean, gemma-4-26b): **13/13 clean** — E1's 3 hallucinations,
  E2's moved noise, all gone.
- legacy_sample re-run (flash-lite): flood cured — 4/5 columns correct
  (cust_name/reg_dt/id all fixed); 1 residual (`parent_shp_mst_id` →
  "category ID") is a legitimate rule-3 firing (3 distinct, not unique).

Gate #2 verdict: pending author judgment on the repaired output (does the
catalog + landscape cure scale/name fear?).

## Gate #2 Verdict (2026-08-09, 3-panel blind)

Panel verdict: **FAIL as run** — with (c) meaningfully cured and (b)
UNTESTED at scale.

- **Fatal (fixed):** the ERD rendered every FK relationship with inverted
  parent/child direction (`"{source}" ||--o{ "{target}"` puts the FK holder
  on the left; Mermaid erDiagram needs the parent left). Deterministic
  writer bug, pinned by a passing test. Fixed in all three writers
  (markdown/confluence/notion) — commit 8e68f82.
- **Fatal (process):** the gate was run on an ad-hoc 6-table sample, not the
  pre-registered legacy-rich (192 tables) — 6 tables cannot test (b):
  clustering 6→6, centrality returns every table, and 4-5 rows re-introduce
  the empty-table uniqueness degeneracy. (b) remains untested at scale;
  legacy-rich with `--landscape` is the pending run.
- **Must-fix (fixed):** "unique foreign key" (false cardinality claim) —
  prompt rule 2 now excludes FK columns; "Summary:" preamble echo stripped.
- **Must-fix (remaining):** semantic overclaims (SHP stem "shipping",
  "alphanumeric", inferred "in 2021") — model inference beyond schema;
  item_cd "category" residual (rule-3 firing on a code column).
- **Confirmed positive:** MST/HDR/DTL/TRN decoding works — (c) cured at
  summary level (all 3 panels).

## Batching (E4/E4b, 2026-08-11)

LLM calls per run batched from tables+columns+views to one call per table
(13 -> 3 on the clean fixture, benchmark-proven; 192-table legacy would be
~1200 -> ~204). Quality re-measured with gemma-4-26b:

- E4 (naive batch): 1/13 regression — `orders.product_id` → "category"
  (the FK context got diluted in the dense batch entries).
- E4b (rules strengthened: FK columns are never unique or categories —
  "References the X table"; non-FK columns marked explicitly): 13/13 clean,
  and FK descriptions now use the real-doc convention ("References the
  products table", cf. jaffle_shop "Foreign key to the customers table").

Parse contract: "SUMMARY:" + numbered "N: <description>" lines — model
agnostic, no JSON-schema dependency. Missing lines become empty drafts.
