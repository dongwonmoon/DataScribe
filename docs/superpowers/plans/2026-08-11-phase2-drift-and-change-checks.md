# Phase 2: Drift And Change Checks Implementation Plan (REVISED)

> **Status:** Revised after the 3-panel attack round (2026-08-11). The
> original Tasks 2+3 (deterministic profile drift) were REJECTED: profile
> stats are never rendered into output, so a stats change cannot invalidate
> documentation — the check would detect data volatility, not documentation
> drift; on small/empty fixtures (all legacy variants are DDL-only) the
> first real data load fires every `is_unique` flag. Recorded as a deferred
> candidate under the PRODUCT.md:132 revisit trigger instead.

**Goal:** Close the known removed-model gap in dbt change detection and
publish an honest, LLM-free CI gate for the core `db` path (PRODUCT.md:96-97
"bounded subset of schema-change and documentation-drift checks").

**Global Constraints:** deterministic gates (no LLM); red test first;
`./scripts/verify.sh` green per task; conventional commits; narrow branches
off `dev`.

---

## Task 1: dbt removed-model detection — DONE

- Seam: `DbtYamlWriter.write(catalog)` in `check`/`drift` mode.
- Node-type scope fix (panel fatal): `model_to_file_map` includes
  sources/seeds/snapshots; removal detection uses `documented_model_names`
  (models node type ONLY) so documented sources are never flagged.
- Semantics: `models_to_remove = documented_model_names - catalog_models`;
  logged per model; sets `is_outdated` in check/drift modes only. No
  deletion in any mode (resolution path = removing the model from the
  documented YAML; the report names what is missing).
- Tests: removed-detection red test; non-model-node-types lock test.
- Commit: `fix: dbt --check detects removed models` (in progress).

## Task 2: CI example (honest baseline story)

**Files:**
- Create: `.github/workflows/docs-check.yml`
- Modify: `docs/TESTING.md` (Phase 2 checks section)

The example must be HONEST about the baseline: the schema-state sidecar is
written only by a successful generation run (which constructs the LLM
client). Two supported patterns:
(a) commit a fixture DB + its baseline sidecar, then CI runs the LLM-free
    gates (`db --check --output md`, exit 0/1);
(b) document the one-time baseline step (run generation once, commit the
    sidecar) with a comment in the workflow.

```yaml
name: docs-check
on: [push, pull_request]
jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install -e '.[all]'
      # Baseline sidecar is committed; these gates never call an LLM:
      - run: schema-scribe db --db fixture --output md --check
```

## Task 3: Policy decision record

**Files:**
- Create: `docs/superpowers/specs/2026-08-11-phase2-policy.md`

Answers (panel-reviewed):
1. Destructive changes (dbt model removal, table drop): `--check` FAILS in
   CI (already true for db; now true for dbt via Task 1). No automatic
   deletion anywhere. **Resolution path:** remove the node from the
   documented YAML — the check report names what is missing, so the team
   acts deliberately; the red state is not a dead-end.
2. Profile-drift gate (original Tasks 2+3): NOT shipped. Recorded as a
   deferred candidate requiring (a) PRODUCT.md:132 evidence (repeated
   demand for automated drift checks), (b) a redesigned signal (report-only
   by default, severity as a parameter, LLM-free baseline refresh) — see
   the panel record in docs/superpowers/specs/2026-08-08-eval-baseline.md
   and the plan revision note above.

## Out Of Scope

- LLM prose drift (dbt MATCH/DRIFT) — existing dbt feature, not extended.
- Profile-data drift gate — deferred candidate (see Task 3).
- Migration-tool compatibility; sidecar orphan cleanup; hosted service.
