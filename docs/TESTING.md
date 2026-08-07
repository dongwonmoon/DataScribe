# Testing

## Local Setup

```bash
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -e '.[all]'
```

The full extra is temporarily required because optional component packages are
eagerly imported. See `ARCHITECTURE.md` for the known boundary defect.

## Deterministic Verification

Run the same command locally and in CI:

```bash
./scripts/verify.sh
```

It runs:

- unit and integration tests with fixture databases and mocked providers;
- sdist and wheel package builds;
- a non-network CLI help smoke check;
- committed tool-configuration checks when present.

Real Ollama, Google, and OpenAI calls do not belong in this gate.

## Evaluation

Real-model quality, latency, token use, monetary cost, database query count, and
human acceptance or edit rates require a fixed fixture and review rubric. They
are deferred product-evaluation work and must not run in deterministic CI.

### Procedure (fixed fixture, human review)

```bash
python scripts/build_fixture_dbs.py --out-dir <dir> --variant clean
python scripts/run_evaluation.py generate --config config.yaml --db clean \
    --output eval/review_sheet.md
# human fills the sheet: mark ACCEPT / EDIT (+Correction) / REJECT per item
python scripts/run_evaluation.py parse eval/review_sheet.md
```

The fixture is deterministic (users/orders/products), so runs are comparable
across models and prompt versions. Acceptance/edit/rejection rates are the
release evidence named in PRODUCT.md:114-116.

### Rubric

- **Meaning**: the description names the real business content of the
  table/column (per the fixture's ground truth); no invented facts.
- **Concision**: table summaries are 1-2 sentences; column descriptions
  under 15 words (the prompt contract).
- **Signal use**: descriptions plausibly use the profile stats (e.g., a
  unique column is described as unique).
- **EDIT** = wrong or weak but fixable (write the correction);
  **REJECT** = unusable (wrong meaning, hallucinated, empty).

Quality claims must name fixture, model, prompt version, and method.
