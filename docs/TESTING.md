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
