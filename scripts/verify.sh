#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python_bin="${SCHEMA_SCRIBE_PYTHON:-${repo_root}/.venv/bin/python}"

if ! command -v "${python_bin}" >/dev/null 2>&1 && [[ ! -x "${python_bin}" ]]; then
  echo "SchemaScribe Python not found: ${python_bin}" >&2
  echo "Run: uv venv --python 3.12 .venv && uv pip install --python .venv/bin/python -e '.[all]'" >&2
  exit 2
fi

cd "${repo_root}"
"${python_bin}" -m pytest schema_scribe/tests
"${python_bin}" -m build
"${python_bin}" -m schema_scribe.main --help >/dev/null
"${python_bin}" -m json.tool opencode.json >/dev/null
"${python_bin}" - <<'PY'
import json
from pathlib import Path

config = json.loads(Path("opencode.json").read_text())
instructions = config.get("instructions")
assert instructions == ["agent-instructions/opencode.md"]
assert Path(instructions[0]).is_file()
PY
