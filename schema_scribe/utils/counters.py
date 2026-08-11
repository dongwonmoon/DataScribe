"""Delegating instrumentation counters for engine runs.

Extracted from scripts/benchmark.py (2026-08-11, Phase 3a): the demo
server reports per-job engine metrics (database query count, LLM call
count). CountingLLM DELEGATES to a real LLM client — it counts
get_description invocations and forwards the call; it is not a mock.
"""

from typing import Any


class CountingCursor:
    """Proxy over a cursor that counts execute() calls and delegates."""

    def __init__(self, cursor):
        self._cursor = cursor
        self.count = 0

    def execute(self, sql, *args):
        self.count += 1
        return self._cursor.execute(sql, *args)

    def fetchall(self):
        return self._cursor.fetchall()

    def fetchone(self):
        return self._cursor.fetchone()

    def close(self):
        self._cursor.close()


class CountingLLM:
    """Delegating LLM wrapper that counts get_description invocations.

    Semantics: counts USER-LEVEL get_description calls (one per prompt).
    Provider-internal retries (e.g. the Google budget ladder and quota
    retries) happen inside the wrapped client and are not counted — the
    metric is "prompts the engine requested", which matches the benchmark.
    """

    def __init__(self, client):
        self._client = client
        self.count = 0

    def get_description(self, prompt: str, max_tokens: int) -> str:
        self.count += 1
        return self._client.get_description(prompt, max_tokens)
