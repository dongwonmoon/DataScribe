"""In-process async job manager for the thin hosted demo.

Engine jobs run off the event loop via asyncio.to_thread — the engine is
synchronous (connectors and LLM clients are sync), so awaiting it directly
would block polling for every request (Phase 3a panel finding, 2026-08-11).
Concurrency is bounded by running at most `max_concurrent` workers, and the
pending queue is bounded (`max_queue`) — overflow raises QueueFullError
(mapped to HTTP 429).
"""

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


class QueueFullError(Exception):
    pass


@dataclass
class Job:
    job_id: str
    status: str = "pending"  # pending | running | succeeded | failed
    error: Optional[str] = None
    submitted_at: Optional[float] = None
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    result: dict = field(default_factory=dict)


class JobManager:
    """Submits sync engine jobs and serves their state to pollers."""

    def __init__(self, max_concurrent: int = 2, max_queue: int = 10):
        self._max_concurrent = max_concurrent
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=max_queue)
        self._jobs: dict[str, Job] = {}
        self._workers: list[asyncio.Task] = []
        self._started = False

    @property
    def pending_count(self) -> int:
        return self._queue.qsize()

    def start(self) -> None:
        if self._started:
            return
        self._started = True
        for _ in range(self._max_concurrent):
            self._workers.append(asyncio.create_task(self._worker_loop()))

    async def stop(self, drain: bool = False) -> None:
        """Cancels workers; running jobs are marked failed on cancel."""
        if not self._started:
            return
        if drain:
            await self._queue.join()
        for w in self._workers:
            w.cancel()
        for w in self._workers:
            try:
                await w
            except asyncio.CancelledError:
                pass
        for job in self._jobs.values():
            if job.status in ("pending", "running"):
                job.status = "failed"
                job.error = "server shutdown"
                job.finished_at = time.time()
        self._workers = []
        self._started = False

    def submit(self, fn: Callable[[], Any]) -> str:
        """Enqueues a sync engine function; returns the job id immediately."""
        if self._queue.full():
            raise QueueFullError(
                f"job queue is full ({self._queue.maxsize} pending)"
            )
        job_id = uuid.uuid4().hex[:8]
        self._jobs[job_id] = Job(
            job_id=job_id, submitted_at=time.time()
        )
        self._queue.put_nowait((job_id, fn))
        return job_id

    def get(self, job_id: str) -> Optional[Job]:
        return self._jobs.get(job_id)

    def job_ids(self) -> list[str]:
        return sorted(self._jobs.keys(), reverse=True)

    async def wait_completion(
        self, job_id: str, timeout: float = 10.0
    ) -> Optional[Job]:
        deadline = time.time() + timeout
        while time.time() < deadline:
            job = self._jobs.get(job_id)
            if job and job.status in ("succeeded", "failed"):
                return job
            await asyncio.sleep(0.01)
        return self._jobs.get(job_id)

    async def _worker_loop(self) -> None:
        while True:
            job_id, fn = await self._queue.get()
            job = self._jobs[job_id]
            job.status = "running"
            job.started_at = time.time()
            try:
                result = await asyncio.to_thread(fn)
                job.result = result
                job.status = "succeeded"
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 — engine errors become job state
                job.error = str(e)
                job.status = "failed"
            finally:
                job.finished_at = time.time()
            self._queue.task_done()
