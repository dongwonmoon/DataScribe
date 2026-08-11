"""Tests for the async JobManager (Phase 3a). No pytest-asyncio: the
manager is driven with asyncio.run from plain sync tests (stdlib only).
"""

import asyncio
import threading
import time

import pytest

from schema_scribe.server.jobs import JobManager, QueueFullError


def test_job_lifecycle_and_serialization():
    """max_concurrent=1 -> jobs run FIFO, serially, all succeed."""
    async def scenario():
        manager = JobManager(max_concurrent=1)
        manager.start()
        ran = []

        def work(n):
            ran.append(n)
            return {"n": n}

        ids = [manager.submit(lambda n=n: work(n)) for n in range(3)]
        await manager.wait_completion(ids[-1], timeout=2)
        assert all(manager.get(jid).status == "succeeded" for jid in ids)
        assert ran == [0, 1, 2]
        await manager.stop()

    asyncio.run(scenario())


def test_job_failure_is_captured():
    async def scenario():
        manager = JobManager(max_concurrent=1)
        manager.start()

        def boom():
            raise ValueError("engine exploded")

        jid = manager.submit(boom)
        job = await manager.wait_completion(jid, timeout=2)
        assert job.status == "failed"
        assert "engine exploded" in job.error
        await manager.stop()

    asyncio.run(scenario())


def test_queue_full_raises():
    async def scenario():
        manager = JobManager(max_concurrent=1, max_queue=1)
        manager.start()
        blocker = threading.Event()

        def slow():
            blocker.wait()

        first = manager.submit(slow)
        # Wait until the worker picked up job 1 (running) so the queue is
        # free for exactly one more entry — otherwise the worker races the
        # submits and the queue never fills.
        for _ in range(200):
            if manager.get(first).status == "running":
                break
            await asyncio.sleep(0.01)
        manager.submit(slow)  # queued (queue size 1 = full)
        with pytest.raises(QueueFullError):
            manager.submit(slow)  # queue full
        blocker.set()
        await manager.wait_completion(first, timeout=2)
        await manager.stop()

    asyncio.run(scenario())


def test_engine_job_runs_off_the_event_loop():
    """Polling must stay responsive while a job is running: the engine
    function blocks its own thread, not the loop (panel finding)."""
    async def scenario():
        manager = JobManager(max_concurrent=1)
        manager.start()

        def slow_engine():
            time.sleep(0.1)  # sync engine work
            return {"done": True}

        jid = manager.submit(slow_engine)
        # While the job runs (its thread sleeps), the loop must still turn:
        ticks = 0
        while ticks < 3:
            await asyncio.sleep(0.01)
            ticks += 1
            assert manager.get(jid).status in ("pending", "running")
        await manager.wait_completion(jid, timeout=2)
        assert manager.get(jid).status == "succeeded"
        await manager.stop()

    asyncio.run(scenario())
