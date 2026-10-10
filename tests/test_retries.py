import threading
import time
from pathlib import Path

import pytest
from test_system import Client, await_until

from distributed_scheduler import distributed_jobs_pb2 as messages
from distributed_scheduler.coordinator import Coordinator
from distributed_scheduler.worker import Worker


@pytest.mark.parametrize("policy", ["first-available", "round-robin", "least-loaded"])
def test_delayed_retry_allows_ready_fifo_jobs(tmp_path, policy):
    starts = []

    def execute(source, target):
        starts.append((source, time.perf_counter()))
        if source == "retry" and len([s for s, _ in starts if s == source]) == 1:
            raise TimeoutError("retry later")
        Path(target).write_text("{}")

    with Coordinator(0, scheduler=policy, retry_backoff=0.3) as coordinator, \
            Worker("127.0.0.1", coordinator.port, "127.0.0.1", 0, execute):
        client = Client(coordinator.port)
        try:
            retry = client.submit("retry", tmp_path / "retry.json")
            await_until(lambda: coordinator.metrics()["retries"] == 1)
            with coordinator.lock:
                deadline = coordinator.jobs[retry].ready_at
            first = client.submit("first", tmp_path / "first.json")
            second = client.submit("second", tmp_path / "second.json")
            await_until(lambda: all(client.status(j) == messages.COMPLETED for j in (retry, first, second)))
            assert [source for source, _ in starts] == ["retry", "first", "second", "retry"]
            assert starts[1][1] < deadline
            assert starts[-1][1] >= deadline
        finally:
            client.close()


def test_retries_use_deadline_order_and_condition_wait(tmp_path, monkeypatch):
    starts = []
    waiting = threading.Event()
    with Coordinator(0, retry_backoff=1) as coordinator:
        client = Client(coordinator.port)
        original_wait = coordinator.changed.wait
        waits = []

        def tracked_wait(timeout=None):
            waits.append(timeout)
            waiting.set()
            return original_wait(timeout)

        monkeypatch.setattr(coordinator.changed, "wait", tracked_wait)
        try:
            first = client.submit("first", tmp_path / "first.json")
            second = client.submit("second", tmp_path / "second.json")
            # Inject failed attempts under the normal scheduler lock, without a worker.
            with coordinator.changed:
                coordinator.pending.clear()
                for key, attempt in ((first, 2), (second, 1)):
                    job = coordinator.jobs[key]
                    job.attempt = attempt
                    coordinator._requeue(job, "injected failure")
                deadlines = {j: coordinator.jobs[j].ready_at for j in (first, second)}
                coordinator.changed.notify_all()
            assert waiting.wait(2)

            def execute(source, target):
                starts.append((source, time.perf_counter()))
                Path(target).write_text("{}")

            with Worker("127.0.0.1", coordinator.port, "127.0.0.1", 0, execute):
                await_until(lambda: all(client.status(j) == messages.COMPLETED for j in (first, second)))
            assert [source for source, _ in starts] == ["second", "first"]
            assert starts[0][1] >= deadlines[second]
            assert starts[1][1] >= deadlines[first]
            # A spin loop would produce thousands of waits; allow heartbeat/monitor notifications.
            assert len(waits) < 30
            assert any(timeout is not None and timeout > 0.1 for timeout in waits)
        finally:
            client.close()
