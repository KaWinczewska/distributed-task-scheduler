import threading
from pathlib import Path
from urllib.request import urlopen

from prometheus_client import generate_latest
from prometheus_client.parser import text_string_to_metric_families
from test_system import Client, await_until

from distributed_scheduler import distributed_jobs_pb2 as messages
from distributed_scheduler.coordinator import Coordinator
from distributed_scheduler.worker import Worker


def samples(coordinator):
    return {sample.name: sample.value
            for family in text_string_to_metric_families(generate_latest(coordinator.registry).decode())
            for sample in family.samples if not sample.labels}


def test_metrics_retry_completion_duplicate_and_failure(tmp_path):
    entered, release = threading.Event(), threading.Event()
    attempts = 0

    def execute(source, target):
        nonlocal attempts
        if source == "bad":
            raise ValueError("permanent")
        attempts += 1
        if attempts == 1:
            raise TimeoutError("temporary")
        entered.set()
        assert release.wait(5)
        Path(target).write_text('{"ok":true}')

    try:
        with Coordinator(0, retry_backoff=0.1, metrics_port=0) as coordinator, \
                Worker("127.0.0.1", coordinator.port, "127.0.0.1", 0, execute):
            client = Client(coordinator.port)
            try:
                key = client.submit("good", tmp_path / "good.json")
                assert entered.wait(3)
                bad = client.submit("bad", tmp_path / "bad.json")
                live = samples(coordinator)
                assert live["scheduler_jobs_submitted_total"] == 2
                assert live["scheduler_jobs_running"] == 1
                assert live["scheduler_jobs_pending"] == 1
                assert live["scheduler_active_workers"] == 1
                assert live["scheduler_jobs_retried_total"] == 1
                assert live["scheduler_job_duration_seconds_count"] == 0
                with urlopen(f"http://127.0.0.1:{coordinator.metrics_port}/metrics", timeout=2) as response:
                    assert b"scheduler_jobs_submitted_total 2.0" in response.read()
                release.set()
                await_until(lambda: client.status(key) == messages.COMPLETED
                            and client.status(bad) == messages.FAILED)
                job = coordinator.jobs[key]
                worker = coordinator.workers[job.worker]
                duplicate = tmp_path / "duplicate"
                duplicate.write_text("stale")
                coordinator._accept_result(job, worker, job.attempt,
                                           messages.ExecuteJobResponse(success=True), str(duplicate))
                final = samples(coordinator)
                assert final["scheduler_jobs_completed_total"] == 1
                assert final["scheduler_jobs_failed_total"] == 1
                assert final["scheduler_jobs_retried_total"] == 1
                assert final["scheduler_jobs_running"] == final["scheduler_jobs_pending"] == 0
                assert final["scheduler_job_duration_seconds_count"] == 2
                assert final["scheduler_queue_wait_seconds_count"] == 2
                assert final["scheduler_queue_wait_seconds_sum"] >= 0.1
                assert final["scheduler_job_duration_seconds_sum"] >= final["scheduler_queue_wait_seconds_sum"]
                assert (tmp_path / "good.json").read_text() == '{"ok":true}'
                assert samples(coordinator) == final  # scrapes must not record events
            finally:
                client.close()
    finally:
        release.set()
