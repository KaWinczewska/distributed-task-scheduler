import json
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from test_system import Client, await_until, sample_csv

from distributed_scheduler import distributed_jobs_pb2 as messages
from distributed_scheduler.coordinator import Coordinator
from distributed_scheduler.csv_processor import process
from distributed_scheduler.worker import Worker


@pytest.mark.parametrize("policy", ["first-available", "round-robin", "least-loaded"])
def test_capacity_fifo_and_concurrent_submissions(tmp_path, policy):
    release = threading.Event()
    full = threading.Event()
    lock = threading.Lock()
    active = peak = 0
    order = []

    def execute(source, target):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
            order.append(source)
            if active == 2:
                full.set()
        try:
            assert release.wait(5)
            Path(target).write_text("{}")
        finally:
            with lock:
                active -= 1

    try:
        with Coordinator(0, scheduler=policy) as coordinator:
            client = Client(coordinator.port)
            try:
                with ThreadPoolExecutor(max_workers=8) as submitters:
                    ids = list(submitters.map(lambda i: client.submit(str(i), tmp_path / f"{i}.json"), range(12)))
                assert all(client.status(key) == messages.PENDING for key in ids)
                expected = [job.input for job in coordinator.jobs.values()]
                with Worker("127.0.0.1", coordinator.port, "127.0.0.1", 0, execute, max_concurrent_jobs=2):
                    assert full.wait(3)
                    with coordinator.lock:
                        assert next(iter(coordinator.workers.values())).active_jobs == 2
                        assert [
                            job.input for job in coordinator.jobs.values() if job.status == messages.RUNNING
                        ] == expected[:2]
                    release.set()
                    await_until(lambda: all(client.status(key) == messages.COMPLETED for key in ids))
                    assert peak == 2
                    assert coordinator.metrics()["submitted"] == 12
            finally:
                client.close()
    finally:
        release.set()


def test_retry_limit_and_transient_backoff(tmp_path):
    attempts = 0

    def transient(source, target):
        nonlocal attempts
        attempts += 1
        raise TimeoutError("temporary")

    with (
        Coordinator(0, max_retries=2, retry_backoff=0.01) as coordinator,
        Worker("127.0.0.1", coordinator.port, "127.0.0.1", 0, transient),
    ):
        client = Client(coordinator.port)
        try:
            key = client.submit("input", tmp_path / "out.json")
            await_until(lambda: client.status(key) == messages.FAILED)
            assert attempts == 3
            assert coordinator.metrics()["retries"] == 2
            assert coordinator.jobs[key].error == "temporary"
        finally:
            client.close()


def test_permanent_csv_error_is_not_retried(tmp_path):
    source = tmp_path / "invalid.csv"
    source.write_text("wrong,header\n")
    with Coordinator(0) as coordinator, Worker("127.0.0.1", coordinator.port, "127.0.0.1", 0):
        client = Client(coordinator.port)
        try:
            key = client.submit(source, tmp_path / "out.json")
            await_until(lambda: client.status(key) == messages.FAILED)
            assert coordinator.jobs[key].attempt == 1
            assert coordinator.metrics()["retries"] == 0
        finally:
            client.close()


def test_timeout_reassignment_fences_late_output(tmp_path):
    started = threading.Event()
    release = threading.Event()
    wrote = threading.Event()

    def late(source, target):
        started.set()
        assert release.wait(5)
        Path(target).write_text('{"stale":true}')
        wrote.set()

    try:
        with Coordinator(0, heartbeat_timeout=0.15, scan_interval=0.01, retry_backoff=0) as coordinator:
            client = Client(coordinator.port)
            try:
                with Worker("127.0.0.1", coordinator.port, "127.0.0.1", 0, late) as old:
                    target = tmp_path / "out.json"
                    key = client.submit(sample_csv(tmp_path / "input.csv"), target)
                    assert started.wait(3)
                    old.stopping.set()  # lose heartbeats while computation continues
                    await_until(lambda: coordinator.worker_count == 0)
                    with Worker("127.0.0.1", coordinator.port, "127.0.0.1", 0):
                        await_until(lambda: client.status(key) == messages.COMPLETED)
                        authoritative = target.read_bytes()
                        release.set()
                        assert wrote.wait(3)
                        old.server.stop(1).wait()
                        assert target.read_bytes() == authoritative
                        assert json.loads(authoritative)["totalRequests"] == 4
                        assert coordinator.jobs[key].attempt == 2
            finally:
                client.close()
    finally:
        release.set()


def test_crash_during_processing_reassigns(tmp_path):
    started, release = threading.Event(), threading.Event()

    def blocked(source, target):
        started.set()
        assert release.wait(5)
        Path(target).write_text("{}")

    try:
        with Coordinator(0, retry_backoff=0) as coordinator:
            client = Client(coordinator.port)
            try:
                with Worker("127.0.0.1", coordinator.port, "127.0.0.1", 0, blocked) as broken:
                    key = client.submit(sample_csv(tmp_path / "input.csv"), tmp_path / "out.json")
                    assert started.wait(3)
                    broken.close()
                    await_until(lambda: client.status(key) == messages.PENDING)
                    with Worker("127.0.0.1", coordinator.port, "127.0.0.1", 0):
                        await_until(lambda: client.status(key) == messages.COMPLETED)
                    release.set()
            finally:
                client.close()
    finally:
        release.set()


def test_duplicate_and_old_attempt_are_not_current(tmp_path):
    with Coordinator(0) as coordinator, Worker("127.0.0.1", coordinator.port, "127.0.0.1", 0):
        client = Client(coordinator.port)
        try:
            key = client.submit(sample_csv(tmp_path / "input.csv"), tmp_path / "out.json")
            await_until(lambda: client.status(key) == messages.COMPLETED)
            with coordinator.lock:
                job = coordinator.jobs[key]
                worker = coordinator.workers[job.worker]
                assert not coordinator._is_current(job, worker, job.attempt)
                assert not coordinator._is_current(job, worker, job.attempt - 1)
            duplicate = tmp_path / "duplicate.json"
            duplicate.write_text('{"stale": true}')
            coordinator._accept_result(
                job, worker, job.attempt, messages.ExecuteJobResponse(success=True), str(duplicate)
            )
            assert json.loads((tmp_path / "out.json").read_text())["totalRequests"] == 4
            assert duplicate.exists()
            assert coordinator.metrics()["completed"] == 1
        finally:
            client.close()


def test_csv_quotes_and_empty_data(tmp_path):
    source = tmp_path / "quoted.csv"
    source.write_text('timestamp,user_id,endpoint,status_code,response_time_ms\nt,"a,b","/a,b",200,10\n')
    target = tmp_path / "out.json"
    process(str(source), str(target))
    assert json.loads(target.read_text())["requestsByEndpoint"] == {"/a,b": 1}

    source.write_text("timestamp,user_id,endpoint,status_code,response_time_ms\n")
    process(str(source), str(target))
    assert json.loads(target.read_text())["totalRequests"] == 0
    assert target.stat().st_mode & 0o777 == 0o644


def test_publication_claim_survives_timeout_without_holding_scheduler_lock(tmp_path, monkeypatch):
    import distributed_scheduler.coordinator as module

    entered, release = threading.Event(), threading.Event()
    replace = module.os.replace

    def paused_replace(source, target):
        entered.set()
        assert release.wait(5)
        replace(source, target)

    monkeypatch.setattr(module.os, "replace", paused_replace)
    try:
        with Coordinator(0) as coordinator:
            client = Client(coordinator.port)
            try:
                # A custom executor avoids pausing csv_processor's own atomic rename.
                def execute(source, target):
                    Path(target).write_text("{}")

                with Worker("127.0.0.1", coordinator.port, "127.0.0.1", 0, execute):
                    key = client.submit("input", tmp_path / "out.json")
                    assert entered.wait(3)
                    # Status RPC and invalidation remain responsive during filesystem I/O.
                    assert client.status(key) == messages.RUNNING
                    with coordinator.changed:
                        worker = next(iter(coordinator.workers.values()))
                        coordinator._remove_worker(worker, "timeout during publication")
                        assert coordinator.jobs[key].publishing
                        assert not coordinator.pending
                    release.set()
                    await_until(lambda: client.status(key) == messages.COMPLETED)
                    assert coordinator.metrics()["retries"] == 0
            finally:
                client.close()
    finally:
        release.set()


def test_old_session_heartbeat_does_not_refresh_replacement():
    with Coordinator(0, heartbeat_timeout=10) as coordinator:
        coordinator.RegisterWorker(messages.RegisterWorkerRequest(host="host", port=1, session_id="new"), None)
        worker = coordinator.workers["host:1"]
        previous = worker.last_heartbeat
        coordinator.Heartbeat(messages.HeartbeatRequest(host="host", port=1, session_id="old"), None)
        assert worker.last_heartbeat == previous
        coordinator.Heartbeat(messages.HeartbeatRequest(host="host", port=1, session_id="new"), None)
        assert worker.last_heartbeat >= previous


def test_worker_rejects_calls_beyond_capacity(tmp_path):
    import grpc

    from distributed_scheduler import distributed_jobs_pb2_grpc as services

    started, release = threading.Event(), threading.Event()

    def blocked(source, target):
        started.set()
        assert release.wait(5)
        Path(target).write_text("{}")

    try:
        with (
            Coordinator(0) as coordinator,
            Worker("127.0.0.1", coordinator.port, "127.0.0.1", 0, blocked) as worker,
            grpc.insecure_channel(f"127.0.0.1:{worker.port}") as channel,
        ):
            stub = services.WorkerServiceStub(channel)
            first = stub.ExecuteJob.future(
                messages.ExecuteJobRequest(input_path="input", output_path=str(tmp_path / "first"))
            )
            assert started.wait(3)
            with pytest.raises(grpc.RpcError) as failure:
                stub.ExecuteJob(
                    messages.ExecuteJobRequest(input_path="input", output_path=str(tmp_path / "second")), timeout=2
                )
            assert failure.value.code() == grpc.StatusCode.RESOURCE_EXHAUSTED
            release.set()
            assert first.result(timeout=3).success
    finally:
        release.set()


@pytest.mark.parametrize(
    "data",
    [
        b"",
        b"wrong,header\n",
        b"timestamp,user_id,endpoint,status_code,response_time_ms\nt,u,/a,200\n",
        b"timestamp,user_id,endpoint,status_code,response_time_ms\nt,u,/a,200,1,extra\n",
        b"timestamp,user_id,endpoint,status_code,response_time_ms\nt,u,/a,,1\n",
        b"timestamp,user_id,endpoint,status_code,response_time_ms\nt,u,/a,99,1\n",
        b"timestamp,user_id,endpoint,status_code,response_time_ms\nt,u,/a,600,1\n",
        b"timestamp,user_id,endpoint,status_code,response_time_ms\nt,u,/a,200,-1\n",
        b"timestamp,user_id,endpoint,status_code,response_time_ms\nt,u,/a,200,1.5\n",
        b'timestamp,user_id,endpoint,status_code,response_time_ms\nt,"unclosed,/a,200,1\n',
        b"timestamp,user_id,endpoint,status_code,response_time_ms\nt,\xff,/a,200,1\n",
    ],
)
def test_invalid_csv_preserves_existing_output(tmp_path, data):
    import csv

    source, target = tmp_path / "bad.csv", tmp_path / "out.json"
    source.write_bytes(data)
    target.write_text('{"previous":true}')

    with pytest.raises((ValueError, csv.Error)):
        process(str(source), str(target))

    assert target.read_text() == '{"previous":true}'
    assert not list(tmp_path.glob(".csv-result-*.tmp"))


def test_all_workers_lost_requeues_multiple_running_jobs(tmp_path):
    both_started, release = threading.Event(), threading.Event()
    lock = threading.Lock()
    started = 0

    def blocked(source, target):
        nonlocal started
        with lock:
            started += 1
            if started == 2:
                both_started.set()
        assert release.wait(5)
        Path(target).write_text('{"stale":true}')

    try:
        with Coordinator(0, retry_backoff=0) as coordinator:
            client = Client(coordinator.port)
            try:
                source = sample_csv(tmp_path / "input.csv")
                with Worker("127.0.0.1", coordinator.port, "127.0.0.1", 0, blocked, max_concurrent_jobs=2) as broken:
                    keys = [client.submit(source, tmp_path / f"{i}.json") for i in range(4)]
                    assert both_started.wait(3)
                    broken.close()
                    await_until(
                        lambda: (
                            coordinator.worker_count == 0
                            and all(client.status(key) == messages.PENDING for key in keys)
                        )
                    )
                    snapshot = coordinator.metrics()
                    assert snapshot["retries"] == 2
                    assert snapshot["submitted"] == 4
                    assert snapshot["failed"] == snapshot["completed"] == 0
                    release.set()
                    with Worker("127.0.0.1", coordinator.port, "127.0.0.1", 0):
                        await_until(lambda: all(client.status(key) == messages.COMPLETED for key in keys))
                        assert sorted(coordinator.jobs[key].attempt for key in keys) == [1, 1, 2, 2]
                        assert all(
                            json.loads((tmp_path / f"{i}.json").read_text())["totalRequests"] == 4 for i in range(4)
                        )
            finally:
                client.close()
    finally:
        release.set()


def test_publication_error_fails_once_and_preserves_previous_output(tmp_path, monkeypatch):
    import distributed_scheduler.coordinator as module

    target = tmp_path / "out.json"
    target.write_text('{"previous":true}')
    replace = module.os.replace

    def fail_final_publication(source, destination):
        if str(destination) == str(target):
            raise PermissionError("publication denied")
        return replace(source, destination)

    monkeypatch.setattr(module.os, "replace", fail_final_publication)
    with Coordinator(0) as coordinator, Worker("127.0.0.1", coordinator.port, "127.0.0.1", 0):
        client = Client(coordinator.port)
        try:
            key = client.submit(sample_csv(tmp_path / "input.csv"), target)
            await_until(lambda: client.status(key) == messages.FAILED)
            assert coordinator.jobs[key].error == "publication failed: publication denied"
            assert coordinator.jobs[key].attempt == 1
            assert coordinator.metrics()["retries"] == 0
            assert target.read_text() == '{"previous":true}'
            await_until(lambda: not list(tmp_path.glob("*.attempt-*")))
        finally:
            client.close()


def test_worker_close_waits_for_heartbeat_before_closing_channel(monkeypatch):
    entered, release = threading.Event(), threading.Event()
    heartbeat_threads = []
    channel_close_observations = []
    closer = None

    with Coordinator(0) as coordinator:
        worker = Worker("127.0.0.1", coordinator.port, "127.0.0.1", 0).start()
        try:

            def blocked_heartbeat(*args, **kwargs):
                heartbeat_threads.append(threading.current_thread())
                entered.set()
                assert release.wait(5)

            monkeypatch.setattr(worker.stub, "Heartbeat", blocked_heartbeat)
            original_close = worker.channel.close

            def observed_close():
                channel_close_observations.append(heartbeat_threads[0].is_alive())
                original_close()

            monkeypatch.setattr(worker.channel, "close", observed_close)
            assert entered.wait(4)
            closer = threading.Thread(target=worker.close)
            closer.start()
            assert worker.stopping.wait(3)
            release.set()
            closer.join(timeout=3)
            assert not closer.is_alive()
            assert not heartbeat_threads[0].is_alive()
            assert channel_close_observations == [False]
        finally:
            release.set()
            if closer is not None:
                closer.join(timeout=3)
            worker.close()


def test_worker_close_releases_idle_rpc_thread(tmp_path):
    execution_threads = []

    def execute(source, target):
        execution_threads.append(threading.current_thread())
        Path(target).write_text("{}")

    with Coordinator(0) as coordinator:
        worker = Worker("127.0.0.1", coordinator.port, "127.0.0.1", 0, execute).start()
        client = Client(coordinator.port)
        try:
            key = client.submit("input", tmp_path / "out.json")
            await_until(lambda: client.status(key) == messages.COMPLETED)
            worker.close()
            execution_threads[0].join(timeout=2)
            assert not execution_threads[0].is_alive()
        finally:
            worker.close()
            client.close()


def test_coordinator_close_releases_idle_rpc_thread(monkeypatch):
    request_threads = []
    with Coordinator(0) as coordinator:
        original_metrics = coordinator.metrics

        def metrics():
            request_threads.append(threading.current_thread())
            return original_metrics()

        monkeypatch.setattr(coordinator, "metrics", metrics)
        client = Client(coordinator.port)
        try:
            client.stub.GetMetrics(messages.MetricsRequest(), timeout=2)
        finally:
            client.close()
    request_threads[0].join(timeout=2)
    assert not request_threads[0].is_alive()
