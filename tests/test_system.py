import json
import threading
import time
from concurrent import futures

import grpc

from distributed_scheduler import distributed_jobs_pb2 as messages
from distributed_scheduler import distributed_jobs_pb2_grpc as services
from distributed_scheduler.coordinator import Coordinator
from distributed_scheduler.worker import Worker


class Client:

    def __init__(self, port: int):
        self.channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        self.stub = services.CoordinatorServiceStub(self.channel)

    def submit(self, input_path, output_path) -> str:
        return self.stub.SubmitJob(
            messages.SubmitJobRequest(input_path=str(input_path),
                                      output_path=str(output_path))).job_id

    def status(self, job_id: str) -> int:
        return self.stub.GetJobStatus(
            messages.GetJobStatusRequest(job_id=job_id)).status

    def close(self):
        self.channel.close()


def sample_csv(path):
    path.write_text(
        "timestamp,user_id,endpoint,status_code,response_time_ms\n"
        "2026-10-06T18:42:01Z,user_1,/api/search,200,100\n"
        "2026-10-06T18:42:02Z,user_2,/api/products,302,200\n"
        "2026-10-06T18:42:03Z,user_1,/api/search,404,300\n"
        "2026-10-06T18:42:04Z,user_3,/api/orders,500,500\n",
        encoding="utf-8",
    )
    return path


def await_until(condition, timeout=5):
    deadline = time.monotonic() + timeout
    while not condition() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert condition(), "condition was not met before timeout"


def test_normal_execution_produces_expected_metrics(tmp_path):
    with Coordinator(0) as coordinator, Worker("127.0.0.1", coordinator.port,
                                               "127.0.0.1", 0):
        client = Client(coordinator.port)
        try:
            output = tmp_path / "result.json"
            job_id = client.submit(sample_csv(tmp_path / "input.csv"), output)
            await_until(lambda: client.status(job_id) == messages.COMPLETED)
            assert json.loads(output.read_text()) == {
                "totalRequests": 4,
                "successfulRequests": 2,
                "clientErrors": 1,
                "serverErrors": 1,
                "averageResponseTimeMs": 275.0,
                "maxResponseTimeMs": 500,
                "uniqueUsers": 3,
                "requestsByEndpoint": {
                    "/api/orders": 1,
                    "/api/products": 1,
                    "/api/search": 2
                },
            }
        finally:
            client.close()


def test_two_workers_run_jobs_in_parallel(tmp_path):
    lock = threading.Lock()
    both_started = threading.Event()
    release = threading.Event()
    count = 0

    def slow(input_path, output_path):
        nonlocal count
        with lock:
            count += 1
            if count == 2:
                both_started.set()
        assert release.wait(5)
        from pathlib import Path
        Path(output_path).write_text("{}")

    try:
        with Coordinator(0) as coordinator, \
             Worker("127.0.0.1", coordinator.port, "127.0.0.1", 0, slow), \
             Worker("127.0.0.1", coordinator.port, "127.0.0.1", 0, slow):
            client = Client(coordinator.port)
            try:
                first = client.submit(tmp_path / "one.csv",
                                      tmp_path / "one.json")
                second = client.submit(tmp_path / "two.csv",
                                       tmp_path / "two.json")
                assert both_started.wait(3), "jobs did not execute concurrently"
                release.set()
                await_until(lambda: client.status(first) == messages.COMPLETED
                            and client.status(second) == messages.COMPLETED)
            finally:
                client.close()
    finally:
        release.set()


def test_worker_without_heartbeats_times_out():
    with Coordinator(0, heartbeat_timeout=0.2,
                     scan_interval=0.025) as coordinator:
        client = Client(coordinator.port)
        try:
            client.stub.RegisterWorker(
                messages.RegisterWorkerRequest(host="127.0.0.1", port=12345))
            assert coordinator.worker_count == 1
            await_until(lambda: coordinator.worker_count == 0)
        finally:
            client.close()


def test_rpc_failure_requeues_job_for_another_worker(tmp_path):

    class BrokenWorker(services.WorkerServiceServicer):

        def ExecuteJob(self, request, context):
            context.abort(grpc.StatusCode.UNAVAILABLE, "worker stopped")

    broken = grpc.server(futures.ThreadPoolExecutor())
    services.add_WorkerServiceServicer_to_server(BrokenWorker(), broken)
    broken_port = broken.add_insecure_port("[::]:0")
    broken.start()
    try:
        with Coordinator(0) as coordinator:
            client = Client(coordinator.port)
            try:
                client.stub.RegisterWorker(
                    messages.RegisterWorkerRequest(host="127.0.0.1",
                                                   port=broken_port))
                output = tmp_path / "retried.json"
                job_id = client.submit(sample_csv(tmp_path / "input.csv"),
                                       output)
                await_until(lambda: coordinator.worker_count == 0 and client.
                            status(job_id) == messages.PENDING)
                with Worker("127.0.0.1", coordinator.port, "127.0.0.1", 0):
                    await_until(
                        lambda: client.status(job_id) == messages.COMPLETED)
                    assert output.exists()
            finally:
                client.close()
    finally:
        broken.stop(0).wait()


def test_processing_error_marks_job_failed(tmp_path):

    def fail(input_path, output_path):
        raise ValueError("bad CSV")

    with Coordinator(0) as coordinator, \
         Worker("127.0.0.1", coordinator.port, "127.0.0.1", 0, fail):
        client = Client(coordinator.port)
        try:
            job_id = client.submit(tmp_path / "bad.csv", tmp_path / "bad.json")
            await_until(lambda: client.status(job_id) == messages.FAILED)
        finally:
            client.close()
