import logging
import os
import threading
import time
import uuid
from concurrent import futures
from typing import Callable

import grpc

from distributed_scheduler import distributed_jobs_pb2 as messages
from distributed_scheduler import distributed_jobs_pb2_grpc as services
from distributed_scheduler.csv_processor import process

log = logging.getLogger(__name__)


class Worker(services.WorkerServiceServicer):
    def __init__(
        self,
        coordinator_host: str,
        coordinator_port: int,
        host: str,
        port: int,
        executor: Callable[[str, str], None] = process,
        max_concurrent_jobs: int = 1,
    ):
        if max_concurrent_jobs < 1:
            raise ValueError("capacity must be positive")

        self.max_concurrent_jobs = max_concurrent_jobs
        self.session_id = uuid.uuid4().hex
        self.slots = threading.BoundedSemaphore(max_concurrent_jobs)
        self.coordinator_address = f"{coordinator_host}:{coordinator_port}"
        self.host = host
        self.executor = executor
        self.stopping = threading.Event()

        self.rpc_executor = futures.ThreadPoolExecutor(max_workers=max_concurrent_jobs + 1)
        self.server = grpc.server(self.rpc_executor)
        services.add_WorkerServiceServicer_to_server(self, self.server)
        self.port = self.server.add_insecure_port(f"[::]:{port}")

        self.channel = None
        self.heartbeat_thread: threading.Thread | None = None

    def start(self):
        self.server.start()
        log.info("worker gRPC server started address=%s:%d", self.host, self.port)

        self.channel = grpc.insecure_channel(self.coordinator_address)
        self.stub = services.CoordinatorServiceStub(self.channel)

        try:
            log.info("registering with coordinator=%s", self.coordinator_address)
            self.stub.RegisterWorker(
                messages.RegisterWorkerRequest(
                    host=self.host,
                    port=self.port,
                    max_concurrent_jobs=self.max_concurrent_jobs,
                    session_id=self.session_id,
                ),
                timeout=5,
            )
        except Exception:
            self.close()
            raise

        log.info("worker registered address=%s:%d", self.host, self.port)
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, name="heartbeat", daemon=True)
        self.heartbeat_thread.start()
        return self

    def wait(self):
        self.server.wait_for_termination()

    def close(self):
        log.info("worker stopping address=%s:%d", self.host, self.port)
        self.stopping.set()
        # Let an in-flight heartbeat finish before closing its channel.
        if self.heartbeat_thread is not None:
            self.heartbeat_thread.join()
        if self.channel:
            self.channel.close()

        self.server.stop(0).wait()
        # Running Python computations may finish after their RPC is cancelled.
        self.rpc_executor.shutdown(wait=False, cancel_futures=True)

    def __enter__(self):
        return self.start()

    def __exit__(self, *_):
        self.close()

    def ExecuteJob(self, request, context):
        if not self.slots.acquire(blocking=False):
            context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "worker capacity exhausted")

        started = time.perf_counter()

        try:
            self.executor(request.input_path, request.output_path)

            if not context.is_active():
                try:
                    os.unlink(request.output_path)
                except FileNotFoundError:
                    pass

            return messages.ExecuteJobResponse(success=True, execution_seconds=time.perf_counter() - started)

        except Exception as error:
            return messages.ExecuteJobResponse(
                success=False,
                error_message=str(error) or type(error).__name__,
                retryable=isinstance(error, (TimeoutError, ConnectionError)),
                execution_seconds=time.perf_counter() - started,
            )

        finally:
            self.slots.release()

    def _heartbeat_loop(self):
        while not self.stopping.wait(2.0):
            try:
                self.stub.Heartbeat(
                    messages.HeartbeatRequest(host=self.host, port=self.port, session_id=self.session_id), timeout=2
                )
                log.debug("heartbeat sent coordinator=%s", self.coordinator_address)

            except grpc.RpcError as error:
                log.warning("heartbeat failed coordinator=%s code=%s", self.coordinator_address, error.code().name)
