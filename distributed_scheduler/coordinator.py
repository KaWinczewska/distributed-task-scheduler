import heapq
import json
import logging
import os
import threading
import time
import uuid
from collections import deque
from concurrent import futures
from dataclasses import dataclass, field

import grpc
from prometheus_client import start_http_server

from distributed_scheduler import distributed_jobs_pb2 as messages
from distributed_scheduler import distributed_jobs_pb2_grpc as services
from distributed_scheduler.monitoring import create_metrics
from distributed_scheduler.scheduling import POLICIES, select_worker

log = logging.getLogger(__name__)


@dataclass
class Job:
    id: str
    input: str
    output: str
    status: int = messages.PENDING
    worker: str | None = None
    attempt: int = 0
    created: float = field(default_factory=time.perf_counter)
    started: float | None = None
    completed: float | None = None
    error: str = ""
    ready_at: float = 0.0
    queued_at: float = field(default_factory=time.perf_counter)
    queue_seconds: float = 0.0
    execution_seconds: float = 0.0
    attempt_id: str = ""
    publishing: bool = False


@dataclass
class Worker:
    host: str
    port: int
    last_heartbeat: float
    max_concurrent_jobs: int = 1
    session_id: str = ""
    jobs: set[str] = field(default_factory=set)

    @property
    def key(self) -> str:
        return f"{self.host}:{self.port}"

    @property
    def active_jobs(self) -> int:
        return len(self.jobs)

    @property
    def available_capacity(self) -> int:
        return self.max_concurrent_jobs - self.active_jobs


class Coordinator(services.CoordinatorServiceServicer):

    def __init__(self,
                 port: int,
                 heartbeat_timeout: float = 6.0,
                 scan_interval: float = 1.0,
                 scheduler: str = "first-available",
                 max_retries: int = 3,
                 retry_backoff: float = 0.1,
                 max_dispatches: int = 32,
                 metrics_port: int | None = None):
        if scheduler not in POLICIES or max_retries < 0 or max_dispatches < 1 or retry_backoff < 0:
            raise ValueError(
                "invalid scheduler, retry limit, backoff or dispatch limit")
        self.heartbeat_timeout = heartbeat_timeout
        self.scan_interval = scan_interval
        self.scheduler = scheduler
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.max_dispatches = max_dispatches
        self.jobs: dict[str, Job] = {}
        self.workers: dict[str, Worker] = {}
        self.pending: deque[str] = deque()
        self.delayed: list[tuple[float, str]] = []
        self.lock = threading.RLock()
        self.changed = threading.Condition(self.lock)
        self.stopping = threading.Event()
        self.calls = futures.ThreadPoolExecutor(max_workers=max_dispatches)
        self.rpc_executor = futures.ThreadPoolExecutor(max_workers=16)
        self.server = grpc.server(self.rpc_executor)
        services.add_CoordinatorServiceServicer_to_server(self, self.server)
        self.port = self.server.add_insecure_port(f"[::]:{port}")
        self.next_id = 0
        self.previous_worker: str | None = None
        self.inflight = 0
        self.rpc_calls: dict[str, grpc.Future] = {}
        self.assignments: dict[str, int] = {}
        self.retry_count = 0
        self.created = time.perf_counter()
        self.threads: list[threading.Thread] = []
        self.registry, self.duration_histogram, self.queue_histogram = create_metrics(
            self)
        self.metrics_port = metrics_port
        self.metrics_server = None
        self.metrics_thread = None

    def start(self):
        if self.metrics_port is not None:
            self.metrics_server, self.metrics_thread = start_http_server(
                self.metrics_port, registry=self.registry)
            self.metrics_port = self.metrics_server.server_port
        self.server.start()
        for target in (self._dispatch_loop, self._monitor_loop):
            thread = threading.Thread(target=target, daemon=True)
            thread.start()
            self.threads.append(thread)
        log.info("coordinator started port=%d scheduler=%s", self.port,
                 self.scheduler)
        return self

    def wait(self):
        self.server.wait_for_termination()

    @property
    def worker_count(self) -> int:
        with self.lock:
            return len(self.workers)

    def close(self):
        with self.changed:
            self.stopping.set()
            for call in self.rpc_calls.values():
                call.cancel()
            self.changed.notify_all()
        self.server.stop(0).wait()
        self.rpc_executor.shutdown(wait=False, cancel_futures=True)
        for thread in self.threads:
            thread.join()
        self.calls.shutdown(wait=True, cancel_futures=True)
        if self.metrics_server:
            self.metrics_server.shutdown()
            self.metrics_server.server_close()
            self.metrics_thread.join()

    def __enter__(self):
        return self.start()

    def __exit__(self, *_):
        self.close()

    def SubmitJob(self, request, context):
        with self.changed:
            self.next_id += 1
            job_id = f"job-{self.next_id}"
            self.jobs[job_id] = Job(job_id, request.input_path,
                                    request.output_path)
            self.pending.append(job_id)
            self.changed.notify_all()
        return messages.SubmitJobResponse(job_id=job_id)

    def GetJobStatus(self, request, context):
        with self.lock:
            job = self.jobs.get(request.job_id)
            return messages.GetJobStatusResponse(
                status=job.status if job else messages.JOB_STATUS_UNSPECIFIED)

    def RegisterWorker(self, request, context):
        if request.max_concurrent_jobs < 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                          "capacity must be positive")
        fresh = Worker(request.host, request.port, time.perf_counter(),
                       request.max_concurrent_jobs or 1, request.session_id)
        with self.changed:
            old = self.workers.get(fresh.key)
            if old:
                self._remove_worker(old, "worker re-registered")
            self.workers[fresh.key] = fresh
            self.assignments.setdefault(fresh.key, 0)
            self.changed.notify_all()
        return messages.RegisterWorkerResponse()

    def Heartbeat(self, request, context):
        with self.lock:
            worker = self.workers.get(f"{request.host}:{request.port}")
            if worker and worker.session_id == request.session_id:
                worker.last_heartbeat = time.perf_counter()
        return messages.HeartbeatResponse()

    def _dispatch_loop(self):
        while True:
            with self.changed:
                while True:
                    if self.stopping.is_set():
                        return
                    now = time.perf_counter()
                    while self.delayed and self.delayed[0][0] <= now:
                        _, job_id = heapq.heappop(self.delayed)
                        self.pending.append(job_id)
                    worker = select_worker(list(self.workers.values()),
                                           self.scheduler, self.previous_worker)
                    if self.pending and worker and self.inflight < self.max_dispatches:
                        break
                    delay = max(0, self.delayed[0][0] -
                                now) if self.delayed else None
                    self.changed.wait(timeout=delay)
                job = self.jobs[self.pending.popleft()]
                now = time.perf_counter()
                job.queue_seconds += now - job.queued_at
                job.started = job.started or now
                job.status = messages.RUNNING
                job.worker = worker.key
                job.attempt += 1
                job.attempt_id = uuid.uuid4().hex
                worker.jobs.add(job.id)
                self.previous_worker = worker.key
                self.assignments[worker.key] = self.assignments.get(
                    worker.key, 0) + 1
                self.inflight += 1
                attempt = job.attempt
                attempt_id = job.attempt_id
            self.calls.submit(self._execute, job.id, worker, attempt,
                              attempt_id)

    def _is_current(self, job: Job, worker: Worker, attempt: int) -> bool:
        return (job.status == messages.RUNNING and not job.publishing and
                job.attempt == attempt and job.worker == worker.key and
                self.workers.get(worker.key) is worker)

    def _execute(self, job_id: str, worker: Worker, attempt: int,
                 attempt_id: str):
        job = self.jobs[job_id]
        staged = f"{job.output}.attempt-{attempt_id}"
        try:
            with grpc.insecure_channel(worker.key) as channel:
                with self.lock:
                    if self.stopping.is_set() or not self._is_current(
                            job, worker, attempt):
                        return
                call = services.WorkerServiceStub(channel).ExecuteJob.future(
                    messages.ExecuteJobRequest(input_path=job.input,
                                               output_path=staged,
                                               attempt_id=attempt_id))
                with self.lock:
                    current = not self.stopping.is_set() and self._is_current(
                        job, worker, attempt)
                    if current:
                        self.rpc_calls[attempt_id] = call
                if not current:
                    call.cancel()
                    return
                response = call.result()
                self._accept_result(job, worker, attempt, response, staged)
        except (grpc.RpcError, grpc.FutureCancelledError) as error:
            with self.changed:
                if self._is_current(job, worker,
                                    attempt) and not self.stopping.is_set():
                    self._remove_worker(worker, str(error))
                self.changed.notify_all()
        finally:
            try:
                os.unlink(staged)
            except FileNotFoundError:
                pass
            except OSError:
                log.warning("cannot remove attempt file %s", staged)
            with self.changed:
                self.rpc_calls.pop(attempt_id, None)
                self.inflight -= 1
                self.changed.notify_all()

    def _accept_result(self, job: Job, worker: Worker, attempt: int,
                       response: messages.ExecuteJobResponse,
                       staged: str) -> None:
        with self.changed:
            if not self._is_current(job, worker, attempt):
                return
            worker.jobs.discard(job.id)
            job.execution_seconds += response.execution_seconds
            if not response.success:
                if response.retryable:
                    self._requeue(job, response.error_message)
                else:
                    self._finish(job, messages.FAILED, response.error_message)
                self.changed.notify_all()
                return
            # Claim publication before releasing the state lock. This attempt can
            # no longer be invalidated or retried; no filesystem I/O holds the lock.
            job.publishing = True
        try:
            os.replace(staged, job.output)
            status, error = messages.COMPLETED, ""
        except OSError as failure:
            status, error = messages.FAILED, f"publication failed: {failure}"
        with self.changed:
            self._finish(job, status, error)
            job.publishing = False
            self.changed.notify_all()

    def _finish(self, job: Job, status: int, error: str = ""):
        if job.completed is not None:
            return
        job.status = status
        job.completed = time.perf_counter()
        job.error = error
        self.duration_histogram.observe(job.completed - job.created)
        self.queue_histogram.observe(job.queue_seconds)

    def _requeue(self, job: Job, reason: str):
        job.error = reason
        if job.attempt > self.max_retries:
            self._finish(job, messages.FAILED, reason)
            return
        job.status = messages.PENDING
        job.worker = None
        job.queued_at = time.perf_counter()
        job.ready_at = job.queued_at + min(
            2.0, self.retry_backoff * 2**min(job.attempt - 1, 10))
        self.retry_count += 1
        heapq.heappush(self.delayed, (job.ready_at, job.id))
        log.warning("job requeued id=%s attempt=%d reason=%s", job.id,
                    job.attempt, reason)

    def _remove_worker(self, worker: Worker, reason: str):
        self.workers.pop(worker.key, None)
        for job_id in sorted(worker.jobs,
                             key=lambda key: self.jobs[key].created):
            job = self.jobs[job_id]
            if job.status == messages.RUNNING and job.worker == worker.key:
                call = self.rpc_calls.get(job.attempt_id)
                if call:
                    call.cancel()
                self._requeue(job, reason)
        worker.jobs.clear()

    def _monitor_loop(self):
        while not self.stopping.wait(self.scan_interval):
            with self.changed:
                cutoff = time.perf_counter() - self.heartbeat_timeout
                for worker in list(self.workers.values()):
                    if worker.last_heartbeat < cutoff:
                        self._remove_worker(worker, "heartbeat timeout")
                self.changed.notify_all()

    def metrics(self) -> dict:
        with self.lock:
            now = time.perf_counter()
            jobs = [{
                "id":
                    job.id,
                "status":
                    messages.JobStatus.Name(job.status),
                "worker":
                    job.worker,
                "attempts":
                    job.attempt,
                "error":
                    job.error,
                "created_seconds":
                    job.created - self.created,
                "started_seconds":
                    job.started - self.created if job.started else None,
                "completed_seconds":
                    job.completed - self.created if job.completed else None,
                "queue_seconds":
                    job.queue_seconds +
                    (now -
                     job.queued_at if job.status == messages.PENDING else 0),
                "execution_seconds":
                    job.execution_seconds,
                "latency_seconds": (job.completed or now) - job.created
            } for job in self.jobs.values()]
            completed = sum(
                job.status == messages.COMPLETED for job in self.jobs.values())
            return {
                "submitted":
                    len(jobs),
                "completed":
                    completed,
                "failed":
                    sum(job.status == messages.FAILED
                        for job in self.jobs.values()),
                "retries":
                    self.retry_count,
                "active_workers":
                    len(self.workers),
                "throughput":
                    completed / (now - self.created),
                "assignments":
                    dict(self.assignments),
                "jobs":
                    jobs,
                "workers": [{
                    "id": worker.key,
                    "max_concurrent_jobs": worker.max_concurrent_jobs,
                    "active_jobs": worker.active_jobs,
                    "available_capacity": worker.available_capacity,
                    "heartbeat_age_seconds": now - worker.last_heartbeat
                } for worker in self.workers.values()]
            }

    def GetMetrics(self, request, context):
        return messages.MetricsResponse(json=json.dumps(self.metrics()))
