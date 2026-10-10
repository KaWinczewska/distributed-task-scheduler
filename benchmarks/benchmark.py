"""Compare real gRPC processes on deterministic streaming CSV workloads."""
import argparse
import json
import math
import platform
import random
import select
import statistics
import subprocess
import sys
import tempfile
import time
from contextlib import ExitStack, contextmanager
from pathlib import Path

import grpc
import psutil

from distributed_scheduler import distributed_jobs_pb2 as messages
from distributed_scheduler import distributed_jobs_pb2_grpc as services
from distributed_scheduler.scheduling import POLICIES

ROOT = Path(__file__).resolve().parents[1]


@contextmanager
def service(*args: str, processes: list | None = None, entrypoint: str = "distributed_scheduler.cli"):
    with tempfile.TemporaryFile(mode="w+") as errors:
        process = subprocess.Popen([sys.executable, "-u", "-m", entrypoint, *args],
                                   cwd=ROOT, stdout=subprocess.PIPE, stderr=errors, text=True)
        if processes is not None:
            processes.append(process)
        try:
            if not select.select([process.stdout], [], [], 15)[0]:
                raise RuntimeError("server startup timed out")
            line = process.stdout.readline()
            if not line:
                errors.seek(0)
                raise RuntimeError(errors.read())
            yield int(line.split()[-1])
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            process.stdout.close()


def generate_inputs(directory: Path, scenario: str, rows: int) -> list[Path]:
    directory.mkdir(parents=True)
    count = 48 if scenario in ("high-load", "worker-failure") else 12
    paths = []
    for index in range(count):
        size = rows * (1, 4, 16, 2)[index % 4] if scenario == "varied" else rows
        path = directory / f"{index}.csv"
        with path.open("w") as target:
            target.write("timestamp,user_id,endpoint,status_code,response_time_ms\n")
            for row in range(size):
                target.write(f"2026-01-01T00:00:00Z,user_{row % 100},/api/{row % 8},200,{row % 500}\n")
        paths.append(path)
    return paths


def sample_resources(processes: list, usage: dict) -> int:
    """Sample RSS and CPU; retain the last observation when a process is killed."""
    resident = 0
    for process in processes:
        try:
            current = psutil.Process(process.pid)
            with current.oneshot():
                times = current.cpu_times()
                cpu = times.user + times.system
                rss = current.memory_info().rss
            record = usage.setdefault(process.pid, {"baseline_cpu": cpu, "cpu_seconds": 0, "peak_rss_bytes": 0})
            record["cpu_seconds"] = cpu - record["baseline_cpu"]
            record["peak_rss_bytes"] = max(record["peak_rss_bytes"], rss)
            resident += rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return resident


def failure_worker(port: int, capacity: int, marker: Path, blocked_input: str):
    """Hold exactly one attempt until SIGKILL, exclusively for the failure benchmark."""
    import threading

    from distributed_scheduler.csv_processor import process
    from distributed_scheduler.worker import Worker

    def execute(source, target):
        if source == blocked_input:
            try:
                marker.touch(exist_ok=False)
            except FileExistsError:
                pass
            else:
                threading.Event().wait()  # this process is killed by the benchmark
        process(source, target)

    with Worker("127.0.0.1", port, "127.0.0.1", 0, execute, max_concurrent_jobs=capacity) as worker:
        print(f"worker listening on port {worker.port}", flush=True)
        worker.wait()


def run(policy: str, worker_count: int, capacity: int, inputs: list[Path], output: Path,
        timeout: float, worker_failure: bool = False) -> dict:
    output.mkdir(parents=True)
    with ExitStack() as stack:
        processes = []
        usage = {}
        addresses = {}
        port = stack.enter_context(service("coordinator", "0", "--scheduler", policy, processes=processes))
        for _ in range(worker_count):
            if worker_failure:
                worker_port = stack.enter_context(service(
                    "--failure-worker", str(port), str(capacity), str(output / "failure-started"),
                    str(inputs[0]), processes=processes, entrypoint="benchmarks.benchmark"))
            else:
                worker_port = stack.enter_context(service(
                    "worker", "127.0.0.1", str(port), "127.0.0.1", "0",
                    "--capacity", str(capacity), processes=processes))
            addresses[f"127.0.0.1:{worker_port}"] = processes[-1]
        channel = stack.enter_context(grpc.insecure_channel(f"127.0.0.1:{port}"))
        stub = services.CoordinatorServiceStub(channel)
        peak_rss = sample_resources(processes, usage)
        start = time.perf_counter()
        next_sample = start + 0.01
        killed = False
        for index, path in enumerate(inputs):
            stub.SubmitJob(messages.SubmitJobRequest(input_path=str(path),
                           output_path=str(output / f"{index}.json")), timeout=5)
            if time.perf_counter() >= next_sample:
                peak_rss = max(peak_rss, sample_resources(processes, usage))
                next_sample = time.perf_counter() + 0.01
        while True:
            metrics = json.loads(stub.GetMetrics(messages.MetricsRequest(), timeout=5).json)
            peak_rss = max(peak_rss, sample_resources(processes, usage))
            if worker_failure and not killed and (output / "failure-started").exists():
                victim = addresses[metrics["jobs"][0]["worker"]]
                victim.kill()
                victim.wait(timeout=5)
                killed = True
                if worker_count == 1:
                    stack.enter_context(service("worker", "127.0.0.1", str(port), "127.0.0.1", "0",
                                                "--capacity", str(capacity), processes=processes))
                    peak_rss = max(peak_rss, sample_resources(processes, usage))
            if metrics["completed"] + metrics["failed"] == len(inputs):
                break
            if time.perf_counter() - start > timeout:
                raise TimeoutError("benchmark did not finish")
            time.sleep(0.01)
        measurement_seconds = time.perf_counter() - start
        if worker_failure and (not killed or not metrics["retries"]):
            raise RuntimeError("failure injection did not cause a retry")
        # Use the coordinator's completion timestamps, excluding polling delay.
        jobs = metrics["jobs"]
        duration = max(job["completed_seconds"] for job in jobs) - min(job["created_seconds"] for job in jobs)
        latencies = sorted(job["latency_seconds"] for job in jobs)
        coordinator_usage = usage[processes[0].pid]
        worker_usage = [usage[p.pid] for p in processes[1:] if p.pid in usage]
        worker_cpu = sum(record["cpu_seconds"] for record in worker_usage)
        return {"seconds": duration, "throughput": metrics["completed"] / duration,
                "average_latency": statistics.mean(latencies),
                "p50_latency": statistics.median(latencies),
                "p95_latency": latencies[math.ceil(0.95 * len(latencies)) - 1],
                "average_queue": statistics.mean(job["queue_seconds"] for job in jobs),
                "average_execution": statistics.mean(job["execution_seconds"] for job in jobs),
                "assignments": metrics["assignments"], "retries": metrics["retries"],
                "failed": metrics["failed"], "completed": metrics["completed"],
                "worker_killed": killed, "resource_window_seconds": measurement_seconds,
                "coordinator_cpu_seconds": coordinator_usage["cpu_seconds"],
                "coordinator_cpu_percent": 100 * coordinator_usage["cpu_seconds"] / measurement_seconds,
                "worker_cpu_seconds": worker_cpu,
                "worker_cpu_seconds_by_process": [record["cpu_seconds"] for record in worker_usage],
                "worker_cpu_percent": 100 * worker_cpu / measurement_seconds,
                "coordinator_peak_rss_bytes": coordinator_usage["peak_rss_bytes"],
                "worker_peak_rss_bytes": [record["peak_rss_bytes"] for record in worker_usage],
                "total_peak_rss_bytes": peak_rss,
                "resource_sample_interval_seconds": 0.01}


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] == "--failure-worker":
        failure_worker(int(sys.argv[2]), int(sys.argv[3]), Path(sys.argv[4]), sys.argv[5])
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4])
    parser.add_argument("--capacity", type=int, default=2)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--output", type=Path, default=Path("benchmarks/results/latest.json"))
    parser.add_argument("--scenarios", nargs="+", choices=("equal", "varied", "high-load", "worker-failure"),
                        default=["equal", "varied", "high-load", "worker-failure"])
    args = parser.parse_args()
    if min(args.rows, args.repeats, args.capacity, *args.workers) < 1:
        parser.error("rows, repeats, capacity and worker counts must be positive")
    if args.timeout <= 0:
        parser.error("timeout must be positive")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    results = []
    with tempfile.TemporaryDirectory(prefix="csv-benchmark-") as temporary:
        directory = Path(temporary)
        workloads = {scenario: generate_inputs(directory / scenario, scenario, args.rows)
                     for scenario in args.scenarios}
        cases = [(scenario, workers, policy, repeat) for scenario in workloads
                 for workers in args.workers for repeat in range(args.repeats) for policy in POLICIES]
        random.Random(42).shuffle(cases)
        for index, (scenario, workers, policy, repeat) in enumerate(cases):
            result = run(policy, workers, args.capacity, workloads[scenario], directory / f"run-{index}", args.timeout,
                         worker_failure=scenario == "worker-failure")
            result.update(scenario=scenario, workers=workers, scheduler=policy, repeat=repeat,
                          rows=args.rows, capacity=args.capacity, python=platform.python_version(),
                          platform=platform.platform(), logical_cpus=psutil.cpu_count())
            results.append(result)
            print(f"{scenario:9} workers={workers} {policy:15} repeat={repeat} "
                  f"{result['seconds']:.3f}s {result['throughput']:.1f} jobs/s", flush=True)
            args.output.write_text(json.dumps(results, indent=2) + "\n")


if __name__ == "__main__":
    main()
