"""Profile isolated worker selection, streaming CSV, and a real gRPC client."""
import argparse
import cProfile
import json
import pstats
import tempfile
import time
from pathlib import Path

import grpc

from benchmarks.benchmark import generate_inputs, service
from distributed_scheduler import distributed_jobs_pb2 as messages
from distributed_scheduler import distributed_jobs_pb2_grpc as services
from distributed_scheduler.coordinator import Worker
from distributed_scheduler.csv_processor import process
from distributed_scheduler.scheduling import POLICIES, select_worker


def profile_selection(policy: str, iterations: int = 100000) -> None:
    workers = [Worker("profile", port, 0, max_concurrent_jobs=2) for port in range(4)]
    previous = None
    for index in range(iterations):
        selected = select_worker(workers, policy, previous)
        if selected is None:
            for worker in workers:
                worker.jobs.clear()
            selected = select_worker(workers, policy, previous)
        selected.jobs.add(str(index))
        previous = selected.key


def process_batch(inputs: list[Path], output: Path) -> None:
    for index, source in enumerate(inputs):
        process(str(source), str(output / f"{index}.json"))


def profile_client(inputs: list[Path], output: Path, policy: str) -> cProfile.Profile:
    # Servers live in separate processes so their execution cannot contaminate
    # client attribution on Python versions with process-wide cProfile events.
    profile = cProfile.Profile()
    with service("coordinator", "0", "--scheduler", policy) as port, \
            service("worker", "127.0.0.1", str(port), "127.0.0.1", "0"), \
            grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
        stub = services.CoordinatorServiceStub(channel)

        def batch():
            for index, source in enumerate(inputs):
                stub.SubmitJob(messages.SubmitJobRequest(
                    input_path=str(source), output_path=str(output / f"{index}.json")), timeout=5)
            deadline = time.perf_counter() + 120
            while True:
                metrics = json.loads(stub.GetMetrics(messages.MetricsRequest(), timeout=5).json)
                if metrics["completed"] + metrics["failed"] == len(inputs):
                    if metrics["failed"]:
                        raise RuntimeError("profile workload failed")
                    return
                if time.perf_counter() > deadline:
                    raise TimeoutError("profile workload timed out")
                time.sleep(0.01)

        profile.runcall(batch)
    return profile


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--scheduler", choices=POLICIES, default="least-loaded")
    parser.add_argument("--output-directory", type=Path, default=Path("profiles"))
    args = parser.parse_args()
    if args.rows < 1:
        parser.error("rows must be positive")
    args.output_directory.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="csv-profile-") as temporary:
        directory = Path(temporary)
        inputs = generate_inputs(directory / "inputs", "equal", args.rows)
        selection = cProfile.Profile()
        selection.runcall(profile_selection, args.scheduler)
        csv = cProfile.Profile()
        csv.runcall(process_batch, inputs, directory / "csv")
        client = profile_client(inputs, directory / "client", args.scheduler)
        for kind, profile in (("scheduling", selection), ("csv", csv), ("client", client)):
            profile.dump_stats(args.output_directory / f"{kind}.prof")
            print(f"\n{kind}: cumulative seconds (profiling changes timings)")
            pstats.Stats(profile).strip_dirs().sort_stats("cumulative").print_stats(12)


if __name__ == "__main__":
    main()
