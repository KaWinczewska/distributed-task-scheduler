import argparse
import os

import grpc

from distributed_scheduler import distributed_jobs_pb2 as messages
from distributed_scheduler import distributed_jobs_pb2_grpc as services
from distributed_scheduler.coordinator import Coordinator
from distributed_scheduler.logging_setup import configure_logging
from distributed_scheduler.scheduling import POLICIES
from distributed_scheduler.worker import Worker


def main(args: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Distributed CSV jobs")
    commands = parser.add_subparsers(dest="command", required=True)

    coordinator = commands.add_parser("coordinator")
    coordinator.add_argument("port", type=int)
    coordinator.add_argument("--scheduler", choices=POLICIES, default=os.getenv("SCHEDULER", "first-available"))
    coordinator.add_argument("--max-retries", type=int, default=int(os.getenv("MAX_RETRIES", "3")))
    coordinator.add_argument(
        "--metrics-port",
        type=int,
        default=os.getenv("METRICS_PORT"),
        help="enable Prometheus HTTP endpoint on this port",
    )

    worker = commands.add_parser("worker")
    worker.add_argument("coordinator_host")
    worker.add_argument("coordinator_port", type=int)
    worker.add_argument("host")
    worker.add_argument("port", type=int)
    worker.add_argument("--capacity", type=int, default=int(os.getenv("WORKER_CAPACITY", "1")))

    for command in ("submit", "status", "metrics"):
        sub = commands.add_parser(command)
        sub.add_argument("host")
        sub.add_argument("port", type=int)
        if command == "submit":
            sub.add_argument("input")
            sub.add_argument("output")
        elif command == "status":
            sub.add_argument("job_id")

    options = parser.parse_args(args)
    configure_logging(options.command)

    if options.command in ("coordinator", "worker"):
        if options.command == "coordinator":
            server = Coordinator(
                options.port,
                scheduler=options.scheduler,
                max_retries=options.max_retries,
                metrics_port=options.metrics_port,
            )
        else:
            server = Worker(
                options.coordinator_host,
                options.coordinator_port,
                options.host,
                options.port,
                max_concurrent_jobs=options.capacity,
            )

        with server:
            print(f"{options.command} listening on port {server.port}", flush=True)
            try:
                server.wait()
            except KeyboardInterrupt:
                pass
        return

    with grpc.insecure_channel(f"{options.host}:{options.port}") as channel:
        stub = services.CoordinatorServiceStub(channel)
        if options.command == "submit":
            response = stub.SubmitJob(
                messages.SubmitJobRequest(
                    input_path=os.path.abspath(options.input), output_path=os.path.abspath(options.output)
                )
            )
            print(response.job_id)
        elif options.command == "status":
            response = stub.GetJobStatus(messages.GetJobStatusRequest(job_id=options.job_id))
            print(messages.JobStatus.Name(response.status))
        else:
            print(stub.GetMetrics(messages.MetricsRequest()).json)


if __name__ == "__main__":
    main()
