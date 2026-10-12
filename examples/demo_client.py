import argparse
import logging
import time
from pathlib import Path

import grpc

from distributed_scheduler import distributed_jobs_pb2 as messages
from distributed_scheduler import distributed_jobs_pb2_grpc as services
from distributed_scheduler.logging_setup import configure_logging

log = logging.getLogger(__name__)
TERMINAL = {messages.COMPLETED, messages.FAILED}


def main() -> None:
    parser = argparse.ArgumentParser(description="Submit every demo CSV and follow its status")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--input-directory", type=Path, default=Path("demo_data"))
    parser.add_argument("--output-directory", type=Path, default=Path("demo_output"))
    parser.add_argument("--poll-seconds", type=float, default=0.5)
    args = parser.parse_args()
    configure_logging("client")

    inputs = sorted(args.input_directory.glob("requests-*.csv"))
    if not inputs:
        raise SystemExit(
            f"No demo CSVs found in {args.input_directory}; run python -m examples.generate_demo_data first"
        )

    args.output_directory.mkdir(parents=True, exist_ok=True)
    address = f"{args.host}:{args.port}"
    jobs: dict[str, tuple[Path, int | None]] = {}

    with grpc.insecure_channel(address) as channel:
        stub = services.CoordinatorServiceStub(channel)
        log.info("connected coordinator=%s files=%d", address, len(inputs))

        for input_path in inputs:
            output_path = args.output_directory / f"{input_path.stem}.json"
            log.info("submitting input=%s output=%s", input_path, output_path)
            response = stub.SubmitJob(
                messages.SubmitJobRequest(input_path=str(input_path.resolve()), output_path=str(output_path.resolve()))
            )
            jobs[response.job_id] = (input_path, None)
            log.info("submitted job=%s input=%s", response.job_id, input_path)

        while any(status not in TERMINAL for _, status in jobs.values()):
            for job_id, (path, previous) in list(jobs.items()):
                status = stub.GetJobStatus(messages.GetJobStatusRequest(job_id=job_id)).status

                if status != previous:
                    log.info(
                        "status changed job=%s input=%s status=%s", job_id, path.name, messages.JobStatus.Name(status)
                    )
                    jobs[job_id] = (path, status)

            time.sleep(args.poll_seconds)

    failed = [job_id for job_id, (_, status) in jobs.items() if status == messages.FAILED]
    log.info(
        "batch finished completed=%d failed=%d output_directory=%s",
        len(jobs) - len(failed),
        len(failed),
        args.output_directory,
    )

    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
