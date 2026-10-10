import argparse
import logging
import os
import time
from pathlib import Path

from distributed_scheduler.logging_setup import configure_logging

log = logging.getLogger(__name__)
HEADER = "timestamp,user_id,endpoint,status_code,response_time_ms\n"
ENDPOINTS = ("/api/search", "/api/products", "/api/orders", "/api/users", "/api/cart")
STATUSES = (200, 201, 204, 302, 400, 404, 500, 503)


def generate(path: Path, rows: int) -> None:
    started = time.monotonic()
    log.info("generating file=%s rows=%s", path, f"{rows:,}")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", buffering=1024 * 1024) as target:
        target.write(HEADER)
        buffer = []
        for index in range(rows):
            buffer.append(
                f"2026-10-06T18:42:{index % 60:02d}Z,user_{index % 1_000_000},"
                f"{ENDPOINTS[index % len(ENDPOINTS)]},{STATUSES[index % len(STATUSES)]},"
                f"{20 + index % 8981}\n"
            )
            if len(buffer) == 20_000:
                target.writelines(buffer)
                buffer.clear()
        target.writelines(buffer)
    log.info("generated file=%s size=%.1fMB duration=%.1fs",
             path, os.path.getsize(path) / 1_000_000, time.monotonic() - started)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate repeatable large CSV files for the gRPC demo")
    parser.add_argument("--files", type=int, default=3)
    parser.add_argument("--rows", type=int, default=100, help="rows in each file")
    parser.add_argument("--directory", type=Path, default=Path("demo_data"))
    args = parser.parse_args()
    configure_logging("generator")
    for number in range(1, args.files + 1):
        generate(args.directory / f"requests-{number}.csv", args.rows)


if __name__ == "__main__":
    main()
