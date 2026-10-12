import csv
import json
import logging
import os
import tempfile
import time

log = logging.getLogger(__name__)


def process(input_path: str, output_path: str) -> None:
    started = time.monotonic()
    size_mb = os.path.getsize(input_path) / 1_000_000
    log.info("CSV processing started input=%s size=%.1fMB", input_path, size_mb)

    total = successful = client_errors = server_errors = 0
    response_time_sum = max_response_time = 0
    users: set[str] = set()
    endpoints: dict[str, int] = {}

    with open(input_path, encoding="utf-8", newline="") as source:
        rows = csv.reader(source, strict=True)
        expected = ["timestamp", "user_id", "endpoint", "status_code", "response_time_ms"]

        if next(rows, None) != expected:
            raise ValueError("invalid CSV header")

        for fields in rows:
            if len(fields) != 5:
                raise ValueError("each CSV row must contain five fields")

            status = int(fields[3])
            response_time = int(fields[4])

            if not 100 <= status <= 599 or response_time < 0:
                raise ValueError("invalid HTTP status or negative response time")

            total += 1
            if 200 <= status <= 399:
                successful += 1
            elif 400 <= status <= 499:
                client_errors += 1
            elif 500 <= status <= 599:
                server_errors += 1

            response_time_sum += response_time
            max_response_time = max(max_response_time, response_time)
            users.add(fields[1])
            endpoints[fields[2]] = endpoints.get(fields[2], 0) + 1

    result = {
        "totalRequests": total,
        "successfulRequests": successful,
        "clientErrors": client_errors,
        "serverErrors": server_errors,
        "averageResponseTimeMs": response_time_sum / total if total else 0.0,
        "maxResponseTimeMs": max_response_time,
        "uniqueUsers": len(users),
        "requestsByEndpoint": dict(sorted(endpoints.items())),
    }

    output_path = os.path.abspath(output_path)
    parent = os.path.dirname(output_path)
    os.makedirs(parent, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=".csv-result-", suffix=".tmp", dir=parent)

    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as target:
            json.dump(result, target, indent=2)
            target.write("\n")
            os.chmod(temporary, 0o644)

        os.replace(temporary, output_path)
        log.info(
            "CSV processing finished input=%s rows=%s users=%s output=%s duration=%.2fs",
            input_path,
            f"{total:,}",
            f"{len(users):,}",
            output_path,
            time.monotonic() - started,
        )

    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
