import json

import pytest

from benchmarks.benchmark import generate_inputs, run


@pytest.mark.integration
def test_failure_benchmark_recovers_and_reports_resources(tmp_path):
    inputs = generate_inputs(tmp_path / "inputs", "equal", 10)[:4]
    result = run("least-loaded", 1, 2, inputs, tmp_path / "outputs", 15, worker_failure=True)

    assert result["worker_killed"]
    assert result["completed"] == len(inputs)
    assert result["failed"] == 0
    assert 1 <= result["retries"] <= 2
    assert result["p95_latency"] >= result["p50_latency"] > 0
    assert result["throughput"] == result["completed"] / result["seconds"]
    assert result["coordinator_cpu_seconds"] >= 0
    assert result["worker_cpu_seconds"] >= 0
    assert result["total_peak_rss_bytes"] > result["coordinator_peak_rss_bytes"] > 0
    assert len(result["worker_peak_rss_bytes"]) == 2  # killed process and replacement

    for index in range(len(inputs)):
        assert json.loads((tmp_path / "outputs" / f"{index}.json").read_text())["totalRequests"] == 10
