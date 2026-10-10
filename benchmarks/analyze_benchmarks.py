"""Summarize repeated runs relative to first-available; no extra dependencies."""
import argparse
import json
import statistics
from collections import defaultdict


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", nargs="?", default="benchmarks/results/2026-10-10-local.json")
    args = parser.parse_args()
    with open(args.path) as source:
        results = json.load(source)
    groups = defaultdict(list)
    for result in results:
        groups[(result["scenario"], result["workers"], result["capacity"], result["rows"], result["scheduler"])].append(result)
    print("scenario workers capacity rows policy runs mean_s median_s stdev_s jobs/s latency p50 p95 queue "
          "delta% speedup efficiency coord_cpu% worker_cpu% peak_MiB failures retries distribution")
    for (scenario, workers, capacity, rows, policy), runs in sorted(groups.items()):
        baseline = groups.get((scenario, workers, capacity, rows, "first-available"), [])
        duration = statistics.mean(run["seconds"] for run in runs)
        spread = statistics.stdev(run["seconds"] for run in runs) if len(runs) > 1 else 0
        delta = 100 * (duration / statistics.mean(run["seconds"] for run in baseline) - 1) if baseline else float("nan")
        means = [statistics.mean(run.get(key, float("nan")) for run in runs) for key in
                 ("throughput", "average_latency", "p50_latency", "p95_latency", "average_queue")]
        single = groups.get((scenario, 1, capacity, rows, policy), [])
        # A failure changes effective worker count, so it is not a scalability control.
        speedup = statistics.mean(run["seconds"] for run in single) / duration if single and scenario != "worker-failure" else float("nan")
        resources = [statistics.mean(run.get(key, float("nan")) for run in runs) for key in
                     ("coordinator_cpu_percent", "worker_cpu_percent", "total_peak_rss_bytes")]
        resources[-1] /= 1024 ** 2
        distributions = [sorted(run["assignments"].values(), reverse=True) for run in runs]
        print(f"{scenario} {workers} {capacity} {rows} {policy} {len(runs)} "
              f"{duration:.4f} {statistics.median(run['seconds'] for run in runs):.4f} {spread:.4f} "
              + " ".join(f"{value:.4f}" for value in means)
              + f" {delta:+.1f} {speedup:.3f} {speedup / workers:.3f} "
              + " ".join(f"{value:.2f}" for value in resources)
              + f" {sum(run['failed'] for run in runs)} {sum(run['retries'] for run in runs)} {distributions}")


if __name__ == "__main__":
    main()
