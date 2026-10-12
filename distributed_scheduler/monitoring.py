from typing import TYPE_CHECKING

from prometheus_client import CollectorRegistry, Histogram
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily

if TYPE_CHECKING:
    from distributed_scheduler.coordinator import Coordinator


class SchedulerCollector:
    """Export the coordinator's existing state without duplicate event counters."""

    def __init__(self, coordinator: "Coordinator"):
        self.coordinator = coordinator

    def collect(self):
        snapshot = self.coordinator.metrics()

        for event in ("submitted", "completed", "failed", "retried"):
            key = "retries" if event == "retried" else event
            yield CounterMetricFamily(
                f"scheduler_jobs_{event}_total",
                f"Jobs {event} since coordinator startup.",
                value=snapshot[key],
            )

        for status in ("pending", "running"):
            yield GaugeMetricFamily(
                f"scheduler_jobs_{status}",
                f"Jobs currently {status}.",
                value=sum(job["status"] == status.upper() for job in snapshot["jobs"]),
            )

        yield GaugeMetricFamily(
            "scheduler_active_workers",
            "Registered live workers.",
            value=snapshot["active_workers"],
        )


def create_metrics(coordinator: "Coordinator") -> tuple[CollectorRegistry, Histogram, Histogram]:
    registry = CollectorRegistry()
    registry.register(SchedulerCollector(coordinator))

    buckets = (0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300)
    duration = Histogram(
        "scheduler_job_duration_seconds",
        "End-to-end duration of terminal jobs, including retries.",
        buckets=buckets,
        registry=registry,
    )
    queue = Histogram(
        "scheduler_queue_wait_seconds",
        "Total queued time of terminal jobs, including retry backoff.",
        buckets=buckets,
        registry=registry,
    )

    return registry, duration, queue
