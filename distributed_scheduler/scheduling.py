"""Worker selection; the coordinator owns FIFO ordering and synchronization."""
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from distributed_scheduler.coordinator import Worker

POLICIES = ("first-available", "round-robin", "least-loaded")


def select_worker(workers: list["Worker"],
                  policy: str,
                  previous: str | None = None) -> "Worker | None":
    """Select in O(W), breaking ties by address independently of registration timing."""
    available = [worker for worker in workers if worker.available_capacity > 0]

    if not available:
        return None

    if policy == "least-loaded":
        return min(
            available,
            key=lambda worker:
            (worker.active_jobs / worker.max_concurrent_jobs, worker.key))

    if policy == "round-robin" and previous is not None:
        following = [worker for worker in available if worker.key > previous]

        if following:
            return min(following, key=lambda worker: worker.key)

    return min(available, key=lambda worker: worker.key)
