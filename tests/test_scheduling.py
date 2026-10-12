from distributed_scheduler.coordinator import Worker
from distributed_scheduler.scheduling import select_worker


def workers():
    return [
        Worker("host", port, 0, capacity)
        for port, capacity in [(1, 2), (2, 4), (3, 1)]
    ]


def test_first_available_uses_address_order_and_capacity():
    pool = workers()
    assert select_worker(pool, "first-available") is pool[0]
    pool[0].jobs.update(["a", "b"])
    assert select_worker(pool, "first-available") is pool[1]


def test_round_robin_wraps_and_skips_busy_workers():
    pool = workers()
    assert select_worker(pool, "round-robin", pool[0].key) is pool[1]
    pool[1].jobs.update("abcd")
    assert select_worker(pool, "round-robin", pool[0].key) is pool[2]
    assert select_worker(pool, "round-robin", pool[2].key) is pool[0]


def test_least_loaded_uses_utilization_and_stable_ties():
    pool = workers()
    pool[0].jobs.add("a")
    pool[1].jobs.add("b")
    pool[2].jobs.add("c")
    assert select_worker(pool, "least-loaded") is pool[1]
    pool[1].jobs.add("d")
    assert select_worker(pool, "least-loaded") is pool[0]


def test_no_capacity():
    assert select_worker([], "round-robin") is None
    pool = workers()
    for worker in pool:
        worker.jobs.update(str(i) for i in range(worker.max_concurrent_jobs))
    assert select_worker(pool, "least-loaded") is None


def test_worker_order_does_not_depend_on_registration_timing():
    pool = list(reversed(workers()))
    assert select_worker(pool, "first-available").port == 1
    assert select_worker(pool, "least-loaded").port == 1
    assert select_worker(pool, "round-robin", "host:1").port == 2
