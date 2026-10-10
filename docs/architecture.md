# Architecture and engineering decisions

## Layout and installation

Application code lives in the flat `distributed_scheduler/` package, including
its protobuf schema and generated bindings. Benchmarking and profiling live in
`benchmarks/`, demonstrations in `examples/`, and tests in `tests/`. Configuration
stays at the root, with detailed documentation in `docs/`. The root `cli.py`
provides the application entry point; benchmarks and demos run with `python -m`.

`pyproject.toml` declares runtime and optional development dependencies, packaging,
the `distributed-scheduler` command, pytest, and Ruff. Install from the repository
root with `python -m pip install -e ".[dev]"`. `requirements.txt` forwards to that
same development extra for existing `pip -r` users. Docker installs only runtime
dependencies. Editable installation also allows imports and the console command
from other working directories.

`distributed_scheduler/distributed_jobs.proto` and both generated Python bindings are committed. Runtime
users do not need a compiler; after editing the schema, developers and CI regenerate
from the root:

```sh
python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. distributed_scheduler/distributed_jobs.proto
```

Using the repository root as the protobuf include path generates package-qualified
imports in the gRPC bindings; no manual edits to generated files are needed.
Generated files are excluded from Ruff. The schema's wire interface is unchanged.

## Communication and responsibilities

Protocol Buffers define typed requests/responses; gRPC supplies generated client
stubs and concurrent RPC handlers. The protocol separates client submission and
status queries from worker registration, heartbeat, and execution. RPCs exchange
paths and control information; CSV files and JSON results use a shared filesystem.

The coordinator owns the job registry, queues, worker sessions, assignment counts,
and authoritative publication. Workers advertise capacity, execute CSV analysis,
and return success/error plus a locally measured duration. `PENDING`, `RUNNING`,
`COMPLETED`, and `FAILED` describe job state; an unknown ID returns
`JOB_STATUS_UNSPECIFIED`.

Each file is a separate job. The processor streams rows, validates the exact
header, field count, HTTP code range, and nonnegative integer response time.
Quoted CSV fields are supported. It counts successes as 200–399, client errors as
400–499, and server errors as 500–599. Informational responses count toward total
requests. Timestamps are present but not aggregated. Exact user/endpoint statistics
need O(U + E) memory; row processing is O(R) for ordinary field lengths.

## Scheduling and thread safety

A deque holds ready jobs; a min-heap holds retry deadlines based on
`time.perf_counter()`. Eligible retries move to the ready-queue tail before the
next assignment. A future retry cannot block ready work, and later submissions
cannot continually overtake an eligible retry. Concurrent submissions are ordered
when accepted under the lock. FIFO is assignment order, not completion order.

Worker selection is separate from queue ordering:

- First Available chooses the smallest available lexical address.
- Round Robin advances through available addresses, wrapping and skipping full workers.
- Least Loaded minimizes active jobs / capacity, breaking ties by address.

Policies do not estimate job cost. Equal capacities and costs can produce similar
behavior. A condition variable protects dequeue, state transitions, selection,
and capacity reservation together. The dispatcher sleeps until a state change or
the next retry deadline; no additional retry thread is needed. RPC execution and
filesystem publication do not hold the state lock.

Worker semaphores provide a second capacity check. Coordinator dispatch calls are
bounded to 32 by default; accepted jobs/history remain unbounded. At equal worker
counts, more thread slots do not imply proportional speedup for Python CPU work.

| Operation | Time | Additional space |
| --- | --- | --- |
| Ready append/pop | O(1) | O(1) per entry |
| Schedule or pop a delayed retry | O(log D) | O(1) per entry |
| Read earliest retry deadline | O(1) | O(1) |
| Select a worker | O(W) | O(W) temporary |

D is delayed retry count and W is registered worker count. Retained state is
O(J + W), with active assignments bounded by worker capacity.

## Failure recovery and result publication

Heartbeats run every two seconds. The coordinator scans once per second and
expires workers whose last heartbeat is older than six seconds. Transport errors
invalidate a worker immediately and requeue its running jobs. Session UUIDs
prevent old-process heartbeats from refreshing a replacement at the same address.
Expired workers must restart to register again.

Default retry backoff is 0.1, 0.2, then 0.4 seconds; larger retry limits cap delay
at two seconds. Three retries allow four total attempts. Timeout/connection errors
reported by execution are transient. Invalid CSV, missing input, other processing
errors, and publication errors fail immediately. Queued work waits indefinitely
when no worker is registered.

Each attempt writes `output.json.attempt-<uuid>`. The processor first writes JSON
into a temporary file in the same directory and replaces the staged path atomically.
The coordinator accepts only the current running attempt and worker incarnation.
It claims publication under the lock, releases the worker reservation, and replaces
the final output outside the lock. A claimed publication cannot be reassigned,
even if its worker expires. COMPLETED follows successful replacement; errors
produce FAILED. Duplicate and stale replies cannot publish.

These mechanisms give **at-least-once execution**, not exactly-once computation.
An expired attempt may continue; cancelling RPCs cannot interrupt arbitrary Python
code. Atomic replacement requires coherent shared visibility and a filesystem with
atomic same-directory `os.replace`. It does not guarantee power-loss durability.
Independent jobs need distinct output paths. Crashes can leave abandoned staged
files; remove them only after stopping the system.

## Metrics

`python cli.py metrics localhost 50051` returns the existing JSON snapshot through
gRPC: counts, capacity, attempts, errors, queue/execution time, and end-to-end
latency. Timestamps are relative to coordinator creation. Worker execution time
is returned as a duration; clocks on different machines are not subtracted.
Crashed attempt duration is unknown and omitted, while end-to-end latency includes
failure detection and retry time. Live throughput uses coordinator lifetime.

`--metrics-port 8000` or `METRICS_PORT=8000` enables HTTP export:

```sh
curl http://localhost:8000/metrics
```

The registry exports `scheduler_jobs_{submitted,completed,failed,retried}_total`,
`scheduler_jobs_{pending,running}`, `scheduler_active_workers`, and histograms
`scheduler_job_duration_seconds` and `scheduler_queue_wait_seconds`.
Counters/gauges reuse retained state. Histograms observe each terminal job once,
including failures. Duration is end-to-end time; queue wait includes all queue
visits and backoff. Duplicates do not add observations. No unbounded labels are
used. Scraping snapshots retained records in O(J) time.

## Benchmarks and profiling

From the root, reproduce the recorded configuration with:

```sh
python -m benchmarks.benchmark --rows 20000 --repeats 3 --output benchmarks/results/latest.json
python -m benchmarks.analyze_benchmarks benchmarks/results/latest.json
python -m benchmarks.profile_workload --rows 100000 --output-directory profiles
python -m pstats profiles/csv.prof
```

The full benchmark runs 108 cases over independent processes and real gRPC.
Equal/varied workloads have 12 files; high-load/failure workloads have 48. Varied
costs repeat 1×, 4×, 16×, and 2× rows. Seeded case shuffling reduces order effects;
inputs are deterministic and reused, with fresh processes and distinct outputs.
Initial process startup/input generation are excluded from batch duration.

Failure injection is isolated in benchmark code: exactly one first attempt blocks
until its process is killed; subsequent attempts process normally. N > 1 loses
one worker's capacity. For N = 1, replacement startup counts as recovery time.
The benchmark verifies a kill and retry occurred. No artificial delay was added
to production CSV processing.

Batch duration uses first acceptance through last terminal result, excluding final
polling delay. Throughput counts completed jobs. p50 is the median job latency;
p95 uses nearest rank. Analysis reports mean/median duration, sample standard
deviation, averaged per-run latency percentiles, queue wait, and differences from
First Available. Speedup T(1)/T(N) and efficiency speedup/N are per policy and
exclude failure runs because capacity changes.

psutil samples approximately every 10 ms during submission/completion polling.
CPU percentages divide process CPU deltas by the resource window, including final
polling delay; 100% means one core and worker CPU is summed across processes.
RSS peaks are sampled lower bounds. Total peak is the maximum sampled sum, not
the sum of independent peaks. Killed workers retain their pre-kill sample;
replacements are measured after registration. Clock granularity can give zero
CPU for very short cases. Polling and sampling themselves add overhead.

Committed results are previously recorded measurements, checked during repository
preparation rather than rerun in full. The 2026-10-10 dataset has environment
metadata and resource measurements; the 2026-10-09 baseline lacks newer fields
(displayed as `nan` by analysis). They are not directly comparable across environments.
The datasets use 100 users and 8 endpoints and do not measure high-cardinality
memory growth. Noise is visible even in one-worker controls; some calculated
efficiencies exceed 100%. These are local CPU/filesystem experiments, not cluster
scalability evidence or a claim that one policy always wins.

The profiler isolates selection, CSV, and the client. Selection simulates 100,000
assignments with four two-slot workers. CSV processes 12 files sequentially, with
no gRPC server threads. The client uses servers in separate processes to avoid
process-wide profiling events mixing their work. Profiles cover selected paths,
not native gRPC internals or server lock contention. Profiling changes timings.
A previously recorded 1.2-million-row CSV profile took 5.819 s under profiling,
with 4.423 s directly in `process`; client polling mostly slept. These numbers
are not additive or directly comparable with unprofiled benchmark durations.

## Docker Compose

A manual Docker demo produced the expected report for 1,000 generated rows.
Automated container lifecycle and HTTP metrics checks remain pending because the
verification environment cannot access the Docker socket:

```sh
SCHEDULER=round-robin WORKER_CAPACITY=2 docker compose up --build -d
docker compose exec coordinator python -m examples.generate_demo_data --directory /data/input
docker compose exec coordinator python -m examples.demo_client --input-directory /data/input --output-directory /data/output
docker compose exec coordinator python cli.py metrics localhost 50051
curl http://localhost:8000/metrics
docker compose down
```

Compose starts a coordinator and three workers after a readiness check. `./data`
is mounted as `/data` in every container; results remain in `data/output`. Host
clients must submit container paths such as `/data/input/requests-1.csv`. Ports
50051 and 8000 must be free. Images use committed protobuf bindings and install
runtime dependencies from `pyproject.toml`. Containers currently run as root;
generated host files can be root-owned, while JSON results have mode 0644.
