import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from test_system import Client, await_until, sample_csv

from benchmarks.benchmark import service
from distributed_scheduler import distributed_jobs_pb2 as messages
from distributed_scheduler.coordinator import Coordinator
from distributed_scheduler.worker import Worker


@pytest.mark.integration
def test_killed_worker_restarts_at_same_address(tmp_path):
    started = tmp_path / "started"
    # This executor is confined to the test process; production processing is unchanged.
    script = tmp_path / "blocked_worker.py"
    script.write_text(
        "import sys, threading\n"
        "from pathlib import Path\n"
        "from distributed_scheduler.worker import Worker\n"
        "def execute(source, target):\n"
        "    Path(sys.argv[2]).write_text('started')\n"
        "    threading.Event().wait(30)\n"
        "with Worker('127.0.0.1', int(sys.argv[1]), '127.0.0.1', 0, execute) as w:\n"
        "    print(w.port, flush=True)\n"
        "    w.wait()\n"
    )
    with Coordinator(0, retry_backoff=0.01) as coordinator:
        client = Client(coordinator.port)
        try:
            process = subprocess.Popen([sys.executable, "-u", str(script), str(coordinator.port), str(started)],
                                       stdout=subprocess.PIPE, text=True, cwd=Path(__file__).resolve().parents[1],
                                       env={**os.environ, "PYTHONPATH": os.pathsep.join([
                                           str(Path(__file__).resolve().parents[1]),
                                           os.environ.get("PYTHONPATH", "")])})
            try:
                import select
                assert select.select([process.stdout], [], [], 5)[0]
                port = int(process.stdout.readline())
                address = f"127.0.0.1:{port}"
                old_session = coordinator.workers[address].session_id
                source = sample_csv(tmp_path / "input.csv")
                key = client.submit(source, tmp_path / "recovered.json")
                await_until(started.exists)
                with service("worker", "127.0.0.1", str(coordinator.port), "127.0.0.1", "0"):
                    extra = [client.submit(source, tmp_path / f"extra-{i}.json") for i in range(8)]
                    process.kill()
                    process.wait(timeout=5)
                    await_until(lambda: all(client.status(j) == messages.COMPLETED for j in [key, *extra]))
                    assert coordinator.jobs[key].attempt == 2
                    assert json.loads((tmp_path / "recovered.json").read_text())["totalRequests"] == 4
                with Worker("127.0.0.1", coordinator.port, "127.0.0.1", port) as recovered:
                    assert recovered.session_id != old_session
                    record = coordinator.workers[address]
                    heartbeat = record.last_heartbeat
                    client.stub.Heartbeat(messages.HeartbeatRequest(
                        host="127.0.0.1", port=port, session_id=old_session))
                    assert record.last_heartbeat == heartbeat
                    another = client.submit(source, tmp_path / "new.json")
                    await_until(lambda: client.status(another) == messages.COMPLETED)
                    assert coordinator.jobs[another].worker == address
                    assert record.active_jobs == 0
            finally:
                if process.poll() is None:
                    process.kill()
                process.wait(timeout=5)
                process.stdout.close()
        finally:
            client.close()
