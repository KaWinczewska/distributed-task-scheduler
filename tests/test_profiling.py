import pstats
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.integration
def test_profiles_record_isolated_paths(tmp_path):
    root = Path(__file__).resolve().parents[1]
    subprocess.run(
        [sys.executable, "-m", "benchmarks.profile_workload", "--rows", "10", "--output-directory", str(tmp_path)],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )

    for kind, function in (("scheduling", "select_worker"), ("csv", "process"), ("client", "batch")):
        stats = pstats.Stats(str(tmp_path / f"{kind}.prof")).stats
        assert any(name == function and calls > 0 for (_, _, name), (_, calls, _, _, _) in stats.items())
        if kind != "csv":
            assert not any(filename.endswith("csv_processor.py") for filename, _, _ in stats)
