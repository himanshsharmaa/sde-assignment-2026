import os
from pathlib import Path
import subprocess
import sys

import pytest

from tools.verify_submission import (
    _descendant_process_ids,
    _publish_generated_report,
    _wait_controller,
)


ROOT = Path(__file__).resolve().parents[1]


def test_submission_verifier_does_not_reference_simulator_private_truth():
    source = (ROOT / "tools" / "verify_submission.py").read_text(encoding="utf-8")

    assert "_truth" not in source


def test_invalid_generated_report_does_not_replace_existing_report(tmp_path):
    destination = tmp_path / "public_01.md"
    candidate = tmp_path / "candidate.md"
    destination.write_text("previous valid report\n", encoding="utf-8")
    candidate.write_text("incomplete output\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="invalid"):
        _publish_generated_report(candidate, destination, "# Grade report")

    assert destination.read_text(encoding="utf-8") == "previous valid report\n"
    assert candidate.read_text(encoding="utf-8") == "incomplete output\n"


def test_valid_generated_report_atomically_replaces_existing_report(tmp_path):
    destination = tmp_path / "public_01.md"
    candidate = tmp_path / "candidate.md"
    destination.write_text("previous valid report\n", encoding="utf-8")
    candidate.write_text("# Grade report - public_01\n", encoding="utf-8")

    _publish_generated_report(candidate, destination, "# Grade report")

    assert destination.read_text(encoding="utf-8") == "# Grade report - public_01\n"
    assert not candidate.exists()


def test_descendant_process_ids_include_nested_python_launcher():
    assert _descendant_process_ids(
        10, [(10, 1), (11, 10), (12, 11), (20, 1)]
    ) == {10, 11, 12}


@pytest.mark.skipif(os.name != "nt", reason="Windows process-tree sampling")
def test_wait_controller_measures_python_child_working_set(tmp_path):
    log = (tmp_path / "controller.log").open("w", encoding="utf-8")
    process = subprocess.Popen([
        sys.executable,
        "-c",
        "x=bytearray(96*1024*1024); x[::4096]=b'x'*(len(x)//4096); "
        "import time; time.sleep(1.5)",
    ])
    process._verification_log = log

    exit_code, peak_working_set = _wait_controller(process, timeout=10)

    assert exit_code == 0
    assert peak_working_set is not None
    assert peak_working_set > 60 * 1024 * 1024
