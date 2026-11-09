#!/usr/bin/env python3
"""Run public scenarios, generate grader reports, and capture a restart proof."""
from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SCENARIOS = {
    "public_01": (Path("scenarios") / "public_01_dev.yaml", "public_01_dev"),
    "public_02": (Path("scenarios") / "public_02_incidents.yaml", "public_02_incidents"),
    "public_03": (Path("scenarios") / "public_03_load.yaml", "public_03_load"),
}
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def _run(args: list[str], *, cwd: Path = ROOT, output: Any = subprocess.PIPE) -> str:
    env = os.environ.copy()
    env["PYTHONUTF8"] = "1"
    result = subprocess.run(
        args, cwd=cwd, stdout=output, stderr=subprocess.STDOUT, text=True,
        check=False, env=env,
    )
    if result.returncode:
        detail = result.stdout if isinstance(result.stdout, str) else "(see command log)"
        raise RuntimeError(f"command failed ({result.returncode}): {args}\n{detail}")
    return result.stdout or ""


def _state(url: str) -> dict[str, Any]:
    with OPENER.open(f"{url}/sim/state", timeout=3) as response:
        return json.load(response)


def _health(url: str) -> bool:
    try:
        with OPENER.open(f"{url}/health", timeout=2) as response:
            return response.status == 200
    except (OSError, urllib.error.URLError):
        return False


def _port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _server(scenario: Path, run_dir: str, port: int, log_path: Path) -> subprocess.Popen:
    log = log_path.open("w", encoding="utf-8")
    env = os.environ.copy()
    env["PYTHONUTF8"] = "1"
    process = subprocess.Popen(
        [
            sys.executable, "-m", "sim", "serve", "--scenario", str(scenario),
            "--run-dir", run_dir, "--host", "127.0.0.1", "--port", str(port),
        ],
        cwd=ROOT,
        stdout=log,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
    )
    process._verification_log = log
    url = f"http://127.0.0.1:{port}"
    deadline = time.monotonic() + 900
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"simulator exited during startup; see {log_path}")
        if _health(url):
            return process
        time.sleep(0.2)
    process.terminate()
    process.wait(timeout=10)
    raise TimeoutError(f"simulator health check timed out; see {log_path}")


def _controller(url: str, db_path: str, log_path: Path) -> subprocess.Popen:
    log = log_path.open("a", encoding="utf-8")
    env = os.environ.copy()
    env["PYTHONUTF8"] = "1"
    process = subprocess.Popen(
        [
            sys.executable, "agentic_controller.py", "--url", url, "--db", db_path,
        ],
        cwd=ROOT,
        stdout=log,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
    )
    process._verification_log = log
    return process


def _descendant_process_ids(
    root_pid: int, entries: list[tuple[int, int]]
) -> set[int]:
    process_ids = {root_pid}
    while True:
        children = {
            pid for pid, parent_pid in entries if parent_pid in process_ids
        } - process_ids
        if not children:
            return process_ids
        process_ids.update(children)


def _wait_controller(
    process: subprocess.Popen, timeout: int = 900
) -> tuple[int, int | None]:
    peak_working_set = None
    try:
        if os.name != "nt":
            return process.wait(timeout=timeout), None

        import ctypes
        from ctypes import wintypes

        class ProcessMemoryCounters(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        class ProcessEntry32(ctypes.Structure):
            _fields_ = [
                ("dwSize", wintypes.DWORD),
                ("cntUsage", wintypes.DWORD),
                ("th32ProcessID", wintypes.DWORD),
                ("th32DefaultHeapID", ctypes.c_size_t),
                ("th32ModuleID", wintypes.DWORD),
                ("cntThreads", wintypes.DWORD),
                ("th32ParentProcessID", wintypes.DWORD),
                ("pcPriClassBase", wintypes.LONG),
                ("dwFlags", wintypes.DWORD),
                ("szExeFile", wintypes.WCHAR * 260),
            ]

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        psapi = ctypes.WinDLL("psapi", use_last_error=True)
        kernel32.CreateToolhelp32Snapshot.argtypes = [
            wintypes.DWORD, wintypes.DWORD
        ]
        kernel32.CreateToolhelp32Snapshot.restype = wintypes.HANDLE
        kernel32.Process32FirstW.argtypes = [
            wintypes.HANDLE, ctypes.POINTER(ProcessEntry32)
        ]
        kernel32.Process32FirstW.restype = wintypes.BOOL
        kernel32.Process32NextW.argtypes = [
            wintypes.HANDLE, ctypes.POINTER(ProcessEntry32)
        ]
        kernel32.Process32NextW.restype = wintypes.BOOL
        kernel32.OpenProcess.argtypes = [
            wintypes.DWORD, wintypes.BOOL, wintypes.DWORD
        ]
        kernel32.OpenProcess.restype = wintypes.HANDLE
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        psapi.GetProcessMemoryInfo.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(ProcessMemoryCounters),
            wintypes.DWORD,
        ]
        psapi.GetProcessMemoryInfo.restype = wintypes.BOOL
        deadline = time.monotonic() + timeout
        while process.poll() is None:
            snapshot = kernel32.CreateToolhelp32Snapshot(0x00000002, 0)
            if snapshot == ctypes.c_void_p(-1).value:
                error = ctypes.get_last_error()
                raise OSError(error, ctypes.FormatError(error))
            entries: list[tuple[int, int]] = []
            try:
                entry = ProcessEntry32()
                entry.dwSize = ctypes.sizeof(entry)
                if kernel32.Process32FirstW(snapshot, ctypes.byref(entry)):
                    while True:
                        entries.append((
                            int(entry.th32ProcessID),
                            int(entry.th32ParentProcessID),
                        ))
                        if not kernel32.Process32NextW(
                            snapshot, ctypes.byref(entry)
                        ):
                            break
            finally:
                kernel32.CloseHandle(snapshot)

            working_set = 0
            for pid in _descendant_process_ids(process.pid, entries):
                process_handle = kernel32.OpenProcess(
                    0x0400 | 0x0010, False, pid
                )
                if not process_handle:
                    error = ctypes.get_last_error()
                    if error in (87, 1168):
                        continue
                    raise OSError(error, ctypes.FormatError(error))
                try:
                    counters = ProcessMemoryCounters()
                    counters.cb = ctypes.sizeof(counters)
                    if not psapi.GetProcessMemoryInfo(
                        process_handle, ctypes.byref(counters), counters.cb
                    ):
                        error = ctypes.get_last_error()
                        raise OSError(error, ctypes.FormatError(error))
                    working_set += int(counters.WorkingSetSize)
                finally:
                    kernel32.CloseHandle(process_handle)
            peak_working_set = max(peak_working_set or 0, working_set)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                process.terminate()
                process.wait(timeout=15)
                raise TimeoutError(
                    "controller exceeded the 15-minute verification limit"
                )
            time.sleep(min(0.25, remaining))
        return int(process.returncode), peak_working_set
    except subprocess.TimeoutExpired:
        process.terminate()
        process.wait(timeout=15)
        raise TimeoutError("controller exceeded the 15-minute verification limit")
    finally:
        process._verification_log.close()


def _stop_server(process: subprocess.Popen) -> None:
    if process.poll() is None:
        process.terminate()
        process.wait(timeout=15)
    process._verification_log.close()


def _publish_generated_report(
    candidate: Path, destination: Path, expected_header: str
) -> None:
    if not candidate.read_text(encoding="utf-8").startswith(expected_header):
        raise RuntimeError(f"generated report {candidate} is invalid")
    candidate.replace(destination)


def run_scenario(name: str, *, crash: bool = False) -> dict[str, Any]:
    scenario_path, scenario_name = SCENARIOS[name]
    run_id = f"{time.time_ns()}"
    run_name = Path("runs") / f"verification_{run_id}_{scenario_name}"
    db_path = Path(".controller") / f"verification_{run_id}_{scenario_name}.sqlite3"
    logs = ROOT / "runs"
    logs.mkdir(exist_ok=True)

    port = _port()
    url = f"http://127.0.0.1:{port}"
    server = _server(
        scenario_path, str(run_name), port,
        logs / f"verification_{run_id}_{scenario_name}_server.log"
    )
    started = time.perf_counter()
    try:
        controller = _controller(
            url, str(db_path), logs / f"verification_{run_id}_{scenario_name}_controller.log"
        )
        killed_at = None
        if crash:
            deadline = time.monotonic() + 300
            while time.monotonic() < deadline and controller.poll() is None:
                state = _state(url)
                if 0 < state["sim_minute"] < state["end_minute"]:
                    killed_at = state
                    controller.terminate()
                    controller.wait(timeout=15)
                    controller._verification_log.close()
                    break
                time.sleep(0.05)
            if killed_at is None:
                controller.terminate()
                controller.wait(timeout=15)
                controller._verification_log.close()
                raise RuntimeError("could not interrupt the controller mid-run")
            controller = _controller(
                url, str(db_path), logs / f"verification_{run_id}_{scenario_name}_controller.log"
            )
        exit_code, peak_working_set_bytes = _wait_controller(controller)
        if exit_code:
            raise RuntimeError(
                f"controller exited with {exit_code}; inspect "
                f"runs/verification_{scenario_name}_controller.log"
            )
        final = _state(url)
        if not final["finished"]:
            raise RuntimeError(f"controller exited before scenario completion: {final}")
        elapsed = time.perf_counter() - started
    finally:
        _stop_server(server)

    if name in SCENARIOS:
        baseline_path = Path("runs") / (
            f"verification_{run_id}_baseline_{scenario_name}"
        )
        _run([
            sys.executable, "-m", "sim", "baseline", "--scenario", str(scenario_path),
            "--run-dir", str(baseline_path),
        ])
        report_name = "public_03_load.md" if name == "public_03" else f"{name}.md"
        report_path = ROOT / "reports" / report_name
        report_path.parent.mkdir(exist_ok=True)
        candidate_report = report_path.with_name(
            f".{name}.{run_id}.candidate.md"
        )
        try:
            _run([
                sys.executable, "-m", "sim", "grade", "--run", str(run_name),
                "--baseline", str(baseline_path),
                "--out", str(candidate_report.relative_to(ROOT)),
            ])
            if name == "public_03":
                days = float(final["end_minute"]) / 1440
                events = int(final["last_event_id"])
                peak_memory = (
                    f"{peak_working_set_bytes / (1024 * 1024):.1f} MiB"
                    if peak_working_set_bytes is not None
                    else "not measured"
                )
                with candidate_report.open("a", encoding="utf-8") as report_file:
                    report_file.write(
                        "\n## Load verification\n\n"
                        f"- Controller wall-clock: {elapsed:.2f} seconds.\n"
                        f"- Simulated runtime: {days:g} days.\n"
                        f"- Events ingested (including history): {events:,}.\n"
                        f"- Ingest throughput: {events / elapsed:,.0f} events/second.\n"
                        f"- Wall-clock per simulated day: {elapsed / days:.2f} seconds.\n"
                        f"- Peak controller process-tree working set (sampled): {peak_memory}.\n"
                        "- CPU utilization: not measured by this verifier.\n"
                        "- Hardware: this runner's host; not the required 4-vCPU/8-GB reference host.\n"
                    )
            _publish_generated_report(
                candidate_report, report_path, "# Grade report"
            )
        finally:
            if candidate_report.exists():
                candidate_report.unlink()
    if crash:
        evidence = [
            "Crash/restart verification",
            f"Scenario: {scenario_name}",
            f"Killed after simulated minute: {killed_at['sim_minute']}",
            f"Final simulated time: {final['ts']}",
            f"Final event ID: {final['last_event_id']}",
            f"Controller wall-clock seconds (includes the interrupted first process): {elapsed:.2f}",
            "Result: scenario finished after restarting against the same SQLite database.",
        ]
        evidence_path = ROOT / "reports" / "crash_recovery_public_01.txt"
        candidate_evidence = evidence_path.with_name(
            f".crash_recovery_public_01.{run_id}.candidate.txt"
        )
        candidate_evidence.write_text(
            "\n".join(evidence) + "\n", encoding="utf-8"
        )
        candidate_evidence.replace(evidence_path)
    return {"scenario": scenario_name, "elapsed_seconds": elapsed, "final_state": final}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenarios", nargs="+", choices=SCENARIOS, default=["public_01", "public_02", "public_03"]
    )
    args = parser.parse_args()
    for name in args.scenarios:
        result = run_scenario(name, crash=(name == "public_01"))
        print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
