#!/usr/bin/env python3
"""Linux entry-point fault checks without models, credentials or device access."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time


def invocation(binary, args, stderr_full=False, stop=False, timeout=4):
    read_fd = write_fd = None
    filled = 0
    child = None
    started = time.monotonic()
    try:
        if stderr_full:
            read_fd, write_fd = os.pipe()
            os.set_blocking(write_fd, False)
            try:
                while True:
                    filled += os.write(write_fd, b"x" * 4096)
            except BlockingIOError:
                pass
            os.set_blocking(write_fd, True)
        child = subprocess.Popen(
            [str(binary), *map(str, args)], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=write_fd if stderr_full else subprocess.PIPE,
        )
        output, error = child.communicate(
            b'{"type":"shutdown"}\n' if stop else b"", timeout=timeout,
        )
        elapsed = time.monotonic() - started
        return {
            "exit_code": child.returncode, "elapsed_seconds": round(elapsed, 4),
            "stdout": output.decode("utf-8", "replace"),
            "stderr": None if error is None else error.decode("utf-8", "replace"),
            "stderr_filled_bytes": filled,
            "passed": child.returncode != 0 and output == b"" and elapsed < timeout,
        }
    except subprocess.TimeoutExpired:
        child.kill()
        try:
            child.communicate(timeout=3)
            cause = "actual entry did not exit within deadline"
        except subprocess.TimeoutExpired:
            cause = "entry deadline exceeded; output pipe remained open after wrapper kill"
        return {"passed": False, "error": cause}
    finally:
        if child is not None:
            for stream in (child.stdin, child.stdout, child.stderr):
                if stream is not None:
                    stream.close()
        for fd in (read_fd, write_fd):
            if fd is not None:
                os.close(fd)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    before = hashlib.sha256(binary.read_bytes()).hexdigest()
    cases = []
    with tempfile.TemporaryDirectory(prefix="pipecat-entry-") as scratch:
        missing = Path(scratch) / "absent.json"
        for name, argv in [
            ("missing_arguments", []),
            ("missing_connection", [missing]),
            ("zero_connection", [missing, "0"]),
            ("invalid_connection", [missing, "abc"]),
            ("overflow_connection", [missing, str(2**64)]),
            ("missing_config_after_supervised_spawn", [missing, "41"]),
        ]:
            case = {"case": name, **invocation(binary, argv)}
            cases.append(case)
            print(json.dumps(case), flush=True)
        cases.append({
            "case": "full_unread_stderr_does_not_block_failure_exit",
            **invocation(binary, [missing, "41"], stderr_full=True),
        })
        print(json.dumps(cases[-1]), flush=True)
        fifo = Path(scratch) / "blocked-config.fifo"
        os.mkfifo(fifo, 0o600)
        result = invocation(binary, [fifo, "41"], stop=True, timeout=14)
        result["passed"] = result["passed"] and (
            "shutdown deadline exceeded" in (result.get("stderr") or "")
            and "kill_requested: true" in result["stderr"]
            and "status: Some" in result["stderr"]
        )
        cases.append({"case": "shutdown_contains_blocking_config_open", **result})
        print(json.dumps(cases[-1]), flush=True)
    after = hashlib.sha256(binary.read_bytes()).hexdigest()
    result = {
        "scope": "actual_linux_entry_faults_no_models_or_devices",
        "binary": str(binary), "binary_sha256": before,
        "binary_unchanged": before == after,
        "passed": before == after and all(case["passed"] for case in cases),
        "cases": cases,
        "not_qualified": ["physical media", "production owner", "non-Linux containment"],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
