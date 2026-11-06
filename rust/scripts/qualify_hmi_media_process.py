#!/usr/bin/env python3
"""Explicit Linux media-process qualification; no production owner or tool calls."""
import argparse
import json
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import time


def process_identity(pid):
    """Pin a proc incarnation; PID existence alone cannot reject PID reuse."""
    try:
        text = Path(f"/proc/{pid}/stat").read_text()
    except FileNotFoundError:
        return None
    # comm (field 2) may contain spaces and closing parentheses.
    fields = text.rsplit(")", 1)[1].split()
    return {"pid": pid, "parent_pid": int(fields[1]),
            "start_time_ticks": int(fields[19]), "state": fields[0]}


def same_incarnation(identity, current):
    return current is not None and all(
        current[key] == identity[key] for key in ["pid", "start_time_ticks"]
    )


def native_child_identity(supervisor, args):
    """Resolve only the unique owned --native-child of this exact supervisor."""
    current = process_identity(supervisor["pid"])
    if not same_incarnation(supervisor, current):
        raise RuntimeError("supervisor incarnation disappeared during startup")
    children = set()
    children_interface = False
    try:
        for task in Path(f"/proc/{supervisor['pid']}/task").iterdir():
            try:
                children.update(int(pid) for pid in (task / "children").read_text().split())
                children_interface = True
            except FileNotFoundError:
                continue
    except FileNotFoundError:
        return None
    if not children_interface:
        # task/TID/children depends on kernel configuration. Its absence is not
        # evidence of having no child. Fall back to a bounded metadata-only PPid
        # scan, then perform the same exact executable/arguments/incarnation
        # checks below. No other process's command line or environment is read.
        inspected = 0
        for entry in Path("/proc").iterdir():
            if not entry.name.isdecimal():
                continue
            inspected += 1
            if inspected > 32768:
                raise RuntimeError("native child metadata search budget exceeded")
            candidate = process_identity(int(entry.name))
            if candidate is not None and candidate["parent_pid"] == supervisor["pid"]:
                children.add(candidate["pid"])
    if not children:
        return None
    if len(children) != 1:
        raise RuntimeError("supervisor has no unique native child")
    pid = children.pop()
    identity = process_identity(pid)
    if identity is None:
        return None
    if identity["parent_pid"] != supervisor["pid"]:
        raise RuntimeError("native child belongs to a different parent")
    try:
        args_bytes = Path(f"/proc/{pid}/cmdline").read_bytes().rstrip(b"\0").split(b"\0")
        native_exe = Path(f"/proc/{pid}/exe").readlink()
        supervisor_exe = Path(f"/proc/{supervisor['pid']}/exe").readlink()
    except FileNotFoundError:
        return None
    expected_args = ["--native-child", str(supervisor["pid"]),
                     str(args.config), str(args.connection_id)]
    if native_exe != supervisor_exe or [os.fsdecode(arg) for arg in args_bytes[1:]] != expected_args:
        raise RuntimeError("native child executable/parent/config/connection mismatch")
    latest = process_identity(pid)
    if (not same_incarnation(identity, latest) or latest["parent_pid"] != supervisor["pid"]
            or not same_incarnation(supervisor, process_identity(supervisor["pid"]))):
        raise RuntimeError("native/supervisor incarnation changed during verification")
    return identity


def owns_capture(path, identity):
    text = Path(path).read_text()
    owner = re.search(r"owner_pid\s*:\s*(\d+)", text)
    if "RUNNING" not in text or not owner:
        return False
    try:
        status = Path(f"/proc/{owner[1]}/status").read_text()
    except FileNotFoundError:
        return False
    current = process_identity(identity["pid"])
    return (same_incarnation(identity, current)
            and current["state"] not in {"Z", "X"}
            and re.search(rf"^Tgid:\s*{identity['pid']}$", status, re.M) is not None)


def wait_native_exit(identity, timeout=5):
    """Observe the native incarnation exit, without claiming waitpid ownership."""
    deadline = time.monotonic() + timeout
    while True:
        current = process_identity(identity["pid"])
        if not same_incarnation(identity, current) or current["state"] in {"Z", "X"}:
            return {"pid": identity["pid"], "start_time_ticks": identity["start_time_ticks"],
                    "observed_state": current["state"] if same_incarnation(identity, current)
                    else "absent" if current is None else "pid_reused",
                    "exited": True, "waitpid_owned_by_harness": False}
        if time.monotonic() >= deadline:
            raise RuntimeError("original native process survived supervisor completion")
        time.sleep(0.01)


def kill_recorded_native(identity):
    """Signal a pinned process handle, never a checked-then-reused numeric PID."""
    if not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal"):
        raise RuntimeError("safe native cleanup requires Linux pidfd support")
    try:
        pidfd = os.pidfd_open(identity["pid"])
    except ProcessLookupError:
        return False
    try:
        # Open the stable handle first, then validate the recorded incarnation.
        # If that process exits after this check, the handle cannot target a new
        # process that later reuses its PID. Unsupported kernels fail closed.
        current = process_identity(identity["pid"])
        if not same_incarnation(identity, current) or current["state"] in {"Z", "X"}:
            return False
        try:
            signal.pidfd_send_signal(pidfd, signal.SIGKILL)
        except ProcessLookupError:
            return False
        return True
    finally:
        os.close(pidfd)


def run_case(args, name):
    started = time.monotonic()
    with open(args.output.parent / f"{name}.stderr.log", "wb") as stderr:
        child = subprocess.Popen(
            [str(args.binary), str(args.config), str(args.connection_id)], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=stderr, bufsize=0,
        )
        supervisor_identity = process_identity(child.pid)
        if supervisor_identity is None:
            child.wait(timeout=5)
            raise RuntimeError("supervisor exited before its process identity was observed")
        native_identity = None
        poller = selectors.DefaultSelector()
        poller.register(child.stdout, selectors.EVENT_READ)
        pending = bytearray()
        counts = {}

        def event(timeout=20):
            deadline = time.monotonic() + timeout
            while True:
                split = pending.find(b"\n")
                if split >= 0:
                    data = bytes(pending[:split])
                    del pending[:split + 1]
                    result = json.loads(data)
                    kind = result.get("type", "unknown")
                    counts[kind] = counts.get(kind, 0) + 1
                    return result
                if len(pending) > 65536:
                    raise RuntimeError("unbounded media line")
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not poller.select(remaining):
                    raise TimeoutError("media response deadline")
                data = os.read(child.stdout.fileno(), 65536)
                if not data:
                    return None
                pending.extend(data)

        def send(value):
            child.stdin.write(json.dumps(value).encode() + b"\n")
            child.stdin.flush()

        try:
            # Native model loading is distinct from a control operation. First
            # locate the exact owned child and observe its actual capture, while
            # draining early events; then test retirement's shorter deadline.
            deadline = time.monotonic() + 120
            while True:
                if child.poll() is not None:
                    raise RuntimeError("supervisor exited before native capture readiness")
                candidate = native_child_identity(supervisor_identity, args)
                if candidate is not None:
                    if native_identity is None:
                        native_identity = candidate
                    elif not same_incarnation(native_identity, candidate):
                        raise RuntimeError("native child incarnation changed during startup")
                    if owns_capture(args.capture_status, native_identity):
                        break
                if time.monotonic() >= deadline:
                    raise RuntimeError("owned RUNNING native capture not observed within startup budget")
                if poller.select(0.02):
                    try:
                        current = event(timeout=min(1, max(0.01, deadline - time.monotonic())))
                    except TimeoutError:
                        continue
                    if current is None or current.get("type") == "media_terminal":
                        raise RuntimeError("native media ended before capture readiness")
            send({"type": "retire", "request_id": 1, "output_epoch": 2})
            for _ in range(64):
                current = event()
                if current is None:
                    raise RuntimeError("native media exited before initialization acknowledgement")
                if current.get("type") == "media_ack" and current.get("request_id") == 1:
                    assert current["operation"] == "retire" and current["error"] is None, current
                    break
            else:
                raise RuntimeError("initialization acknowledgement not observed")
            if not owns_capture(args.capture_status, native_identity):
                raise RuntimeError("native capture was lost before the fault scenario")
            if name == "revoke":
                send({"type": "revoke_capture", "request_id": 2})
            elif name == "invalid_command":
                child.stdin.write(b'{"type":"invalid_qualification_command"}\n')
                child.stdin.flush()
            elif name == "model_process_kill":
                config = json.loads(args.config.read_text())
                stream = config["stream"]
                scope = {"principalId": stream["principal_id"], "deviceId": stream["device_id"],
                         "deviceGeneration": stream["device_generation"], "organId": stream["organ_id"],
                         "organGeneration": stream["organ_generation"], "logicalSessionId": stream["logical_session_id"],
                         "utteranceSequence": 1, "connectionId": args.connection_id, "outputEpoch": 2,
                         "turnId": "qualification:crash-turn", "utteranceId": "qualification:crash-utterance"}
                for request, operation in [({"type": "turn", "request_id": 2, "scope": scope}, "turn"),
                                           ({"type": "speak", "request_id": 3, "scope": scope,
                                             "text": "这是模型进程崩溃测试，排队的输出必须保持结果未知。" * 10}, "speak")]:
                    send(request)
                    for _ in range(64):
                        ack = event()
                        if ack is None:
                            raise RuntimeError("media exited before queued-crash scenario")
                        if ack.get("type") == "media_ack" and ack.get("request_id") == request["request_id"]:
                            assert ack["operation"] == operation and ack["error"] is None, ack
                            break
                    else:
                        raise RuntimeError("queued-crash acknowledgement missing")
                time.sleep(0.05)
                # Killing the supervisor must also stop the exact native child;
                # the following check independently observes that incarnation.
                child.kill()
            terminal = None
            for _ in range(64):
                current = event()
                if current is None:
                    break
                if current.get("type") == "media_terminal":
                    terminal = current
            code = child.wait(timeout=5)
            native_exit = wait_native_exit(native_identity)
            released = not owns_capture(args.capture_status, native_identity)
            assert released, "owned native capture survives process completion"
            if name == "model_process_kill":
                assert code == -9 and terminal is None, (code, terminal)
                outcome = "unknown: supervisor killed and original native process exited; no terminal receipt"
            else:
                assert terminal is not None, "terminal failure context lost"
                for key in ["local_stop_error", "input_error", "cleanup_error", "verification_error"]:
                    assert terminal[key] is None, terminal
                assert terminal["input_errors"] == 0, terminal
                if name == "revoke":
                    assert code == 0 and terminal["request_id"] == 2 and terminal["error"] is None, terminal
                    outcome = "joined capture revocation and native cleanup"
                else:
                    assert code != 0 and terminal["error"] == "invalid JSONL command", terminal
                    outcome = "failed terminal retains stream and cause"
            return {"case": name, "exit_code": code, "outcome": outcome,
                    "terminal": terminal, "event_counts": counts,
                    "supervisor_process": supervisor_identity,
                    "native_process": native_identity, "native_exit": native_exit,
                    "owned_physical_capture_running_observed": True,
                    "owned_capture_released": released,
                    "elapsed_ms": round((time.monotonic() - started) * 1000, 2)}
        finally:
            # Retain identity before killing the wrapper even on a failed setup.
            if native_identity is None and child.poll() is None:
                try:
                    native_identity = native_child_identity(supervisor_identity, args)
                except (OSError, RuntimeError):
                    pass
            poller.close()
            for stream in [child.stdin, child.stdout]:
                try:
                    stream.close()
                except OSError:
                    pass
            if child.poll() is None:
                child.kill()
                child.wait(timeout=5)
            if native_identity is not None:
                try:
                    wait_native_exit(native_identity)
                except RuntimeError as error:
                    # Cleanup is narrowly scoped to the recorded test child. It
                    # does not turn a parent-death failure into a passing case.
                    kill_recorded_native(native_identity)
                    wait_native_exit(native_identity)
                    raise RuntimeError("native containment failed; explicit test-child cleanup required") from error


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ["binary", "config", "output", "capture-status"]:
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--connection-id", type=int, required=True,
                        help="nonzero isolated qualification incarnation; never a production grant")
    args = parser.parse_args()
    if not 0 < args.connection_id <= 2**64 - 1:
        raise ValueError("connection ID must be a nonzero u64")
    config = json.loads(args.config.read_text())
    if config.get("media_store") is not None:
        raise ValueError("use an output-only qualification config; this harness retains no microphone audio")
    if not config["stream"]["logical_session_id"].startswith("qualification:"):
        raise ValueError("explicit isolated qualification scope required")
    args.output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.umask(0o077)
    results = [run_case(args, name) for name in ["revoke", "invalid_command", "model_process_kill"]]
    result = {"schema_version": 2, "scope": "actual_supervised_native_media_process_not_live_hepta_owner",
              "cases": results,
              "not_proven": ["OS permission revocation", "physical hotplug", "live Hepta interruption/reconciliation", "tool effect rollback"]}
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
