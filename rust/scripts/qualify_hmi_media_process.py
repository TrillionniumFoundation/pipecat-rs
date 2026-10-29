#!/usr/bin/env python3
"""Explicit Linux media-process qualification; no production owner or tool calls."""
import argparse
import json
import os
from pathlib import Path
import re
import selectors
import subprocess
import time


def owns_capture(path, pid):
    text = Path(path).read_text()
    owner = re.search(r"owner_pid\s*:\s*(\d+)", text)
    if "RUNNING" not in text or not owner:
        return False
    try:
        status = Path(f"/proc/{owner[1]}/status").read_text()
    except FileNotFoundError:
        return False
    return re.search(rf"^Tgid:\s*{pid}$", status, re.M) is not None


def run_case(args, name):
    started = time.monotonic()
    with open(args.output.parent / f"{name}.stderr.log", "wb") as stderr:
        child = subprocess.Popen(
            [str(args.binary), str(args.config)], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=stderr, bufsize=0,
        )
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
            deadline = time.monotonic() + 2
            while not owns_capture(args.capture_status, child.pid):
                if time.monotonic() >= deadline:
                    raise RuntimeError("owned RUNNING physical capture not observed")
                time.sleep(0.02)
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
                         "utteranceSequence": 1, "connectionId": 1, "outputEpoch": 2,
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
                child.kill()  # Only this explicitly owned qualification child.
            terminal = None
            for _ in range(64):
                current = event()
                if current is None:
                    break
                if current.get("type") == "media_terminal":
                    terminal = current
            code = child.wait(timeout=5)
            released = not owns_capture(args.capture_status, child.pid)
            assert released, "owned capture survives process completion"
            if name == "model_process_kill":
                assert code == -9 and terminal is None, (code, terminal)
                outcome = "unknown: abrupt native-process loss; no terminal receipt"
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
                    "owned_physical_capture_running_observed": True,
                    "owned_capture_released": released,
                    "elapsed_ms": round((time.monotonic() - started) * 1000, 2)}
        finally:
            poller.close()
            child.stdin.close()
            child.stdout.close()
            if child.poll() is None:
                child.kill()
                child.wait(timeout=5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ["binary", "config", "output", "capture-status"]:
        parser.add_argument(f"--{name}", type=Path, required=True)
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    if config.get("media_store") is not None:
        raise ValueError("use an output-only qualification config; this harness retains no microphone audio")
    if not config["stream"]["logical_session_id"].startswith("qualification:"):
        raise ValueError("explicit isolated qualification scope required")
    args.output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.umask(0o077)
    results = [run_case(args, name) for name in ["revoke", "invalid_command", "model_process_kill"]]
    result = {"schema_version": 1, "scope": "actual_native_media_process_not_live_hepta_owner",
              "cases": results,
              "not_proven": ["OS permission revocation", "physical hotplug", "live Hepta interruption/reconciliation", "tool effect rollback"]}
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
