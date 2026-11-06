"""No-device final-binary probe, gated on the exact frozen source/build evidence."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from types import SimpleNamespace

REPO = Path('/workspace/scratch/b2b6f3e38b6d/pipecat-rs')
VERIFICATION = REPO.parent / 'verification'
BINARY = Path('/tmp/pipecat-hmi-native-27653505a519/debug/examples/local_speech')
OUTPUT = VERIFICATION / 'parent-guard/final-native-entry-fifo.json'
GATES = VERIFICATION / 'final-gates.json'
BUILD_LOG = VERIFICATION / 'final-native-build.log'
EXPECTED_COMMIT = '772e3ca152c53a33f70b2f47bab8610efd6e5ce8'
EXPECTED_TREE = '652aceec8a2dbf70acc6d58dcfc39af12e710f82'
SOURCES = [
    'rust/Cargo.lock',
    'rust/crates/pipecat-hepta/Cargo.toml',
    'rust/crates/pipecat-hepta/examples/local_speech.rs',
    'rust/crates/pipecat-hepta/src/parent_guard.rs',
    'rust/crates/pipecat-hepta/src/process_supervision.rs',
    'rust/scripts/qualify_hmi_media_process.py',
]
sys.dont_write_bytecode = True


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as source:
        for chunk in iter(lambda: source.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def snapshot():
    def git(*args):
        return subprocess.check_output(
            ['git', *args], cwd=REPO, text=True, timeout=5).strip()
    return {'commit': git('rev-parse', 'HEAD'), 'tree': git('rev-parse', 'HEAD^{tree}'),
            'worktree_status': git('status', '--porcelain')}


def preflight(record):
    expected = {'commit': EXPECTED_COMMIT, 'tree': EXPECTED_TREE, 'worktree_status': ''}
    record['source_before'] = snapshot()
    require(record['source_before'] == expected, 'frozen commit/tree or clean worktree mismatch')
    gates = json.loads(GATES.read_text())
    require(gates.get('passed') is True, 'final gates have not completed successfully')
    require(gates.get('source_before') == expected and gates.get('source_after') == expected,
            'final gates do not bind the expected unchanged source')
    native_builds = [gate for gate in gates.get('gates', []) if gate.get('name') == 'native-build']
    require(len(native_builds) == 1 and native_builds[0].get('exit_code') == 0,
            'successful final native build gate missing or ambiguous')
    native_build = native_builds[0]
    require(native_build.get('log') == BUILD_LOG.name, 'final native build log path mismatch')
    require(native_build.get('log_sha256') == sha256(BUILD_LOG), 'final native build log hash mismatch')
    require(all(gate.get('exit_code') == 0 for gate in gates['gates']), 'a final gate failed')
    binary_hash = sha256(BINARY)
    require(gates.get('binary_sha256') == binary_hash, 'binary differs from the final-gates artifact')
    source_hashes = {path: sha256(REPO / path) for path in SOURCES}
    require(all(gates.get('source_files_sha256', {}).get(path) == digest
                for path, digest in source_hashes.items()), 'source fingerprints differ from final gates')
    record.update({
        'binary': str(BINARY),
        'binary_sha256': binary_hash,
        'binary_mtime_ns': BINARY.stat().st_mtime_ns,
        'binary_size': BINARY.stat().st_size,
        'final_gates': str(GATES),
        'final_gates_sha256': sha256(GATES),
        'final_gates_passed': True,
        'build_log': str(BUILD_LOG),
        'build_log_sha256': native_build['log_sha256'],
        'native_build_argv': native_build['argv'],
        'source_sha256': source_hashes,
        'source_binding': 'Clean frozen commit/tree matches both final-gates snapshots; successful native-build log and binary SHA match that manifest. Running executable hashes are checked separately below.',
    })
    spec = importlib.util.spec_from_file_location(
        'final_media_process_qualification', REPO / 'rust/scripts/qualify_hmi_media_process.py')
    qualification = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(qualification)
    return qualification


def probe(record, qualification):
    wrapper = None
    native = None
    supervisor = None
    cleanup_errors = []
    with tempfile.TemporaryDirectory(prefix='pipecat-final-native-fifo-') as directory:
        fifo = Path(directory) / 'unwritten-config.fifo'
        os.mkfifo(fifo, 0o600)
        args = SimpleNamespace(binary=BINARY, config=fifo, connection_id=41)
        try:
            wrapper = subprocess.Popen([str(BINARY), str(fifo), '41'], stdin=subprocess.PIPE,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            supervisor = qualification.process_identity(wrapper.pid)
            require(supervisor is not None, 'supervisor identity was not observed')
            record['supervisor_process'] = supervisor
            record['running_supervisor_exe_sha256'] = sha256(Path(f'/proc/{wrapper.pid}/exe'))
            require(record['running_supervisor_exe_sha256'] == record['binary_sha256'],
                    'running supervisor differs from the final binary')
            deadline = time.monotonic() + 5
            while native is None:
                require(wrapper.poll() is None, 'wrapper exited before native identity was established')
                native = qualification.native_child_identity(supervisor, args)
                if native is None:
                    require(time.monotonic() < deadline, 'native identity startup deadline exceeded')
                    time.sleep(0.005)
            record['native_process'] = native
            record['running_native_exe_sha256'] = sha256(Path(f"/proc/{native['pid']}/exe"))
            require(record['running_native_exe_sha256'] == record['binary_sha256'],
                    'running native process differs from the final binary')
            # No writer ever opens this FIFO. Configuration cannot be read or
            # parsed; native model/device initialization cannot be reached.
            deadline = time.monotonic() + 5
            while True:
                current = qualification.process_identity(native['pid'])
                require(qualification.same_incarnation(native, current), 'native incarnation changed')
                blocked = Path(f"/proc/{native['pid']}/wchan").read_text().strip()
                if blocked in {'wait_for_partner', 'fifo_open'}:
                    break
                require(time.monotonic() < deadline, 'native did not block at FIFO configuration open')
                time.sleep(0.005)
            descriptors = []
            for fd in Path(f"/proc/{native['pid']}/fd").iterdir():
                try:
                    descriptors.append(os.readlink(fd))
                except FileNotFoundError:
                    pass
            require(not any(path.startswith(('/dev/snd/', '/dev/video')) for path in descriptors),
                    'unexpected media device descriptor before configuration')
            record['native_blocked_wchan'] = blocked
            record['native_open_descriptors'] = sorted(descriptors)
            started = time.monotonic()
            wrapper.kill()
            record['supervisor_exit_code'] = wrapper.wait(timeout=5)
            require(record['supervisor_exit_code'] == -signal.SIGKILL, 'supervisor did not exit on SIGKILL')
            record['native_exit'] = qualification.wait_native_exit(native, timeout=5)
            record['parent_kill_to_native_exit_ms'] = round((time.monotonic() - started) * 1000, 3)
            out, error = wrapper.communicate(timeout=2)
            record['stdout_bytes'] = len(out)
            record['stderr'] = error.decode(errors='replace')[:4096]
            require(out == b'', 'unexpected owner-facing output before configuration was read')
            record['native_terminal_observed'] = False
            record['effects_status'] = 'No device/model stage entered; no media terminal manufactured.'
            record['case_passed'] = True
        finally:
            # Popen owns the unreaped supervisor. Native cleanup uses only the
            # qualified pidfd helper; there is no numeric-PID fallback.
            if wrapper is not None:
                if native is None and supervisor is not None and wrapper.poll() is None:
                    try:
                        native = qualification.native_child_identity(supervisor, args)
                    except Exception as error:
                        cleanup_errors.append(f'native inventory: {type(error).__name__}: {error}')
                if wrapper.poll() is None:
                    try:
                        wrapper.kill()
                        wrapper.wait(timeout=5)
                    except Exception as error:
                        cleanup_errors.append(f'supervisor cleanup: {type(error).__name__}: {error}')
                if native is not None:
                    try:
                        qualification.wait_native_exit(native, timeout=1)
                    except RuntimeError:
                        record['native_emergency_cleanup'] = True
                        try:
                            record['native_cleanup_signal_sent'] = qualification.kill_recorded_native(native)
                            record['native_exit_after_cleanup'] = qualification.wait_native_exit(native, timeout=5)
                        except Exception as error:
                            cleanup_errors.append(f'pidfd native cleanup: {type(error).__name__}: {error}')
                for stream in [wrapper.stdin, wrapper.stdout, wrapper.stderr]:
                    if stream is not None:
                        try:
                            stream.close()
                        except OSError as error:
                            cleanup_errors.append(f'pipe cleanup: {type(error).__name__}: {error}')
            record['cleanup_errors'] = cleanup_errors


def main():
    require(not OUTPUT.exists(), 'final output already exists; preserve it instead of rerunning in place')
    record = {
        'schema_version': 2,
        'observed_at_utc': datetime.now(timezone.utc).isoformat(),
        'case': 'final_frozen_local_speech_supervisor_killed_during_native_config_open',
        'scope': 'Actual Linux entrypoint and parent binding; no model/device/live owner qualification.',
        'expected_commit': EXPECTED_COMMIT,
        'expected_tree': EXPECTED_TREE,
        'probe_script_sha256': sha256(Path(__file__)),
        'device_effects_performed': False,
        'model_load_attempted': False,
        'native_emergency_cleanup': False,
        'case_passed': False,
        'not_a_performance_benchmark': True,
        'passed': False,
    }
    try:
        qualification = preflight(record)
        probe(record, qualification)
        record['binary_sha256_after'] = sha256(BINARY)
        record['final_gates_sha256_after'] = sha256(GATES)
        record['build_log_sha256_after'] = sha256(BUILD_LOG)
        record['source_after'] = snapshot()
        require(record['binary_sha256_after'] == record['binary_sha256'], 'binary changed during probe')
        require(record['final_gates_sha256_after'] == record['final_gates_sha256'], 'gates changed during probe')
        require(record['build_log_sha256_after'] == record['build_log_sha256'], 'build log changed during probe')
        require(record['source_before'] == record['source_after'], 'source changed during probe')
        require(record['case_passed'] and not record['native_emergency_cleanup']
                and not record['cleanup_errors'], 'native containment or cleanup failed')
        record['passed'] = True
    except Exception as error:
        record['error'] = f'{type(error).__name__}: {error}'
    with OUTPUT.open('x') as output:
        output.write(json.dumps(record, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(record, ensure_ascii=False, indent=2))
    return 0 if record['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
