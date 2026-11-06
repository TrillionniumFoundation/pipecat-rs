#!/usr/bin/env python3
"""Run one original strict Clippy command after the exact Q MoQ window terminates.

No checkout, patch, retry, test matrix, model inference, or legacy same-head
default qualification is performed here. The launcher must enforce 900 seconds,
CPU 800%, 32 GiB and nice 10 and observe terminal managed jobs before admission.
The script holds one owner lock; it does not call the lock-taking v3 inner runner.
"""
import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import signal
import stat
import subprocess
import sys
import time
import traceback

ROOT = Path('/home/qian-qi/workspace/pipecat-workspace-f9f576cc6c66')
SOURCE, TARGET = ROOT / 'source-pin', ROOT / 'target-pin'
EVIDENCE = ROOT / 'evidence/strict-clippy-after-moq-pin-r1'
CONTRACT = ROOT / 'requests/moq-diagnostic-pin-r1.source-contract.json'
MOQ_RECEIPT = ROOT / 'evidence/moq-diagnostic-pin-r1/repetitions.json'
MOQ_RUNNER = ROOT / 'desktop_moq_diagnostic_pin_r1.py'
MOQ_RUNNER_SHA = 'bfbba41f328657e6db1bc3498a0636d2209e84ae163ff51c0912bc277d94e8af'
BASE = '0cc16e8123cb05aaad480d5ab7860a267d998bac'
BASE_TREE = 'aec037b9cf2be53fcf6f85fdc149382942277702'
BASE_MANIFEST = '1e07f773ca2827e8db5b06175f96137b5d85bb6dcb68229d29601efe29658535'
Q_HEAD = 'c1c3addb07dde0a143462c10961ff2494eeba90a'
Q_TREE = '4df0d197540551ed9b74d4e11c0936e5bedee5f5'
Q_MANIFEST = '23a585582dba548ece61948fcb87ec32a0689ff46bef20ace1739edd3986e20d'
Q_CONTRACT_SHA = 'c255d772305af493508754d78797cad6a8abb827aa40669b2b392b5c0b3256a3'
REVIEWED_SOURCES = {
    'rust/crates/pipecat-moq/src/tests.rs': '8695fd709059794132f88133a5d17bb6f3e82e8cd8b84dcdf9bce62176633219',
    'rust/crates/pipecat-evals/src/simulation/quality.rs': '4f5b66f3ba68317ab834ef8883ee0ed149477a2d032cae9ab243da513da02780',
}
RUST_PREFIX = ROOT / 'tools/rust-1.99.0-r1/bin'
RUST_READY = ROOT / 'evidence/rust-1.99.0-restore-r1/ready.json'
RUST_READY_SHA = '0d8da014686ec0eb66874e90527e668836830f873a3ac98f43de4451fe7bf3c9'
HELPER = ROOT / 'run_full_gate.py'
HELPER_SHA = 'c6b639d10e4e0d48910d007a161d7b35e81744bd5e31f878f960bdcd452b47e4'
C3 = ROOT / 'evidence/smart-turn-wheel-reference-pin-r3/reference.json'
ENVIRONMENT = ROOT / 'evidence/full-workspace-rust-1.99.0-r5/environment.json'
ENVIRONMENT_SHA = '2a85b9407e7990a522063635508efbfeb014e7298dafb82f4460e929acfee026'
BASE_GATE = ROOT / 'evidence/full-workspace-rust-1.99.0-r5/gate.json'
BASE_GATE_SHA = '3ecde89b358f86a3a8e36a1f182c1f2bd0e524ad56f30bcd4d56ec3b4dc397af'
ORIGINAL_STRICT = ROOT / 'evidence/strict-clippy-pin-r2/gate.json'
ORIGINAL_STRICT_SHA = 'a9778f0aabaeeb9b6b12182978361f4d160c3f4b25dbe21e3ed2a8b09e89067e'
STRICT_ARGV = [str(RUST_PREFIX / 'cargo'), 'clippy', '--workspace', '--all-targets',
               '--all-features', '--locked', '--', '-D', 'warnings']
INNER_SECONDS = 870
OUTER_SECONDS = 900
RESERVE_SECONDS = 15
BUILD_ENVIRONMENT_KEYS = [
    'CARGO_HOME', 'CARGO_TARGET_DIR', 'CARGO_BUILD_JOBS', 'CARGO_INCREMENTAL',
    'CARGO_PROFILE_DEV_DEBUG', 'CARGO_PROFILE_TEST_DEBUG', 'RUSTC', 'RUSTDOC',
    'RUSTUP_TOOLCHAIN', 'RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS', 'CC', 'CXX',
    'CXXFLAGS', 'CFLAGS', 'LDFLAGS', 'LIBCLANG_PATH', 'LD_LIBRARY_PATH',
    'PKG_CONFIG_PATH', 'PKG_CONFIG_SYSROOT_DIR', 'LK_CUSTOM_WEBRTC',
    'PIPECAT_ONNX_RUNTIME', 'ORT_DYLIB_PATH', 'PIPECAT_SILERO_MODEL',
    'PIPECAT_SMART_TURN_MODEL', 'RUST_TEST_THREADS',
]


class Failed(Exception):
    def __init__(self, message, code=127):
        super().__init__(message)
        self.code = code


def identity(path):
    path = Path(path)
    first = path.lstat()
    if not stat.S_ISREG(first.st_mode) or first.st_size > 512 * 1024 * 1024:
        raise ValueError('expected bounded regular non-symlink file: ' + str(path))
    digest, count = hashlib.sha256(), 0
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            count += len(chunk)
            if count > 512 * 1024 * 1024:
                raise ValueError('identity read exceeded its file limit')
            digest.update(chunk)
    last = path.lstat()
    fields = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns)
    if fields(first) != fields(last) or count != first.st_size:
        raise ValueError('file changed during identity observation: ' + str(path))
    return {'path': str(path), 'resolved': str(path.resolve()), 'bytes': count,
            'sha256': digest.hexdigest()}


def relative_path(value):
    return (isinstance(value, str) and bool(value) and
            not PurePosixPath(value).is_absolute() and
            '..' not in PurePosixPath(value).parts and
            '\x00' not in value)


def group_probe(pgid):
    try:
        os.killpg(pgid, 0)
        return {'present': True}
    except ProcessLookupError:
        return {'present': False}
    except OSError as error:
        return {'present': None, 'error': repr(error)}


def contain(child):
    result = {'pid': child.pid, 'pgid': child.pid, 'signals': [], 'errors': []}
    result['leader_exit_before'] = child.poll()
    result['group_before'] = group_probe(child.pid)
    result['group_survived_leader'] = (
        child.returncode is not None and result['group_before']['present'] is not False)
    if child.returncode is None or result['group_before']['present'] is not False:
        for sig, wait_seconds in ((signal.SIGTERM, 2), (signal.SIGKILL, 1)):
            if sig == signal.SIGKILL and child.poll() is not None and group_probe(child.pid)['present'] is False:
                break
            request = {'signal': int(sig), 'pgid': child.pid}
            try:
                os.killpg(child.pid, sig)
                request['result'] = 'requested'
            except ProcessLookupError:
                request['result'] = 'group absent at request'
            except OSError as error:
                request['error'] = repr(error)
                result['errors'].append(repr(error))
            result['signals'].append(request)
            try:
                child.wait(timeout=wait_seconds)
            except subprocess.TimeoutExpired:
                request['leader_wait_timed_out'] = True
            except OSError as error:
                result['errors'].append('leader wait: ' + repr(error))
    result['leader_reaped'] = child.poll() is not None
    result['leader_exit_after'] = child.returncode
    result['group_after'] = group_probe(child.pid)
    result['checks_succeeded'] = (
        result['leader_reaped'] and result['group_after']['present'] is False and
        not result['errors'])
    result['scope'] = ('Only the newly owned process group is signalled. Actual leader reap and '
                       'non-atomic final group absence do not assert individual descendant joins.')
    return result



def main():
    began = time.monotonic()
    deadline = began + INNER_SECONDS
    expected, expected_tree, contract_sha = sys.argv[1:]
    if (not all(re.fullmatch('[0-9a-f]{40}', value) for value in (expected, expected_tree)) or
            not re.fullmatch('[0-9a-f]{64}', contract_sha) or expected == BASE):
        raise ValueError('the actual published Q commit, tree and complete contract SHA are required')
    if (expected, expected_tree, contract_sha) != (Q_HEAD, Q_TREE, Q_CONTRACT_SHA):
        raise ValueError('this prepared window is restricted to the actual published Q checkpoint')
    EVIDENCE.mkdir(mode=0o700, exist_ok=False)
    result_path = EVIDENCE / 'verification.json'
    record = {
        'schema': 'pipecat-strict-clippy-after-moq-v1',
        'head': expected, 'tree': expected_tree, 'base': BASE, 'base_tree': BASE_TREE,
        'source': str(SOURCE), 'target': str(TARGET), 'phase': 'preparing',
        'start_unix': time.time(), 'source_contract_sha256': contract_sha,
        'reviewed_source_sha256': REVIEWED_SOURCES,
        'inner_whole_seconds': INNER_SECONDS, 'outer_job_seconds': OUTER_SECONDS,
        'cleanup_and_postflight_reserve_seconds': RESERVE_SECONDS,
        'resource_budget': {'cpu_percent': 800, 'memory_bytes': 34359738368,
                            'build_jobs': 8, 'nice': 10},
        'scope': 'One original strict Clippy command and two independent leaf-file format checks on actual Q; no test matrix or model execution.',
        'source_scope': 'Complete canonical source map includes documentation; the old 0cc default is an environment/history source, not Q runtime qualification.',
        'failure_policy': 'No phase retries. Ordinary rustfmt diagnostic exits remain failures while the one Clippy command still runs; timeout, containment or integrity failure stops admission.',
        'prior_failures_cleared': False, 'production_qualification': False,
        'processes': [], 'qualification_stages': [],
        'identity_before': {}, 'identity_after': {},
        'containment_exit_code': 0, 'integrity_exit_code': 0,
    }
    lock, source_before = None, None
    tracked = {}
    status = 127
    environment = dict(os.environ)
    git = shutil.which('git')
    if git is None:
        raise ValueError('git is unavailable')

    def save():
        temporary = result_path.with_suffix('.json.tmp')
        temporary.write_text(json.dumps(record, indent=2) + '\n')
        temporary.replace(result_path)

    def budget(reserve=RESERVE_SECONDS):
        if time.monotonic() + reserve >= deadline:
            raise Failed('internal whole-job budget exhausted', 124)

    def observe(path, expected_sha=None, expected_bytes=None):
        item = identity(path)
        tracked[str(path)] = Path(path)
        record['identity_before'][str(path)] = item
        if expected_sha is not None and item['sha256'] != expected_sha:
            raise ValueError('immutable file hash mismatch: ' + str(path))
        if expected_bytes is not None and item['bytes'] != expected_bytes:
            raise ValueError('immutable file size mismatch: ' + str(path))
        return item

    def run_owned(name, argv, limit, *, separate_stderr=False):
        entry = {'name': name, 'argv': argv, 'cwd': str(SOURCE / 'rust'),
                 'phase': 'preparing', 'start_unix': time.time(),
                 'phase_limit_seconds': limit, 'launch_attempts': 0,
                 'processes_started': 0, 'original_exit_code': None,
                 'timed_out': False, 'containment_exit_code': 0,
                 'log_integrity_exit_code': 0}
        record['processes'].append(entry)
        stdout_path = EVIDENCE / (name + '.stdout.log')
        stderr_path = EVIDENCE / (name + '.stderr.log') if separate_stderr else None
        phase_begin, execution_end, child = time.monotonic(), None, None
        phase_status = 127
        save()
        try:
            effective = min(limit, deadline - time.monotonic() - RESERVE_SECONDS)
            entry['effective_timeout_seconds'] = max(0, effective)
            if effective <= 0:
                raise Failed('no budget remains for phase admission: ' + name, 124)
            phase_deadline = time.monotonic() + effective
            with contextlib.ExitStack() as stack:
                stdout = stack.enter_context(stdout_path.open('xb'))
                stderr = stack.enter_context(stderr_path.open('xb')) if stderr_path else subprocess.STDOUT
                entry.update(phase='launching', launch_attempts=1)
                save()
                child = subprocess.Popen(argv, cwd=SOURCE / 'rust', env=environment,
                                         stdout=stdout, stderr=stderr, start_new_session=True)
                entry.update(phase='running', processes_started=1,
                             pid=child.pid, pgid=child.pid)
                save()
                wait_budget = max(0, min(phase_deadline, deadline - RESERVE_SECONDS) - time.monotonic())
                entry['effective_wait_seconds'] = wait_budget
                try:
                    original = child.wait(timeout=wait_budget)
                    entry['original_exit_code_observed_before_cleanup'] = original
                    phase_status = original if original >= 0 else 127
                except subprocess.TimeoutExpired:
                    entry['timed_out'] = True
                    phase_status = 124
                    entry['timeout_scope'] = 'Runner process budget; the original Clippy and rustfmt arguments were not modified.'
                finally:
                    execution_end = time.monotonic()
        except Failed as error:
            phase_status = error.code
            entry['exception'] = str(error)
        except Exception as error:
            entry['exception'] = repr(error)
            phase_status = 127
        finally:
            if execution_end is None:
                execution_end = time.monotonic()
            entry['phase_exit_code_before_containment'] = phase_status
            if child is not None:
                try:
                    cleanup = contain(child)
                    entry['cleanup'] = cleanup
                    entry['original_exit_code'] = child.returncode
                    if not cleanup['checks_succeeded'] or cleanup['group_survived_leader']:
                        entry['containment_exit_code'] = 125
                        record['containment_exit_code'] = 125
                except Exception as error:
                    entry['cleanup_error'] = repr(error)
                    entry['original_exit_code'] = child.returncode
                    entry['containment_exit_code'] = 125
                    record['containment_exit_code'] = 125
            entry['execution_elapsed_seconds'] = execution_end - phase_begin
            entry['logs'] = {}
            for label, path in [('stdout', stdout_path), ('stderr', stderr_path)]:
                if path is None:
                    continue
                if not path.exists() and entry['launch_attempts'] == 0:
                    entry['logs'][label] = {
                        'path': str(path), 'not_created_before_process_admission': True}
                    continue
                try:
                    entry['logs'][label] = identity(path)
                except Exception as error:
                    entry['logs'][label] = {'path': str(path), 'error': repr(error)}
                    entry['log_integrity_exit_code'] = 125
                    record['integrity_exit_code'] = 125
            entry['wrapper_exit_code'] = (
                125 if entry['containment_exit_code'] or entry['log_integrity_exit_code']
                else phase_status)
            entry.update(phase='completed' if child is not None and child.returncode is not None
                         else 'not_started_or_not_reaped',
                         elapsed_seconds=time.monotonic() - phase_begin, end_unix=time.time())
            save()
        return entry

    def stopped_phase(entry):
        stopped = {
            key: entry.get(key) for key in (
                'name', 'round', 'launch_attempts', 'processes_started',
                'original_exit_code_observed_before_cleanup', 'original_exit_code',
                'phase_exit_code_before_containment', 'containment_exit_code',
                'log_integrity_exit_code', 'wrapper_exit_code', 'timed_out')}
        record.setdefault('first_failed_phase', stopped)
        record['last_failed_phase'] = stopped

    def require_process(entry):
        if entry['wrapper_exit_code'] != 0:
            stopped_phase(entry)
            raise Failed('phase failed without retry: ' + entry['name'],
                         entry['wrapper_exit_code'])

    def git_read(label, arguments):
        entry = run_owned(label, [git, '-C', str(SOURCE), *arguments], 15,
                          separate_stderr=True)
        require_process(entry)
        return Path(entry['logs']['stdout']['path']).read_bytes()

    def snapshot(label):
        raw = git_read(label + '-paths',
                       ['ls-files', '-z', '--cached', '--others', '--exclude-standard'])
        files = []
        for raw_path in sorted(set(raw.split(b'\0')) - {b''}):
            budget()
            relative = os.fsdecode(raw_path)
            path = SOURCE / relative
            if path.is_symlink():
                data = os.fsencode(os.readlink(path))
                files.append({'path': relative, 'kind': 'symlink', 'size': len(data),
                              'sha256': hashlib.sha256(data).hexdigest()})
            elif path.is_file():
                item = identity(path)
                files.append({'path': relative, 'kind': 'file', 'size': item['bytes'],
                              'sha256': item['sha256']})
            else:
                files.append({'path': relative, 'kind': 'absent'})
        canonical = json.dumps(files, sort_keys=True, separators=(',', ':')).encode()
        value = {'files': files, 'manifest_sha256': hashlib.sha256(canonical).hexdigest()}
        (EVIDENCE / (label + '.json')).write_text(json.dumps(value, indent=2) + '\n')
        return value


    def intact_terminal(receipt, *, head, tree, manifest):
        return (
            receipt.get('phase') == 'completed' and
            type(receipt.get('exit_code')) is int and receipt['exit_code'] != 125 and
            receipt.get('head') == head and
            receipt.get('tree', receipt.get('base_git_tree')) == tree and
            receipt.get('source_before_sha256') == manifest and
            receipt.get('source_after_sha256') == manifest and
            receipt.get('source_unchanged') is True and
            receipt.get('status_after') == '')

    save()
    try:
        lock = (ROOT / 'qualification-owner-pin.lock').open('a')
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        record['lock_acquired'] = True
        observe(Path(__file__).resolve())
        observe(Path(sys.executable).resolve())
        observe(Path(git).resolve())
        observe(HELPER, HELPER_SHA, 5176)
        observe(CONTRACT, contract_sha)
        observe(RUST_READY, RUST_READY_SHA, 6059)
        observe(ENVIRONMENT, ENVIRONMENT_SHA, 2003)
        observe(BASE_GATE, BASE_GATE_SHA, 4847)
        observe(ORIGINAL_STRICT, ORIGINAL_STRICT_SHA, 3214)
        observe(MOQ_RUNNER, MOQ_RUNNER_SHA, 33499)
        observe(MOQ_RECEIPT)
        observe(C3)
        owner = TARGET / '.pipecat-source-owner.json'
        observe(owner)
        if json.loads(owner.read_bytes()) != {'source': str(SOURCE), 'task': 'workspace-f9f576cc6c66'}:
            raise ValueError('target is not exclusive to this source checkout')

        contract = json.loads(CONTRACT.read_bytes())
        if (contract.get('schema') != 'pipecat-moq-diagnostic-source-contract-v1' or
                contract.get('base') != BASE or contract.get('base_tree') != BASE_TREE or
                contract.get('head') != expected or contract.get('tree') != expected_tree or
                contract.get('source_manifest_sha256') != Q_MANIFEST or
                contract.get('source_sha256') != REVIEWED_SOURCES):
            raise ValueError('Q source contract differs from the two reviewed fixture edits')
        changes = contract.get('changed_paths')
        if (not isinstance(changes, list) or not all(relative_path(p) for p in changes) or
                len(changes) != len(set(changes)) or
                not set(REVIEWED_SOURCES).issubset(set(changes))):
            raise ValueError('invalid complete published changed-path contract')
        (EVIDENCE / 'source-contract.json').write_bytes(CONTRACT.read_bytes())

        preceding_moq = json.loads(MOQ_RECEIPT.read_bytes())
        if (not intact_terminal(preceding_moq, head=expected, tree=expected_tree,
                                manifest=contract['source_manifest_sha256']) or
                preceding_moq.get('head_after') != expected or
                preceding_moq.get('tree_after') != expected_tree or
                preceding_moq.get('containment_exit_code') != 0 or
                preceding_moq.get('integrity_exit_code') != 0 or
                preceding_moq.get('all_observed_files_unchanged') is not True or
                preceding_moq.get('identity_before', {}).get(str(CONTRACT), {}).get('sha256') != contract_sha or
                preceding_moq.get('identity_before', {}).get(str(MOQ_RUNNER), {}).get('sha256') != MOQ_RUNNER_SHA):
            raise ValueError('MoQ must reach a recorded terminal state on this exact unchanged Q contract')
        for entry in preceding_moq.get('processes', []) + preceding_moq.get('rounds', []):
            if entry.get('processes_started'):
                cleanup = entry.get('cleanup', {})
                if (type(entry.get('original_exit_code')) is not int or
                        entry.get('containment_exit_code') != 0 or
                        entry.get('log_integrity_exit_code') != 0 or
                        cleanup.get('checks_succeeded') is not True or
                        cleanup.get('group_survived_leader') or cleanup.get('errors')):
                    raise ValueError('MoQ process containment or raw-log integrity is unresolved')
        moq_contract = MOQ_RECEIPT.parent / 'source-contract.json'
        observe(moq_contract, contract_sha)
        if moq_contract.read_bytes() != CONTRACT.read_bytes():
            raise ValueError('MoQ did not record this exact Q contract')
        (EVIDENCE / 'preceding-moq.json').write_bytes(MOQ_RECEIPT.read_bytes())
        record['preceding_moq'] = {
            'path': str(MOQ_RECEIPT), 'sha256': record['identity_before'][str(MOQ_RECEIPT)]['sha256'],
            'head': preceding_moq['head'], 'tree': preceding_moq['tree'],
            'exit_code': preceding_moq['exit_code'],
            'execution_exit_code_before_integrity_checks': preceding_moq.get('execution_exit_code_before_integrity_checks'),
            'source_unchanged': True,
            'scope': 'A prior nonzero functional/phase result is retained, not retried or counted as Q workspace success.',
        }

        prior = json.loads(C3.read_bytes())
        if (not intact_terminal(prior, head=BASE, tree=BASE_TREE, manifest=BASE_MANIFEST) or
                prior.get('all_observed_files_unchanged') is not True or
                set(prior.get('preceding', {})) != {'grok', 'default', 'expanded', 'models', 'webrtc', 'aishell'}):
            raise ValueError('all original 0cc stages and C3 must be terminal with intact inputs')
        if preceding_moq.get('preceding_c3', {}).get('sha256') != record['identity_before'][str(C3)]['sha256']:
            raise ValueError('C3 receipt differs from the one admitted by MoQ')
        started = prior.get('probe_processes_started')
        if type(started) is not int or started not in (0, 1):
            raise ValueError('C3 process count is unavailable')
        if started:
            cleanup = prior.get('process_cleanup', {})
            if (cleanup.get('checks_succeeded') is not True or
                    cleanup.get('surviving_group_after_normal_exit') or cleanup.get('errors')):
                raise ValueError('C3 owned process cleanup is unresolved')
        record['preceding_c3'] = {
            'path': str(C3), 'sha256': record['identity_before'][str(C3)]['sha256'],
            'head': BASE, 'tree': BASE_TREE, 'exit_code': prior['exit_code'],
            'native_exit_code': prior.get('native_exit_code'),
            'comparison_exit_code': prior.get('comparison_exit_code'),
        }
        (EVIDENCE / 'preceding-c3.json').write_bytes(C3.read_bytes())
        for name, receipt in prior['preceding'].items():
            path = Path(receipt['path'])
            if path.parent.parent != ROOT / 'evidence' or receipt.get('phase') not in (
                    'completed', 'completed_successfully', 'completed_with_failures'):
                raise ValueError('C3 preceding receipt path/phase escaped its owned evidence')
            observe(path, receipt['sha256'])

        base_gate = json.loads(BASE_GATE.read_bytes())
        old_strict = json.loads(ORIGINAL_STRICT.read_bytes())
        if (not intact_terminal(base_gate, head=BASE, tree=BASE_TREE, manifest=BASE_MANIFEST) or
                not intact_terminal(old_strict, head=BASE, tree=BASE_TREE, manifest=BASE_MANIFEST) or
                base_gate.get('exit_code') != 101 or old_strict.get('exit_code') != 101 or
                old_strict.get('argv') != STRICT_ARGV):
            raise ValueError('the original 0cc default/strict evidence or strict command changed')
        record['original_0cc_scope'] = {
            'default_exit_code': base_gate['exit_code'],
            'strict_exit_code': old_strict['exit_code'],
            'head': BASE, 'tree': BASE_TREE,
            'statement': 'Historical nonzero evidence and native environment provenance only; this default was not run on Q.',
        }
        (EVIDENCE / 'original-0cc-strict-gate.json').write_bytes(ORIGINAL_STRICT.read_bytes())
        (EVIDENCE / 'original-0cc-default-gate.json').write_bytes(BASE_GATE.read_bytes())

        rust_ready = json.loads(RUST_READY.read_bytes())
        if (rust_ready.get('prefix') != str(RUST_PREFIX.parent) or
                rust_ready.get('manifest_sha256') != 'ce6dddc886364f8d786514771212cebe9b731ba82d6b859951c6b0ccc516b6a2'):
            raise ValueError('official Rust 1.99.0 restore identity differs')
        for name, tool in rust_ready['executables'].items():
            path = Path(tool['path'])
            if path.parent != RUST_PREFIX or name != path.name or tool.get('exit_code') != 0:
                raise ValueError('unexpected pinned Rust executable')
            observe(path, tool['sha256'])
        for name in ('rustdoc', 'clippy-driver'):
            observe(RUST_PREFIX / name)
        overrides = json.loads(ENVIRONMENT.read_bytes())
        if not isinstance(overrides, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in overrides.items()):
            raise ValueError('native environment receipt is invalid')
        overrides.update(CARGO_BUILD_JOBS='8', CARGO_TARGET_DIR=str(TARGET))
        if overrides != old_strict.get('environment_overrides'):
            raise ValueError('native environment overrides differ from the original strict command')
        environment = dict(os.environ, **overrides)
        effective = {key: environment[key] for key in BUILD_ENVIRONMENT_KEYS if key in environment}
        if effective != base_gate.get('effective_build_environment'):
            raise ValueError('recorded effective native build environment differs from original 0cc')
        if (environment.get('RUSTC') != str(RUST_PREFIX / 'rustc') or
                environment.get('RUSTDOC') != str(RUST_PREFIX / 'rustdoc') or
                environment.get('RUSTUP_TOOLCHAIN') != '1.99.0'):
            raise ValueError('native environment is not bound to the exact Rust pin')
        for name in ('CC', 'CXX'):
            value = environment.get(name)
            if value and Path(value).is_absolute() and Path(value).is_file():
                observe(Path(value).resolve())
        record['environment_overrides'] = overrides
        record['effective_build_environment'] = effective
        record['environment_comparison_scope'] = 'Exact original overrides plus the original full-gate whitelist; no claim of an unrecorded historical complete environment.'
        (EVIDENCE / 'environment.json').write_text(json.dumps(overrides, indent=2) + '\n')

        if git_read('source-head', ['rev-parse', 'HEAD']).decode().strip() != expected:
            raise ValueError('source checkout must already be updated to the published Q freeze')
        if git_read('source-tree', ['rev-parse', 'HEAD^{tree}']).decode().strip() != expected_tree:
            raise ValueError('Q source tree differs')
        if git_read('source-status', ['status', '--porcelain=v1']):
            raise ValueError('source checkout has unreviewed changes')
        if git_read('base-tree', ['rev-parse', BASE + '^{tree}']).decode().strip() != BASE_TREE:
            raise ValueError('original 0cc tree differs')
        actual_changes = [os.fsdecode(p) for p in git_read(
            'source-diff', ['diff', '--no-renames', '--name-only', '-z', BASE, expected]).split(b'\0') if p]
        if sorted(actual_changes) != sorted(changes):
            raise ValueError('published Q diff differs from the complete root-reviewed path list')
        if sorted(p for p in actual_changes if not p.startswith('rust/docs/')) != sorted(REVIEWED_SOURCES):
            raise ValueError('Q changes outside documentation exceed the two reviewed fixture files')
        for relative, digest in REVIEWED_SOURCES.items():
            observe(SOURCE / relative, digest)
        source_before = snapshot('source-before')
        record['source_before_sha256'] = source_before['manifest_sha256']
        if source_before['manifest_sha256'] != contract['source_manifest_sha256']:
            raise ValueError('complete Q canonical manifest differs, including documentation')

        commands = [
            ('format-moq-diagnostic', [str(RUST_PREFIX / 'rustfmt'), '--check', '--edition', '2021',
                                       '--config', 'skip_children=true', str(SOURCE / 'rust/crates/pipecat-moq/src/tests.rs')], 30),
            ('format-quality-ledger', [str(RUST_PREFIX / 'rustfmt'), '--check', '--edition', '2021',
                                      '--config', 'skip_children=true', str(SOURCE / 'rust/crates/pipecat-evals/src/simulation/quality.rs')], 30),
            ('strict-clippy', STRICT_ARGV, INNER_SECONDS),
        ]
        record['qualification_argv'] = [{'name': name, 'argv': argv, 'limit_seconds': limit}
                                        for name, argv, limit in commands]
        record['phase'] = 'running'
        save()
        status = 0
        for name, argv, limit in commands:
            budget()
            entry = run_owned(name, argv, limit)
            record['qualification_stages'].append({
                key: entry.get(key) for key in (
                    'name', 'argv', 'phase', 'launch_attempts', 'processes_started',
                    'original_exit_code_observed_before_cleanup', 'original_exit_code',
                    'phase_exit_code_before_containment', 'containment_exit_code',
                    'log_integrity_exit_code', 'wrapper_exit_code', 'timed_out',
                    'execution_elapsed_seconds', 'elapsed_seconds', 'logs')})
            if name == 'strict-clippy':
                record['strict_clippy_original_exit_code'] = entry.get('original_exit_code')
                record['strict_clippy_wrapper_exit_code'] = entry['wrapper_exit_code']
            if entry['wrapper_exit_code'] != 0:
                stopped_phase(entry)
                if status == 0:
                    status = entry['wrapper_exit_code']
            save()
            unsafe = (entry['timed_out'] or entry['containment_exit_code'] or
                      entry['log_integrity_exit_code'] or entry.get('exception') or
                      entry['processes_started'] != 1 or
                      type(entry.get('original_exit_code')) is not int or
                      entry['original_exit_code'] < 0)
            if unsafe:
                status = entry['wrapper_exit_code'] or 127
                record['stop_reason'] = 'Failed phase admission, timeout, termination, containment or integrity; no retry.'
                break
        else:
            record['stop_reason'] = 'All three independent checks ran once; earlier failed checks remain failed.'
    except Failed as error:
        status = error.code
        record['failure'] = str(error)
        (EVIDENCE / 'error.log').write_text(traceback.format_exc())
    except Exception as error:
        record['failure'] = repr(error)
        (EVIDENCE / 'error.log').write_text(traceback.format_exc())
        if status == 0:
            status = 127
    finally:
        record['execution_exit_code_before_integrity_checks'] = status
        if source_before is not None:
            try:
                source_after = snapshot('source-after')
                record['source_after_sha256'] = source_after['manifest_sha256']
                record['source_unchanged'] = source_before == source_after
                if not record['source_unchanged']:
                    record['integrity_exit_code'] = 125
            except Exception as error:
                record['source_unchanged'] = False
                record['source_after_error'] = repr(error)
                record['integrity_exit_code'] = 125
            for key, args, wanted in [
                    ('head_after', ['rev-parse', 'HEAD'], expected),
                    ('tree_after', ['rev-parse', 'HEAD^{tree}'], expected_tree),
                    ('status_after', ['status', '--porcelain=v1'], '')]:
                try:
                    actual = git_read(key, args).decode().strip()
                    record[key] = actual
                    if actual != wanted:
                        record['integrity_exit_code'] = 125
                except Exception as error:
                    record[key + '_error'] = repr(error)
                    record['integrity_exit_code'] = 125
        for path, tracked_path in tracked.items():
            try:
                after = identity(tracked_path)
                before = record['identity_before'][path]
                after['unchanged'] = all(after[k] == before[k] for k in ('resolved', 'bytes', 'sha256'))
                record['identity_after'][path] = after
                if not after['unchanged']:
                    record['integrity_exit_code'] = 125
            except Exception as error:
                record['identity_after'][path] = {'unchanged': False, 'error': repr(error)}
                record['integrity_exit_code'] = 125
        record['all_observed_files_unchanged'] = bool(tracked) and all(
            item.get('unchanged') for item in record['identity_after'].values())
        record['qualification_commands_attempted'] = sum(
            p['name'] in ('format-moq-diagnostic', 'format-quality-ledger', 'strict-clippy')
            and p.get('launch_attempts', 0) for p in record['processes'])
        record['qualification_commands_started'] = sum(
            p['name'] in ('format-moq-diagnostic', 'format-quality-ledger', 'strict-clippy')
            and p.get('processes_started', 0) for p in record['processes'])
        record['strict_clippy_launch_attempts'] = sum(
            p.get('launch_attempts', 0) for p in record['processes'] if p['name'] == 'strict-clippy')
        record['strict_clippy_processes_started'] = sum(
            p.get('processes_started', 0) for p in record['processes'] if p['name'] == 'strict-clippy')
        record['elapsed_seconds'] = time.monotonic() - began
        if record['elapsed_seconds'] > INNER_SECONDS:
            record['whole_budget_exceeded'] = True
            status = 124
        if record['containment_exit_code'] or record['integrity_exit_code']:
            status = 125
        record.update(phase='completed', exit_code=status, end_unix=time.time())
        save()
        after_save = time.monotonic() - began
        if after_save > INNER_SECONDS:
            record.update(elapsed_seconds=after_save, whole_budget_exceeded=True)
            if status != 125:
                status = record['exit_code'] = 124
            save()
        if lock is not None:
            lock.close()
        print(json.dumps({key: record.get(key) for key in [
            'phase', 'exit_code', 'execution_exit_code_before_integrity_checks',
            'containment_exit_code', 'integrity_exit_code', 'qualification_commands_started',
            'strict_clippy_launch_attempts', 'strict_clippy_processes_started',
            'strict_clippy_original_exit_code', 'strict_clippy_wrapper_exit_code',
            'source_unchanged', 'all_observed_files_unchanged', 'stop_reason', 'failure']}),
              flush=True)
    return status


if __name__ == '__main__':
    raise SystemExit(main())
