#!/usr/bin/env python3
"""One C API20 fixed-feature diagnostic. No Cargo, frontend, device or golden writes."""
import fcntl
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import shutil
import signal
import stat
import struct
import subprocess
import sys
import time
import traceback

ROOT = Path('/home/qian-qi/workspace/pipecat-workspace-f9f576cc6c66')
SOURCE = ROOT / 'source-pin'
BUNDLE = ROOT / 'requests/smart-turn-ort-reference-pin-r1'
EVIDENCE = ROOT / 'evidence/smart-turn-ort-reference-pin-r1'
LIBRARY = Path('/home/qian-qi/workspace/hepta-hmi-20261029/assets/onnxruntime-linux-x64-1.21.0/lib/libonnxruntime.so.1.21.0')
MODEL = SOURCE / 'src/pipecat/audio/turn/smart_turn/data/smart-turn-v3.2-cpu.onnx'
HEADER = BUNDLE / 'onnxruntime_c_api.h'
C_SOURCE = BUNDLE / 'ort_reference_probe.c'
COMPILER = ROOT / 'tools/llvm21-ranged-r1/usr/lib/llvm-21/bin/clang'
C_SHA = 'fed472ea3c7b8deb7039327c2a10bb4cbc49f0af6c34c5c7f38445809fd7d547'
HEADER_SHA = '7783e9664bdb0b1b36145b5c8587dd1bc9a399aa000edf2e3dd03003cafc6882'
EXPECTED_FILES = {
    LIBRARY: ('3c8181b339025dd18899916357e74290029722e1d5a5778b4071a733f813420d', None),
    MODEL: ('2bb026316b14a660486a75b1733cd3fbab8c2fd0314dc9af7be49f8cca967e4f', 8679182),
    HEADER: (HEADER_SHA, 219712),
    C_SOURCE: (C_SHA, 11814),
}
CASES = [
    ('speech', '618645fb485f531de515aa83b13627cb3d19a5cb8a0d1faf617c99e76fb8b575',
     '17f53fa2fd6586f8ccca5f8a576e972c8d0631ab0efcf6d84cc5747369940783'),
    ('tone', 'f838341ea22fe1aac684c6eabfc2ccd8f38330b2dbca1220985963d4d90dedc5',
     '3c2b2ba20fc2d02511c3ab4371e12c03d2e1d6ddb06daa6e618b438305d2f4de'),
    ('silence', 'c4a1d90bbbdc1c49cf89b8f73360019b1b0fe01ae48a9dfd6fc2c4a3228b8859',
     '7ec5e2f9ea54c1b8aaf8701afb68b5742a99dce27ac5546adb9e06646cc24a8c'),
]
for case, digest, reference_digest in CASES:
    base = SOURCE / 'rust/crates/pipecat-vad/tests/fixtures' / ('smart_turn_' + case)
    EXPECTED_FILES[base.with_suffix('.f32')] = (digest, 256000)
    EXPECTED_FILES[base.with_suffix('.probability.txt')] = (reference_digest, None)


def identity(path):
    with path.open('rb') as handle:
        digest = hashlib.file_digest(handle, 'sha256').hexdigest()
    info = path.stat()
    return {'path': str(path), 'resolved': str(path.resolve()), 'bytes': info.st_size,
            'sha256': digest}


def main():
    if len(sys.argv) != 4:
        raise ValueError('expected immutable native commit, tree and preceding expanded batch directory name')
    head, tree, preceding_name = sys.argv[1:]
    if not all(re.fullmatch(r'[0-9a-f]{40}', value) for value in (head, tree)):
        raise ValueError('exact commit/tree required')
    if not re.fullmatch(r'[a-z0-9.-]+', preceding_name) or '..' in preceding_name:
        raise ValueError('invalid preceding batch identifier')
    if head != '272671b95297313d24db921eceda8e95f3a8d545' or tree != '94424ffa2a8fbcda0f81321ee68e7461663e191c':
        raise ValueError('this diagnostic was reviewed against only native freeze 272671 / 94424')
    EVIDENCE.mkdir(exist_ok=False)
    record = {
        'schema': 'pipecat-smart-turn-independent-c-api-reference-v1',
        'head': head, 'tree': tree, 'phase': 'preparing', 'start_unix': time.time(),
        'scope': 'Independent fixed-feature C API20 calls only. Original native failures, fixtures, thresholds and production qualifications remain unchanged.',
        'source': str(SOURCE), 'cargo_invocations': 0, 'probe_invocations': 0,
        'declared_case_order': [case for case, _, _ in CASES],
        'declared_native_calls': 3, 'commands': [], 'identity_before': {},
        'requested_outer_resources': {'cpu_quota_percent': 200, 'memory_max_bytes': 4294967296,
                                      'nice': 10, 'timeout_seconds': 120},
        'inner_limits': {'whole_seconds': 110, 'compile_seconds': 30,
                         'probe_seconds': 60, 'cleanup_seconds': 10},
        'scheduling': 'Run only after the preceding expanded batch is terminal, while holding the same exclusive pin owner lock.',
    }
    record_path = EVIDENCE / 'reference.json'
    deadline = time.monotonic() + 110
    helper = before = None
    lock = None
    status = 127
    inputs = list(EXPECTED_FILES)
    observed = {}
    script_path = Path(__file__).resolve()
    helper_path = ROOT / 'run_full_gate.py'

    def save():
        temp = EVIDENCE / 'reference.json.tmp'
        temp.write_text(json.dumps(record, indent=2) + '\n')
        temp.replace(record_path)

    def probe_group(pgid):
        try:
            os.killpg(pgid, 0)
            return {'present': True}
        except ProcessLookupError:
            return {'present': False}
        except OSError as error:
            return {'present': None, 'error': repr(error)}

    def cleanup(child, row):
        end = min(time.monotonic() + 10, deadline)
        row['group_before_cleanup'] = probe_group(child.pid)
        if child.poll() is None or row['group_before_cleanup']['present'] is not False:
            for sig, wait_seconds in ((signal.SIGTERM, 2), (signal.SIGKILL, 2)):
                if sig == signal.SIGKILL and child.poll() is not None and probe_group(child.pid)['present'] is False:
                    break
                request = {'signal': int(sig), 'pgid': child.pid, 'unix': time.time()}
                try:
                    os.killpg(child.pid, sig)
                    request['result'] = 'requested'
                except ProcessLookupError:
                    request['result'] = 'group absent'
                except OSError as error:
                    request['error'] = repr(error)
                row.setdefault('signal_requests', []).append(request)
                try:
                    child.wait(timeout=max(0.01, min(wait_seconds, end - time.monotonic())))
                except subprocess.TimeoutExpired:
                    pass
        try:
            child.wait(timeout=max(0.01, min(1, end - time.monotonic())))
        except subprocess.TimeoutExpired:
            pass
        row['leader_reaped'] = child.poll() is not None
        row['original_exit_code'] = child.returncode
        row['group_after_cleanup'] = probe_group(child.pid)
        row['cleanup_scope'] = 'Owned process-group signal requests, actual leader wait/reap and a non-atomic final group-existence probe; not a claim that every descendant was joined.'

    def run(name, argv, env, seconds):
        left = deadline - time.monotonic() - 10
        if left <= 0:
            raise TimeoutError('whole diagnostic budget expired before ' + name)
        budget = min(seconds, left)
        row = {'name': name, 'argv': argv, 'phase': 'launching',
               'start_unix': time.time(), 'timeout_seconds': budget}
        record['commands'].append(row)
        save()
        child = None
        stdout_path = EVIDENCE / (name + '.stdout.log')
        stderr_path = EVIDENCE / (name + '.stderr.log')
        with stdout_path.open('xb') as stdout, stderr_path.open('xb') as stderr:
            try:
                child = subprocess.Popen(argv, cwd=BUNDLE, env=env, stdout=stdout,
                                         stderr=stderr, start_new_session=True)
                row.update(pid=child.pid, phase='running')
                if name == 'probe':
                    record['probe_invocations'] += 1
                save()
                try:
                    child.wait(timeout=budget)
                except subprocess.TimeoutExpired:
                    row['timed_out'] = True
            except Exception as error:
                row['exception'] = repr(error)
            finally:
                if child is not None:
                    cleanup(child, row)
        row['stdout'] = identity(stdout_path)
        row['stderr'] = identity(stderr_path)
        row['exit_code'] = row.get('original_exit_code')
        if row['exit_code'] is None:
            row['exit_code'] = 127
        if row.get('timed_out'):
            row['exit_code'] = 124
        elif (row.get('exception') or not row.get('leader_reaped') or
              row.get('group_after_cleanup', {}).get('present') is not False):
            row['exit_code'] = 127
        row.update(phase='completed', end_unix=time.time())
        save()
        return row

    save()
    try:
        lock = (ROOT / 'qualification-owner-pin.lock').open('a')
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        record['lock_acquired'] = True
        preceding_path = ROOT / 'evidence' / preceding_name / 'batch.json'
        preceding_bytes = preceding_path.read_bytes()
        preceding = json.loads(preceding_bytes)
        if (preceding.get('mode') != 'expanded' or preceding.get('head') != head or
                preceding.get('tree') != tree or preceding.get('phase') not in
                ('completed_successfully', 'completed_with_failures') or
                type(preceding.get('exit_code')) is not int):
            raise ValueError('preceding expanded batch is not terminal on this exact native source')
        if preceding.get('integrity_failure') or preceding['exit_code'] == 125 or not preceding.get('driver', {}).get('unchanged'):
            raise ValueError('preceding expanded batch has unresolved integrity failure')
        record['preceding_expanded'] = {'path': str(preceding_path),
                                       'sha256': hashlib.sha256(preceding_bytes).hexdigest(),
                                       'exit_code': preceding['exit_code'], 'phase': preceding['phase']}
        (EVIDENCE / 'preceding-expanded-batch.json').write_bytes(preceding_bytes)
        default_name = preceding['preceding_default']['evidence']
        if not re.fullmatch(r'[a-z0-9.-]+', default_name) or '..' in default_name:
            raise ValueError('invalid preceding default evidence path')
        environment_path = ROOT / 'evidence' / default_name / 'environment.json'
        environment_raw = environment_path.read_bytes()
        environment = json.loads(environment_raw)
        if not isinstance(environment, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in environment.items()):
            raise ValueError('unexpected native environment receipt')
        (EVIDENCE / 'native-environment.json').write_bytes(environment_raw)
        record['native_environment_sha256'] = hashlib.sha256(environment_raw).hexdigest()
        env = dict(os.environ, **environment)
        record['environment_note'] = 'Existing native environment retained. No CPU/ISA, model, inference or optimizer flags are added or modified.'
        for path in (script_path, helper_path, COMPILER):
            observed[str(path)] = identity(path)
        (EVIDENCE / 'executed-runner.py').write_bytes(script_path.read_bytes())
        (EVIDENCE / 'source-manifest-helper.py').write_bytes(helper_path.read_bytes())
        for path, (digest, size) in EXPECTED_FILES.items():
            if path.is_symlink() or not stat.S_ISREG(path.stat().st_mode):
                raise ValueError('fixed input must be a regular non-symlink file: ' + str(path))
            item = identity(path)
            if item['sha256'] != digest or (size is not None and item['bytes'] != size):
                raise ValueError('fixed input identity mismatch: ' + str(path))
            observed[str(path)] = item
        record['identity_before'] = observed
        for path in (HEADER, C_SOURCE):
            shutil.copyfile(path, EVIDENCE / path.name)
        spec = importlib.util.spec_from_file_location('source_manifest_helper', helper_path)
        helper = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(helper)
        if (helper.git_value(SOURCE, ['rev-parse', 'HEAD']) != head or
                helper.git_value(SOURCE, ['rev-parse', 'HEAD^{tree}']) != tree or
                helper.git_value(SOURCE, ['status', '--porcelain=v1'])):
            raise ValueError('native source is not the exact clean freeze')
        before = helper.source_manifest(SOURCE)
        (EVIDENCE / 'source-before.json').write_text(json.dumps(before, indent=2) + '\n')
        record['source_before_sha256'] = before['manifest_sha256']
        record['phase'] = 'compiling'
        save()
        version = run('compiler-version', [str(COMPILER), '--version'], env, 5)
        if version['exit_code']:
            raise RuntimeError('existing compiler version observation failed')
        executable = BUNDLE / 'ort_reference_probe'
        if executable.exists():
            raise ValueError('diagnostic executable path already exists; no replacement/re-run')
        compile_argv = [str(COMPILER), '-std=c11', '-O0', '-Wall', '-Wextra', '-Werror',
                        '-ferror-limit=10', '-I', str(BUNDLE), str(C_SOURCE), '-ldl', '-lm',
                        '-o', str(executable)]
        compiled = run('compile', compile_argv, env, 30)
        if compiled['exit_code']:
            status = compiled['exit_code']
            raise RuntimeError('standalone diagnostic compilation failed')
        observed[str(executable)] = identity(executable)
        record['executable_before'] = observed[str(executable)]
        feature_paths = [SOURCE / 'rust/crates/pipecat-vad/tests/fixtures' / ('smart_turn_' + name + '.f32')
                         for name, _, _ in CASES]
        record['phase'] = 'inference'
        save()
        native = run('probe', [str(executable), str(LIBRARY), str(MODEL)] +
                     [str(path) for path in feature_paths], env, 60)
        record['native_exit_code'] = native.get('original_exit_code')
        records = []
        invalid_lines = []
        raw = Path(native['stdout']['path']).read_bytes()
        for index, line in enumerate(raw.splitlines(), 1):
            try:
                item = json.loads(line)
                if not isinstance(item, dict):
                    raise ValueError('expected object')
                records.append(item)
            except (ValueError, UnicodeDecodeError) as error:
                invalid_lines.append({'line': index, 'error': repr(error)})
        record['native_records'] = records
        record['invalid_stdout_lines'] = invalid_lines
        if native['exit_code']:
            status = native['exit_code']
            raise RuntimeError('native diagnostic failed or did not fully terminate')
        if invalid_lines:
            raise ValueError('native stdout is not complete JSON lines')
        runtime_records = [r for r in records if r.get('kind') == 'runtime']
        session_records = [r for r in records if r.get('kind') == 'session']
        input_records = [r for r in records if r.get('kind') == 'model_input']
        output_records = [r for r in records if r.get('kind') == 'model_output']
        predictions = [r for r in records if r.get('kind') == 'prediction']
        cleanup_records = [r for r in records if r.get('kind') == 'cleanup']
        if (len(records) != 8 or len(runtime_records) != 1 or len(session_records) != 1 or
                len(input_records) != 1 or len(output_records) != 1 or len(cleanup_records) != 1 or
                [r.get('case') for r in predictions] != [case for case, _, _ in CASES]):
            raise ValueError('native output is not the exact one-session three-case diagnostic')
        runtime = runtime_records[0]
        if (runtime.get('version') != '1.21.0' or runtime.get('api_requested') != 20 or
                runtime.get('header_api') != 21 or
                Path(runtime['loaded_filename']).resolve() != LIBRARY.resolve()):
            raise ValueError('actual loaded runtime identity differs')
        if session_records[0] != {'kind': 'session', 'execution': 'ORT_SEQUENTIAL',
                'inter_threads': 1, 'intra_threads': 1, 'optimization': 'ORT_ENABLE_ALL',
                'memory': 'Cpu/Arena/CPUInput', 'telemetry': False, 'explicit_providers': []}:
            raise ValueError('session options differ from the reviewed contract')
        if input_records[0].get('name') != 'input_features' or input_records[0].get('dtype') != 1 or output_records[0].get('name') != 'logits' or output_records[0].get('dtype') != 1:
            raise ValueError('model metadata differs')
        if cleanup_records != [{'kind': 'cleanup', 'release_calls_returned': True,
                                'completed_cases': 3, 'exit_code': 0}]:
            raise ValueError('ordinary native release did not complete')
        comparisons = []
        for result, (name, _, _), feature_path in zip(predictions, CASES, feature_paths):
            bits = result['f32_bits']
            if not re.fullmatch(r'[0-9a-f]{8}', bits):
                raise ValueError('invalid exact float32 bit pattern')
            actual = struct.unpack('<f', struct.pack('<I', int(bits, 16)))[0]
            if (actual != result['value'] or not math.isfinite(actual) or not 0 <= actual <= 1 or
                    result.get('input_file') != str(feature_path) or result.get('input_bytes') != 256000 or
                    result.get('input_shape') != [1, 80, 800] or result.get('dtype') != 1 or
                    not isinstance(result.get('shape'), list) or math.prod(result['shape']) != 1):
                raise ValueError('actual output/input metadata is inconsistent')
            reference_path = feature_path.with_suffix('.probability.txt')
            reference_text = reference_path.read_text().strip()
            expected = struct.unpack('<f', struct.pack('<f', float(reference_text)))[0]
            delta = actual - expected
            comparisons.append({'case': name, 'actual': actual, 'f32_bits': bits,
                                'unchanged_reference_text': reference_text,
                                'expected_f32': expected, 'signed_delta': delta,
                                'absolute_delta': abs(delta), 'original_direct_tolerance': 1e-5,
                                'within_original_direct_tolerance': abs(delta) <= 1e-5})
        record['comparisons'] = comparisons
        record['execution_and_output_checks_succeeded'] = True
        record['all_reference_probabilities_within_original_tolerance'] = all(
            r['within_original_direct_tolerance'] for r in comparisons)
        record['production_qualification'] = False
        record['native_failures_cleared'] = False
        status = 0 if record['all_reference_probabilities_within_original_tolerance'] else 1
    except Exception as error:
        record['exception'] = repr(error)
        (EVIDENCE / 'runner-error.log').write_text(traceback.format_exc())
        if status == 0:
            status = 127
    finally:
        record['execution_exit_code_before_integrity_checks'] = status
        after_map = {}
        for name, initial in observed.items():
            try:
                after = identity(Path(name))
                after['unchanged'] = all(after[k] == initial[k] for k in ('resolved', 'bytes', 'sha256'))
                after_map[name] = after
                if not after['unchanged']:
                    status = 125
            except Exception as error:
                after_map[name] = {'error': repr(error), 'unchanged': False}
                status = 125
        record['identity_after'] = after_map
        record['all_observed_files_unchanged'] = all(r.get('unchanged') for r in after_map.values())
        if before is not None:
            try:
                after = helper.source_manifest(SOURCE)
                (EVIDENCE / 'source-after.json').write_text(json.dumps(after, indent=2) + '\n')
                record['source_after_sha256'] = after['manifest_sha256']
                record['source_unchanged'] = before == after
                record['status_after'] = helper.git_value(SOURCE, ['status', '--porcelain=v1'])
                record['head_after'] = helper.git_value(SOURCE, ['rev-parse', 'HEAD'])
                record['tree_after'] = helper.git_value(SOURCE, ['rev-parse', 'HEAD^{tree}'])
                if (not record['source_unchanged'] or record['status_after'] or
                        record['head_after'] != head or record['tree_after'] != tree):
                    status = 125
            except Exception as error:
                record['source_after_error'] = repr(error)
                status = 125
        record.update(phase='completed', exit_code=status, end_unix=time.time())
        save()
        files = sorted(p for p in EVIDENCE.iterdir() if p.is_file() and p.name != 'SHA256SUMS')
        (EVIDENCE / 'SHA256SUMS').write_text(''.join(identity(p)['sha256'] + '  ' + p.name + '\n' for p in files))
        if lock is not None:
            lock.close()
        print(json.dumps({key: record.get(key) for key in (
            'phase', 'exit_code', 'native_exit_code', 'probe_invocations', 'source_unchanged',
            'all_observed_files_unchanged', 'execution_and_output_checks_succeeded',
            'all_reference_probabilities_within_original_tolerance', 'comparisons', 'exception')}), flush=True)
    return status


if __name__ == '__main__':
    raise SystemExit(main())
