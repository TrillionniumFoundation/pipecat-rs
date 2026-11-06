#!/usr/bin/env python3
"""Run selected acceptance gates against one frozen local source tree."""
from pathlib import Path
import ast
import hashlib
import json
import os
import re
import subprocess
import time

ROOT = Path('/workspace/scratch/b2b6f3e38b6d/pipecat-rs')
OUT = ROOT.parent / 'verification'
BIN = Path('/workspace/scratch/27653505a519/toolchain/rustup/toolchains/1.99.0-x86_64-unknown-linux-gnu/bin')
TARGET = Path('/tmp/pipecat-hmi-native-27653505a519')
ENV = os.environ.copy()
BUILD_ENV = {
    'CARGO_HOME': '/workspace/scratch/27653505a519/toolchain/cargo',
    'CARGO_TARGET_DIR': str(TARGET),
    'CARGO_BUILD_JOBS': '2',
    'CARGO_PROFILE_TEST_DEBUG': '0',
    'CARGO_PROFILE_DEV_DEBUG': '0',
    'RUSTC': str(BIN / 'rustc'),
    'RUSTDOC': str(BIN / 'rustdoc'),
    'CMAKE': '/workspace/scratch/848006513f75/p0-build-tools/cmake/data/bin/cmake',
    'LIBCLANG_PATH': '/usr/lib/x86_64-linux-gnu',
    'PKG_CONFIG_PATH': '/workspace/scratch/27653505a519/native-sysroot/usr/lib/x86_64-linux-gnu/pkgconfig',
    'PKG_CONFIG_SYSROOT_DIR': '/workspace/scratch/27653505a519/native-sysroot',
}
ENV.update(BUILD_ENV)
ENV['PATH'] = str(BIN) + os.pathsep + ENV['PATH']


def git(*args):
    return subprocess.check_output(['git', *args], cwd=ROOT, text=True).strip()


def snapshot():
    return {'commit': git('rev-parse', 'HEAD'), 'tree': git('rev-parse', 'HEAD^{tree}'),
            'worktree_status': git('status', '--porcelain')}


report = {'scope': 'selected_packages_not_full_workspace_or_physical_acceptance',
          'source_before': snapshot(), 'build_environment': BUILD_ENV,
          'toolchain': subprocess.check_output([str(BIN / 'rustc'), '--version'], text=True).strip(),
          'gates': []}
if report['source_before']['worktree_status']:
    raise SystemExit('source must be clean before final gates')


def save():
    (OUT / 'final-gates.json').write_text(json.dumps(report, indent=2) + '\n')


def run(name, command):
    print(f'START {name}', flush=True)
    started = time.monotonic()
    log = OUT / f'final-{name}.log'
    with log.open('w') as stream:
        done = subprocess.run(command, cwd=ROOT, env=ENV, stdout=stream, stderr=subprocess.STDOUT)
    content = log.read_text(errors='replace')
    counts = [tuple(map(int, match)) for match in re.findall(
        r'test result: \w+\. (\d+) passed; (\d+) failed; (\d+) ignored;', content)]
    entry = {'name': name, 'argv': command, 'exit_code': done.returncode,
             'elapsed_seconds': round(time.monotonic() - started, 3),
             'log': log.name, 'log_sha256': hashlib.sha256(log.read_bytes()).hexdigest()}
    if counts:
        entry['test_counts'] = dict(zip(('passed', 'failed', 'ignored'), map(sum, zip(*counts))))
    report['gates'].append(entry)
    save()
    print(json.dumps(entry), flush=True)
    if done.returncode:
        report['passed'] = False
        report['source_after'] = snapshot()
        save()
        raise SystemExit(done.returncode)


changed = git('diff', '--name-only', 'HEAD^', 'HEAD').splitlines()
rust_files = [p for p in changed if p.endswith('.rs')]
initial_format = OUT / 'final-format.log'
if initial_format.exists() and not (OUT / 'format-before-freeze.log').exists():
    initial_format.rename(OUT / 'format-before-freeze.log')
run('format', [str(BIN / 'rustfmt'), '--check', '--edition', '2021', '--config', 'skip_children=true', *rust_files])
run('diff-check', ['git', 'diff', 'HEAD^', 'HEAD', '--check'])
for p in changed:
    if p.endswith('.py'):
        ast.parse((ROOT / p).read_text(), filename=p)
report['python_ast'] = {'passed': True, 'files': [p for p in changed if p.endswith('.py')]}
report['source_files_sha256'] = {p: hashlib.sha256((ROOT / p).read_bytes()).hexdigest() for p in changed}
report['root_readme_bytes'] = (ROOT / 'README.md').stat().st_size
cargo = [str(BIN / 'cargo')]
shared = ['--locked', '--offline', '--manifest-path', 'rust/Cargo.toml']
run('core-pipeline', cargo + ['test', *shared, '-p', 'pipecat-core', '-p', 'pipecat-pipeline'])
run('hepta-native', cargo + ['test', *shared, '-p', 'pipecat-hepta', '--features', 'local-speech', '--all-targets'])
run('webrtc', cargo + ['test', *shared, '-p', 'pipecat-webrtc', '--all-targets'])
run('clippy', cargo + ['clippy', *shared, '-p', 'pipecat-core', '-p', 'pipecat-pipeline', '-p', 'pipecat-hepta', '-p', 'pipecat-webrtc', '--features', 'pipecat-hepta/local-speech', '--all-targets', '--', '-D', 'warnings'])
run('native-build', cargo + ['build', *shared, '-p', 'pipecat-hepta', '--features', 'local-speech', '--example', 'local_speech'])
binary = TARGET / 'debug/examples/local_speech'
report['binary_sha256'] = hashlib.sha256(binary.read_bytes()).hexdigest()
run('actual-entry', ['python', 'rust/scripts/qualify_hmi_supervisor_entry.py', '--binary', str(binary), '--output', str(OUT / 'final-actual-entry.json')])
report['source_after'] = snapshot()
report['passed'] = report['source_before'] == report['source_after'] and report['root_readme_bytes'] == 0
report['not_qualified'] = ['full workspace', 'physical models/audio/video/devices', 'production Hepta owner composition', 'non-Linux containment', 'remote provider/SFU/browser qualification', 'unexplained WebRTC data-channel setup timeout']
save()
print(json.dumps({'passed': report['passed'], 'source': report['source_after'], 'binary_sha256': report['binary_sha256']}), flush=True)
raise SystemExit(0 if report['passed'] else 1)
