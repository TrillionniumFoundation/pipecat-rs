#!/usr/bin/env python3
"""Isolated Cargo 1.99.0 example-artifact behavior probe; no production tests."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / 'cargo-probe-source'
TOOLCHAIN = Path('/workspace/scratch/27653505a519/toolchain/rustup/toolchains/1.99.0-x86_64-unknown-linux-gnu/bin')
TARGET = Path('/workspace/scratch/f9f576cc6c66/cargo-example-artifact-probe-target')
ENV = os.environ.copy()
ENV.update({
    'PATH': str(TOOLCHAIN) + ':' + ENV.get('PATH', ''),
    'CARGO_HOME': '/workspace/scratch/27653505a519/toolchain/cargo',
    'CARGO_NET_OFFLINE': 'true',
    'CARGO_INCREMENTAL': '0',
    'CARGO_PROFILE_DEV_DEBUG': '0',
    'CARGO_PROFILE_TEST_DEBUG': '0',
    'CARGO_BUILD_JOBS': '1',
})


def snapshot(target):
    example = target / 'debug/examples/provider_probe'
    state = {'path': str(example), 'exists': example.is_file()}
    if example.is_file():
        state['sha256'] = hashlib.sha256(example.read_bytes()).hexdigest()
        process = subprocess.run([str(example), '--help'], capture_output=True, timeout=10)
        state.update(exit_code=process.returncode, stdout=process.stdout.decode(), stderr=process.stderr.decode())
    return state


def main():
    SOURCE.mkdir(exist_ok=False)
    (SOURCE / 'src').mkdir()
    (SOURCE / 'examples').mkdir()
    (SOURCE / 'Cargo.toml').write_text('''[package]
name = "cargo-example-artifact-probe"
version = "0.0.0"
edition = "2021"
autoexamples = false

[workspace]

[features]
variant = []

[[example]]
name = "provider_probe"
path = "examples/pcm_session.rs"
''')
    (SOURCE / 'src/lib.rs').write_text('pub fn probe_library() -> bool { cfg!(feature = "variant") }\n')
    (SOURCE / 'examples/pcm_session.rs').write_text('''fn main() {
    println!("PROBE_REAL_EXAMPLE feature={}", if cfg!(feature = "variant") { "variant" } else { "default" });
}
''')
    lock_argv = [str(TOOLCHAIN / 'cargo'), 'generate-lockfile', '--offline']
    lock = subprocess.run(lock_argv, cwd=SOURCE, env=ENV, capture_output=True, timeout=30)
    assert lock.returncode == 0, lock.stderr
    (ROOT / 'cargo-probe-lock.log').write_bytes(lock.stdout + lock.stderr)
    results = []
    for label, target, extra in [
        ('default-sequential', TARGET / 'sequential', []),
        ('all-targets-after-default', TARGET / 'sequential', ['--all-features', '--all-targets']),
        ('all-targets-fresh', TARGET / 'fresh', ['--all-features', '--all-targets']),
    ]:
        env = {**ENV, 'CARGO_TARGET_DIR': str(target)}
        before = snapshot(target)
        argv = [str(TOOLCHAIN / 'cargo'), 'test', '--workspace', '--locked', '--offline',
                '--no-run', '--message-format=json', *extra]
        start = time.monotonic()
        process = subprocess.run(argv, cwd=SOURCE, env=env, capture_output=True, timeout=45)
        (ROOT / (label + '.stdout.jsonl')).write_bytes(process.stdout)
        (ROOT / (label + '.stderr.log')).write_bytes(process.stderr)
        messages = [json.loads(line) for line in process.stdout.splitlines() if line.strip()]
        artifacts = [{
            'target': message['target'], 'profile': message['profile'],
            'filenames': message['filenames'], 'executable': message.get('executable'),
            'features': message['features'], 'fresh': message['fresh'],
        } for message in messages if message.get('reason') == 'compiler-artifact']
        results.append({
            'label': label, 'argv': argv, 'cwd': str(SOURCE), 'target_dir': str(target),
            'exit_code': process.returncode, 'elapsed_seconds': round(time.monotonic() - start, 6),
            'normal_example_before': before, 'normal_example_after': snapshot(target),
            'artifacts': artifacts,
            'stdout_sha256': hashlib.sha256(process.stdout).hexdigest(),
            'stderr_sha256': hashlib.sha256(process.stderr).hexdigest(),
        })
        assert process.returncode == 0, process.stderr
    result = {
        'scope': 'Isolated no-dependency Cargo artifact probe only; not a production acceptance run.',
        'toolchain': subprocess.check_output([str(TOOLCHAIN / 'cargo'), '--version']).decode().strip(),
        'source_sha256': {str(p.relative_to(SOURCE)): hashlib.sha256(p.read_bytes()).hexdigest()
                          for p in sorted(SOURCE.rglob('*')) if p.is_file()},
        'lock_argv': lock_argv, 'runs': results,
    }
    (ROOT / 'cargo-artifact-behavior.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps([{
        'label': r['label'], 'exit_code': r['exit_code'], 'elapsed_seconds': r['elapsed_seconds'],
        'normal_example_before': r['normal_example_before'],
        'normal_example_after': r['normal_example_after'],
        'example_artifacts': [{'test_harness': a['profile']['test'], 'executable': a['executable'],
                              'features': a['features']} for a in r['artifacts'] if 'example' in a['target']['kind']],
    } for r in results], indent=2))


if __name__ == '__main__':
    main()
