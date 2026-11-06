#!/usr/bin/env python3
"""One controlled CLOEXEC inheritance observation; no Grok or Cargo execution."""
import errno
import fcntl
import hashlib
import json
import os
import pathlib
import select
import signal
import socket
import sys
import time
import traceback


def digest(path):
    with path.open('rb') as handle:
        return hashlib.file_digest(handle, 'sha256').hexdigest()


def emit(fd, data):
    encoded = (json.dumps(data, sort_keys=True) + '\n').encode()
    if len(encoded) > 4096 or os.write(fd, encoded) != len(encoded):
        raise RuntimeError('bounded observation packet was not written atomically')


def receive(fd, seconds=3):
    deadline = time.monotonic() + seconds
    data = bytearray()
    while b'\n' not in data:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not select.select([fd], [], [], remaining)[0]:
            raise TimeoutError('bounded child observation deadline')
        chunk = os.read(fd, 4096 - len(data))
        if not chunk:
            raise RuntimeError('child observation pipe reached EOF')
        data.extend(chunk)
        if len(data) >= 4096 and b'\n' not in data:
            raise RuntimeError('child observation exceeds byte cap')
    return json.loads(data)


def control(fd, expected):
    if not select.select([fd], [], [], 5)[0] or os.read(fd, 1) != expected:
        raise RuntimeError('bounded parent control did not arrive')


def child_fd_state(fd):
    result = {'fd': fd, 'pid': os.getpid()}
    try:
        info = os.fstat(fd)
        result.update(inode=info.st_ino, device=info.st_dev, mode=info.st_mode,
                      fd_flags=fcntl.fcntl(fd, fcntl.F_GETFD),
                      link=os.readlink(f'/proc/self/fd/{fd}'))
    except OSError as error:
        result.update(errno=error.errno, error=str(error))
    return result


def new_listener():
    result = socket.socket(socket.AF_INET, socket.SOCK_STREAM | socket.SOCK_CLOEXEC | socket.SOCK_NONBLOCK)
    result.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    return result


def main():
    if len(sys.argv) > 1 and sys.argv[1] == '--after-exec':
        original_fd, ready, commands = map(int, sys.argv[2:])
        emit(ready, {'phase': 'post_exec_alive', 'original_fd_state': child_fd_state(original_fd)})
        control(commands, b'Q')
        os._exit(0)

    evidence = pathlib.Path(sys.argv[1]).resolve()
    evidence.mkdir(exist_ok=False)
    script = pathlib.Path(__file__).resolve()
    report = {
        'schema': 'controlled-cloexec-inheritance-v1',
        'scope': 'Single standalone local mechanism experiment. Not a Grok test, Tokio/Rust qualification, historical failure reproduction, or proof about the traced child696897.',
        'argv': sys.argv, 'python': sys.executable, 'python_version': sys.version,
        'python_sha256': digest(pathlib.Path(sys.executable).resolve()),
        'script_sha256': digest(script), 'kernel': os.uname().release,
        'uid': os.getuid(), 'parent_pid': os.getpid(), 'start_unix': time.time(),
        'phase': 'preparing', 'events': [], 'signal_requests': [],
        'child_reaped': False, 'exit_code': 1,
    }
    listener = None
    pipes = []
    child = None

    def save():
        (evidence / 'experiment.json').write_text(json.dumps(report, indent=2) + '\n')

    def event(kind, data):
        report['events'].append({'kind': kind, 'unix': time.time(), 'data': data})
        save()

    def attempt_bind(stage, address):
        probe = new_listener()
        try:
            probe.bind(address)
            probe.listen(128)
            result = {'success': True, 'endpoint': probe.getsockname(),
                      'inode': os.fstat(probe.fileno()).st_ino}
        except OSError as error:
            result = {'success': False, 'errno': error.errno, 'error': str(error)}
        finally:
            probe.close()
        event(stage, result)
        return result

    save()
    try:
        if len(list(pathlib.Path('/proc/self/task').iterdir())) != 1:
            raise RuntimeError('controlled fork requires this Python observer to be single-threaded')
        listener = new_listener()
        listener.bind(('127.0.0.1', 0))
        listener.listen(128)
        address, original_fd = listener.getsockname(), listener.fileno()
        original = child_fd_state(original_fd)
        if original.get('fd_flags', 0) & fcntl.FD_CLOEXEC == 0:
            raise RuntimeError('fixture listener is not CLOEXEC')
        event('parent_original_listener', {'endpoint': address, **original})
        ready_r, ready_w = os.pipe()
        command_r, command_w = os.pipe()
        pipes = [ready_r, ready_w, command_r, command_w]
        # Only these two explicit experiment-control pipe ends survive exec.
        os.set_inheritable(ready_w, True)
        os.set_inheritable(command_r, True)
        child = os.fork()
        if child == 0:
            try:
                os.close(ready_r)
                os.close(command_w)
                emit(ready_w, {'phase': 'pre_exec_held', 'original_fd_state': child_fd_state(original_fd)})
                control(command_r, b'E')
                os.execve(sys.executable,
                          [sys.executable, str(script), '--after-exec', str(original_fd), str(ready_w), str(command_r)],
                          {})
            except BaseException:
                os.write(2, traceback.format_exc().encode())
                os._exit(111)
        report['child_pid'] = child
        os.close(ready_w)
        os.close(command_r)
        pipes = [ready_r, command_w]
        pre_exec = receive(ready_r)
        event('child_direct_pre_exec_fstat', pre_exec)
        state = pre_exec['original_fd_state']
        if state.get('inode') != original['inode'] or state.get('link') != original['link'] or state.get('fd_flags', 0) & fcntl.FD_CLOEXEC == 0:
            raise AssertionError('owned child did not directly confirm the same CLOEXEC socket inode')
        listener.close()
        listener = None
        child_path = pathlib.Path(f'/proc/{child}/fd/{original_fd}')
        child_info = child_path.stat()
        witness = {'owned_child_pid': child, 'fd': original_fd,
                   'inode': child_info.st_ino, 'device': child_info.st_dev,
                   'link': os.readlink(child_path)}
        event('parent_direct_child_fd_after_parent_close', witness)
        if witness['inode'] != original['inode'] or witness['link'] != original['link']:
            raise AssertionError('positive child inode witness disappeared before controlled bind')
        held = attempt_bind('single_bind_while_child_holds_original_inode', address)
        if held.get('success') or held.get('errno') != errno.EADDRINUSE:
            raise AssertionError('expected occupied bind while directly witnessed child holds the socket')
        os.write(command_w, b'E')
        post_exec = receive(ready_r)
        event('same_child_post_exec_user_code_ack', post_exec)
        if post_exec['original_fd_state'].get('pid') != child:
            raise AssertionError('post-exec observer is not the same child PID')
        if post_exec['original_fd_state'].get('link') == original['link']:
            raise AssertionError('original socket survived exec despite CLOEXEC')
        waited, raw_status = os.waitpid(child, os.WNOHANG)
        if waited:
            report.update(child_reaped=True, child_wait_status=raw_status)
            raise AssertionError('post-exec success control requires the child still alive')
        event('child_still_alive_before_post_exec_bind', {'pid': child, 'waitpid_WNOHANG': waited})
        released = attempt_bind('single_bind_after_exec_while_same_child_is_alive', address)
        if not released['success']:
            raise AssertionError('post-exec bind did not succeed')
        os.write(command_w, b'Q')
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            waited, raw_status = os.waitpid(child, os.WNOHANG)
            if waited:
                report.update(child_reaped=True, child_wait_status=raw_status)
                if not os.WIFEXITED(raw_status) or os.WEXITSTATUS(raw_status) != 0:
                    raise AssertionError('controlled child did not exit cleanly')
                break
            time.sleep(0.01)
        if not report['child_reaped']:
            raise TimeoutError('controlled child reap deadline')
        report['exit_code'] = 0
    except BaseException as error:
        report['error'] = repr(error)
        (evidence / 'error.log').write_text(traceback.format_exc())
        traceback.print_exc()
    finally:
        if listener is not None:
            listener.close()
        for fd in pipes:
            try:
                os.close(fd)
            except OSError:
                pass
        if child is not None and child > 0 and not report['child_reaped']:
            try:
                os.kill(child, signal.SIGKILL)
                report['signal_requests'].append({'pid': child, 'signal': int(signal.SIGKILL), 'result': 'requested'})
            except ProcessLookupError:
                report['signal_requests'].append({'pid': child, 'signal': int(signal.SIGKILL), 'result': 'absent'})
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                try:
                    waited, raw_status = os.waitpid(child, os.WNOHANG)
                except ChildProcessError:
                    report['child_wait_error'] = 'ECHILD; not recorded as a successful reap by this observer'
                    break
                if waited:
                    report.update(child_reaped=True, child_wait_status=raw_status)
                    break
                time.sleep(0.01)
        report.update(phase='completed', end_unix=time.time(), script_sha256_after=digest(script))
        if not report['child_reaped'] or report['script_sha256_after'] != report['script_sha256']:
            report['exit_code'] = 1
        save()
        print(json.dumps(report, indent=2), flush=True)
    return report['exit_code']


if __name__ == '__main__':
    raise SystemExit(main())
