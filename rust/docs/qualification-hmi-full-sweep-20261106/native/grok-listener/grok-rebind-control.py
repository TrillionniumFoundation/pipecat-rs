#!/usr/bin/env python3
"""Controlled loopback oracle-ambiguity witness, not a reproduction of Grok r3."""
import argparse
import errno
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import sys
import time
import traceback


def owned_fds(inode):
    result = []
    for path in Path('/proc/self/fd').iterdir():
        try:
            if os.readlink(path) == f'socket:[{inode}]':
                result.append(int(path.name))
        except FileNotFoundError:
            pass
    return sorted(result)


def identity(sock):
    fd = sock.fileno()
    inode = os.fstat(fd).st_ino
    assert os.readlink(f'/proc/self/fd/{fd}') == f'socket:[{inode}]'
    return {'fd': fd, 'inode': inode, 'address': list(sock.getsockname()),
            'reuseaddr': sock.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR),
            'reuseport': sock.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT),
            'inheritable': sock.get_inheritable()}


def endpoint_rows(address):
    exact = socket.inet_aton(address[0])[::-1].hex().upper() + ':' + f'{address[1]:04X}'
    with open('/proc/self/net/tcp', 'rb') as source:
        raw = source.read(2 * 1024 * 1024 + 1)
    assert len(raw) <= 2 * 1024 * 1024, 'proc TCP table exceeds control bound'
    result = []
    for line in raw.decode('ascii').splitlines()[1:]:
        parts = line.split()
        if len(parts) >= 10 and parts[1] == exact:
            result.append({'local_hex': parts[1], 'remote_hex': parts[2],
                           'state_hex': parts[3], 'inode': int(parts[9]), 'row': line.strip()})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    started = time.monotonic()
    sockets = []
    identities = []
    report = {'schema_version': 1, 'result': 'failed', 'pid': os.getpid(),
              'scope': 'controlled same-endpoint competing-listener witness only; not historical r3 reproduction',
              'source_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              'platform': sys.platform, 'observations': [], 'failure': None,
              'cleanup_errors': [], 'original_test_changed': False,
              'retry_count': 0, 'connections_or_payloads_sent': 0,
              'socket_policy': {'family': 'AF_INET', 'type': 'SOCK_STREAM',
                                'SO_REUSEADDR': 1, 'SO_REUSEPORT': 0,
                                'reference': 'https://docs.rs/tokio/1.53.2/src/tokio/net/tcp/listener.rs.html',
                                'limit': 'Matches the relevant Unix reuse setting; this is not executing the Rust fixture or claiming every Tokio option is identical.'},
              'snapshot_limit': 'proc/self/fd and exact IPv4 endpoint rows are bounded observations, not atomic system-wide snapshots.'}

    def expire(_signum, _frame):
        raise TimeoutError('control exceeded its 10-second deadline')

    def new_socket():
        value = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sockets.append(value)
        value.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        value.settimeout(1)
        assert value.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT) == 0
        return value

    signal.signal(signal.SIGALRM, expire)
    signal.alarm(10)
    try:
        assert sys.platform == 'linux', 'control requires Linux proc/socket semantics'
        original = new_socket()
        original.bind(('127.0.0.1', 0))
        original.listen(16)
        address = original.getsockname()
        first = identity(original)
        identities.append(first)
        rows = endpoint_rows(address)
        assert any(row['inode'] == first['inode'] and row['state_hex'] == '0A' for row in rows)
        report['observations'].append({'stage': 'original-listening', 'socket': first, 'rows': rows})

        original.close()
        holders = owned_fds(first['inode'])
        assert original.fileno() == -1 and holders == []
        report['observations'].append({'stage': 'original-dropped', 'original_inode_fds': holders,
                                       'rows': endpoint_rows(address)})

        # Deliberately create the interloper in the gap in the original oracle.
        # This controlled claim does not identify what occupied the real r3 port.
        competitor = new_socket()
        competitor.bind(address)
        competitor.listen(16)
        second = identity(competitor)
        identities.append(second)
        assert second['inode'] != first['inode']
        assert owned_fds(first['inode']) == []
        rows = endpoint_rows(address)
        assert any(row['inode'] == second['inode'] and row['state_hex'] == '0A' for row in rows)
        report['observations'].append({'stage': 'competitor-listening', 'socket': second,
                                       'original_inode_fds': owned_fds(first['inode']), 'rows': rows})

        attempt = new_socket()
        try:
            attempt.bind(address)
            attempt.listen(16)
        except OSError as error:
            report['observations'].append({'stage': 'single-rebind-with-competitor',
                                           'errno': error.errno, 'error_name': errno.errorcode.get(error.errno),
                                           'original_inode_fds': owned_fds(first['inode']),
                                           'rows': endpoint_rows(address)})
            assert error.errno == errno.EADDRINUSE, 'unexpected failure instead of EADDRINUSE'
        else:
            raise AssertionError('rebind unexpectedly succeeded while competitor was listening')
        finally:
            attempt.close()

        competitor.close()
        assert owned_fds(second['inode']) == []
        rebound = new_socket()
        rebound.bind(address)
        rebound.listen(16)
        third = identity(rebound)
        identities.append(third)
        report['observations'].append({'stage': 'single-rebind-after-competitor-close', 'socket': third,
                                       'original_inode_fds': owned_fds(first['inode']),
                                       'competitor_inode_fds': owned_fds(second['inode']),
                                       'rows': endpoint_rows(address)})
        report['result'] = 'passed'
    except BaseException as error:
        report['failure'] = {'type': type(error).__name__, 'message': str(error),
                             'traceback': traceback.format_exc()}
    finally:
        for value in sockets:
            try:
                value.close()
            except BaseException as error:
                report['cleanup_errors'].append(type(error).__name__ + ': ' + str(error))
        signal.alarm(0)
        report['all_socket_objects_closed'] = all(value.fileno() == -1 for value in sockets)
        report['final_owned_inode_holders'] = {str(item['inode']): owned_fds(item['inode']) for item in identities}
        if not report['all_socket_objects_closed'] or any(report['final_owned_inode_holders'].values()) or report['cleanup_errors']:
            report['result'] = 'failed'
        report['elapsed_seconds'] = time.monotonic() - started
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + '\n')
        print(json.dumps({'result': report['result'], 'output': str(args.output),
                          'elapsed_seconds': report['elapsed_seconds'], 'failure': report['failure'],
                          'all_socket_objects_closed': report['all_socket_objects_closed']}))
    return 0 if report['result'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
