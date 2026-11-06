#!/usr/bin/env python3
"""One bounded, read-only projection of already named HMI engineering paths."""
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import time

START = time.monotonic()
DEADLINE = START + 12
ROOT = Path('/home/qian-qi/work/hepta-agentd-product-inputs-evidence-20261104')
HMI = Path('/home/qian-qi/workspace/hepta-hmi-20261029')
SUBDIRS = ['correct-selector-r2', 'phase-diagnostic-r3', 'timeout-diagnostic']
MAX_BYTES = 262144
MAX_TOTAL = 2097152
TOTAL = 0
FIELDS = {
    'owner', 'owner_id', 'agent', 'agent_id', 'agent_generation', 'spawn_generation',
    'thread', 'thread_id', 'conversation_id', 'session_id', 'run_root', 'agent_run_root',
    'fleet_root', 'agent_home', 'owner_home', 'codex_home', 'workspace', 'cwd',
    'unit', 'unit_name', 'service_unit', 'socket', 'socket_path', 'hmi_socket',
    'hmi_text_socket', 'app_server_socket', 'app_server_uds', 'app_server_endpoint',
    'authbus_trust_file', 'authbus_checkpoint_file', 'hmi_audio_authority_file',
}
FLAGS = {('--' + x.replace('_', '-')): x for x in FIELDS}
FLAGS.update({'--hmi-text': 'hmi_text_enabled', '--authbus-trust-file': 'authbus_trust_file',
              '--authbus-checkpoint-file': 'authbus_checkpoint_file',
              '--hmi-audio-authority-file': 'hmi_audio_authority_file'})
DENIED = re.compile(r'private|secret|password|credential|api[_-]?key|access[_-]?token|refresh[_-]?token|signing[_-]?key|seed', re.I)
SAFE = re.compile(r'[A-Za-z0-9_./:@+={}\-$-]{1,512}\Z')
REPORT = {'schema_version': 1, 'scope': 'bounded public entry metadata in named historical engineering directories',
          'observed_unix': time.time(), 'roots': [str(ROOT), str(HMI)],
          'directories': [], 'files': [], 'public_entry_fields': [], 'referenced_path_metadata': [],
          'effects': {'remote_file_writes': False, 'process_or_environment_scan': False,
                      'service_start_or_replay': False, 'permissions_changed': False,
                      'physical_audio_opened': False, 'secret_files_read': False},
          'limits': {'seconds': 12, 'files': 32, 'per_file_bytes': MAX_BYTES, 'total_read_bytes': MAX_TOTAL,
                     'directory_entries_per_root': 80, 'recursive_depth': 'one level in each named directory only',
                     'projection': 'Allowlisted public identity/path fields; no raw argv/logs/config values emitted.'}}


def alive():
    return time.monotonic() < DEADLINE


def kind(mode):
    if stat.S_ISREG(mode): return 'file'
    if stat.S_ISDIR(mode): return 'directory'
    if stat.S_ISSOCK(mode): return 'socket'
    if stat.S_ISLNK(mode): return 'symlink_not_followed'
    return 'other_not_opened'


def metadata(path):
    try:
        s = path.lstat()
        return {'path': str(path), 'kind': kind(s.st_mode), 'bytes': s.st_size,
                'mode': oct(stat.S_IMODE(s.st_mode)), 'mtime_ns': s.st_mtime_ns}
    except OSError as e:
        return {'path': str(path), 'error': type(e).__name__, 'errno': e.errno}


def emit(field, value, file, locator):
    if len(REPORT['public_entry_fields']) >= 80: return
    if isinstance(value, bool): value = str(value).lower()
    elif isinstance(value, int): value = str(value)
    if not isinstance(value, str) or not SAFE.fullmatch(value): return
    # Never export URLs carrying authority/query data as an entry hint.
    if '://' in value or '?' in value or '@' in value: return
    REPORT['public_entry_fields'].append({'file': file, 'locator': locator, 'field': field,
        'value': value, 'provenance': 'historical engineering record; not live owner validation',
        'placeholder_or_fixture_hint': any(x in value.lower() for x in ['fixture', 'qualification', '/tmp/', '${', 'test-'])})


def argv_fields(argv, file, locator):
    for i, item in enumerate(argv):
        if not isinstance(item, str): continue
        flag, _, inline = item.partition('=')
        if flag not in FLAGS: continue
        if flag == '--hmi-text': emit(FLAGS[flag], True, file, locator + ':' + str(i))
        else:
            value = inline or (argv[i+1] if i+1 < len(argv) else None)
            emit(FLAGS[flag], value, file, locator + ':' + str(i))


def project(value, file, locator='$', depth=0):
    if depth > 10 or not alive(): return
    if isinstance(value, dict):
        for key, child in list(value.items())[:120]:
            if DENIED.search(str(key)): continue
            normalized = str(key).lower().replace('-', '_')
            here = locator + '.' + str(key)
            if normalized in FIELDS: emit(normalized, child, file, here)
            if normalized in {'argv', 'args', 'command'} and isinstance(child, list): argv_fields(child, file, here)
            project(child, file, here, depth+1)
    elif isinstance(value, list):
        for i, child in enumerate(value[:120]): project(child, file, locator + '[' + str(i) + ']', depth+1)


def read_public(path):
    global TOTAL
    if len(REPORT['files']) >= 32 or not alive(): return
    row = metadata(path)
    REPORT['files'].append(row)
    if row.get('kind') != 'file': return
    if row['bytes'] > MAX_BYTES or TOTAL + row['bytes'] > MAX_TOTAL:
        row['content_read'] = False; row['skip_reason'] = 'byte_bound'; return
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            before = os.fstat(fd)
            if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
                row['skip_reason'] = 'not_single_link_regular_file'; return
            raw = os.read(fd, MAX_BYTES + 1)
            after = os.fstat(fd)
        finally: os.close(fd)
        TOTAL += len(raw)
        if len(raw) != before.st_size or (before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns):
            row['skip_reason'] = 'changed_or_incomplete_read'; return
        row.update(content_read=True, sha256=hashlib.sha256(raw).hexdigest())
        text = raw.decode('utf-8')
        if path.suffix == '.json':
            project(json.loads(text), str(path))
        else:
            for number, line in enumerate(text.splitlines(), 1):
                if DENIED.search(line): continue
                for flag, key in FLAGS.items():
                    match = re.search(re.escape(flag) + r'(?:=|\s+)[\"\x27]?([A-Za-z0-9_./:+={}\-$-]{1,512})', line)
                    if match: emit(key, match.group(1), str(path), 'line:' + str(number))
                for match in re.finditer(r'\b(owner_id|agent_id|thread_id|run_root|fleet_root|codex_home|app_server_uds|socket_path|unit_name)\b[\"\x27]?\s*[:=]\s*[\"\x27]?([A-Za-z0-9_./:+={}\-$-]{1,512})', line):
                    emit(match.group(1), match.group(2), str(path), 'line:' + str(number))
    except (OSError, UnicodeError, ValueError) as e:
        row['read_error'] = type(e).__name__


def entries(directory):
    row = metadata(directory); REPORT['directories'].append(row)
    if row.get('kind') != 'directory' or not alive(): return []
    try:
        with os.scandir(directory) as scan:
            found = []
            for i, entry in enumerate(scan):
                if i >= 80: row['truncated'] = True; break
                found.append(Path(entry.path))
        found.sort(key=lambda p:p.name)
        row['entries'] = [{'name': p.name, 'kind': metadata(p).get('kind', 'unavailable')} for p in found]
        return found
    except OSError as e:
        row['listing_error'] = type(e).__name__; return []


# The two exact original test logs are already named in product-inputs.json.
for name in ['cognitive-original.log', 'supervisord-original.log']:
    read_public(ROOT / name)
for subdir in SUBDIRS:
    for path in entries(ROOT / subdir):
        if not alive(): break
        name = path.name.lower()
        if DENIED.search(name): continue
        # Public engineering metadata/logs only; no config, key, db, dump or archive.
        if path.suffix == '.json' and re.search(r'command|environment|validation|result|source|summary|status|manifest', name):
            read_public(path)
        elif path.suffix == '.log' and re.search(r'cognitive|supervisord|agentd|phase|timeout|run|test', name):
            read_public(path)
for path in entries(HMI):
    name = path.name.lower()
    if DENIED.search(name): continue
    if path.suffix in {'.service', '.socket'} or stat.S_ISSOCK(path.lstat().st_mode):
        REPORT['referenced_path_metadata'].append(metadata(path))
    elif path.suffix == '.json' and re.search(r'deploy|manifest|owner|agentd|supervisord|run-root|hmi', name) and not name.startswith('qualification'):
        row = metadata(path)
        row.update(content_read=False, reason='candidate deployment metadata only; unknown config contents not opened')
        REPORT['referenced_path_metadata'].append(row)

# Stat at most sixteen exact paths yielded by the public projection; never open them.
seen = set()
for row in REPORT['public_entry_fields']:
    value = row['value']
    if not value.startswith('/') or value in seen or not alive() or len(seen) >= 16: continue
    if any(part == '..' for part in Path(value).parts): continue
    seen.add(value)
    REPORT['referenced_path_metadata'].append(metadata(Path(value)))
REPORT['read_bytes'] = TOTAL
REPORT['elapsed_seconds'] = time.monotonic() - START
REPORT['deadline_reached'] = not alive()
REPORT['interpretation'] = 'No entry is treated as current or authorized merely because a historical record names it; no production composition is created or admitted.'
print(json.dumps(REPORT, ensure_ascii=False, separators=(',', ':')))
