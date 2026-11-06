import hashlib, io, json, os, pathlib, signal, stat, time, wave

ROOT = pathlib.Path('/home/qian-qi/workspace/hepta-hmi-20261029')
ASSETS = ROOT / 'assets'
WAV_SHA = '2f9fc9c912bb71c85fb286cb88b599c81efb8f727c727a5ea8f6d1c89c55ac13'
ANNOTATION_SHA = 'aa82642e1ccfa6cc90b85f6cddc82838f1b34f45f05f0a5f0c10005006dbd1e7'
MODEL_SHA = '60ed5bc3dd14eea856493d334349b405782ddcaf0028d4b5df4088345fba2efe'
REFERENCE = '广州市房地产中介协会分析'
START = time.monotonic()
REPORT = {'schema_version': 1, 'scope': 'read_only_exact_historical_public_AISHELL_fixture_presence_not_STT_execution',
          'root': str(ROOT), 'observed_unix': time.time(), 'files': [], 'source_records': [],
          'asset_name_candidates': [], 'wav_candidates': [], 'annotation_candidates': [],
          'config_projections': [], 'model': None, 'fatal_error': None,
          'effects': {'audio_playback': False, 'audio_capture': False, 'model_inference': False,
                      'remote_file_writes': False, 'configuration_changes': False,
                      'owner_or_thread_created': False},
          'expected': {'repository': 'wenet-e2e/wenet', 'revision': 'be6ad97fdb512589df113042f7ff3a29d392fb38',
                       'wav_sha256': WAV_SHA, 'annotation_sha256': ANNOTATION_SHA,
                       'reference': REFERENCE, 'rate_hz': 16000, 'channels': 1, 'frames': 68496}}

class ProbeDeadline(RuntimeError):
    pass

def deadline(signum=None, frame=None):
    raise ProbeDeadline('12 second read-only fixture probe deadline')

signal.signal(signal.SIGALRM, deadline)
signal.alarm(12)

def identity(st):
    return {'device': st.st_dev, 'inode': st.st_ino, 'bytes': st.st_size, 'mtime_ns': st.st_mtime_ns}

def read_file(path, maximum=1048576, keep=True):
    path = pathlib.Path(path)
    row = {'path': str(path), 'content_read': False}
    REPORT['files'].append(row)
    try:
        before = path.lstat()
        row.update(identity(before), mode=oct(stat.S_IMODE(before.st_mode)))
        if not stat.S_ISREG(before.st_mode):
            row['refused'] = 'not an ordinary file'; return None, row
        if before.st_size > maximum:
            row['refused'] = 'file exceeds explicit byte budget'; return None, row
        digest = hashlib.sha256(); chunks = []; total = 0
        with path.open('rb') as f:
            opened = os.fstat(f.fileno())
            assert identity(opened) == identity(before), 'file changed before open'
            while True:
                chunk = f.read(min(1048576, maximum + 1 - total))
                if not chunk: break
                total += len(chunk)
                assert total <= maximum, 'file grew over byte budget'
                digest.update(chunk)
                if keep: chunks.append(chunk)
            after = os.fstat(f.fileno())
        row.update(content_read=True, sha256=digest.hexdigest(), read_bytes=total,
                   stable_metadata=identity(before) == identity(after), after=identity(after))
        assert row['stable_metadata'] and total == before.st_size, 'file changed during read'
        return b''.join(chunks) if keep else b'', row
    except (OSError, ValueError, AssertionError) as error:
        row['error'] = repr(error); return None, row

def strings(value):
    if isinstance(value, str): yield value
    elif isinstance(value, dict):
        for child in value.values(): yield from strings(child)
    elif isinstance(value, list):
        for child in value: yield from strings(child)

def explicit_asset_path(value):
    p = pathlib.Path(value)
    if p.is_absolute() and '..' not in p.parts and p.parent == ASSETS:
        return p
    return None

try:
    candidates = set()
    public_keys = {'repository', 'revision', 'wav_path', 'wav_sha256', 'annotation_path',
                   'annotation_sha256', 'expected', 'reference', 'wav_format',
                   'annotation_audio_alias', 'alias_blob_sha256', 'local_wav_path', 'local_annotation_path'}
    for name in ['human-chinese-source-actual.json', 'human-chinese-source.json']:
        raw, row = read_file(ROOT / name, 131072)
        if raw is None: continue
        data = json.loads(raw)
        REPORT['source_records'].append({'path': str(ROOT / name), 'sha256': row['sha256'],
                                        'keys': sorted(data),
                                        'public_fields': {k: v for k, v in data.items() if k in public_keys}})
        if WAV_SHA in set(strings(data)):
            for value in strings(data):
                p = explicit_asset_path(value)
                if p is not None and (p.suffix == '.wav' or p.name.endswith('data.list')):
                    candidates.add(p)
    with os.scandir(ASSETS) as entries:
        count = 0
        for entry in entries:
            count += 1
            if count > 80:
                REPORT['asset_listing_truncated'] = True; break
            name = entry.name.lower()
            if not entry.is_file(follow_symlinks=False): continue
            if any(token in name for token in ['aishell', 'wenet', 'human', 'chinese', 'data.list']):
                REPORT['asset_name_candidates'].append({'name': entry.name, 'bytes': entry.stat(follow_symlinks=False).st_size})
            if (name.endswith('.wav') and ('aishell' in name or 'bac009s0724w0121' in name)) or name.endswith('data.list'):
                candidates.add(ASSETS / entry.name)
    REPORT.setdefault('asset_listing_truncated', False)
    assert len(candidates) <= 8, 'too many explicit public-fixture candidates'
    for path in sorted(candidates):
        raw, row = read_file(path)
        if raw is None: continue
        if path.suffix.lower() == '.wav':
            out = {'path': str(path), 'sha256': row['sha256'], 'bytes': row['bytes'], 'matches_original': row['sha256'] == WAV_SHA}
            if out['matches_original']:
                with wave.open(io.BytesIO(raw), 'rb') as wav:
                    out['wav_format'] = {'rate_hz': wav.getframerate(), 'channels': wav.getnchannels(),
                                         'frames': wav.getnframes(), 'sample_width_bytes': wav.getsampwidth(),
                                         'compression': wav.getcomptype()}
            REPORT['wav_candidates'].append(out)
        else:
            out = {'path': str(path), 'sha256': row['sha256'], 'bytes': row['bytes'], 'matches_original': row['sha256'] == ANNOTATION_SHA}
            if out['matches_original']:
                out['target_lines'] = [line for line in raw.decode('utf-8').splitlines() if 'BAC009S0724W0121' in line]
                out['contains_reference'] = REFERENCE in raw.decode('utf-8')
            REPORT['annotation_candidates'].append(out)
    for name in ['qualification-speech-release.json', 'qualification-speech.json']:
        raw, row = read_file(ROOT / name, 131072)
        if raw is None: continue
        config = json.loads(raw)
        REPORT['config_projections'].append({'path': str(ROOT / name), 'sha256': row['sha256'],
            'whisper_model': config.get('whisper_model'), 'whisper_settings': config.get('whisper_settings'),
            'limit': 'Unused LocalSpeech fields/identity labels omitted; stt_fixture does not admit them.'})
    raw, row = read_file(ASSETS / 'whisper-base.bin', 160 * 1024 * 1024, keep=False)
    REPORT['model'] = {'path': str(ASSETS / 'whisper-base.bin'), 'sha256': row.get('sha256'),
                       'bytes': row.get('bytes'), 'matches_original': row.get('sha256') == MODEL_SHA}
    raw, row = read_file(ROOT / 'human-configured-base.log', 262144)
    REPORT['historical_base_log'] = {'path': row['path'], 'sha256': row.get('sha256'), 'bytes': row.get('bytes'),
        'matches_published': row.get('sha256') == '3c3fc16e34bc258a056fcceb5a87a556ca5c1faa4d6f172a66ff4023b9786b93',
        'limit': 'Historical log hash only; not a new inference result.'}
except Exception as error:
    REPORT['fatal_error'] = repr(error)
finally:
    signal.alarm(0)
    REPORT['elapsed_seconds'] = time.monotonic() - START
    REPORT['matching_wav_paths'] = [r['path'] for r in REPORT['wav_candidates'] if r['matches_original']]
    REPORT['matching_annotation_paths'] = [r['path'] for r in REPORT['annotation_candidates'] if r['matches_original']]
    REPORT['limits'] = {'seconds': 12, 'asset_directory_depth': 1, 'asset_directory_entries': 80,
                         'candidate_files': 8, 'fixture_file_bytes': 1048576,
                         'model_file_bytes': 160 * 1024 * 1024,
                         'inference_or_current_owner_qualification': False}
    print(json.dumps(REPORT, ensure_ascii=False, separators=(',', ':')))
