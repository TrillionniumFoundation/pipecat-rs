import concurrent.futures,fcntl,hashlib,json,os,pathlib,re,shutil,signal,subprocess,sys,tarfile,threading,time,tomllib,traceback,urllib.parse,urllib.request

root=pathlib.Path('/home/qian-qi/workspace/pipecat-browser-requalification-f9f576cc6c66')
source=root/'source-browser'
target=root/'target-browser'
prior=root/'evidence'
evidence=root/'evidence-wasm-r2'
distribution=root/'official-wasm-component'
downloads=root/'official-wasm-range-r2'
private_sysroot=root/'sysroot-browser-wasm-1.99.0-r2'
toolbin=pathlib.Path('/home/qian-qi/workspace/pipecat-workspace-f9f576cc6c66/tools/rust-1.99.0-r1/bin')
base='7105367d7d607653c3ab758f0131c029c4373faf'
tree='19a5c08fc2cbfeabacd344668fc7b25d03a282c2'
expected_size=23152600
expected_hash='b760b0c9b08a6843c05de0098bd6802e23178ff473c4715ae57c1d679125bf22'
manifest_hash='ce6dddc886364f8d786514771212cebe9b731ba82d6b859951c6b0ccc516b6a2'
expected_url='https://static.rust-lang.org/dist/2026-10-01/rust-std-1.99.0-wasm32-unknown-unknown.tar.xz'
assert root.is_dir() and source.is_dir() and target.is_dir()
assert not evidence.exists() and not downloads.exists() and not private_sysroot.exists(), 'create-only WASM r2 scope'
evidence.mkdir()
downloads.mkdir()
env=os.environ.copy()
removed_flags={k:env.pop(k) for k in ['RUSTFLAGS','CARGO_ENCODED_RUSTFLAGS','CARGO_BUILD_RUSTFLAGS'] if k in env}
env.update(PATH=str(toolbin)+':'+env.get('PATH',''),CARGO_HOME='/home/qian-qi/.cargo',RUSTC=str(toolbin/'rustc'),RUSTDOC=str(toolbin/'rustdoc'),RUSTFMT=str(toolbin/'rustfmt'),CARGO_TARGET_DIR=str(target),CARGO_BUILD_JOBS='2',CARGO_INCREMENTAL='0',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0')
result={'scope':'New WASM-only strict Clippy gate after the recorded truncated official download. Native 54 tests/native Clippy and browser protocols are not rerun or counted again. Prior evidence and bad archive remain unchanged.','base_commit':base,'base_tree':tree,'source':str(source),'target':str(target),'resource_policy':{'cpu_percent':200,'memory_bytes':2147483648,'jobs':2,'nice':15,'outer_timeout_seconds':900},'preparation':[],'range_attempts':[],'gates':[],'fatal_error':None,'source_before':None,'source_after':None,'removed_inherited_global_rustflags':removed_flags,'private_sysroot':str(private_sysroot)}
native_paths=[toolbin/name for name in ['cargo','rustc','rustdoc','rustfmt','cargo-clippy','clippy-driver']]
lock_file=None
write_lock=threading.Lock()

def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda:f.read(1024*1024),b''):h.update(chunk)
    return h.hexdigest()

def save(path,value):
    path.write_text(json.dumps(value,indent=2)+'\n')

def snapshot():
    paths=subprocess.check_output(['git','ls-files','-z','--','rust/client-templates','rust/vendor/jpeg-decoder-0.3.2','rust/rust-toolchain.toml'],cwd=source).decode().split('\0')
    return {p:digest(source/p) for p in sorted(paths) if p and (source/p).is_file()}

def state():
    save(evidence/'result.in-progress.json',result)

def run(label,argv,cwd,seconds=180,environment=None,gate=False):
    use_env=env if environment is None else environment
    before=snapshot() if gate else None
    start=time.monotonic()
    timed_out=False
    cleanup=None
    with (evidence/(label+'.log')).open('wb') as log:
        process=subprocess.Popen(argv,cwd=cwd,env=use_env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        try:code=process.wait(timeout=seconds)
        except subprocess.TimeoutExpired:
            timed_out=True
            cleanup={'process_group':process.pid,'term_requested':False,'kill_requested':False,'leader_reaped':False}
            try:os.killpg(process.pid,signal.SIGTERM);cleanup['term_requested']=True
            except ProcessLookupError:pass
            try:code=process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                try:os.killpg(process.pid,signal.SIGKILL);cleanup['kill_requested']=True
                except ProcessLookupError:pass
                code=process.wait(timeout=3)
            cleanup['leader_reaped']=process.returncode is not None
    record={'id':label,'argv':argv,'cwd':str(cwd),'exit_code':code,'timed_out':timed_out,'timeout_cleanup':cleanup,'elapsed_seconds':round(time.monotonic()-start,6),'log':label+'.log','log_sha256':digest(evidence/(label+'.log')),'environment':{k:use_env[k] for k in ['CARGO_HOME','RUSTC','RUSTDOC','RUSTFMT','CARGO_TARGET_DIR','CARGO_BUILD_JOBS','CARGO_INCREMENTAL','CARGO_PROFILE_DEV_DEBUG','CARGO_PROFILE_TEST_DEBUG','CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUSTFLAGS'] if k in use_env}}
    if gate:
        after=snapshot()
        record.update(source_before=before,source_after=after,source_unchanged=before==after)
        result['gates'].append(record)
        save(evidence/(label+'.json'),record)
    else:result['preparation'].append(record)
    state()
    if timed_out:raise RuntimeError(label+' timed out; no subsequent build phase will run')
    return record

def require(record):
    assert record['exit_code']==0 and not record['timed_out'],record['id']+' failed'

try:
    lock_file=(root/'browser-target.lock').open('a+')
    fcntl.flock(lock_file,fcntl.LOCK_EX|fcntl.LOCK_NB)
    result['exclusive_lock']=str(root/'browser-target.lock')
    assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip()==base
    assert subprocess.check_output(['git','rev-parse','HEAD^{tree}'],cwd=source,text=True).strip()==tree
    result['source_before']=snapshot()
    previous=json.loads((prior/'result.json').read_text())
    diagnosis=json.loads((prior/'wasm-download-diagnosis.json').read_text())
    assert previous['source_before']==previous['source_after']==result['source_before']
    assert len(previous['gates'])==2 and all(g['exit_code']==0 for g in previous['gates'])
    assert previous['fatal_error'] is not None and 'checksum mismatch' in previous['fatal_error']
    retained=[prior/'result.json',prior/'preparation-error.log',prior/'SHA256SUMS',prior/'wasm-download-diagnosis.json',distribution/'rust-std-wasm32.tar.xz']
    result['prior_retained_files_before']={str(p):{'bytes':p.stat().st_size,'sha256':digest(p)} for p in retained}
    assert diagnosis['actual_sha256']==digest(distribution/'rust-std-wasm32.tar.xz')
    assert diagnosis['actual_bytes']==(distribution/'rust-std-wasm32.tar.xz').stat().st_size==8338068
    assert diagnosis['expected_sha256']==expected_hash
    result['native_tools_before']={str(p):digest(p) for p in native_paths}
    result['versions']={name:subprocess.check_output([str(toolbin/name),'--version'],env=env,text=True).strip() for name in ['cargo','rustc','rustfmt','cargo-clippy']}
    assert result['versions']['rustc'].startswith('rustc 1.99.0 ')
    mem={}
    for line in pathlib.Path('/proc/meminfo').read_text().splitlines():
        fields=line.split()
        if fields[0] in ['MemTotal:','MemAvailable:']:mem[fields[0].rstrip(':')]=int(fields[1])*1024
    result['resource_precheck']={'memory':mem,'disk_free_bytes':shutil.disk_usage(root).free}
    assert mem.get('MemAvailable',0)>=4*1024**3 and result['resource_precheck']['disk_free_bytes']>=2*1024**3,'deferred: insufficient headroom; native matrix has priority'
    manifest=distribution/'channel-rust-1.99.0.toml'
    assert digest(manifest)==manifest_hash
    assert (distribution/'channel-rust-1.99.0.toml.sha256').read_text().split()[0]==manifest_hash
    channel=tomllib.loads(manifest.read_text())
    component=channel['pkg']['rust-std']['target']['wasm32-unknown-unknown']
    assert channel['pkg']['rust']['version'].startswith('1.99.0 ')
    assert component['available'] and component['xz_url']==expected_url and component['xz_hash']==expected_hash
    assert urllib.parse.urlparse(expected_url).hostname=='static.rust-lang.org'
    result['official_component']={'manifest':str(manifest),'manifest_sha256':manifest_hash,'url':expected_url,'expected_archive_sha256':expected_hash,'expected_archive_bytes':expected_size,'version':channel['pkg']['rust']['version'],'channel_date':channel['date']}
    state()
    deadline=time.monotonic()+450
    chunk_size=1024*1024
    ranges=[(i,start,min(start+chunk_size,expected_size)-1) for i,start in enumerate(range(0,expected_size,chunk_size))]
    def fetch_range(item):
        index,start,end=item
        want=end-start+1
        for attempt in range(1,3):
            path=downloads/('range-%03d-attempt-%d.part'%(index,attempt))
            record={'index':index,'attempt':attempt,'start':start,'end':end,'expected_bytes':want,'url':expected_url,'request_headers':{'Range':'bytes=%d-%d'%(start,end),'Accept-Encoding':'identity'},'status':None,'response_headers':None,'path':str(path),'error':None}
            started=time.monotonic()
            try:
                assert time.monotonic()<deadline,'range download overall deadline'
                request=urllib.request.Request(expected_url,headers=record['request_headers'])
                with urllib.request.urlopen(request,timeout=30) as response,path.open('xb') as output:
                    record['status']=response.status
                    record['response_headers']=dict(response.headers)
                    record['final_url']=response.geturl()
                    assert response.status==206,'expected HTTP206'
                    assert response.headers.get('Content-Range')=='bytes %d-%d/%d'%(start,end,expected_size),'incorrect Content-Range'
                    if response.headers.get('Content-Length') is not None:
                        assert int(response.headers['Content-Length'])==want,'incorrect Content-Length'
                    count=0
                    while True:
                        assert time.monotonic()<deadline,'range download overall deadline'
                        assert time.monotonic()-started<100,'per-range deadline'
                        data=response.read(min(65536,want-count+1))
                        if not data:break
                        output.write(data)
                        count+=len(data)
                        assert count<=want,'range returned too many bytes'
                assert path.stat().st_size==want,'short range'
            except Exception as error:
                record['error']=repr(error)
                record['traceback']=traceback.format_exc()
            finally:
                record.update(actual_bytes=path.stat().st_size if path.exists() else 0,sha256=digest(path) if path.exists() else None,elapsed_seconds=round(time.monotonic()-started,6))
                save(evidence/('range-%03d-attempt-%d.json'%(index,attempt)),record)
                with write_lock:
                    result['range_attempts'].append(record)
                    state()
            if record['error'] is None:return record
        raise RuntimeError('range %d failed both recorded attempts'%index)
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        successful=list(pool.map(fetch_range,ranges))
    archive=downloads/'rust-std-1.99.0-wasm32-unknown-unknown.complete.tar.xz'
    with archive.open('xb') as output:
        for part in sorted(successful,key=lambda row:row['index']):
            with pathlib.Path(part['path']).open('rb') as reader:shutil.copyfileobj(reader,output)
    result['completed_archive']={'path':str(archive),'bytes':archive.stat().st_size,'sha256':digest(archive),'range_count':len(successful),'range_attempt_count':len(result['range_attempts'])}
    assert result['completed_archive']['bytes']==expected_size
    assert result['completed_archive']['sha256']==expected_hash,'complete official archive checksum mismatch'
    require(run('official-archive-xz-integrity',['xz','--test',str(archive)],root,60))
    unpacked=downloads/'unpacked'
    unpacked.mkdir()
    with tarfile.open(archive,'r:xz') as tf:
        for member in tf.getmembers():
            name=pathlib.PurePosixPath(member.name)
            assert not name.is_absolute() and '..' not in name.parts
            assert not member.issym() and not member.islnk() and not member.isdev()
        tf.extractall(unpacked,filter='data')
    installers=list(unpacked.glob('*/install.sh'))
    assert len(installers)==1
    require(run('install-private-wasm-std',['/bin/sh',str(installers[0]),'--prefix='+str(private_sysroot),'--disable-ldconfig'],root,90))
    target_libdir=subprocess.check_output([str(toolbin/'rustc'),'--target','wasm32-unknown-unknown','--sysroot',str(private_sysroot),'--print','target-libdir'],env=env,text=True).strip()
    assert pathlib.Path(target_libdir).is_dir() and list(pathlib.Path(target_libdir).glob('libstd-*.rlib'))
    wasm_env={**env,'CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUSTFLAGS':'--sysroot='+str(private_sysroot)}
    result['wasm_component_install']={'prefix':str(private_sysroot),'target_libdir':target_libdir,'target_lib_files':{p.name:digest(p) for p in sorted(pathlib.Path(target_libdir).iterdir()) if p.is_file()},'target_only_rustflags':wasm_env['CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUSTFLAGS'],'host_sysroot_overridden':False}
    state()
    require(run('wasm-strict-clippy',[str(toolbin/'cargo'),'clippy','--locked','--offline','--target','wasm32-unknown-unknown','--','-D','warnings'],source/'rust/client-templates',300,wasm_env,gate=True))
except Exception as error:
    result['fatal_error']=repr(error)
    (evidence/'error.log').write_text(traceback.format_exc())
finally:
    result['source_after']=snapshot()
    result['source_unchanged']=result['source_before'] is not None and result['source_before']==result['source_after']
    result['native_tools_after']={str(p):digest(p) for p in native_paths if p.exists()}
    result['native_tools_unchanged']=result.get('native_tools_before')==result['native_tools_after']
    if 'prior_retained_files_before' in result:
        result['prior_retained_files_after']={p:{'bytes':pathlib.Path(p).stat().st_size,'sha256':digest(pathlib.Path(p))} for p in result['prior_retained_files_before']}
        result['prior_records_unchanged']=result['prior_retained_files_before']==result['prior_retained_files_after']
    result['complete']=len(result['gates'])==1 and result['fatal_error'] is None
    result['passed']=result['complete'] and result['source_unchanged'] and result['native_tools_unchanged'] and result.get('prior_records_unchanged',False) and all(g['exit_code']==0 and not g['timed_out'] and g['source_unchanged'] for g in result['gates'])
    save(evidence/'result.json',result)
    files=sorted(p for p in evidence.rglob('*') if p.is_file())
    (evidence/'SHA256SUMS').write_text(''.join(digest(p)+'  '+p.relative_to(evidence).as_posix()+'\n' for p in files))
    print(json.dumps({'scope':result['scope'],'evidence':str(evidence),'base_commit':base,'base_tree':tree,'passed':result['passed'],'source_unchanged':result['source_unchanged'],'native_tools_unchanged':result['native_tools_unchanged'],'prior_records_unchanged':result.get('prior_records_unchanged'),'fatal_error':result['fatal_error'],'range_attempt_count':len(result['range_attempts']),'completed_archive':result.get('completed_archive'),'gates':[{k:g[k] for k in ['id','exit_code','timed_out','elapsed_seconds','log_sha256']} for g in result['gates']],'result_sha256':digest(evidence/'result.json'),'checksums_sha256':digest(evidence/'SHA256SUMS')},indent=2))
    if lock_file is not None:lock_file.close()
sys.exit(0 if result['passed'] else 1)
