import os,sys,json,hashlib,pathlib,subprocess,time,shutil,urllib.request,tomllib,tarfile,re,traceback
root=pathlib.Path('/home/qian-qi/workspace/pipecat-browser-requalification-f9f576cc6c66')
assert not root.exists(), 'create-only browser requalification scope'
root.mkdir()
evidence=root/'evidence';evidence.mkdir()
source=root/'source-browser';target=root/'target-browser'
upstream=pathlib.Path('/home/qian-qi/workspace/pipecat-workspace-f9f576cc6c66/source-pin')
tools=pathlib.Path('/home/qian-qi/workspace/pipecat-workspace-f9f576cc6c66/tools/rust-1.99.0-r1/bin')
base='7105367d7d607653c3ab758f0131c029c4373faf'
tree='19a5c08fc2cbfeabacd344668fc7b25d03a282c2'
env=os.environ.copy()
env.update(PATH=str(tools)+':'+env.get('PATH',''),CARGO_HOME='/home/qian-qi/.cargo',RUSTC=str(tools/'rustc'),RUSTDOC=str(tools/'rustdoc'),RUSTFMT=str(tools/'rustfmt'),CARGO_TARGET_DIR=str(target),CARGO_BUILD_JOBS='2',CARGO_INCREMENTAL='0',CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0')
result={'scope':'New browser native tests/strict-Clippy and WASM strict-Clippy gates after local evidence became unavailable. Does not replace or relabel any earlier run; browser protocols are not rerun.','base_commit':base,'base_tree':tree,'source':str(source),'target':str(target),'resource_policy':{'cpu_percent':200,'memory_bytes':2147483648,'jobs':2,'nice':15},'preparation':[],'gates':[],'fatal_error':None,'source_before':None,'source_after':None}
def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def write_json(path,value):path.write_text(json.dumps(value,indent=2)+'\n')
def snapshot():
    if not (source/'.git').is_dir():return None
    paths=subprocess.check_output(['git','ls-files','-z','--','rust/client-templates','rust/vendor/jpeg-decoder-0.3.2','rust/rust-toolchain.toml'],cwd=source).decode().split('\0')
    return {p:digest(source/p) for p in sorted(paths) if p and (source/p).is_file()}
def run(label,argv,cwd,seconds=180,environment=None,gate=False):
    use_env=env if environment is None else environment
    before=snapshot() if gate else None
    start=time.monotonic()
    timed_out=False
    with (evidence/(label+'.log')).open('wb') as log:
        try:r=subprocess.run(argv,cwd=cwd,env=use_env,stdout=log,stderr=subprocess.STDOUT,timeout=seconds);code=r.returncode
        except subprocess.TimeoutExpired:code=None;timed_out=True
    raw=(evidence/(label+'.log')).read_bytes()
    record={'id':label,'argv':argv,'cwd':str(cwd),'exit_code':code,'timed_out':timed_out,'elapsed_seconds':round(time.monotonic()-start,6),'log':label+'.log','log_sha256':hashlib.sha256(raw).hexdigest(),'environment':{k:use_env[k] for k in ['CARGO_HOME','RUSTC','RUSTDOC','RUSTFMT','CARGO_TARGET_DIR','CARGO_BUILD_JOBS','CARGO_INCREMENTAL','CARGO_PROFILE_DEV_DEBUG','CARGO_PROFILE_TEST_DEBUG','CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUSTFLAGS'] if k in use_env}}
    if gate:
        after=snapshot()
        record.update(source_before=before,source_after=after,source_unchanged=before==after)
        matches=re.findall(r'test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;',raw.decode(errors='replace'))
        record['test_result_counts']=[{'passed':int(a),'failed':int(b),'ignored':int(c)} for a,b,c in matches]
        result['gates'].append(record)
        write_json(evidence/(label+'.json'),record)
    else:result['preparation'].append(record)
    write_json(evidence/'result.in-progress.json',result)
    return record
def require(record):
    assert record['exit_code']==0 and not record['timed_out'],record['id']+' failed'
def download(url,path,expected=None,max_seconds=180):
    started=time.monotonic()
    with urllib.request.urlopen(url,timeout=30) as response,path.open('wb') as out:
        while True:
            part=response.read(1024*1024)
            if not part:break
            out.write(part)
            assert time.monotonic()-started<max_seconds,'download deadline: '+url
    actual=digest(path)
    if expected is not None:assert actual==expected,'checksum mismatch: '+url
    return {'url':url,'path':str(path),'bytes':path.stat().st_size,'sha256':actual,'expected_sha256':expected,'elapsed_seconds':round(time.monotonic()-started,6)}
try:
    mem={}
    for line in pathlib.Path('/proc/meminfo').read_text().splitlines():
        fields=line.split()
        if fields[0] in ['MemTotal:','MemAvailable:']:mem[fields[0].rstrip(':')]=int(fields[1])*1024
    disk=shutil.disk_usage(root)
    result['resource_precheck']={'memory':mem,'disk_free_bytes':disk.free}
    assert mem.get('MemAvailable',0)>=4*1024**3 and disk.free>=3*1024**3,'deferred: insufficient browser headroom; native gate has priority'
    require(run('clone',['git','clone','--no-hardlinks','--no-checkout',str(upstream),str(source)],root,60))
    require(run('checkout',['git','checkout','--detach',base],source,60))
    assert subprocess.check_output(['git','rev-parse','HEAD^{tree}'],cwd=source,text=True).strip()==tree
    result['source_before']=snapshot()
    result['versions']={name:subprocess.check_output([str(tools/name),'--version'],env=env,text=True).strip() for name in ['cargo','rustc','rustfmt','cargo-clippy']}
    assert result['versions']['rustc'].startswith('rustc 1.99.0 ')
    native_identity_paths=[tools/x for x in ['cargo','rustc','rustdoc','rustfmt','cargo-clippy','clippy-driver']]
    native_before={str(p):digest(p) for p in native_identity_paths}
    cwd=source/'rust/client-templates'
    meta=run('dependency-readiness-offline',[str(tools/'cargo'),'metadata','--locked','--offline','--format-version','1'],cwd,60)
    if meta['exit_code']!=0:
        require(run('dependency-fetch',[str(tools/'cargo'),'fetch','--locked'],cwd,180))
        require(run('dependency-readiness-after-fetch',[str(tools/'cargo'),'metadata','--locked','--offline','--format-version','1'],cwd,60))
    run('native-tests',[str(tools/'cargo'),'test','--locked','--offline'],cwd,300,gate=True)
    run('native-strict-clippy',[str(tools/'cargo'),'clippy','--locked','--offline','--all-targets','--','-D','warnings'],cwd,240,gate=True)
    distribution=root/'official-wasm-component';distribution.mkdir()
    manifest=distribution/'channel-rust-1.99.0.toml'
    manifest_hash=distribution/'channel-rust-1.99.0.toml.sha256'
    sha_receipt=download('https://static.rust-lang.org/dist/channel-rust-1.99.0.toml.sha256',manifest_hash, max_seconds=60)
    expected=manifest_hash.read_text().split()[0]
    assert re.fullmatch('[0-9a-f]{64}',expected)
    manifest_receipt=download('https://static.rust-lang.org/dist/channel-rust-1.99.0.toml',manifest,expected)
    channel=tomllib.loads(manifest.read_text())
    assert channel['pkg']['rust']['version'].startswith('1.99.0 ')
    wasm=channel['pkg']['rust-std']['target']['wasm32-unknown-unknown']
    assert wasm['available'] and urllib.request.urlparse(wasm['xz_url']).hostname=='static.rust-lang.org'
    archive=distribution/'rust-std-wasm32.tar.xz'
    archive_receipt=download(wasm['xz_url'],archive,wasm['xz_hash'])
    unpacked=distribution/'unpacked';unpacked.mkdir()
    with tarfile.open(archive,'r:xz') as tf:
        for member in tf.getmembers():
            path=pathlib.PurePosixPath(member.name)
            assert not path.is_absolute() and '..' not in path.parts
            assert not member.issym() and not member.islnk() and not member.isdev()
        tf.extractall(unpacked,filter='data')
    installers=list(unpacked.glob('*/install.sh'));assert len(installers)==1
    private_sysroot=root/'sysroot-browser-wasm-1.99.0'
    install=run('install-private-wasm-std',['/bin/sh',str(installers[0]),'--prefix='+str(private_sysroot),'--disable-ldconfig'],root,90)
    require(install)
    sysroot_env={**env,'CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUSTFLAGS':'--sysroot='+str(private_sysroot)}
    target_libdir=subprocess.check_output([str(tools/'rustc'),'--target','wasm32-unknown-unknown','--sysroot',str(private_sysroot),'--print','target-libdir'],env=env,text=True).strip()
    assert pathlib.Path(target_libdir).is_dir() and list(pathlib.Path(target_libdir).glob('libstd-*.rlib'))
    result['wasm_component']={'sha_receipt':sha_receipt,'manifest':manifest_receipt,'channel_date':channel['date'],'rust_version':channel['pkg']['rust']['version'],'archive':archive_receipt,'private_sysroot':str(private_sysroot),'target_libdir':target_libdir,'target_only_rustflags':sysroot_env['CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUSTFLAGS'],'host_sysroot_overridden':False}
    write_json(evidence/'official-wasm-component.json',result['wasm_component'])
    run('wasm-strict-clippy',[str(tools/'cargo'),'clippy','--locked','--offline','--target','wasm32-unknown-unknown','--','-D','warnings'],cwd,300,environment=sysroot_env,gate=True)
    native_after={str(p):digest(p) for p in native_identity_paths}
    result['native_tools_identity']={'before':native_before,'after':native_after,'unchanged':native_before==native_after}
    assert native_before==native_after,'native tool binaries changed'
except Exception as error:
    result['fatal_error']=repr(error)
    (evidence/'preparation-error.log').write_text(traceback.format_exc())
finally:
    result['source_after']=snapshot()
    result['source_unchanged']=result['source_before'] is not None and result['source_before']==result['source_after']
    result['complete']=len(result['gates'])==3 and result['fatal_error'] is None
    result['passed']=result['complete'] and result['source_unchanged'] and all(g['exit_code']==0 and not g['timed_out'] and g['source_unchanged'] for g in result['gates'])
    write_json(evidence/'result.json',result)
    files=sorted(p for p in evidence.rglob('*') if p.is_file())
    (evidence/'SHA256SUMS').write_text(''.join(digest(p)+'  '+p.relative_to(evidence).as_posix()+'\n' for p in files))
    print(json.dumps({'evidence':str(evidence),'base_commit':base,'base_tree':tree,'complete':result['complete'],'passed':result['passed'],'source_unchanged':result['source_unchanged'],'fatal_error':result['fatal_error'],'gates':[{k:g[k] for k in ['id','exit_code','timed_out','elapsed_seconds','log_sha256','test_result_counts']} for g in result['gates']],'result_sha256':digest(evidence/'result.json'),'checksums_sha256':digest(evidence/'SHA256SUMS')},indent=2))
sys.exit(0 if result['passed'] else 1)
