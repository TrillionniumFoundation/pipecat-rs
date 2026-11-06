import base64,gzip,hashlib,json,pathlib,sys
root=pathlib.Path('/home/qian-qi/workspace/pipecat-browser-requalification-f9f576cc6c66')
out=root/'export-browser-gates-r2'
assert not out.exists(), 'create-only browser evidence export'
evidence=root/'evidence'
wasm=root/'evidence-wasm-r2'
assert (evidence/'result.json').is_file() and (wasm/'result.json').is_file(), 'terminal results required'
native_result=json.loads((evidence/'result.json').read_text())
wasm_result=json.loads((wasm/'result.json').read_text())
assert native_result['source_before']==native_result['source_after']
assert wasm_result['source_before']==wasm_result['source_after']==native_result['source_before']
out.mkdir()
sources=json.load(sys.stdin)
scripts=out/'scripts'
scripts.mkdir()
for name,content in sources.items():
    assert '/' not in name and name.endswith('.py')
    (scripts/name).write_text(content)
files={}
def add(path,name):
    raw=path.read_bytes()
    files[name]={'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'content':raw.decode('utf-8'),'origin':str(path)}
for folder in [evidence,wasm]:
    for path in sorted(folder.rglob('*')):
        if path.is_file():add(path,folder.name+'/'+path.relative_to(folder).as_posix())
for name in ['channel-rust-1.99.0.toml','channel-rust-1.99.0.toml.sha256']:
    add(root/'official-wasm-component'/name,'official-wasm-component/'+name)
for path in sorted(scripts.iterdir()):add(path,'scripts/'+path.name)
index={'scope':'Exact new browser qualification raw receipts and scripts. The first run has native 54 tests/native Clippy success and a failed truncated official WASM archive; the second run is WASM-only. No browser protocol suite is rerun here. The original bad binary archive remains on desktop; its actual bytes/sha and XZ failure are in the diagnosis. This export contains complete UTF-8 evidence, not reconstructed logs.','root':str(root),'file_count':len(files),'uncompressed_content_bytes':sum(x['bytes'] for x in files.values()),'first_run_overall_passed':native_result['passed'],'native_phase_results':[{'id':g['id'],'exit_code':g['exit_code'],'log_sha256':g['log_sha256']} for g in native_result['gates']],'native_r1_fatal_error':native_result['fatal_error'],'wasm_r2_passed':wasm_result['passed'],'wasm_r2_fatal_error':wasm_result['fatal_error'],'base_commit':native_result['base_commit'],'base_tree':native_result['base_tree'],'files':[{k:v for k,v in {'path':name,**entry}.items() if k!='content'} for name,entry in sorted(files.items())]}
(out/'index.json').write_text(json.dumps(index,indent=2)+'\n')
payload=json.dumps({'index':index,'files':files},ensure_ascii=False,separators=(',',':')).encode()
archive=gzip.compress(payload,compresslevel=9,mtime=0)
(out/'bundle.json.gz').write_bytes(archive)
encoded=base64.b64encode(archive).decode('ascii')
chunk_paths=[]
for i,start in enumerate(range(0,len(encoded),32768)):
    path=out/('bundle-%03d.b64'%i)
    path.write_text(encoded[start:start+32768]+'\n')
    chunk_paths.append(str(path))
summary={'export':str(out),'file_count':len(files),'content_bytes':index['uncompressed_content_bytes'],'json_bytes':len(payload),'gzip_bytes':len(archive),'gzip_sha256':hashlib.sha256(archive).hexdigest(),'index_sha256':hashlib.sha256((out/'index.json').read_bytes()).hexdigest(),'chunks':chunk_paths,'chunk_sha256':{path:hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest() for path in chunk_paths},'native_r1_fatal_error':native_result['fatal_error'],'wasm_r2_passed':wasm_result['passed'],'wasm_r2_fatal_error':wasm_result['fatal_error']}
(out/'export-receipt.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(summary,indent=2))
