import pathlib,json,hashlib,tomllib,urllib.request,subprocess,time
root=pathlib.Path('/home/qian-qi/workspace/pipecat-browser-requalification-f9f576cc6c66')
manifest=root/'official-wasm-component/channel-rust-1.99.0.toml'
archive=root/'official-wasm-component/rust-std-wasm32.tar.xz'
channel=tomllib.loads(manifest.read_text())
entry=channel['pkg']['rust-std']['target']['wasm32-unknown-unknown']
result={'scope':'Read-only diagnosis of the first failed WASM component download; bad archive retained unchanged.','archive':str(archive),'actual_bytes':archive.stat().st_size,'actual_sha256':hashlib.sha256(archive.read_bytes()).hexdigest(),'first_32_bytes_hex':archive.open('rb').read(32).hex(),'expected_sha256':entry['xz_hash'],'official_url':entry['xz_url'],'manifest_sha256':hashlib.sha256(manifest.read_bytes()).hexdigest(),'manifest_version':channel['pkg']['rust']['version']}
try:
 with urllib.request.urlopen(urllib.request.Request(entry['xz_url'],method='HEAD'),timeout=20) as response:
  result['head']={'status':response.status,'headers':dict(response.headers.items()),'final_url':response.geturl()}
except Exception as error:result['head_error']=repr(error)
try:
 p=subprocess.run(['xz','--test',str(archive)],capture_output=True,timeout=15)
 result['xz_integrity']={'exit_code':p.returncode,'stdout':p.stdout.decode(errors='replace'),'stderr':p.stderr.decode(errors='replace')}
except Exception as error:result['xz_integrity_error']=repr(error)
out=root/'evidence/wasm-download-diagnosis.json'
out.write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
