import hashlib,json,os,pathlib,subprocess,sys,time,traceback
root=pathlib.Path('/home/qian-qi/workspace/pipecat-workspace-f9f576cc6c66')
payload=json.load(sys.stdin)
bundle=root/'requests/smart-turn-ort-levels-pin-r2'
bundle.mkdir(mode=0o700,exist_ok=False)
record={'schema':'pipecat-smart-turn-levels-launch-v2','phase':'preparing','start_unix':time.time(),'files_before':[],'files_after':[],'requested_resource_budget':{'cpu_quota_percent':200,'memory_max_bytes':4294967296,'nice':10,'timeout_seconds':120}}
def sha(b):return hashlib.sha256(b).hexdigest()
def save():(bundle/'launcher.json').write_text(json.dumps(record,indent=2)+'\n')
status=127
try:
 expected={"ort_reference_probe_disable.c":[11816,"4e5623210792a87b71509d0ff2ba538ec9fd73baef4cab897c72f132abfc7844"],"ort_reference_probe_basic.c":[11818,"d110fe4fd5e76856f8d9874a16da2899e9c92437659b0af289b17d675d7e35b1"],"onnxruntime_c_api.h":[219712,"7783e9664bdb0b1b36145b5c8587dd1bc9a399aa000edf2e3dd03003cafc6882"],"desktop_smart_turn_levels_pin_r2.py":[30275,"51eb47b6ed022620d98074e0063afa52c547cfa5f2086e67e130d0c526be6450"]}
 for name,(size,digest) in expected.items():
  raw=payload[name].encode()
  if len(raw)!=size or sha(raw)!=digest:raise ValueError('frozen source payload mismatch: '+name)
  path=bundle/name
  with path.open('xb') as out:out.write(raw)
  path.chmod(0o600)
  record['files_before'].append({'path':str(path),'name':name,'bytes':len(raw),'sha256':sha(raw)})
 helper=root/'run_full_gate.py';helper_raw=helper.read_bytes()
 if sha(helper_raw)!='c6b639d10e4e0d48910d007a161d7b35e81744bd5e31f878f960bdcd452b47e4':raise ValueError('reviewed source manifest helper changed')
 (bundle/'source-manifest-helper-before.py').write_bytes(helper_raw)
 record['files_before'].append({'path':str(helper),'name':'run_full_gate.py','bytes':len(helper_raw),'sha256':sha(helper_raw)})
 preceding=root/'requests/expanded-batch-launch-pin-r1/launcher.json'
 previous_raw=preceding.read_bytes();previous=json.loads(previous_raw)
 (bundle/'preceding-expanded-launcher.json').write_bytes(previous_raw)
 if previous.get('phase')!='completed' or not previous.get('scripts_unchanged') or previous.get('exit_code')==125:raise ValueError('expanded outer launcher must be complete with verified scripts and no integrity failure')
 record['preceding_expanded_launcher']={'path':str(preceding),'sha256':sha(previous_raw),'exit_code':previous.get('exit_code')}
 record['actual_argv']=[sys.executable,str(bundle/'desktop_smart_turn_levels_pin_r2.py'),'272671b95297313d24db921eceda8e95f3a8d545','94424ffa2a8fbcda0f81321ee68e7461663e191c','expanded-batch-pin-r1']
 record['cwd']=str(root);record['phase']='running';save()
 result=subprocess.run(record['actual_argv'],cwd=root);status=result.returncode;record['runner_exit_code']=status
except Exception as exc:
 record['failure']=repr(exc);(bundle/'launcher-error.log').write_text(traceback.format_exc())
finally:
 for before in record['files_before']:
  try:
   raw=pathlib.Path(before['path']).read_bytes()
   (bundle/(before['name']+'.after')).write_bytes(raw)
   record['files_after'].append({'path':before['path'],'name':before['name'],'bytes':len(raw),'sha256':sha(raw),'unchanged':len(raw)==before['bytes'] and sha(raw)==before['sha256']})
  except Exception as exc:record['files_after'].append({'name':before['name'],'unchanged':False,'error':repr(exc)})
 record['files_unchanged']=len(record['files_after'])==5 and all(x['unchanged'] for x in record['files_after'])
 if not record['files_unchanged']:status=125
 record.update(phase='completed',exit_code=status,end_unix=time.time());save()
 print(json.dumps(record),flush=True)
raise SystemExit(status)
