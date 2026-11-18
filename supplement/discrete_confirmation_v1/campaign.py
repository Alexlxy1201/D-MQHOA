from adapter import *
from concurrent.futures import ProcessPoolExecutor
from mqhoa_supp.instances import synthetic
from codec import schema,iter_events,iter_sidecar,verify_files
import argparse,random,zipfile,shutil,gzip,datetime
BASELINE=ROOT/'supplement/discrete_confirmation_checks_20261109/OLD_FILES_BASELINE.json'
OLD=kernel.OUT

def physical_input(data):return kernel.digest({'tl':list(map(float,data['tl'])),'ts':list(map(float,data['ts'])),'edges':sorted([[int(a),int(b),float(c)] for a,b,c in data['edges']]),'pinned':sorted(map(int,data['pinned']))})

def seed_table():
 result=[]
 for s,n in enumerate(CFG['sizes']):
  for h,shape in enumerate(CFG['shapes']):
   for c,ccr in enumerate(CFG['ccrs']):
    for r in range(3):
     text='0p1' if c==0 else '1' if c==1 else '10';result.append({'instance_id':f'confirm-v1-n{n}-{shape}-ccr{text}-r{r}','n':n,'shape':shape,'ccr':ccr,'r':r,'stratum':s*6+h*3+c,'generation_seed':CFG['seed_base']+10000*s+1000*h+100*c+r})
 return result

def scan_old_inputs():
 found={};seeds=set();paths=[]
 def walk(x,origin):
  if isinstance(x,dict):
   if {'tl','ts','edges','pinned'}<=set(x):
    try:key=physical_input(x)
    except (TypeError,ValueError):return
    found[key]=origin;seed=x.get('metadata',{}).get('seed')
    if isinstance(seed,int):seeds.add(seed)
   for v in x.values():walk(v,origin)
  elif isinstance(x,list):
   for v in x:walk(v,origin)
 for p in ROOT.rglob('*.json'):
  if '.git' in p.parts or OUT in p.parents or 'matplotlib_cache' in p.parts:continue
  try:walk(json.loads(p.read_text(encoding='utf8')),str(p.relative_to(ROOT)));paths.append(str(p.relative_to(ROOT)))
  except (UnicodeError,json.JSONDecodeError):continue
 for p in ROOT.rglob('*.zip'):
  if OUT in p.parents or '.git' in p.parts:continue
  try:
   with zipfile.ZipFile(p) as z:
    for name in z.namelist():
     if 'instances/' in name.replace('\\','/') and name.endswith('.json'):
      walk(json.loads(z.read(name)),str(p.relative_to(ROOT))+':'+name)
  except zipfile.BadZipFile:continue
 return {'physical_hashes':found,'used_input_generation_seeds':sorted(seeds),'scanned_JSON_paths':paths,'raw_trace_records_or_vectors_exported':False}

def matrix(stage,design,oldfrozen):
 if stage=='smoke':ids=[('archive10',10),('archive50',50),('eval-n100-deep-r0',100)]
 else:ids=[(d['instance_id'],d['n']) for d in design]
 blocks={}
 for i,n in ids:
  block=[]
  for m in CFG['methods']:
   for seed in ([0] if stage in ['smoke','timing'] else CFG['algorithm_seeds']) if m in CFG['stochastic_methods'] else [None]:
    t={'protocol':CFG['protocol'],'stage':stage,'instance_id':i,'n':n,'method':m,'seed':seed,'budget':1 if m=='HEFT' else 10000,'pso_parameters':oldfrozen['PSO']};t['run_id']=kernel.digest(t)[:24];block.append(t)
  blocks[i]=block
 if stage=='smoke':return [t for i,n in ids for t in blocks[i]]
 if stage=='quality':
  rng=random.Random(CFG['quality_shuffle_seed']);names=list(blocks);rng.shuffle(names)
  for b in blocks.values():rng.shuffle(b)
  return [blocks[i][slot] for slot in range(122) for i in names]
 eligible=[d['instance_id'] for d in design if d['r']==0];base=[t for i in eligible for t in blocks[i]];rng=random.Random(CFG['timing_shuffle_seed']);out=[]
 for rep in range(6):
  chosen=base.copy();rng.shuffle(chosen)
  for t in chosen:
   q={**t,'timing_repeat':rep,'warmup':rep==0};q['run_id']=kernel.digest(q)[:24];out.append(q)
 return out

def sources():
 paths=sorted(Path(__file__).parent.glob('*.py'))+sorted(Path(__file__).parent.glob('*.md'))+sorted(Path(__file__).parent.glob('*.json'))+[ROOT/'supplement/DISCRETE_CONFIRMATION_PLAN_20261109.md']
 old=read(OLD/'freeze.json')
 for p,h in old['sources'].items():assert sha(ROOT/p)==h;paths.append(ROOT/p)
 return {str(p.relative_to(ROOT)):sha(p) for p in paths}

def freeze():
 OUT.mkdir(parents=True,exist_ok=True);assert not (OUT/'freeze.json').exists();assert sha(OLD/'freeze.json')==CFG['core_freeze_sha256'];design=seed_table();catalog=scan_old_inputs();assert not set(d['generation_seed'] for d in design)&set(catalog['used_input_generation_seeds'])
 write(OUT/'reserved_seed_table.json',design);write(OUT/'old_input_catalog.json',catalog);write(Path(__file__).with_name('schema.json'),schema());oldfrozen=kernel.load()[2]
 for stage in ['smoke','quality','timing']:write(OUT/(stage+'_matrix.json'),matrix(stage,design,oldfrozen))
 assert len(read(OUT/'smoke_matrix.json'))==24 and len(read(OUT/'quality_matrix.json'))==4392 and len(read(OUT/'timing_matrix.json'))==576
 f={'sources':sources(),'core_freeze_sha256':sha(OLD/'freeze.json'),'core_config_sha256':sha(ROOT/'supplement/discrete_neighborhood_v1/config.json'),'protected_baseline_sha256':sha(BASELINE),'matrices':{p:sha(OUT/p) for p in ['smoke_matrix.json','quality_matrix.json','timing_matrix.json','reserved_seed_table.json','old_input_catalog.json']},'config':CFG,'new_inputs_not_generated_at_freeze':True};write(OUT/'freeze.json',f);print('FROZEN',sha(OUT/'freeze.json'),flush=True)

def gate():
 f=read(OUT/'freeze.json');assert f['sources']==sources();assert sha(OLD/'freeze.json')==CFG['core_freeze_sha256'];assert f['protected_baseline_sha256']==sha(BASELINE)
 for p,h in read(BASELINE)['sha256'].items():assert sha(ROOT/p)==h,'old tracked changed:'+p
 for p,h in f['matrices'].items():assert sha(OUT/p)==h
 return f

def tests():
 gate();assert not (OUT/'tests_report.txt').exists();import subprocess
 p=subprocess.run([sys.executable,str(Path(__file__).with_name('tests.py'))],capture_output=True,text=True);(OUT/'tests_report.txt').write_text(p.stdout+p.stderr);assert p.returncode==0;write(OUT/'tests_receipt.json',{'status':'passed','source_freeze_sha256':sha(OUT/'freeze.json'),'calls':read(OUT/'unit_counts.json')});print('TESTS passed',flush=True)

def compare_smoke(row,old):
 for k in ['best_l','best_pi','starts','finishes','best_makespan','evals_used']:assert row[k]==old[k],('smoke final',k)
 for k in ['candidate_stream_sha256','initial_physical_sha256']:assert row['audit'][k]==old['audit'][k]
 assert [[a,c] for a,b,c in row['trace']]==[[a,c] for a,b,c in old['trace']]
 root=ROOT/row['details']['compact_log_dir']
 with gzip.open(ROOT/old['details']['event_files']['events'],'rt') as f:
  count=0
  for e,line in zip(iter_events(root),f,strict=True):assert e==json.loads(line);count+=1
 assert count==row['evals_used']
 for kind in ['critical_cache','groups','cycles']:
  with gzip.open(ROOT/old['details']['event_files'][kind],'rt') as f:
   for a,line in zip(iter_sidecar(root,kind),f,strict=True):
    b=json.loads(line)
    if kind=='critical_cache':a.pop('t_label_s');b.pop('t_label_s')
    assert a==b,('smoke semantic',kind)
 verify_files(root)

def smoke():
 gate();assert read(OUT/'tests_receipt.json')['status']=='passed';dest=OUT/'smoke';dest.mkdir(exist_ok=True);log=dest/'runs.jsonl';assert not log.exists();items=kernel.load()[0];matrix=read(OUT/'smoke_matrix.json');old={(r['instance_id'],r['method'],r['seed']):r for r in rows(OLD/'smoke/runs.jsonl')};start=time.perf_counter();completed=[]
 for index,t in enumerate(matrix,1):
  r=run_one((t,items[t['instance_id']].data(),{'source_freeze_sha256':sha(OUT/'freeze.json'),'matrix_sha256':sha(OUT/'smoke_matrix.json')},str(dest/'compact')));compare_smoke(r,old[t['instance_id'],t['method'],t['seed']]);append(log,r);completed.append(r);print('smoke',index,'/24',t['instance_id'],t['method'],round(r['t_total_wall_s'],3),flush=True)
 profile={(r['n'],r['method']):r for r in completed};q=read(OUT/'quality_matrix.json');tm=read(OUT/'timing_matrix.json');body_quality=sum(profile[t['n'],t['method']]['t_total_wall_s'] for t in q)/2;body_timing=sum(profile[t['n'],t['method']]['t_total_wall_s'] for t in tm)
 size={(n,m):sum(p.stat().st_size for p in (ROOT/r['details']['compact_log_dir']).iterdir()) for (n,m),r in profile.items()};projection=int(sum(size[t['n'],t['method']] for t in q)*1.5+100000000);free=shutil.disk_usage(ROOT).free;resource={'projected_compressed_bytes_conservative':projection,'free_bytes':free,'required_free_bytes':3*projection+2*2**30,'quality_seconds_conservative':body_quality*1.3,'timing_seconds_conservative_using_logged_smoke_upper_estimate':body_timing*1.3}
 passed=projection<=CFG['compact_projected_bytes_cap'] and free>=resource['required_free_bytes'] and body_quality*1.3<=7200 and body_timing*1.3<=3600
 receipt={'status':'passed' if passed else 'resource-stop','runs':24,'calls':sum(r['evals_used'] for r in completed),'elapsed_s':time.perf_counter()-start,'exact_old_semantics_all24':True,'resource':resource,'compact_bytes':sum(p.stat().st_size for p in (dest/'compact').rglob('*') if p.is_file()),'peak_RSS':max(r['rss_process_highwater_bytes'] for r in completed)};write(OUT/'smoke_receipt.json',receipt);print(kernel.canonical(receipt),flush=True);assert passed,'resource gate failed; preserve and stop before new input generation'

def generate():
 gate();assert read(OUT/'quality_release.json')['authorized'];assert read(OUT/'smoke_receipt.json')['status']=='passed';dest=OUT/'inputs';assert not dest.exists();dest.mkdir();old=read(OUT/'old_input_catalog.json');seen=set(old['physical_hashes']);manifest=[]
 for d in read(OUT/'reserved_seed_table.json'):
  x=synthetic(d['n'],d['shape'],d['generation_seed'],d['instance_id'],'confirmation',d['ccr']);data=x.data();key=physical_input(data);assert key not in seen,'physical duplicate input; no substitution';seen.add(key);path=dest/(x.identifier+'.json');write(path,data);manifest.append({**d,'instance_sha256':x.sha256,'input_file_sha256':sha(path),'physical_input_sha256':key})
 write(OUT/'input_manifest.json',manifest);print('GENERATED36 unique new physical inputs',flush=True)

def new_inputs():
 manifest=read(OUT/'input_manifest.json');items={}
 for d in manifest:
  p=OUT/'inputs'/(d['instance_id']+'.json');assert sha(p)==d['input_file_sha256'];x=kernel.Instance(**read(p));assert x.sha256==d['instance_sha256'] and physical_input(x.data())==d['physical_input_sha256'];items[x.identifier]=x
 return items,manifest

def stage_calls():return sum(read(p)['calls'] for p in [OUT/'smoke_receipt.json',OUT/'quality_receipt.json',OUT/'timing_receipt.json'] if p.exists())

def quality():
 gate();assert read(OUT/'quality_release.json')['authorized'];items,manifest=new_inputs();dest=OUT/'quality';dest.mkdir(exist_ok=True);log=dest/'runs.jsonl';assert not log.exists();matrix=read(OUT/'quality_matrix.json');source={'source_freeze_sha256':sha(OUT/'freeze.json'),'matrix_sha256':sha(OUT/'quality_matrix.json'),'input_manifest_sha256':sha(OUT/'input_manifest.json')};payload=[(t,items[t['instance_id']].data(),source,str(dest/'compact')) for t in matrix];start=time.perf_counter();count=calls=bytes_written=0
 #Bounded submission:atmost2scientificconditions exist in the executor.
 from collections import deque
 ex=ProcessPoolExecutor(max_workers=2);pending=deque();remaining=iter(payload);submitted=[]
 def submit_one():
  try:p=next(remaining)
  except StopIteration:return False
  pending.append((p[0],ex.submit(run_one,p)));submitted.append(p[0]['run_id']);return True
 def consume(r,after_stop=False):
  nonlocal count,calls,bytes_written
  if after_stop:r['completed_after_stop']=True
  append(log,r);count+=1;calls+=r['evals_used'];bytes_written+=sum(p.stat().st_size for p in (ROOT/r['details']['compact_log_dir']).iterdir())
  assert bytes_written<=CFG['compact_projected_bytes_cap'];assert calls+read(OUT/'smoke_receipt.json')['calls']<=CFG['objective_total_cap']
 submit_one();submit_one()
 try:
  while pending:
   task,future=pending.popleft();r=future.result();consume(r)
   projected=read(OUT/'smoke_receipt.json')['resource']['projected_compressed_bytes_conservative'];required=max(0,projected-bytes_written)+2*projected+2*2**30;free=shutil.disk_usage(ROOT).free
   assert free>=required,'runtime disk reserve crossed; preserve partial outputs and stop'
   if count%36==0:print('quality',count,'/4392',round(time.perf_counter()-start,1),'s','free',free,'reserve',required,flush=True)
   submit_one()
 except BaseException as error:
  cleanup=[]
  for task,future in pending:
   if future.cancel():cleanup.append({'run_id':task['run_id'],'status':'cancelled-before-start'})
   else:
    try:late=future.result();consume(late,True);cleanup.append({'run_id':task['run_id'],'status':'already-running-finished-and-preserved','calls':late['evals_used']})
    except BaseException as other:cleanup.append({'run_id':task['run_id'],'status':'worker-failed-preserved','error':repr(other)})
  headers=[]
  for runid in submitted:
   p=dest/'compact'/runid/'header.json'
   if p.exists():headers.append({'run_id':runid,'successful_paid_events':read(p)['events']})
  write(OUT/'quality_failure_receipt.json',{'error':repr(error),'completed_summaries':count,'accounted_summary_calls':calls,'submitted':len(submitted),'cleanup':cleanup,'successful_event_counts_in_all_submitted_headers':headers,'no_retry_or_new_submission_after_error':True});raise
 finally:ex.shutdown(wait=True,cancel_futures=True)

 assert count==4392;receipt={'status':'completed','runs':count,'calls':calls,'elapsed_s':time.perf_counter()-start,'compact_bytes':bytes_written,'input_manifest_sha256':source['input_manifest_sha256']};write(OUT/'quality_receipt.json',receipt);print(kernel.canonical(receipt),flush=True)

def timing():
 gate();assert read(OUT/'timing_release.json')['authorized'];assert read(OUT/'quality_receipt.json')['status']=='completed';items,manifest=new_inputs();reference={(r['instance_id'],r['method']):r for r in rows(OUT/'quality/runs.jsonl') if r['seed'] in [0,None]};dest=OUT/'timing';dest.mkdir(exist_ok=True);log=dest/'runs.jsonl';assert not log.exists();matrix=read(OUT/'timing_matrix.json');calls=0;start=time.perf_counter()
 for index,t in enumerate(matrix,1):
  r=run_one((t,items[t['instance_id']].data(),{'source_freeze_sha256':sha(OUT/'freeze.json'),'matrix_sha256':sha(OUT/'timing_matrix.json')},None));ref=reference[t['instance_id'],t['method']]
  for k in ['best_l','best_pi','starts','finishes','best_makespan','evals_used']:assert r[k]==ref[k]
  assert r['details']['physical_score_sha256']==ref['details']['physical_score_sha256'] and r['audit']['candidate_stream_sha256']==ref['audit']['candidate_stream_sha256'];assert [[a,c] for a,b,c in r['trace']]==[[a,c] for a,b,c in ref['trace']];append(log,r);calls+=r['evals_used'];assert calls+stage_calls()<=50000000
  if index%16==0:print('timing',index,'/576',round(time.perf_counter()-start,1),'s',flush=True)
 receipt={'status':'completed','runs':576,'warmups':96,'formal_repeats':480,'calls':calls,'elapsed_s':time.perf_counter()-start,'all_quality_seed0_hashes_exact':True,'no_concurrent_quality_or_validator':True};write(OUT/'timing_receipt.json',receipt);print(kernel.canonical(receipt),flush=True)

if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('stage',choices=['freeze','tests','smoke','generate','quality','timing','verify','analyze']);a=p.parse_args()
 if a.stage in ['freeze','tests','smoke','generate','quality','timing']:globals()[a.stage]()
 else:
  from report import verify,analyze
  globals()[a.stage]()
