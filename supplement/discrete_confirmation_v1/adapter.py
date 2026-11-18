from pathlib import Path
import sys,time,json,hashlib,struct
import numpy as np
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'supplement'))
from discrete_neighborhood_v1 import engine as kernel
from codec import EventWriter,SideWriter
CFG=json.loads(Path(__file__).with_name('config.json').read_text())
OUT=ROOT/'supplement/results/20261109_discrete_confirmation_v1'

def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def read(p):return json.loads(Path(p).read_text())
def write(p,x):Path(p).write_text(kernel.canonical(x)+'\n',encoding='utf8')
def rows(p):return [json.loads(x) for x in Path(p).read_text().splitlines() if x] if Path(p).exists() else []
def append(p,x):
 import os
 with Path(p).open('a',encoding='utf8') as f:f.write(kernel.canonical(x)+'\n');f.flush();os.fsync(f.fileno())

class NullEvents:
 handle=None
 def __init__(self):self.total=0;self.score_hash=hashlib.sha256()
 def emit(self,e):self.total+=1;self.score_hash.update(kernel.physical_key(e['loc'],e['order'])+struct.pack('<d',e['candidate_value']))
 def close(self):self.header={'events':self.total,'physical_score_sha256':self.score_hash.hexdigest()}
class NullSide:
 handle=None
 def emit(self,row):pass
 def close(self):pass

def run_one(payload):
 task,data,source,logroot=payload;inst=kernel.Instance(**data);start=time.perf_counter();persist=logroot is not None
 if persist:
  root=Path(logroot)/task['run_id'];event=EventWriter(root,inst.n,CFG['event_chunk_size']);side={k:SideWriter(root,k,inst.n) for k in ['critical_cache','groups','cycles']}
 else:root=None;event=NullEvents();side={k:NullSide() for k in ['critical_cache','groups','cycles']}
 ev=kernel.RecordedBudget(inst,task['budget'],event);rng=np.random.default_rng(task['seed']);method=task['method']
 try:
  if method in kernel.CFG['new_methods']:details=kernel.discrete(inst,ev,rng,method,side['critical_cache'],side['groups'],side['cycles'])
  elif method=='PSO_ReadyKeyHEFT':details=kernel.frozen.ready_pso(inst,ev,rng,task['pso_parameters'])
  elif method=='LS_HEFT':details=kernel.frozen.local_search(inst,ev,rng)
  elif method=='HEFT':_,_,details=kernel.frozen.heft_seed(inst,ev,encode=False)
  elif method=='FlexDO_N2':
   tic=time.perf_counter();decisions,metadata=kernel.frozen.flex_candidates(inst,'N2');assert len(decisions)<=ev.limit;details={'flex_candidates':metadata,'flex_generation_s':time.perf_counter()-tic};ev.phase='flex_candidate'
   for l,_ in decisions:ev.evaluate(l,kernel.frozen.critical_order(inst,l))
  else:raise ValueError(method)
  result=ev.result();audit=ev.accounting();assert not kernel.validate(inst,result['best_l'],result['best_pi'],result['starts'],result['finishes'])
  if method in CFG['stochastic_methods']:assert ev.used==10000
 finally:
  event.close()
  for sink in side.values():sink.close()
 details.update(physical_score_sha256=event.header['physical_score_sha256'],compact_log_dir=str(root.relative_to(ROOT)) if persist else None,raw_global_updates=ev.new_globals,meaningful_global_updates=ev.meaningful_globals,tiny_global_updates=ev.tiny_globals)
 return {**task,**source,**result,'audit':audit,'details':details,'instance_sha256':inst.sha256,'t_total_wall_s':time.perf_counter()-start,'t_candidate_checks_s':ev.t_checks,'t_accounting_s':ev.t_accounting,'t_event_logging_s':ev.t_event_logging_s,'rss_process_highwater_bytes':kernel.frozen.peak_rss_bytes(),'logging':'compact-lossless' if persist else 'null-sink-with-full-scientific-path-and-physical-score-hash','status':'completed'}
