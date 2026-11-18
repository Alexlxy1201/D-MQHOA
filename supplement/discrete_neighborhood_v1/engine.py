from __future__ import annotations
import sys,json,math,time,hashlib,gzip,copy,resource,os
from pathlib import Path
from collections import Counter
import numpy as np
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'supplement'))
from algorithm_improvement_v2 import engine as frozen
from neighborhood_diagnostic_v1.engine import critical_labels
from mqhoa_supp.model import Instance,canonical,digest,schedule,validate,check_solution
CFG=json.loads(Path(__file__).with_name('config.json').read_text())
OUT=ROOT/'supplement/results/20261109_discrete_neighborhood_v1'

def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def read(p):return json.loads(Path(p).read_text())
def write(p,x):Path(p).write_text(canonical(x)+'\n',encoding='utf8')
def rows(p):return [json.loads(s) for s in Path(p).read_text().splitlines() if s] if Path(p).exists() else []
def append(p,x):
 with Path(p).open('a',encoding='utf8') as f:f.write(canonical(x)+'\n');f.flush();os.fsync(f.fileno())
def physical_key(l,p):return np.asarray(l,dtype=np.int8).tobytes()+np.asarray(p,dtype=np.int32).tobytes()
def physical_sha(l,p):return digest([list(l),list(p)])
def load():return frozen.load()

class Sink:
 def __init__(self,path=None):
  self.path=path;self.data=[] if path is None else None;self.handle=None
  if path is not None:Path(path).parent.mkdir(parents=True,exist_ok=True);assert not Path(path).exists();self.handle=gzip.open(path,'wt',encoding='utf8',compresslevel=CFG['gzip_compresslevel'])
 def emit(self,row):
  if self.handle:self.handle.write(canonical(row)+'\n')
  else:self.data.append(copy.deepcopy(row))
 def close(self):
  if self.handle:self.handle.close()

class RecordedBudget(frozen.AuditBudget):
 def __init__(self,inst,limit,sink=None):super().__init__(inst,limit);self.sink=sink or Sink();self.context=None;self.new_globals=self.meaningful_globals=self.tiny_globals=0;self.t_event_logging_s=0.
 def evaluate(self,l,p):
  before=self.best;duplicate=physical_key(l,p) in self.seen;v=super().evaluate(l,p);delta=None if not math.isfinite(before) else before-v;updated=v<before;meaningful=delta is not None and delta>CFG['strict_tolerance'];tiny=delta is not None and 0<delta<=CFG['strict_tolerance'];self.new_globals+=updated;self.meaningful_globals+=meaningful;self.tiny_globals+=tiny
  if self.context is not None and self.context['type']=='ordinary':
   self.context['stale_after']=0 if v<self.context['parent_fitness']-CFG['strict_tolerance'] else self.context['stale_before']+1;self.context['restart_pending_after']=self.context['stale_after']>=2*self.instance.n
  log_tic=time.perf_counter();self.sink.emit({'eval_index':self.used,'loc':list(l),'order':list(p),'candidate_value':v,'phase':self.phase,'global_best_before':None if not math.isfinite(before) else before,'global_best_after':self.best,'global_updated':updated,'meaningful_global_improvement':meaningful,'tiny_float_global_improvement':tiny,'duplicate':duplicate,'physical_sha256':physical_sha(l,p),'proposal':self.context})
  if self.sink.handle and self.used%CFG['event_flush_every']==0:self.sink.handle.flush()
  self.t_event_logging_s+=time.perf_counter()-log_tic
  return v

class CriticalCache:
 def __init__(self,inst,sink):self.inst=inst;self.sink=sink;self.cache={};self.hits=0;self.seconds=0.;self.calls=0;self.lookup_seconds=0.;self.lookups=0
 def get(self,l,p,value,paid_eval):
  lookup_tic=time.perf_counter();self.lookups+=1;key=physical_key(l,p)
  if key in self.cache:
   self.hits+=1;row=self.cache[key];assert row['objective']==value;self.lookup_seconds+=time.perf_counter()-lookup_tic;return row
  self.lookup_seconds+=time.perf_counter()-lookup_tic;tic=time.perf_counter();labels=critical_labels(self.inst,l,p,value);critical=[r['node'] for r in labels if r['critical']];elapsed=time.perf_counter()-tic;self.seconds+=elapsed;self.calls+=1
  row={'cache_id':self.calls,'source_paid_eval':paid_eval,'physical_sha256':physical_sha(l,p),'objective':value,'critical_tasks':critical,'critical_tasks_sha256':digest(critical),'t_label_s':elapsed};self.cache[key]=row;self.sink.emit(row);return row

def atomic(inst,l,p,critical,rng,reach,stats):
 stats['total_atomic_steps']+=1;tic=time.perf_counter();free=[j for j in range(inst.n) if j not in inst.pinned];swaps=frozen.legal_swaps(p,reach);stats['legal_enumerations']+=1;stats['t_legal_enumeration_s']+=time.perf_counter()-tic;families=(['flip'] if free else [])+(['swap'] if swaps else []);ll,pp=list(l),list(p)
 if not families:
  stats['atomic_noop']+=1;return ll,pp,{'kind':'noop','nodes':[],'swap_position':None,'family_count':0,'critical_coin':None,'critical_pool_size':0,'selected_from_critical':False}
 kind=families[int(rng.integers(len(families)))];allactions=free if kind=='flip' else swaps;preferred=[j for j in allactions if j in critical] if kind=='flip' else [j for j in allactions if p[j] in critical or p[j+1] in critical];coin=float(rng.random());use=coin<.5 and bool(preferred);pool=preferred if use else allactions;j=pool[int(rng.integers(len(pool)))];nodes=[j] if kind=='flip' else list(p[j:j+2])
 if kind=='flip':ll[j]=1-ll[j]
 else:pp[j],pp[j+1]=pp[j+1],pp[j]
 stats['atomic_'+kind]+=1;stats['critical_requested']+=coin<.5;stats['critical_subset_used']+=use
 return ll,pp,{'kind':kind,'nodes':nodes,'swap_position':j if kind=='swap' else None,'family_count':len(families),'critical_coin':coin,'critical_pool_size':len(preferred),'selected_from_critical':bool(use)}

def chain(inst,parent,critical,L,rng,reach,stats):
 tic=time.perf_counter();l,p=list(parent['loc']),list(parent['order']);seen={physical_key(l,p)};moves=[];revisits=0
 for _ in range(L):
  l,p,move=atomic(inst,l,p,critical,rng,reach,stats);moves.append(move);key=physical_key(l,p);revisits+=key in seen;seen.add(key)
 stats['t_chain_s']+=time.perf_counter()-tic
 return l,p,moves,revisits,physical_key(l,p)==physical_key(parent['loc'],parent['order'])

def distance(centers,n):
 if len(centers)==1:return 0.
 vals=[(sum(a!=b for a,b in zip(x['loc'],y['loc']))+sum(a!=b for a,b in zip(x['order'],y['order'])))/(2*n) for i,x in enumerate(centers) for y in centers[i+1:]];return sum(vals)/len(vals)

def length(rng,sigma,R,step=False):
 if step:return 1,None
 u1=max(float(rng.random()),np.finfo(float).tiny);u2=float(rng.random());z=sigma*math.sqrt(-2*math.log(u1))*math.sin(2*math.pi*u2);return min(R,max(1,math.ceil(abs(z)))),z

def discrete(inst,ev,rng,method,cache_sink,group_sink,cycle_sink):
 k,m=(1,20) if method=='D_MQ_OneCenter' else (4,5);R=max(2,math.ceil(.1*inst.n));sigma=float(R);eps=CFG['strict_tolerance'];decoder=frozen.ReadyDecoder(inst);initial,fitness,meta=frozen.init_heft(inst,ev,rng,decoder);indices=list(map(int,np.argsort(fitness,kind='stable')[:k]));paid=ev.initial;cache=CriticalCache(inst,cache_sink);heftstate={'loc':paid[0]['loc'],'order':paid[0]['order'],'value':paid[0]['makespan'],'eval':1};hc=cache.get(heftstate['loc'],heftstate['order'],heftstate['value'],1)
 centers=[]
 for j in indices:
  q={'loc':list(paid[j]['loc']),'order':list(paid[j]['order']),'value':float(fitness[j]),'eval':j+1};q['critical']=cache.get(q['loc'],q['order'],q['value'],q['eval']);centers.append(q)
 reach=frozen.reachability(inst);stale=[0]*k;pending=[False]*k;stats=Counter();stats['t_legal_enumeration_s']=0.;cycle=groupno=0;reductions=0;ev.phase='discrete_proposal'
 def close_group(parent,c,proposals,stop,gid):
  best=min(proposals,key=lambda q:q['value']);accepted=best['value']<parent['value']-eps
  if accepted:
   new=dict(best);new['critical']=cache.get(new['loc'],new['order'],new['value'],new['eval']);centers[c]=new;stats['center_accepts']+=1
  else:stats['center_rejects']+=1
  group_sink.emit({'group_id':gid,'cycle':cycle,'center':c,'parent_eval':parent['eval'],'parent_fitness':parent['value'],'proposal_evals':[q['eval'] for q in proposals],'best_eval':best['eval'],'best_value':best['value'],'after_center_eval':centers[c]['eval'],'strict_update':accepted,'stop_reason':stop,'stale':stale[c],'restart_pending':pending[c],'cache_objective_calls':0})
 while ev.remaining:
  cycle+=1;complete=True
  for c in range(k):
   slot=0
   while slot<m and ev.remaining:
    if pending[c]:
     slot+=1;before=stale[c];prior=centers[c];l,p,moves,revisits,cancel=chain(inst,heftstate,set(hc['critical_tasks']),1,rng,reach,stats);stale[c]=0;pending[c]=False;ev.phase='discrete_restart';ev.context={'type':'restart','cycle':cycle,'center':c,'cycle_slot':slot,'group_id':None,'parent_eval':1,'parent_fitness':heftstate['value'],'parent_sha256':physical_sha(heftstate['loc'],heftstate['order']),'replaced_center_eval':prior['eval'],'critical_cache_id':hc['cache_id'],'critical_tasks_sha256':hc['critical_tasks_sha256'],'L':1,'R':R,'sigma_before':sigma,'z':None,'moves':moves,'chain_state_revisits':revisits,'returns_to_parent':cancel,'stale_before':before,'stale_after':0,'restart_pending_after':False};v=ev.evaluate(l,p);new={'loc':l,'order':p,'value':v,'eval':ev.used};new['critical']=cache.get(l,p,v,ev.used);centers[c]=new;stats['restarts']+=1;stats['restart_edit_steps']+=1;stats['chain_state_revisits']+=revisits;stats['returns_to_parent']+=cancel;continue
    parent=centers[c];proposals=[];groupno+=1;gid=groupno
    while slot<m and ev.remaining and not pending[c]:
     slot+=1;L,z=length(rng,sigma,R,method=='D_Step');l,p,moves,revisits,cancel=chain(inst,parent,set(parent['critical']['critical_tasks']),L,rng,reach,stats);before=stale[c]
     # One scoring call; stale labels are filled from its returned fitness.
     context={'type':'ordinary','cycle':cycle,'center':c,'cycle_slot':slot,'group_id':gid,'parent_eval':parent['eval'],'parent_fitness':parent['value'],'parent_sha256':physical_sha(parent['loc'],parent['order']),'critical_cache_id':parent['critical']['cache_id'],'critical_tasks_sha256':parent['critical']['critical_tasks_sha256'],'L':L,'R':R,'sigma_before':sigma,'z':z,'moves':moves,'chain_state_revisits':revisits,'returns_to_parent':cancel,'stale_before':before}
     # Observer callback computes exact labels before serialization, without scoring.
     ev.context=context;ev.phase='discrete_proposal';v=ev.evaluate(l,p);stale[c]=0 if v<parent['value']-eps else before+1;pending[c]=stale[c]>=2*inst.n;proposals.append({'loc':l,'order':p,'value':v,'eval':ev.used});stats['ordinary_proposals']+=1;stats['edit_steps']+=L;stats['chain_state_revisits']+=revisits;stats['returns_to_parent']+=cancel
    stop='stagnation_trigger' if pending[c] else 'budget' if not ev.remaining and slot<m else 'slots_end';close_group(parent,c,proposals,stop,gid)
   if slot<m:complete=False
   if not ev.remaining:
    if c<k-1:complete=False
    break
  if complete:
   d=distance(centers,inst.n);h=1+(R-1)*d;before=sigma
   if method!='D_MQ_Fixed' and h<sigma:sigma=max(1.,sigma/CFG['lambda']);reductions+=sigma<before
   cycle_sink.emit({'cycle':cycle,'distance':d,'h':h,'sigma_before':before,'sigma_after':sigma,'updated':sigma<before,'center_evals':[q['eval'] for q in centers],'last_eval':ev.used,'completed_cycle':True,'k':k,'m':m})
  else:cycle_sink.emit({'cycle':cycle,'distance':None,'h':None,'sigma_before':sigma,'sigma_after':sigma,'updated':False,'center_evals':[q['eval'] for q in centers],'last_eval':ev.used,'completed_cycle':False,'k':k,'m':m})
 return {**meta,'initial_coordinates_sha256':digest(initial.tolist()),'initial_center_indices':indices,'k':k,'m':m,'R':R,'sigma_final':sigma,'scale_reductions':reductions,'cycles':cycle,'critical_calls':cache.calls,'critical_cache_hits':cache.hits,'critical_label_s':cache.seconds,'critical_cache_lookup_s':cache.lookup_seconds,'critical_cache_lookups':cache.lookups,'ready_initialization_decode_s':decoder.seconds,'stats':dict(stats),'center_final_paid_evals':[q['eval'] for q in centers],'center_stale':stale,'center_restart_pending':pending}

def run_one(payload):
 task,data,source,event_dir=payload;inst=Instance(**data);tic=time.perf_counter();method=task['method'];event_dir=Path(event_dir);runid=task['run_id'];paths={name:event_dir/(runid+'.'+name+'.jsonl.gz') for name in ['events','critical_cache','groups','cycles']};sinks={name:Sink(path) for name,path in paths.items()};ev=RecordedBudget(inst,task['budget'],sinks['events']);rng=np.random.default_rng(task['seed']);details={}
 try:
  if method in CFG['new_methods']:details=discrete(inst,ev,rng,method,sinks['critical_cache'],sinks['groups'],sinks['cycles'])
  elif method=='PSO_ReadyKeyHEFT':details=frozen.ready_pso(inst,ev,rng,task['pso_parameters'])
  elif method=='LS_HEFT':details=frozen.local_search(inst,ev,rng)
  elif method=='HEFT':_,_,details=frozen.heft_seed(inst,ev,encode=False)
  elif method=='FlexDO_N2':
   start=time.perf_counter();decisions,metadata=frozen.flex_candidates(inst,'N2');assert len(decisions)<=ev.limit;details={'flex_candidates':metadata,'flex_generation_s':time.perf_counter()-start};ev.phase='flex_candidate'
   for l,_ in decisions:ev.evaluate(l,frozen.critical_order(inst,l))
  else:raise ValueError(method)
  result=ev.result();audit=ev.accounting();assert not validate(inst,result['best_l'],result['best_pi'],result['starts'],result['finishes'])
  if method in CFG['stochastic_methods']:assert ev.used==task['budget']
 finally:
  for sink in sinks.values():sink.close()
 details['event_files']={key:str(p.relative_to(ROOT)) for key,p in paths.items()};details['event_file_sha256']={key:sha(p) for key,p in paths.items()};details['raw_global_updates']=ev.new_globals;details['meaningful_global_updates']=ev.meaningful_globals;details['tiny_global_updates']=ev.tiny_globals
 return {**task,**source,**result,'audit':audit,'details':details,'instance_sha256':inst.sha256,'t_total_wall_s':time.perf_counter()-tic,'t_candidate_checks_s':ev.t_checks,'t_accounting_s':ev.t_accounting,'t_event_logging_s':ev.t_event_logging_s,'rss_process_highwater_bytes':frozen.peak_rss_bytes(),'memory_scope':task['memory_scope'],'gzip_level':CFG['gzip_compresslevel'],'status':'completed'}
