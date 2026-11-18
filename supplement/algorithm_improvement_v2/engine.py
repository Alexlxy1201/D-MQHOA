from __future__ import annotations
import sys,json,hashlib,time,math,heapq,platform,resource,zipfile
from pathlib import Path
from collections import Counter
import numpy as np
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'supplement'))
from mqhoa_supp.model import Instance,canonical,digest,schedule,validate,check_solution
from mqhoa_supp.methods import Budget,heft
from recent_baseline.flexdo import candidates as flex_candidates,critical_order
from extended_validation.search.engine import direct_geometry
CFG=json.loads(Path(__file__).with_name('config.json').read_text())
OUT=ROOT/'supplement/results/20261109_algorithm_improvement_v2'
SOURCE=ROOT/'supplement/results/20261108_v1/raw/quality_dataset.zip'

def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def read(p):return json.loads(Path(p).read_text())
def write(p,x):Path(p).write_text(canonical(x)+'\n',encoding='utf8')
def records(p):return [json.loads(l) for l in Path(p).read_text().splitlines() if l] if Path(p).exists() else []
def append(p,x):
 import os
 with Path(p).open('a',encoding='utf8') as f:f.write(canonical(x)+'\n');f.flush();os.fsync(f.fileno())

def load():
 with zipfile.ZipFile(SOURCE) as z:
  config=json.loads(z.read('protocol.json'));frozen=json.loads(z.read('frozen_defaults.json'))
  items={x.identifier:x for n in z.namelist() if n.startswith('instances/') and n.endswith('.json') for x in [Instance(**json.loads(z.read(n)))] if x.metadata.get('split')!='tuning'}
 assert len(items)==14 and config['mqhoa']==CFG['mqhoa'];assert frozen['PSO']['population']==20
 return items,config,frozen

def ready_decode(inst,x):
 if len(x)!=2*inst.n or not np.all(np.isfinite(x)):raise ValueError('invalid coordinate vector')
 loc=(np.asarray(x[:inst.n])>=CFG['position_threshold']).astype(int).tolist()
 for p in inst.pinned:loc[p]=0
 indeg=[len(p) for p in inst.pre];ready=[(float(x[inst.n+j]),j) for j in range(inst.n) if indeg[j]==0];heapq.heapify(ready);order=[]
 while ready:
  _,j=heapq.heappop(ready);order.append(j)
  for child,_ in inst.succ[j]:
   indeg[child]-=1
   if indeg[child]==0:heapq.heappush(ready,(float(x[inst.n+child]),child))
 if len(order)!=inst.n:raise ValueError('incomplete ready order')
 return loc,order

def ready_encode(inst,loc,order):
 check_solution(inst,loc,order);x=np.empty(2*inst.n);x[:inst.n]=[.75 if p else .25 for p in loc]
 for rank,j in enumerate(order):x[inst.n+j]=(rank+1)/(inst.n+1)
 if ready_decode(inst,x)!=(list(loc),list(order)):raise RuntimeError('HEFT roundtrip changed physical schedule')
 return x

class AuditBudget(Budget):
 def __init__(self,inst,limit,auto_initial=0):
  super().__init__(inst,limit,CFG['trace_every']);self.phase='search';self.auto_initial=auto_initial;self.calls=0;self.phases=Counter();self.seen=set();self.duplicates=0;self.initial=[];self.stream=hashlib.sha256();self.t_checks=0.;self.t_accounting=0.;self.heft_reference=None;self.first_heft_improvement=None;self.best_origin_phase=None;self.best_evaluation=None
 def evaluate(self,loc,order):
  tic=time.perf_counter();check_solution(self.instance,loc,order);self.t_checks+=time.perf_counter()-tic
  if self.remaining<=0:raise RuntimeError('budget exhausted')
  tic=time.perf_counter();key=np.asarray(loc,dtype=np.int8).tobytes()+np.asarray(order,dtype=np.int32).tobytes();duplicate=key in self.seen;self.seen.add(key);self.duplicates+=duplicate;self.stream.update(key)
  phase='random_initialization' if self.auto_initial and self.used<self.auto_initial else self.phase;self.calls+=1;self.phases[phase]+=1;self.t_accounting+=time.perf_counter()-tic
  previous_best=self.best;value=super().evaluate(loc,order)
  if value<previous_best:self.best_origin_phase=phase;self.best_evaluation=self.used
  if self.heft_reference is not None and self.first_heft_improvement is None and value<self.heft_reference-CFG['gate_strict_tolerance']:self.first_heft_improvement=self.used
  if phase in ['heft_initialization','random_initialization']:
   self.initial.append({'loc':list(loc),'order':list(order),'makespan':value,'phase':phase})
  return value
 def accounting(self):
  if self.calls!=self.used or sum(self.phases.values())!=self.used:raise RuntimeError('evaluation accounting mismatch')
  return {'calls':self.calls,'phase_evaluations':dict(self.phases),'duplicate_evaluations':self.duplicates,'distinct_physical_candidates':len(self.seen),'candidate_stream_sha256':self.stream.hexdigest(),'best_origin_phase':self.best_origin_phase,'best_evaluation':self.best_evaluation,'first_strict_heft_improvement_eval':self.first_heft_improvement,'initial_physical_candidates':self.initial,'initial_physical_sha256':digest([[x['loc'],x['order']] for x in self.initial])}

class ReadyDecoder:
 def __init__(self,inst):self.inst=inst;self.seconds=0.;self.calls=0
 def __call__(self,x):
  tic=time.perf_counter();value=ready_decode(self.inst,x);self.seconds+=time.perf_counter()-tic;self.calls+=1;return value

def heft_seed(inst,ev,encode=True):
 tic=time.perf_counter();ev.phase='heft_initialization';before=ev.t_eval;details=heft(inst,ev);elapsed=time.perf_counter()-tic
 loc,order,starts,finishes=ev.solution;loc,order,starts,finishes=map(list,(loc,order,starts,finishes));value=ev.best;ev.heft_reference=value
 tic=time.perf_counter();coords=ready_encode(inst,loc,order) if encode else None;encoding=time.perf_counter()-tic if encode else 0.
 tic=time.perf_counter();bad=validate(inst,loc,order,starts,finishes);validation=time.perf_counter()-tic
 if bad:raise RuntimeError('invalid HEFT seed')
 physical={'loc':loc,'order':order};full={**physical,'starts':starts,'finishes':finishes,'makespan':value}
 return coords,value,{'heft_start':full,'heft_start_sha256':digest(full),'heft_physical_sha256':digest(physical),'heft_construct_including_first_evaluation_s':elapsed,'heft_first_objective_s':ev.t_eval-before,'heft_encoding_roundtrip_s':encoding,'heft_seed_validation_s':validation,'heft_readykey_encoding_performed':bool(encode),'heft_construct_calls':1,'heft_objective_evaluations':1,'heft_details':details}

def init_heft(inst,ev,rng,decoder):
 h,value,meta=heft_seed(inst,ev);initial=np.vstack([h,rng.random((19,2*inst.n))]);fitness=[value];ev.phase='random_initialization'
 for x in initial[1:]:fitness.append(ev.evaluate(*decoder(x)))
 return initial,np.asarray(fitness),meta

def elitist_center_update(centers,fitness,index,proposal_value,proposal_coordinates):
 # Both values were already paid for by initialization or candidate evaluation.
 # No objective/evaluator is called by this cache-only state update.
 if proposal_value<float(fitness[index]):
  centers[index]=proposal_coordinates;fitness[index]=proposal_value;return True
 return False

def ready_mq(inst,ev,rng,seeded=False,fixed=False):
 decoder=ReadyDecoder(inst);params=CFG['mqhoa'];k,m=params['k'],params['m'];d=2*inst.n
 if seeded:
  initial,fitness,meta=init_heft(inst,ev,rng,decoder);indices=[0]+[int(j) for j in np.argsort(fitness[1:],kind='stable')[:k-1]+1]
 else:
  initial=rng.random((k*m,d));ev.phase='random_initialization';fitness=np.asarray([ev.evaluate(*decoder(x)) for x in initial]);indices=[int(j) for j in np.argsort(fitness,kind='stable')[:k]];meta={}
 centers=initial[indices].copy();center_fitness=fitness[indices].copy();sigma=.5;iterations=reductions=0;trajectory=[];ev.phase='search';accepts=rejects_equal=rejects_worse=0;first_cycle=[];accepts_per_center=[0]*k;rejects_per_center=[0]*k;cache_extra_evaluations=0
 while ev.remaining:
  candidates=[]
  for c in range(k):
   group=[]
   for _ in range(min(m,ev.remaining)):
    u=rng.random((2,d));z=np.sqrt(-2*np.log(np.maximum(u[0],np.finfo(float).tiny)))*np.sin(2*np.pi*u[1]);x=np.clip(centers[c]+sigma*z,0,1);loc,order=decoder(x);v=ev.evaluate(loc,order);item=(v,x,loc,order);group.append(item);candidates.append(item)
   if group:
    proposal=min(group,key=lambda q:q[0]);before=float(center_fitness[c]);used_before=ev.used;accepted=elitist_center_update(centers,center_fitness,c,proposal[0],proposal[1]);cache_extra_evaluations+=ev.used-used_before
    if accepted:accepts+=1;accepts_per_center[c]+=1
    else:
     rejects_per_center[c]+=1
     if proposal[0]==before:rejects_equal+=1
     else:rejects_worse+=1
    if iterations==0:first_cycle.append({'center':c,'before_fitness':before,'proposal_group_best_fitness':proposal[0],'after_fitness':float(center_fitness[c]),'accepted':accepted,'objective_evaluations_for_cache_update':ev.used-used_before})
   if not ev.remaining:break
  best=min(candidates,key=lambda q:q[0]);h=max(sum(a!=b for a,b in zip(q[2],best[2]))+sum(a!=b for a,b in zip(q[3],best[3])) for q in candidates)/(2*inst.n)
  if not fixed and h<sigma:sigma/=params['lambda'];reductions+=1
  iterations+=1
  if iterations==1 or iterations%10==0 or not ev.remaining:trajectory.append([ev.used,sigma,h])
 return {**meta,'iterations':iterations,'scale_reductions':reductions,'sigma_final':sigma,'sigma_trajectory':trajectory,'initial_coordinates_sha256':digest(initial.tolist()),'initial_center_indices':indices,'initial_center_contains_heft':bool(seeded and 0 in indices),'ready_decode_calls':decoder.calls,'ready_decode_s':decoder.seconds,'center_update_accepts':accepts,'center_update_rejects_equal':rejects_equal,'center_update_rejects_worse':rejects_worse,'center_accepts_per_center':accepts_per_center,'center_rejects_per_center':rejects_per_center,'center_first_cycle':first_cycle,'center_final_cached_fitness':center_fitness.tolist(),'center_cache_extra_objective_evaluations':cache_extra_evaluations}

def ready_pso(inst,ev,rng,params):
 decoder=ReadyDecoder(inst);x,fitness,meta=init_heft(inst,ev,rng,decoder);initial_hash=digest(x.tolist());pop=len(x);vmax=params['velocity_fraction'];velocity=rng.uniform(-vmax,vmax,x.shape);pbest=x.copy();pfit=fitness.copy();index=int(np.argmin(fitness));gbest=x[index].copy();gfit=float(fitness[index]);ev.phase='search';iterations=0
 while ev.remaining:
  velocity=params['inertia']*velocity+params['c1']*rng.random(x.shape)*(pbest-x)+params['c2']*rng.random(x.shape)*(gbest-x);velocity=np.clip(velocity,-vmax,vmax);proposed=x+velocity;hit=(proposed<0)|(proposed>1);x=np.clip(proposed,0,1);velocity[hit]=0
  for i in range(min(pop,ev.remaining)):
   val=ev.evaluate(*decoder(x[i]))
   if val<pfit[i]:pfit[i]=val;pbest[i]=x[i].copy()
   if val<gfit:gfit=val;gbest=x[i].copy()
  iterations+=1
 return {**meta,'iterations':iterations,'initial_coordinates_sha256':initial_hash,'ready_decode_calls':decoder.calls,'ready_decode_s':decoder.seconds,'pso_parameters':params}

def reachability(inst):
 reach=[set() for _ in range(inst.n)]
 for j in reversed(inst.topo):
  for child,_ in inst.succ[j]:reach[j].add(child);reach[j].update(reach[child])
 return reach

def legal_swaps(order,reach):return [j for j in range(len(order)-1) if order[j+1] not in reach[order[j]] and order[j] not in reach[order[j+1]]]
def perturb(inst,loc,order,rng,reach):
 free=[i for i in range(inst.n) if i not in inst.pinned];swaps=legal_swaps(order,reach);families=(['flip'] if free else [])+(['swap'] if swaps else []);l,p=list(loc),list(order)
 if not families:return l,p,'noop'
 kind=families[int(rng.integers(len(families)))]
 if kind=='flip':j=free[int(rng.integers(len(free)))];l[j]=1-l[j]
 else:j=swaps[int(rng.integers(len(swaps)))];p[j],p[j+1]=p[j+1],p[j]
 return l,p,kind

def local_search(inst,ev,rng):
 _,current,meta=heft_seed(inst,ev,encode=False);h=meta['heft_start'];loc,order=list(h['loc']),list(h['order']);reach=reachability(inst);stale=restarts=accepted=0;moves=Counter()
 while ev.remaining:
  restart=stale>=2*inst.n;base_l,base_p=(h['loc'],h['order']) if restart else (loc,order);l,p,kind=perturb(inst,base_l,base_p,rng,reach);ev.phase='restart_perturbation' if restart else 'local_'+kind;value=ev.evaluate(l,p);moves[kind]+=1
  if restart:restarts+=1;loc,order,current=l,p,value;stale=0
  elif value<current:accepted+=1;loc,order,current=l,p,value;stale=0
  else:stale+=1
 return {**meta,'restarts':restarts,'accepted_strict_improvements':accepted,'perturbation_move_counts':dict(moves),'stale_at_end':stale}

def peak_rss_bytes():
 value=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss;return int(value if sys.platform=='darwin' else value*1024)

def run_one(payload):
 task,data,source=payload;inst=Instance(**data);tic=time.perf_counter();rss_start=peak_rss_bytes();method=task['method'];ev=AuditBudget(inst,task['budget'],20 if method=='A_DirectGeometry' else 0);rng=np.random.default_rng(task['seed']);details={}
 if method=='A_DirectGeometry':details=direct_geometry(inst,ev,rng,CFG['mqhoa'])
 elif method in ['B_ReadyKey','C_ReadyKeyHEFT','C_fixed']:details=ready_mq(inst,ev,rng,method!='B_ReadyKey',method=='C_fixed')
 elif method=='PSO_ReadyKeyHEFT':details=ready_pso(inst,ev,rng,task['pso_parameters'])
 elif method=='LS_HEFT':details=local_search(inst,ev,rng)
 elif method=='HEFT':_,_,details=heft_seed(inst,ev,encode=False)
 elif method=='FlexDO_N2':
  start=time.perf_counter();decisions,metadata=flex_candidates(inst,'N2');details={'flex_candidates':metadata,'flex_generation_s':time.perf_counter()-start};assert len(decisions)<=ev.limit;ev.phase='flex_candidate';decode_time=0.
  for loc,_ in decisions:
   start=time.perf_counter();order=critical_order(inst,loc);decode_time+=time.perf_counter()-start;ev.evaluate(loc,order)
  details['flex_critical_order_s']=decode_time
 else:raise ValueError(method)
 before=time.perf_counter();result=ev.result();account=ev.accounting();errors=validate(inst,result['best_l'],result['best_pi'],result['starts'],result['finishes']);final_check=time.perf_counter()-before
 if errors:raise RuntimeError(errors)
 if method in CFG['stochastic_methods'] and ev.used!=task['budget']:raise RuntimeError('search did not consume exact budget')
 if method=='HEFT' and ev.used!=1:raise RuntimeError('HEFT count not1')
 if 'heft_start' in details:
  h=details['heft_start'];details['strict_improved_heft']=result['best_makespan']<h['makespan']-CFG['gate_strict_tolerance'];details['heft_fallback_or_equal_score']=abs(result['best_makespan']-h['makespan'])<=CFG['gate_strict_tolerance'];details['best_equals_heft_physical']=result['best_l']==h['loc'] and result['best_pi']==h['order']
 total=time.perf_counter()-tic
 return {**task,**source,**result,'audit':account,'details':details,'instance_sha256':inst.sha256,'t_total_wall_s':total,'t_candidate_checks_s':ev.t_checks,'t_accounting_s':ev.t_accounting,'t_final_checks_s':final_check,'t_uninstrumented_method_s':max(0.,total-ev.t_eval-ev.t_checks-ev.t_accounting-final_check),'rss_process_highwater_start_bytes':rss_start,'rss_process_highwater_end_bytes':peak_rss_bytes(),'memory_scope':task['memory_scope'],'host':platform.node(),'python':sys.version.split()[0],'numpy':np.__version__,'status':'completed'}
