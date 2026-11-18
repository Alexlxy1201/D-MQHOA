from __future__ import annotations
import sys,json,hashlib,math,time,resource,zipfile,os
from pathlib import Path
import numpy as np
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'supplement'))
from mqhoa_supp.model import Instance,canonical,digest,schedule,validate,check_solution
from mqhoa_supp.methods import heft
from algorithm_improvement_v2 import engine as frozen
CFG=json.loads(Path(__file__).with_name('config.json').read_text())
OUT=ROOT/'supplement/results/20261109_neighborhood_diagnostic_v1'
V2=ROOT/'supplement/results/20261109_algorithm_improvement_v2'
SOURCE=ROOT/'supplement/results/20261108_v1/raw/quality_dataset.zip'

def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def read(p):return json.loads(Path(p).read_text())
def write(p,x):Path(p).write_text(canonical(x)+'\n',encoding='utf8')
def rows(p):return [json.loads(l) for l in Path(p).read_text().splitlines() if l] if Path(p).exists() else []
def append(p,x):
 p=Path(p);p.parent.mkdir(parents=True,exist_ok=True)
 with p.open('a',encoding='utf8') as f:f.write(canonical(x)+'\n');f.flush();os.fsync(f.fileno())
def rss():return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024))

def load():
 with zipfile.ZipFile(SOURCE) as z:
  items={x.identifier:x for n in z.namelist() if n.startswith('instances/') and n.endswith('.json') for x in [Instance(**json.loads(z.read(n)))] if x.metadata.get('split')!='tuning'}
 old=rows(V2/'development/runs.jsonl');hefts={r['instance_id']:r for r in old if r['method']=='HEFT'};ls={(r['instance_id'],r['seed']):r for r in old if r['method']=='LS_HEFT'};assert len(items)==len(hefts)==14
 return items,hefts,ls

def independent_reachability(inst):
 result=[]
 for root in range(inst.n):
  seen=set();todo=[b for b,_ in inst.succ[root]]
  while todo:
   j=todo.pop()
   if j not in seen:seen.add(j);todo.extend(b for b,_ in inst.succ[j])
  result.append(seen)
 return result

def actions(inst,loc,order):
 check_solution(inst,loc,order);reach=independent_reachability(inst);result=[]
 for j in range(inst.n):
  if j not in inst.pinned:result.append({'instance_id':inst.identifier,'kind':'flip','nodes':[j]})
 for pos,(a,b) in enumerate(zip(order,order[1:])):
  if b not in reach[a] and a not in reach[b]:
   p=list(order);p[pos],p[pos+1]=p[pos+1],p[pos];check_solution(inst,loc,p);result.append({'instance_id':inst.identifier,'kind':'swap','nodes':[a,b],'swap_position':pos})
 for r in result:r['action_id']=digest(r)[:24]
 return result

def apply_action(loc,order,action):
 l,p=list(loc),list(order)
 if action['kind']=='flip':j=action['nodes'][0];l[j]=1-l[j]
 elif action['kind']=='swap':j=action['swap_position'];assert p[j:j+2]==action['nodes'];p[j],p[j+1]=p[j+1],p[j]
 else:raise ValueError('unknown action')
 return l,p

def critical_labels(inst,loc,order,makespan):
 check_solution(inst,loc,order);duration=[inst.ts[i] if loc[i] else inst.tl[i] for i in range(inst.n)];arcs={}
 for a,b,c in inst.edges:arcs[a,b]=max(arcs.get((a,b),0),duration[a]+(c if loc[a]!=loc[b] else 0))
 for resource in [0,1]:
  tasks=[i for i in order if loc[i]==resource]
  for a,b in zip(tasks,tasks[1:]):arcs[a,b]=max(arcs.get((a,b),0),duration[a])
 pre=[[] for _ in range(inst.n)];succ=[[] for _ in range(inst.n)];position={j:i for i,j in enumerate(order)}
 for (a,b),w in arcs.items():
  if position[a]>=position[b]:raise ValueError('augmented graph cycle/order violation')
  pre[b].append((a,w));succ[a].append((b,w))
 earliest=[0.]*inst.n
 for j in order:earliest[j]=max((earliest[a]+w for a,w in pre[j]),default=0.)
 latest=[makespan-d for d in duration]
 for j in reversed(order):latest[j]=min([latest[j]]+[latest[b]-w for b,w in succ[j]])
 slack=[latest[j]-earliest[j] for j in range(inst.n)]
 if min(slack)<-CFG['critical_slack_tolerance']:raise ValueError('negative slack')
 return [{'node':j,'earliest':earliest[j],'latest':latest[j],'slack':slack[j],'critical':abs(slack[j])<=CFG['critical_slack_tolerance']} for j in range(inst.n)]

def category(value,reference):
 delta=value-reference;eps=CFG['strict_threshold'];return 'improved' if delta< -eps else 'worse' if delta>eps else 'neutral'

class FullScheduleBudget:
 # Counted stage1 wrapper captures the one objective evaluation's full return.
 def __init__(self,inst,limit):self.inst=inst;self.limit=limit;self.used=0;self.last=None;self.t_eval=0.
 def evaluate(self,loc,order):
  if self.used>=self.limit:raise RuntimeError('objective cap')
  tic=time.perf_counter();v,s,f=schedule(self.inst,loc,order);self.t_eval+=time.perf_counter()-tic;self.used+=1
  if validate(self.inst,loc,order,s,f):raise RuntimeError('invalid evaluated schedule')
  self.last={'best_l':list(loc),'best_pi':list(order),'starts':s,'finishes':f,'best_makespan':v,'validator_ok':True};return v

def infer_action(base_l,base_p,loc,order):
 changed=[j for j,(a,b) in enumerate(zip(base_l,loc)) if a!=b];positions=[j for j,(a,b) in enumerate(zip(base_p,order)) if a!=b]
 if len(changed)==1 and not positions:return {'kind':'flip','nodes':changed}
 if not changed and len(positions)==2 and positions[1]==positions[0]+1:
  j=positions[0]
  if base_p[j]==order[j+1] and base_p[j+1]==order[j]:return {'kind':'swap','nodes':list(base_p[j:j+2]),'swap_position':j}
 if not changed and not positions:return {'kind':'noop','nodes':[]}
 raise RuntimeError('not a single legal perturbation')

class EventBudget(frozen.AuditBudget):
 def __init__(self,inst,limit,sink=None):
  super().__init__(inst,limit);self.sink=sink;self.events=[] if sink is None else None;self.current=None;self.anchor=None;self.restarts=0;self.last_restart_eval=None
 def evaluate(self,loc,order):
  before_global=self.best;before_current=self.current;phase=self.phase
  value=super().evaluate(loc,order) # exactly one frozen call; no RNG/objective elsewhere
  initial=phase=='heft_initialization';restart=phase=='restart_perturbation'
  if initial:
   self.anchor=(list(loc),list(order));action={'kind':'heft_initialization','nodes':[]};accepted=True
  else:
   if before_current is None:raise RuntimeError('missing observer current state')
   base=self.anchor if restart else (before_current['loc'],before_current['order']);action=infer_action(*base,loc,order);accepted=restart or value<before_current['value']
   if restart:self.restarts+=1;self.last_restart_eval=self.used
  if accepted:self.current={'loc':list(loc),'order':list(order),'value':value}
  global_delta=None if not math.isfinite(before_global) else before_global-value;current_delta=None if before_current is None else before_current['value']-value
  row={'raw_global_improvement_delta':global_delta,'meaningful_global_improvement':global_delta is not None and global_delta>CFG['strict_threshold'],'tiny_float_global_improvement':global_delta is not None and 0<global_delta<=CFG['strict_threshold'],'raw_current_improvement_delta':current_delta,'tiny_float_current_accept':not restart and not initial and accepted and current_delta is not None and 0<current_delta<=CFG['strict_threshold'],'eval':self.used,'eval_index':self.used,'candidate_value':value,'old_incumbent':None if not math.isfinite(before_global) else before_global,'new_incumbent':self.best,'phase':phase,'action':action,'loc':list(loc),'order':list(order),'objective':value,'global_best_before':None if not math.isfinite(before_global) else before_global,'global_best_after':self.best,'global_updated':value<before_global,'current_before_objective':None if before_current is None else before_current['value'],'current_after_objective':self.current['value'],'current_accepted':accepted,'restart_count':self.restarts,'last_restart_eval':self.last_restart_eval,'candidate_sha256':digest([list(loc),list(order)]),'current_before_sha256':None if before_current is None else digest([before_current['loc'],before_current['order']]),'current_after_sha256':digest([self.current['loc'],self.current['order']])}
  if self.sink is None:self.events.append(row)
  else:
   self.sink.write(canonical(row)+'\n')
   if self.used%CFG['event_flush_every']==0:self.sink.flush()
  return value
