"""Lossless compact typed logs. Codec only: no optimizer/model imports."""
from pathlib import Path
import numpy as np
import json,hashlib,struct
FORMAT=1
PHASES=['','heft_initialization','random_initialization','discrete_proposal','discrete_restart','search','local_flip','local_swap','restart_perturbation','flex_candidate','local_noop']
EVENT=np.dtype([('eval','<u2'),('phase','u1'),('value','<f8'),('gbefore','<f8'),('gafter','<f8'),('flags','u1'),('ptype','u1'),('cycle','<u2'),('center','u1'),('slot','u1'),('group','<u4'),('parent','<u2'),('parent_value','<f8'),('replaced','<u2'),('cache','<u2'),('L','u1'),('R','u1'),('sigma','<f8'),('z','<f8'),('pflags','u1'),('revisits','u1'),('stale_before','<u2'),('stale_after','<u2')])
MOVE=np.dtype([('kind','u1'),('a','u1'),('b','u1'),('position','<i2'),('families','u1'),('coin','<f8'),('critpool','<u2'),('flags','u1')])
CACHE=np.dtype([('id','<u2'),('paid','<u2'),('value','<f8'),('time','<f8')])
GROUP=np.dtype([('id','<u4'),('cycle','<u2'),('center','u1'),('parent','<u2'),('pvalue','<f8'),('best','<u2'),('bvalue','<f8'),('after','<u2'),('strict','u1'),('stop','u1'),('stale','<u2'),('pending','u1'),('extra','u1')])
CYCLE=np.dtype([('cycle','<u2'),('d','<f8'),('h','<f8'),('before','<f8'),('after','<f8'),('updated','u1'),('centers','<u2',(4,)),('last','<u2'),('complete','u1'),('k','u1'),('m','u1')])
STOPS=['slots_end','stagnation_trigger','budget']
KINDS=['noop','flip','swap']

def canonical(x):return json.dumps(x,sort_keys=True,ensure_ascii=False,separators=(',',':'),allow_nan=False)
def digest(x):return hashlib.sha256(canonical(x).encode()).hexdigest()
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def state_sha(l,p):return digest([list(map(int,l)),list(map(int,p))])
def schema():return {'format':FORMAT,'chunk_events':1000,'byteorder':'little','float_precision':'IEEE754float64','placement':'np.packbits(axis1,bitorder=little);unused bits0','order':'u1N<=100','event_dtype':EVENT.descr,'move_dtype':MOVE.descr,'cache_dtype':CACHE.descr,'group_dtype':GROUP.descr,'cycle_dtype':CYCLE.descr,'phase_codes':PHASES,'move_codes':KINDS,'group_stop_codes':STOPS,'derived_fields':['physical_sha256 fromloc/order','parent_sha256 frompaidparent','critical_tasks_sha256 fromcachebitset'],'runtime_fields_excluded_only_from_semantic_parity':['cache.t_label_s']}

class EventWriter:
 handle=None
 def __init__(self,directory,n,chunk=1000):
  self.root=Path(directory);self.root.mkdir(parents=True,exist_ok=False);self.n=n;self.chunk=chunk;self.meta=[];self.loc=[];self.order=[];self.moves=[];self.offsets=[0];self.files=[];self.total=0;self.score_hash=hashlib.sha256();self.parents_hash=[]
 def emit(self,e):
  if isinstance(e,tuple):l,p,v,before,after,phase,updated,meaningful,tiny,duplicate,prop=e
  else:l,p,v,before,after,phase,updated,meaningful,tiny,duplicate,prop=e['loc'],e['order'],e['candidate_value'],e['global_best_before'],e['global_best_after'],e['phase'],e['global_updated'],e['meaningful_global_improvement'],e['tiny_float_global_improvement'],e['duplicate'],e['proposal']
  l=np.asarray(l,dtype='u1');p=np.asarray(p,dtype='u1');assert len(l)==len(p)==self.n and self.n<=100
  self.total+=1;row=np.zeros((),dtype=EVENT);row['eval']=self.total;row['phase']=PHASES.index(phase);row['value']=v;row['gbefore']=0 if before is None else before;row['gafter']=after;row['flags']=(before is None)|int(updated)<<1|int(meaningful)<<2|int(tiny)<<3|int(duplicate)<<4
  if prop is not None:
   q=prop;row['ptype']=1 if q['type']=='ordinary' else 2
   for key,target in [('cycle','cycle'),('center','center'),('cycle_slot','slot'),('parent_eval','parent'),('parent_fitness','parent_value'),('critical_cache_id','cache'),('L','L'),('R','R'),('sigma_before','sigma'),('chain_state_revisits','revisits'),('stale_before','stale_before'),('stale_after','stale_after')]:row[target]=q[key]
   row['group']=q['group_id'] or 0;row['replaced']=q.get('replaced_center_eval',0);row['z']=0 if q['z'] is None else q['z'];row['pflags']=int(q['returns_to_parent'])|int(q['restart_pending_after'])<<1|int(q['z'] is None)<<2
   for m in q['moves']:
    mm=np.zeros((),dtype=MOVE);mm['kind']=KINDS.index(m['kind']);mm['a']=m['nodes'][0] if m['nodes'] else 0;mm['b']=m['nodes'][1] if len(m['nodes'])==2 else 0;mm['position']=-1 if m['swap_position'] is None else m['swap_position'];mm['families']=m['family_count'];mm['coin']=0 if m['critical_coin'] is None else m['critical_coin'];mm['critpool']=m['critical_pool_size'];mm['flags']=int(m['selected_from_critical'])|int(m['critical_coin'] is None)<<1;self.moves.append(mm)
  self.meta.append(row);self.loc.append(np.packbits(l,bitorder='little'));self.order.append(p);self.offsets.append(len(self.moves));self.score_hash.update(l.astype('i1',copy=False).tobytes()+p.astype('<i4').tobytes()+struct.pack('<d',float(v)))
  if len(self.meta)>=self.chunk:self.flush()
 def flush(self):
  if not self.meta:return
  path=self.root/f'events-{len(self.files):04d}.npz';np.savez_compressed(path,meta=np.asarray(self.meta,dtype=EVENT),loc=np.asarray(self.loc,dtype='u1'),order=np.asarray(self.order,dtype='u1'),moves=np.asarray(self.moves,dtype=MOVE),offsets=np.asarray(self.offsets,dtype='<u4'));self.files.append({'file':path.name,'sha256':sha(path),'events':len(self.meta)});self.meta=[];self.loc=[];self.order=[];self.moves=[];self.offsets=[0]
 def close(self):
  self.flush();self.header={'format':FORMAT,'n':self.n,'events':self.total,'chunks':self.files,'physical_score_sha256':self.score_hash.hexdigest()};(self.root/'header.json').write_text(canonical(self.header)+'\n')

class SideWriter:
 handle=None
 def __init__(self,root,kind,n):self.root=Path(root);self.kind=kind;self.n=n;self.data=[]
 def emit(self,r):self.data.append(r)
 def close(self):
  kind=self.kind;rs=self.data
  if kind=='critical_cache':
   meta=np.zeros(len(rs),dtype=CACHE);bits=np.zeros((len(rs),(self.n+7)//8),dtype='u1')
   for i,r in enumerate(rs):
    meta[i]=(r['cache_id'],r['source_paid_eval'],r['objective'],r['t_label_s']);mask=np.zeros(self.n,dtype='u1');mask[r['critical_tasks']]=1;bits[i]=np.packbits(mask,bitorder='little')
   np.savez_compressed(self.root/(kind+'.npz'),meta=meta,bits=bits)
  elif kind=='groups':
   meta=np.zeros(len(rs),dtype=GROUP);offset=[0];proposals=[]
   for i,r in enumerate(rs):
    meta[i]=(r['group_id'],r['cycle'],r['center'],r['parent_eval'],r['parent_fitness'],r['best_eval'],r['best_value'],r['after_center_eval'],r['strict_update'],STOPS.index(r['stop_reason']),r['stale'],r['restart_pending'],r['cache_objective_calls']);proposals.extend(r['proposal_evals']);offset.append(len(proposals))
   np.savez_compressed(self.root/(kind+'.npz'),meta=meta,offsets=np.asarray(offset,dtype='<u4'),proposals=np.asarray(proposals,dtype='<u2'))
  elif kind=='cycles':
   meta=np.zeros(len(rs),dtype=CYCLE)
   for i,r in enumerate(rs):meta[i]=(r['cycle'],0 if r['distance'] is None else r['distance'],0 if r['h'] is None else r['h'],r['sigma_before'],r['sigma_after'],r['updated'],r['center_evals']+[0]*(4-len(r['center_evals'])),r['last_eval'],r['completed_cycle'],r['k'],r['m'])
   np.savez_compressed(self.root/(kind+'.npz'),meta=meta)
  else:raise ValueError(kind)

def _stored_vectors(root):
 h=json.loads((Path(root)/'header.json').read_text());n=h['n'];loc=[];order=[]
 for chunk in h['chunks']:
  with np.load(Path(root)/chunk['file'],allow_pickle=False) as z:loc.append(np.unpackbits(z['loc'],axis=1,bitorder='little')[:,:n]);order.append(z['order'])
 return np.concatenate(loc),np.concatenate(order)

def iter_sidecar(root,kind,vectors=None):
 root=Path(root)
 if vectors is None and kind=='critical_cache':vectors=_stored_vectors(root)
 with np.load(root/(kind+'.npz'),allow_pickle=False) as z:
  if kind=='critical_cache':
   n=vectors[0].shape[1];bits=np.unpackbits(z['bits'],axis=1,bitorder='little')[:,:n]
   for m,b in zip(z['meta'],bits):
    paid=int(m['paid']);tasks=np.flatnonzero(b).astype(int).tolist();yield {'cache_id':int(m['id']),'source_paid_eval':paid,'physical_sha256':state_sha(vectors[0][paid-1],vectors[1][paid-1]),'objective':float(m['value']),'critical_tasks':tasks,'critical_tasks_sha256':digest(tasks),'t_label_s':float(m['time'])}
  elif kind=='groups':
   props=z['proposals'];off=z['offsets']
   for i,m in enumerate(z['meta']):yield {'group_id':int(m['id']),'cycle':int(m['cycle']),'center':int(m['center']),'parent_eval':int(m['parent']),'parent_fitness':float(m['pvalue']),'proposal_evals':props[off[i]:off[i+1]].astype(int).tolist(),'best_eval':int(m['best']),'best_value':float(m['bvalue']),'after_center_eval':int(m['after']),'strict_update':bool(m['strict']),'stop_reason':STOPS[int(m['stop'])],'stale':int(m['stale']),'restart_pending':bool(m['pending']),'cache_objective_calls':int(m['extra'])}
  elif kind=='cycles':
   for m in z['meta']:
    complete=bool(m['complete']);k=int(m['k']);yield {'cycle':int(m['cycle']),'distance':float(m['d']) if complete else None,'h':float(m['h']) if complete else None,'sigma_before':float(m['before']),'sigma_after':float(m['after']),'updated':bool(m['updated']),'center_evals':m['centers'][:k].astype(int).tolist(),'last_eval':int(m['last']),'completed_cycle':complete,'k':k,'m':int(m['m'])}

def iter_events(root):
 root=Path(root);h=json.loads((root/'header.json').read_text());vectors=_stored_vectors(root);cache={r['cache_id']:r for r in iter_sidecar(root,'critical_cache',vectors)};n=h['n']
 for chunk in h['chunks']:
  with np.load(root/chunk['file'],allow_pickle=False) as z:
   for idx,m in enumerate(z['meta']):
    j=int(m['eval']);l=vectors[0][j-1].astype(int).tolist();p=vectors[1][j-1].astype(int).tolist();flags=int(m['flags']);q=None
    if m['ptype']:
     parent=int(m['parent']);cid=int(m['cache']);pf=int(m['pflags']);moves=[]
     for mm in z['moves'][z['offsets'][idx]:z['offsets'][idx+1]]:
      kind=KINDS[int(mm['kind'])];mf=int(mm['flags']);moves.append({'kind':kind,'nodes':[] if kind=='noop' else [int(mm['a'])] if kind=='flip' else [int(mm['a']),int(mm['b'])],'swap_position':None if mm['position']<0 else int(mm['position']),'family_count':int(mm['families']),'critical_coin':None if mf&2 else float(mm['coin']),'critical_pool_size':int(mm['critpool']),'selected_from_critical':bool(mf&1)})
     q={'type':'ordinary' if m['ptype']==1 else 'restart','cycle':int(m['cycle']),'center':int(m['center']),'cycle_slot':int(m['slot']),'group_id':int(m['group']) or None,'parent_eval':parent,'parent_fitness':float(m['parent_value']),'parent_sha256':state_sha(vectors[0][parent-1],vectors[1][parent-1]),'critical_cache_id':cid,'critical_tasks_sha256':cache[cid]['critical_tasks_sha256'],'L':int(m['L']),'R':int(m['R']),'sigma_before':float(m['sigma']),'z':None if pf&4 else float(m['z']),'moves':moves,'chain_state_revisits':int(m['revisits']),'returns_to_parent':bool(pf&1),'stale_before':int(m['stale_before']),'stale_after':int(m['stale_after']),'restart_pending_after':bool(pf&2)}
     if m['ptype']==2:q['replaced_center_eval']=int(m['replaced'])
    yield {'eval_index':j,'loc':l,'order':p,'candidate_value':float(m['value']),'phase':PHASES[int(m['phase'])],'global_best_before':None if flags&1 else float(m['gbefore']),'global_best_after':float(m['gafter']),'global_updated':bool(flags&2),'meaningful_global_improvement':bool(flags&4),'tiny_float_global_improvement':bool(flags&8),'duplicate':bool(flags&16),'physical_sha256':state_sha(l,p),'proposal':q}

def iter_raw_chunks(root):
 root=Path(root);h=json.loads((root/'header.json').read_text())
 for c in h['chunks']:
  with np.load(root/c['file'],allow_pickle=False) as z:yield {k:z[k] for k in z.files}

def verify_files(root):
 h=json.loads((Path(root)/'header.json').read_text())
 for c in h['chunks']:assert sha(Path(root)/c['file'])==c['sha256']
 return h
