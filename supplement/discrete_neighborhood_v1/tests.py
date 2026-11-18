from engine import *
import unittest
from decimal import Decimal

class DiscreteTests(unittest.TestCase):
 def i(self):return Instance('toy',[2.,3.,2.,2.],[1.,.5,1.,1.],[(0,1,0.),(0,2,0.),(1,3,0.),(2,3,0.)],[0,3])
 def test_actions_legal_pins_and_chain(self):
  i=self.i();rng=np.random.default_rng(0);reach=frozen.reachability(i);stats=Counter();stats['t_legal_enumeration_s']=0.;parent={'loc':[0,1,0,0],'order':[0,1,2,3]}
  for _ in range(100):l,p,moves,_,_=chain(i,parent,{0,2,3},5,rng,reach,stats);check_solution(i,l,p);self.assertEqual(l[0],0);self.assertEqual(l[3],0);self.assertEqual(len(moves),5)
 def test_length_distance_and_scale_bounds(self):
  rng=np.random.default_rng(1)
  for sigma in [1,2,10]:
   for _ in range(100):L,z=length(rng,sigma,5);self.assertTrue(1<=L<=5)
  x={'loc':[0,0],'order':[0,1]};y={'loc':[1,1],'order':[1,0]};self.assertEqual(distance([x],2),0);self.assertEqual(distance([x,y],2),1)
 def test_same20_initialization_and_budget(self):
  i=self.i();hashes=[]
  for method in CFG['new_methods']:
   ev=RecordedBudget(i,60);d=discrete(i,ev,np.random.default_rng(7),method,Sink(),Sink(),Sink());hashes.append(ev.accounting()['initial_physical_sha256']);self.assertEqual(ev.used,60);self.assertEqual(ev.phases['heft_initialization'],1);self.assertEqual(ev.phases['random_initialization'],19)
  ev=frozen.AuditBudget(i,60);frozen.ready_pso(i,ev,np.random.default_rng(7),load()[2]['PSO']);self.assertEqual(hashes,[ev.accounting()['initial_physical_sha256']]*4)
 def test_restart_slots_fixed_parent_and_cached_objectives(self):
  i=Instance('no-action',[1.],[.5],[],[0]);ev=RecordedBudget(i,75);g=Sink();cy=Sink();d=discrete(i,ev,np.random.default_rng(0),'D_MQ',Sink(),g,cy);events=ev.sink.data;self.assertEqual(ev.used,75);self.assertGreater(d['stats']['restarts'],0);self.assertEqual(ev.calls,75);self.assertEqual(len(events),75)
  for row in g.data:
   self.assertEqual(row['cache_objective_calls'],0);self.assertEqual(len({events[e-1]['proposal']['parent_eval'] for e in row['proposal_evals']}),1)
  self.assertFalse(cy.data[-1]['completed_cycle']);self.assertEqual(cy.data[-1]['sigma_before'],cy.data[-1]['sigma_after'])
  for e in events[20:]:
   p=e['proposal'];self.assertEqual(p['stale_after'],0 if p['type']=='restart' else p['stale_before']+1)
 def test_duplicates_cancellation_not_resampled(self):
  i=Instance('onefree',[1.],[.5],[],[]);parent={'loc':[0],'order':[0]};stats=Counter();stats['t_legal_enumeration_s']=0.;l,p,moves,revisits,cancel=chain(i,parent,{0},2,np.random.default_rng(2),frozen.reachability(i),stats);self.assertTrue(cancel);self.assertEqual(revisits,1);self.assertEqual(l,[0]);self.assertEqual(len(moves),2)
  ev=RecordedBudget(i,2);ev.evaluate([0],[0]);ev.evaluate([0],[0]);self.assertEqual(ev.accounting()['duplicate_evaluations'],1)
 def test_hand_decimal_and_critical_cache(self):
  i=self.i();l=[0,1,0,0];p=[0,1,2,3];ev=RecordedBudget(i,4);value=ev.evaluate(l,p);self.assertEqual(Decimal(str(value)),Decimal('6.0'));cache=CriticalCache(i,Sink());r=cache.get(l,p,value,1);self.assertEqual(r['critical_tasks'],[0,2,3]);self.assertEqual(cache.get(l,p,value,1)['cache_id'],r['cache_id']);self.assertEqual(cache.hits,1);self.assertEqual(ev.used,1)
  from neighborhood_diagnostic_v1.engine import actions,apply_action
  for a in actions(i,l,p):
   ll,pp=apply_action(l,p,a);v=ev.evaluate(ll,pp);available=[Decimal(0),Decimal(0)];finish={}
   for j in pp:
    rr=ll[j];release=max((finish[u]+(Decimal(str(c)) if ll[u]!=rr else Decimal(0)) for u,c in i.pre[j]),default=Decimal(0));start=max(release,available[rr]);finish[j]=start+Decimal(str(i.ts[j] if rr else i.tl[j]));available[rr]=finish[j]
   self.assertLess(abs(Decimal(str(v))-max(finish.values())),Decimal('1e-12'))
  self.assertEqual(ev.used,4)
 def test_broken_order_pins_and_corrupt_counts_rejected(self):
  i=self.i();ev=RecordedBudget(i,2)
  for l,p in [([1,0,0,0],[0,1,2,3]),([0]*4,[3,2,1,0])]:
   with self.assertRaises(ValueError):ev.evaluate(l,p)
  ev.evaluate([0]*4,[0,1,2,3]);ev.used+=1
  with self.assertRaises(RuntimeError):ev.accounting()
 def test_frozen_control_observer_is_exact(self):
  i=self.i();ps=load()[2]['PSO']
  for method in ['LS_HEFT','PSO_ReadyKeyHEFT']:
   a=frozen.AuditBudget(i,100);b=RecordedBudget(i,100);r1=np.random.default_rng(0);r2=np.random.default_rng(0)
   for ev,rng in [(a,r1),(b,r2)]:
    if method=='LS_HEFT':frozen.local_search(i,ev,rng)
    else:frozen.ready_pso(i,ev,rng,ps)
   self.assertEqual(a.accounting()['candidate_stream_sha256'],b.accounting()['candidate_stream_sha256']);self.assertEqual(a.result()['best_makespan'],b.result()['best_makespan']);self.assertEqual(r1.bit_generator.state,r2.bit_generator.state)

if __name__=='__main__':
 original=frozen.AuditBudget.evaluate;count={'successful_objective_calls':0,'rejected_invalid_attempts':0,'independent_decimal_schedule_reconstructions':3,'hand_decimal_comparisons':1,'test_only_spy_restored':False}
 def counted(self,*args,**kwargs):
  try:value=original(self,*args,**kwargs)
  except ValueError:count['rejected_invalid_attempts']+=1;raise
  count['successful_objective_calls']+=1;return value
 frozen.AuditBudget.evaluate=counted
 try:result=unittest.main(verbosity=2,exit=False)
 finally:
  frozen.AuditBudget.evaluate=original;count['test_only_spy_restored']=True;write(OUT/'unit_call_counts.json',count)
 sys.exit(0 if result.result.wasSuccessful() else 1)

