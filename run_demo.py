"""Application walkthrough; the imported scheduler is unchanged research code."""
from pathlib import Path
from itertools import permutations, product
import json
import sys
from time import perf_counter

import numpy as np

root = Path(__file__).resolve().parent
sys.path.insert(0, str(root / "supplement"))
from discrete_neighborhood_v1 import engine
from mqhoa_supp.model import Instance, schedule, validate


def main():
    instance = Instance(**json.loads((root / "toy_graph.json").read_text()))
    expected = json.loads((root / "expected.json").read_text())
    # For this tiny graph only, enumerate every legal schedule as an oracle.
    orders = [pi for pi in permutations(range(instance.n))
              if all(pi.index(a) < pi.index(b) for a, b, _ in instance.edges)]
    placements = [loc for loc in product((0, 1), repeat=instance.n)
                  if all(loc[j] == 0 for j in instance.pinned)]
    optimum = min(schedule(instance, loc, pi)[0] for pi in orders for loc in placements)
    local = schedule(instance, [0] * instance.n, orders[0])[0]
    budget = engine.RecordedBudget(instance, 60)
    start = perf_counter()
    engine.discrete(instance, budget, np.random.default_rng(7), "D_MQ",
                    engine.Sink(), engine.Sink(), engine.Sink())
    duration = perf_counter() - start
    result = budget.result()
    violations = validate(instance, result["best_l"], result["best_pi"],
                          result["starts"], result["finishes"])
    observed = dict(legal_schedules=len(orders) * len(placements),
                    all_local_makespan=local, exhaustive_optimum=optimum,
                    dmq_makespan=result["best_makespan"], evaluations=budget.used,
                    constraint_violations=violations)
    if observed != expected:
        raise RuntimeError(f"Expected {expected}, observed {observed}")
    print(json.dumps(observed, indent=2))
    print(f"D-MQ search time: {duration:.6f} s (varies by machine). PASS")


if __name__ == "__main__":
    main()
