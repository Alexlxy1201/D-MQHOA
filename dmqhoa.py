import numpy as np

from supplement.discrete_neighborhood_v1 import engine


class _DiscardTrace:
    handle = None

    def emit(self, record):
        pass


def solve_dmqhoa(graph, *, budget=10_000, seed=None):
    """Search a task graph for a feasible device/edge schedule of minimum makespan."""
    if not isinstance(budget, int) or budget < 20:
        raise ValueError("budget must be an integer of at least 20 evaluations")
    instance = engine.Instance(**graph)
    trace = _DiscardTrace()
    evaluator = engine.RecordedBudget(instance, budget, sink=trace)
    engine.discrete(instance, evaluator, np.random.default_rng(seed), "D_MQ",
                    trace, trace, trace)
    best = evaluator.result()
    return {
        "placement": best["best_l"],
        "order": best["best_pi"],
        "makespan": best["best_makespan"],
        "evaluations": evaluator.used,
    }
