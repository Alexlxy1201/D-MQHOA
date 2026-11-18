# Discrete Multi-scale Quantum Harmonic Oscillator Algorithm (D-MQHOA)

D-MQHOA schedules a directed acyclic graph of tasks across a local device and an edge server to minimize makespan. A schedule specifies where each task runs and a task order that respects dependencies. The decoder accounts for processor availability and for transfer delays when dependent tasks run on different devices.

The search starts with a Heterogeneous Earliest Finish Time (HEFT) schedule and 19 random schedules, keeping the best four as search centers. Each candidate is built by moving an unpinned task to the other device or swapping adjacent tasks that have no precedence relation. The search favors edits involving critical tasks. A Gaussian sample sets the number of edits per candidate; as centers become similar, the sampling scale shrinks. A stalled center restarts from a one-edit neighbor of the HEFT schedule.

## Usage

Install NumPy with `python -m pip install -r requirements.txt` (Python 3.12, Linux or macOS).

```python
import json
from pathlib import Path
from dmqhoa import solve_dmqhoa

graph = json.loads(Path("toy_graph.json").read_text())
schedule = solve_dmqhoa(graph, budget=10_000, seed=7)
print(schedule)
```

`tl` and `ts` give task durations on the device and server. Each edge is a triple `[predecessor, successor, transfer_delay]`; tasks in `pinned` stay on the device. The result contains the placement (0: device, 1: server), task order, makespan and number of evaluations. Run `python run_demo.py` for the four-task example.
