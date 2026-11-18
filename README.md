# Dependency-aware edge scheduling

**Xiuya Liao · Research code sample for DTU Compute, position 8569**

When is it faster to move a task from a device to an edge server? Moving it can shorten computation but add a transfer delay for the next task. This Python example searches over both the placement of tasks and their execution order, then checks the resulting schedule against the exact optimum for a four-task graph.

## Run the example

With Python 3.12 on Linux or macOS, run these commands in the repository root:

```sh
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python run_demo.py
```

If NumPy 2.3.5 is already installed, run `python run_demo.py` directly. No external dataset is needed.

## What to look for

The first and last tasks stay on the device. The two middle tasks can run locally or on the server. The example enumerates all eight feasible schedules and compares the D-MQ search result with the best of them.

| Measurement | Expected result |
| --- | ---: |
| All-local completion time | 9 model time units |
| Best completion time by enumeration | 6 model time units |
| Completion time found by D-MQ (seed 7) | 6 model time units |
| Search evaluations | 60 |
| Constraint violations | 0 |

The script checks these results against [expected.json](expected.json). Its last line reports the solver's elapsed time in seconds, separately from the model time needed to execute the scheduled tasks.

## My contribution and where to start

In the collaborative D-MQ project, I worked on algorithm implementation, designed the comparison experiments, and prepared and analyzed the experimental data. For a short review:

1. [Walk through the search and exact check](run_demo.py).
2. [Inspect the search moves](supplement/discrete_neighborhood_v1/engine.py): the search can change where a task runs or swap the order of independent tasks.
3. [Read the shared scheduling model](supplement/mqhoa_supp/model.py) to see how precedence and transfer delays determine start times, and how a separate function checks feasibility.

