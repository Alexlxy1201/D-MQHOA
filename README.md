# MQHOA Edge Scheduling

This repository runs the MQHOA project's discrete scheduler (D-MQ) on a graph of dependent tasks. The search chooses which tasks to offload to an edge server and in what order to execute them. When a task and its predecessor run on different devices, the scheduling model accounts for the time needed to transfer their data.

## Run the example

With Python 3.12 on Linux or macOS, run these commands in the repository root:

```sh
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python run_demo.py
```

If NumPy 2.3.5 is already installed, run `python run_demo.py` directly. No external dataset is needed.

## Example output

The first and last tasks stay on the device. The two middle tasks can run locally or on the server. The example enumerates all eight feasible schedules and compares the D-MQ search result with the best of them.

| Measurement | Expected result |
| --- | ---: |
| All-local completion time | 9 model time units |
| Best completion time by enumeration | 6 model time units |
| Completion time found by D-MQ (seed 7) | 6 model time units |
| Search evaluations | 60 |
| Constraint violations | 0 |

The script checks these results against [expected.json](expected.json). Its last line reports the solver's elapsed time in seconds, separately from the model time needed to execute the scheduled tasks.

## Code

- [run_demo.py](run_demo.py) loads the task graph, runs D-MQ, and checks the result against exhaustive enumeration.
- [engine.py](supplement/discrete_neighborhood_v1/engine.py) implements the discrete search: change a task's device or swap two independent tasks.
- [model.py](supplement/mqhoa_supp/model.py) computes start and finish times and checks precedence and resource constraints.

