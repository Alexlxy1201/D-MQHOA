D-MQ: Dependency-aware edge scheduling | Xiuya Liao

Read the one-page Code_Sample_Guide.pdf for the research question, my contribution,
and how to interpret the results.

Run the four-task example from the unpacked folder (macOS or Linux, Python 3.12):
  python3 -m venv .venv
  .venv/bin/python -m pip install -r requirements.txt
  .venv/bin/python run_demo.py
If NumPy 2.3.5 is already installed, only the last command is needed with that Python.
The demonstration itself runs offline, from any working directory.

Graph: tasks 0 and 3 are pinned to the local device. Tasks 1 and 2 may run on
either device after task 0 finishes; task 3 waits for both. A transfer costs 0.25
time units if a dependency crosses devices. There are eight feasible schedules.
Compare the printed values with expected.json. Runtime in seconds is intentionally
not fixed; the makespan is a model quantity in the same units as the task durations.

Suggested reading order (all research paths below are under supplement/):
  mqhoa_supp/model.py                     Instance, schedule and independent validate
  discrete_neighborhood_v1/engine.py       atomic, chain and discrete
  discrete_confirmation_v1/adapter.py      run_one: common comparison and validation
  discrete_confirmation_v1/campaign.py     quality vs. timing: original research driver
  discrete_neighborhood_v1/tests.py        legality, budgets and hand-calculated examples
  results_excerpt/ (package root)          archived aggregate comparisons and timings

Files under supplement/ and the two CSVs are copies from the collaborative
research project. SHA256SUMS.txt identifies the packaged files. run_demo.py,
toy_graph.json, expected.json and this guide are application walkthrough materials.
Only run_demo.py is the entry point for this small sample. The original campaign
drivers are included for inspection; the historical dataset, logs and the entire
benchmark environment are not shipped. No full-study rerun is implied.
