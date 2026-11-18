import json
from pathlib import Path

from dmqhoa import solve_dmqhoa


if __name__ == "__main__":
    graph = json.loads(Path(__file__).with_name("toy_graph.json").read_text())
    print(json.dumps(solve_dmqhoa(graph, budget=60, seed=7), indent=2))
