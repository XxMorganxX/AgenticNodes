from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from graph_agent.examples.tool_schema_repair import build_example_services
from graph_agent.runtime.core import GraphDefinition, GraphValidationError
from graph_agent.runtime.engine import GraphRuntime


def parallel_splitter_graph_payload(branch_labels: list[str] | None = None) -> dict[str, object]:
    labels = branch_labels or ["left", "right"]
    branch_nodes: list[dict[str, object]] = []
    finish_nodes: list[dict[str, object]] = []
    branch_edges: list[dict[str, object]] = []
    finish_edges: list[dict[str, object]] = []
    start_y = -80 * (len(labels) - 1)
    for index, label in enumerate(labels):
        branch_id = label
        finish_id = f"finish_{label}"
        branch_title = f"{label.replace('_', ' ').title()} Branch"
        finish_title = f"Finish {label.replace('_', ' ').title()}"
        y = start_y + index * 160
        branch_nodes.append(
            {
                "id": branch_id,
                "kind": "data",
                "category": "data",
                "label": branch_title,
                "provider_id": "core.data",
                "provider_label": "Core Data Node",
                "config": {"mode": "template", "template": f"{label}:{{input_payload}}"},
                "position": {"x": 440, "y": y},
            }
        )
        finish_nodes.append(
            {
                "id": finish_id,
                "kind": "output",
                "category": "end",
                "label": finish_title,
                "provider_id": "core.output",
                "provider_label": "Core Output Node",
                "config": {"source_binding": {"type": "latest_payload", "source": branch_id}},
                "position": {"x": 660, "y": y},
            }
        )
        branch_edges.append(
            {"id": f"split-{branch_id}", "source_id": "split", "target_id": branch_id, "label": "", "kind": "standard", "priority": 100}
        )
        finish_edges.append(
            {"id": f"{branch_id}-{finish_id}", "source_id": branch_id, "target_id": finish_id, "label": "", "kind": "standard", "priority": 100}
        )

    return {
        "graph_id": "parallel-splitter-graph",
        "name": "Parallel Splitter Graph",
        "description": "",
        "version": "1.0",
        "start_node_id": "start",
        "nodes": [
            {
                "id": "start",
                "kind": "input",
                "category": "start",
                "label": "Start",
                "provider_id": "start.manual_run",
                "provider_label": "Run Button Start",
                "config": {"input_binding": {"type": "input_payload"}},
                "position": {"x": 0, "y": 0},
            },
            {
                "id": "split",
                "kind": "control_flow_unit",
                "category": "control_flow_unit",
                "label": "Parallel Splitter",
                "provider_id": "core.parallel_splitter",
                "provider_label": "Parallel Splitter",
                "config": {"mode": "parallel_splitter"},
                "position": {"x": 220, "y": 0},
            },
            *branch_nodes,
            *finish_nodes,
        ],
        "edges": [
            {"id": "e1", "source_id": "start", "target_id": "split", "label": "", "kind": "standard", "priority": 100},
            *branch_edges,
            *finish_edges,
        ],
    }


class ParallelSplitterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.services = build_example_services()
        self.runtime = GraphRuntime(
            services=self.services,
            max_steps=self.services.config["max_steps"],
            max_visits_per_node=self.services.config["max_visits_per_node"],
        )

    def test_parallel_splitter_allows_multiple_standard_outgoing_edges(self) -> None:
        graph = GraphDefinition.from_dict(parallel_splitter_graph_payload())
        graph.validate_against_services(self.services)

    def test_parallel_splitter_fans_out_to_multiple_branches(self) -> None:
        graph = GraphDefinition.from_dict(parallel_splitter_graph_payload())
        graph.validate_against_services(self.services)

        state = self.runtime.run(graph, "hello", run_id="run-parallel-splitter")

        self.assertEqual(state.status, "completed")
        self.assertEqual(state.visit_counts["split"], 1)
        self.assertEqual(state.visit_counts["left"], 1)
        self.assertEqual(state.visit_counts["right"], 1)
        transition_edges = [transition.edge_id for transition in state.transition_history]
        self.assertIn("split-left", transition_edges)
        self.assertIn("split-right", transition_edges)
        self.assertEqual(state.node_outputs["left"]["payload"], "left:hello")
        self.assertEqual(state.node_outputs["right"]["payload"], "right:hello")

    def test_parallel_splitter_fans_out_to_more_than_three_branches(self) -> None:
        branch_labels = ["left", "right", "third", "fourth", "fifth"]
        graph = GraphDefinition.from_dict(parallel_splitter_graph_payload(branch_labels))
        graph.validate_against_services(self.services)

        state = self.runtime.run(graph, "hello", run_id="run-parallel-splitter-many")

        self.assertEqual(state.status, "completed")
        self.assertEqual(state.visit_counts["split"], 1)
        for label in branch_labels:
            self.assertEqual(state.visit_counts[label], 1)
            self.assertEqual(state.node_outputs[label]["payload"], f"{label}:hello")
            self.assertIn(f"split-{label}", [transition.edge_id for transition in state.transition_history])

    def test_regular_node_still_rejects_multiple_standard_outgoing_edges(self) -> None:
        payload = parallel_splitter_graph_payload()
        payload["nodes"][1]["provider_id"] = "core.logic_conditions"
        payload["nodes"][1]["provider_label"] = "Logic Conditions"
        payload["nodes"][1]["config"] = {
            "mode": "logic_conditions",
            "clauses": [
                {
                    "id": "if",
                    "label": "If",
                    "path": "",
                    "operator": "equals",
                    "value": "",
                    "source_contracts": [],
                    "output_handle_id": "control-flow-if",
                }
            ],
            "else_output_handle_id": "control-flow-else",
        }
        with self.assertRaises(GraphValidationError):
            GraphDefinition.from_dict(payload)


if __name__ == "__main__":
    unittest.main()
