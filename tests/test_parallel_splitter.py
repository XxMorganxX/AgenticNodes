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


def parallel_splitter_graph_payload() -> dict[str, object]:
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
            {
                "id": "left",
                "kind": "data",
                "category": "data",
                "label": "Left Branch",
                "provider_id": "core.data",
                "provider_label": "Core Data Node",
                "config": {"mode": "template", "template": "left:{input_payload}"},
                "position": {"x": 440, "y": -80},
            },
            {
                "id": "right",
                "kind": "data",
                "category": "data",
                "label": "Right Branch",
                "provider_id": "core.data",
                "provider_label": "Core Data Node",
                "config": {"mode": "template", "template": "right:{input_payload}"},
                "position": {"x": 440, "y": 80},
            },
            {
                "id": "finish_left",
                "kind": "output",
                "category": "end",
                "label": "Finish Left",
                "provider_id": "core.output",
                "provider_label": "Core Output Node",
                "config": {"source_binding": {"type": "latest_payload", "source": "left"}},
                "position": {"x": 660, "y": -80},
            },
            {
                "id": "finish_right",
                "kind": "output",
                "category": "end",
                "label": "Finish Right",
                "provider_id": "core.output",
                "provider_label": "Core Output Node",
                "config": {"source_binding": {"type": "latest_payload", "source": "right"}},
                "position": {"x": 660, "y": 80},
            },
        ],
        "edges": [
            {"id": "e1", "source_id": "start", "target_id": "split", "label": "", "kind": "standard", "priority": 100},
            {"id": "e2", "source_id": "split", "target_id": "left", "label": "", "kind": "standard", "priority": 100},
            {"id": "e3", "source_id": "split", "target_id": "right", "label": "", "kind": "standard", "priority": 100},
            {"id": "e4", "source_id": "left", "target_id": "finish_left", "label": "", "kind": "standard", "priority": 100},
            {"id": "e5", "source_id": "right", "target_id": "finish_right", "label": "", "kind": "standard", "priority": 100},
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
        self.assertIn("e2", transition_edges)
        self.assertIn("e3", transition_edges)
        self.assertEqual(state.node_outputs["left"]["payload"], "left:hello")
        self.assertEqual(state.node_outputs["right"]["payload"], "right:hello")

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
