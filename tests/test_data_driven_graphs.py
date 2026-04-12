from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from graph_agent.api.graph_store import GraphStore
from graph_agent.examples.tool_schema_repair import build_example_graph_payload, build_example_services
from graph_agent.providers.base import ModelRequest, ModelResponse
from graph_agent.runtime.core import GraphDefinition, GraphValidationError
from graph_agent.runtime.engine import GraphRuntime


class RecordingModelProvider:
    name = "openai"

    def __init__(self) -> None:
        self.last_request: ModelRequest | None = None

    def generate(self, request: ModelRequest) -> ModelResponse:
        self.last_request = request
        return ModelResponse(content="resolved-output")


class DataDrivenGraphTests(unittest.TestCase):
    def setUp(self) -> None:
        self.services = build_example_services()

    def test_catalog_marks_pass_through_side_effect_nodes(self) -> None:
        catalog = GraphStore(self.services).catalog()
        providers = {provider["provider_id"]: provider for provider in catalog["node_providers"]}

        self.assertTrue(providers["core.data_display"]["produces_side_effects"])
        self.assertTrue(providers["core.data_display"]["preserves_input_payload"])
        self.assertTrue(providers["core.write_text_file"]["produces_side_effects"])
        self.assertTrue(providers["core.write_text_file"]["preserves_input_payload"])
        self.assertTrue(providers["end.discord_message"]["produces_side_effects"])
        self.assertFalse(providers["end.discord_message"]["preserves_input_payload"])

    def test_example_graph_runs_end_to_end(self) -> None:
        graph = GraphDefinition.from_dict(build_example_graph_payload())
        graph.validate_against_services(self.services)

        runtime = GraphRuntime(
            services=self.services,
            max_steps=self.services.config["max_steps"],
            max_visits_per_node=self.services.config["max_visits_per_node"],
        )

        state = runtime.run(graph, "Find graph-agent references for a schema repair workflow.")

        self.assertEqual(state.status, "completed")
        self.assertIsNotNone(state.final_output)
        self.assertIn("message", state.final_output)
        self.assertIn("repair_tool", [transition.target_id for transition in state.transition_history])

    def test_invalid_category_connection_is_rejected(self) -> None:
        payload = build_example_graph_payload()
        payload["edges"].append(
            {
                "id": "edge-finish-start",
                "source_id": "finish",
                "target_id": "start",
                "label": "invalid",
                "kind": "standard",
                "priority": 100,
                "condition": None,
            }
        )

        with self.assertRaises(GraphValidationError):
            GraphDefinition.from_dict(payload)

    def test_graph_env_vars_resolve_in_runtime_templates_and_provider_config(self) -> None:
        recording_provider = RecordingModelProvider()
        self.services.model_providers["openai"] = recording_provider

        payload: dict[str, Any] = {
            "graph_id": "env-agent",
            "name": "Env Agent",
            "description": "",
            "version": "1.0",
            "start_node_id": "start",
            "env_vars": {
                "OPENAI_API_KEY": "OPENAI_API_KEY",
                "SYSTEM_ROLE": "schema fixer",
                "MODEL_NAME": "gpt-4.1-mini",
            },
            "nodes": [
                {
                    "id": "start",
                    "kind": "input",
                    "category": "start",
                    "label": "Start",
                    "provider_id": "core.input",
                    "provider_label": "Core Input Node",
                    "description": "",
                    "position": {"x": 0, "y": 0},
                    "config": {"input_binding": {"type": "input_payload"}},
                },
                {
                    "id": "model",
                    "kind": "model",
                    "category": "api",
                    "label": "Call Model",
                    "provider_id": "core.api",
                    "provider_label": "Core API Node",
                    "description": "",
                    "position": {"x": 240, "y": 0},
                    "model_provider_name": "openai",
                    "prompt_name": "env_prompt",
                    "config": {
                        "provider_name": "openai",
                        "prompt_name": "env_prompt",
                        "model": "{MODEL_NAME}",
                        "api_key_env_var": "{OPENAI_API_KEY}",
                        "system_prompt": "You are a {SYSTEM_ROLE}.",
                        "user_message_template": "Input: {input_payload}",
                    },
                },
                {
                    "id": "finish",
                    "kind": "output",
                    "category": "end",
                    "label": "Finish",
                    "provider_id": "core.output",
                    "provider_label": "Core Output Node",
                    "description": "",
                    "position": {"x": 480, "y": 0},
                    "config": {},
                },
            ],
            "edges": [
                {
                    "id": "edge-start-model",
                    "source_id": "start",
                    "target_id": "model",
                    "label": "",
                    "kind": "standard",
                    "priority": 100,
                    "condition": None,
                },
                {
                    "id": "edge-model-finish",
                    "source_id": "model",
                    "target_id": "finish",
                    "label": "",
                    "kind": "standard",
                    "priority": 100,
                    "condition": None,
                },
            ],
        }

        graph = GraphDefinition.from_dict(payload)
        graph.validate_against_services(self.services)

        runtime = GraphRuntime(
            services=self.services,
            max_steps=self.services.config["max_steps"],
            max_visits_per_node=self.services.config["max_visits_per_node"],
        )
        state = runtime.run(graph, "repair the broken schema")

        self.assertEqual(state.status, "completed")
        self.assertIsNotNone(recording_provider.last_request)
        assert recording_provider.last_request is not None
        self.assertEqual(recording_provider.last_request.messages[0].content, "You are a schema fixer.")
        self.assertEqual(recording_provider.last_request.messages[1].content, "Input: repair the broken schema")
        self.assertEqual(recording_provider.last_request.provider_config["model"], "gpt-4.1-mini")
        self.assertEqual(recording_provider.last_request.provider_config["api_key_env_var"], "OPENAI_API_KEY")

    def test_logic_conditions_resolve_graph_env_references_in_branch_values_and_handles(self) -> None:
        payload: dict[str, Any] = {
            "graph_id": "logic-conditions-env-graph",
            "name": "Logic Conditions Env Graph",
            "description": "",
            "version": "1.0",
            "start_node_id": "start",
            "env_vars": {
                "EXPECTED_STATUS": "approved",
                "MATCH_HANDLE": "branch-approved",
                "ELSE_HANDLE": "branch-fallback",
            },
            "nodes": [
                {
                    "id": "start",
                    "kind": "input",
                    "category": "start",
                    "label": "Start",
                    "provider_id": "start.manual_run",
                    "provider_label": "Run Button Start",
                    "description": "",
                    "position": {"x": 0, "y": 0},
                    "config": {"input_binding": {"type": "input_payload"}},
                },
                {
                    "id": "branch",
                    "kind": "control_flow_unit",
                    "category": "control_flow_unit",
                    "label": "Branch",
                    "provider_id": "core.logic_conditions",
                    "provider_label": "Logic Conditions",
                    "description": "",
                    "position": {"x": 240, "y": 0},
                    "config": {
                        "mode": "logic_conditions",
                        "branches": [
                            {
                                "id": "status-branch",
                                "label": "Approved Status",
                                "output_handle_id": "{MATCH_HANDLE}",
                                "root_group": {
                                    "id": "group-1",
                                    "type": "group",
                                    "combinator": "all",
                                    "negated": False,
                                    "children": [
                                        {
                                            "id": "rule-1",
                                            "type": "rule",
                                            "path": "status",
                                            "operator": "equals",
                                            "value": "{EXPECTED_STATUS}",
                                            "source_contracts": ["message_envelope"],
                                        }
                                    ],
                                },
                            }
                        ],
                        "else_output_handle_id": "{ELSE_HANDLE}",
                    },
                },
                {
                    "id": "if_finish",
                    "kind": "output",
                    "category": "end",
                    "label": "If Finish",
                    "provider_id": "core.output",
                    "provider_label": "Core Output Node",
                    "description": "",
                    "position": {"x": 480, "y": -60},
                    "config": {},
                },
                {
                    "id": "else_finish",
                    "kind": "output",
                    "category": "end",
                    "label": "Else Finish",
                    "provider_id": "core.output",
                    "provider_label": "Core Output Node",
                    "description": "",
                    "position": {"x": 480, "y": 60},
                    "config": {},
                },
            ],
            "edges": [
                {
                    "id": "e1",
                    "source_id": "start",
                    "target_id": "branch",
                    "label": "",
                    "kind": "standard",
                    "priority": 100,
                    "condition": None,
                },
                {
                    "id": "e2",
                    "source_id": "branch",
                    "source_handle_id": "branch-approved",
                    "target_id": "if_finish",
                    "label": "",
                    "kind": "standard",
                    "priority": 100,
                    "condition": None,
                },
                {
                    "id": "e3",
                    "source_id": "branch",
                    "source_handle_id": "branch-fallback",
                    "target_id": "else_finish",
                    "label": "",
                    "kind": "standard",
                    "priority": 100,
                    "condition": None,
                },
            ],
        }

        graph = GraphDefinition.from_dict(payload)
        graph.validate_against_services(self.services)

        serialized_branch_config = graph.to_dict()["nodes"][1]["config"]
        self.assertEqual(serialized_branch_config["branches"][0]["output_handle_id"], "{MATCH_HANDLE}")
        self.assertEqual(
            serialized_branch_config["branches"][0]["root_group"]["children"][0]["value"],
            "{EXPECTED_STATUS}",
        )
        self.assertEqual(serialized_branch_config["else_output_handle_id"], "{ELSE_HANDLE}")

        runtime = GraphRuntime(
            services=self.services,
            max_steps=self.services.config["max_steps"],
            max_visits_per_node=self.services.config["max_visits_per_node"],
        )

        matched_state = runtime.run(graph, {"status": "approved"}, run_id="logic-conditions-env-if")
        self.assertEqual(matched_state.status, "completed")
        self.assertEqual(matched_state.final_output, {"status": "approved"})
        self.assertTrue(any(transition.target_id == "if_finish" for transition in matched_state.transition_history))
        self.assertFalse(any(transition.target_id == "else_finish" for transition in matched_state.transition_history))
        branch_output = matched_state.node_outputs["branch"]
        self.assertEqual(branch_output["metadata"]["selected_handle_id"], "branch-approved")
        self.assertEqual(branch_output["metadata"]["condition_evaluations"][0]["expected_value"], "approved")
        self.assertEqual(branch_output["metadata"]["condition_evaluations"][0]["matched"], True)

        else_state = runtime.run(graph, {"status": "pending"}, run_id="logic-conditions-env-else")
        self.assertEqual(else_state.status, "completed")
        self.assertEqual(else_state.final_output, {"status": "pending"})
        self.assertTrue(any(transition.target_id == "else_finish" for transition in else_state.transition_history))
        self.assertFalse(any(transition.target_id == "if_finish" for transition in else_state.transition_history))
        else_branch_output = else_state.node_outputs["branch"]
        self.assertEqual(else_branch_output["metadata"]["selected_handle_id"], "branch-fallback")
        self.assertEqual(else_branch_output["metadata"]["condition_evaluations"][0]["expected_value"], "approved")
        self.assertEqual(else_branch_output["metadata"]["condition_evaluations"][0]["matched"], False)

    def test_graph_store_crud_and_catalog(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = GraphStore(self.services, path=Path(directory) / "graphs.json")

            catalog = store.catalog()
            self.assertTrue(catalog["node_providers"])
            self.assertTrue(catalog["tools"])
            self.assertTrue(catalog["connection_rules"])

            new_graph = build_example_graph_payload()
            new_graph["graph_id"] = "editable-agent"
            new_graph["name"] = "Editable Agent"

            created = store.create_graph(new_graph)
            self.assertEqual(created["graph_id"], "editable-agent")

            created["description"] = "Updated description"
            updated = store.update_graph("editable-agent", created)
            self.assertEqual(updated["description"], "Updated description")

            store.delete_graph("editable-agent")
            remaining_ids = {graph["graph_id"] for graph in store.list_graphs()}
            self.assertNotIn("editable-agent", remaining_ids)
            self.assertIn("tool-schema-repair", remaining_ids)

    def test_graph_store_keeps_built_in_graphs_and_uses_local_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = GraphStore(self.services, path=Path(directory) / "graphs.json")

            sample = store.get_graph("tool-schema-repair")
            sample["name"] = "Local Sample Override"

            updated = store.update_graph("tool-schema-repair", sample)
            self.assertEqual(updated["name"], "Local Sample Override")
            self.assertEqual(store.get_graph("tool-schema-repair")["name"], "Local Sample Override")

            store.delete_graph("tool-schema-repair")
            self.assertEqual(store.get_graph("tool-schema-repair")["name"], "Tool Schema Repair Example")

    def test_envelope_display_node_shows_input_envelope_and_preserves_payload(self) -> None:
        payload: dict[str, Any] = {
            "graph_id": "display-envelope-agent",
            "name": "Display Envelope Agent",
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
                    "description": "",
                    "position": {"x": 0, "y": 0},
                    "config": {"input_binding": {"type": "input_payload"}},
                },
                {
                    "id": "display",
                    "kind": "data",
                    "category": "data",
                    "label": "Display Envelope",
                    "provider_id": "core.data_display",
                    "provider_label": "Envelope Display Node",
                    "description": "",
                    "position": {"x": 240, "y": 0},
                    "config": {
                        "mode": "template",
                        "template": "This should be ignored.",
                        "show_input_envelope": True,
                        "lock_passthrough": True,
                    },
                },
                {
                    "id": "finish",
                    "kind": "output",
                    "category": "end",
                    "label": "Finish",
                    "provider_id": "core.output",
                    "provider_label": "Core Output Node",
                    "description": "",
                    "position": {"x": 480, "y": 0},
                    "config": {"source_binding": {"type": "latest_payload", "source": "display"}},
                },
            ],
            "edges": [
                {
                    "id": "edge-start-display",
                    "source_id": "start",
                    "target_id": "display",
                    "label": "",
                    "kind": "standard",
                    "priority": 100,
                    "condition": None,
                },
                {
                    "id": "edge-display-finish",
                    "source_id": "display",
                    "target_id": "finish",
                    "label": "",
                    "kind": "standard",
                    "priority": 100,
                    "condition": None,
                },
            ],
        }

        graph = GraphDefinition.from_dict(payload)
        graph.validate_against_services(self.services)
        runtime = GraphRuntime(
            services=self.services,
            max_steps=self.services.config["max_steps"],
            max_visits_per_node=self.services.config["max_visits_per_node"],
        )

        state = runtime.run(graph, "hello envelope")

        self.assertEqual(state.status, "completed")
        self.assertEqual(state.final_output, "hello envelope")
        display_output = state.node_outputs["display"]
        assert isinstance(display_output, dict)
        self.assertEqual(display_output["payload"], "hello envelope")
        self.assertEqual(display_output["artifacts"]["display_envelope"]["payload"], "hello envelope")
        self.assertEqual(display_output["artifacts"]["display_envelope"]["metadata"]["contract"], "message_envelope")
        self.assertEqual(display_output["metadata"]["contract"], "message_envelope")
        self.assertTrue(display_output["metadata"]["display_only"])


if __name__ == "__main__":
    unittest.main()
