from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from graph_agent.api.manager import GraphRunManager
from graph_agent.examples.tool_schema_repair import build_example_services
from graph_agent.providers.base import ModelRequest, ModelResponse, ProviderPreflightResult
from graph_agent.runtime.core import GraphDefinition, GraphValidationError
from graph_agent.runtime.engine import GraphRuntime


class PayloadListEchoProvider:
    name = "payload_list_echo"

    def __init__(self) -> None:
        self.user_messages: list[str] = []

    def generate(self, request: ModelRequest) -> ModelResponse:
        user_message = request.messages[-1].content if request.messages else ""
        self.user_messages.append(user_message)
        return ModelResponse(
            content=user_message,
            structured_output={
                "message": user_message,
                "need_tool": False,
                "tool_calls": [],
            },
        )

    def preflight(self, provider_config=None) -> ProviderPreflightResult:
        return ProviderPreflightResult(
            status="available",
            ok=True,
            message="Payload list echo provider is available for tests.",
            details={"backend_type": "test"},
        )


class PayloadListIteratorTests(unittest.TestCase):
    def test_catalog_registers_payload_list_iterator(self) -> None:
        manager = GraphRunManager(services=build_example_services())
        catalog = manager.get_catalog()
        provider = next(p for p in catalog["node_providers"] if p["provider_id"] == "core.payload_list_iterator")
        self.assertEqual(provider["category"], "control_flow_unit")
        self.assertEqual(provider["node_kind"], "control_flow_unit")
        self.assertEqual(provider["default_config"], {"mode": "payload_list_iterator", "start_index": 0})

    def test_runtime_iterates_list_of_dicts(self) -> None:
        services = build_example_services()
        provider = PayloadListEchoProvider()
        services.model_providers["payload_list_echo"] = provider
        runtime = GraphRuntime(
            services=services,
            max_steps=services.config["max_steps"],
            max_visits_per_node=services.config["max_visits_per_node"],
        )
        graph_payload = {
            "graph_id": "payload-list-iterator-graph",
            "name": "Payload List Iterator Graph",
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
                    "id": "iter",
                    "kind": "control_flow_unit",
                    "category": "control_flow_unit",
                    "label": "Payload List Iterator",
                    "provider_id": "core.payload_list_iterator",
                    "provider_label": "Payload List Iterator",
                    "config": {"mode": "payload_list_iterator", "start_index": 0},
                    "position": {"x": 100, "y": 0},
                },
                {
                    "id": "model",
                    "kind": "model",
                    "category": "api",
                    "label": "Model",
                    "provider_id": "core.api",
                    "provider_label": "API Call Node",
                    "model_provider_name": "payload_list_echo",
                    "prompt_name": "payload_list_prompt",
                    "config": {
                        "provider_name": "payload_list_echo",
                        "prompt_name": "payload_list_prompt",
                        "system_prompt": "Process the current item.",
                        "user_message_template": "{input_payload}",
                        "response_mode": "message",
                    },
                    "position": {"x": 220, "y": 0},
                },
                {
                    "id": "finish",
                    "kind": "output",
                    "category": "end",
                    "label": "Finish",
                    "provider_id": "core.output",
                    "provider_label": "Core Output Node",
                    "config": {"source_binding": {"type": "latest_payload", "source": "model"}},
                    "position": {"x": 340, "y": 0},
                },
            ],
            "edges": [
                {"id": "e1", "source_id": "start", "target_id": "iter", "label": "", "kind": "standard", "priority": 100},
                {
                    "id": "e2",
                    "source_id": "iter",
                    "source_handle_id": "control-flow-loop-body",
                    "target_id": "model",
                    "label": "",
                    "kind": "standard",
                    "priority": 100,
                },
                {"id": "e3", "source_id": "model", "target_id": "finish", "label": "", "kind": "standard", "priority": 100},
            ],
        }
        graph = GraphDefinition.from_dict(graph_payload)
        graph.validate_against_services(services)
        input_list = [{"city": "Seattle"}, {"city": "Portland"}]
        state = runtime.run(graph, input_list, run_id="payload-list-iterator-runtime")

        self.assertEqual(state.status, "completed")
        self.assertEqual(len(provider.user_messages), 2)
        self.assertIn('"city": "Seattle"', provider.user_messages[0])
        self.assertIn('"city": "Portland"', provider.user_messages[1])
        self.assertIn('"input_index": 1', provider.user_messages[0])
        self.assertIn('"input_index": 2', provider.user_messages[1])
        self.assertIn('"item_index": 1', provider.user_messages[0])
        self.assertIn('"item_index": 2', provider.user_messages[1])
        self.assertEqual(state.visit_counts.get("model"), 2)
        self.assertEqual(state.iterator_states["iter"]["status"], "completed")
        self.assertEqual(state.iterator_states["iter"]["current_row_index"], 2)
        self.assertEqual(state.iterator_states["iter"]["total_rows"], 2)

    def test_empty_list_completes_without_loop_body(self) -> None:
        services = build_example_services()
        provider = PayloadListEchoProvider()
        services.model_providers["payload_list_echo"] = provider
        runtime = GraphRuntime(
            services=services,
            max_steps=services.config["max_steps"],
            max_visits_per_node=services.config["max_visits_per_node"],
        )
        graph_payload = {
            "graph_id": "payload-list-empty-graph",
            "name": "Empty List Graph",
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
                    "id": "iter",
                    "kind": "control_flow_unit",
                    "category": "control_flow_unit",
                    "label": "Payload List Iterator",
                    "provider_id": "core.payload_list_iterator",
                    "provider_label": "Payload List Iterator",
                    "config": {"mode": "payload_list_iterator", "start_index": 0},
                    "position": {"x": 100, "y": 0},
                },
                {
                    "id": "model",
                    "kind": "model",
                    "category": "api",
                    "label": "Model",
                    "provider_id": "core.api",
                    "provider_label": "API Call Node",
                    "model_provider_name": "payload_list_echo",
                    "prompt_name": "payload_list_prompt",
                    "config": {
                        "provider_name": "payload_list_echo",
                        "prompt_name": "payload_list_prompt",
                        "system_prompt": "Process.",
                        "user_message_template": "{input_payload}",
                        "response_mode": "message",
                    },
                    "position": {"x": 220, "y": 0},
                },
                {
                    "id": "finish",
                    "kind": "output",
                    "category": "end",
                    "label": "Finish",
                    "provider_id": "core.output",
                    "provider_label": "Core Output Node",
                    "config": {"source_binding": {"type": "latest_payload", "source": "model"}},
                    "position": {"x": 340, "y": 0},
                },
            ],
            "edges": [
                {"id": "e1", "source_id": "start", "target_id": "iter", "label": "", "kind": "standard", "priority": 100},
                {
                    "id": "e2",
                    "source_id": "iter",
                    "source_handle_id": "control-flow-loop-body",
                    "target_id": "model",
                    "label": "",
                    "kind": "standard",
                    "priority": 100,
                },
                {"id": "e3", "source_id": "model", "target_id": "finish", "label": "", "kind": "standard", "priority": 100},
            ],
        }
        graph = GraphDefinition.from_dict(graph_payload)
        graph.validate_against_services(services)
        state = runtime.run(graph, [], run_id="payload-list-empty")

        self.assertEqual(state.status, "completed")
        self.assertEqual(len(provider.user_messages), 0)
        self.assertIsNone(state.visit_counts.get("model"))
        self.assertEqual(state.iterator_states["iter"]["status"], "completed")
        self.assertEqual(state.iterator_states["iter"]["total_rows"], 0)

    def test_invalid_payload_type_fails(self) -> None:
        services = build_example_services()
        provider = PayloadListEchoProvider()
        services.model_providers["payload_list_echo"] = provider
        runtime = GraphRuntime(
            services=services,
            max_steps=services.config["max_steps"],
            max_visits_per_node=services.config["max_visits_per_node"],
        )
        graph_payload = {
            "graph_id": "payload-list-invalid-graph",
            "name": "Invalid Payload Graph",
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
                    "id": "iter",
                    "kind": "control_flow_unit",
                    "category": "control_flow_unit",
                    "label": "Payload List Iterator",
                    "provider_id": "core.payload_list_iterator",
                    "provider_label": "Payload List Iterator",
                    "config": {"mode": "payload_list_iterator", "start_index": 0},
                    "position": {"x": 100, "y": 0},
                },
                {
                    "id": "model",
                    "kind": "model",
                    "category": "api",
                    "label": "Model",
                    "provider_id": "core.api",
                    "provider_label": "API Call Node",
                    "model_provider_name": "payload_list_echo",
                    "prompt_name": "payload_list_prompt",
                    "config": {
                        "provider_name": "payload_list_echo",
                        "prompt_name": "payload_list_prompt",
                        "system_prompt": "Process.",
                        "user_message_template": "{input_payload}",
                        "response_mode": "message",
                    },
                    "position": {"x": 220, "y": 0},
                },
                {
                    "id": "finish",
                    "kind": "output",
                    "category": "end",
                    "label": "Finish",
                    "provider_id": "core.output",
                    "provider_label": "Core Output Node",
                    "config": {"source_binding": {"type": "latest_payload", "source": "model"}},
                    "position": {"x": 340, "y": 0},
                },
            ],
            "edges": [
                {"id": "e1", "source_id": "start", "target_id": "iter", "label": "", "kind": "standard", "priority": 100},
                {
                    "id": "e2",
                    "source_id": "iter",
                    "source_handle_id": "control-flow-loop-body",
                    "target_id": "model",
                    "label": "",
                    "kind": "standard",
                    "priority": 100,
                },
                {"id": "e3", "source_id": "model", "target_id": "finish", "label": "", "kind": "standard", "priority": 100},
            ],
        }
        graph = GraphDefinition.from_dict(graph_payload)
        graph.validate_against_services(services)
        state = runtime.run(graph, {"not": "a list"}, run_id="payload-list-invalid")

        self.assertEqual(state.status, "failed")
        err = state.node_errors.get("iter")
        self.assertIsNotNone(err)
        self.assertEqual(err.get("type"), "payload_list_iterator_invalid_payload")

    def test_invalid_list_item_fails(self) -> None:
        services = build_example_services()
        provider = PayloadListEchoProvider()
        services.model_providers["payload_list_echo"] = provider
        runtime = GraphRuntime(
            services=services,
            max_steps=services.config["max_steps"],
            max_visits_per_node=services.config["max_visits_per_node"],
        )
        graph_payload = {
            "graph_id": "payload-list-bad-item-graph",
            "name": "Bad Item Graph",
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
                    "id": "iter",
                    "kind": "control_flow_unit",
                    "category": "control_flow_unit",
                    "label": "Payload List Iterator",
                    "provider_id": "core.payload_list_iterator",
                    "provider_label": "Payload List Iterator",
                    "config": {"mode": "payload_list_iterator", "start_index": 0},
                    "position": {"x": 100, "y": 0},
                },
                {
                    "id": "model",
                    "kind": "model",
                    "category": "api",
                    "label": "Model",
                    "provider_id": "core.api",
                    "provider_label": "API Call Node",
                    "model_provider_name": "payload_list_echo",
                    "prompt_name": "payload_list_prompt",
                    "config": {
                        "provider_name": "payload_list_echo",
                        "prompt_name": "payload_list_prompt",
                        "system_prompt": "Process.",
                        "user_message_template": "{input_payload}",
                        "response_mode": "message",
                    },
                    "position": {"x": 220, "y": 0},
                },
                {
                    "id": "finish",
                    "kind": "output",
                    "category": "end",
                    "label": "Finish",
                    "provider_id": "core.output",
                    "provider_label": "Core Output Node",
                    "config": {"source_binding": {"type": "latest_payload", "source": "model"}},
                    "position": {"x": 340, "y": 0},
                },
            ],
            "edges": [
                {"id": "e1", "source_id": "start", "target_id": "iter", "label": "", "kind": "standard", "priority": 100},
                {
                    "id": "e2",
                    "source_id": "iter",
                    "source_handle_id": "control-flow-loop-body",
                    "target_id": "model",
                    "label": "",
                    "kind": "standard",
                    "priority": 100,
                },
                {"id": "e3", "source_id": "model", "target_id": "finish", "label": "", "kind": "standard", "priority": 100},
            ],
        }
        graph = GraphDefinition.from_dict(graph_payload)
        graph.validate_against_services(services)
        state = runtime.run(graph, [{"ok": 1}, "nope"], run_id="payload-list-bad-item")

        self.assertEqual(state.status, "failed")
        err = state.node_errors.get("iter")
        self.assertIsNotNone(err)
        self.assertEqual(err.get("type"), "payload_list_iterator_invalid_item")
        self.assertEqual(err.get("item_index"), 1)

    def test_start_index_skips_leading_items(self) -> None:
        services = build_example_services()
        provider = PayloadListEchoProvider()
        services.model_providers["payload_list_echo"] = provider
        runtime = GraphRuntime(
            services=services,
            max_steps=services.config["max_steps"],
            max_visits_per_node=services.config["max_visits_per_node"],
        )
        graph_payload = {
            "graph_id": "payload-list-start-idx-graph",
            "name": "Start Index Graph",
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
                    "id": "iter",
                    "kind": "control_flow_unit",
                    "category": "control_flow_unit",
                    "label": "Payload List Iterator",
                    "provider_id": "core.payload_list_iterator",
                    "provider_label": "Payload List Iterator",
                    "config": {"mode": "payload_list_iterator", "start_index": 1},
                    "position": {"x": 100, "y": 0},
                },
                {
                    "id": "model",
                    "kind": "model",
                    "category": "api",
                    "label": "Model",
                    "provider_id": "core.api",
                    "provider_label": "API Call Node",
                    "model_provider_name": "payload_list_echo",
                    "prompt_name": "payload_list_prompt",
                    "config": {
                        "provider_name": "payload_list_echo",
                        "prompt_name": "payload_list_prompt",
                        "system_prompt": "Process.",
                        "user_message_template": "{input_payload}",
                        "response_mode": "message",
                    },
                    "position": {"x": 220, "y": 0},
                },
                {
                    "id": "finish",
                    "kind": "output",
                    "category": "end",
                    "label": "Finish",
                    "provider_id": "core.output",
                    "provider_label": "Core Output Node",
                    "config": {"source_binding": {"type": "latest_payload", "source": "model"}},
                    "position": {"x": 340, "y": 0},
                },
            ],
            "edges": [
                {"id": "e1", "source_id": "start", "target_id": "iter", "label": "", "kind": "standard", "priority": 100},
                {
                    "id": "e2",
                    "source_id": "iter",
                    "source_handle_id": "control-flow-loop-body",
                    "target_id": "model",
                    "label": "",
                    "kind": "standard",
                    "priority": 100,
                },
                {"id": "e3", "source_id": "model", "target_id": "finish", "label": "", "kind": "standard", "priority": 100},
            ],
        }
        graph = GraphDefinition.from_dict(graph_payload)
        graph.validate_against_services(services)
        payload = [{"n": "a"}, {"n": "b"}, {"n": "c"}]
        state = runtime.run(graph, payload, run_id="payload-list-start-index")

        self.assertEqual(state.status, "completed")
        self.assertEqual(len(provider.user_messages), 2)
        self.assertIn('"n": "b"', provider.user_messages[0])
        self.assertIn('"n": "c"', provider.user_messages[1])
        self.assertEqual(state.iterator_states["iter"]["total_rows"], 2)
        self.assertEqual(state.iterator_states["iter"]["start_index"], 1)
        self.assertEqual(state.iterator_states["iter"]["source_item_count"], 3)

    def test_start_index_beyond_list_processes_nothing(self) -> None:
        services = build_example_services()
        provider = PayloadListEchoProvider()
        services.model_providers["payload_list_echo"] = provider
        runtime = GraphRuntime(
            services=services,
            max_steps=services.config["max_steps"],
            max_visits_per_node=services.config["max_visits_per_node"],
        )
        graph_payload = {
            "graph_id": "payload-list-start-overflow-graph",
            "name": "Start Index Overflow Graph",
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
                    "id": "iter",
                    "kind": "control_flow_unit",
                    "category": "control_flow_unit",
                    "label": "Payload List Iterator",
                    "provider_id": "core.payload_list_iterator",
                    "provider_label": "Payload List Iterator",
                    "config": {"mode": "payload_list_iterator", "start_index": 10},
                    "position": {"x": 100, "y": 0},
                },
                {
                    "id": "model",
                    "kind": "model",
                    "category": "api",
                    "label": "Model",
                    "provider_id": "core.api",
                    "provider_label": "API Call Node",
                    "model_provider_name": "payload_list_echo",
                    "prompt_name": "payload_list_prompt",
                    "config": {
                        "provider_name": "payload_list_echo",
                        "prompt_name": "payload_list_prompt",
                        "system_prompt": "Process.",
                        "user_message_template": "{input_payload}",
                        "response_mode": "message",
                    },
                    "position": {"x": 220, "y": 0},
                },
                {
                    "id": "finish",
                    "kind": "output",
                    "category": "end",
                    "label": "Finish",
                    "provider_id": "core.output",
                    "provider_label": "Core Output Node",
                    "config": {"source_binding": {"type": "latest_payload", "source": "model"}},
                    "position": {"x": 340, "y": 0},
                },
            ],
            "edges": [
                {"id": "e1", "source_id": "start", "target_id": "iter", "label": "", "kind": "standard", "priority": 100},
                {
                    "id": "e2",
                    "source_id": "iter",
                    "source_handle_id": "control-flow-loop-body",
                    "target_id": "model",
                    "label": "",
                    "kind": "standard",
                    "priority": 100,
                },
                {"id": "e3", "source_id": "model", "target_id": "finish", "label": "", "kind": "standard", "priority": 100},
            ],
        }
        graph = GraphDefinition.from_dict(graph_payload)
        graph.validate_against_services(services)
        state = runtime.run(graph, [{"x": 1}, {"x": 2}], run_id="payload-list-start-overflow")

        self.assertEqual(state.status, "completed")
        self.assertEqual(len(provider.user_messages), 0)
        self.assertEqual(state.iterator_states["iter"]["total_rows"], 0)
        self.assertEqual(state.iterator_states["iter"]["start_index"], 2)
        self.assertEqual(state.iterator_states["iter"]["source_item_count"], 2)

    def test_validation_rejects_non_loop_body_handle(self) -> None:
        services = build_example_services()
        graph_payload = {
            "graph_id": "payload-list-handle-graph",
            "name": "Bad Handle Graph",
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
                    "id": "iter",
                    "kind": "control_flow_unit",
                    "category": "control_flow_unit",
                    "label": "Payload List Iterator",
                    "provider_id": "core.payload_list_iterator",
                    "provider_label": "Payload List Iterator",
                    "config": {"mode": "payload_list_iterator", "start_index": 0},
                    "position": {"x": 100, "y": 0},
                },
                {
                    "id": "model",
                    "kind": "model",
                    "category": "api",
                    "label": "Model",
                    "provider_id": "core.api",
                    "provider_label": "API Call Node",
                    "model_provider_name": "mock",
                    "prompt_name": "p",
                    "config": {
                        "provider_name": "mock",
                        "prompt_name": "p",
                        "system_prompt": "x",
                        "user_message_template": "{input_payload}",
                        "response_mode": "message",
                    },
                    "position": {"x": 220, "y": 0},
                },
                {
                    "id": "finish",
                    "kind": "output",
                    "category": "end",
                    "label": "Finish",
                    "provider_id": "core.output",
                    "provider_label": "Core Output Node",
                    "config": {"source_binding": {"type": "latest_payload", "source": "model"}},
                    "position": {"x": 340, "y": 0},
                },
            ],
            "edges": [
                {"id": "e1", "source_id": "start", "target_id": "iter", "label": "", "kind": "standard", "priority": 100},
                {
                    "id": "e2",
                    "source_id": "iter",
                    "source_handle_id": "control-flow-if",
                    "target_id": "model",
                    "label": "",
                    "kind": "standard",
                    "priority": 100,
                },
                {"id": "e3", "source_id": "model", "target_id": "finish", "label": "", "kind": "standard", "priority": 100},
            ],
        }
        graph = GraphDefinition.from_dict(graph_payload)
        with self.assertRaises(GraphValidationError) as ctx:
            graph.validate_against_services(services)
        self.assertIn("unsupported output handle", str(ctx.exception).lower())


if __name__ == "__main__":
    unittest.main()
