from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from graph_agent.api.manager import _with_mock_api_provider_override
from graph_agent.examples.tool_schema_repair import build_example_services
from graph_agent.runtime.documents import load_graph_document


def _agent_payload(agent_id: str) -> dict[str, object]:
    return {
        "agent_id": agent_id,
        "name": f"Agent {agent_id}",
        "start_node_id": f"{agent_id}-input",
        "nodes": [
            {
                "id": f"{agent_id}-input",
                "kind": "input",
                "label": "Input",
                "provider_id": "core.input",
                "provider_label": "Core Input Node",
                "config": {},
            },
            {
                "id": f"{agent_id}-provider",
                "kind": "provider",
                "label": "OpenAI Provider",
                "provider_id": "provider.openai",
                "provider_label": "OpenAI Provider",
                "model_provider_name": "openai",
                "config": {"provider_name": "openai", "model": "gpt-4o"},
            },
            {
                "id": f"{agent_id}-api",
                "kind": "model",
                "label": "API Call",
                "provider_id": "core.api",
                "provider_label": "API Call Node",
                "model_provider_name": "openai",
                "config": {
                    "provider_name": "openai",
                    "provider_binding_node_id": f"{agent_id}-provider",
                    "model": "gpt-4o",
                    "prompt_name": "default",
                    "system_prompt": "You are helpful.",
                    "user_message_template": "{input_payload}",
                },
            },
            {
                "id": f"{agent_id}-output",
                "kind": "output",
                "label": "Output",
                "provider_id": "core.output",
                "provider_label": "Core Output Node",
                "config": {},
            },
        ],
        "edges": [
            {"id": f"{agent_id}-provider-edge", "source_id": f"{agent_id}-provider", "target_id": f"{agent_id}-api", "kind": "binding"},
            {"id": f"{agent_id}-input-edge", "source_id": f"{agent_id}-input", "target_id": f"{agent_id}-api"},
            {"id": f"{agent_id}-output-edge", "source_id": f"{agent_id}-api", "target_id": f"{agent_id}-output"},
        ],
    }


class MockApiProviderOverrideTests(unittest.TestCase):
    def test_override_switches_api_and_provider_nodes_without_mutating_document(self) -> None:
        document = load_graph_document(
            {
                "graph_id": "mock-test",
                "name": "Mock Test",
                "graph_type": "test_environment",
                "agents": [_agent_payload("agent-a"), _agent_payload("agent-b")],
            }
        )

        overridden = _with_mock_api_provider_override(document)
        overridden.validate_against_services(build_example_services())

        original_payload = document.to_dict()
        overridden_payload = overridden.to_dict()
        original_first_agent_nodes = original_payload["agents"][0]["nodes"]
        overridden_first_agent_nodes = overridden_payload["agents"][0]["nodes"]

        self.assertEqual(original_first_agent_nodes[1]["provider_id"], "provider.openai")
        self.assertEqual(original_first_agent_nodes[1]["config"]["provider_name"], "openai")
        self.assertEqual(original_first_agent_nodes[2]["config"]["provider_name"], "openai")

        self.assertEqual(overridden_first_agent_nodes[1]["provider_id"], "provider.mock")
        self.assertEqual(overridden_first_agent_nodes[1]["config"]["provider_name"], "mock")
        self.assertEqual(overridden_first_agent_nodes[1]["config"]["model"], "mock-default")
        self.assertEqual(overridden_first_agent_nodes[2]["config"]["provider_name"], "mock")
        self.assertEqual(overridden_first_agent_nodes[2]["config"]["model"], "mock-default")


if __name__ == "__main__":
    unittest.main()
