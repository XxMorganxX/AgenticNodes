from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from graph_agent.api.graph_store import GraphStore
from graph_agent.examples.tool_schema_repair import build_example_services
from graph_agent.runtime.core import GraphDefinition, GraphValidationError
from graph_agent.runtime.engine import GraphRuntime


def structured_payload_builder_graph_payload(
    graph_id: str = "structured-payload-builder-graph",
    *,
    node_config: dict[str, object] | None = None,
) -> dict[str, object]:
    resolved_node_config: dict[str, object] = {
        "mode": "structured_payload_builder",
        "input_binding": {"type": "input_payload"},
        "template_json": '{\n  "name": "",\n  "domain": "",\n  "linkedin_url": "",\n  "email": ""\n}',
        "case_sensitive": False,
        "max_matches_per_field": 25,
    }
    if node_config:
        resolved_node_config.update(node_config)
    return {
        "graph_id": graph_id,
        "name": "Structured Payload Builder Graph",
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
                "id": "builder",
                "kind": "data",
                "category": "data",
                "label": "Structured Payload Builder",
                "provider_id": "core.structured_payload_builder",
                "provider_label": "Structured Payload Builder",
                "config": resolved_node_config,
                "position": {"x": 220, "y": 0},
            },
            {
                "id": "finish",
                "kind": "output",
                "category": "end",
                "label": "Finish",
                "provider_id": "core.output",
                "provider_label": "Core Output Node",
                "config": {"source_binding": {"type": "latest_payload", "source": "builder"}},
                "position": {"x": 440, "y": 0},
            },
        ],
        "edges": [
            {"id": "e1", "source_id": "start", "target_id": "builder", "label": "", "kind": "standard", "priority": 100},
            {"id": "e2", "source_id": "builder", "target_id": "finish", "label": "", "kind": "standard", "priority": 100},
        ],
    }


class StructuredPayloadBuilderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.services = build_example_services()

    def _runtime(self) -> GraphRuntime:
        return GraphRuntime(
            services=self.services,
            max_steps=self.services.config["max_steps"],
            max_visits_per_node=self.services.config["max_visits_per_node"],
        )

    def test_provider_appears_in_catalog(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = GraphStore(self.services, path=Path(directory) / "graphs.json")
            catalog = store.catalog()

        provider = next(
            candidate
            for candidate in catalog["node_providers"]
            if candidate["provider_id"] == "core.structured_payload_builder"
        )
        self.assertEqual(provider["default_config"]["mode"], "structured_payload_builder")
        self.assertFalse(provider["default_config"]["case_sensitive"])

    def test_builder_fills_missing_fields_and_preserves_explicit_values(self) -> None:
        graph = GraphDefinition.from_dict(
            structured_payload_builder_graph_payload(
                node_config={
                    "template_json": json.dumps(
                        {
                            "name": "",
                            "domain": "openai.com",
                            "linkedin_url": "",
                            "email": "",
                        }
                    )
                }
            )
        )
        graph.validate_against_services(self.services)
        runtime = self._runtime()

        state = runtime.run(
            graph,
            {
                "person": {
                    "name": "Taylor Doe",
                    "linkedin_url": "https://www.linkedin.com/in/taylor-doe/",
                },
                "contact": {"email": "taylor@openai.com"},
                "company": {"domain": "example.com"},
            },
        )

        self.assertEqual(state.status, "completed")
        self.assertEqual(
            state.final_output,
            {
                "name": "Taylor Doe",
                "domain": "openai.com",
                "linkedin_url": "https://www.linkedin.com/in/taylor-doe/",
                "email": "taylor@openai.com",
            },
        )
        builder_output = state.node_outputs["builder"]
        self.assertEqual(builder_output["metadata"]["filled_field_count"], 3)
        self.assertEqual(builder_output["metadata"]["preserved_field_count"], 1)

    def test_builder_uses_parent_context_for_nested_objects(self) -> None:
        graph = GraphDefinition.from_dict(
            structured_payload_builder_graph_payload(
                "structured-payload-builder-nested",
                node_config={
                    "template_json": json.dumps(
                        {
                            "person": {
                                "name": "",
                                "headline": "",
                            },
                            "email": "",
                        }
                    )
                }
            )
        )
        graph.validate_against_services(self.services)
        runtime = self._runtime()

        state = runtime.run(
            graph,
            {
                "person": {
                    "name": "Taylor Doe",
                    "headline": "Engineer",
                },
                "contact": {"email": "taylor@example.com"},
            },
        )

        self.assertEqual(state.status, "completed")
        self.assertEqual(state.final_output["person"]["name"], "Taylor Doe")
        self.assertEqual(state.final_output["person"]["headline"], "Engineer")
        self.assertEqual(state.final_output["email"], "taylor@example.com")

    def test_builder_parses_json_string_input_payload(self) -> None:
        graph = GraphDefinition.from_dict(structured_payload_builder_graph_payload("structured-payload-string-input"))
        graph.validate_against_services(self.services)
        runtime = self._runtime()

        state = runtime.run(
            graph,
            json.dumps(
                {
                    "person": {"name": "Taylor Doe", "linkedin_url": "https://www.linkedin.com/in/taylor-doe/"},
                    "contact": {"email": "taylor@example.com"},
                    "domain": "example.com",
                }
            ),
        )

        self.assertEqual(state.status, "completed")
        self.assertEqual(state.final_output["name"], "Taylor Doe")
        self.assertEqual(state.final_output["domain"], "example.com")

    def test_builder_reads_from_nested_payload_body_and_ignores_outer_wrapper_fields(self) -> None:
        graph = GraphDefinition.from_dict(structured_payload_builder_graph_payload("structured-payload-body-only"))
        graph.validate_against_services(self.services)
        runtime = self._runtime()

        state = runtime.run(
            graph,
            {
                "email": "outer@example.com",
                "domain": "outer.example.com",
                "payload": {
                    "person": {
                        "fullName": "Taylor Doe",
                    },
                    "contact": {
                        "workEmailAddress": "inner@example.com",
                    },
                    "company": {
                        "websiteDomain": "example.com",
                    },
                    "profiles": {
                        "linkedinProfileUrl": "https://www.linkedin.com/in/taylor-doe/",
                    },
                },
            },
        )

        self.assertEqual(state.status, "completed")
        self.assertEqual(
            state.final_output,
            {
                "name": "Taylor Doe",
                "domain": "example.com",
                "linkedin_url": "https://www.linkedin.com/in/taylor-doe/",
                "email": "inner@example.com",
            },
        )

    def test_builder_matches_multi_keyword_alias_fields_deterministically(self) -> None:
        graph = GraphDefinition.from_dict(
            structured_payload_builder_graph_payload(
                "structured-payload-keyword-aliases",
                node_config={
                    "template_json": json.dumps(
                        {
                            "first_name": "",
                            "last_name": "",
                            "organization_name": "",
                            "email": "",
                            "linkedin_url": "",
                        }
                    )
                },
            )
        )
        graph.validate_against_services(self.services)
        runtime = self._runtime()

        state = runtime.run(
            graph,
            {
                "payload": {
                    "profile": {
                        "givenName": "Taylor",
                        "familyName": "Doe",
                    },
                    "account": {
                        "companyName": "OpenAI",
                    },
                    "contact": {
                        "primaryWorkEmailAddress": "taylor@openai.com",
                    },
                    "social": {
                        "linkedin": {
                            "url": "https://www.linkedin.com/in/taylor-doe/",
                        }
                    },
                }
            },
        )

        self.assertEqual(state.status, "completed")
        self.assertEqual(state.final_output["first_name"], "Taylor")
        self.assertEqual(state.final_output["last_name"], "Doe")
        self.assertEqual(state.final_output["organization_name"], "OpenAI")
        self.assertEqual(state.final_output["email"], "taylor@openai.com")
        self.assertEqual(state.final_output["linkedin_url"], "https://www.linkedin.com/in/taylor-doe/")
        field_matches = state.node_outputs["builder"]["artifacts"]["field_matches"]
        email_match = next(match for match in field_matches if match["target_path"] == "email")
        linkedin_match = next(match for match in field_matches if match["target_path"] == "linkedin_url")
        self.assertEqual(email_match["match_type"], "alias_keywords")
        self.assertEqual(linkedin_match["match_type"], "alias_path_keywords")

    def test_invalid_template_json_is_rejected_during_validation(self) -> None:
        graph = GraphDefinition.from_dict(
            structured_payload_builder_graph_payload(
                "structured-payload-invalid-template",
                node_config={"template_json": "{not-json}"},
            )
        )

        with self.assertRaises(GraphValidationError):
            graph.validate_against_services(self.services)


if __name__ == "__main__":
    unittest.main()
