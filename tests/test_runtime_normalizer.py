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
from graph_agent.runtime.core import GraphDefinition
from graph_agent.runtime.engine import GraphRuntime


def runtime_field_extractor_graph_payload(
    graph_id: str = "runtime-field-extractor-graph",
    *,
    node_config: dict[str, object] | None = None,
) -> dict[str, object]:
    resolved_node_config: dict[str, object] = {
        "mode": "runtime_normalizer",
        "input_binding": {"type": "input_payload"},
        "field_name": "url",
        "fallback_field_names": [],
        "preferred_path": "",
        "case_sensitive": False,
        "max_matches": 25,
    }
    if node_config:
        resolved_node_config.update(node_config)
    return {
        "graph_id": graph_id,
        "name": "Runtime Field Extractor Graph",
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
                "id": "extract",
                "kind": "data",
                "category": "data",
                "label": "Payload Field Extractor",
                "provider_id": "core.runtime_normalizer",
                "provider_label": "Payload Field Extractor",
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
                "config": {"source_binding": {"type": "latest_payload", "source": "extract"}},
                "position": {"x": 440, "y": 0},
            },
        ],
        "edges": [
            {"id": "e1", "source_id": "start", "target_id": "extract", "label": "", "kind": "standard", "priority": 100},
            {"id": "e2", "source_id": "extract", "target_id": "finish", "label": "", "kind": "standard", "priority": 100},
        ],
    }


def upstream_envelope_runtime_field_extractor_graph_payload() -> dict[str, object]:
    return {
        "graph_id": "runtime-field-upstream-envelope-graph",
        "name": "Runtime Field Extractor Upstream Envelope Graph",
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
                "id": "prepare",
                "kind": "data",
                "category": "data",
                "label": "Prepare Envelope",
                "provider_id": "core.data",
                "provider_label": "Core Data Node",
                "config": {
                    "mode": "template",
                    "input_binding": {"type": "input_payload"},
                    "template": '{{"record": {{"LinkedInURL": "https://www.linkedin.com/in/example/"}}}}',
                },
                "position": {"x": 220, "y": 0},
            },
            {
                "id": "extract",
                "kind": "data",
                "category": "data",
                "label": "Payload Field Extractor",
                "provider_id": "core.runtime_normalizer",
                "provider_label": "Payload Field Extractor",
                "config": {
                    "mode": "runtime_normalizer",
                    "input_binding": {"type": "input_payload"},
                    "field_name": "LinkedInURL",
                    "fallback_field_names": [],
                    "preferred_path": "",
                    "case_sensitive": False,
                    "max_matches": 25,
                },
                "position": {"x": 440, "y": 0},
            },
            {
                "id": "finish",
                "kind": "output",
                "category": "end",
                "label": "Finish",
                "provider_id": "core.output",
                "provider_label": "Core Output Node",
                "config": {"source_binding": {"type": "latest_payload", "source": "extract"}},
                "position": {"x": 660, "y": 0},
            },
        ],
        "edges": [
            {"id": "e1", "source_id": "start", "target_id": "prepare", "label": "", "kind": "standard", "priority": 100},
            {"id": "e2", "source_id": "prepare", "target_id": "extract", "label": "", "kind": "standard", "priority": 100},
            {"id": "e3", "source_id": "extract", "target_id": "finish", "label": "", "kind": "standard", "priority": 100},
        ],
    }


class RuntimeFieldExtractorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.services = build_example_services()

    def _runtime(self) -> GraphRuntime:
        return GraphRuntime(
            services=self.services,
            max_steps=self.services.config["max_steps"],
            max_visits_per_node=self.services.config["max_visits_per_node"],
        )

    def test_field_extractor_provider_appears_in_catalog(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = GraphStore(self.services, path=Path(directory) / "graphs.json")
            catalog = store.catalog()

        provider = next(candidate for candidate in catalog["node_providers"] if candidate["provider_id"] == "core.runtime_normalizer")
        self.assertEqual(provider["default_config"]["mode"], "runtime_normalizer")
        self.assertEqual(provider["default_config"]["field_name"], "url")
        self.assertEqual(provider["default_config"]["max_matches"], 25)

    def test_extracts_field_recursively_when_structure_is_unknown(self) -> None:
        graph = GraphDefinition.from_dict(runtime_field_extractor_graph_payload(node_config={"field_name": "headline"}))
        graph.validate_against_services(self.services)

        runtime = self._runtime()
        state = runtime.run(
            graph,
            {
                "person": {
                    "profile": {
                        "headline": "Engineer",
                    }
                }
            },
            run_id="run-field-extractor-recursive",
        )

        self.assertEqual(state.status, "completed")
        self.assertEqual(state.final_output, "Engineer")
        self.assertEqual(state.node_outputs["extract"]["metadata"]["matched_path"], "payload.person.profile.headline")

    def test_preferred_path_wins_when_present(self) -> None:
        graph = GraphDefinition.from_dict(
            runtime_field_extractor_graph_payload(
                "runtime-field-preferred",
                node_config={"field_name": "url", "preferred_path": "result.primary.url"},
            )
        )
        graph.validate_against_services(self.services)

        runtime = self._runtime()
        state = runtime.run(
            graph,
            {
                "result": {
                    "primary": {"url": "https://preferred.example"},
                    "secondary": {"url": "https://secondary.example"},
                }
            },
            run_id="run-field-extractor-preferred",
        )

        self.assertEqual(state.status, "completed")
        self.assertEqual(state.final_output, "https://preferred.example")
        self.assertEqual(state.node_outputs["extract"]["metadata"]["matched_path"], "result.primary.url")

    def test_fallback_field_names_are_used(self) -> None:
        graph = GraphDefinition.from_dict(
            runtime_field_extractor_graph_payload(
                "runtime-field-fallback",
                node_config={"field_name": "url", "fallback_field_names": "profile_url\nlinkedin_url"},
            )
        )
        graph.validate_against_services(self.services)

        runtime = self._runtime()
        state = runtime.run(
            graph,
            {"person": {"linkedin_url": "https://www.linkedin.com/in/example/"}},
            run_id="run-field-extractor-fallback",
        )

        self.assertEqual(state.status, "completed")
        self.assertEqual(state.final_output, "https://www.linkedin.com/in/example/")
        self.assertEqual(state.node_outputs["extract"]["metadata"]["matched_path"], "payload.person.linkedin_url")

    def test_extracts_multiple_fields_into_an_object_payload(self) -> None:
        graph = GraphDefinition.from_dict(
            runtime_field_extractor_graph_payload(
                "runtime-field-multiple",
                node_config={"field_name": "headline\ncompany"},
            )
        )
        graph.validate_against_services(self.services)

        runtime = self._runtime()
        state = runtime.run(
            graph,
            {
                "person": {
                    "profile": {
                        "headline": "Engineer",
                        "company": "Acme",
                    }
                }
            },
            run_id="run-field-extractor-multiple",
        )

        self.assertEqual(state.status, "completed")
        self.assertEqual(
            state.final_output,
            {
                "headline": "Engineer",
                "company": "Acme",
            },
        )
        self.assertEqual(
            state.node_outputs["extract"]["metadata"]["matched_paths_by_field"],
            {
                "headline": "payload.person.profile.headline",
                "company": "payload.person.profile.company",
            },
        )
        self.assertEqual(state.node_outputs["extract"]["metadata"]["missing_field_names"], [])

    def test_multiple_fields_fail_when_any_requested_field_is_missing(self) -> None:
        graph = GraphDefinition.from_dict(
            runtime_field_extractor_graph_payload(
                "runtime-field-multiple-missing",
                node_config={"field_name": "headline\ncompany"},
            )
        )
        graph.validate_against_services(self.services)

        runtime = self._runtime()
        state = runtime.run(
            graph,
            {
                "person": {
                    "profile": {
                        "headline": "Engineer",
                    }
                }
            },
            run_id="run-field-extractor-multiple-missing",
        )

        self.assertEqual(state.status, "failed")
        self.assertEqual(state.terminal_error["type"], "fields_not_found")
        self.assertEqual(state.terminal_error["missing_field_names"], ["company"])
        self.assertEqual(
            state.node_outputs["extract"]["payload"],
            {
                "headline": "Engineer",
            },
        )

    def test_extracts_field_from_full_envelope_not_just_payload(self) -> None:
        graph = GraphDefinition.from_dict(runtime_field_extractor_graph_payload("runtime-field-envelope", node_config={"field_name": "contract"}))
        graph.validate_against_services(self.services)

        runtime = self._runtime()
        state = runtime.run(
            graph,
            {
                "schema_version": "1.0",
                "payload": {"person": {"name": "Taylor"}},
                "metadata": {"contract": "data_envelope"},
            },
            run_id="run-field-extractor-envelope",
        )

        self.assertEqual(state.status, "completed")
        self.assertEqual(state.final_output, "data_envelope")
        self.assertEqual(state.node_outputs["extract"]["metadata"]["matched_path"], "payload.metadata.contract")

    def test_extracts_field_from_json_string_envelope_payload(self) -> None:
        graph = GraphDefinition.from_dict(runtime_field_extractor_graph_payload("runtime-field-envelope-string", node_config={"field_name": "headline"}))
        graph.validate_against_services(self.services)

        runtime = self._runtime()
        state = runtime.run(
            graph,
            json.dumps(
                {
                    "schema_version": "1.0",
                    "payload": {"person": {"profile": {"headline": "Engineer"}}},
                    "metadata": {"contract": "data_envelope"},
                }
            ),
            run_id="run-field-extractor-envelope-string",
        )

        self.assertEqual(state.status, "completed")
        self.assertEqual(state.final_output, "Engineer")
        self.assertEqual(state.node_outputs["extract"]["metadata"]["matched_path"], "payload.payload.person.profile.headline")

    def test_started_event_received_input_reflects_upstream_bound_value(self) -> None:
        graph = GraphDefinition.from_dict(runtime_field_extractor_graph_payload("runtime-field-started-input", node_config={"field_name": "headline"}))
        graph.validate_against_services(self.services)

        runtime = self._runtime()
        envelope_input = {
            "schema_version": "1.0",
            "payload": {"person": {"profile": {"headline": "Engineer"}}},
            "metadata": {"contract": "data_envelope"},
        }
        state = runtime.run(
            graph,
            envelope_input,
            run_id="run-field-extractor-started-input",
        )

        started_event = next(
            event
            for event in state.event_history
            if event.event_type == "node.started" and isinstance(event.payload, dict) and event.payload.get("node_id") == "extract"
        )
        self.assertEqual(started_event.payload.get("received_input"), state.node_outputs["start"])

    def test_prefers_incoming_handle_envelope_over_graph_input_payload(self) -> None:
        graph = GraphDefinition.from_dict(upstream_envelope_runtime_field_extractor_graph_payload())
        graph.validate_against_services(self.services)

        runtime = self._runtime()
        state = runtime.run(
            graph,
            "Find graph-agent references for a schema repair workflow.",
            run_id="run-field-extractor-upstream-envelope",
        )

        self.assertEqual(state.status, "completed")
        self.assertEqual(state.final_output, "https://www.linkedin.com/in/example/")
        self.assertEqual(state.node_outputs["extract"]["metadata"]["matched_path"], "payload.record.LinkedInURL")

        started_event = next(
            event
            for event in state.event_history
            if event.event_type == "node.started" and isinstance(event.payload, dict) and event.payload.get("node_id") == "extract"
        )
        received_input = started_event.payload.get("received_input")
        self.assertIsInstance(received_input, dict)
        self.assertEqual(received_input, state.node_outputs["prepare"])

    def test_missing_field_fails_cleanly(self) -> None:
        graph = GraphDefinition.from_dict(runtime_field_extractor_graph_payload("runtime-field-missing", node_config={"field_name": "headline"}))
        graph.validate_against_services(self.services)

        runtime = self._runtime()
        state = runtime.run(
            graph,
            {"person": {"name": "Taylor"}},
            run_id="run-field-extractor-missing",
        )

        self.assertEqual(state.status, "failed")
        self.assertEqual(state.terminal_error["type"], "field_not_found")


if __name__ == "__main__":
    unittest.main()
