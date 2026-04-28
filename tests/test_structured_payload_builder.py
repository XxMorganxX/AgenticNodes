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

    def test_builder_parses_fenced_json_payload_string(self) -> None:
        graph = GraphDefinition.from_dict(
            structured_payload_builder_graph_payload(
                "structured-payload-fenced-json",
                node_config={
                    "template_json": json.dumps(
                        {
                            "organization": "",
                            "domain": "",
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
                "payload": "```json\n{\n  \"organization\": \"OpenAI\",\n  \"organization_confidence\": 1.0,\n  \"domain\": \"openai.com\",\n  \"domain_confidence\": 0.95\n}\n```",
            },
        )

        self.assertEqual(state.status, "completed")
        self.assertEqual(
            state.final_output,
            {
                "organization": "OpenAI",
                "domain": "openai.com",
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

    def test_user_defined_search_keys_resolve_arbitrary_field_names(self) -> None:
        graph = GraphDefinition.from_dict(
            structured_payload_builder_graph_payload(
                "structured-payload-field-aliases",
                node_config={
                    "template_json": json.dumps({"recipient_email": "", "subject": "", "body": ""}),
                    "field_aliases": {
                        "recipient_email": ["resolved_email", "work_email"],
                        "subject": ["headline", "title"],
                    },
                },
            )
        )
        graph.validate_against_services(self.services)
        runtime = self._runtime()

        state = runtime.run(
            graph,
            {
                "metadata": {"resolved_email": "peterm@anthropic.com"},
                "person": {"headline": "Member of Technical Staff"},
                "body": "Hello Peter,",
            },
        )

        self.assertEqual(state.status, "completed")
        self.assertEqual(state.final_output["recipient_email"], "peterm@anthropic.com")
        self.assertEqual(state.final_output["subject"], "Member of Technical Staff")
        self.assertEqual(state.final_output["body"], "Hello Peter,")
        field_matches = state.node_outputs["builder"]["artifacts"]["field_matches"]
        recipient_match = next(match for match in field_matches if match["target_path"] == "recipient_email")
        self.assertEqual(recipient_match["path"], "metadata.resolved_email")
        self.assertEqual(recipient_match["match_type"], "exact_key")

    def test_field_aliases_accept_json_string_payload(self) -> None:
        graph = GraphDefinition.from_dict(
            structured_payload_builder_graph_payload(
                "structured-payload-field-aliases-string",
                node_config={
                    "template_json": json.dumps({"recipient_email": ""}),
                    "field_aliases": json.dumps({"recipient_email": ["resolved_email"]}),
                },
            )
        )
        graph.validate_against_services(self.services)
        runtime = self._runtime()

        state = runtime.run(
            graph,
            {"metadata": {"resolved_email": "peterm@anthropic.com"}},
        )
        self.assertEqual(state.status, "completed")
        self.assertEqual(state.final_output["recipient_email"], "peterm@anthropic.com")

    def test_builder_scans_envelope_metadata_when_payload_does_not_have_value(self) -> None:
        graph = GraphDefinition.from_dict(
            structured_payload_builder_graph_payload(
                "structured-payload-envelope-metadata",
                node_config={
                    "template_json": json.dumps({"recipient_email": ""}),
                    "field_aliases": {"recipient_email": ["resolved_email"]},
                    "default_search_section": "metadata",
                },
            )
        )
        graph.validate_against_services(self.services)
        runtime = self._runtime()

        envelope = {
            "schema_version": "1.0",
            "from_node_id": "upstream",
            "from_category": "data",
            "payload": {"person": {"name": "Taylor"}},
            "metadata": {"resolved_email": "peterm@anthropic.com"},
            "artifacts": {},
        }

        state = runtime.run(graph, envelope)
        self.assertEqual(state.status, "completed")
        self.assertEqual(state.final_output["recipient_email"], "peterm@anthropic.com")
        match = next(
            entry
            for entry in state.node_outputs["builder"]["artifacts"]["field_matches"]
            if entry["target_path"] == "recipient_email"
        )
        self.assertEqual(match["path"], "resolved_email")

    def test_null_source_value_does_not_win_over_template_default(self) -> None:
        graph = GraphDefinition.from_dict(
            structured_payload_builder_graph_payload(
                "structured-payload-skip-null",
                node_config={
                    "template_json": json.dumps({"email": "", "subject": "", "body": ""}),
                },
            )
        )
        graph.validate_against_services(self.services)
        runtime = self._runtime()

        state = runtime.run(
            graph,
            {
                "email": None,
                "subject": "From Airbnb to Anthropic",
                "body": "Hello Peter,",
            },
        )
        self.assertEqual(state.status, "completed")
        self.assertEqual(state.final_output["email"], "")
        self.assertEqual(state.final_output["subject"], "From Airbnb to Anthropic")
        self.assertEqual(state.final_output["body"], "Hello Peter,")
        self.assertIn("email", state.node_outputs["builder"]["artifacts"]["unresolved_paths"])
        self.assertNotIn("email", state.node_outputs["builder"]["artifacts"]["filled_paths"])

    def test_default_search_section_payload_skips_envelope_metadata(self) -> None:
        # default_search_section defaults to "payload" — metadata is not scanned.
        graph = GraphDefinition.from_dict(
            structured_payload_builder_graph_payload(
                "structured-payload-skip-metadata",
                node_config={
                    "template_json": json.dumps({"recipient_email": ""}),
                    "field_aliases": {"recipient_email": ["resolved_email"]},
                },
            )
        )
        graph.validate_against_services(self.services)
        runtime = self._runtime()

        envelope = {
            "schema_version": "1.0",
            "from_node_id": "upstream",
            "from_category": "data",
            "payload": {"person": {"name": "Taylor"}},
            "metadata": {"resolved_email": "peterm@anthropic.com"},
            "artifacts": {},
        }
        state = runtime.run(graph, envelope)
        self.assertEqual(state.status, "completed")
        self.assertEqual(state.final_output["recipient_email"], "")
        self.assertIn("recipient_email", state.node_outputs["builder"]["artifacts"]["unresolved_paths"])

    def test_per_entry_search_scope_overrides_global_default(self) -> None:
        # Global default is payload; per-entry override on primary_email points it
        # at metadata, while fallback_email stays on payload.
        graph = GraphDefinition.from_dict(
            structured_payload_builder_graph_payload(
                "structured-payload-per-entry-scope",
                node_config={
                    "template_json": json.dumps({"primary_email": "", "fallback_email": ""}),
                    "field_aliases": {
                        "primary_email": ["resolved_email"],
                        "fallback_email": ["resolved_email"],
                    },
                    "default_search_section": "payload",
                    "field_search_scopes": {"primary_email": "metadata"},
                },
            )
        )
        graph.validate_against_services(self.services)
        runtime = self._runtime()

        envelope = {
            "schema_version": "1.0",
            "from_node_id": "upstream",
            "from_category": "data",
            "payload": {"person": {"name": "Taylor"}},
            "metadata": {"resolved_email": "peterm@anthropic.com"},
            "artifacts": {},
        }
        state = runtime.run(graph, envelope)
        self.assertEqual(state.status, "completed")
        # primary_email is scoped to metadata → resolved from envelope.metadata.
        self.assertEqual(state.final_output["primary_email"], "peterm@anthropic.com")
        # fallback_email follows global default (payload) → not found.
        self.assertEqual(state.final_output["fallback_email"], "")
        self.assertIn(
            "fallback_email", state.node_outputs["builder"]["artifacts"]["unresolved_paths"]
        )

    def test_search_keys_replace_output_field_label(self) -> None:
        # When search keys are configured for an entry, the matcher searches for
        # those keys only. A source field whose name matches the entry's output
        # label (but not the search keys) must NOT be picked.
        graph = GraphDefinition.from_dict(
            structured_payload_builder_graph_payload(
                "structured-payload-search-keys-replace-label",
                node_config={
                    "template_json": json.dumps({"email": "", "subject": ""}),
                    "field_aliases": {"email": ["resolved_email"]},
                },
            )
        )
        graph.validate_against_services(self.services)
        runtime = self._runtime()

        state = runtime.run(
            graph,
            {
                "email": "label@example.com",
                "metadata": {"resolved_email": "search-key@example.com"},
                "subject": "Hello",
            },
        )

        self.assertEqual(state.status, "completed")
        # `email` has a search key configured → only `resolved_email` is matched,
        # the literal `email` field at the root is ignored.
        self.assertEqual(state.final_output["email"], "search-key@example.com")
        # `subject` has no search keys → falls back to matching the output label.
        self.assertEqual(state.final_output["subject"], "Hello")

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
