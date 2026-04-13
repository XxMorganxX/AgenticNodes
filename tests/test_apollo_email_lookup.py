from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from graph_agent.api.graph_store import GraphStore
from graph_agent.examples.tool_schema_repair import build_example_services
from graph_agent.runtime.apollo_email_lookup import (
    ApolloEmailLookupRequest,
    ApolloLookupError,
    build_apollo_email_lookup_cache_info,
)
from graph_agent.runtime.core import GraphDefinition
from graph_agent.runtime.engine import GraphRuntime


def apollo_graph_payload(
    graph_id: str = "apollo-email-graph",
    *,
    node_config: dict[str, object] | None = None,
    env_vars: dict[str, str] | None = None,
) -> dict[str, object]:
    resolved_node_config: dict[str, object] = {
        "mode": "apollo_email_lookup",
        "input_binding": {"type": "input_payload"},
        "api_key_env_var": "APOLLO_API_KEY",
        "name": "",
        "domain": "",
        "organization_name": "",
        "first_name": "",
        "last_name": "",
        "linkedin_url": "",
        "email": "",
        "twitter_url": "",
        "reveal_personal_emails": False,
        "use_cache": True,
        "force_refresh": False,
        "workspace_cache_path_template": "cache/apollo-email/{cache_key}.json",
    }
    if node_config:
        resolved_node_config.update(node_config)
    return {
        "graph_id": graph_id,
        "name": "Apollo Email Lookup Graph",
        "description": "",
        "version": "1.0",
        "start_node_id": "start",
        "env_vars": env_vars or {},
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
                "id": "apollo",
                "kind": "data",
                "category": "data",
                "label": "Apollo Email Lookup",
                "provider_id": "core.apollo_email_lookup",
                "provider_label": "Apollo Email Lookup",
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
                "config": {"source_binding": {"type": "latest_payload", "source": "apollo"}},
                "position": {"x": 440, "y": 0},
            },
        ],
        "edges": [
            {"id": "e1", "source_id": "start", "target_id": "apollo", "label": "", "kind": "standard", "priority": 100},
            {"id": "e2", "source_id": "apollo", "target_id": "finish", "label": "", "kind": "standard", "priority": 100},
        ],
    }


def sample_apollo_response(
    name: str,
    *,
    email: str | None,
    organization_name: str = "Example Co",
) -> dict[str, object]:
    person: dict[str, object] | None
    if email is None and not name:
        person = None
    else:
        first_name, _, last_name = name.partition(" ")
        person = {
            "id": "person-123",
            "name": name,
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "organization": {"name": organization_name},
            "contact": {"email": email},
        }
    return {
        "person": person,
        "organization": {"name": organization_name} if organization_name else None,
        "breadcrumbs": [],
    }


class ApolloEmailLookupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.services = build_example_services()

    def _runtime(self) -> GraphRuntime:
        return GraphRuntime(
            services=self.services,
            max_steps=self.services.config["max_steps"],
            max_visits_per_node=self.services.config["max_visits_per_node"],
        )

    def test_apollo_provider_appears_in_catalog(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = GraphStore(self.services, path=Path(directory) / "graphs.json")
            catalog = store.catalog()

        provider = next(
            candidate for candidate in catalog["node_providers"] if candidate["provider_id"] == "core.apollo_email_lookup"
        )
        self.assertEqual(provider["default_config"]["mode"], "apollo_email_lookup")
        self.assertEqual(provider["default_config"]["api_key_env_var"], "APOLLO_API_KEY")
        self.assertEqual(provider["default_config"]["conversation"], "")
        self.assertTrue(provider["default_config"]["use_cache"])

    def test_runtime_resolves_lookup_from_payload_config_and_graph_env(self) -> None:
        graph = GraphDefinition.from_dict(
            apollo_graph_payload(
                "apollo-env-graph",
                node_config={
                    "api_key_env_var": "{APOLLO_KEY_ALIAS}",
                    "first_name": "Taylor",
                    "organization_name": "{COMPANY_NAME}",
                },
                env_vars={
                    "APOLLO_KEY_ALIAS": "APOLLO_API_KEY",
                    "COMPANY_NAME": "Example Co",
                },
            )
        )
        graph.validate_against_services(self.services)
        runtime = self._runtime()
        workspace_root = Path(tempfile.mkdtemp()) / ".graph-agent" / "runs"

        with patch.dict(
            "os.environ",
            {"GRAPH_AGENT_WORKSPACE_DIR": str(workspace_root), "APOLLO_API_KEY": "live-key"},
            clear=False,
        ):
            with patch(
                "graph_agent.runtime.core.fetch_apollo_person_match_live",
                return_value=sample_apollo_response("Taylor Doe", email="taylor@example.com"),
            ) as fetch_mock:
                state = runtime.run(
                    graph,
                    {
                        "first_name": "Source",
                        "last_name": "Doe",
                        "linkedin_url": "https://www.linkedin.com/in/taylor-doe/",
                        "organization_name": "Ignored Co",
                    },
                    run_id="run-apollo-env",
                    agent_id="agent-alpha",
                )

        self.assertEqual(state.status, "completed")
        request = fetch_mock.call_args.kwargs["request"]
        self.assertEqual(fetch_mock.call_args.kwargs["api_key"], "live-key")
        self.assertEqual(request.first_name, "Taylor")
        self.assertEqual(request.last_name, "Doe")
        self.assertEqual(request.linkedin_url, "https://www.linkedin.com/in/taylor-doe/")
        self.assertEqual(request.organization_name, "Example Co")

    def test_runtime_reuses_shared_cache_across_runs(self) -> None:
        graph = GraphDefinition.from_dict(apollo_graph_payload())
        graph.validate_against_services(self.services)
        runtime = self._runtime()
        workspace_root = Path(tempfile.mkdtemp()) / ".graph-agent" / "runs"
        response_payload = sample_apollo_response("Taylor Doe", email="taylor@example.com")
        cache_info = build_apollo_email_lookup_cache_info(
            ApolloEmailLookupRequest.from_mapping(
                {
                    "name": "Taylor Doe",
                    "domain": "example.com",
                    "reveal_personal_emails": False,
                }
            )
        )

        with patch.dict(
            "os.environ",
            {"GRAPH_AGENT_WORKSPACE_DIR": str(workspace_root), "APOLLO_API_KEY": "live-key"},
            clear=False,
        ):
            with patch(
                "graph_agent.runtime.core.fetch_apollo_person_match_live",
                return_value=response_payload,
            ) as fetch_mock:
                first_state = runtime.run(
                    graph,
                    {"name": "Taylor Doe", "domain": "example.com"},
                    run_id="run-apollo-1",
                    agent_id="agent-alpha",
                )
                second_state = runtime.run(
                    graph,
                    {"name": "Taylor Doe", "domain": "example.com"},
                    run_id="run-apollo-2",
                    agent_id="agent-alpha",
                )

        self.assertEqual(fetch_mock.call_count, 1)
        self.assertEqual(first_state.status, "completed")
        self.assertEqual(second_state.status, "completed")
        self.assertEqual(first_state.final_output["person"]["email"], "taylor@example.com")
        self.assertEqual(first_state.node_outputs["apollo"]["metadata"]["cache_status"], "miss")
        self.assertEqual(second_state.node_outputs["apollo"]["metadata"]["cache_status"], "hit")
        self.assertEqual(second_state.node_outputs["apollo"]["metadata"]["lookup_status"], "matched")

        shared_cache_path = workspace_root.parent / "cache" / "apollo-email" / f"{cache_info.cache_key}.json"
        self.assertTrue(shared_cache_path.exists())
        cached_entry = json.loads(shared_cache_path.read_text())
        self.assertEqual(cached_entry["lookup_status"], "matched")
        self.assertEqual(cached_entry["resolved_email"], "taylor@example.com")
        self.assertEqual(cached_entry["payload"], response_payload)

        first_workspace_copy = (
            workspace_root / "run-apollo-1" / "agents" / "agent-alpha" / "workspace" / "cache" / "apollo-email" / f"{cache_info.cache_key}.json"
        )
        second_workspace_copy = (
            workspace_root / "run-apollo-2" / "agents" / "agent-alpha" / "workspace" / "cache" / "apollo-email" / f"{cache_info.cache_key}.json"
        )
        self.assertTrue(first_workspace_copy.exists())
        self.assertTrue(second_workspace_copy.exists())

    def test_force_refresh_bypasses_shared_cache_and_overwrites_it(self) -> None:
        initial_graph = GraphDefinition.from_dict(apollo_graph_payload("apollo-refresh-initial"))
        refresh_graph = GraphDefinition.from_dict(
            apollo_graph_payload("apollo-refresh-graph", node_config={"force_refresh": True})
        )
        initial_graph.validate_against_services(self.services)
        refresh_graph.validate_against_services(self.services)
        runtime = self._runtime()
        workspace_root = Path(tempfile.mkdtemp()) / ".graph-agent" / "runs"
        first_response = sample_apollo_response("Refresh Person", email="first@example.com")
        refreshed_response = sample_apollo_response("Refresh Person", email="updated@example.com")
        cache_info = build_apollo_email_lookup_cache_info(
            ApolloEmailLookupRequest.from_mapping({"name": "Refresh Person", "domain": "example.com"})
        )

        with patch.dict(
            "os.environ",
            {"GRAPH_AGENT_WORKSPACE_DIR": str(workspace_root), "APOLLO_API_KEY": "live-key"},
            clear=False,
        ):
            with patch(
                "graph_agent.runtime.core.fetch_apollo_person_match_live",
                side_effect=[first_response, refreshed_response],
            ) as fetch_mock:
                runtime.run(
                    initial_graph,
                    {"name": "Refresh Person", "domain": "example.com"},
                    run_id="run-apollo-refresh-1",
                    agent_id="agent-alpha",
                )
                refreshed_state = runtime.run(
                    refresh_graph,
                    {"name": "Refresh Person", "domain": "example.com"},
                    run_id="run-apollo-refresh-2",
                    agent_id="agent-alpha",
                )

        self.assertEqual(fetch_mock.call_count, 2)
        self.assertEqual(refreshed_state.status, "completed")
        self.assertEqual(refreshed_state.node_outputs["apollo"]["metadata"]["cache_status"], "refresh")

        shared_cache_path = workspace_root.parent / "cache" / "apollo-email" / f"{cache_info.cache_key}.json"
        self.assertEqual(json.loads(shared_cache_path.read_text())["resolved_email"], "updated@example.com")

    def test_no_email_and_no_match_responses_are_cached_and_reused(self) -> None:
        runtime = self._runtime()
        workspace_root = Path(tempfile.mkdtemp()) / ".graph-agent" / "runs"

        scenarios = [
            ("no-email@example.com", sample_apollo_response("No Email", email=None), "no_email", {"name": "No Email", "domain": "example.com"}),
            ("no-match", sample_apollo_response("", email=None, organization_name=""), "no_match", {"name": "No Match", "domain": "example.com"}),
        ]
        for graph_suffix, response_payload, expected_status, lookup_payload in scenarios:
            graph = GraphDefinition.from_dict(apollo_graph_payload(f"apollo-{graph_suffix}"))
            graph.validate_against_services(self.services)
            cache_info = build_apollo_email_lookup_cache_info(ApolloEmailLookupRequest.from_mapping(lookup_payload))

            with patch.dict(
                "os.environ",
                {"GRAPH_AGENT_WORKSPACE_DIR": str(workspace_root), "APOLLO_API_KEY": "live-key"},
                clear=False,
            ):
                with patch(
                    "graph_agent.runtime.core.fetch_apollo_person_match_live",
                    return_value=response_payload,
                ) as fetch_mock:
                    first_state = runtime.run(
                        graph,
                        lookup_payload,
                        run_id=f"run-{graph_suffix}-1",
                        agent_id="agent-alpha",
                    )
                    second_state = runtime.run(
                        graph,
                        lookup_payload,
                        run_id=f"run-{graph_suffix}-2",
                        agent_id="agent-alpha",
                    )

            self.assertEqual(fetch_mock.call_count, 1)
            self.assertEqual(first_state.status, "completed")
            self.assertEqual(second_state.status, "completed")
            self.assertEqual(first_state.node_outputs["apollo"]["metadata"]["lookup_status"], expected_status)
            self.assertEqual(second_state.node_outputs["apollo"]["metadata"]["cache_status"], "hit")

            shared_cache_path = workspace_root.parent / "cache" / "apollo-email" / f"{cache_info.cache_key}.json"
            self.assertTrue(shared_cache_path.exists())
            cached_entry = json.loads(shared_cache_path.read_text())
            self.assertEqual(cached_entry["lookup_status"], expected_status)

    def test_underspecified_input_fails_before_attempting_live_fetch(self) -> None:
        graph = GraphDefinition.from_dict(apollo_graph_payload("apollo-invalid-input"))
        graph.validate_against_services(self.services)
        runtime = self._runtime()
        workspace_root = Path(tempfile.mkdtemp()) / ".graph-agent" / "runs"

        with patch.dict(
            "os.environ",
            {"GRAPH_AGENT_WORKSPACE_DIR": str(workspace_root), "APOLLO_API_KEY": "live-key"},
            clear=False,
        ):
            with patch("graph_agent.runtime.core.fetch_apollo_person_match_live") as fetch_mock:
                state = runtime.run(
                    graph,
                    {"first_name": "Only"},
                    run_id="run-apollo-invalid",
                    agent_id="agent-alpha",
                )

        self.assertEqual(state.status, "failed")
        self.assertEqual(state.terminal_error["type"], "invalid_apollo_lookup_input")
        fetch_mock.assert_not_called()

    def test_apollo_errors_are_not_cached(self) -> None:
        graph = GraphDefinition.from_dict(apollo_graph_payload("apollo-http-error"))
        graph.validate_against_services(self.services)
        runtime = self._runtime()
        workspace_root = Path(tempfile.mkdtemp()) / ".graph-agent" / "runs"
        cache_info = build_apollo_email_lookup_cache_info(
            ApolloEmailLookupRequest.from_mapping({"name": "Error Person", "domain": "example.com"})
        )

        with patch.dict(
            "os.environ",
            {"GRAPH_AGENT_WORKSPACE_DIR": str(workspace_root), "APOLLO_API_KEY": "live-key"},
            clear=False,
        ):
            with patch(
                "graph_agent.runtime.core.fetch_apollo_person_match_live",
                side_effect=ApolloLookupError("apollo_http_error", "Apollo API returned HTTP 422."),
            ):
                state = runtime.run(
                    graph,
                    {"name": "Error Person", "domain": "example.com"},
                    run_id="run-apollo-error",
                    agent_id="agent-alpha",
                )

        self.assertEqual(state.status, "failed")
        self.assertEqual(state.terminal_error["type"], "apollo_http_error")
        shared_cache_path = workspace_root.parent / "cache" / "apollo-email" / f"{cache_info.cache_key}.json"
        self.assertFalse(shared_cache_path.exists())


if __name__ == "__main__":
    unittest.main()
