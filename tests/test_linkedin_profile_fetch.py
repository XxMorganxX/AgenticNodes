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
from graph_agent.runtime.core import GraphDefinition
from graph_agent.runtime.engine import GraphRuntime
from graph_agent.runtime.linkedin_profile_fetch import build_linkedin_profile_cache_info, sanitize_linkedin_profile_payload


def linkedin_graph_payload(
    graph_id: str = "linkedin-fetch-graph",
    *,
    node_config: dict[str, object] | None = None,
) -> dict[str, object]:
    resolved_node_config: dict[str, object] = {
        "mode": "linkedin_profile_fetch",
        "input_binding": {"type": "input_payload"},
        "url_field": "url",
        "linkedin_data_dir": "/Users/morgannstuart/Desktop/Linkedin Data",
        "session_state_path": "",
        "headless": False,
        "navigation_timeout_ms": 45000,
        "page_settle_ms": 3000,
        "use_cache": True,
        "force_refresh": False,
        "workspace_cache_path_template": "cache/linkedin/{cache_key}.json",
    }
    if node_config:
        resolved_node_config.update(node_config)
    return {
        "graph_id": graph_id,
        "name": "LinkedIn Fetch Graph",
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
                "id": "linkedin",
                "kind": "data",
                "category": "data",
                "label": "LinkedIn Profile Fetch",
                "provider_id": "core.linkedin_profile_fetch",
                "provider_label": "LinkedIn Profile Fetch",
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
                "config": {"source_binding": {"type": "latest_payload", "source": "linkedin"}},
                "position": {"x": 440, "y": 0},
            },
        ],
        "edges": [
            {"id": "e1", "source_id": "start", "target_id": "linkedin", "label": "", "kind": "standard", "priority": 100},
            {"id": "e2", "source_id": "linkedin", "target_id": "finish", "label": "", "kind": "standard", "priority": 100},
        ],
    }


def linkedin_graph_with_payload_extractor() -> dict[str, object]:
    return {
        "graph_id": "linkedin-fetch-via-extractor-graph",
        "name": "LinkedIn Fetch Via Extractor Graph",
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
                "config": {
                    "mode": "runtime_normalizer",
                    "input_binding": {"type": "input_payload"},
                    "field_name": "LinkedInURL",
                    "fallback_field_names": [],
                    "preferred_path": "",
                    "case_sensitive": False,
                    "max_matches": 25,
                },
                "position": {"x": 220, "y": 0},
            },
            {
                "id": "linkedin",
                "kind": "data",
                "category": "data",
                "label": "LinkedIn Profile Fetch",
                "provider_id": "core.linkedin_profile_fetch",
                "provider_label": "LinkedIn Profile Fetch",
                "config": {
                    "mode": "linkedin_profile_fetch",
                    "input_binding": {"type": "input_payload"},
                    "url_field": "url",
                    "linkedin_data_dir": "/Users/morgannstuart/Desktop/Linkedin Data",
                    "session_state_path": "",
                    "headless": False,
                    "navigation_timeout_ms": 45000,
                    "page_settle_ms": 3000,
                    "use_cache": True,
                    "force_refresh": False,
                    "workspace_cache_path_template": "cache/linkedin/{cache_key}.json",
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
                "config": {"source_binding": {"type": "latest_payload", "source": "linkedin"}},
                "position": {"x": 660, "y": 0},
            },
        ],
        "edges": [
            {"id": "e1", "source_id": "start", "target_id": "extract", "label": "", "kind": "standard", "priority": 100},
            {"id": "e2", "source_id": "extract", "target_id": "linkedin", "label": "", "kind": "standard", "priority": 100},
            {"id": "e3", "source_id": "linkedin", "target_id": "finish", "label": "", "kind": "standard", "priority": 100},
        ],
    }


def sample_profile(name: str, *, status: str = "ok", page_type: str = "profile", title: str | None = None) -> dict[str, object]:
    resolved_title = title or f"{name} | LinkedIn"
    return {
        "pageType": page_type,
        "status": status,
        "canonical": f"https://www.linkedin.com/in/{name.lower().replace(' ', '-')}/",
        "slug": name.lower().replace(" ", "-"),
        "title": resolved_title,
        "person": {
            "name": name,
            "slug": name.lower().replace(" ", "-"),
            "headline": "Engineer",
            "current": "Example Co",
            "location": "New York, New York, United States",
        },
        "profile": {
            "name": name,
            "headline": "Engineer",
            "current": "Example Co",
            "location": "New York, New York, United States",
            "raw_lines": [name, "Engineer", "Example Co", "New York, New York, United States"],
        },
        "sections": {
            "about": "Builds useful software.",
            "experience": [],
            "education": [],
            "projects": [],
            "skills": ["Python"],
        },
        "summary": [name, "Engineer", "Example Co"],
        "textLines": [name, "Engineer", "Example Co"],
    }


class LinkedInProfileFetchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.services = build_example_services()

    def _runtime(self) -> GraphRuntime:
        return GraphRuntime(
            services=self.services,
            max_steps=self.services.config["max_steps"],
            max_visits_per_node=self.services.config["max_visits_per_node"],
        )

    def test_linkedin_provider_appears_in_catalog(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = GraphStore(self.services, path=Path(directory) / "graphs.json")
            catalog = store.catalog()

        provider = next(
            candidate for candidate in catalog["node_providers"] if candidate["provider_id"] == "core.linkedin_profile_fetch"
        )
        self.assertEqual(provider["default_config"]["mode"], "linkedin_profile_fetch")
        self.assertEqual(provider["default_config"]["url_field"], "url")
        self.assertTrue(provider["default_config"]["use_cache"])

    def test_runtime_reuses_shared_cache_across_runs_and_accepts_both_input_shapes(self) -> None:
        graph = GraphDefinition.from_dict(linkedin_graph_payload())
        graph.validate_against_services(self.services)
        runtime = self._runtime()
        workspace_root = Path(tempfile.mkdtemp()) / ".graph-agent" / "runs"
        profile_payload = sample_profile("Taylor Doe")
        sanitized_profile_payload = sanitize_linkedin_profile_payload(profile_payload)
        cache_info = build_linkedin_profile_cache_info("https://www.linkedin.com/in/taylor-doe/")

        with patch.dict("os.environ", {"GRAPH_AGENT_WORKSPACE_DIR": str(workspace_root)}, clear=False):
            with patch(
                "graph_agent.runtime.core.fetch_linkedin_profile_live",
                return_value={
                    "extracted": profile_payload,
                    "final_page_url": "https://www.linkedin.com/in/taylor-doe/",
                    "storage_state_path": "/tmp/linkedin-session.json",
                },
            ) as fetch_mock:
                first_state = runtime.run(
                    graph,
                    "https://www.linkedin.com/in/taylor-doe/",
                    run_id="run-linkedin-1",
                    agent_id="agent-alpha",
                )
                second_state = runtime.run(
                    graph,
                    {"url": "https://www.linkedin.com/in/taylor-doe/?trk=public-profile"},
                    run_id="run-linkedin-2",
                    agent_id="agent-alpha",
                )

        self.assertEqual(fetch_mock.call_count, 1)
        self.assertEqual(first_state.status, "completed")
        self.assertEqual(second_state.status, "completed")
        self.assertEqual(first_state.final_output["person"]["name"], "Taylor Doe")
        self.assertEqual(second_state.final_output["person"]["name"], "Taylor Doe")
        self.assertNotIn("textLines", first_state.final_output)
        self.assertNotIn("raw_lines", first_state.final_output["profile"])
        self.assertNotIn("skills", first_state.final_output["sections"])

        first_output = first_state.node_outputs["linkedin"]
        second_output = second_state.node_outputs["linkedin"]
        self.assertEqual(first_output["metadata"]["cache_status"], "miss")
        self.assertEqual(second_output["metadata"]["cache_status"], "hit")

        shared_cache_path = workspace_root.parent / "cache" / "linkedin" / f"{cache_info.cache_key}.json"
        self.assertTrue(shared_cache_path.exists())
        self.assertEqual(json.loads(shared_cache_path.read_text()), sanitized_profile_payload)

        first_workspace_copy = (
            workspace_root / "run-linkedin-1" / "agents" / "agent-alpha" / "workspace" / "cache" / "linkedin" / f"{cache_info.cache_key}.json"
        )
        second_workspace_copy = (
            workspace_root / "run-linkedin-2" / "agents" / "agent-alpha" / "workspace" / "cache" / "linkedin" / f"{cache_info.cache_key}.json"
        )
        self.assertTrue(first_workspace_copy.exists())
        self.assertTrue(second_workspace_copy.exists())
        self.assertEqual(json.loads(first_workspace_copy.read_text()), sanitized_profile_payload)
        self.assertEqual(json.loads(second_workspace_copy.read_text()), sanitized_profile_payload)

    def test_sanitize_linkedin_profile_payload_preserves_raw_text_only_as_fallback(self) -> None:
        payload = {
            "profile": {
                "name": "Taylor Doe",
                "headline": "Engineer",
                "raw_lines": ["Taylor Doe", "Engineer"],
            },
            "experience": [
                {
                    "title": None,
                    "subtitle": None,
                    "description": None,
                    "raw_lines": ["Independent Consultant", "Freelance"],
                }
            ],
            "textLines": ["Taylor Doe", "Engineer"],
            "sections": {
                "skills": ["Python"],
                "about": "Builds useful software.",
            },
        }

        sanitized = sanitize_linkedin_profile_payload(payload)

        self.assertNotIn("raw_lines", sanitized["profile"])
        self.assertNotIn("textLines", sanitized)
        self.assertNotIn("skills", sanitized["sections"])
        self.assertEqual(
            sanitized["experience"][0]["raw_lines"],
            ["Independent Consultant", "Freelance"],
        )

    def test_runtime_accepts_upstream_payload_extractor_output(self) -> None:
        graph = GraphDefinition.from_dict(linkedin_graph_with_payload_extractor())
        graph.validate_against_services(self.services)
        runtime = self._runtime()
        workspace_root = Path(tempfile.mkdtemp()) / ".graph-agent" / "runs"
        profile_payload = sample_profile("Taylor Extracted")

        with patch.dict("os.environ", {"GRAPH_AGENT_WORKSPACE_DIR": str(workspace_root)}, clear=False):
            with patch(
                "graph_agent.runtime.core.fetch_linkedin_profile_live",
                return_value={
                    "extracted": profile_payload,
                    "final_page_url": "https://www.linkedin.com/in/taylor-extracted/",
                    "storage_state_path": "/tmp/linkedin-session.json",
                },
            ) as fetch_mock:
                state = runtime.run(
                    graph,
                    {
                        "schema_version": "1.0",
                        "payload": {
                            "record": {
                                "LinkedInURL": "https://www.linkedin.com/in/taylor-extracted/",
                            }
                        },
                        "metadata": {"contract": "data_envelope"},
                    },
                    run_id="run-linkedin-via-extractor",
                    agent_id="agent-alpha",
                )

        self.assertEqual(fetch_mock.call_count, 1)
        self.assertEqual(state.status, "completed")
        self.assertEqual(state.node_outputs["extract"]["payload"], "https://www.linkedin.com/in/taylor-extracted/")
        self.assertEqual(state.final_output["person"]["name"], "Taylor Extracted")

    def test_force_refresh_bypasses_shared_cache_and_overwrites_it(self) -> None:
        initial_graph = GraphDefinition.from_dict(linkedin_graph_payload("linkedin-refresh-initial"))
        refresh_graph = GraphDefinition.from_dict(
            linkedin_graph_payload("linkedin-refresh-graph", node_config={"force_refresh": True})
        )
        initial_graph.validate_against_services(self.services)
        refresh_graph.validate_against_services(self.services)
        runtime = self._runtime()
        workspace_root = Path(tempfile.mkdtemp()) / ".graph-agent" / "runs"
        first_profile = sample_profile("Refresh Person", title="Refresh Person | LinkedIn")
        refreshed_profile = sample_profile("Refresh Person", title="Refresh Person Updated | LinkedIn")
        cache_info = build_linkedin_profile_cache_info("https://www.linkedin.com/in/refresh-person/")

        with patch.dict("os.environ", {"GRAPH_AGENT_WORKSPACE_DIR": str(workspace_root)}, clear=False):
            with patch(
                "graph_agent.runtime.core.fetch_linkedin_profile_live",
                side_effect=[
                    {
                        "extracted": first_profile,
                        "final_page_url": "https://www.linkedin.com/in/refresh-person/",
                        "storage_state_path": "/tmp/linkedin-session.json",
                    },
                    {
                        "extracted": refreshed_profile,
                        "final_page_url": "https://www.linkedin.com/in/refresh-person/",
                        "storage_state_path": "/tmp/linkedin-session.json",
                    },
                ],
            ) as fetch_mock:
                runtime.run(
                    initial_graph,
                    "https://www.linkedin.com/in/refresh-person/",
                    run_id="run-linkedin-refresh-1",
                    agent_id="agent-alpha",
                )
                refreshed_state = runtime.run(
                    refresh_graph,
                    "https://www.linkedin.com/in/refresh-person/",
                    run_id="run-linkedin-refresh-2",
                    agent_id="agent-alpha",
                )

        self.assertEqual(fetch_mock.call_count, 2)
        self.assertEqual(refreshed_state.status, "completed")
        self.assertEqual(refreshed_state.node_outputs["linkedin"]["metadata"]["cache_status"], "refresh")

        shared_cache_path = workspace_root.parent / "cache" / "linkedin" / f"{cache_info.cache_key}.json"
        self.assertEqual(json.loads(shared_cache_path.read_text())["title"], "Refresh Person Updated | LinkedIn")

    def test_failed_linkedin_page_is_mirrored_to_workspace_but_not_shared_cache(self) -> None:
        graph = GraphDefinition.from_dict(linkedin_graph_payload("linkedin-failure-graph"))
        graph.validate_against_services(self.services)
        runtime = self._runtime()
        workspace_root = Path(tempfile.mkdtemp()) / ".graph-agent" / "runs"
        blocked_profile = sample_profile("Blocked Person", status="blocked", page_type="auth_wall")
        cache_info = build_linkedin_profile_cache_info("https://www.linkedin.com/in/blocked-person/")

        with patch.dict("os.environ", {"GRAPH_AGENT_WORKSPACE_DIR": str(workspace_root)}, clear=False):
            with patch(
                "graph_agent.runtime.core.fetch_linkedin_profile_live",
                return_value={
                    "extracted": blocked_profile,
                    "final_page_url": "https://www.linkedin.com/authwall",
                    "storage_state_path": "/tmp/linkedin-session.json",
                },
            ):
                state = runtime.run(
                    graph,
                    "https://www.linkedin.com/in/blocked-person/",
                    run_id="run-linkedin-failure",
                    agent_id="agent-alpha",
                )

        self.assertEqual(state.status, "failed")
        self.assertEqual(state.terminal_error["type"], "linkedin_fetch_blocked")
        self.assertEqual(state.node_outputs["linkedin"]["metadata"]["cache_status"], "miss")

        shared_cache_path = workspace_root.parent / "cache" / "linkedin" / f"{cache_info.cache_key}.json"
        self.assertFalse(shared_cache_path.exists())

        workspace_copy = (
            workspace_root / "run-linkedin-failure" / "agents" / "agent-alpha" / "workspace" / "cache" / "linkedin" / f"{cache_info.cache_key}.json"
        )
        self.assertTrue(workspace_copy.exists())
        self.assertEqual(json.loads(workspace_copy.read_text())["status"], "blocked")

    def test_invalid_url_fails_before_attempting_live_fetch(self) -> None:
        graph = GraphDefinition.from_dict(linkedin_graph_payload("linkedin-invalid-url"))
        graph.validate_against_services(self.services)
        runtime = self._runtime()
        workspace_root = Path(tempfile.mkdtemp()) / ".graph-agent" / "runs"

        with patch.dict("os.environ", {"GRAPH_AGENT_WORKSPACE_DIR": str(workspace_root)}, clear=False):
            with patch("graph_agent.runtime.core.fetch_linkedin_profile_live") as fetch_mock:
                state = runtime.run(
                    graph,
                    "https://example.com/not-linkedin",
                    run_id="run-linkedin-invalid",
                    agent_id="agent-alpha",
                )

        self.assertEqual(state.status, "failed")
        self.assertEqual(state.terminal_error["type"], "invalid_linkedin_profile_url")
        fetch_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
