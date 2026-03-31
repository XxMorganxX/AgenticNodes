from __future__ import annotations

import json
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from graph_agent.api.graph_store import GraphStore
from graph_agent.api.manager import GraphRunManager
from graph_agent.api.run_log_store import RunLogStore
from graph_agent.examples.tool_schema_repair import build_example_graph_payload, build_example_services


def wait_for_run_completion(manager: GraphRunManager, run_id: str, timeout_seconds: float = 5.0) -> dict[str, object]:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        state = manager.get_run(run_id)
        if state["status"] in {"completed", "failed"}:
            return state
        time.sleep(0.05)
    raise AssertionError(f"Run '{run_id}' did not finish within {timeout_seconds} seconds.")


def read_jsonl(path: Path) -> list[dict[str, object]]:
    lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    return [json.loads(line) for line in lines]


class RunLogStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self._temp_dir.name)
        self.logs_root = self.temp_path / ".logs" / "runs"
        self.services = build_example_services()

    def tearDown(self) -> None:
        self._temp_dir.cleanup()

    def test_single_agent_runs_write_manifest_events_and_state(self) -> None:
        bundled_path = self.temp_path / "bundled_graphs.json"
        bundled_path.write_text(json.dumps({"graphs": []}), encoding="utf-8")
        store = GraphStore(
            self.services,
            path=self.temp_path / "graphs.json",
            bundled_path=bundled_path,
        )
        graph_payload = build_example_graph_payload()
        graph_payload["graph_id"] = "manual-agent"
        store.create_graph(graph_payload)
        manager = GraphRunManager(
            services=self.services,
            store=store,
            run_log_store=RunLogStore(self.logs_root),
        )

        run_id = manager.start_run("manual-agent", "Review the persisted execution log.")
        state = wait_for_run_completion(manager, run_id)

        run_dir = self.logs_root / run_id
        self.assertTrue((run_dir / "manifest.json").exists())
        self.assertTrue((run_dir / "events.jsonl").exists())
        self.assertTrue((run_dir / "state.json").exists())

        manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["run_id"], run_id)
        self.assertEqual(manifest["graph_id"], "manual-agent")
        self.assertEqual(manifest["parent_run_id"], None)

        events = read_jsonl(run_dir / "events.jsonl")
        self.assertEqual(events[0]["event_type"], "run.started")
        self.assertEqual(events[-1]["event_type"], "run.completed")
        self.assertTrue(any(event["event_type"] == "node.completed" for event in events))
        self.assertTrue(
            any("output" in event["payload"] for event in events if event["event_type"] == "node.completed")
        )

        logged_state = json.loads((run_dir / "state.json").read_text(encoding="utf-8"))
        self.assertEqual(logged_state, state)

    def test_multi_agent_runs_write_parent_and_child_run_logs(self) -> None:
        store = GraphStore(
            self.services,
            path=self.temp_path / "graphs_store.json",
            bundled_path=ROOT / "src" / "graph_agent" / "api" / "graphs_store.json",
        )
        manager = GraphRunManager(
            services=self.services,
            store=store,
            run_log_store=RunLogStore(self.logs_root),
        )

        run_id = manager.start_run("test-environment", "Find tools that can help plan and execute this task.")
        state = wait_for_run_completion(manager, run_id)

        parent_dir = self.logs_root / run_id
        parent_events = read_jsonl(parent_dir / "events.jsonl")
        self.assertTrue(any(event["event_type"].startswith("agent.") for event in parent_events))
        self.assertTrue(
            any(
                "output" in event["payload"]
                for event in parent_events
                if event["event_type"] == "agent.node.completed"
            )
        )
        logged_parent_state = json.loads((parent_dir / "state.json").read_text(encoding="utf-8"))
        self.assertEqual(logged_parent_state, state)

        for agent_id, agent_state in state["agent_runs"].items():
            child_run_id = agent_state["run_id"]
            child_dir = self.logs_root / child_run_id
            self.assertTrue((child_dir / "manifest.json").exists())
            self.assertTrue((child_dir / "events.jsonl").exists())
            self.assertTrue((child_dir / "state.json").exists())

            child_events = read_jsonl(child_dir / "events.jsonl")
            self.assertEqual(child_events[0]["event_type"], "run.started")
            self.assertEqual(child_events[-1]["event_type"], "run.completed")
            self.assertTrue(all(event["run_id"] == child_run_id for event in child_events))
            self.assertTrue(any(event.get("agent_id") == agent_id for event in child_events))

            logged_child_state = json.loads((child_dir / "state.json").read_text(encoding="utf-8"))
            self.assertEqual(logged_child_state, agent_state)


if __name__ == "__main__":
    unittest.main()
