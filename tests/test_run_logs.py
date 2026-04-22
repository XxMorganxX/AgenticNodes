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
from graph_agent.api.run_state_reducer import apply_single_run_event, build_run_state
from graph_agent.examples.tool_schema_repair import build_example_graph_payload, build_example_services
from graph_agent.runtime.event_contract import RUNTIME_EVENT_SCHEMA_VERSION


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
        self.assertTrue(all(event["schema_version"] == RUNTIME_EVENT_SCHEMA_VERSION for event in events))
        self.assertEqual(events[0]["event_type"], "run.started")
        self.assertEqual(events[-1]["event_type"], "run.completed")
        self.assertTrue(any(event["event_type"] == "node.completed" for event in events))
        self.assertTrue(any("received_input" in event["payload"] for event in events if event["event_type"] == "node.started"))
        self.assertTrue(
            any("output" in event["payload"] for event in events if event["event_type"] == "node.completed")
        )
        started_timing_events = [event for event in events if event["event_type"] == "node.started" and "timing_ms" in event["payload"]]
        self.assertTrue(started_timing_events)
        self.assertIn("queue_wait", started_timing_events[0]["payload"]["timing_ms"])
        self.assertIn("runtime_input_preview", started_timing_events[0]["payload"]["timing_ms"])
        self.assertIn("timing_counts", started_timing_events[0]["payload"])
        self.assertIn("not_ready_requeues", started_timing_events[0]["payload"]["timing_counts"])

        completed_timing_events = [event for event in events if event["event_type"] == "node.completed" and "timing_ms" in event["payload"]]
        self.assertTrue(completed_timing_events)
        self.assertIn("node_started_emit", completed_timing_events[0]["payload"]["timing_ms"])
        self.assertIn("node_execute", completed_timing_events[0]["payload"]["timing_ms"])

        edge_timing_events = [event for event in events if event["event_type"] == "edge.selected" and "source_timing_ms" in event["payload"]]
        self.assertTrue(edge_timing_events)
        self.assertIn("edge_selection", edge_timing_events[0]["payload"]["source_timing_ms"])
        self.assertIn("edge_enqueue", edge_timing_events[0]["payload"]["source_timing_ms"])
        self.assertIn("post_execute_bookkeeping", edge_timing_events[0]["payload"]["source_timing_ms"])

        logged_state = json.loads((run_dir / "state.json").read_text(encoding="utf-8"))
        self.assertEqual(logged_state, state)

        recovered_manager = GraphRunManager(
            services=self.services,
            store=store,
            run_log_store=RunLogStore(self.logs_root),
        )
        recovered_state = recovered_manager.get_run(run_id)
        self.assertEqual(recovered_state, state)

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
        self.assertTrue(all(event["schema_version"] == RUNTIME_EVENT_SCHEMA_VERSION for event in parent_events))
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
            self.assertTrue(all(event["schema_version"] == RUNTIME_EVENT_SCHEMA_VERSION for event in child_events))
            self.assertEqual(child_events[0]["event_type"], "run.started")
            self.assertEqual(child_events[-1]["event_type"], "run.completed")
            self.assertTrue(all(event["run_id"] == child_run_id for event in child_events))
            self.assertTrue(any(event.get("agent_id") == agent_id for event in child_events))

            logged_child_state = json.loads((child_dir / "state.json").read_text(encoding="utf-8"))
            self.assertEqual(logged_child_state, agent_state)

        recovered_manager = GraphRunManager(
            services=self.services,
            store=store,
            run_log_store=RunLogStore(self.logs_root),
        )
        recovered_parent_state = recovered_manager.get_run(run_id)
        self.assertEqual(recovered_parent_state, state)
        for agent_state in state["agent_runs"].values():
            recovered_child_state = recovered_manager.get_run(agent_state["run_id"])
            self.assertEqual(recovered_child_state, agent_state)

    def test_filesystem_store_keeps_prior_runs_queryable_on_rerun(self) -> None:
        bundled_path = self.temp_path / "bundled_graphs.json"
        bundled_path.write_text(json.dumps({"graphs": []}), encoding="utf-8")
        store = GraphStore(
            self.services,
            path=self.temp_path / "graphs.json",
            bundled_path=bundled_path,
        )
        graph_payload = build_example_graph_payload()
        graph_payload["graph_id"] = "rerun-agent"
        store.create_graph(graph_payload)
        manager = GraphRunManager(
            services=self.services,
            store=store,
            run_log_store=RunLogStore(self.logs_root),
        )

        first_run_id = manager.start_run("rerun-agent", "first request")
        first_state = wait_for_run_completion(manager, first_run_id)
        second_run_id = manager.start_run("rerun-agent", "second request")
        second_state = wait_for_run_completion(manager, second_run_id)

        self.assertNotEqual(first_run_id, second_run_id)
        self.assertEqual(manager.get_run(first_run_id), first_state)
        self.assertEqual(manager.get_run(second_run_id), second_state)
        history = manager.list_runs("rerun-agent", limit=10)
        self.assertEqual({row["run_id"] for row in history}, {first_run_id, second_run_id})

    def test_recovered_running_run_becomes_interrupted_when_heartbeat_is_stale(self) -> None:
        run_store = RunLogStore(self.logs_root)
        bundled_path = self.temp_path / "bundled_graphs.json"
        bundled_path.write_text(json.dumps({"graphs": []}), encoding="utf-8")
        state = build_run_state("stale-run", "graph-1", {"prompt": "resume"})
        state = apply_single_run_event(
            state,
            {
                "event_type": "run.started",
                "summary": "started",
                "payload": {},
                "run_id": "stale-run",
                "timestamp": "2026-04-02T00:00:00Z",
                "agent_id": None,
                "parent_run_id": None,
            },
        )
        state["runtime_instance_id"] = "old-runtime"
        state["last_heartbeat_at"] = "2026-04-02T00:00:01Z"
        run_store.initialize_run(state)
        run_store.append_event("stale-run", state["event_history"][-1])
        run_store.write_state("stale-run", state)

        manager = GraphRunManager(
            services=self.services,
            store=GraphStore(
                self.services,
                path=self.temp_path / "graphs.json",
                bundled_path=bundled_path,
            ),
            run_log_store=run_store,
        )
        recovered = manager.get_run("stale-run")

        self.assertEqual(recovered["status"], "interrupted")
        self.assertEqual(recovered["status_reason"], "runtime_heartbeat_expired")
        self.assertEqual(recovered["terminal_error"]["type"], "runtime_interrupted")
        history = manager.list_runs(limit=10)
        stale_row = next(row for row in history if row["run_id"] == "stale-run")
        self.assertEqual(stale_row["status"], "interrupted")

    def test_record_event_flushes_state_only_for_terminal_events(self) -> None:
        class CountingRunStore:
            def __init__(self) -> None:
                self.appended_events: list[dict[str, object]] = []
                self.written_states: list[dict[str, object]] = []

            def initialize_run(self, state: dict[str, object]) -> None:
                return None

            def append_event(self, run_id: str, event: dict[str, object]) -> None:
                self.appended_events.append(dict(event))

            def write_state(self, run_id: str, state: dict[str, object]) -> None:
                self.written_states.append(dict(state))

            def load_manifest(self, run_id: str) -> dict[str, object] | None:
                return None

            def load_events(self, run_id: str) -> list[dict[str, object]]:
                return []

            def load_state(self, run_id: str) -> dict[str, object] | None:
                return None

            def recover_run_state(self, run_id: str) -> dict[str, object] | None:
                return None

            def list_runs(self, *, graph_id: str | None = None, limit: int = 50) -> list[dict[str, object]]:
                return []

        run_store = CountingRunStore()
        manager = GraphRunManager(
            services=self.services,
            store=GraphStore(
                self.services,
                path=self.temp_path / "graphs.json",
                bundled_path=self.temp_path / "bundled_graphs.json",
            ),
            run_log_store=run_store,
        )
        state = build_run_state("perf-run", "graph-1", {"prompt": "run"})
        manager._run_states["perf-run"] = state
        manager._event_backlog["perf-run"] = []
        manager._subscribers["perf-run"] = []

        manager._record_event(
            "perf-run",
            {
                "event_type": "node.started",
                "summary": "started node",
                "payload": {"node_id": "node-1"},
                "run_id": "perf-run",
                "timestamp": "2026-04-02T00:00:00Z",
                "agent_id": None,
                "parent_run_id": None,
            },
        )
        self.assertEqual(len(run_store.written_states), 0)

        manager._record_event(
            "perf-run",
            {
                "event_type": "run.completed",
                "summary": "done",
                "payload": {"final_output": {"message": "ok"}},
                "run_id": "perf-run",
                "timestamp": "2026-04-02T00:00:01Z",
                "agent_id": None,
                "parent_run_id": None,
            },
        )
        self.assertEqual(len(run_store.written_states), 1)

    def test_recover_run_state_uses_snapshot_event_history_for_backlog(self) -> None:
        class SnapshotOnlyRunStore:
            def initialize_run(self, state: dict[str, object]) -> None:
                return None

            def append_event(self, run_id: str, event: dict[str, object]) -> None:
                return None

            def write_state(self, run_id: str, state: dict[str, object]) -> None:
                return None

            def load_manifest(self, run_id: str) -> dict[str, object] | None:
                return None

            def load_events(self, run_id: str) -> list[dict[str, object]]:
                raise AssertionError("load_events should not be called during recovery hydration")

            def load_state(self, run_id: str) -> dict[str, object] | None:
                return None

            def recover_run_state(self, run_id: str) -> dict[str, object] | None:
                return {
                    **build_run_state(run_id, "graph-1", {"prompt": "resume"}),
                    "event_history": [
                        {
                            "schema_version": RUNTIME_EVENT_SCHEMA_VERSION,
                            "event_type": "run.started",
                            "summary": "started",
                            "payload": {},
                            "run_id": run_id,
                            "timestamp": "2026-04-02T00:00:00Z",
                            "agent_id": None,
                            "parent_run_id": None,
                        }
                    ],
                }

            def list_runs(self, *, graph_id: str | None = None, limit: int = 50) -> list[dict[str, object]]:
                return []

        manager = GraphRunManager(
            services=self.services,
            store=GraphStore(
                self.services,
                path=self.temp_path / "graphs-2.json",
                bundled_path=self.temp_path / "bundled-2.json",
            ),
            run_log_store=SnapshotOnlyRunStore(),
        )

        recovered = manager._recover_run_state("snapshot-run")

        self.assertIsNotNone(recovered)
        self.assertEqual(len(manager._event_backlog["snapshot-run"]), 1)

    def test_list_runs_skips_recovery_for_terminal_rows(self) -> None:
        class ListOnlyRunStore:
            def initialize_run(self, state: dict[str, object]) -> None:
                return None

            def append_event(self, run_id: str, event: dict[str, object]) -> None:
                return None

            def write_state(self, run_id: str, state: dict[str, object]) -> None:
                return None

            def load_manifest(self, run_id: str) -> dict[str, object] | None:
                return None

            def load_events(self, run_id: str) -> list[dict[str, object]]:
                return []

            def load_state(self, run_id: str) -> dict[str, object] | None:
                return None

            def recover_run_state(self, run_id: str) -> dict[str, object] | None:
                raise AssertionError("recover_run_state should not be called for terminal rows")

            def list_runs(self, *, graph_id: str | None = None, limit: int = 50) -> list[dict[str, object]]:
                return [
                    {
                        "run_id": "terminal-run",
                        "graph_id": "graph-1",
                        "status": "completed",
                        "status_reason": None,
                        "started_at": "2026-04-02T00:00:00Z",
                        "ended_at": "2026-04-02T00:00:01Z",
                        "agent_id": None,
                        "agent_name": None,
                        "parent_run_id": None,
                        "runtime_instance_id": None,
                        "last_heartbeat_at": None,
                    }
                ]

        manager = GraphRunManager(
            services=self.services,
            store=GraphStore(
                self.services,
                path=self.temp_path / "graphs-3.json",
                bundled_path=self.temp_path / "bundled-3.json",
            ),
            run_log_store=ListOnlyRunStore(),
        )

        history = manager.list_runs(limit=10)

        self.assertEqual(history[0]["run_id"], "terminal-run")


if __name__ == "__main__":
    unittest.main()
