from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from graph_agent.runtime.core import RuntimeEvent
from graph_agent.runtime.event_contract import (
    RUNTIME_EVENT_SCHEMA_VERSION,
    normalize_runtime_event_dict,
    normalize_runtime_state_snapshot,
)


class RuntimeEventContractTests(unittest.TestCase):
    def test_runtime_event_to_dict_includes_schema_version(self) -> None:
        event = RuntimeEvent(
            event_type="run.started",
            summary="started",
            payload={"graph_id": "graph-1"},
            run_id="run-1",
            timestamp="2026-04-03T00:00:00Z",
        )

        self.assertEqual(
            event.to_dict(),
            {
                "schema_version": RUNTIME_EVENT_SCHEMA_VERSION,
                "event_type": "run.started",
                "summary": "started",
                "payload": {"graph_id": "graph-1"},
                "run_id": "run-1",
                "agent_id": None,
                "parent_run_id": None,
                "timestamp": "2026-04-03T00:00:00Z",
            },
        )

    def test_normalize_runtime_event_upgrades_legacy_events(self) -> None:
        normalized = normalize_runtime_event_dict(
            {
                "event_type": "node.completed",
                "summary": "done",
                "payload": {
                    "node_id": "node-a",
                    "output": {"answer": "ok"},
                    "timing_ms": {"queue_wait": 1.25, "node_execute": 4.5},
                    "timing_counts": {"not_ready_requeues": 2},
                },
                "run_id": "run-1",
                "timestamp": "2026-04-03T00:00:01Z",
            }
        )

        self.assertEqual(normalized["schema_version"], RUNTIME_EVENT_SCHEMA_VERSION)
        self.assertEqual(normalized["event_type"], "node.completed")
        self.assertEqual(normalized["payload"]["node_id"], "node-a")
        self.assertEqual(normalized["payload"]["timing_ms"]["node_execute"], 4.5)
        self.assertEqual(normalized["payload"]["timing_counts"]["not_ready_requeues"], 2)

    def test_normalize_runtime_state_snapshot_recurses_into_agent_runs(self) -> None:
        normalized = normalize_runtime_state_snapshot(
            {
                "run_id": "parent",
                "graph_id": "graph-1",
                "node_statuses": {"node-a": "active"},
                "event_history": [
                    {
                        "event_type": "agent.run.started",
                        "summary": "agent started",
                        "payload": {"agent_id": "agent-a", "child_run_id": "child"},
                        "run_id": "parent",
                        "agent_id": "agent-a",
                        "parent_run_id": "parent",
                        "timestamp": "2026-04-03T00:00:02Z",
                    }
                ],
                "agent_runs": {
                    "agent-a": {
                        "run_id": "child",
                        "graph_id": "graph-1",
                        "node_statuses": {"node-b": "success"},
                        "event_history": [
                            {
                                "event_type": "run.started",
                                "summary": "child started",
                                "payload": {},
                                "run_id": "child",
                                "timestamp": "2026-04-03T00:00:03Z",
                            }
                        ],
                        "agent_runs": {},
                    }
                },
            }
        )

        self.assertIsNotNone(normalized)
        self.assertEqual(normalized["node_statuses"], {"node-a": "active"})
        self.assertEqual(normalized["event_history"][0]["schema_version"], RUNTIME_EVENT_SCHEMA_VERSION)
        self.assertEqual(normalized["agent_runs"]["agent-a"]["node_statuses"], {"node-b": "success"})
        self.assertEqual(
            normalized["agent_runs"]["agent-a"]["event_history"][0]["schema_version"],
            RUNTIME_EVENT_SCHEMA_VERSION,
        )


if __name__ == "__main__":
    unittest.main()
