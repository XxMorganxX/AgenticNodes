from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from graph_agent.api.graph_store import GraphStore
from graph_agent.api.manager import GraphRunManager
from graph_agent.examples.tool_schema_repair import build_example_services
from graph_agent.providers.cron import CronSchedule, CronTriggerService, next_cron_fire_after, normalize_cron_schedule_payload

UTC = timezone.utc


def build_isolated_store(services) -> tuple[GraphStore, tempfile.TemporaryDirectory[str]]:
    temp_dir = tempfile.TemporaryDirectory()
    bundled_path = Path(temp_dir.name) / "bundled_graphs.json"
    bundled_path.write_text(json.dumps({"graphs": []}))
    store = GraphStore(
        services,
        path=Path(temp_dir.name) / "graphs.json",
        bundled_path=bundled_path,
    )
    return store, temp_dir


def build_cron_graph_payload(graph_id: str = "cron-agent") -> dict[str, object]:
    return {
        "graph_id": graph_id,
        "name": "Cron Agent",
        "description": "",
        "version": "1.0",
        "start_node_id": "start",
        "nodes": [
            {
                "id": "start",
                "kind": "input",
                "category": "start",
                "label": "Cron Start",
                "provider_id": "start.cron_schedule",
                "provider_label": "Cron Schedule Start",
                "description": "Captures scheduled prompts as the graph input.",
                "position": {"x": 0, "y": 0},
                "config": {
                    "trigger_mode": "cron_schedule",
                    "cron_expression": "0 9 * * *",
                    "timezone": "UTC",
                    "prompt": "Run the morning workflow",
                    "input_binding": {"type": "input_payload"},
                },
            },
            {
                "id": "finish",
                "kind": "output",
                "category": "end",
                "label": "Finish",
                "provider_id": "core.output",
                "provider_label": "Core Output Node",
                "description": "Returns the scheduled payload.",
                "position": {"x": 280, "y": 0},
                "config": {"source_binding": {"type": "latest_envelope", "source": "start"}},
            },
        ],
        "edges": [
            {
                "id": "edge-start-finish",
                "source_id": "start",
                "target_id": "finish",
                "label": "complete",
                "kind": "standard",
                "priority": 100,
                "condition": None,
            }
        ],
    }


class CronTriggerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.services = build_example_services()

    def test_cron_provider_is_registered(self) -> None:
        provider_ids = {provider.provider_id for provider in self.services.node_provider_registry.list_definitions()}

        self.assertIn("start.cron_schedule", provider_ids)
        provider = self.services.node_provider_registry.get("start.cron_schedule")
        self.assertEqual(provider.trigger_mode, "listener")
        self.assertIsNone(provider.listener_transport)

    def test_next_cron_fire_uses_timezone(self) -> None:
        next_fire = next_cron_fire_after(
            "30 9 * * mon-fri",
            "America/New_York",
            datetime(2026, 4, 30, 13, 29, tzinfo=UTC),
        )

        self.assertEqual(next_fire, datetime(2026, 4, 30, 13, 30, tzinfo=UTC))

    def test_cron_service_fires_due_schedule_once_and_reschedules(self) -> None:
        schedule = CronSchedule(
            graph_id="cron-agent",
            cron_expression="0 9 * * *",
            timezone="UTC",
            prompt="Run the morning workflow",
        )
        fired: list[dict[str, object]] = []
        service = CronTriggerService(lambda graph_id: schedule if graph_id == "cron-agent" else None, lambda _, payload: fired.append(payload))
        self.addCleanup(service.stop)
        service.activate("cron-agent")
        service._next_fire_at["cron-agent"] = datetime(2026, 4, 30, 9, 0, tzinfo=UTC)  # noqa: SLF001

        fired_graphs = service.trigger_due(datetime(2026, 4, 30, 9, 0, tzinfo=UTC))

        self.assertEqual(fired_graphs, ["cron-agent"])
        self.assertEqual(fired[0]["source"], "cron_schedule")
        self.assertEqual(fired[0]["prompt"], "Run the morning workflow")
        self.assertEqual(
            service._next_fire_at["cron-agent"],  # noqa: SLF001
            datetime(2026, 5, 1, 9, 0, tzinfo=UTC),
        )

    def test_due_cron_schedule_starts_child_run(self) -> None:
        store, temp_dir = build_isolated_store(self.services)
        self.addCleanup(temp_dir.cleanup)
        store.create_graph(build_cron_graph_payload())
        manager = GraphRunManager(services=self.services, store=store)
        self.addCleanup(manager.reset_runtime)

        session_run_id = manager.start_listener_session("cron-agent")
        manager._cron_service._next_fire_at["cron-agent"] = datetime(2026, 4, 30, 9, 0, tzinfo=UTC)  # noqa: SLF001
        fired_graphs = manager._cron_service.trigger_due(datetime(2026, 4, 30, 9, 0, tzinfo=UTC))  # noqa: SLF001

        self.assertEqual(fired_graphs, ["cron-agent"])
        session_state = manager.get_run(session_run_id)
        child_run_id = next(
            event["payload"]["child_run_id"]
            for event in session_state["event_history"]
            if event["event_type"] == "listener.event.received"
        )
        child_state = self._wait_for_terminal_run(manager, child_run_id)
        self.assertEqual(child_state["input_payload"]["source"], "cron_schedule")
        self.assertEqual(child_state["input_payload"]["prompt"], "Run the morning workflow")

    def test_normalized_payload_includes_schedule_metadata(self) -> None:
        schedule = CronSchedule("graph-a", "*/15 * * * *", "UTC", "Check status")
        payload = normalize_cron_schedule_payload(
            schedule,
            scheduled_for=datetime(2026, 4, 30, 9, 15, tzinfo=UTC),
            fired_at=datetime(2026, 4, 30, 9, 15, 2, tzinfo=UTC),
        )

        self.assertEqual(payload["source"], "cron_schedule")
        self.assertEqual(payload["cron_expression"], "*/15 * * * *")
        self.assertEqual(payload["scheduled_for"], "2026-04-30T09:15:00+00:00")
        self.assertEqual(payload["fired_at"], "2026-04-30T09:15:02+00:00")

    def _wait_for_terminal_run(self, manager: GraphRunManager, run_id: str) -> dict[str, object]:
        deadline = time.time() + 5
        while time.time() < deadline:
            state = manager.get_run(run_id)
            if state.get("status") in {"completed", "failed", "cancelled", "interrupted"}:
                return state
            time.sleep(0.05)
        self.fail(f"Run {run_id} did not terminate.")


if __name__ == "__main__":
    unittest.main()
