from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from graph_agent.api.graph_store import GraphStore
from graph_agent.api.manager import GraphRunManager, _DiscordTriggerAdapter
from graph_agent.examples.tool_schema_repair import build_example_services


class FakeDiscordService:
    def __init__(self) -> None:
        self.started_tokens: list[str] = []
        self.stop_calls = 0

    def start(self, token: str) -> bool:
        self.started_tokens.append(token)
        return True

    def stop(self) -> None:
        self.stop_calls += 1


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


def build_discord_listener_graph(graph_id: str) -> dict[str, object]:
    return {
        "graph_id": graph_id,
        "name": f"Discord Listener {graph_id}",
        "description": "",
        "version": "1.0",
        "start_node_id": "start",
        "env_vars": {"DISCORD_BOT_TOKEN": "DISCORD_BOT_TOKEN"},
        "nodes": [
            {
                "id": "start",
                "kind": "input",
                "category": "start",
                "label": "Discord Start",
                "provider_id": "start.discord_message",
                "provider_label": "Discord Message Start",
                "description": "",
                "position": {"x": 0, "y": 0},
                "config": {
                    "trigger_mode": "discord_message",
                    "discord_bot_token_env_var": "{DISCORD_BOT_TOKEN}",
                    "discord_channel_id": "channel-1",
                    "ignore_bot_messages": True,
                    "ignore_self_messages": True,
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
                "description": "",
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


class DiscordTriggerAdapterRefcountTests(unittest.TestCase):
    def test_first_activate_starts_subsequent_calls_no_op(self) -> None:
        service = FakeDiscordService()
        adapter = _DiscordTriggerAdapter(service, lambda: "discord-token")

        adapter.activate("graph-a")
        adapter.activate("graph-a")

        self.assertEqual(service.started_tokens, ["discord-token"])
        self.assertEqual(service.stop_calls, 0)

    def test_two_graphs_share_one_socket_until_both_deactivate(self) -> None:
        service = FakeDiscordService()
        adapter = _DiscordTriggerAdapter(service, lambda: "discord-token")

        adapter.activate("graph-a")
        adapter.activate("graph-b")
        self.assertEqual(service.started_tokens, ["discord-token"])

        adapter.deactivate("graph-a")
        self.assertEqual(service.stop_calls, 0)

        adapter.deactivate("graph-b")
        self.assertEqual(service.stop_calls, 1)

    def test_deactivate_unknown_graph_is_noop(self) -> None:
        service = FakeDiscordService()
        adapter = _DiscordTriggerAdapter(service, lambda: "discord-token")

        adapter.deactivate("never-activated")
        self.assertEqual(service.stop_calls, 0)

    def test_stop_clears_all_active_graph_ids(self) -> None:
        service = FakeDiscordService()
        adapter = _DiscordTriggerAdapter(service, lambda: "discord-token")

        adapter.activate("graph-a")
        adapter.activate("graph-b")
        adapter.stop()

        self.assertEqual(service.stop_calls, 1)
        adapter.activate("graph-c")
        self.assertEqual(service.started_tokens, ["discord-token", "discord-token"])

    def test_activate_without_token_raises_and_does_not_register(self) -> None:
        service = FakeDiscordService()
        adapter = _DiscordTriggerAdapter(service, lambda: "")

        with self.assertRaises(RuntimeError):
            adapter.activate("graph-a")

        # Subsequent activate should not be a no-op since the prior failure
        # rolled back the membership; provide a token now and confirm it boots.
        adapter._token_provider = lambda: "discord-token"  # type: ignore[attr-defined]
        adapter.activate("graph-a")
        self.assertEqual(service.started_tokens, ["discord-token"])


class ListenerSessionLifecycleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.services = build_example_services()

    def _build_manager(self) -> tuple[GraphRunManager, FakeDiscordService, tempfile.TemporaryDirectory[str]]:
        store, temp_dir = build_isolated_store(self.services)
        self.addCleanup(temp_dir.cleanup)
        store.create_graph(build_discord_listener_graph("listener-a"))
        fake = FakeDiscordService()
        manager = GraphRunManager(services=self.services, store=store, discord_service=fake)
        return manager, fake, temp_dir

    def test_session_status_listening_and_listener_active(self) -> None:
        manager, fake, _ = self._build_manager()
        with patch.dict(os.environ, {"DISCORD_BOT_TOKEN": "discord-token"}, clear=False):
            run_id = manager.start_listener_session("listener-a")

        self.assertEqual(fake.started_tokens, ["discord-token"])
        state = manager.get_run(run_id)
        self.assertEqual(state["status"], "listening")
        self.assertTrue(manager.is_listening_session(run_id))

        types = [event["event_type"] for event in state["event_history"]]
        self.assertIn("run.started", types)
        self.assertIn("listener.session.started", types)

    def test_start_listener_session_idempotent_per_graph(self) -> None:
        manager, fake, _ = self._build_manager()
        with patch.dict(os.environ, {"DISCORD_BOT_TOKEN": "discord-token"}, clear=False):
            first = manager.start_listener_session("listener-a")
            second = manager.start_listener_session("listener-a")

        self.assertEqual(first, second)
        self.assertEqual(fake.started_tokens, ["discord-token"])

    def test_stop_listener_session_deactivates_and_records_terminal(self) -> None:
        manager, fake, _ = self._build_manager()
        with patch.dict(os.environ, {"DISCORD_BOT_TOKEN": "discord-token"}, clear=False):
            run_id = manager.start_listener_session("listener-a")
            self.assertTrue(manager.is_listening_session(run_id))
            manager.stop_listener_session(run_id, reason="client_disconnected")

        self.assertEqual(fake.stop_calls, 1)
        self.assertFalse(manager.is_listening_session(run_id))
        state = manager.get_run(run_id)
        types = [event["event_type"] for event in state["event_history"]]
        self.assertIn("listener.session.stopped", types)
        self.assertIn("run.completed", types)

    def test_stop_listener_session_user_initiated_records_cancelled(self) -> None:
        manager, _, _ = self._build_manager()
        with patch.dict(os.environ, {"DISCORD_BOT_TOKEN": "discord-token"}, clear=False):
            run_id = manager.start_listener_session("listener-a")
            manager.stop_listener_session(run_id, reason="user_initiated")

        state = manager.get_run(run_id)
        types = [event["event_type"] for event in state["event_history"]]
        self.assertIn("run.cancelled", types)
        self.assertNotIn("run.completed", types[-3:])

    def test_stop_listener_session_idempotent(self) -> None:
        manager, fake, _ = self._build_manager()
        with patch.dict(os.environ, {"DISCORD_BOT_TOKEN": "discord-token"}, clear=False):
            run_id = manager.start_listener_session("listener-a")
            manager.stop_listener_session(run_id)
            manager.stop_listener_session(run_id)

        self.assertEqual(fake.stop_calls, 1)

    def test_start_run_rejects_listener_mode_graph(self) -> None:
        manager, _, _ = self._build_manager()
        with self.assertRaises(ValueError) as ctx:
            manager.start_run("listener-a", "manual run attempt")
        self.assertIn("listener session", str(ctx.exception))

    def test_listener_status_flips_to_listening_only_after_activate(self) -> None:
        """Status must reach 'listening' only after the trigger transport is live."""
        manager, fake, _ = self._build_manager()

        observed_states: list[str] = []
        original_activate = fake.start

        def recording_start(token: str) -> bool:
            # When activate() runs, the run state should not yet be 'listening'.
            session_run_id = next(iter(manager._listener_session_metadata.keys()))  # noqa: SLF001
            observed_states.append(manager._run_states[session_run_id]["status"])  # noqa: SLF001
            return original_activate(token)

        fake.start = recording_start  # type: ignore[method-assign]

        with patch.dict(os.environ, {"DISCORD_BOT_TOKEN": "discord-token"}, clear=False):
            run_id = manager.start_listener_session("listener-a")

        # During activate(): status is still 'running' (the post-activate flip
        # hasn't happened yet). After activate(): status is 'listening'.
        self.assertEqual(observed_states, ["running"])
        self.assertEqual(manager.get_run(run_id)["status"], "listening")

    def test_listener_activation_failure_marks_run_failed(self) -> None:
        store, temp_dir = build_isolated_store(self.services)
        self.addCleanup(temp_dir.cleanup)
        store.create_graph(build_discord_listener_graph("listener-a"))

        class FailingDiscordService(FakeDiscordService):
            def start(self, token: str) -> bool:
                raise RuntimeError("simulated boot failure")

        fake = FailingDiscordService()
        manager = GraphRunManager(services=self.services, store=store, discord_service=fake)

        with patch.dict(os.environ, {"DISCORD_BOT_TOKEN": "discord-token"}, clear=False):
            with self.assertRaises(RuntimeError):
                manager.start_listener_session("listener-a")

        # Session metadata should be cleaned up; no leaked listening session.
        self.assertEqual(manager._active_sessions, {})  # noqa: SLF001
        self.assertEqual(manager._listener_session_metadata, {})  # noqa: SLF001

    def test_delete_listener_graph_terminates_active_session(self) -> None:
        manager, fake, _ = self._build_manager()
        with patch.dict(os.environ, {"DISCORD_BOT_TOKEN": "discord-token"}, clear=False):
            run_id = manager.start_listener_session("listener-a")
            self.assertTrue(manager.is_listening_session(run_id))
            manager.delete_graph("listener-a")

        self.assertFalse(manager.is_listening_session(run_id))
        self.assertEqual(fake.stop_calls, 1)


if __name__ == "__main__":
    unittest.main()
