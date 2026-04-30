from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from graph_agent.api.graph_store import GraphStore
from graph_agent.api.manager import GraphRunManager
from graph_agent.examples.tool_schema_repair import build_example_graph_payload, build_example_services
from graph_agent.providers.discord import (
    DiscordDeliveryResult,
    DiscordMessageEvent,
    normalize_discord_message_payload,
)
from graph_agent.runtime.core import GraphDefinition
from graph_agent.runtime.engine import GraphRuntime


class FakeDiscordService:
    def __init__(self) -> None:
        self.started_tokens: list[str] = []
        self.stop_calls = 0

    def start(self, token: str) -> bool:
        self.started_tokens.append(token)
        return True

    def stop(self) -> None:
        self.stop_calls += 1


class FakeDiscordMessageSender:
    def __init__(self, *, should_fail: bool = False) -> None:
        self.should_fail = should_fail
        self.sent_messages: list[dict[str, str]] = []

    def send_message(self, *, token: str, channel_id: str, content: str) -> DiscordDeliveryResult:
        if self.should_fail:
            raise RuntimeError("Discord send failed.")
        self.sent_messages.append(
            {
                "token": token,
                "channel_id": channel_id,
                "content": content,
            }
        )
        return DiscordDeliveryResult(
            channel_id=channel_id,
            message_id=f"message-{len(self.sent_messages)}",
            content=content,
            timestamp="2026-04-02T12:00:00+00:00",
            raw_response={"ok": True},
        )


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


def build_discord_graph_payload() -> dict[str, object]:
    return {
        "graph_id": "discord-agent",
        "name": "Discord Agent",
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
                "description": "Captures Discord messages as the graph input.",
                "position": {"x": 0, "y": 0},
                "config": {
                    "trigger_mode": "discord_message",
                    "discord_bot_token_env_var": "{DISCORD_BOT_TOKEN}",
                    "discord_channel_id": "channel-123",
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
                "description": "Returns the Discord payload.",
                "position": {"x": 280, "y": 0},
                "config": {
                    "source_binding": {"type": "latest_envelope", "source": "start"},
                },
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


def build_discord_message(*, author_is_bot: bool = False, author_is_self: bool = False) -> DiscordMessageEvent:
    return DiscordMessageEvent(
        channel_id="channel-123",
        channel_name="agent-inputs",
        guild_id="guild-1",
        author_id="user-42",
        author_name="Morgan",
        message_id="message-99",
        content="Run the Discord-started graph.",
        timestamp="2026-03-30T12:00:00+00:00",
        author_is_bot=author_is_bot,
        author_is_self=author_is_self,
        raw_event={"jump_url": "https://discord.com/channels/guild-1/channel-123/message-99"},
    )


def build_discord_end_graph_payload(*, include_core_output: bool = True) -> dict[str, object]:
    nodes: list[dict[str, object]] = [
        {
            "id": "start",
            "kind": "input",
            "category": "start",
            "label": "Run Button Start",
            "provider_id": "start.manual_run",
            "provider_label": "Run Button Start",
            "description": "",
            "position": {"x": 0, "y": 0},
            "config": {"input_binding": {"type": "input_payload"}},
        },
        {
            "id": "discord_finish",
            "kind": "output",
            "category": "end",
            "label": "Discord End",
            "provider_id": "end.discord_message",
            "provider_label": "Discord Message End",
            "description": "",
            "position": {"x": 280, "y": 120},
            "config": {
                "discord_bot_token_env_var": "{DISCORD_BOT_TOKEN}",
                "discord_channel_id": "channel-456",
                "message_template": "Discord says: {message_payload}",
            },
        },
    ]
    edges: list[dict[str, object]] = [
        {
            "id": "edge-start-discord",
            "source_id": "start",
            "target_id": "discord_finish",
            "label": "send-discord",
            "kind": "standard",
            "priority": 100,
            "condition": None,
        }
    ]
    if include_core_output:
        nodes.append(
            {
                "id": "finish",
                "kind": "output",
                "category": "end",
                "label": "Finish",
                "provider_id": "core.output",
                "provider_label": "Core Output Node",
                "description": "",
                "position": {"x": 280, "y": -80},
                "config": {},
            }
        )
        edges.insert(
            0,
            {
                "id": "edge-start-finish",
                "source_id": "start",
                "target_id": "finish",
                "label": "complete",
                "kind": "standard",
                "priority": 100,
                "condition": None,
            },
        )
    return {
        "graph_id": "discord-end-agent",
        "name": "Discord End Agent",
        "description": "",
        "version": "1.0",
        "start_node_id": "start",
        "env_vars": {"DISCORD_BOT_TOKEN": "DISCORD_BOT_TOKEN"},
        "nodes": nodes,
        "edges": edges,
    }


def wait_for_run_completion(manager: GraphRunManager, run_id: str, timeout_seconds: float = 5.0) -> dict[str, object]:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        state = manager.get_run(run_id)
        if state["status"] in {"completed", "failed"}:
            return state
        time.sleep(0.05)
    raise AssertionError(f"Run '{run_id}' did not finish within {timeout_seconds} seconds.")


class DiscordTriggerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.services = build_example_services()

    def test_catalog_includes_manual_and_discord_start_providers(self) -> None:
        provider_ids = {provider.provider_id for provider in self.services.node_provider_registry.list_definitions()}
        self.assertIn("start.manual_run", provider_ids)
        self.assertIn("start.discord_message", provider_ids)
        self.assertIn("end.discord_message", provider_ids)

    def test_normalize_discord_message_payload_shape(self) -> None:
        payload = normalize_discord_message_payload(build_discord_message())
        self.assertEqual(payload["source"], "discord_message")
        self.assertEqual(payload["content"], "Run the Discord-started graph.")
        self.assertEqual(payload["channel_id"], "channel-123")
        self.assertEqual(payload["author_name"], "Morgan")
        self.assertEqual(payload["message_id"], "message-99")
        self.assertIn("raw_event", payload)

    def test_listener_session_routes_discord_message_to_child_run(self) -> None:
        store, temp_dir = build_isolated_store(self.services)
        self.addCleanup(temp_dir.cleanup)
        store.create_graph(build_discord_graph_payload())
        manager = GraphRunManager(services=self.services, store=store, discord_service=FakeDiscordService())

        with patch.dict(os.environ, {"DISCORD_BOT_TOKEN": "discord-token"}, clear=False):
            session_run_id = manager.start_listener_session("discord-agent")
            run_ids = manager.handle_discord_message(build_discord_message())

        self.assertEqual(len(run_ids), 1)
        self.assertNotEqual(run_ids[0], session_run_id)
        run_state = wait_for_run_completion(manager, run_ids[0])
        self.assertEqual(run_state["status"], "completed")
        self.assertEqual(run_state["parent_run_id"], session_run_id)
        self.assertEqual(run_state["input_payload"]["source"], "discord_message")
        self.assertEqual(run_state["input_payload"]["channel_id"], "channel-123")

    def test_listener_session_resolves_token_from_graph_env_value(self) -> None:
        store, temp_dir = build_isolated_store(self.services)
        self.addCleanup(temp_dir.cleanup)
        graph = build_discord_graph_payload()
        graph["env_vars"] = {"DISCORD_BOT_TOKEN": "graph-env-token"}
        store.create_graph(graph)
        fake_service = FakeDiscordService()
        manager = GraphRunManager(services=self.services, store=store, discord_service=fake_service)

        manager.start_listener_session("discord-agent")

        self.assertEqual(fake_service.started_tokens, ["graph-env-token"])

    def test_handle_discord_message_drops_when_no_session_active(self) -> None:
        store, temp_dir = build_isolated_store(self.services)
        self.addCleanup(temp_dir.cleanup)
        store.create_graph(build_discord_graph_payload())
        manager = GraphRunManager(services=self.services, store=store, discord_service=FakeDiscordService())

        run_ids = manager.handle_discord_message(build_discord_message())
        self.assertEqual(run_ids, [])

    def test_bot_messages_are_ignored_by_default(self) -> None:
        store, temp_dir = build_isolated_store(self.services)
        self.addCleanup(temp_dir.cleanup)
        store.create_graph(build_discord_graph_payload())
        manager = GraphRunManager(services=self.services, store=store, discord_service=FakeDiscordService())

        run_ids = manager.handle_discord_message(build_discord_message(author_is_bot=True))
        self.assertEqual(run_ids, [])

    def test_manual_run_start_provider_still_works(self) -> None:
        store, temp_dir = build_isolated_store(self.services)
        self.addCleanup(temp_dir.cleanup)
        manual_graph = build_example_graph_payload()
        manual_graph["graph_id"] = "manual-agent"
        store.create_graph(manual_graph)
        manager = GraphRunManager(services=self.services, store=store, discord_service=FakeDiscordService())

        run_id = manager.start_run("manual-agent", "Run from the existing editor flow.")
        run_state = wait_for_run_completion(manager, run_id)

        self.assertEqual(run_state["status"], "completed")
        self.assertEqual(run_state["input_payload"], "Run from the existing editor flow.")

    def test_listener_does_not_boot_until_first_activate(self) -> None:
        store, temp_dir = build_isolated_store(self.services)
        self.addCleanup(temp_dir.cleanup)
        store.create_graph(build_discord_graph_payload())
        fake_service = FakeDiscordService()
        manager = GraphRunManager(services=self.services, store=store, discord_service=fake_service)

        with patch.dict(os.environ, {"DISCORD_BOT_TOKEN": "discord-token"}, clear=False):
            manager.start_background_services()
            self.assertEqual(fake_service.started_tokens, [])
            manager.start_listener_session("discord-agent")

        self.assertEqual(fake_service.started_tokens, ["discord-token"])

    def test_discord_end_runs_alongside_core_output_without_overwriting_final_output(self) -> None:
        fake_sender = FakeDiscordMessageSender()
        self.services.discord_message_sender = fake_sender
        graph = GraphDefinition.from_dict(build_discord_end_graph_payload(include_core_output=True))
        graph.validate_against_services(self.services)

        runtime = GraphRuntime(
            services=self.services,
            max_steps=self.services.config["max_steps"],
            max_visits_per_node=self.services.config["max_visits_per_node"],
        )

        with patch.dict(os.environ, {"DISCORD_BOT_TOKEN": "discord-token"}, clear=False):
            state = runtime.run(graph, "Ship the result to Discord too.")

        self.assertEqual(state.status, "completed")
        self.assertEqual(state.final_output, "Ship the result to Discord too.")
        self.assertEqual(len(fake_sender.sent_messages), 1)
        self.assertEqual(fake_sender.sent_messages[0]["channel_id"], "channel-456")
        self.assertEqual(fake_sender.sent_messages[0]["content"], "Discord says: Ship the result to Discord too.")
        self.assertEqual(state.node_outputs["finish"], "Ship the result to Discord too.")
        self.assertEqual(state.node_outputs["discord_finish"]["delivery_status"], "sent")
        self.assertEqual(state.node_outputs["discord_finish"]["source_payload"], "Ship the result to Discord too.")
        self.assertEqual(state.event_history[-1].event_type, "run.completed")

    def test_discord_end_failure_fails_the_run(self) -> None:
        self.services.discord_message_sender = FakeDiscordMessageSender(should_fail=True)
        graph = GraphDefinition.from_dict(build_discord_end_graph_payload(include_core_output=False))
        graph.validate_against_services(self.services)

        runtime = GraphRuntime(
            services=self.services,
            max_steps=self.services.config["max_steps"],
            max_visits_per_node=self.services.config["max_visits_per_node"],
        )

        with patch.dict(os.environ, {"DISCORD_BOT_TOKEN": "discord-token"}, clear=False):
            state = runtime.run(graph, "This run should fail.")

        self.assertEqual(state.status, "failed")
        self.assertIsNotNone(state.terminal_error)
        assert state.terminal_error is not None
        self.assertEqual(state.terminal_error["type"], "node_exception")
        self.assertEqual(state.terminal_error["node_id"], "discord_finish")
        self.assertEqual(state.terminal_error["message"], "Discord send failed.")


if __name__ == "__main__":
    unittest.main()
