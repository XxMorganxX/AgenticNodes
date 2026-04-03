from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import logging
from threading import Lock, Thread, current_thread
from typing import Any, Callable
from urllib import error as urllib_error, parse as urllib_parse, request as urllib_request

try:
    import discord
except ImportError:  # pragma: no cover - exercised only when dependency is missing at runtime
    discord = None


LOGGER = logging.getLogger(__name__)
DISCORD_API_BASE_URL = "https://discord.com/api/v10"
DISCORD_MESSAGE_MAX_LENGTH = 2000


def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


@dataclass(frozen=True)
class DiscordMessageEvent:
    channel_id: str
    author_id: str
    author_name: str
    message_id: str
    content: str
    timestamp: str
    guild_id: str | None = None
    channel_name: str | None = None
    author_is_bot: bool = False
    author_is_self: bool = False
    raw_event: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DiscordDeliveryResult:
    channel_id: str
    message_id: str
    content: str
    timestamp: str
    raw_response: dict[str, Any] = field(default_factory=dict)


def normalize_discord_message_payload(message: DiscordMessageEvent) -> dict[str, Any]:
    return {
        "content": message.content,
        "channel_id": message.channel_id,
        "channel_name": message.channel_name,
        "guild_id": message.guild_id,
        "author_id": message.author_id,
        "author_name": message.author_name,
        "author_is_bot": message.author_is_bot,
        "author_is_self": message.author_is_self,
        "message_id": message.message_id,
        "timestamp": message.timestamp,
        "source": "discord_message",
        "raw_event": dict(message.raw_event),
    }


class DiscordMessageSender:
    def __init__(self, *, api_base_url: str = DISCORD_API_BASE_URL, timeout_seconds: float = 10.0) -> None:
        self._api_base_url = api_base_url.rstrip("/")
        self._timeout_seconds = max(timeout_seconds, 1.0)

    def send_message(self, *, token: str, channel_id: str, content: str) -> DiscordDeliveryResult:
        normalized_token = token.strip()
        normalized_channel_id = channel_id.strip()
        normalized_content = content.strip()

        if not normalized_token:
            raise ValueError("Discord bot token is required to send a message.")
        if not normalized_channel_id:
            raise ValueError("Discord channel id is required to send a message.")
        if not normalized_content:
            raise ValueError("Discord message content is empty.")
        if len(normalized_content) > DISCORD_MESSAGE_MAX_LENGTH:
            raise ValueError(
                f"Discord message content exceeds the {DISCORD_MESSAGE_MAX_LENGTH} character limit."
            )

        body = json.dumps({"content": normalized_content}).encode("utf-8")
        request = urllib_request.Request(
            url=f"{self._api_base_url}/channels/{urllib_parse.quote(normalized_channel_id, safe='')}/messages",
            data=body,
            headers={
                "Authorization": f"Bot {normalized_token}",
                "Content-Type": "application/json",
                "User-Agent": "graph-agent-discord-sender/1.0",
            },
            method="POST",
        )

        try:
            with urllib_request.urlopen(request, timeout=self._timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8") or "{}")
        except urllib_error.HTTPError as exc:
            response_text = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Discord API request failed with status {exc.code}: {response_text}") from exc
        except urllib_error.URLError as exc:
            raise RuntimeError(f"Discord API request failed: {exc.reason}") from exc

        return DiscordDeliveryResult(
            channel_id=str(payload.get("channel_id", normalized_channel_id)),
            message_id=str(payload.get("id", "")),
            content=str(payload.get("content", normalized_content)),
            timestamp=str(payload.get("timestamp", _utc_now_iso())),
            raw_response=payload if isinstance(payload, dict) else {},
        )


class DiscordTriggerService:
    def __init__(self, on_message: Callable[[DiscordMessageEvent], None]) -> None:
        self._on_message = on_message
        self._lock = Lock()
        self._client: Any | None = None
        self._thread: Thread | None = None
        self._token: str = ""

    def start(self, token: str) -> bool:
        normalized_token = token.strip()
        if not normalized_token:
            self.stop()
            return False
        if discord is None:
            raise RuntimeError("discord.py is not installed. Run `pip install -e .` to enable Discord triggers.")

        with self._lock:
            thread = self._thread
            if thread is not None and thread.is_alive() and self._token == normalized_token:
                return False

        self.stop()
        client = self._build_client()
        thread = Thread(target=self._run_client, args=(client, normalized_token), daemon=True, name="discord-trigger-service")

        with self._lock:
            self._client = client
            self._thread = thread
            self._token = normalized_token

        thread.start()
        return True

    def stop(self) -> None:
        with self._lock:
            client = self._client
            thread = self._thread
            self._client = None
            self._thread = None
            self._token = ""

        if client is not None:
            loop = getattr(client, "loop", None)
            if loop is not None and loop.is_running():
                try:
                    asyncio.run_coroutine_threadsafe(client.close(), loop).result(timeout=5)
                except Exception:  # noqa: BLE001
                    LOGGER.exception("Failed to close Discord client cleanly.")

        if thread is not None and thread.is_alive() and current_thread() is not thread:
            thread.join(timeout=5)

    def _build_client(self) -> Any:
        assert discord is not None
        intents = discord.Intents.default()
        intents.guilds = True
        intents.messages = True
        intents.message_content = True
        client = discord.Client(intents=intents)

        @client.event
        async def on_ready() -> None:
            user = getattr(client, "user", None)
            LOGGER.info("Discord trigger service connected as %s.", getattr(user, "name", "unknown"))

        @client.event
        async def on_message(message: Any) -> None:
            self._handle_message(client, message)

        return client

    def _run_client(self, client: Any, token: str) -> None:
        try:
            client.run(token, log_handler=None)
        except Exception:  # noqa: BLE001
            LOGGER.exception("Discord trigger service stopped after an error.")
        finally:
            with self._lock:
                if self._client is client:
                    self._client = None
                if self._thread is current_thread():
                    self._thread = None
                self._token = ""

    def _handle_message(self, client: Any, message: Any) -> None:
        author = getattr(message, "author", None)
        client_user = getattr(client, "user", None)
        channel = getattr(message, "channel", None)
        guild = getattr(message, "guild", None)
        event = DiscordMessageEvent(
            channel_id=str(getattr(channel, "id", "")),
            channel_name=str(getattr(channel, "name", "")) if channel is not None else None,
            guild_id=str(getattr(guild, "id", "")) if guild is not None else None,
            author_id=str(getattr(author, "id", "")),
            author_name=str(getattr(author, "display_name", None) or getattr(author, "name", "")),
            message_id=str(getattr(message, "id", "")),
            content=str(getattr(message, "content", "") or ""),
            timestamp=(
                getattr(message, "created_at", None).isoformat()
                if getattr(message, "created_at", None) is not None
                else _utc_now_iso()
            ),
            author_is_bot=bool(getattr(author, "bot", False)),
            author_is_self=bool(client_user is not None and getattr(author, "id", None) == getattr(client_user, "id", object())),
            raw_event={
                "jump_url": str(getattr(message, "jump_url", "") or ""),
                "channel_type": type(channel).__name__ if channel is not None else None,
            },
        )
        self._on_message(event)
