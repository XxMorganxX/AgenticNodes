"""Inbound HTTP webhook trigger for listener-mode graphs (`start.webhook`)."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import hmac
import json
import re
from threading import Lock
from typing import Any, Callable, Mapping
from urllib.parse import parse_qsl

from graph_agent.providers.triggers import TriggerService
from graph_agent.runtime.core import GraphValidationError, resolve_graph_process_env, utc_now_iso

WEBHOOK_START_PROVIDER_ID = "start.webhook"


class WebhookHttpError(Exception):
    """Raised by webhook dispatch to map to HTTP status codes in FastAPI."""

    def __init__(self, status_code: int, detail: str) -> None:
        self.status_code = int(status_code)
        self.detail = str(detail)
        super().__init__(detail)



WEBHOOK_SLUG_PATTERN = re.compile(r"^[a-zA-Z0-9_-]{4,128}$")

_ALLOWED_HTTP_METHODS = frozenset({"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"})

# Headers copied into run payload (lowercase keys); do not include auth/signature secrets.
WEBHOOK_LOG_HEADER_ALLOWLIST = frozenset(
    {
        "content-type",
        "user-agent",
        "x-request-id",
        "x-github-event",
        "x-gitlab-event",
        "x-gitlab-token",
    }
)


def normalize_webhook_slug(raw: Any) -> str:
    return str(raw or "").strip()


def parse_http_methods(raw: Any) -> list[str]:
    if isinstance(raw, list):
        items = [str(x).strip().upper() for x in raw if str(x).strip()]
    else:
        text = str(raw or "POST").strip()
        items = [part.strip().upper() for part in text.replace(";", ",").split(",") if part.strip()]
    out: list[str] = []
    for m in items:
        if m in _ALLOWED_HTTP_METHODS and m not in out:
            out.append(m)
    return out if out else ["POST"]


def validate_webhook_slug(slug: str) -> None:
    if not slug:
        raise GraphValidationError("Webhook start node requires a non-empty webhook_path_slug.")
    if not WEBHOOK_SLUG_PATTERN.match(slug):
        raise GraphValidationError(
            "webhook_path_slug must be 4–128 characters and use only letters, digits, underscores, and hyphens."
        )


@dataclass(frozen=True)
class WebhookStartResolved:
    graph_id: str
    slug: str
    http_methods: tuple[str, ...]
    verification_mode: str
    webhook_secret_env_var: str
    webhook_shared_secret_header: str
    signature_header: str
    signature_prefix: str
    event_type_json_path: str
    event_type_allowlist: tuple[str, ...]
    prompt: str
    listener_agent_id: str | None = None  # agent_id when webhook is on an environment swimlane


def _extract_json_path(obj: Any, path: str) -> Any:
    path = str(path or "").strip()
    if not path or not isinstance(obj, dict):
        return None
    cur: Any = obj
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def parse_event_type_allowlist(raw: Any) -> tuple[str, ...]:
    text = str(raw or "").strip()
    if not text:
        return ()
    parts = [p.strip() for p in text.replace(";", ",").split(",") if p.strip()]
    return tuple(dict.fromkeys(parts))


def verify_webhook_request(
    resolved: WebhookStartResolved,
    env_vars: Mapping[str, str],
    http_method: str,
    headers_lower: dict[str, str],
    body_bytes: bytes,
) -> None:
    mode = str(resolved.verification_mode or "none").strip().lower()
    if mode == "none":
        return

    secret_template = str(resolved.webhook_secret_env_var or "{WEBHOOK_SECRET}").strip()
    secret = resolve_graph_process_env(secret_template, env_vars).strip()
    if not secret:
        raise ValueError(
            f"Webhook verification requires secret env-var {secret_template!r} to resolve to a non-empty value."
        )

    if mode == "shared_secret":
        header_name = str(resolved.webhook_shared_secret_header or "X-Webhook-Secret").strip().lower()
        got = headers_lower.get(header_name, "").strip()
        if got and secrets_compare(secret, got):
            return
        auth = headers_lower.get("authorization", "")
        if auth.lower().startswith("bearer "):
            token = auth[7:].strip()
            if token and secrets_compare(secret, token):
                return
        raise ValueError("Webhook shared-secret verification failed.")

    if mode == "hmac_sha256":
        sig_header = str(resolved.signature_header or "X-Signature").strip().lower()
        raw_sig = headers_lower.get(sig_header, "").strip()
        prefix = str(resolved.signature_prefix or "").strip()
        if prefix and raw_sig.startswith(prefix):
            raw_sig = raw_sig[len(prefix) :].strip()
        expected = hmac.new(secret.encode("utf-8"), body_bytes, hashlib.sha256).hexdigest()
        if not raw_sig or not secrets_compare(expected, raw_sig):
            raise ValueError("Webhook HMAC verification failed.")
        return

    raise ValueError(f"Unknown webhook verification_mode '{mode}'.")


def secrets_compare(a: str, b: str) -> bool:
    try:
        return hmac.compare_digest(a.encode("utf-8"), b.encode("utf-8"))
    except Exception:  # noqa: BLE001
        return False


def passes_event_filter(
    resolved: WebhookStartResolved,
    parsed_body: Any,
) -> bool:
    path = str(resolved.event_type_json_path or "").strip()
    allow: tuple[str, ...] = resolved.event_type_allowlist
    if not path or not allow:
        return True
    value = _extract_json_path(parsed_body, path)
    if value is None:
        return False
    text = str(value).strip()
    return text in allow


def filter_webhook_log_headers(headers_lower: dict[str, str]) -> dict[str, str]:
    return {k: v for k, v in headers_lower.items() if k in WEBHOOK_LOG_HEADER_ALLOWLIST}


def build_webhook_child_payload(
    *,
    graph_id: str,
    http_method: str,
    path: str,
    query_string: str,
    header_snapshot: dict[str, str],
    body_value: Any,
    prompt: str,
    listener_agent_id: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "source": "webhook",
        "graph_id": graph_id,
        "http_method": http_method,
        "path": path,
        "query": dict(parse_qsl(query_string, keep_blank_values=True)) if query_string else {},
        "headers": header_snapshot,
        "body": body_value,
        "received_at": utc_now_iso(),
    }
    if prompt.strip():
        payload["prompt"] = prompt
    if listener_agent_id:
        payload["listener_agent_id"] = listener_agent_id
    return payload


def parse_body_for_storage(body_bytes: bytes, content_type: str | None) -> Any:
    ct = str(content_type or "").split(";")[0].strip().lower()
    text = body_bytes.decode("utf-8", errors="replace")
    if not text.strip():
        return None
    if "json" in ct or ct in {"", "application/octet-stream"}:
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text
    return text


class WebhookTriggerService:
    """Maps webhook slug -> graph_id while a listener session is active."""

    name = "webhook"

    def __init__(self, slug_resolver: Callable[[str], str | None]) -> None:
        self._slug_resolver = slug_resolver
        self._lock = Lock()
        self._slug_to_graph: dict[str, str] = {}
        self._graph_slugs: dict[str, set[str]] = {}

    def register_slugs(self, graph_id: str, slugs: list[str]) -> None:
        """Register one or more path slugs for the same document (multi-agent environments)."""
        normalized = str(graph_id or "").strip()
        if not normalized:
            return
        cleaned: list[str] = []
        for raw in slugs:
            s = normalize_webhook_slug(raw)
            if not s:
                continue
            validate_webhook_slug(s)
            cleaned.append(s)
        if not cleaned:
            raise RuntimeError(
                f"Graph '{normalized}' has no webhook_path_slug; set it on the Webhook start node before listening."
            )
        with self._lock:
            old_slugs = self._graph_slugs.pop(normalized, set())
            for old in old_slugs:
                if self._slug_to_graph.get(old) == normalized:
                    self._slug_to_graph.pop(old, None)
            for s in cleaned:
                existing = self._slug_to_graph.get(s)
                if existing is not None and existing != normalized:
                    raise RuntimeError(
                        f"Webhook slug '{s}' is already active for another listening graph '{existing}'."
                    )
            for s in cleaned:
                self._slug_to_graph[s] = normalized
            self._graph_slugs[normalized] = set(cleaned)

    def activate(self, graph_id: str) -> None:
        normalized = str(graph_id or "").strip()
        if not normalized:
            return
        slug = self._slug_resolver(normalized)
        self.register_slugs(normalized, [str(slug or "")])

    def deactivate(self, graph_id: str) -> None:
        normalized = str(graph_id or "").strip()
        with self._lock:
            old_slugs = self._graph_slugs.pop(normalized, set())
            for old in old_slugs:
                if self._slug_to_graph.get(old) == normalized:
                    self._slug_to_graph.pop(old, None)

    def stop(self) -> None:
        with self._lock:
            self._graph_slugs.clear()
            self._slug_to_graph.clear()

    def resolve_graph_id(self, slug: str) -> str | None:
        key = normalize_webhook_slug(slug)
        with self._lock:
            return self._slug_to_graph.get(key)
