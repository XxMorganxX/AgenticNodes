from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
from typing import Any
from collections.abc import Iterator


DEFAULT_OUTLOOK_DEDUPE_DB_PATH = Path(__file__).resolve().parents[3] / ".graph-agent" / "outlook-draft-dedupe.sqlite3"
DEFAULT_IN_PROGRESS_STALE_AFTER_SECONDS = 15 * 60


@dataclass(frozen=True)
class OutlookDraftDeduplicationScope:
    graph_id: str
    node_id: str
    iterator_node_id: str
    iteration_id: str
    source_file: str
    recipients: list[str]
    agent_id: str = ""
    subject: str = ""
    body: str = ""

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "graph_id": self.graph_id,
            "agent_id": self.agent_id,
            "node_id": self.node_id,
            "iterator_node_id": self.iterator_node_id,
            "iteration_id": self.iteration_id,
            "source_file": self.source_file,
            "subject": self.subject,
            "body": self.body,
            "recipients": sorted(dict.fromkeys(self.recipients)),
        }

    def idempotency_key(self) -> str:
        encoded = json.dumps(self.canonical_payload(), sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _dedupe_db_path() -> Path:
    configured = os.environ.get("GRAPH_AGENT_OUTLOOK_DEDUPE_DB", "").strip()
    if configured:
        return Path(configured).expanduser()
    return DEFAULT_OUTLOOK_DEDUPE_DB_PATH


def _in_progress_stale_after_seconds() -> int:
    raw = os.environ.get("GRAPH_AGENT_OUTLOOK_DEDUPE_STALE_SECONDS", "").strip()
    if not raw:
        return DEFAULT_IN_PROGRESS_STALE_AFTER_SECONDS
    try:
        return max(int(raw), 1)
    except ValueError:
        return DEFAULT_IN_PROGRESS_STALE_AFTER_SECONDS


def _is_stale(updated_at: str, stale_after_seconds: int) -> bool:
    try:
        parsed = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
    except ValueError:
        return True
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    age_seconds = (datetime.now(timezone.utc) - parsed).total_seconds()
    return age_seconds >= stale_after_seconds


def _row_to_dict(cursor: sqlite3.Cursor, row: tuple[Any, ...] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    keys = [column[0] for column in cursor.description or []]
    return dict(zip(keys, row, strict=False))


def _json_dumps_safe(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, default=str)
    except (TypeError, ValueError):
        return json.dumps(str(value), sort_keys=True)


class OutlookDraftDedupeStore:
    def __init__(self, db_path: Path | None = None) -> None:
        self._db_path = db_path or _dedupe_db_path()
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize_schema()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(str(self._db_path), timeout=30)
        connection.execute("PRAGMA journal_mode=WAL;")
        connection.execute("PRAGMA synchronous=NORMAL;")
        return connection

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            yield connection
        finally:
            connection.close()

    def _initialize_schema(self) -> None:
        with self._connection() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS outlook_draft_dedupe (
                    idempotency_key TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    scope_json TEXT NOT NULL,
                    graph_id TEXT NOT NULL,
                    agent_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    iterator_node_id TEXT NOT NULL,
                    iteration_id TEXT NOT NULL,
                    source_file TEXT NOT NULL,
                    recipients_json TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    parent_run_id TEXT NOT NULL,
                    draft_id TEXT,
                    provider_message_id TEXT,
                    web_link TEXT,
                    success_output_json TEXT,
                    last_error_json TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            connection.commit()

    def begin_attempt(
        self,
        *,
        scope: OutlookDraftDeduplicationScope,
        run_id: str,
        parent_run_id: str | None,
    ) -> tuple[str, dict[str, Any] | None]:
        key = scope.idempotency_key()
        now_iso = _utc_now_iso()
        stale_after_seconds = _in_progress_stale_after_seconds()
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE;")
            cursor = connection.execute(
                """
                SELECT *
                FROM outlook_draft_dedupe
                WHERE idempotency_key = ?
                """,
                (key,),
            )
            existing = _row_to_dict(cursor, cursor.fetchone())
            if existing is None:
                connection.execute(
                    """
                    INSERT INTO outlook_draft_dedupe (
                        idempotency_key,
                        status,
                        scope_json,
                        graph_id,
                        agent_id,
                        node_id,
                        iterator_node_id,
                        iteration_id,
                        source_file,
                        recipients_json,
                        run_id,
                        parent_run_id,
                        created_at,
                        updated_at
                    ) VALUES (?, 'in_progress', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        key,
                        json.dumps(scope.canonical_payload(), sort_keys=True),
                        scope.graph_id,
                        scope.agent_id,
                        scope.node_id,
                        scope.iterator_node_id,
                        scope.iteration_id,
                        scope.source_file,
                        json.dumps(scope.recipients),
                        run_id,
                        str(parent_run_id or ""),
                        now_iso,
                        now_iso,
                    ),
                )
                connection.commit()
                return "proceed", None

            status = str(existing.get("status", "") or "")
            if status == "success":
                connection.commit()
                return "deduped_success", existing
            if status == "in_progress" and not _is_stale(str(existing.get("updated_at", "") or ""), stale_after_seconds):
                connection.commit()
                return "deduped_in_progress", existing

            connection.execute(
                """
                UPDATE outlook_draft_dedupe
                SET status = 'in_progress',
                    run_id = ?,
                    parent_run_id = ?,
                    last_error_json = NULL,
                    updated_at = ?
                WHERE idempotency_key = ?
                """,
                (
                    run_id,
                    str(parent_run_id or ""),
                    now_iso,
                    key,
                ),
            )
            connection.commit()
            return "proceed", existing

    def mark_success(
        self,
        *,
        scope: OutlookDraftDeduplicationScope,
        output_payload: dict[str, Any],
    ) -> None:
        key = scope.idempotency_key()
        now_iso = _utc_now_iso()
        with self._connection() as connection:
            connection.execute(
                """
                UPDATE outlook_draft_dedupe
                SET status = 'success',
                    draft_id = ?,
                    provider_message_id = ?,
                    web_link = ?,
                    success_output_json = ?,
                    last_error_json = NULL,
                    updated_at = ?
                WHERE idempotency_key = ?
                """,
                (
                    str(output_payload.get("draft_id", "") or ""),
                    str(output_payload.get("provider_message_id", "") or ""),
                    str(output_payload.get("web_link", "") or ""),
                    _json_dumps_safe(output_payload),
                    now_iso,
                    key,
                ),
            )
            connection.commit()

    def mark_failure(
        self,
        *,
        scope: OutlookDraftDeduplicationScope,
        error_payload: dict[str, Any],
    ) -> None:
        key = scope.idempotency_key()
        now_iso = _utc_now_iso()
        with self._connection() as connection:
            connection.execute(
                """
                UPDATE outlook_draft_dedupe
                SET status = 'failed',
                    last_error_json = ?,
                    updated_at = ?
                WHERE idempotency_key = ?
                """,
                (
                    _json_dumps_safe(error_payload),
                    now_iso,
                    key,
                ),
            )
            connection.commit()

    @staticmethod
    def decode_success_output(row: dict[str, Any] | None) -> dict[str, Any] | None:
        if not isinstance(row, dict):
            return None
        raw = row.get("success_output_json")
        if not isinstance(raw, str) or not raw.strip():
            return None
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return None
        return dict(parsed) if isinstance(parsed, dict) else None
