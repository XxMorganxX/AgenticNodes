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
from collections.abc import Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from graph_agent.runtime.agent_filesystem import resolve_agent_filesystem_root
from graph_agent.runtime.supabase_data import (
    SupabaseDataError,
    build_supabase_rest_auth_headers,
    parse_supabase_filter_lines,
)


DEFAULT_SUPABASE_TABLE_ROWS_DB_PATH = resolve_agent_filesystem_root().parent / "supabase-table-rows.sqlite3"


@dataclass(frozen=True)
class SupabaseTableRowsCursorScope:
    graph_id: str
    agent_id: str
    node_id: str
    connection_identity: str
    schema: str
    table_name: str
    filters_text: str
    cursor_column: str
    row_id_column: str

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "graph_id": self.graph_id,
            "agent_id": self.agent_id,
            "node_id": self.node_id,
            "connection_identity": self.connection_identity,
            "schema": self.schema,
            "table_name": self.table_name,
            "filters_text": self.filters_text,
            "cursor_column": self.cursor_column,
            "row_id_column": self.row_id_column,
        }

    def scope_key(self) -> str:
        encoded = json.dumps(self.canonical_payload(), sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class SupabaseTableRowsWatermark:
    last_cursor_value: str
    last_row_id: str
    updated_at: str


@dataclass(frozen=True)
class SupabaseTableRowsRequest:
    supabase_url: str
    supabase_key: str
    schema: str
    table_name: str
    select: str
    filters_text: str
    cursor_column: str
    row_id_column: str
    page_size: int
    include_previously_processed_rows: bool = False
    last_cursor_value: str = ""
    last_row_id: str = ""


@dataclass(frozen=True)
class SupabaseTableRowsResult:
    rows: list[dict[str, Any]]
    request_urls: list[str]
    row_count: int
    last_cursor_value: str
    last_row_id: str
    schema: str
    table_name: str
    select: str
    cursor_column: str
    row_id_column: str
    include_previously_processed_rows: bool


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _cursor_store_db_path() -> Path:
    configured = os.environ.get("GRAPH_AGENT_SUPABASE_TABLE_ROWS_DB", "").strip()
    if configured:
        return Path(configured).expanduser()
    return DEFAULT_SUPABASE_TABLE_ROWS_DB_PATH


def _row_to_dict(cursor: sqlite3.Cursor, row: tuple[Any, ...] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    keys = [column[0] for column in cursor.description or []]
    return dict(zip(keys, row))


def _normalize_filter_value(value: Any, *, field_name: str) -> str:
    if value is None:
        raise SupabaseDataError(
            f"Supabase iterator row is missing required field '{field_name}'.",
            error_type="missing_supabase_iterator_field",
            details={"field_name": field_name},
        )
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    normalized = str(value).strip()
    if not normalized:
        raise SupabaseDataError(
            f"Supabase iterator row field '{field_name}' resolved to an empty value.",
            error_type="missing_supabase_iterator_field",
            details={"field_name": field_name},
        )
    return normalized


def _ensure_select_includes_columns(select: str, required_columns: list[str]) -> str:
    normalized_select = str(select or "").strip()
    if not normalized_select:
        existing: set[str] = set()
        extras = [column for column in required_columns if column not in existing]
        return ",".join(extras)
    if normalized_select == "*":
        return normalized_select
    existing = {part.strip() for part in normalized_select.split(",") if part.strip()}
    extras = [column for column in required_columns if column not in existing]
    if not extras:
        return normalized_select
    return ",".join([normalized_select, *extras])


def filter_supabase_table_row_output(row: Mapping[str, Any], select: str) -> dict[str, Any]:
    normalized_select = str(select or "").strip()
    if not normalized_select:
        return {}
    if normalized_select == "*":
        return dict(row)
    selected_columns = [part.strip() for part in normalized_select.split(",") if part.strip()]
    return {column: row[column] for column in selected_columns if column in row}


def _build_iterator_or_filter(
    *,
    cursor_column: str,
    row_id_column: str,
    last_cursor_value: str,
    last_row_id: str,
) -> str:
    if not last_cursor_value:
        return ""
    if not last_row_id:
        return f"{cursor_column}.gt.{last_cursor_value}"
    return (
        f"({cursor_column}.gt.{last_cursor_value},"
        f"and({cursor_column}.eq.{last_cursor_value},{row_id_column}.gt.{last_row_id}))"
    )


def _fetch_supabase_table_rows_page(
    request: SupabaseTableRowsRequest,
    *,
    last_cursor_value: str,
    last_row_id: str,
) -> tuple[str, list[dict[str, Any]]]:
    supabase_url = str(request.supabase_url or "").strip().rstrip("/")
    supabase_key = str(request.supabase_key or "").strip()
    schema = str(request.schema or "public").strip() or "public"
    table_name = str(request.table_name or "").strip()
    cursor_column = str(request.cursor_column or "").strip()
    row_id_column = str(request.row_id_column or "").strip()
    if not supabase_url:
        raise SupabaseDataError("Supabase URL is required.", error_type="missing_supabase_url")
    if not supabase_key:
        raise SupabaseDataError("Supabase key is required.", error_type="missing_supabase_key")
    if not table_name:
        raise SupabaseDataError("Supabase table_name is required.", error_type="missing_supabase_table_name")
    if not cursor_column:
        raise SupabaseDataError("Supabase iterator cursor_column is required.", error_type="missing_supabase_cursor_column")
    if not row_id_column:
        raise SupabaseDataError("Supabase iterator row_id_column is required.", error_type="missing_supabase_row_id_column")
    if int(request.page_size) < 1:
        raise SupabaseDataError("Supabase iterator page_size must be at least 1.", error_type="invalid_supabase_limit")

    select = _ensure_select_includes_columns(request.select, [cursor_column, row_id_column])
    query_pairs: list[tuple[str, str]] = [("select", select)]
    query_pairs.extend(parse_supabase_filter_lines(request.filters_text))
    or_filter = _build_iterator_or_filter(
        cursor_column=cursor_column,
        row_id_column=row_id_column,
        last_cursor_value=last_cursor_value,
        last_row_id=last_row_id,
    )
    if or_filter:
        if or_filter.startswith("("):
            query_pairs.append(("or", or_filter))
        else:
            key, _, value = or_filter.partition("=")
            if key and value:
                query_pairs.append((key, value))
    query_pairs.append(("order", f"{cursor_column}.asc"))
    if row_id_column != cursor_column:
        query_pairs.append(("order", f"{row_id_column}.asc"))
    query_pairs.append(("limit", str(int(request.page_size))))

    source_path = f"/rest/v1/{quote(table_name, safe='')}"
    request_url = f"{supabase_url}{source_path}?{urlencode(query_pairs)}"
    headers = {
        **build_supabase_rest_auth_headers(supabase_key),
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Accept-Profile": schema,
        "Content-Profile": schema,
    }
    http_request = Request(request_url, method="GET", headers=headers)
    try:
        with urlopen(http_request) as response:
            raw_body = response.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise SupabaseDataError(
            f"Supabase iterator request failed: {exc.code} {detail}".strip(),
            error_type="supabase_request_failed",
            details={"status_code": exc.code, "table_name": table_name},
        ) from exc
    except URLError as exc:
        raise SupabaseDataError(
            f"Supabase iterator request failed: {exc.reason}",
            error_type="supabase_request_failed",
            details={"table_name": table_name},
        ) from exc

    if not raw_body.strip():
        return request_url, []
    try:
        decoded = json.loads(raw_body)
    except json.JSONDecodeError as exc:
        raise SupabaseDataError(
            "Supabase iterator response was not valid JSON.",
            error_type="invalid_supabase_response",
            details={"table_name": table_name},
        ) from exc
    if not isinstance(decoded, list):
        raise SupabaseDataError(
            "Supabase iterator response must be a JSON array of table rows.",
            error_type="invalid_supabase_response",
            details={"table_name": table_name},
        )
    rows: list[dict[str, Any]] = []
    for item in decoded:
        if not isinstance(item, dict):
            raise SupabaseDataError(
                "Supabase iterator received a non-object row.",
                error_type="invalid_supabase_response",
                details={"table_name": table_name},
            )
        rows.append(dict(item))
    return request_url, rows


def materialize_supabase_table_rows(request: SupabaseTableRowsRequest) -> SupabaseTableRowsResult:
    all_rows: list[dict[str, Any]] = []
    request_urls: list[str] = []
    last_cursor_value = str(request.last_cursor_value or "").strip()
    last_row_id = str(request.last_row_id or "").strip()

    while True:
        request_url, page_rows = _fetch_supabase_table_rows_page(
            request,
            last_cursor_value=last_cursor_value,
            last_row_id=last_row_id,
        )
        request_urls.append(request_url)
        if not page_rows:
            break
        all_rows.extend(page_rows)
        last_row = page_rows[-1]
        last_cursor_value = _normalize_filter_value(last_row.get(request.cursor_column), field_name=request.cursor_column)
        last_row_id = _normalize_filter_value(last_row.get(request.row_id_column), field_name=request.row_id_column)
        if len(page_rows) < int(request.page_size):
            break

    return SupabaseTableRowsResult(
        rows=all_rows,
        request_urls=request_urls,
        row_count=len(all_rows),
        last_cursor_value=last_cursor_value,
        last_row_id=last_row_id,
        schema=str(request.schema or "public").strip() or "public",
        table_name=str(request.table_name or "").strip(),
        select="*" if request.select is None else str(request.select or "").strip(),
        cursor_column=str(request.cursor_column or "").strip(),
        row_id_column=str(request.row_id_column or "").strip(),
        include_previously_processed_rows=bool(request.include_previously_processed_rows),
    )


class SupabaseTableRowsCursorStore:
    def __init__(self, db_path: Path | None = None) -> None:
        self._db_path = db_path or _cursor_store_db_path()
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
                CREATE TABLE IF NOT EXISTS supabase_table_rows_cursor (
                    scope_key TEXT PRIMARY KEY,
                    scope_json TEXT NOT NULL,
                    graph_id TEXT NOT NULL,
                    agent_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    connection_identity TEXT NOT NULL,
                    schema_name TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    filters_text TEXT NOT NULL,
                    cursor_column TEXT NOT NULL,
                    row_id_column TEXT NOT NULL,
                    last_cursor_value TEXT,
                    last_row_id TEXT,
                    last_run_id TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            connection.commit()

    def load_watermark(self, scope: SupabaseTableRowsCursorScope) -> SupabaseTableRowsWatermark | None:
        scope_key = scope.scope_key()
        with self._connection() as connection:
            cursor = connection.execute(
                """
                SELECT last_cursor_value, last_row_id, updated_at
                FROM supabase_table_rows_cursor
                WHERE scope_key = ?
                """,
                (scope_key,),
            )
            row = _row_to_dict(cursor, cursor.fetchone())
        if row is None:
            return None
        last_cursor_value = str(row.get("last_cursor_value", "") or "").strip()
        last_row_id = str(row.get("last_row_id", "") or "").strip()
        updated_at = str(row.get("updated_at", "") or "").strip()
        if not last_cursor_value or not last_row_id:
            return None
        return SupabaseTableRowsWatermark(
            last_cursor_value=last_cursor_value,
            last_row_id=last_row_id,
            updated_at=updated_at,
        )

    def mark_completed(
        self,
        *,
        scope: SupabaseTableRowsCursorScope,
        last_cursor_value: str,
        last_row_id: str,
        run_id: str,
    ) -> None:
        normalized_cursor_value = str(last_cursor_value or "").strip()
        normalized_row_id = str(last_row_id or "").strip()
        if not normalized_cursor_value or not normalized_row_id:
            return
        scope_key = scope.scope_key()
        now_iso = _utc_now_iso()
        with self._connection() as connection:
            connection.execute(
                """
                INSERT INTO supabase_table_rows_cursor (
                    scope_key,
                    scope_json,
                    graph_id,
                    agent_id,
                    node_id,
                    connection_identity,
                    schema_name,
                    table_name,
                    filters_text,
                    cursor_column,
                    row_id_column,
                    last_cursor_value,
                    last_row_id,
                    last_run_id,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(scope_key) DO UPDATE SET
                    scope_json = excluded.scope_json,
                    graph_id = excluded.graph_id,
                    agent_id = excluded.agent_id,
                    node_id = excluded.node_id,
                    connection_identity = excluded.connection_identity,
                    schema_name = excluded.schema_name,
                    table_name = excluded.table_name,
                    filters_text = excluded.filters_text,
                    cursor_column = excluded.cursor_column,
                    row_id_column = excluded.row_id_column,
                    last_cursor_value = excluded.last_cursor_value,
                    last_row_id = excluded.last_row_id,
                    last_run_id = excluded.last_run_id,
                    updated_at = excluded.updated_at
                """,
                (
                    scope_key,
                    json.dumps(scope.canonical_payload(), sort_keys=True),
                    scope.graph_id,
                    scope.agent_id,
                    scope.node_id,
                    scope.connection_identity,
                    scope.schema,
                    scope.table_name,
                    scope.filters_text,
                    scope.cursor_column,
                    scope.row_id_column,
                    normalized_cursor_value,
                    normalized_row_id,
                    str(run_id or ""),
                    now_iso,
                ),
            )
            connection.commit()
