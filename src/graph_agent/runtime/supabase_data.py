from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


SUPPORTED_SUPABASE_SOURCE_KINDS = {"table", "rpc"}
SUPPORTED_SUPABASE_OUTPUT_MODES = {"records", "markdown"}
SUPPORTED_SUPABASE_WRITE_MODES = {"insert", "upsert"}
SUPPORTED_SUPABASE_RETURNING_MODES = {"representation", "minimal"}
DEFAULT_SUPABASE_MANAGEMENT_API_BASE_URL = "https://api.supabase.com"

POSTGRES_TYPE_ALIASES: dict[str, tuple[str, ...]] = {
    "string": (
        "string",
        "text",
        "character varying",
        "varchar",
        "character",
        "char",
        "uuid",
        "date",
        "timestamp",
        "timestamp with time zone",
        "timestamp without time zone",
        "timestamptz",
        "time with time zone",
        "time without time zone",
        "timetz",
        "time",
    ),
    "integer": ("integer", "smallint", "bigint", "int2", "int4", "int8"),
    "number": ("number", "numeric", "decimal", "real", "double precision", "float4", "float8"),
    "object": ("object", "json", "jsonb"),
    "boolean": ("boolean", "bool"),
}


class SupabaseDataError(RuntimeError):
    def __init__(self, message: str, *, error_type: str = "supabase_data_error", details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.error_type = error_type
        self.details = dict(details or {})

    def to_error_payload(self) -> dict[str, Any]:
        return {
            "type": self.error_type,
            "message": str(self),
            **self.details,
        }


@dataclass(frozen=True)
class SupabaseDataRequest:
    supabase_url: str
    supabase_key: str
    schema: str
    source_kind: str
    source_name: str
    select: str = "*"
    filters_text: str = ""
    order_by: str = ""
    order_desc: bool = False
    limit: int = 25
    single_row: bool = False
    output_mode: str = "records"
    rpc_params: dict[str, Any] | None = None


@dataclass(frozen=True)
class SupabaseDataResult:
    payload: Any
    raw_payload: Any
    row_count: int | None
    request_url: str
    source_path: str
    source_kind: str
    source_name: str
    schema: str
    output_mode: str


@dataclass(frozen=True)
class SupabaseSqlQueryRequest:
    project_ref: str
    access_token: str
    query: str
    parameters: list[Any] | None = None
    read_only: bool = True
    output_mode: str = "records"
    management_api_base_url: str = DEFAULT_SUPABASE_MANAGEMENT_API_BASE_URL


@dataclass(frozen=True)
class SupabaseSqlQueryResult:
    payload: Any
    raw_payload: Any
    row_count: int | None
    request_url: str
    query: str
    parameters: list[Any]
    read_only: bool
    output_mode: str


@dataclass(frozen=True)
class SupabaseRowWriteRequest:
    supabase_url: str
    supabase_key: str
    schema: str
    table_name: str
    row: dict[str, Any]
    write_mode: str = "insert"
    on_conflict: str = ""
    ignore_duplicates: bool = False
    returning: str = "representation"


@dataclass(frozen=True)
class SupabaseRowWriteResult:
    payload: Any
    raw_payload: Any
    row_count: int | None
    request_url: str
    source_path: str
    schema: str
    table_name: str
    write_mode: str
    returning: str
    inserted_row: dict[str, Any]


@dataclass(frozen=True)
class SupabaseSchemaColumn:
    name: str
    data_type: str
    nullable: bool
    description: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "data_type": self.data_type,
            "nullable": self.nullable,
            "description": self.description,
        }


@dataclass(frozen=True)
class SupabaseSchemaSource:
    name: str
    source_kind: str
    columns: list[SupabaseSchemaColumn]
    description: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "source_kind": self.source_kind,
            "columns": [column.to_dict() for column in self.columns],
            "description": self.description,
        }


@dataclass(frozen=True)
class SupabaseExpectedColumn:
    name: str
    accepted_types: tuple[str, ...]
    required: bool = True


def _canonical_supabase_type_name(raw_type: str) -> str:
    normalized = str(raw_type or "unknown").strip().lower() or "unknown"
    candidates = [normalized]
    if normalized.endswith(")") and "(" in normalized:
        base_type, _, format_suffix = normalized.partition("(")
        stripped_base_type = base_type.strip()
        stripped_format_suffix = format_suffix[:-1].strip()
        candidates = [
            candidate
            for candidate in (stripped_format_suffix, stripped_base_type, normalized)
            if candidate
        ]
    for candidate in candidates:
        for canonical, aliases in POSTGRES_TYPE_ALIASES.items():
            if candidate == canonical or candidate in aliases:
                return canonical
    return normalized


@dataclass(frozen=True)
class SupabaseSchemaTypeMismatch:
    column_name: str
    expected_types: tuple[str, ...]
    actual_type: str
    required: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "column_name": self.column_name,
            "expected_types": list(self.expected_types),
            "actual_type": self.actual_type,
            "required": self.required,
        }


@dataclass(frozen=True)
class SupabaseSchemaValidationResult:
    schema: str
    table_name: str
    configured: bool
    table_found: bool
    valid: bool
    available_columns: list[str]
    missing_required_columns: list[str]
    missing_optional_columns: list[str]
    type_mismatches: list[SupabaseSchemaTypeMismatch]
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "table_name": self.table_name,
            "configured": self.configured,
            "table_found": self.table_found,
            "valid": self.valid,
            "available_columns": list(self.available_columns),
            "missing_required_columns": list(self.missing_required_columns),
            "missing_optional_columns": list(self.missing_optional_columns),
            "type_mismatches": [mismatch.to_dict() for mismatch in self.type_mismatches],
            "warnings": list(self.warnings),
        }


OUTBOUND_EMAIL_LOG_EXPECTED_COLUMNS: tuple[SupabaseExpectedColumn, ...] = (
    SupabaseExpectedColumn("provider", ("string",), required=False),
    SupabaseExpectedColumn("mailbox_account", ("string",), required=False),
    SupabaseExpectedColumn("recipient_email", ("string",)),
    SupabaseExpectedColumn("subject", ("string",), required=False),
    SupabaseExpectedColumn("body_text", ("string",), required=False),
    SupabaseExpectedColumn("message_type", ("string",), required=False),
    SupabaseExpectedColumn("outreach_step", ("integer", "number"), required=False),
    SupabaseExpectedColumn("sales_approach", ("string",), required=False),
    SupabaseExpectedColumn("provider_draft_id", ("string",), required=False),
    SupabaseExpectedColumn("provider_message_id", ("string",), required=False),
    SupabaseExpectedColumn("internet_message_id", ("string",), required=False),
    SupabaseExpectedColumn("conversation_id", ("string",), required=False),
    SupabaseExpectedColumn("drafted_at", ("string",), required=False),
    SupabaseExpectedColumn("metadata", ("object",), required=False),
    SupabaseExpectedColumn("raw_provider_payload", ("object",), required=False),
    SupabaseExpectedColumn("source_run_id", ("string",), required=False),
    SupabaseExpectedColumn("sales_approach_version", ("string",), required=False),
    SupabaseExpectedColumn("parent_outbound_email_id", ("string",), required=False),
    SupabaseExpectedColumn("root_outbound_email_id", ("string",), required=False),
    SupabaseExpectedColumn("observed_sent_at", ("string",), required=False),
    SupabaseExpectedColumn("created_at", ("string",), required=False),
)


def build_supabase_rest_auth_headers(supabase_key: str) -> dict[str, str]:
    normalized_key = str(supabase_key or "").strip()
    headers = {"apikey": normalized_key}
    if normalized_key and not normalized_key.startswith("sb_secret_"):
        headers["Authorization"] = f"Bearer {normalized_key}"
    return headers


def parse_supabase_filter_lines(raw_filters: str) -> list[tuple[str, str]]:
    parsed: list[tuple[str, str]] = []
    for raw_line in str(raw_filters or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        key, separator, value = line.partition("=")
        normalized_key = key.strip()
        normalized_value = value.strip()
        if not separator or not normalized_key or not normalized_value:
            raise SupabaseDataError(
                f"Invalid Supabase filter line '{line}'. Use one query parameter per line in the form key=value.",
                error_type="invalid_supabase_filters",
            )
        parsed.append((normalized_key, normalized_value))
    return parsed


def validate_supabase_source_schema(
    *,
    sources: list[SupabaseSchemaSource],
    schema: str,
    table_name: str,
    expected_columns: Sequence[SupabaseExpectedColumn],
) -> SupabaseSchemaValidationResult:
    normalized_schema = str(schema or "public").strip() or "public"
    normalized_table_name = str(table_name or "").strip()
    if not normalized_table_name:
        return SupabaseSchemaValidationResult(
            schema=normalized_schema,
            table_name="",
            configured=False,
            table_found=False,
            valid=False,
            available_columns=[],
            missing_required_columns=[],
            missing_optional_columns=[],
            type_mismatches=[],
            warnings=["Choose a Supabase table to validate the outbound email logger attachment."],
        )

    source = next((candidate for candidate in sources if candidate.name == normalized_table_name and candidate.source_kind == "table"), None)
    if source is None:
        return SupabaseSchemaValidationResult(
            schema=normalized_schema,
            table_name=normalized_table_name,
            configured=True,
            table_found=False,
            valid=False,
            available_columns=[],
            missing_required_columns=[column.name for column in expected_columns if column.required],
            missing_optional_columns=[column.name for column in expected_columns if not column.required],
            type_mismatches=[],
            warnings=[f"Table '{normalized_table_name}' was not found in the '{normalized_schema}' schema preview."],
        )

    available_by_name = {column.name: column for column in source.columns}
    missing_required_columns: list[str] = []
    missing_optional_columns: list[str] = []
    type_mismatches: list[SupabaseSchemaTypeMismatch] = []
    for expected in expected_columns:
        actual = available_by_name.get(expected.name)
        if actual is None:
            if expected.required:
                missing_required_columns.append(expected.name)
            else:
                missing_optional_columns.append(expected.name)
            continue
        normalized_type = _canonical_supabase_type_name(actual.data_type)
        normalized_expected_types = tuple(_canonical_supabase_type_name(expected_type) for expected_type in expected.accepted_types)
        if normalized_expected_types and normalized_type not in set(normalized_expected_types):
            type_mismatches.append(
                SupabaseSchemaTypeMismatch(
                    column_name=expected.name,
                    expected_types=normalized_expected_types,
                    actual_type=normalized_type,
                    required=expected.required,
                )
            )

    warnings: list[str] = []
    if missing_optional_columns:
        warnings.append(
            "Optional columns are missing and will be omitted from runtime writes: "
            + ", ".join(missing_optional_columns)
        )

    return SupabaseSchemaValidationResult(
        schema=normalized_schema,
        table_name=normalized_table_name,
        configured=True,
        table_found=True,
        valid=not missing_required_columns and not type_mismatches,
        available_columns=sorted(available_by_name),
        missing_required_columns=missing_required_columns,
        missing_optional_columns=missing_optional_columns,
        type_mismatches=type_mismatches,
        warnings=warnings,
    )


def validate_outbound_email_log_schema(
    *,
    sources: list[SupabaseSchemaSource],
    schema: str,
    table_name: str,
) -> SupabaseSchemaValidationResult:
    return validate_supabase_source_schema(
        sources=sources,
        schema=schema,
        table_name=table_name,
        expected_columns=OUTBOUND_EMAIL_LOG_EXPECTED_COLUMNS,
    )


def _render_markdown_table(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "No rows returned."
    headers: list[str] = []
    for row in rows:
        for key in row:
            if key not in headers:
                headers.append(str(key))
    if not headers:
        return "No rows returned."
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        values = ["" if row.get(header) is None else str(row.get(header)) for header in headers]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def render_supabase_payload(payload: Any, *, output_mode: str) -> Any:
    normalized_output_mode = str(output_mode or "records").strip().lower() or "records"
    if normalized_output_mode not in SUPPORTED_SUPABASE_OUTPUT_MODES:
        raise SupabaseDataError(
            f"Unsupported Supabase output mode '{normalized_output_mode}'.",
            error_type="invalid_supabase_output_mode",
        )
    if normalized_output_mode == "records":
        return payload
    if isinstance(payload, list) and all(isinstance(item, dict) for item in payload):
        return _render_markdown_table([dict(item) for item in payload])
    if isinstance(payload, dict):
        return json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True)
    if payload is None:
        return "No rows returned."
    if isinstance(payload, str):
        return payload
    return json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True)


def fetch_supabase_data(request: SupabaseDataRequest) -> SupabaseDataResult:
    supabase_url = str(request.supabase_url or "").strip().rstrip("/")
    supabase_key = str(request.supabase_key or "").strip()
    schema = str(request.schema or "public").strip() or "public"
    source_kind = str(request.source_kind or "table").strip().lower() or "table"
    source_name = str(request.source_name or "").strip()
    select = str(request.select or "*").strip() or "*"
    output_mode = str(request.output_mode or "records").strip().lower() or "records"

    if not supabase_url:
        raise SupabaseDataError("Supabase URL is required.", error_type="missing_supabase_url")
    if not supabase_key:
        raise SupabaseDataError("Supabase key is required.", error_type="missing_supabase_key")
    if source_kind not in SUPPORTED_SUPABASE_SOURCE_KINDS:
        raise SupabaseDataError(
            f"Unsupported Supabase source kind '{source_kind}'.",
            error_type="invalid_supabase_source_kind",
        )
    if not source_name:
        raise SupabaseDataError("Supabase source_name is required.", error_type="missing_supabase_source_name")
    if output_mode not in SUPPORTED_SUPABASE_OUTPUT_MODES:
        raise SupabaseDataError(
            f"Unsupported Supabase output mode '{output_mode}'.",
            error_type="invalid_supabase_output_mode",
        )
    if int(request.limit) < 1:
        raise SupabaseDataError("Supabase limit must be at least 1.", error_type="invalid_supabase_limit")

    query_pairs: list[tuple[str, str]] = []
    if source_kind == "table":
        query_pairs.append(("select", select))
        query_pairs.extend(parse_supabase_filter_lines(request.filters_text))
        if request.order_by:
            direction = "desc" if request.order_desc else "asc"
            query_pairs.append(("order", f"{str(request.order_by).strip()}.{direction}"))
        query_pairs.append(("limit", str(int(request.limit))))
        source_path = f"/rest/v1/{quote(source_name, safe='')}"
        method = "GET"
        body = None
    else:
        rpc_params = request.rpc_params or {}
        if not isinstance(rpc_params, dict):
            raise SupabaseDataError(
                "Supabase rpc_params must be a JSON object.",
                error_type="invalid_supabase_rpc_params",
            )
        if select:
            query_pairs.append(("select", select))
        source_path = f"/rest/v1/rpc/{quote(source_name, safe='')}"
        method = "POST"
        body = json.dumps(rpc_params).encode("utf-8")

    request_url = f"{supabase_url}{source_path}"
    if query_pairs:
        request_url = f"{request_url}?{urlencode(query_pairs)}"

    headers = {
        **build_supabase_rest_auth_headers(supabase_key),
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Accept-Profile": schema,
        "Content-Profile": schema,
    }
    http_request = Request(request_url, data=body, method=method, headers=headers)
    try:
        with urlopen(http_request) as response:
            raw_body = response.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise SupabaseDataError(
            f"Supabase request failed: {exc.code} {detail}".strip(),
            error_type="supabase_request_failed",
            details={"status_code": exc.code, "source_name": source_name, "source_kind": source_kind},
        ) from exc
    except URLError as exc:
        raise SupabaseDataError(
            f"Supabase request failed: {exc.reason}",
            error_type="supabase_request_failed",
            details={"source_name": source_name, "source_kind": source_kind},
        ) from exc

    decoded: Any
    if not raw_body.strip():
        decoded = [] if source_kind == "table" else {}
    else:
        try:
            decoded = json.loads(raw_body)
        except json.JSONDecodeError as exc:
            raise SupabaseDataError(
                "Supabase response was not valid JSON.",
                error_type="invalid_supabase_response",
                details={"source_name": source_name, "source_kind": source_kind},
            ) from exc

    row_count: int | None = None
    raw_payload = decoded
    if request.single_row and isinstance(decoded, list):
        row_count = len(decoded)
        decoded = decoded[0] if decoded else None
    elif isinstance(decoded, list):
        row_count = len(decoded)
    elif isinstance(decoded, dict):
        row_count = 1 if decoded else 0

    return SupabaseDataResult(
        payload=render_supabase_payload(decoded, output_mode=output_mode),
        raw_payload=raw_payload,
        row_count=row_count,
        request_url=request_url,
        source_path=source_path,
        source_kind=source_kind,
        source_name=source_name,
        schema=schema,
        output_mode=output_mode,
    )


def write_supabase_row(request: SupabaseRowWriteRequest) -> SupabaseRowWriteResult:
    supabase_url = str(request.supabase_url or "").strip().rstrip("/")
    supabase_key = str(request.supabase_key or "").strip()
    schema = str(request.schema or "public").strip() or "public"
    table_name = str(request.table_name or "").strip()
    write_mode = str(request.write_mode or "insert").strip().lower() or "insert"
    returning = str(request.returning or "representation").strip().lower() or "representation"
    on_conflict = str(request.on_conflict or "").strip()
    row = dict(request.row or {})

    if not supabase_url:
        raise SupabaseDataError("Supabase URL is required.", error_type="missing_supabase_url")
    if not supabase_key:
        raise SupabaseDataError("Supabase key is required.", error_type="missing_supabase_key")
    if not table_name:
        raise SupabaseDataError("Supabase table_name is required.", error_type="missing_supabase_table_name")
    if write_mode not in SUPPORTED_SUPABASE_WRITE_MODES:
        raise SupabaseDataError(
            f"Unsupported Supabase write mode '{write_mode}'.",
            error_type="invalid_supabase_write_mode",
        )
    if returning not in SUPPORTED_SUPABASE_RETURNING_MODES:
        raise SupabaseDataError(
            f"Unsupported Supabase returning mode '{returning}'.",
            error_type="invalid_supabase_returning_mode",
        )
    if not row:
        raise SupabaseDataError(
            "Supabase row payload must include at least one column value.",
            error_type="empty_supabase_row_payload",
        )

    query_pairs: list[tuple[str, str]] = []
    if write_mode == "upsert" and on_conflict:
        query_pairs.append(("on_conflict", on_conflict))

    source_path = f"/rest/v1/{quote(table_name, safe='')}"
    request_url = f"{supabase_url}{source_path}"
    if query_pairs:
        request_url = f"{request_url}?{urlencode(query_pairs)}"

    prefer_values = [f"return={returning}"]
    if write_mode == "upsert":
        prefer_values.append("resolution=ignore-duplicates" if request.ignore_duplicates else "resolution=merge-duplicates")

    headers = {
        **build_supabase_rest_auth_headers(supabase_key),
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Accept-Profile": schema,
        "Content-Profile": schema,
        "Prefer": ",".join(prefer_values),
    }
    body = json.dumps(row).encode("utf-8")
    http_request = Request(request_url, data=body, method="POST", headers=headers)
    try:
        with urlopen(http_request) as response:
            raw_body = response.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise SupabaseDataError(
            f"Supabase write request failed: {exc.code} {detail}".strip(),
            error_type="supabase_write_request_failed",
            details={"status_code": exc.code, "table_name": table_name, "write_mode": write_mode},
        ) from exc
    except URLError as exc:
        raise SupabaseDataError(
            f"Supabase write request failed: {exc.reason}",
            error_type="supabase_write_request_failed",
            details={"table_name": table_name, "write_mode": write_mode},
        ) from exc

    if not raw_body.strip():
        decoded: Any = None
    else:
        try:
            decoded = json.loads(raw_body)
        except json.JSONDecodeError as exc:
            raise SupabaseDataError(
                "Supabase write response was not valid JSON.",
                error_type="invalid_supabase_write_response",
                details={"table_name": table_name, "write_mode": write_mode},
            ) from exc

    row_count: int | None = None
    if isinstance(decoded, list):
        row_count = len(decoded)
    elif isinstance(decoded, dict):
        row_count = 1 if decoded else 0
    elif decoded is None:
        row_count = 1

    payload: Any
    if returning == "representation":
        payload = decoded
    else:
        payload = {
            "table_name": table_name,
            "schema": schema,
            "write_mode": write_mode,
            "row_count": row_count,
            "returning": returning,
        }

    return SupabaseRowWriteResult(
        payload=payload,
        raw_payload=decoded,
        row_count=row_count,
        request_url=request_url,
        source_path=source_path,
        schema=schema,
        table_name=table_name,
        write_mode=write_mode,
        returning=returning,
        inserted_row=row,
    )


def execute_supabase_sql_query(request: SupabaseSqlQueryRequest) -> SupabaseSqlQueryResult:
    project_ref = str(request.project_ref or "").strip()
    access_token = str(request.access_token or "").strip()
    query = str(request.query or "").strip()
    output_mode = str(request.output_mode or "records").strip().lower() or "records"
    read_only = bool(request.read_only)
    parameters = list(request.parameters or [])
    management_api_base_url = (
        str(request.management_api_base_url or DEFAULT_SUPABASE_MANAGEMENT_API_BASE_URL).strip().rstrip("/")
        or DEFAULT_SUPABASE_MANAGEMENT_API_BASE_URL
    )

    if not project_ref:
        raise SupabaseDataError("Supabase project ref is required.", error_type="missing_supabase_project_ref")
    if not access_token:
        raise SupabaseDataError("Supabase access token is required.", error_type="missing_supabase_access_token")
    if not query:
        raise SupabaseDataError("Supabase SQL query is required.", error_type="missing_supabase_sql_query")
    if output_mode not in SUPPORTED_SUPABASE_OUTPUT_MODES:
        raise SupabaseDataError(
            f"Unsupported Supabase output mode '{output_mode}'.",
            error_type="invalid_supabase_output_mode",
        )

    path = (
        f"/v1/projects/{quote(project_ref, safe='')}/database/query/read-only"
        if read_only
        else f"/v1/projects/{quote(project_ref, safe='')}/database/query"
    )
    request_url = f"{management_api_base_url}{path}"
    request_body = {"query": query}
    if parameters:
        request_body["parameters"] = parameters
    if not read_only:
        request_body["read_only"] = False
    http_request = Request(
        request_url,
        data=json.dumps(request_body).encode("utf-8"),
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}",
            "User-Agent": "graph-agent-supabase-sql/0.1",
        },
    )
    try:
        with urlopen(http_request) as response:
            raw_body = response.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise SupabaseDataError(
            f"Supabase SQL query failed: {exc.code} {detail}".strip(),
            error_type="supabase_sql_query_failed",
            details={"status_code": exc.code, "project_ref": project_ref, "read_only": read_only},
        ) from exc
    except URLError as exc:
        raise SupabaseDataError(
            f"Supabase SQL query failed: {exc.reason}",
            error_type="supabase_sql_query_failed",
            details={"project_ref": project_ref, "read_only": read_only},
        ) from exc

    if not raw_body.strip():
        decoded: Any = {}
    else:
        try:
            decoded = json.loads(raw_body)
        except json.JSONDecodeError as exc:
            raise SupabaseDataError(
                "Supabase SQL query response was not valid JSON.",
                error_type="invalid_supabase_sql_query_response",
                details={"project_ref": project_ref, "read_only": read_only},
            ) from exc

    payload = decoded
    if isinstance(decoded, dict) and "result" in decoded:
        payload = decoded.get("result")
        query_error = decoded.get("error")
        if query_error not in (None, "", []):
            raise SupabaseDataError(
                f"Supabase SQL query returned an error: {query_error}",
                error_type="supabase_sql_query_failed",
                details={"project_ref": project_ref, "read_only": read_only},
            )

    row_count: int | None = None
    if isinstance(payload, list):
        row_count = len(payload)
    elif isinstance(payload, dict):
        row_count = 1 if payload else 0

    return SupabaseSqlQueryResult(
        payload=render_supabase_payload(payload, output_mode=output_mode),
        raw_payload=decoded,
        row_count=row_count,
        request_url=request_url,
        query=query,
        parameters=parameters,
        read_only=read_only,
        output_mode=output_mode,
    )


def _extract_openapi_schema_ref(schema: Any) -> str:
    if not isinstance(schema, dict):
        return ""
    ref = str(schema.get("$ref", "") or "").strip()
    if ref:
        return ref.rsplit("/", 1)[-1]
    items = schema.get("items")
    if isinstance(items, dict):
        item_ref = str(items.get("$ref", "") or "").strip()
        if item_ref:
            return item_ref.rsplit("/", 1)[-1]
    return ""


def _normalize_openapi_type(property_schema: Any) -> tuple[str, bool]:
    if not isinstance(property_schema, dict):
        return "unknown", True
    if property_schema.get("nullable") is True:
        return str(property_schema.get("type", "unknown") or "unknown"), True
    schema_type = property_schema.get("type")
    if isinstance(schema_type, list):
        non_null_types = [str(item) for item in schema_type if str(item) != "null"]
        nullable = len(non_null_types) != len(schema_type)
        normalized_type = non_null_types[0] if non_null_types else "unknown"
        return normalized_type, nullable
    if isinstance(property_schema.get("oneOf"), list):
        options = [option for option in property_schema.get("oneOf", []) if isinstance(option, dict)]
        non_null = [option for option in options if str(option.get("type", "")) != "null"]
        nullable = len(non_null) != len(options)
        normalized_type = str(non_null[0].get("type", "unknown") if non_null else "unknown")
        return normalized_type, nullable
    if isinstance(property_schema.get("anyOf"), list):
        options = [option for option in property_schema.get("anyOf", []) if isinstance(option, dict)]
        non_null = [option for option in options if str(option.get("type", "")) != "null"]
        nullable = len(non_null) != len(options)
        normalized_type = str(non_null[0].get("type", "unknown") if non_null else "unknown")
        return normalized_type, nullable
    return str(schema_type or "unknown"), False


def fetch_supabase_schema_catalog(*, supabase_url: str, supabase_key: str, schema: str = "public") -> list[SupabaseSchemaSource]:
    normalized_url = str(supabase_url or "").strip().rstrip("/")
    normalized_key = str(supabase_key or "").strip()
    normalized_schema = str(schema or "public").strip() or "public"
    if not normalized_url:
        raise SupabaseDataError("Supabase URL is required.", error_type="missing_supabase_url")
    if not normalized_key:
        raise SupabaseDataError("Supabase key is required.", error_type="missing_supabase_key")

    request = Request(
        f"{normalized_url}/rest/v1/",
        method="GET",
        headers={
            **build_supabase_rest_auth_headers(normalized_key),
            "Accept": "application/openapi+json",
            "Accept-Profile": normalized_schema,
            "Content-Profile": normalized_schema,
        },
    )
    try:
        with urlopen(request) as response:
            raw_body = response.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise SupabaseDataError(
            f"Supabase schema request failed: {exc.code} {detail}".strip(),
            error_type="supabase_schema_request_failed",
            details={"status_code": exc.code},
        ) from exc
    except URLError as exc:
        raise SupabaseDataError(
            f"Supabase schema request failed: {exc.reason}",
            error_type="supabase_schema_request_failed",
        ) from exc

    try:
        spec = json.loads(raw_body)
    except json.JSONDecodeError as exc:
        raise SupabaseDataError(
            "Supabase schema response was not valid JSON.",
            error_type="invalid_supabase_schema_response",
        ) from exc

    if not isinstance(spec, dict):
        raise SupabaseDataError(
            "Supabase schema response was not an object.",
            error_type="invalid_supabase_schema_response",
        )

    component_schemas = spec.get("components", {}).get("schemas", {}) if isinstance(spec.get("components"), dict) else {}
    if not isinstance(component_schemas, dict):
        component_schemas = {}
    if not component_schemas and isinstance(spec.get("definitions"), dict):
        component_schemas = spec.get("definitions", {})

    paths = spec.get("paths", {})
    if not isinstance(paths, dict):
        return []

    sources: list[SupabaseSchemaSource] = []
    for raw_path, raw_path_item in sorted(paths.items()):
        if not isinstance(raw_path, str) or not raw_path.startswith("/"):
            continue
        source_name = raw_path.strip("/")
        if not source_name or source_name.startswith("rpc/") or "{" in source_name or "/" in source_name:
            continue
        if not isinstance(raw_path_item, dict):
            continue
        get_operation = raw_path_item.get("get")
        if not isinstance(get_operation, dict):
            continue
        schema_name = ""
        responses = get_operation.get("responses", {})
        if isinstance(responses, dict):
            for response_payload in responses.values():
                if not isinstance(response_payload, dict):
                    continue
                content = response_payload.get("content", {})
                if not isinstance(content, dict):
                    continue
                for media_payload in content.values():
                    if not isinstance(media_payload, dict):
                        continue
                    schema_name = _extract_openapi_schema_ref(media_payload.get("schema"))
                    if schema_name:
                        break
                if schema_name:
                    break
        component = component_schemas.get(schema_name) if schema_name else component_schemas.get(source_name)
        properties = component.get("properties", {}) if isinstance(component, dict) else {}
        if not isinstance(properties, dict):
            properties = {}
        columns: list[SupabaseSchemaColumn] = []
        for column_name, property_schema in properties.items():
            normalized_type, nullable = _normalize_openapi_type(property_schema)
            if isinstance(property_schema, dict) and property_schema.get("format"):
                normalized_type = f"{normalized_type} ({property_schema['format']})"
            columns.append(
                SupabaseSchemaColumn(
                    name=str(column_name),
                    data_type=normalized_type,
                    nullable=nullable,
                    description=str(property_schema.get("description", "") or "") if isinstance(property_schema, dict) else "",
                )
            )
        sources.append(
            SupabaseSchemaSource(
                name=source_name,
                source_kind="table",
                columns=columns,
                description=str(get_operation.get("description", "") or get_operation.get("summary", "") or ""),
            )
        )
    return sources


def verify_supabase_mcp_auth(*, project_ref: str, access_token: str, base_url: str | None = None) -> dict[str, Any]:
    normalized_project_ref = str(project_ref or "").strip()
    normalized_access_token = str(access_token or "").strip()
    if not normalized_project_ref:
        raise SupabaseDataError("Supabase project ref is required.", error_type="missing_supabase_project_ref")
    if not normalized_access_token:
        raise SupabaseDataError("Supabase access token is required.", error_type="missing_supabase_access_token")

    endpoint = (
        str(base_url or "").strip()
        or f"https://mcp.supabase.com/mcp?project_ref={quote(normalized_project_ref, safe='')}&read_only=true&features=database,docs"
    )
    request = Request(
        endpoint,
        method="POST",
        data=json.dumps(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "graph-agent", "version": "0.1.0"},
                },
            },
            separators=(",", ":"),
        ).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "Authorization": f"Bearer {normalized_access_token}",
            "User-Agent": "graph-agent-supabase-auth/0.1",
        },
    )
    try:
        with urlopen(request) as response:
            raw_body = response.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise SupabaseDataError(
            f"Supabase MCP authentication failed: {exc.code} {detail}".strip(),
            error_type="supabase_mcp_auth_failed",
            details={"status_code": exc.code},
        ) from exc
    except URLError as exc:
        raise SupabaseDataError(
            f"Supabase MCP authentication failed: {exc.reason}",
            error_type="supabase_mcp_auth_failed",
        ) from exc

    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError as exc:
        raise SupabaseDataError(
            "Supabase MCP authentication returned invalid JSON.",
            error_type="invalid_supabase_mcp_response",
        ) from exc
    if not isinstance(payload, dict):
        raise SupabaseDataError(
            "Supabase MCP authentication returned an invalid payload.",
            error_type="invalid_supabase_mcp_response",
        )
    error = payload.get("error")
    if isinstance(error, dict) and error:
        raise SupabaseDataError(
            str(error.get("message", "Supabase MCP authentication failed.")),
            error_type="supabase_mcp_auth_failed",
            details={"error": error},
        )
    result = payload.get("result", {})
    if not isinstance(result, dict):
        raise SupabaseDataError(
            "Supabase MCP authentication returned an invalid result.",
            error_type="invalid_supabase_mcp_response",
        )
    server_info = result.get("serverInfo", {})
    return {
        "server_name": str(server_info.get("name", "") or ""),
        "server_version": str(server_info.get("version", "") or ""),
    }
