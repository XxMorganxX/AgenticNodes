from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping as MappingABC
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import os
from pathlib import Path, PurePosixPath
import re
import socket
import time
from typing import Any, Mapping, Sequence
from uuid import uuid4

from graph_agent.providers.discord import DiscordMessageSender
from graph_agent.providers.outlook import OutlookDraftClient, parse_outlook_recipient_addresses
from graph_agent.providers.base import (
    ModelMessage,
    ModelProvider,
    ModelRequest,
    ModelToolDefinition,
    api_decision_response_schema,
    normalize_api_decision_output,
    validate_api_decision_output,
)
from graph_agent.runtime.event_contract import (
    RUNTIME_EVENT_SCHEMA_VERSION,
    normalize_runtime_event_dict,
)
from graph_agent.runtime.agent_filesystem import (
    normalize_workspace_text_write_behavior,
    resolve_agent_workspace,
    resolve_agent_workspace_path,
    write_agent_workspace_text_file,
)
from graph_agent.runtime.apollo_email_lookup import (
    ApolloEmailLookupRequest,
    ApolloLookupError,
    build_apollo_email_cache_entry,
    build_apollo_email_lookup_cache_info,
    build_apollo_person_summary,
    determine_apollo_lookup_status,
    extract_apollo_email,
    extract_apollo_lookup_fields,
    fetch_apollo_person_match_live,
    is_cacheable_apollo_response,
    read_cached_apollo_email_lookup,
    validate_apollo_lookup_request,
    workspace_cache_relative_path as apollo_workspace_cache_relative_path,
    write_apollo_email_lookup_workspace_copy,
    write_cached_apollo_email_lookup,
)
from graph_agent.runtime.linkedin_profile_fetch import (
    LinkedInFetchError,
    build_linkedin_profile_cache_info,
    error_from_linkedin_profile_payload,
    extract_linkedin_profile_url,
    fetch_linkedin_profile_live,
    is_cacheable_linkedin_profile,
    read_cached_linkedin_profile,
    sanitize_linkedin_profile_payload,
    write_cached_linkedin_profile,
    write_linkedin_profile_workspace_copy,
    workspace_cache_relative_path,
)
from graph_agent.runtime.python_script_runner import (
    DEFAULT_SCRIPT_TIMEOUT_SECONDS,
    PYTHON_SCRIPT_RUNNER_MODE,
    PYTHON_SCRIPT_RUNNER_PROVIDER_ID,
    run_script as run_python_script,
)
from graph_agent.runtime.runtime_normalizer import (
    RuntimeFieldExtractorConfig,
    extract_field_candidates,
    parse_field_name_list,
)
from graph_agent.runtime.structured_payload_builder import (
    StructuredPayloadBuilderConfig,
    build_structured_payload,
    parse_structured_payload_template,
)
from graph_agent.runtime.supabase_data import (
    SupabaseDataError,
    SupabaseDataRequest,
    SupabaseSqlQueryRequest,
    execute_supabase_sql_query,
    fetch_supabase_schema_catalog,
    fetch_supabase_data,
    SupabaseRowWriteRequest,
    SupabaseRowWriteResult,
    validate_outbound_email_log_schema,
    write_supabase_row,
)
from graph_agent.runtime.supabase_table_rows import (
    filter_supabase_table_row_output,
    SupabaseTableRowsCursorScope,
    SupabaseTableRowsCursorStore,
    SupabaseTableRowsRequest,
    SupabaseTableRowsWatermark,
    materialize_supabase_table_rows,
)
from graph_agent.runtime.node_providers import (
    NodeCategory,
    NodeProviderRegistry,
    get_category_contract,
    is_valid_category_connection,
)
from graph_agent.runtime.microsoft_auth import MicrosoftAuthService, MicrosoftAuthStatus
from graph_agent.runtime.outlook_dedupe import (
    OutlookDraftDeduplicationScope,
    OutlookDraftDedupeStore,
)
from graph_agent.runtime.run_documents import normalize_run_documents
from graph_agent.runtime.spreadsheets import (
    SPREADSHEET_FIRST_DATA_ROW_INDEX,
    SPREADSHEET_HEADER_ROW_INDEX,
    SpreadsheetMatrixParseResult,
    SpreadsheetParseError,
    parse_spreadsheet_matrix,
    parse_spreadsheet,
    resolve_spreadsheet_path_from_run_documents,
)
from graph_agent.tools.base import ToolContext, ToolRegistry
from graph_agent.tools.mcp import McpServerManager


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _coerce_start_row_index(raw: Any) -> int:
    if raw is None:
        return SPREADSHEET_FIRST_DATA_ROW_INDEX
    if isinstance(raw, str):
        raw = raw.strip()
        if not raw:
            raise SpreadsheetParseError("Starting row index is required.")
    try:
        value = int(raw)
    except (TypeError, ValueError):
        raise SpreadsheetParseError("Starting row index must be a whole number.") from None
    return value


API_TOOL_CALL_HANDLE_ID = "api-tool-call"
API_MESSAGE_HANDLE_ID = "api-message"
NO_TOOL_CALL_MESSAGE = "No Tool Call Made"
MCP_TERMINAL_OUTPUT_HANDLE_ID = "mcp-terminal-output"
RUN_STATE_EVENT_HISTORY_LIMIT = 500
RUN_STATE_TRANSITION_HISTORY_LIMIT = 500
RUN_EVENT_BACKLOG_LIMIT = 500
PROMPT_BLOCK_PROVIDER_ID = "core.prompt_block"
PROMPT_BLOCK_MODE = "prompt_block"
PROMPT_BLOCK_ROLES = {"system", "user", "assistant"}
DISCORD_END_PROVIDER_ID = "end.discord_message"
DEFAULT_DISCORD_BOT_TOKEN_ENV_VAR = "{DISCORD_BOT_TOKEN}"
OUTLOOK_DRAFT_PROVIDER_ID = "end.outlook_draft"
END_AGENT_RUN_PROVIDER_ID = "end.agent_run"
SPREADSHEET_ROW_PROVIDER_ID = "core.spreadsheet_rows"
SPREADSHEET_MATRIX_DECISION_PROVIDER_ID = "core.spreadsheet_matrix_decision"
JSON_CODE_FENCE_PATTERN = re.compile(r"^```(?:[A-Za-z0-9_-]+)?[ \t]*\n(?P<body>.*)\n```$", re.DOTALL)
GRAPH_ENV_REFERENCE_EXACT_PATTERN = re.compile(r"^\{([A-Za-z_][A-Za-z0-9_]*)\}$")
ENV_VAR_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
LOGIC_CONDITIONS_PROVIDER_ID = "core.logic_conditions"
PARALLEL_SPLITTER_PROVIDER_ID = "core.parallel_splitter"
WRITE_TEXT_FILE_PROVIDER_ID = "core.write_text_file"
APOLLO_EMAIL_LOOKUP_PROVIDER_ID = "core.apollo_email_lookup"
APOLLO_EMAIL_LOOKUP_MODE = "apollo_email_lookup"
DEFAULT_APOLLO_API_KEY_ENV_VAR = "APOLLO_API_KEY"
LINKEDIN_PROFILE_FETCH_PROVIDER_ID = "core.linkedin_profile_fetch"
LINKEDIN_PROFILE_FETCH_MODE = "linkedin_profile_fetch"
STRUCTURED_PAYLOAD_BUILDER_PROVIDER_ID = "core.structured_payload_builder"
STRUCTURED_PAYLOAD_BUILDER_MODE = "structured_payload_builder"
RUNTIME_NORMALIZER_PROVIDER_ID = "core.runtime_normalizer"
RUNTIME_NORMALIZER_MODE = "runtime_normalizer"
SUPABASE_DATA_PROVIDER_ID = "core.supabase_data"
SUPABASE_DATA_MODE = "supabase_data"
SUPABASE_SQL_PROVIDER_ID = "core.supabase_sql"
SUPABASE_SQL_MODE = "supabase_sql"
SUPABASE_TABLE_ROWS_PROVIDER_ID = "core.supabase_table_rows"
SUPABASE_TABLE_ROWS_MODE = "supabase_table_rows"
SUPABASE_ROW_WRITE_PROVIDER_ID = "core.supabase_row_write"
SUPABASE_ROW_WRITE_MODE = "supabase_row_write"
OUTBOUND_EMAIL_LOGGER_PROVIDER_ID = "core.outbound_email_logger"
OUTBOUND_EMAIL_LOGGER_MODE = "outbound_email_logger"
MISSING_RECIPIENT_SENTINEL_EMAIL = "missing-recipient@graph-agent.invalid"
SPREADSHEET_MATRIX_DECISION_MODE = "spreadsheet_matrix_decision"
CONTROL_FLOW_LOOP_BODY_HANDLE_ID = "control-flow-loop-body"
CONTROL_FLOW_IF_HANDLE_ID = "control-flow-if"
CONTROL_FLOW_ELSE_HANDLE_ID = "control-flow-else"
WORKSPACE_PATH_SUFFIX_PATTERN = re.compile(r"[^A-Za-z0-9._-]+")
TEMPLATE_PLACEHOLDER_PATTERN = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")


def _json_safe(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, indent=2, sort_keys=True)


def _normalize_prompt_block_role(value: Any) -> str:
    role = str(value or "user").strip().lower()
    return role if role in PROMPT_BLOCK_ROLES else "user"


def _coerce_logic_order_operand(value: Any) -> Any:
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            return value
        if re.fullmatch(r"[+-]?\d+", candidate):
            try:
                return int(candidate)
            except ValueError:
                return value
        if re.fullmatch(r"[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?", candidate):
            try:
                return float(candidate)
            except ValueError:
                return value
    return value


def _coerce_bool(value: Any, *, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "on"}:
            return True
        if normalized in {"false", "0", "no", "off"}:
            return False
    if value is None:
        return default
    return bool(value)


def _coerce_int(value: Any, *, default: int, minimum: int | None = None) -> int:
    if isinstance(value, bool):
        return default
    try:
        resolved = int(str(value).strip()) if isinstance(value, str) else int(value)
    except (TypeError, ValueError):
        return default
    if minimum is not None:
        return max(resolved, minimum)
    return resolved


def _coerce_float(value: Any, *, default: float, minimum: float | None = None) -> float:
    if isinstance(value, bool):
        return default
    try:
        resolved = float(str(value).strip()) if isinstance(value, str) else float(value)
    except (TypeError, ValueError):
        return default
    if minimum is not None:
        return max(resolved, minimum)
    return resolved


def _is_timeout_like_exception(exc: BaseException) -> bool:
    if isinstance(exc, (TimeoutError, socket.timeout)):
        return True
    message = str(exc).lower()
    return "timed out" in message or "timeout" in message


def _coerce_structured_payload_field_aliases(value: Any) -> tuple[tuple[str, tuple[str, ...]], ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return ()
        try:
            value = json.loads(text)
        except json.JSONDecodeError:
            return ()
    if not isinstance(value, Mapping):
        return ()
    rows: list[tuple[str, tuple[str, ...]]] = []
    for raw_key, raw_aliases in value.items():
        key = str(raw_key or "").strip()
        if not key:
            continue
        if isinstance(raw_aliases, str):
            iterable: Sequence[Any] = [raw_aliases]
        elif isinstance(raw_aliases, Sequence):
            iterable = raw_aliases
        else:
            continue
        cleaned: list[str] = []
        seen: set[str] = set()
        for entry in iterable:
            text = str(entry or "").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            cleaned.append(text)
        if cleaned:
            rows.append((key, tuple(cleaned)))
    return tuple(rows)


_VALID_SEARCH_SECTIONS = ("payload", "metadata", "artifacts")


def _coerce_structured_payload_search_section(
    value: Any, *, default: str = "payload"
) -> str:
    text = str(value or "").strip().lower()
    if text in _VALID_SEARCH_SECTIONS:
        return text
    return default


def _coerce_structured_payload_field_search_scopes(
    value: Any,
) -> tuple[tuple[str, str], ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return ()
        try:
            value = json.loads(text)
        except json.JSONDecodeError:
            return ()
    if not isinstance(value, Mapping):
        return ()
    rows: list[tuple[str, str]] = []
    for raw_key, raw_scope in value.items():
        key = str(raw_key or "").strip()
        if not key:
            continue
        if isinstance(raw_scope, str):
            section = _coerce_structured_payload_search_section(raw_scope)
            rows.append((key, section))
            continue
        # Tolerate the legacy {"metadata": bool, "artifacts": bool} shape briefly written
        # to disk during the previous iteration of this feature.
        if isinstance(raw_scope, Mapping):
            sm = _coerce_bool(raw_scope.get("metadata"), default=True)
            sa = _coerce_bool(raw_scope.get("artifacts"), default=True)
            if sm and not sa:
                rows.append((key, "metadata"))
            elif sa and not sm:
                rows.append((key, "artifacts"))
            else:
                rows.append((key, "payload"))
            continue
    return tuple(rows)


def _sanitize_workspace_path_suffix(value: str, *, fallback: str = "iteration") -> str:
    normalized = WORKSPACE_PATH_SUFFIX_PATTERN.sub("-", str(value or "").strip()).strip("-.")
    return normalized or fallback


def _resolve_write_text_file_relative_path(relative_path: str, *, context: NodeContext) -> str:
    resolved_path = str(relative_path or "response.txt").strip() or "response.txt"
    if not context.is_loop_execution():
        return resolved_path

    iteration_context = context.current_iteration_context()
    iteration_id = str(iteration_context.get("iteration_id", "") or "").strip()
    if iteration_id:
        suffix = _sanitize_workspace_path_suffix(iteration_id)
    else:
        row_index = iteration_context.get("iterator_row_index")
        if not isinstance(row_index, int) or row_index <= 0:
            return resolved_path
        suffix = f"row-{row_index}"

    normalized_path = PurePosixPath(resolved_path)
    filename = normalized_path.name or "response.txt"
    extension = normalized_path.suffix
    stem = filename[: -len(extension)] if extension else filename
    suffixed_name = f"{stem}-{suffix}{extension}"
    if normalized_path.parent == PurePosixPath("."):
        return suffixed_name
    return (normalized_path.parent / suffixed_name).as_posix()


def _is_prompt_block_payload(value: Any) -> bool:
    return bool(isinstance(value, Mapping) and str(value.get("kind", "")).strip() == "prompt_block")


def _prompt_block_text(value: Mapping[str, Any]) -> str:
    return str(value.get("content", "") or "")


def _render_prompt_block_text(value: Mapping[str, Any]) -> str:
    role = _normalize_prompt_block_role(value.get("role"))
    label = role.capitalize()
    name = str(value.get("name", "") or "").strip()
    header = f"{label} ({name})" if name else label
    content = _prompt_block_text(value).strip()
    return f"{header}: {content}" if content else f"{header}:"


def _render_chatgpt_style_messages(prompt_blocks: Sequence[Mapping[str, Any]]) -> list[dict[str, str]]:
    rendered_messages: list[dict[str, str]] = []
    for payload in prompt_blocks:
        content = _prompt_block_text(payload).strip()
        if not content:
            continue
        message_payload: dict[str, str] = {
            "role": _normalize_prompt_block_role(payload.get("role")),
            "content": content,
        }
        name = str(payload.get("name", "") or "").strip()
        if name:
            message_payload["name"] = name
        rendered_messages.append(message_payload)
    return rendered_messages


def _is_spreadsheet_row_payload(value: Any) -> bool:
    return bool(
        isinstance(value, Mapping)
        and isinstance(value.get("row_data"), Mapping)
        and ("row_index" in value or "row_number" in value)
    )


def _render_spreadsheet_row_text(value: Mapping[str, Any]) -> str:
    row_index = value.get("row_index")
    header = "Spreadsheet record"
    if isinstance(row_index, int):
        header += f" {row_index}"
    sheet_name = str(value.get("sheet_name", "") or "").strip()
    row_data = value.get("row_data")
    lines = [header]
    if sheet_name:
        lines.append(f"Sheet: {sheet_name}")
    if isinstance(row_data, Mapping):
        for key, cell_value in row_data.items():
            label = str(key or "").strip() or "column"
            rendered_cell = "" if cell_value is None else str(cell_value)
            lines.append(f"{label}: {rendered_cell}")
    return "\n".join(lines)


def _render_spreadsheet_matrix_markdown(matrix: SpreadsheetMatrixParseResult) -> str:
    header_label = matrix.corner_label or "row"
    header_cells = [header_label, *matrix.column_labels]
    divider = ["---"] * len(header_cells)
    rows = [
        "| " + " | ".join(str(cell) for cell in header_cells) + " |",
        "| " + " | ".join(divider) + " |",
    ]
    for row in matrix.rows:
        row_cells = [row.row_label, *[row.values.get(column_label) for column_label in matrix.column_labels]]
        rendered_cells = ["" if cell is None else str(cell) for cell in row_cells]
        rows.append("| " + " | ".join(rendered_cells) + " |")
    return "\n".join(rows)


def _spreadsheet_matrix_selection_response_schema(matrix: SpreadsheetMatrixParseResult) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "row_label": {
                "type": "string",
                "enum": list(matrix.row_labels),
            },
            "column_label": {
                "type": "string",
                "enum": list(matrix.column_labels),
            },
            "reasoning": {
                "type": "string",
            },
        },
        "required": ["row_label", "column_label"],
    }


def _render_context_builder_value(value: Any) -> Any:
    return value


def _render_workspace_file_content(value: Any) -> str:
    if _is_prompt_block_payload(value):
        return _render_prompt_block_text(value)
    if _is_spreadsheet_row_payload(value):
        return _render_spreadsheet_row_text(value)
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return _json_safe(value)


def _is_chat_message_payload(value: Any) -> bool:
    return bool(
        isinstance(value, Mapping)
        and str(value.get("role", "")).strip().lower() in PROMPT_BLOCK_ROLES
        and "content" in value
    )


def _normalize_chat_message_payload(value: Mapping[str, Any]) -> dict[str, Any]:
    payload = {
        "kind": "prompt_block",
        "role": _normalize_prompt_block_role(value.get("role")),
        "content": _prompt_block_text(value),
    }
    name = str(value.get("name", "") or "").strip()
    if name:
        payload["name"] = name
    return payload


def _infer_message_role_from_envelope(
    envelope: "MessageEnvelope",
    *,
    source_node_kind: str | None = None,
) -> str | None:
    explicit_role = envelope.metadata.get("prompt_block_role") or envelope.metadata.get("role")
    if isinstance(explicit_role, str) and explicit_role.strip().lower() in PROMPT_BLOCK_ROLES:
        return _normalize_prompt_block_role(explicit_role)
    if source_node_kind == "input":
        return "user"
    contract = str(envelope.metadata.get("contract", "") or "").strip()
    node_kind = str(envelope.metadata.get("node_kind", "") or "").strip()
    if contract == "message_envelope" and node_kind == "model":
        return "assistant"
    return None


def _extract_prompt_like_payloads(
    value: Any,
    *,
    source_node_kind: str | None = None,
) -> list[dict[str, Any]]:
    if _is_prompt_block_payload(value):
        return [dict(value)]
    if _is_chat_message_payload(value):
        return [_normalize_chat_message_payload(value)]
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        payloads: list[dict[str, Any]] = []
        for item in value:
            item_payloads = _extract_prompt_like_payloads(item, source_node_kind=source_node_kind)
            if not item_payloads:
                return []
            payloads.extend(item_payloads)
        return payloads
    if isinstance(value, Mapping) and "schema_version" in value and "payload" in value:
        try:
            envelope = MessageEnvelope.from_dict(value)
        except Exception:  # noqa: BLE001
            return []
        display_envelope = envelope.artifacts.get("display_envelope")
        if isinstance(display_envelope, Mapping):
            payloads = _extract_prompt_like_payloads(display_envelope, source_node_kind=source_node_kind)
            if payloads:
                return payloads
        payloads = _extract_prompt_like_payloads(envelope.payload, source_node_kind=source_node_kind)
        if payloads:
            return payloads
        role = _infer_message_role_from_envelope(envelope, source_node_kind=source_node_kind)
        content = envelope.payload
        if role is None or content is None or content == "":
            return []
        if isinstance(content, Mapping):
            if _is_chat_message_payload(content):
                return [_normalize_chat_message_payload(content)]
            content = next(
                (
                    str(content[key]).strip()
                    for key in ("message", "content", "text", "summary")
                    if isinstance(content.get(key), str) and str(content.get(key)).strip()
                ),
                "",
            )
            if not content:
                return []
        elif isinstance(content, Sequence) and not isinstance(content, (str, bytes)):
            return []
        return [
            {
                "kind": "prompt_block",
                "role": role,
                "content": content if isinstance(content, str) else _json_safe(content),
            }
        ]
    return []


def _deep_get(value: Any, path: str | None) -> Any:
    if path in {None, "", "$"}:
        return value
    current = value
    for segment in path.split("."):
        if isinstance(current, Mapping):
            current = current.get(segment)
        elif isinstance(current, list) and segment.isdigit():
            index = int(segment)
            current = current[index] if index < len(current) else None
        else:
            return None
    return current


def _deep_get_with_presence(value: Any, path: str | None) -> tuple[bool, Any]:
    if path in {None, "", "$"}:
        return True, value
    current = value
    for segment in path.split("."):
        if isinstance(current, Mapping):
            if segment not in current:
                return False, None
            current = current.get(segment)
        elif isinstance(current, list) and segment.isdigit():
            index = int(segment)
            if index >= len(current):
                return False, None
            current = current[index]
        else:
            return False, None
    return True, current


def _parse_json_object_or_array(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    candidate = value.strip()
    if not candidate:
        return value
    fence_match = JSON_CODE_FENCE_PATTERN.match(candidate)
    if fence_match is not None:
        candidate = str(fence_match.group("body") or "").strip()
    if not candidate or candidate[0] not in {"{", "["}:
        return value
    try:
        parsed = json.loads(candidate)
    except json.JSONDecodeError:
        return value
    return parsed


def _is_message_envelope_like(value: Any) -> bool:
    return isinstance(value, Mapping) and "schema_version" in value and "payload" in value


def _normalize_json_like_source_value(value: Any) -> Any:
    normalized = _parse_json_object_or_array(value)
    if not _is_message_envelope_like(normalized):
        return normalized
    payload_value = _parse_json_object_or_array(normalized.get("payload"))
    if payload_value is normalized.get("payload"):
        return normalized
    return {**dict(normalized), "payload": payload_value}


def _render_variable_template(template: str, variables: Mapping[str, Any]) -> str:
    """Render {placeholder} tokens while keeping Markdown literal braces intact."""
    rendered_template = str(template or "")
    if not rendered_template:
        return rendered_template

    escaped_open = "\x00GRAPH_AGENT_ESCAPED_OPEN\x00"
    escaped_close = "\x00GRAPH_AGENT_ESCAPED_CLOSE\x00"
    rendered_template = rendered_template.replace("{{", escaped_open).replace("}}", escaped_close)

    def _replace_placeholder(match: re.Match[str]) -> str:
        key = str(match.group(1) or "")
        if key not in variables:
            return match.group(0)
        value = variables.get(key)
        if value is None:
            return ""
        return str(value)

    rendered_template = TEMPLATE_PLACEHOLDER_PATTERN.sub(_replace_placeholder, rendered_template)
    return rendered_template.replace(escaped_open, "{").replace(escaped_close, "}")


DEFAULT_GRAPH_ENV_VARS = {
    "OPENAI_API_KEY": "OPENAI_API_KEY",
    "ANTHROPIC_API_KEY": "ANTHROPIC_API_KEY",
    "DISCORD_BOT_TOKEN": "DISCORD_BOT_TOKEN",
    "APOLLO_API_KEY": "APOLLO_API_KEY",
    "LINKEDIN_DATA_DIR": "LINKEDIN_DATA_DIR",
    "EMAIL_TABLE_SUFFIX": "_dev",
}
DEFAULT_SUPABASE_URL_ENV_VAR = "GRAPH_AGENT_SUPABASE_URL"
DEFAULT_SUPABASE_KEY_ENV_VAR = "GRAPH_AGENT_SUPABASE_SECRET_KEY"
DEFAULT_SUPABASE_PROJECT_REF_ENV_VAR = "SUPABASE_PROJECT_REF"
DEFAULT_SUPABASE_ACCESS_TOKEN_ENV_VAR = "SUPABASE_ACCESS_TOKEN"

GRAPH_ENV_REFERENCE_PATTERN = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")
SUPABASE_SQL_TOKEN_PATTERN = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")
CONTEXT_BUILDER_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
CONTEXT_BUILDER_SLUG_PATTERN = re.compile(r"[^A-Za-z0-9_]+")


@dataclass(frozen=True)
class SupabaseConnectionDefinition:
    connection_id: str
    name: str
    supabase_url_env_var: str = DEFAULT_SUPABASE_URL_ENV_VAR
    supabase_key_env_var: str = DEFAULT_SUPABASE_KEY_ENV_VAR
    project_ref_env_var: str = DEFAULT_SUPABASE_PROJECT_REF_ENV_VAR
    access_token_env_var: str = DEFAULT_SUPABASE_ACCESS_TOKEN_ENV_VAR

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> SupabaseConnectionDefinition | None:
        connection_id = str(payload.get("connection_id", "") or "").strip()
        name = str(payload.get("name", "") or "").strip()
        if not connection_id or not name:
            return None
        return cls(
            connection_id=connection_id,
            name=name,
            supabase_url_env_var=str(payload.get("supabase_url_env_var", DEFAULT_SUPABASE_URL_ENV_VAR) or DEFAULT_SUPABASE_URL_ENV_VAR).strip() or DEFAULT_SUPABASE_URL_ENV_VAR,
            supabase_key_env_var=str(payload.get("supabase_key_env_var", DEFAULT_SUPABASE_KEY_ENV_VAR) or DEFAULT_SUPABASE_KEY_ENV_VAR).strip() or DEFAULT_SUPABASE_KEY_ENV_VAR,
            project_ref_env_var=str(payload.get("project_ref_env_var", DEFAULT_SUPABASE_PROJECT_REF_ENV_VAR) or DEFAULT_SUPABASE_PROJECT_REF_ENV_VAR).strip() or DEFAULT_SUPABASE_PROJECT_REF_ENV_VAR,
            access_token_env_var=str(payload.get("access_token_env_var", DEFAULT_SUPABASE_ACCESS_TOKEN_ENV_VAR) or DEFAULT_SUPABASE_ACCESS_TOKEN_ENV_VAR).strip() or DEFAULT_SUPABASE_ACCESS_TOKEN_ENV_VAR,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "connection_id": self.connection_id,
            "name": self.name,
            "supabase_url_env_var": self.supabase_url_env_var,
            "supabase_key_env_var": self.supabase_key_env_var,
            "project_ref_env_var": self.project_ref_env_var,
            "access_token_env_var": self.access_token_env_var,
        }


def _normalize_supabase_connections(payload: Any) -> list[SupabaseConnectionDefinition]:
    if not isinstance(payload, Sequence) or isinstance(payload, (str, bytes, bytearray)):
        return []
    connections: list[SupabaseConnectionDefinition] = []
    seen_ids: set[str] = set()
    for raw_item in payload:
        if not isinstance(raw_item, Mapping):
            continue
        connection = SupabaseConnectionDefinition.from_dict(raw_item)
        if connection is None or connection.connection_id in seen_ids:
            continue
        seen_ids.add(connection.connection_id)
        connections.append(connection)
    return connections


def _normalize_graph_env_vars(payload: Mapping[str, Any] | None) -> dict[str, str]:
    env_vars = dict(DEFAULT_GRAPH_ENV_VARS)
    if not isinstance(payload, Mapping):
        return env_vars
    for key, value in payload.items():
        normalized_key = str(key).strip()
        if not normalized_key:
            continue
        env_vars[normalized_key] = str(value if value is not None else "")
    return env_vars


def _resolve_graph_env_string(value: str, env_vars: Mapping[str, str]) -> str:
    return GRAPH_ENV_REFERENCE_PATTERN.sub(lambda match: env_vars.get(match.group(1), match.group(0)), value)


def _resolve_graph_filesystem_path(value: Any, env_vars: Mapping[str, str]) -> str:
    raw_value = str(value or "").strip()
    if not raw_value:
        return ""

    def _replace_reference(match: re.Match[str]) -> str:
        name = str(match.group(1) or "")
        graph_value = str(env_vars.get(name, "") or "").strip()
        if graph_value and graph_value != name:
            return graph_value
        process_value = str(os.environ.get(name, "") or "").strip()
        if process_value:
            return process_value
        return match.group(0)

    return os.path.expandvars(GRAPH_ENV_REFERENCE_PATTERN.sub(_replace_reference, raw_value)).strip()


def _slugify_context_builder_placeholder(value: Any, *, fallback: str = "source") -> str:
    raw_value = str(value or "").strip().lower()
    normalized = CONTEXT_BUILDER_SLUG_PATTERN.sub("_", raw_value).strip("_")
    if not normalized:
        normalized = CONTEXT_BUILDER_SLUG_PATTERN.sub("_", fallback.strip().lower()).strip("_")
    if not normalized:
        normalized = "source"
    if normalized[0].isdigit():
        normalized = f"source_{normalized}"
    return normalized


def _base_node_instance_label(node: Any) -> str:
    explicit_label = str(getattr(node, "label", "") or "").strip()
    if explicit_label:
        return explicit_label
    provider_label = str(getattr(node, "provider_label", "") or "").strip()
    if provider_label:
        return provider_label
    return str(getattr(node, "id", "") or "").strip()


def _node_instance_label_map(nodes: Sequence[Any]) -> dict[str, str]:
    nodes_by_base_label: dict[str, list[Any]] = {}
    for node in nodes:
        base_label = _base_node_instance_label(node)
        nodes_by_base_label.setdefault(base_label, []).append(node)

    labels: dict[str, str] = {}
    for base_label, matching_nodes in nodes_by_base_label.items():
        if len(matching_nodes) <= 1:
            labels[str(getattr(matching_nodes[0], "id", "") or "")] = base_label
            continue
        for index, node in enumerate(matching_nodes, start=1):
            labels[str(getattr(node, "id", "") or "")] = f"{base_label} {index}"
    return labels


def _normalize_context_builder_header(value: Any, *, fallback: str = "Context") -> str:
    header = str(value or "").strip()
    if header:
        return header
    fallback_header = str(fallback or "").strip()
    return fallback_header or "Context"


def _build_context_builder_section(header: str, body: Any) -> dict[str, Any]:
    return {_normalize_context_builder_header(header): body}


def _is_context_builder_section_list(value: Any) -> bool:
    return bool(
        isinstance(value, list)
        and value
        and all(
            isinstance(item, Mapping)
            and len(item) == 1
            and all(isinstance(key, str) and key.strip() for key in item.keys())
            for item in value
        )
    )


def _context_builder_section_body(section: Any) -> Any:
    if not isinstance(section, Mapping) or len(section) != 1:
        return None
    return next(iter(section.values()))


def resolve_graph_env_value(value: Any, env_vars: Mapping[str, str]) -> Any:
    if isinstance(value, str):
        return _resolve_graph_env_string(value, env_vars)
    if isinstance(value, Mapping):
        return {str(key): resolve_graph_env_value(child, env_vars) for key, child in value.items()}
    if isinstance(value, list):
        return [resolve_graph_env_value(item, env_vars) for item in value]
    return value


def resolve_graph_env_var_name(value: str, env_vars: Mapping[str, str]) -> str:
    return str(resolve_graph_env_value(value, env_vars)).strip()


def resolve_graph_env_reference_name(value: Any, env_vars: Mapping[str, str], *, default: str = "") -> str:
    raw_value = str(value or "").strip()
    if not raw_value:
        return default
    exact_reference = GRAPH_ENV_REFERENCE_EXACT_PATTERN.match(raw_value)
    if exact_reference is not None:
        reference_name = str(exact_reference.group(1) or "").strip()
        aliased_name = str(env_vars.get(reference_name, "") or "").strip()
        if aliased_name and ENV_VAR_NAME_PATTERN.fullmatch(aliased_name):
            return aliased_name
        return reference_name or default
    if ENV_VAR_NAME_PATTERN.fullmatch(raw_value):
        aliased_name = str(env_vars.get(raw_value, "") or "").strip()
        if aliased_name and ENV_VAR_NAME_PATTERN.fullmatch(aliased_name):
            return aliased_name
    return raw_value or default


def resolve_graph_process_env(value: str, env_vars: Mapping[str, str]) -> str:
    env_var_name = resolve_graph_env_reference_name(value, env_vars)
    if not env_var_name:
        return ""
    process_value = os.environ.get(env_var_name, "")
    if str(process_value).strip():
        return process_value
    graph_value = str(env_vars.get(env_var_name, "") or "").strip()
    if graph_value and graph_value != env_var_name:
        return graph_value
    return ""


def resolve_supabase_runtime_env_var_names(
    config: Mapping[str, Any],
    graph: GraphDefinition,
) -> tuple[str, str]:
    connection_id = str(config.get("supabase_connection_id", "") or "").strip()
    if connection_id:
        connection = graph.get_supabase_connection(connection_id)
        if connection is None:
            raise SupabaseDataError(
                f"Supabase connection '{connection_id}' was not found.",
                error_type="missing_supabase_connection",
                details={"connection_id": connection_id},
            )
        return connection.supabase_url_env_var, connection.supabase_key_env_var
    return (
        str(config.get("supabase_url_env_var", DEFAULT_SUPABASE_URL_ENV_VAR) or DEFAULT_SUPABASE_URL_ENV_VAR).strip() or DEFAULT_SUPABASE_URL_ENV_VAR,
        str(config.get("supabase_key_env_var", DEFAULT_SUPABASE_KEY_ENV_VAR) or DEFAULT_SUPABASE_KEY_ENV_VAR).strip() or DEFAULT_SUPABASE_KEY_ENV_VAR,
    )


def resolve_supabase_management_runtime_env_var_names(
    config: Mapping[str, Any],
    graph: GraphDefinition,
) -> tuple[str, str]:
    connection_id = str(config.get("supabase_connection_id", "") or "").strip()
    if connection_id:
        connection = graph.get_supabase_connection(connection_id)
        if connection is None:
            raise SupabaseDataError(
                f"Supabase connection '{connection_id}' was not found.",
                error_type="missing_supabase_connection",
                details={"connection_id": connection_id},
            )
        return connection.project_ref_env_var, connection.access_token_env_var
    return (
        str(config.get("project_ref_env_var", DEFAULT_SUPABASE_PROJECT_REF_ENV_VAR) or DEFAULT_SUPABASE_PROJECT_REF_ENV_VAR).strip()
        or DEFAULT_SUPABASE_PROJECT_REF_ENV_VAR,
        str(config.get("access_token_env_var", DEFAULT_SUPABASE_ACCESS_TOKEN_ENV_VAR) or DEFAULT_SUPABASE_ACCESS_TOKEN_ENV_VAR).strip()
        or DEFAULT_SUPABASE_ACCESS_TOKEN_ENV_VAR,
    )


class ResolvedConfigMapping(MappingABC[str, Any]):
    def __init__(self, raw_config: Mapping[str, Any] | None = None, env_vars: Mapping[str, str] | None = None) -> None:
        self._raw_config = dict(raw_config or {})
        self._env_vars = dict(env_vars or {})

    def __getitem__(self, key: str) -> Any:
        return resolve_graph_env_value(self._raw_config[key], self._env_vars)

    def __iter__(self):
        return iter(self._raw_config)

    def __len__(self) -> int:
        return len(self._raw_config)

    def get(self, key: str, default: Any = None) -> Any:
        if key in self._raw_config:
            return self[key]
        return resolve_graph_env_value(default, self._env_vars)

    def items(self):
        for key in self._raw_config:
            yield key, self[key]

    def values(self):
        for key in self._raw_config:
            yield self[key]


@dataclass
class MessageEnvelope:
    schema_version: str
    from_node_id: str
    from_category: str
    payload: Any
    artifacts: dict[str, Any] = field(default_factory=dict)
    errors: list[dict[str, Any]] = field(default_factory=list)
    tool_calls: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> MessageEnvelope:
        return cls(
            schema_version=str(payload.get("schema_version", "1.0")),
            from_node_id=str(payload.get("from_node_id", "")),
            from_category=str(payload.get("from_category", "")),
            payload=payload.get("payload"),
            artifacts=dict(payload.get("artifacts", {})),
            errors=list(payload.get("errors", [])),
            tool_calls=list(payload.get("tool_calls", [])),
            metadata=dict(payload.get("metadata", {})),
        )


def _message_envelope_from_value(value: Any) -> MessageEnvelope | None:
    if isinstance(value, Mapping) and "schema_version" in value and "payload" in value:
        try:
            return MessageEnvelope.from_dict(value)
        except Exception:  # noqa: BLE001
            return None
    return None


def _request_message_payloads(messages: Sequence[ModelMessage]) -> list[dict[str, str]]:
    payloads: list[dict[str, str]] = []
    for message in messages:
        role = str(message.role or "").strip()
        if not role:
            continue
        payloads.append(
            {
                "role": role,
                "content": str(message.content or ""),
            }
        )
    return payloads


def _request_prompt_trace_artifacts(messages: Sequence[ModelMessage]) -> dict[str, Any]:
    request_messages = _request_message_payloads(messages)
    if not request_messages:
        return {}
    artifacts: dict[str, Any] = {"request_messages": request_messages}
    system_messages = [message["content"] for message in request_messages if message["role"] == "system"]
    user_messages = [message["content"] for message in request_messages if message["role"] == "user"]
    if system_messages:
        artifacts["system_prompt"] = "\n\n".join(system_messages)
    if user_messages:
        artifacts["user_prompt"] = user_messages[-1]
    return artifacts


def _generation_prompt_capture_from_envelope(envelope: MessageEnvelope) -> dict[str, Any] | None:
    if str(envelope.metadata.get("node_kind", "") or "").strip() != "model":
        return None
    raw_request_messages = envelope.artifacts.get("request_messages", [])
    if not isinstance(raw_request_messages, Sequence) or isinstance(raw_request_messages, (str, bytes, bytearray)):
        return None
    request_messages: list[dict[str, str]] = []
    for candidate in raw_request_messages:
        if not isinstance(candidate, Mapping):
            continue
        role = str(candidate.get("role", "") or "").strip()
        if not role:
            continue
        request_messages.append(
            {
                "role": role,
                "content": str(candidate.get("content", "") or ""),
            }
        )
    if not request_messages:
        return None
    system_prompt = str(envelope.artifacts.get("system_prompt", "") or "")
    if not system_prompt:
        system_prompt = "\n\n".join(
            message["content"] for message in request_messages if message["role"] == "system"
        )
    user_prompt = str(envelope.artifacts.get("user_prompt", "") or "")
    if not user_prompt:
        user_messages = [message["content"] for message in request_messages if message["role"] == "user"]
        if user_messages:
            user_prompt = user_messages[-1]
    return {
        "source_node_id": envelope.from_node_id,
        "prompt_name": str(envelope.metadata.get("prompt_name", "") or "").strip(),
        "messages": request_messages,
        "system_prompt": system_prompt,
        "user_prompt": user_prompt,
    }


def _generation_prompt_capture_from_value(value: Any) -> dict[str, Any] | None:
    envelope = _message_envelope_from_value(value)
    if envelope is not None:
        capture = _generation_prompt_capture_from_envelope(envelope)
        if capture is not None:
            return capture
        display_envelope = envelope.artifacts.get("display_envelope")
        if display_envelope is not None:
            capture = _generation_prompt_capture_from_value(display_envelope)
            if capture is not None:
                return capture
        payload = envelope.payload
        if isinstance(payload, Mapping):
            capture = _generation_prompt_capture_from_value(payload)
            if capture is not None:
                return capture
        return None
    if isinstance(value, Mapping):
        for key in ("display_envelope", "payload", "source_payload"):
            if key not in value:
                continue
            capture = _generation_prompt_capture_from_value(value.get(key))
            if capture is not None:
                return capture
    return None


def generation_prompt_capture_from_value(value: Any) -> dict[str, Any] | None:
    capture = _generation_prompt_capture_from_value(value)
    return dict(capture) if isinstance(capture, Mapping) else None


def generation_prompt_captures_from_node_outputs(node_outputs: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(node_outputs, Mapping):
        return []
    captures: list[dict[str, Any]] = []
    for node_id, value in node_outputs.items():
        capture = generation_prompt_capture_from_value(value)
        if capture is None:
            continue
        source_node_id = str(capture.get("source_node_id", "") or "").strip()
        if not source_node_id and isinstance(node_id, str) and node_id:
            capture["source_node_id"] = node_id
        captures.append(capture)
    return captures


@dataclass
class NodeExecutionResult:
    status: str
    output: Any = None
    error: dict[str, Any] | None = None
    summary: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    route_outputs: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class RuntimeEvent:
    event_type: str
    summary: str
    payload: dict[str, Any]
    run_id: str
    schema_version: str = RUNTIME_EVENT_SCHEMA_VERSION
    agent_id: str | None = None
    parent_run_id: str | None = None
    timestamp: str = field(default_factory=utc_now_iso)

    def to_dict(self) -> dict[str, Any]:
        return normalize_runtime_event_dict(asdict(self))


@dataclass
class Condition:
    id: str
    label: str
    condition_type: str
    value: Any = None
    path: str | None = None

    def evaluate(self, state: RunState, result: NodeExecutionResult) -> bool:
        if self.condition_type == "result_status_equals":
            return result.status == self.value
        if self.condition_type == "result_has_error":
            return result.error is not None
        if self.condition_type == "result_payload_path_equals":
            return _deep_get(result.output, self.path) == self.value
        if self.condition_type == "state_error_path_equals":
            return _deep_get(state.node_errors, self.path) == self.value
        raise ValueError(f"Unsupported condition type '{self.condition_type}'.")

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "label": self.label,
            "type": self.condition_type,
            "value": self.value,
            "path": self.path,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> Condition:
        return cls(
            id=str(payload["id"]),
            label=str(payload.get("label", payload["id"])),
            condition_type=str(payload.get("type", "result_status_equals")),
            value=payload.get("value"),
            path=payload.get("path"),
        )


def _is_tool_call_contract_condition(condition: Condition | None) -> bool:
    return bool(
        condition is not None
        and condition.condition_type == "result_payload_path_equals"
        and condition.path == "metadata.contract"
        and condition.value == "tool_call_envelope"
    )


def _is_message_contract_condition(condition: Condition | None) -> bool:
    return bool(
        condition is not None
        and condition.condition_type == "result_payload_path_equals"
        and condition.path == "metadata.contract"
        and condition.value == "message_envelope"
    )


def _is_terminal_output_contract_condition(condition: Condition | None) -> bool:
    return bool(
        condition is not None
        and condition.condition_type == "result_payload_path_equals"
        and condition.path == "metadata.contract"
        and condition.value == "terminal_output_envelope"
    )


def _model_has_exposed_tool_context(graph: GraphDefinition, node: BaseNode) -> bool:
    candidate_node_ids: set[str] = set()
    configured_target_ids = node.config.get("tool_target_node_ids", [])
    if isinstance(configured_target_ids, Sequence) and not isinstance(configured_target_ids, (str, bytes)):
        candidate_node_ids.update(str(node_id).strip() for node_id in configured_target_ids if str(node_id).strip())
    for edge in graph.get_incoming_edges(node.id):
        if edge.kind == "binding":
            candidate_node_ids.add(edge.source_id)
    for node_id in candidate_node_ids:
        candidate = graph.nodes.get(node_id)
        if candidate is None or candidate.kind != "mcp_context_provider":
            continue
        if not bool(candidate.config.get("expose_mcp_tools", True)):
            continue
        tool_names = candidate.config.get("tool_names", [])
        if isinstance(tool_names, Sequence) and not isinstance(tool_names, (str, bytes)):
            if any(str(tool_name).strip() for tool_name in tool_names):
                return True
    return False


def _node_supports_mcp_tool_context(node: BaseNode | None) -> bool:
    return isinstance(node, ModelNode)


def _canonicalize_api_decision_tool_names(
    decision_output: Mapping[str, Any],
    tool_registry: ToolRegistry,
) -> dict[str, Any]:
    normalized_output = dict(decision_output)
    raw_tool_calls = decision_output.get("tool_calls", [])
    if not isinstance(raw_tool_calls, Sequence) or isinstance(raw_tool_calls, (str, bytes)):
        return normalized_output
    normalized_tool_calls: list[dict[str, Any]] = []
    for candidate in raw_tool_calls:
        if not isinstance(candidate, Mapping):
            continue
        tool_name = str(candidate.get("tool_name", "")).strip()
        if not tool_name:
            continue
        try:
            tool_name = tool_registry.canonical_name_for(tool_name)
        except (KeyError, ValueError):
            pass
        normalized_candidate = dict(candidate)
        normalized_candidate["tool_name"] = tool_name
        normalized_tool_calls.append(normalized_candidate)
    normalized_output["tool_calls"] = normalized_tool_calls
    return normalized_output


def _model_has_tool_output_route(graph: GraphDefinition, node: BaseNode) -> bool:
    for edge in graph.get_outgoing_edges(node.id):
        if edge.kind == "binding":
            continue
        if edge.source_handle_id == API_TOOL_CALL_HANDLE_ID:
            return True
        if edge.source_handle_id == API_MESSAGE_HANDLE_ID:
            continue
        target_node = graph.nodes.get(edge.target_id)
        if target_node is None:
            continue
        if target_node.category == NodeCategory.TOOL or _is_tool_call_contract_condition(edge.condition):
            return True
    return False


def _model_has_message_output_route(graph: GraphDefinition, node: BaseNode) -> bool:
    for edge in graph.get_outgoing_edges(node.id):
        if edge.kind == "binding":
            continue
        if edge.source_handle_id == API_MESSAGE_HANDLE_ID:
            return True
        if edge.source_handle_id == API_TOOL_CALL_HANDLE_ID:
            continue
        target_node = graph.nodes.get(edge.target_id)
        if target_node is None:
            continue
        if _is_message_contract_condition(edge.condition):
            return True
        if target_node.category in {NodeCategory.API, NodeCategory.CONTROL_FLOW_UNIT, NodeCategory.DATA, NodeCategory.END}:
            return True
    return False


def infer_model_response_mode(graph: GraphDefinition, node: BaseNode) -> str:
    if getattr(node, "provider_id", "") == SPREADSHEET_MATRIX_DECISION_PROVIDER_ID:
        return "message"
    configured_mode = str(node.config.get("response_mode", "") or "").strip()
    if configured_mode in {"message", "tool_call", "auto"}:
        return configured_mode
    has_tool_output_route = _model_has_tool_output_route(graph, node)
    has_message_output_route = _model_has_message_output_route(graph, node)
    if has_tool_output_route and has_message_output_route:
        return "auto"
    if has_tool_output_route:
        return "tool_call"
    if _model_has_exposed_tool_context(graph, node) and not has_message_output_route:
        return "tool_call"
    return "message"


@dataclass
class Edge:
    id: str
    source_id: str
    target_id: str
    source_handle_id: str | None = None
    target_handle_id: str | None = None
    label: str = ""
    kind: str = "standard"
    priority: int = 100
    waypoints: list[dict[str, float]] = field(default_factory=list)
    condition: Condition | None = None

    def is_match(self, state: RunState, result: NodeExecutionResult) -> bool:
        if self.kind != "conditional":
            return True
        if self.condition is None:
            raise ValueError(f"Conditional edge '{self.id}' is missing a condition.")
        return self.condition.evaluate(state, result)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "source_id": self.source_id,
            "target_id": self.target_id,
            "source_handle_id": self.source_handle_id,
            "target_handle_id": self.target_handle_id,
            "label": self.label,
            "kind": self.kind,
            "priority": self.priority,
            "waypoints": self.waypoints,
            "condition": None if self.condition is None else self.condition.to_dict(),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> Edge:
        condition = payload.get("condition")
        raw_waypoints = payload.get("waypoints", [])
        waypoints = []
        if isinstance(raw_waypoints, Sequence) and not isinstance(raw_waypoints, (str, bytes)):
            for waypoint in raw_waypoints:
                if isinstance(waypoint, Mapping):
                    waypoints.append(
                        {
                            "x": float(waypoint.get("x", 0)),
                            "y": float(waypoint.get("y", 0)),
                        }
                    )
        return cls(
            id=str(payload["id"]),
            source_id=str(payload["source_id"]),
            target_id=str(payload["target_id"]),
            source_handle_id=str(payload.get("source_handle_id")) if payload.get("source_handle_id") is not None else None,
            target_handle_id=str(payload.get("target_handle_id")) if payload.get("target_handle_id") is not None else None,
            label=str(payload.get("label", "")),
            kind=str(payload.get("kind", "standard")),
            priority=int(payload.get("priority", 100)),
            waypoints=waypoints,
            condition=Condition.from_dict(condition) if isinstance(condition, Mapping) else None,
        )


@dataclass
class TransitionRecord:
    edge_id: str
    source_id: str
    target_id: str
    timestamp: str = field(default_factory=utc_now_iso)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class RunState:
    graph_id: str
    input_payload: Any
    documents: list[dict[str, Any]] = field(default_factory=list)
    run_id: str = field(default_factory=lambda: str(uuid4()))
    agent_id: str | None = None
    parent_run_id: str | None = None
    current_node_id: str | None = None
    current_edge_id: str | None = None
    current_iteration_context: dict[str, Any] = field(default_factory=dict)
    status: str = "pending"
    started_at: str = field(default_factory=utc_now_iso)
    ended_at: str | None = None
    node_outputs: dict[str, Any] = field(default_factory=dict)
    edge_outputs: dict[str, Any] = field(default_factory=dict)
    node_errors: dict[str, Any] = field(default_factory=dict)
    node_statuses: dict[str, str] = field(default_factory=dict)
    iterator_states: dict[str, Any] = field(default_factory=dict)
    visit_counts: dict[str, int] = field(default_factory=dict)
    transition_count: int = 0
    transition_history: list[TransitionRecord] = field(default_factory=list)
    event_count: int = 0
    event_history: list[RuntimeEvent] = field(default_factory=list)
    final_output: Any = None
    terminal_error: dict[str, Any] | None = None
    agent_runs: dict[str, Any] = field(default_factory=dict)
    runtime_preview_cache: dict[str, Any] = field(default_factory=dict)

    def snapshot(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "graph_id": self.graph_id,
            "agent_id": self.agent_id,
            "parent_run_id": self.parent_run_id,
            "current_node_id": self.current_node_id,
            "current_edge_id": self.current_edge_id,
            "status": self.status,
            "started_at": self.started_at,
            "ended_at": self.ended_at,
            "input_payload": self.input_payload,
            "documents": normalize_run_documents(self.documents),
            "node_outputs": self.node_outputs,
            "edge_outputs": self.edge_outputs,
            "node_errors": self.node_errors,
            "node_statuses": self.node_statuses,
            "iterator_states": self.iterator_states,
            "visit_counts": self.visit_counts,
            "transition_count": self.transition_count,
            "transition_history": [transition.to_dict() for transition in self.transition_history],
            "event_count": self.event_count,
            "event_history": [event.to_dict() for event in self.event_history],
            "final_output": self.final_output,
            "terminal_error": self.terminal_error,
            "agent_runs": self.agent_runs,
        }


@dataclass
class RuntimeServices:
    model_providers: dict[str, ModelProvider] = field(default_factory=dict)
    node_provider_registry: NodeProviderRegistry = field(default_factory=NodeProviderRegistry)
    tool_registry: ToolRegistry = field(default_factory=ToolRegistry)
    mcp_server_manager: McpServerManager | None = None
    discord_message_sender: DiscordMessageSender | None = None
    outlook_draft_client: OutlookDraftClient | None = None
    microsoft_auth_service: MicrosoftAuthService | None = None
    config: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class NodeContext:
    graph: GraphDefinition
    state: RunState
    services: RuntimeServices
    node_id: str

    def current_input_edge(self) -> Edge | None:
        current_edge_id = self.state.current_edge_id
        if not current_edge_id:
            return None
        for edge in self.graph.get_incoming_edges(self.node_id):
            if edge.id == current_edge_id:
                return edge
        return None

    def _current_route_output_for_source(self, node_id: str) -> Any:
        current_edge = self.current_input_edge()
        if current_edge is None or current_edge.source_id != node_id:
            return None
        return self.state.edge_outputs.get(current_edge.id)

    def _binding_sources_in_resolution_order(self, sources: Sequence[Any]) -> list[str]:
        ordered_sources = [str(source) for source in sources]
        current_edge = self.current_input_edge()
        if current_edge is None or current_edge.source_id not in ordered_sources:
            return ordered_sources
        prioritized_sources = [current_edge.source_id]
        prioritized_sources.extend(source for source in ordered_sources if source != current_edge.source_id)
        return prioritized_sources

    def latest_output(self, node_id: str) -> Any:
        route_output = self._current_route_output_for_source(node_id)
        if route_output is not None:
            return route_output
        if node_id in self.state.node_outputs:
            return self.state.node_outputs.get(node_id)
        prompt_block_envelope = self.prompt_block_envelope_for_node(node_id)
        if prompt_block_envelope is not None:
            return prompt_block_envelope.to_dict()
        display_node_output = self.display_node_output_for_node(node_id)
        if display_node_output is not None:
            return display_node_output
        return None

    def latest_completed_output(self, node_id: str) -> Any:
        """Full source envelope from completed execution, ignoring the current edge's routed slice."""
        if node_id in self.state.node_outputs:
            return self.state.node_outputs.get(node_id)
        prompt_block_envelope = self.prompt_block_envelope_for_node(node_id)
        if prompt_block_envelope is not None:
            return prompt_block_envelope.to_dict()
        display_node_output = self.display_node_output_for_node(node_id)
        if display_node_output is not None:
            return display_node_output
        return None

    def latest_completed_payload(self, node_id: str) -> Any:
        output = self.latest_completed_output(node_id)
        if isinstance(output, Mapping) and "schema_version" in output and "payload" in output:
            try:
                return MessageEnvelope.from_dict(output).payload
            except Exception:  # noqa: BLE001
                return output.get("payload")
        return output

    def latest_error(self, node_id: str) -> Any:
        return self.state.node_errors.get(node_id)

    def latest_envelope(self, node_id: str) -> MessageEnvelope | None:
        output = self.latest_output(node_id)
        if isinstance(output, Mapping) and "schema_version" in output and "payload" in output:
            return MessageEnvelope.from_dict(output)
        return None

    def latest_payload(self, node_id: str) -> Any:
        envelope = self.latest_envelope(node_id)
        if envelope is not None:
            return envelope.payload
        return self.latest_output(node_id)

    def graph_env_vars(self) -> dict[str, str]:
        return dict(self.graph.env_vars)

    def workspace_dir(self, *, create: bool = False) -> str:
        workspace = resolve_agent_workspace(self.state.run_id, self.state.agent_id, create=create)
        return str(workspace.workspace_dir)

    def resolve_workspace_path(self, relative_path: str, *, create_parent: bool = False) -> tuple[str, str]:
        _, normalized_relative_path, target_path = resolve_agent_workspace_path(
            self.state.run_id,
            self.state.agent_id,
            relative_path,
            create_parent=create_parent,
        )
        return str(target_path), normalized_relative_path.as_posix()

    def current_iteration_context(self) -> dict[str, Any]:
        return dict(self.state.current_iteration_context)

    def is_loop_execution(self) -> bool:
        iteration_context = self.current_iteration_context()
        return bool(iteration_context.get("iteration_id")) or isinstance(iteration_context.get("iterator_row_index"), int)

    def resolve_graph_env_value(self, value: Any) -> Any:
        return resolve_graph_env_value(value, self.graph.env_vars)

    def available_tool_definitions(self, names: list[str]) -> list[dict[str, Any]]:
        definitions: list[dict[str, Any]] = []
        for tool in self.services.tool_registry.exposable_definitions(names):
            definitions.append(self._apply_tool_node_overrides(tool.name, tool.to_dict()))
        return definitions

    def _resolved_tool_definition(self, tool_name: str) -> ToolDefinition | None:
        try:
            return self.services.tool_registry.get_optional(tool_name)
        except ValueError:
            return None

    def _resolved_tool_name(self, tool_name: str) -> str:
        resolved = self._resolved_tool_definition(tool_name)
        if resolved is None:
            return str(tool_name).strip()
        return resolved.canonical_name

    def _tool_reference_matches(self, candidate_name: str, target_name: str) -> bool:
        resolved_candidate = self._resolved_tool_name(candidate_name)
        resolved_target = self._resolved_tool_name(target_name)
        if not resolved_candidate or not resolved_target:
            return False
        return resolved_candidate == resolved_target

    def _candidate_mcp_context_nodes_for_model(self, node_id: str | None = None) -> list[McpContextProviderNode]:
        target_node_id = node_id or self.node_id
        target_node = self.graph.nodes.get(target_node_id)
        if not _node_supports_mcp_tool_context(target_node):
            return []

        candidate_tool_ids: list[str] = []
        configured_target_ids = target_node.config.get("tool_target_node_ids", [])
        if isinstance(configured_target_ids, Sequence) and not isinstance(configured_target_ids, (str, bytes)):
            candidate_tool_ids.extend(str(tool_id) for tool_id in configured_target_ids if str(tool_id).strip())
        for edge in self.graph.get_incoming_edges(target_node_id):
            if edge.kind != "binding":
                continue
            candidate_tool_ids.append(str(edge.source_id))

        seen_tool_ids: set[str] = set()
        candidates: list[McpContextProviderNode] = []
        for tool_node_id in candidate_tool_ids:
            if tool_node_id in seen_tool_ids:
                continue
            seen_tool_ids.add(tool_node_id)
            candidate = self.graph.nodes.get(tool_node_id)
            if isinstance(candidate, McpContextProviderNode):
                candidates.append(candidate)
        return candidates

    def _mcp_tool_prompt_enabled(self, node: BaseNode) -> bool:
        return bool(node.config.get("include_mcp_tool_context", False))

    def _mcp_tool_exposure_enabled(self, node: BaseNode) -> bool:
        return bool(node.config.get("expose_mcp_tools", True))

    def _exposable_mcp_tool(self, tool_name: str) -> dict[str, Any] | None:
        try:
            registry_tool = self.services.tool_registry.require_exposable(tool_name)
        except (KeyError, ValueError):
            return None
        if registry_tool.source_type != "mcp":
            return None
        return registry_tool.to_dict()

    def _configured_tool_names(self, node: BaseNode) -> list[str]:
        tool_names: list[str] = []
        raw_tool_names = node.config.get("tool_names", [])
        if isinstance(raw_tool_names, Sequence) and not isinstance(raw_tool_names, (str, bytes)):
            tool_names.extend(str(tool_name).strip() for tool_name in raw_tool_names if str(tool_name).strip())
        configured_name = str(node.config.get("tool_name", "") or getattr(node, "tool_name", "")).strip()
        if configured_name:
            tool_names.append(configured_name)
        deduped: list[str] = []
        seen: set[str] = set()
        for tool_name in tool_names:
            if tool_name in seen:
                continue
            seen.add(tool_name)
            deduped.append(tool_name)
        return deduped

    def _matching_tool_node(self, tool_name: str) -> BaseNode | None:
        for node in self.graph.nodes.values():
            if node.kind not in {"tool", "mcp_context_provider"}:
                continue
            if any(self._tool_reference_matches(configured_tool_name, tool_name) for configured_tool_name in self._configured_tool_names(node)):
                return node
        return None

    def _apply_tool_node_overrides(self, tool_name: str, definition: Mapping[str, Any]) -> dict[str, Any]:
        tool_node = self._matching_tool_node(tool_name)
        if tool_node is None:
            return dict(definition)

        resolved_name = self._resolved_tool_name(tool_name)
        resolved_definition = dict(definition)
        resolved_definition["name"] = resolved_name
        resolved_definition.setdefault("canonical_name", resolved_name)

        user_description_text = self.resolve_graph_env_value(
            str(tool_node.config.get("tool_user_description") or resolved_definition.get("description", ""))
        )
        agent_description_text = self.resolve_graph_env_value(
            str(
                tool_node.config.get("tool_agent_description")
                or tool_node.config.get("tool_model_description")
                or resolved_definition.get("description", "")
            )
        )
        schema_text = self.resolve_graph_env_value(
            str(
                tool_node.config.get("tool_model_schema_text")
                or json.dumps(resolved_definition.get("input_schema", {}), indent=2, sort_keys=True)
            )
        )
        template = self.resolve_graph_env_value(
            str(
                tool_node.config.get("tool_model_template")
                or "Tool: {tool_name}\nDescription:\n{tool_agent_description}\n\nSchema:\n{tool_schema}"
            )
        )

        resolved_schema: Any = resolved_definition.get("input_schema", {})
        try:
            parsed_schema = json.loads(schema_text)
            if isinstance(parsed_schema, Mapping):
                resolved_schema = parsed_schema
        except json.JSONDecodeError:
            pass

        return {
            **resolved_definition,
            "description": _render_variable_template(
                template,
                self.template_variables(
                    {
                        "tool_name": resolved_name,
                        "tool_user_description": user_description_text,
                        "tool_agent_description": agent_description_text,
                        "tool_description": agent_description_text,
                        "tool_schema": schema_text,
                    }
                ),
            ),
            "input_schema": resolved_schema,
        }

    def prompt_block_payload_for_node(self, node_id: str) -> dict[str, Any] | None:
        candidate = self.graph.nodes.get(node_id)
        if candidate is None or candidate.kind != "data" or candidate.provider_id != PROMPT_BLOCK_PROVIDER_ID:
            return None
        role = _normalize_prompt_block_role(candidate.config.get("role"))
        content = self.render_template(str(candidate.config.get("content", "") or ""))
        name = self.render_template(str(candidate.config.get("name", "") or "")).strip()
        payload = {
            "kind": "prompt_block",
            "role": role,
            "content": content,
        }
        if name:
            payload["name"] = name
        return payload

    def prompt_block_envelope_for_node(self, node_id: str) -> MessageEnvelope | None:
        payload = self.prompt_block_payload_for_node(node_id)
        if payload is None:
            return None
        return MessageEnvelope(
            schema_version="1.0",
            from_node_id=node_id,
            from_category=NodeCategory.DATA.value,
            payload=payload,
            metadata={
                "contract": "data_envelope",
                "node_kind": "data",
                "data_mode": PROMPT_BLOCK_MODE,
                "provider_id": PROMPT_BLOCK_PROVIDER_ID,
                "binding_only": True,
                "prompt_block_role": payload["role"],
            },
        )

    def display_node_output_for_node(self, node_id: str) -> dict[str, Any] | None:
        candidate = self.graph.nodes.get(node_id)
        if candidate is None or candidate.kind != "data" or candidate.provider_id != "core.data_display":
            return None
        if not self.graph.get_incoming_edges(node_id) and not candidate.config.get("input_binding"):
            return None
        display_context = NodeContext(graph=self.graph, state=self.state, services=self.services, node_id=node_id)
        try:
            result = candidate.execute(display_context)
        except Exception:  # noqa: BLE001
            return None
        if not isinstance(result.output, Mapping):
            return None
        output = dict(result.output)
        artifacts = output.get("artifacts")
        has_display_envelope = isinstance(artifacts, Mapping) and artifacts.get("display_envelope") is not None
        if output.get("payload") is None and not has_display_envelope:
            return None
        return output

    def _bound_prompt_block_node_ids(self, node_id: str) -> list[str]:
        target_node = self.graph.nodes.get(node_id)
        if target_node is None:
            return []
        candidate_node_ids: list[str] = []
        configured_node_ids = target_node.config.get("prompt_block_node_ids", [])
        if isinstance(configured_node_ids, Sequence) and not isinstance(configured_node_ids, (str, bytes)):
            candidate_node_ids.extend(str(candidate_id).strip() for candidate_id in configured_node_ids if str(candidate_id).strip())
        for edge in self.graph.get_incoming_edges(node_id):
            if edge.kind == "binding":
                candidate_node_ids.append(edge.source_id)
        ordered_ids: list[str] = []
        seen: set[str] = set()
        for candidate_id in candidate_node_ids:
            if candidate_id in seen:
                continue
            seen.add(candidate_id)
            ordered_ids.append(candidate_id)
        return ordered_ids

    def prompt_block_payloads_for_node(self, node_id: str) -> list[dict[str, Any]]:
        payloads: list[dict[str, Any]] = []
        for candidate_id in self._bound_prompt_block_node_ids(node_id):
            payload = self.prompt_block_payload_for_node(candidate_id)
            if payload is not None:
                payloads.append(payload)
        return payloads

    def prompt_block_messages_for_model(self, node_id: str | None = None) -> list[ModelMessage]:
        target_node_id = node_id or self.node_id
        messages: list[ModelMessage] = []
        for payload in self.prompt_block_payloads_for_node(target_node_id):
            content = _prompt_block_text(payload).strip()
            if not content:
                continue
            messages.append(ModelMessage(role=_normalize_prompt_block_role(payload.get("role")), content=content))
        return messages

    def resolve_binding(self, binding: Mapping[str, Any] | None) -> Any:
        if not binding:
            current_edge = self.current_input_edge()
            if current_edge is not None and current_edge.id in self.state.edge_outputs:
                return self.state.edge_outputs[current_edge.id]
            incoming_edges = self.graph.get_incoming_edges(self.node_id)
            if not incoming_edges:
                return self.state.input_payload
            for edge in reversed(incoming_edges):
                if edge.source_id in self.state.node_outputs:
                    return self.latest_output(edge.source_id)
            return None

        binding_type = str(binding.get("type", "latest_output"))
        if binding_type == "input_payload":
            return self.state.input_payload
        if binding_type == "documents":
            return self.state.documents
        if binding_type == "latest_output":
            return self.latest_output(str(binding["source"]))
        if binding_type == "latest_payload":
            return self.latest_payload(str(binding["source"]))
        if binding_type == "latest_envelope":
            envelope = self.latest_envelope(str(binding["source"]))
            return envelope.to_dict() if envelope else None
        if binding_type == "latest_error":
            return self.latest_error(str(binding["source"]))
        if binding_type == "first_available_payload":
            for source in self._binding_sources_in_resolution_order(binding.get("sources", [])):
                payload = self.latest_payload(str(source))
                if payload is not None:
                    return payload
            return None
        if binding_type == "first_available_envelope":
            for source in self._binding_sources_in_resolution_order(binding.get("sources", [])):
                envelope = self.latest_envelope(str(source))
                if envelope is not None:
                    return envelope.to_dict()
            return None
        if binding_type == "available_tools":
            return self.available_tool_definitions(list(binding.get("names", [])))
        raise ValueError(f"Unsupported binding type '{binding_type}'.")

    def template_variables(self, extra: Mapping[str, Any] | None = None) -> dict[str, Any]:
        input_payload = self.resolve_binding(None)
        if isinstance(input_payload, Mapping) and "payload" in input_payload:
            input_payload = input_payload.get("payload")
        if input_payload is None:
            input_payload = self.state.input_payload
        variables = {
            **self.graph.env_vars,
            "input_payload": input_payload,
            "documents": self.state.documents,
            "run_id": self.state.run_id,
            "graph_id": self.state.graph_id,
            "current_node_id": self.state.current_node_id,
        }
        variables.update(self.current_input_source_template_variables(input_payload))
        if extra:
            variables.update(extra)
        return {key: _json_safe(value) for key, value in variables.items()}

    def render_template(self, template: str, extra: Mapping[str, Any] | None = None) -> str:
        resolved_template = self.resolve_graph_env_value(template)
        return _render_variable_template(resolved_template, self.template_variables(extra))

    def current_input_source_template_variables(self, input_payload: Any) -> dict[str, Any]:
        current_edge = self.current_input_edge()
        if current_edge is None:
            return {}
        source_node = self.graph.nodes.get(current_edge.source_id)
        if source_node is None:
            return {}

        labels = _node_instance_label_map(list(self.graph.nodes.values()))
        aliases = [
            current_edge.source_id,
            _base_node_instance_label(source_node),
            labels.get(source_node.id, ""),
        ]
        variables: dict[str, Any] = {}
        for alias in aliases:
            token = _slugify_context_builder_placeholder(alias, fallback=current_edge.source_id)
            if not CONTEXT_BUILDER_IDENTIFIER_PATTERN.match(token):
                continue
            variables.setdefault(token, input_payload)
        return variables

    def context_builder_section_variables_for_current_input(self) -> dict[str, Any]:
        source_value = self.resolve_binding(None)
        if not _is_message_envelope_like(source_value):
            return {}
        try:
            envelope = MessageEnvelope.from_dict(source_value)
        except Exception:  # noqa: BLE001
            return {}
        if str(envelope.metadata.get("data_mode", "") or "").strip() != "context_builder":
            return {}

        raw_placeholders = envelope.metadata.get("placeholders", [])
        if not isinstance(raw_placeholders, Sequence) or isinstance(raw_placeholders, (str, bytes, bytearray)):
            return {}
        placeholders = [str(placeholder).strip() for placeholder in raw_placeholders if str(placeholder).strip()]
        if not placeholders:
            return {}

        raw_sections = envelope.metadata.get("structured_sections")
        sections = raw_sections if _is_context_builder_section_list(raw_sections) else envelope.payload
        if not _is_context_builder_section_list(sections):
            return {}

        variables: dict[str, Any] = {}
        for placeholder, section in zip(placeholders, sections):
            if not CONTEXT_BUILDER_IDENTIFIER_PATTERN.match(placeholder):
                continue
            body = _context_builder_section_body(section)
            if body is None:
                continue
            variables.setdefault(placeholder, body)
        return variables

    def bound_provider_node(self, node_id: str | None = None) -> ProviderNode | None:
        target_node_id = node_id or self.node_id
        target_node = self.graph.nodes.get(target_node_id)
        if target_node is None:
            return None

        binding_node_id = str(target_node.config.get("provider_binding_node_id", "")).strip()
        if binding_node_id:
            candidate = self.graph.nodes.get(binding_node_id)
            if isinstance(candidate, ProviderNode):
                return candidate

        for edge in self.graph.get_incoming_edges(target_node_id):
            candidate = self.graph.nodes.get(edge.source_id)
            if isinstance(candidate, ProviderNode):
                return candidate
        return None

    def mcp_tool_context_for_model(self, node_id: str | None = None) -> dict[str, Any] | None:
        target_node_id = node_id or self.node_id
        context_tools: list[dict[str, Any]] = []
        server_ids: set[str] = set()
        prompt_blocks: list[str] = []
        usage_hint_blocks: list[str] = []
        placeholder_blocks: list[dict[str, str]] = []
        for candidate in self._candidate_mcp_context_nodes_for_model(target_node_id):
            if not self._mcp_tool_prompt_enabled(candidate):
                continue
            candidate_usage_hint = str(candidate.config.get("usage_hint", "") or "").strip()
            candidate_tool_display_names: list[str] = []
            for configured_tool_name in self._configured_tool_names(candidate):
                registry_tool = self._exposable_mcp_tool(configured_tool_name)
                if registry_tool is None:
                    continue
                tool_name = str(registry_tool.get("name", "")).strip() or self._resolved_tool_name(configured_tool_name)
                model_tool_definition = self._apply_tool_node_overrides(tool_name, registry_tool)
                rendered_prompt_text = str(model_tool_definition.get("description", "")).strip()
                prompt_blocks.append(rendered_prompt_text)
                candidate_tool_display_names.append(str(registry_tool.get("display_name", tool_name) or tool_name))
                placeholder_blocks.append(
                    {
                        "token": f"MCP_TOOL_{len(placeholder_blocks) + 1}",
                        "tool_name": tool_name,
                        "display_name": str(registry_tool.get("display_name", tool_name) or tool_name),
                        "prompt_text": rendered_prompt_text,
                    }
                )
                server = None
                server_id = str(registry_tool.get("server_id", "") or "")
                if server_id and self.services.mcp_server_manager is not None:
                    try:
                        server = self.services.mcp_server_manager.get_server(server_id)
                        server_ids.add(server_id)
                    except KeyError:
                        server = None
                context_tools.append(
                    {
                        "tool_node_id": candidate.id,
                        "tool_node_label": candidate.label,
                        "configured_tool_name": configured_tool_name,
                        "tool_name": tool_name,
                        "display_name": str(registry_tool.get("display_name", tool_name) or tool_name),
                        "tool": registry_tool,
                        "model_tool_definition": model_tool_definition,
                        "server": server,
                        "include_mcp_tool_context": self._mcp_tool_prompt_enabled(candidate),
                        "expose_mcp_tools": self._mcp_tool_exposure_enabled(candidate),
                    }
                )
            if candidate_usage_hint and candidate_tool_display_names:
                usage_hint_blocks.append(
                    "\n".join(
                        [
                            f"Tools: {', '.join(list(dict.fromkeys(candidate_tool_display_names)))}",
                            "Guidance:",
                            candidate_usage_hint,
                        ]
                    )
                )

        if not context_tools:
            return None

        servers: list[dict[str, Any]] = []
        if self.services.mcp_server_manager is not None:
            for server_id in sorted(server_ids):
                try:
                    servers.append(self.services.mcp_server_manager.get_server(server_id))
                except KeyError:
                    continue

        return {
            "tool_names": [tool["tool_name"] for tool in context_tools],
            "tool_nodes": context_tools,
            "servers": servers,
            "prompt_blocks": [block for block in prompt_blocks if block],
            "placeholder_blocks": placeholder_blocks,
            "rendered_prompt_text": "\n\n".join(block for block in prompt_blocks if block),
            "usage_hints_text": "\n\n".join(block for block in usage_hint_blocks if block),
            "run_context": {
                "run_id": self.state.run_id,
                "graph_id": self.state.graph_id,
                "node_id": target_node_id,
            },
        }

    def mcp_tool_definitions_for_model(self, node_id: str | None = None) -> list[dict[str, Any]]:
        definitions_by_name: dict[str, dict[str, Any]] = {}
        for candidate in self._candidate_mcp_context_nodes_for_model(node_id):
            if not self._mcp_tool_exposure_enabled(candidate):
                continue
            for configured_tool_name in self._configured_tool_names(candidate):
                registry_tool = self._exposable_mcp_tool(configured_tool_name)
                if registry_tool is None:
                    continue
                tool_name = str(registry_tool.get("name", "")).strip() or self._resolved_tool_name(configured_tool_name)
                definitions_by_name[tool_name] = self._apply_tool_node_overrides(tool_name, registry_tool)
        return list(definitions_by_name.values())


class BaseNode(ABC):
    kind = "base"

    def __init__(
        self,
        node_id: str,
        label: str,
        *,
        category: NodeCategory,
        provider_id: str,
        provider_label: str | None = None,
        description: str = "",
        config: Mapping[str, Any] | None = None,
        position: Mapping[str, Any] | None = None,
    ) -> None:
        self.id = node_id
        self.label = label
        self.category = category
        self.provider_id = provider_id
        self.provider_label = provider_label or provider_id
        self.description = description
        self.raw_config = dict(config or {})
        self.config: Mapping[str, Any] = ResolvedConfigMapping(self.raw_config)
        self.position = {
            "x": float((position or {}).get("x", 0)),
            "y": float((position or {}).get("y", 0)),
        }

    def attach_graph_env_vars(self, env_vars: Mapping[str, str]) -> None:
        self.config = ResolvedConfigMapping(self.raw_config, env_vars)

    @abstractmethod
    def execute(self, context: NodeContext) -> NodeExecutionResult:
        raise NotImplementedError

    def is_ready(self, context: NodeContext) -> bool:
        return True

    def runtime_input_preview(self, context: NodeContext) -> Any:
        return None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "kind": self.kind,
            "category": self.category.value,
            "label": self.label,
            "provider_id": self.provider_id,
            "provider_label": self.provider_label,
            "description": self.description,
            "position": self.position,
            "config": dict(self.raw_config),
        }


def _incoming_edges_are_all_binding(graph: Any, node_id: str) -> bool:
    edges = graph.get_incoming_edges(node_id)
    if not edges:
        return False
    return all(edge.kind == "binding" for edge in edges)


@dataclass(frozen=True)
class OutboundEmailLoggerBinding:
    node_id: str
    schema: str
    table_name: str
    supabase_url: str
    supabase_key: str
    message_type: str
    outreach_step: int
    sales_approach_template: str
    sales_approach_version_template: str
    parent_outbound_email_id_template: str
    root_outbound_email_id_template: str
    metadata_json_template: str


class InputNode(BaseNode):
    kind = "input"

    def __init__(
        self,
        node_id: str,
        label: str,
        provider_id: str = "core.input",
        provider_label: str = "Core Input Node",
        description: str = "",
        config: Mapping[str, Any] | None = None,
        position: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(
            node_id=node_id,
            label=label,
            category=NodeCategory.START,
            provider_id=provider_id,
            provider_label=provider_label,
            description=description,
            config=config,
            position=position,
        )

    def execute(self, context: NodeContext) -> NodeExecutionResult:
        payload = context.resolve_binding(self.config.get("input_binding"))
        if payload is None:
            payload = context.state.input_payload
        envelope = MessageEnvelope(
            schema_version="1.0",
            from_node_id=self.id,
            from_category=self.category.value,
            payload=payload,
            metadata={"contract": "message_envelope", "node_kind": self.kind},
        )
        return NodeExecutionResult(status="success", output=envelope.to_dict(), summary="Input payload captured.")

    def runtime_input_preview(self, context: NodeContext) -> Any:
        payload = context.resolve_binding(self.config.get("input_binding"))
        if payload is None:
            payload = context.state.input_payload
        return payload


class DataNode(BaseNode):
    kind = "data"

    def __init__(
        self,
        node_id: str,
        label: str,
        provider_id: str = "core.data",
        provider_label: str = "Core Data Node",
        description: str = "",
        config: Mapping[str, Any] | None = None,
        position: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(
            node_id=node_id,
            label=label,
            category=NodeCategory.DATA,
            provider_id=provider_id,
            provider_label=provider_label,
            description=description,
            config=config,
            position=position,
        )

    def _context_builder_bindings(self, context: NodeContext) -> list[dict[str, Any]]:
        incoming_edges = context.graph.get_incoming_edges(self.id)
        binding_source_ids = [edge.source_id for edge in incoming_edges if edge.kind == "binding"]
        node_instance_labels = _node_instance_label_map(list(context.graph.nodes.values()))
        preferred_source_ids: list[str] = []
        current_edge = context.current_input_edge()
        if current_edge is not None:
            preferred_source_ids.append(current_edge.source_id)
        preferred_source_ids.extend(binding_source_ids)
        preferred_source_ids.extend(edge.source_id for edge in incoming_edges)
        incoming_source_ids: list[str] = []
        for source_id in preferred_source_ids:
            if source_id == self.id or source_id in incoming_source_ids:
                continue
            incoming_source_ids.append(source_id)
        incoming_source_set = set(incoming_source_ids)

        bindings: list[dict[str, Any]] = []
        configured_bindings = self.config.get("input_bindings", [])
        if isinstance(configured_bindings, Sequence) and not isinstance(configured_bindings, (str, bytes)):
            for index, raw_binding in enumerate(configured_bindings):
                if not isinstance(raw_binding, Mapping):
                    continue
                source_node_id = str(raw_binding.get("source_node_id") or raw_binding.get("source") or "").strip()
                if not source_node_id or source_node_id not in incoming_source_set:
                    continue
                source_node = context.graph.nodes.get(source_node_id)
                source_label = (
                    node_instance_labels.get(source_node_id)
                    if source_node is not None
                    else source_node_id
                )
                placeholder = _slugify_context_builder_placeholder(
                    raw_binding.get("placeholder"),
                    fallback=(source_label or f"source_{index + 1}"),
                )
                header = _normalize_context_builder_header(
                    raw_binding.get("header"),
                    fallback=(source_label or f"Source {index + 1}"),
                )
                binding = raw_binding.get("binding")
                if not isinstance(binding, Mapping):
                    binding = {"type": "latest_payload", "source": source_node_id}
                bindings.append(
                    {
                        "source_node_id": source_node_id,
                        "header": header,
                        "placeholder": placeholder,
                        "binding": dict(binding),
                    }
                )

        has_explicit_input_bindings = bool(bindings)
        if has_explicit_input_bindings:
            configured_source_ids = {str(b.get("source_node_id", "")).strip() for b in bindings}
            for edge in incoming_edges:
                if edge.kind != "standard":
                    continue
                sid = edge.source_id
                if sid == self.id or sid in configured_source_ids:
                    continue
                source_node = context.graph.nodes.get(sid)
                if source_node is None or source_node.provider_id != "core.data_display":
                    continue
                source_label = node_instance_labels.get(sid, source_node.label)
                placeholder = _slugify_context_builder_placeholder(
                    source_label,
                    fallback=sid,
                )
                bindings.append(
                    {
                        "source_node_id": sid,
                        "header": _normalize_context_builder_header(source_label, fallback=sid),
                        "placeholder": placeholder,
                        "binding": {"type": "latest_payload", "source": sid},
                    }
                )
                configured_source_ids.add(sid)

        if not has_explicit_input_bindings:
            for index, source_node_id in enumerate(incoming_source_ids):
                source_node = context.graph.nodes.get(source_node_id)
                source_label = (
                    node_instance_labels.get(source_node_id)
                    if source_node is not None
                    else source_node_id
                )
                placeholder = _slugify_context_builder_placeholder(
                    source_label,
                    fallback=f"source_{index + 1}",
                )
                bindings.append(
                    {
                        "source_node_id": source_node_id,
                        "header": _normalize_context_builder_header(
                            source_label,
                            fallback=f"Source {index + 1}",
                        ),
                        "placeholder": placeholder,
                        "binding": {"type": "latest_payload", "source": source_node_id},
                    }
                )
        return bindings

    def _context_builder_source_ready(self, context: NodeContext, source_node_id: str) -> bool:
        source = context.graph.nodes.get(source_node_id)
        if source is None:
            return False
        if source.provider_id == PROMPT_BLOCK_PROVIDER_ID:
            return True
        if source.provider_id == "core.data_display":
            if _incoming_edges_are_all_binding(context.graph, source_node_id):
                return context.latest_completed_output(source_node_id) is not None
            return source_node_id in context.state.node_outputs
        if source.kind == "input":
            return source_node_id in context.state.node_outputs
        return source_node_id in context.state.node_outputs

    def _context_builder_all_sources_fulfilled(self, context: NodeContext) -> bool:
        bindings = self._context_builder_bindings(context)
        if not bindings:
            return True
        for binding in bindings:
            source_node_id = str(binding.get("source_node_id", "")).strip()
            if not source_node_id:
                continue
            if not self._context_builder_source_ready(context, source_node_id):
                return False
        return True

    def _execute_context_builder(self, context: NodeContext) -> NodeExecutionResult:
        bindings = self._context_builder_bindings(context)
        all_fulfilled = self._context_builder_all_sources_fulfilled(context)
        resolved_variables: dict[str, Any] = {}
        ordered_sections: list[dict[str, Any]] = []
        ordered_prompt_blocks: list[dict[str, Any]] = []
        saw_non_prompt_value = False
        for binding in bindings:
            source_node_id = str(binding.get("source_node_id", "")).strip()
            header = _normalize_context_builder_header(binding.get("header"), fallback=source_node_id or "Context")
            placeholder = str(binding.get("placeholder", "")).strip()
            if not source_node_id:
                if placeholder:
                    resolved_variables[placeholder] = ""
                continue
            if not self._context_builder_source_ready(context, source_node_id):
                if placeholder:
                    resolved_variables[placeholder] = ""
                continue
            if context.latest_completed_output(source_node_id) is None:
                if placeholder:
                    resolved_variables[placeholder] = ""
                continue

            source_binding = binding.get("binding")
            source_node = context.graph.nodes.get(source_node_id)
            if isinstance(source_binding, Mapping):
                binding_type = str(source_binding.get("type", "latest_payload"))
                if binding_type == "latest_payload":
                    resolved_value = context.latest_completed_payload(source_node_id)
                elif binding_type == "latest_output":
                    resolved_value = context.latest_completed_output(source_node_id)
                elif binding_type == "latest_envelope":
                    resolved_value = context.latest_completed_output(source_node_id)
                elif binding_type == "input_payload":
                    resolved_value = context.state.input_payload
                else:
                    resolved_value = context.resolve_binding(source_binding)
            else:
                resolved_value = context.latest_completed_payload(source_node_id)
            prompt_source_value = context.latest_completed_output(source_node_id) or resolved_value
            prompt_like_values = _extract_prompt_like_payloads(
                prompt_source_value,
                source_node_kind=(source_node.kind if source_node is not None else None),
            )
            value = resolved_value
            if isinstance(value, Mapping) and "payload" in value:
                value = value.get("payload")
            placeholder = str(binding.get("placeholder", "")).strip()
            synthetic_prompt_block: dict[str, Any] | None = None
            if not prompt_like_values and source_node is not None and source_node.kind == "input" and value is not None and value != "":
                synthetic_prompt_block = {
                    "kind": "prompt_block",
                    "role": "user",
                    "content": value if isinstance(value, str) else _json_safe(value),
                }
            prompt_like_value = (
                [dict(prompt_like_value) for prompt_like_value in prompt_like_values]
                if prompt_like_values
                else ([synthetic_prompt_block] if synthetic_prompt_block is not None else [])
            )
            if prompt_like_value:
                rendered_value = "\n\n".join(_render_prompt_block_text(payload) for payload in prompt_like_value)
            else:
                rendered_value = _render_context_builder_value(value)
            if placeholder:
                resolved_variables[placeholder] = rendered_value
            if prompt_like_value:
                ordered_prompt_blocks.extend(prompt_like_value)
            elif value is not None and value != "":
                saw_non_prompt_value = True
            if value is not None and value != "":
                if source_node is not None and source_node.provider_id == "core.context_builder" and _is_context_builder_section_list(value):
                    ordered_sections.extend(dict(item) for item in value)
                else:
                    ordered_sections.append(
                        _build_context_builder_section(
                            header,
                            rendered_value,
                        )
                    )

        template = str(self.config.get("template", "") or "")
        if template.strip():
            payload = context.render_template(template, resolved_variables)
        else:
            should_compile_chatgpt_messages = bool(ordered_prompt_blocks) and not saw_non_prompt_value
            if should_compile_chatgpt_messages:
                rendered_messages = _render_chatgpt_style_messages(ordered_prompt_blocks)
                payload = rendered_messages if rendered_messages else ordered_sections
            else:
                payload = ordered_sections

        envelope = MessageEnvelope(
            schema_version="1.0",
            from_node_id=self.id,
            from_category=self.category.value,
            payload=payload,
            metadata={
                "contract": "data_envelope",
                "node_kind": self.kind,
                "data_mode": "context_builder",
                "binding_count": len(bindings),
                "headers": [str(binding.get("header", "")) for binding in bindings],
                "placeholders": [str(binding.get("placeholder", "")) for binding in bindings],
                "prompt_blocks": ordered_prompt_blocks,
                "structured_sections": ordered_sections,
                "context_builder_complete": all_fulfilled,
            },
        )
        return NodeExecutionResult(
            status="success",
            output=envelope.to_dict(),
            summary="Context builder rendered a context payload." if bindings else "Context builder rendered an empty context payload.",
            metadata={"hold_outgoing_edges": not all_fulfilled},
        )

    def _is_context_builder_ready(self, context: NodeContext) -> bool:
        bindings = self._context_builder_bindings(context)
        if not bindings:
            return True
        for binding in bindings:
            source_node_id = str(binding.get("source_node_id", "")).strip()
            if not source_node_id:
                continue
            if self._context_builder_source_ready(context, source_node_id) and context.latest_completed_output(source_node_id) is not None:
                return True
        return False

    def _execute_prompt_block(self, context: NodeContext) -> NodeExecutionResult:
        payload = context.prompt_block_payload_for_node(self.id) or {
            "kind": "prompt_block",
            "role": "user",
            "content": "",
        }
        envelope = MessageEnvelope(
            schema_version="1.0",
            from_node_id=self.id,
            from_category=self.category.value,
            payload=payload,
            metadata={
                "contract": "data_envelope",
                "node_kind": self.kind,
                "data_mode": PROMPT_BLOCK_MODE,
                "provider_id": self.provider_id,
                "binding_only": True,
                "prompt_block_role": payload["role"],
            },
        )
        return NodeExecutionResult(
            status="success",
            output=envelope.to_dict(),
            summary=f"Prompt block '{self.label}' prepared a {payload['role']} message.",
        )

    def _spreadsheet_config(self, context: NodeContext) -> dict[str, Any]:
        resolved = context.resolve_graph_env_value(dict(self.config))
        file_format = str(resolved.get("file_format", "auto") or "auto").strip().lower() or "auto"
        file_path = str(resolved.get("file_path", "") or "").strip()
        if not file_path:
            file_path = resolve_spreadsheet_path_from_run_documents(
                context.state.documents,
                run_document_id=str(resolved.get("run_document_id", "") or ""),
                run_document_name=str(resolved.get("run_document_name", "") or ""),
            )
        sheet_name = str(resolved.get("sheet_name", "") or "").strip()
        empty_row_policy = str(resolved.get("empty_row_policy", "skip") or "skip").strip().lower() or "skip"
        start_row_index = _coerce_start_row_index(resolved.get("start_row_index"))
        return {
            "file_format": file_format,
            "file_path": file_path,
            "sheet_name": sheet_name,
            "header_row_index": SPREADSHEET_HEADER_ROW_INDEX,
            "start_row_index": start_row_index,
            "empty_row_policy": empty_row_policy,
        }

    def _spreadsheet_iterator_state(self, parse_result: Any) -> dict[str, Any]:
        return {
            "iterator_type": "spreadsheet_rows",
            "status": "ready" if parse_result.row_count > 0 else "completed",
            "current_row_index": 0,
            "total_rows": parse_result.row_count,
            "headers": list(parse_result.headers),
            "sheet_name": parse_result.sheet_name,
            "source_file": parse_result.source_file,
            "file_format": parse_result.file_format,
        }

    def _spreadsheet_row_envelopes(self, parse_result: Any) -> list[dict[str, Any]]:
        row_envelopes: list[dict[str, Any]] = []
        total_rows = parse_result.row_count
        for position, row in enumerate(parse_result.rows, start=1):
            envelope = MessageEnvelope(
                schema_version="1.0",
                from_node_id=self.id,
                from_category=self.category.value,
                payload={
                    "row_index": position,
                    "row_number": row.row_number,
                    "row_data": dict(row.row_data),
                    "sheet_name": parse_result.sheet_name,
                    "source_file": parse_result.source_file,
                },
                metadata={
                    "contract": "data_envelope",
                    "node_kind": self.kind,
                    "data_mode": "spreadsheet_row",
                    "provider_id": self.provider_id,
                    "iterator_type": "spreadsheet_rows",
                    "row_index": position,
                    "row_number": row.row_number,
                    "total_rows": total_rows,
                    "headers": list(parse_result.headers),
                    "sheet_name": parse_result.sheet_name,
                    "source_file": parse_result.source_file,
                    "file_format": parse_result.file_format,
                },
            )
            row_envelopes.append(envelope.to_dict())
        return row_envelopes

    def _execute_spreadsheet_rows(self, context: NodeContext) -> NodeExecutionResult:
        resolved_config = self._spreadsheet_config(context)
        try:
            parse_result = parse_spreadsheet(**resolved_config)
        except SpreadsheetParseError as exc:
            return NodeExecutionResult(
                status="failed",
                error={"type": "spreadsheet_parse_error", "message": str(exc)},
                summary=str(exc),
            )
        iterator_state = self._spreadsheet_iterator_state(parse_result)
        summary_envelope = MessageEnvelope(
            schema_version="1.0",
            from_node_id=self.id,
            from_category=self.category.value,
            payload={
                "source_file": parse_result.source_file,
                "file_format": parse_result.file_format,
                "sheet_name": parse_result.sheet_name,
                "headers": list(parse_result.headers),
                "row_count": parse_result.row_count,
            },
            metadata={
                "contract": "data_envelope",
                "node_kind": self.kind,
                "data_mode": "spreadsheet_rows",
                "provider_id": self.provider_id,
                "iterator_type": "spreadsheet_rows",
                "headers": list(parse_result.headers),
                "sheet_name": parse_result.sheet_name,
                "source_file": parse_result.source_file,
                "file_format": parse_result.file_format,
                "total_rows": parse_result.row_count,
            },
        )
        return NodeExecutionResult(
            status="success",
            output=summary_envelope.to_dict(),
            summary=f"Prepared {parse_result.row_count} spreadsheet row(s).",
            metadata={
                "iterator_state": iterator_state,
                "_internal": {
                    "spreadsheet_row_envelopes": self._spreadsheet_row_envelopes(parse_result),
                },
            },
        )

    def _execute_python_script_runner(self, context: NodeContext) -> NodeExecutionResult:
        resolved = context.resolve_graph_env_value(dict(self.config))
        script_file_id = str(resolved.get("script_file_id", "") or "").strip()
        script_path = str(resolved.get("script_path", "") or "").strip()
        script_file_name = str(resolved.get("script_file_name", "") or "").strip()
        try:
            timeout_seconds = float(resolved.get("timeout_seconds") or DEFAULT_SCRIPT_TIMEOUT_SECONDS)
        except (TypeError, ValueError):
            timeout_seconds = float(DEFAULT_SCRIPT_TIMEOUT_SECONDS)
        if timeout_seconds <= 0:
            timeout_seconds = float(DEFAULT_SCRIPT_TIMEOUT_SECONDS)

        if not script_path:
            return NodeExecutionResult(
                status="failed",
                error={
                    "type": "python_script_not_hydrated",
                    "message": (
                        "Python script runner is missing a resolved script path. "
                        "Select a .py project file and re-run."
                    ),
                    "script_file_id": script_file_id,
                },
                summary="Python script runner has no script selected.",
            )

        payload_source = context.resolve_binding(self.config.get("input_binding"))
        if isinstance(payload_source, Mapping) and "schema_version" in payload_source and "payload" in payload_source:
            payload_value = payload_source.get("payload")
        else:
            payload_value = payload_source
        if payload_value is None:
            payload_value = {}
        try:
            payload_json = json.dumps(payload_value, default=str)
        except (TypeError, ValueError):
            payload_json = "{}"

        result = run_python_script(
            script_path,
            payload_json=payload_json,
            timeout_seconds=timeout_seconds,
        )

        summary_label = script_file_name or Path(script_path).name
        if result.success:
            summary = f"Script '{summary_label}' reported success."
        elif result.timed_out:
            summary = f"Script '{summary_label}' timed out after {int(timeout_seconds)}s."
        else:
            summary = f"Script '{summary_label}' reported failure (exit {result.exit_code})."

        envelope = MessageEnvelope(
            schema_version="1.0",
            from_node_id=self.id,
            from_category=self.category.value,
            payload=result.to_dict(),
            errors=[result.error] if result.error else [],
            metadata={
                "contract": "data_envelope",
                "node_kind": self.kind,
                "data_mode": PYTHON_SCRIPT_RUNNER_MODE,
                "provider_id": self.provider_id,
                "script_file_id": script_file_id,
                "script_file_name": script_file_name,
                "script_path": result.script_path,
                "timeout_seconds": int(timeout_seconds),
                "exit_code": result.exit_code,
                "timed_out": result.timed_out,
                "duration_ms": result.duration_ms,
            },
        )

        return NodeExecutionResult(
            status="success" if result.success else "failed",
            output=envelope.to_dict(),
            error=result.error,
            summary=summary,
        )

    def _execute_write_text_file(self, context: NodeContext) -> NodeExecutionResult:
        configured_relative_path = str(self.config.get("relative_path", "response.txt") or "response.txt").strip() or "response.txt"
        relative_path = _resolve_write_text_file_relative_path(configured_relative_path, context=context)
        source_value = context.resolve_binding(self.config.get("input_binding"))
        if isinstance(source_value, Mapping) and "schema_version" in source_value and "payload" in source_value:
            source_envelope = MessageEnvelope.from_dict(source_value)
        else:
            source_envelope = MessageEnvelope(
                schema_version="1.0",
                from_node_id="",
                from_category="",
                payload=source_value,
                metadata={"contract": "data_envelope", "node_kind": self.kind},
            )
        source_value = source_envelope.payload
        rendered_content = _render_workspace_file_content(source_value)
        is_loop_execution = context.is_loop_execution()
        resolved_exists_behavior = normalize_workspace_text_write_behavior(self.config.get("exists_behavior"))
        if resolved_exists_behavior is None:
            resolved_exists_behavior = "overwrite"
        append_newline = bool(self.config.get("append_newline", True))
        file_record = write_agent_workspace_text_file(
            context.state.run_id,
            context.state.agent_id,
            relative_path,
            rendered_content,
            exists_behavior=resolved_exists_behavior,
            append_newline=append_newline,
        )
        write_mode = str(file_record.get("write_mode", "created") or "created")
        preview_limit = 500
        content_preview = rendered_content if len(rendered_content) <= preview_limit else f"{rendered_content[:preview_limit].rstrip()}..."
        envelope = MessageEnvelope(
            schema_version=source_envelope.schema_version,
            from_node_id=self.id,
            from_category=self.category.value,
            payload=source_envelope.payload,
            artifacts={
                **dict(source_envelope.artifacts),
                "workspace_file": file_record,
            },
            errors=list(source_envelope.errors),
            tool_calls=list(source_envelope.tool_calls),
            metadata={
                **dict(source_envelope.metadata),
                "contract": str(source_envelope.metadata.get("contract", "") or "data_envelope"),
                "node_kind": self.kind,
                "data_mode": "write_text_file",
                "provider_id": self.provider_id,
                "workspace_dir": context.workspace_dir(create=True),
                "write_mode": write_mode,
                "exists_behavior": resolved_exists_behavior,
                "append_newline": append_newline,
                "loop_execution": is_loop_execution,
                "configured_path": configured_relative_path,
                "workspace_file": file_record,
                "content_preview": content_preview,
            },
        )
        action_label = {
            "created": "Wrote",
            "overwritten": "Overwrote",
            "appended": "Appended",
        }.get(write_mode, "Wrote")
        return NodeExecutionResult(
            status="success",
            output=envelope.to_dict(),
            summary=f"{action_label} {file_record['path']} in the agent workspace.",
        )

    def _apollo_email_lookup_config(self, context: NodeContext) -> dict[str, Any]:
        resolved = context.resolve_graph_env_value(dict(self.config))
        return {
            "api_key_env_var": resolve_graph_env_reference_name(
                self.raw_config.get("api_key_env_var", DEFAULT_APOLLO_API_KEY_ENV_VAR),
                context.graph_env_vars(),
                default=DEFAULT_APOLLO_API_KEY_ENV_VAR,
            ),
            "name": str(resolved.get("name", "") or "").strip(),
            "domain": str(resolved.get("domain", "") or "").strip(),
            "organization_name": str(resolved.get("organization_name", "") or "").strip(),
            "first_name": str(resolved.get("first_name", "") or "").strip(),
            "last_name": str(resolved.get("last_name", "") or "").strip(),
            "linkedin_url": str(resolved.get("linkedin_url", "") or "").strip(),
            "email": str(resolved.get("email", "") or "").strip(),
            "twitter_url": str(resolved.get("twitter_url", "") or "").strip(),
            "conversation": str(resolved.get("conversation", "") or "").strip(),
            "reveal_personal_emails": _coerce_bool(resolved.get("reveal_personal_emails"), default=True),
            "reveal_phone_number": _coerce_bool(resolved.get("reveal_phone_number"), default=False),
            "use_cache": _coerce_bool(resolved.get("use_cache"), default=True),
            "force_refresh": _coerce_bool(resolved.get("force_refresh"), default=False),
            "workspace_cache_path_template": str(
                resolved.get("workspace_cache_path_template", "cache/apollo-email/{cache_key}.json")
                or "cache/apollo-email/{cache_key}.json"
            ).strip()
            or "cache/apollo-email/{cache_key}.json",
        }

    def _apollo_email_lookup_source_value(self, context: NodeContext) -> Any:
        source_value = context.resolve_binding(None)
        if source_value is None:
            source_value = context.resolve_binding(self.config.get("input_binding"))
        if isinstance(source_value, Mapping) and "payload" in source_value:
            source_value = source_value.get("payload")
        return source_value

    def _apollo_email_lookup_request(
        self,
        context: NodeContext,
        *,
        resolved_config: Mapping[str, Any] | None = None,
        source_value: Any = None,
    ) -> ApolloEmailLookupRequest:
        config = dict(resolved_config or self._apollo_email_lookup_config(context))
        resolved_source_value = source_value if source_value is not None else self._apollo_email_lookup_source_value(context)
        merged = extract_apollo_lookup_fields(resolved_source_value)
        for key in (
            "name",
            "domain",
            "organization_name",
            "first_name",
            "last_name",
            "linkedin_url",
            "email",
            "twitter_url",
        ):
            value = str(config.get(key, "") or "").strip()
            if value:
                merged[key] = value
        merged["reveal_personal_emails"] = bool(config.get("reveal_personal_emails", False))
        merged["reveal_phone_number"] = bool(config.get("reveal_phone_number", False))
        return ApolloEmailLookupRequest.from_mapping(merged)

    def _build_apollo_email_lookup_envelope(
        self,
        apollo_payload: Mapping[str, Any],
        *,
        cache_status: str,
        cache_key: str,
        lookup_status: str,
        resolved_email: str | None,
        workspace_relative_path: str,
        workspace_file: Mapping[str, Any],
        shared_cache_file: Mapping[str, Any] | None = None,
    ) -> MessageEnvelope:
        artifacts: dict[str, Any] = {
            "workspace_file": dict(workspace_file),
        }
        if shared_cache_file is not None:
            artifacts["shared_cache_file"] = dict(shared_cache_file)
        summary_payload = build_apollo_person_summary(
            apollo_payload,
            resolved_email=resolved_email,
            lookup_status=lookup_status,
        )
        return MessageEnvelope(
            schema_version="1.0",
            from_node_id=self.id,
            from_category=self.category.value,
            payload=summary_payload,
            artifacts=artifacts,
            metadata={
                "contract": "data_envelope",
                "node_kind": self.kind,
                "data_mode": APOLLO_EMAIL_LOOKUP_MODE,
                "provider_id": self.provider_id,
                "cache_status": cache_status,
                "cache_key": cache_key,
                "lookup_status": lookup_status,
                "resolved_email": resolved_email,
                "workspace_cache_path": workspace_relative_path,
                **({"shared_cache_path": str(shared_cache_file.get("path", ""))} if shared_cache_file is not None else {}),
            },
        )

    def _execute_apollo_email_lookup(self, context: NodeContext) -> NodeExecutionResult:
        resolved_config = self._apollo_email_lookup_config(context)
        source_value = self._apollo_email_lookup_source_value(context)
        request = self._apollo_email_lookup_request(
            context,
            resolved_config=resolved_config,
            source_value=source_value,
        )
        request_error = validate_apollo_lookup_request(request)
        if request_error:
            return NodeExecutionResult(
                status="failed",
                error={
                    "type": "invalid_apollo_lookup_input",
                    "message": request_error,
                    "lookup": request.to_lookup_fields(),
                },
                summary="Apollo email lookup did not receive enough identifying information.",
            )

        cache_info = build_apollo_email_lookup_cache_info(request)
        workspace_template = resolved_config["workspace_cache_path_template"]
        use_cache = resolved_config["use_cache"]
        force_refresh = resolved_config["force_refresh"]

        if use_cache and not force_refresh:
            cached_entry, shared_cache_file = read_cached_apollo_email_lookup(cache_info)
            if cached_entry is not None:
                try:
                    workspace_relative_path, workspace_file = write_apollo_email_lookup_workspace_copy(
                        context.state.run_id,
                        context.state.agent_id,
                        workspace_template,
                        cache_key=cache_info.cache_key,
                        cache_entry=cached_entry,
                    )
                except Exception as exc:  # noqa: BLE001
                    return NodeExecutionResult(
                        status="failed",
                        error={
                            "type": "apollo_workspace_cache_write_failed",
                            "message": str(exc),
                            "cache_key": cache_info.cache_key,
                        },
                        summary="Apollo cache hit could not be mirrored into the agent workspace.",
                    )
                envelope = self._build_apollo_email_lookup_envelope(
                    cached_entry["payload"],
                    cache_status="hit",
                    cache_key=cache_info.cache_key,
                    lookup_status=str(cached_entry.get("lookup_status", "no_match") or "no_match"),
                    resolved_email=str(cached_entry.get("resolved_email", "") or "") or None,
                    workspace_relative_path=workspace_relative_path,
                    workspace_file=workspace_file,
                    shared_cache_file=shared_cache_file,
                )
                return NodeExecutionResult(
                    status="success",
                    output=envelope.to_dict(),
                    summary="Loaded Apollo lookup from shared cache.",
                    metadata=envelope.metadata,
                )

        api_key = resolve_graph_process_env(resolved_config["api_key_env_var"], context.graph_env_vars())
        cache_status = "refresh" if force_refresh else "miss"
        try:
            apollo_payload = fetch_apollo_person_match_live(request=request, api_key=api_key)
        except ApolloLookupError as exc:
            error = exc.to_error_dict()
            if exc.error_type == "apollo_api_key_missing":
                error["api_key_env_var"] = resolved_config["api_key_env_var"]
                error.setdefault("attempted_api_key", api_key)
            return NodeExecutionResult(status="failed", error=error, summary=str(exc))

        lookup_status = determine_apollo_lookup_status(apollo_payload)
        resolved_email = extract_apollo_email(apollo_payload)
        cache_entry = build_apollo_email_cache_entry(request, apollo_payload)

        try:
            workspace_relative_path, workspace_file = write_apollo_email_lookup_workspace_copy(
                context.state.run_id,
                context.state.agent_id,
                workspace_template,
                cache_key=cache_info.cache_key,
                cache_entry=cache_entry,
            )
        except Exception as exc:  # noqa: BLE001
            return NodeExecutionResult(
                status="failed",
                error={
                    "type": "apollo_workspace_cache_write_failed",
                    "message": str(exc),
                    "cache_key": cache_info.cache_key,
                },
                summary="Apollo lookup completed but the workspace mirror could not be written.",
            )

        shared_cache_file: dict[str, Any] | None = None
        if use_cache and is_cacheable_apollo_response(apollo_payload):
            try:
                shared_cache_file = write_cached_apollo_email_lookup(cache_info, cache_entry)
            except Exception as exc:  # noqa: BLE001
                return NodeExecutionResult(
                    status="failed",
                    error={
                        "type": "apollo_shared_cache_write_failed",
                        "message": str(exc),
                        "cache_key": cache_info.cache_key,
                    },
                    summary="Apollo lookup completed but the shared cache entry could not be written.",
                )

        envelope = self._build_apollo_email_lookup_envelope(
            apollo_payload,
            cache_status=cache_status,
            cache_key=cache_info.cache_key,
            lookup_status=lookup_status,
            resolved_email=resolved_email,
            workspace_relative_path=workspace_relative_path,
            workspace_file=workspace_file,
            shared_cache_file=shared_cache_file,
        )
        summary_prefix = "Refreshed" if cache_status == "refresh" else "Fetched"
        summary = (
            f"{summary_prefix} Apollo match and updated shared cache."
            if shared_cache_file is not None
            else f"{summary_prefix} Apollo lookup."
        )
        return NodeExecutionResult(
            status="success",
            output=envelope.to_dict(),
            summary=summary,
            metadata=envelope.metadata,
        )

    def _linkedin_profile_fetch_config(self, context: NodeContext) -> dict[str, Any]:
        resolved = context.resolve_graph_env_value(dict(self.config))
        return {
            "url_field": str(resolved.get("url_field", "url") or "url").strip() or "url",
            "linkedin_data_dir": _resolve_graph_filesystem_path(self.raw_config.get("linkedin_data_dir", ""), context.graph_env_vars()),
            "session_state_path": _resolve_graph_filesystem_path(self.raw_config.get("session_state_path", ""), context.graph_env_vars()),
            "headless": _coerce_bool(resolved.get("headless"), default=False),
            "navigation_timeout_ms": _coerce_int(resolved.get("navigation_timeout_ms"), default=45000, minimum=1000),
            "page_settle_ms": _coerce_int(resolved.get("page_settle_ms"), default=3000, minimum=0),
            "use_cache": _coerce_bool(resolved.get("use_cache"), default=True),
            "force_refresh": _coerce_bool(resolved.get("force_refresh"), default=False),
            "workspace_cache_path_template": str(
                resolved.get("workspace_cache_path_template", "cache/linkedin/{cache_key}.json") or "cache/linkedin/{cache_key}.json"
            ).strip()
            or "cache/linkedin/{cache_key}.json",
        }

    def _linkedin_profile_fetch_source_value(self, context: NodeContext) -> Any:
        source_value = context.resolve_binding(None)
        if source_value is None:
            source_value = context.resolve_binding(self.config.get("input_binding"))
        if isinstance(source_value, Mapping) and "payload" in source_value:
            source_value = source_value.get("payload")
        return source_value

    def _build_linkedin_profile_envelope(
        self,
        profile_payload: Mapping[str, Any],
        *,
        cache_status: str,
        cache_key: str,
        source_url: str,
        normalized_url: str,
        workspace_relative_path: str,
        workspace_file: Mapping[str, Any],
        shared_cache_file: Mapping[str, Any] | None = None,
        final_page_url: str = "",
        storage_state_path: str = "",
    ) -> MessageEnvelope:
        artifacts: dict[str, Any] = {
            "workspace_file": dict(workspace_file),
        }
        if shared_cache_file is not None:
            artifacts["shared_cache_file"] = dict(shared_cache_file)
        if final_page_url:
            artifacts["final_page_url"] = final_page_url
        if storage_state_path:
            artifacts["storage_state_path"] = storage_state_path
        return MessageEnvelope(
            schema_version="1.0",
            from_node_id=self.id,
            from_category=self.category.value,
            payload=dict(profile_payload),
            artifacts=artifacts,
            metadata={
                "contract": "data_envelope",
                "node_kind": self.kind,
                "data_mode": LINKEDIN_PROFILE_FETCH_MODE,
                "provider_id": self.provider_id,
                "cache_status": cache_status,
                "cache_key": cache_key,
                "source_url": source_url,
                "normalized_url": normalized_url,
                "workspace_cache_path": workspace_relative_path,
                **({"shared_cache_path": str(shared_cache_file.get("path", ""))} if shared_cache_file is not None else {}),
            },
        )

    def _execute_linkedin_profile_fetch(self, context: NodeContext) -> NodeExecutionResult:
        resolved_config = self._linkedin_profile_fetch_config(context)
        source_value = self._linkedin_profile_fetch_source_value(context)
        source_url = extract_linkedin_profile_url(source_value, url_field=resolved_config["url_field"])
        if not source_url:
            return NodeExecutionResult(
                status="failed",
                error={
                    "type": "invalid_linkedin_profile_input",
                    "message": f"No LinkedIn URL was found in the incoming payload. Expected a string URL or a '{resolved_config['url_field']}' field.",
                    "url_field": resolved_config["url_field"],
                },
                summary="LinkedIn profile fetch did not receive a URL.",
            )

        try:
            cache_info = build_linkedin_profile_cache_info(source_url)
        except LinkedInFetchError as exc:
            return NodeExecutionResult(status="failed", error=exc.to_error_dict(), summary=str(exc))

        workspace_template = resolved_config["workspace_cache_path_template"]
        use_cache = resolved_config["use_cache"]
        force_refresh = resolved_config["force_refresh"]

        if use_cache and not force_refresh:
            cached_payload, shared_cache_file = read_cached_linkedin_profile(cache_info)
            if cached_payload is not None:
                cached_payload = sanitize_linkedin_profile_payload(cached_payload)
                try:
                    workspace_relative_path, workspace_file = write_linkedin_profile_workspace_copy(
                        context.state.run_id,
                        context.state.agent_id,
                        workspace_template,
                        cache_key=cache_info.cache_key,
                        payload=cached_payload,
                    )
                except Exception as exc:  # noqa: BLE001
                    return NodeExecutionResult(
                        status="failed",
                        error={
                            "type": "linkedin_workspace_cache_write_failed",
                            "message": str(exc),
                            "cache_key": cache_info.cache_key,
                        },
                        summary="LinkedIn cache hit could not be mirrored into the agent workspace.",
                    )
                envelope = self._build_linkedin_profile_envelope(
                    cached_payload,
                    cache_status="hit",
                    cache_key=cache_info.cache_key,
                    source_url=source_url,
                    normalized_url=cache_info.normalized_url,
                    workspace_relative_path=workspace_relative_path,
                    workspace_file=workspace_file,
                    shared_cache_file=shared_cache_file,
                )
                return NodeExecutionResult(
                    status="success",
                    output=envelope.to_dict(),
                    summary=f"Loaded LinkedIn profile '{cache_info.normalized_url}' from shared cache.",
                    metadata=envelope.metadata,
                )

        cache_status = "refresh" if force_refresh else "miss"
        try:
            live_result = fetch_linkedin_profile_live(
                url=cache_info.normalized_url,
                linkedin_data_dir=resolved_config["linkedin_data_dir"],
                session_state_path=resolved_config["session_state_path"],
                headless=resolved_config["headless"],
                navigation_timeout_ms=resolved_config["navigation_timeout_ms"],
                page_settle_ms=resolved_config["page_settle_ms"],
            )
        except LinkedInFetchError as exc:
            return NodeExecutionResult(status="failed", error=exc.to_error_dict(), summary=str(exc))

        profile_payload = sanitize_linkedin_profile_payload(dict(live_result["extracted"]))
        try:
            workspace_relative_path, workspace_file = write_linkedin_profile_workspace_copy(
                context.state.run_id,
                context.state.agent_id,
                workspace_template,
                cache_key=cache_info.cache_key,
                payload=profile_payload,
            )
        except Exception as exc:  # noqa: BLE001
            return NodeExecutionResult(
                status="failed",
                error={
                    "type": "linkedin_workspace_cache_write_failed",
                    "message": str(exc),
                    "cache_key": cache_info.cache_key,
                },
                summary="LinkedIn fetch completed but the workspace mirror could not be written.",
            )

        shared_cache_file: dict[str, Any] | None = None
        if use_cache and is_cacheable_linkedin_profile(profile_payload):
            try:
                shared_cache_file = write_cached_linkedin_profile(cache_info, profile_payload)
            except Exception as exc:  # noqa: BLE001
                return NodeExecutionResult(
                    status="failed",
                    error={
                        "type": "linkedin_shared_cache_write_failed",
                        "message": str(exc),
                        "cache_key": cache_info.cache_key,
                    },
                    summary="LinkedIn fetch completed but the shared cache entry could not be written.",
                )

        envelope = self._build_linkedin_profile_envelope(
            profile_payload,
            cache_status=cache_status,
            cache_key=cache_info.cache_key,
            source_url=source_url,
            normalized_url=cache_info.normalized_url,
            workspace_relative_path=workspace_relative_path,
            workspace_file=workspace_file,
            shared_cache_file=shared_cache_file,
            final_page_url=str(live_result.get("final_page_url", "") or ""),
            storage_state_path=str(live_result.get("storage_state_path", "") or ""),
        )

        if not is_cacheable_linkedin_profile(profile_payload):
            error = error_from_linkedin_profile_payload(
                profile_payload,
                source_url=source_url,
                normalized_url=cache_info.normalized_url,
            )
            return NodeExecutionResult(
                status="failed",
                output=envelope.to_dict(),
                error=error,
                summary=error["message"],
                metadata=envelope.metadata,
            )

        summary_prefix = "Refreshed" if cache_status == "refresh" else "Fetched"
        summary_suffix = " and updated shared cache." if shared_cache_file is not None else "."
        return NodeExecutionResult(
            status="success",
            output=envelope.to_dict(),
            summary=f"{summary_prefix} LinkedIn profile '{cache_info.normalized_url}'{summary_suffix}",
            metadata=envelope.metadata,
        )

    def _structured_payload_builder_config(self, context: NodeContext) -> StructuredPayloadBuilderConfig:
        resolved = context.resolve_graph_env_value(dict(self.config))
        return StructuredPayloadBuilderConfig(
            template_json=str(resolved.get("template_json", "{}") or "{}").strip() or "{}",
            case_sensitive=_coerce_bool(resolved.get("case_sensitive"), default=False),
            max_matches_per_field=_coerce_int(resolved.get("max_matches_per_field"), default=25, minimum=1),
            field_aliases=_coerce_structured_payload_field_aliases(resolved.get("field_aliases")),
            default_search_section=_coerce_structured_payload_search_section(
                resolved.get("default_search_section"), default="payload"
            ),
            field_search_scopes=_coerce_structured_payload_field_search_scopes(
                resolved.get("field_search_scopes")
            ),
        )

    def _structured_payload_builder_source_value(self, context: NodeContext) -> Any:
        source_value = context.resolve_binding(None)
        if source_value is None:
            source_value = context.resolve_binding(self.config.get("input_binding"))
        source_value = _parse_json_object_or_array(source_value)
        if _is_message_envelope_like(source_value):
            source_value = _parse_json_object_or_array(source_value.get("payload"))
        if isinstance(source_value, Mapping) and "payload" in source_value:
            source_value = _parse_json_object_or_array(source_value.get("payload"))
        return source_value

    def _structured_payload_builder_labeled_source_roots(
        self,
        context: NodeContext,
    ) -> tuple[tuple[str, Any], ...]:
        """Return all derived source roots labeled by kind (``payload``/``metadata``/``artifacts``).

        Always returns the full unfiltered set; per-field/global scope filtering happens
        downstream in ``build_structured_payload`` so per-entry overrides can take effect.
        """
        raw_value = context.resolve_binding(None)
        if raw_value is None:
            raw_value = context.resolve_binding(self.config.get("input_binding"))
        raw_value = _parse_json_object_or_array(raw_value)

        metadata_roots: list[Any] = []
        artifacts_roots: list[Any] = []
        primary: Any = raw_value
        for _ in range(8):
            if _is_message_envelope_like(primary):
                metadata_roots.append(_parse_json_object_or_array(primary.get("metadata")))
                artifacts_roots.append(_parse_json_object_or_array(primary.get("artifacts")))
                primary = _parse_json_object_or_array(primary.get("payload"))
                continue
            if isinstance(primary, Mapping) and "payload" in primary:
                primary = _parse_json_object_or_array(primary.get("payload"))
                continue
            break

        labeled: list[tuple[str, Any]] = []
        seen_ids: set[int] = set()

        def add(kind: str, candidate: Any) -> None:
            if candidate is None:
                return
            if isinstance(candidate, Mapping) and len(candidate) == 0:
                return
            if isinstance(candidate, list) and len(candidate) == 0:
                return
            identifier = id(candidate)
            if identifier in seen_ids:
                return
            seen_ids.add(identifier)
            labeled.append((kind, candidate))

        add("payload", primary)
        for candidate in metadata_roots:
            add("metadata", candidate)
        for candidate in artifacts_roots:
            add("artifacts", candidate)
        if not labeled:
            labeled.append(("payload", primary))
        return tuple(labeled)

    def _execute_structured_payload_builder(self, context: NodeContext) -> NodeExecutionResult:
        resolved_config = self._structured_payload_builder_config(context)
        labeled_roots = self._structured_payload_builder_labeled_source_roots(context)
        source_roots = tuple(value for _, value in labeled_roots)
        source_root_kinds = tuple(kind for kind, _ in labeled_roots)
        try:
            template = parse_structured_payload_template(resolved_config.template_json)
        except ValueError as exc:
            error = {
                "type": "invalid_template_json",
                "message": str(exc),
            }
            return NodeExecutionResult(status="failed", error=error, summary=error["message"])

        result = build_structured_payload(
            template,
            source_roots,
            case_sensitive=resolved_config.case_sensitive,
            max_matches_per_field=resolved_config.max_matches_per_field,
            field_aliases=dict(resolved_config.field_aliases) if resolved_config.field_aliases else None,
            source_root_kinds=source_root_kinds,
            field_search_scopes=dict(resolved_config.field_search_scopes) if resolved_config.field_search_scopes else None,
            default_search_section=resolved_config.default_search_section,
        )
        envelope = MessageEnvelope(
            schema_version="1.0",
            from_node_id=self.id,
            from_category=self.category.value,
            payload=result.payload,
            artifacts={
                "field_matches": list(result.field_matches),
                "filled_paths": list(result.filled_paths),
                "preserved_paths": list(result.preserved_paths),
                "unresolved_paths": list(result.unresolved_paths),
                "template": template,
            },
            metadata={
                "contract": "data_envelope",
                "node_kind": self.kind,
                "data_mode": STRUCTURED_PAYLOAD_BUILDER_MODE,
                "provider_id": self.provider_id,
                "filled_field_count": len(result.filled_paths),
                "preserved_field_count": len(result.preserved_paths),
                "unresolved_field_count": len(result.unresolved_paths),
                "case_sensitive": resolved_config.case_sensitive,
                "max_matches_per_field": resolved_config.max_matches_per_field,
                "default_search_section": resolved_config.default_search_section,
            },
        )
        summary = (
            f"Built structured payload with {len(result.filled_paths)} auto-filled field(s)."
            if result.filled_paths
            else "Built structured payload without any auto-filled fields."
        )
        return NodeExecutionResult(
            status="success",
            output=envelope.to_dict(),
            summary=summary,
            metadata=envelope.metadata,
        )

    def _runtime_normalizer_config(self, context: NodeContext) -> RuntimeFieldExtractorConfig:
        resolved = context.resolve_graph_env_value(dict(self.config))
        field_names = parse_field_name_list(resolved.get("field_names"))
        if not field_names:
            field_names = parse_field_name_list(resolved.get("field_name"))
        fallback_field_names = parse_field_name_list(resolved.get("fallback_field_names"))
        return RuntimeFieldExtractorConfig(
            field_names=field_names,
            fallback_field_names=fallback_field_names,
            preferred_path=str(resolved.get("preferred_path", "") or "").strip(),
            case_sensitive=_coerce_bool(resolved.get("case_sensitive"), default=False),
            max_matches=_coerce_int(resolved.get("max_matches"), default=25, minimum=1),
        )

    def _runtime_normalizer_source_roots(self, context: NodeContext) -> tuple[Any, ...]:
        source_value = self._runtime_normalizer_source_value(context)
        source_value = _normalize_json_like_source_value(source_value)
        roots: list[Any] = [source_value]
        if _is_message_envelope_like(source_value):
            roots.append(source_value.get("payload"))
        return tuple(roots)

    def _runtime_normalizer_source_value(self, context: NodeContext) -> Any:
        source_value = context.resolve_binding(None)
        if source_value is None:
            source_value = context.resolve_binding(self.config.get("input_binding"))
        return source_value

    def _display_source_value(self, context: NodeContext) -> tuple[Any, Any]:
        source_value = context.resolve_binding(self.config.get("input_binding"))
        display_value = _normalize_json_like_source_value(source_value)
        if isinstance(display_value, Mapping) and "payload" in display_value:
            return display_value.get("payload"), display_value
        return display_value, display_value

    def _execute_runtime_normalizer(self, context: NodeContext) -> NodeExecutionResult:
        resolved_config = self._runtime_normalizer_config(context)
        source_roots = self._runtime_normalizer_source_roots(context)
        if not resolved_config.field_names:
            error = {
                "type": "missing_field_name",
                "message": "Runtime field extractor requires at least one configured field name.",
            }
            return NodeExecutionResult(status="failed", error=error, summary=error["message"])

        requested_field_names = resolved_config.field_names
        single_field_mode = len(requested_field_names) == 1
        matched_value = None
        matched_path = ""
        matched_values_by_field: dict[str, Any] = {}
        matched_paths_by_field: dict[str, str] = {}
        all_matches: list[dict[str, Any]] = []

        if resolved_config.preferred_path and single_field_mode:
            for source_value in source_roots:
                preferred_value = _deep_get(source_value, resolved_config.preferred_path)
                if preferred_value is None:
                    continue
                matched_value = preferred_value
                matched_path = resolved_config.preferred_path
                matched_values_by_field[resolved_config.field_name] = preferred_value
                matched_paths_by_field[resolved_config.field_name] = resolved_config.preferred_path
                all_matches.append(
                    {
                        "field": resolved_config.field_name,
                        "requested_field": resolved_config.field_name,
                        "path": resolved_config.preferred_path,
                        "value": preferred_value,
                    }
                )
                break

        if matched_value is None:
            for requested_field_name in requested_field_names:
                search_fields = (
                    (requested_field_name, *resolved_config.fallback_field_names)
                    if single_field_mode
                    else (requested_field_name,)
                )
                field_matches: list[dict[str, Any]] = []
                for source_value in source_roots:
                    field_matches = extract_field_candidates(
                        source_value,
                        field_names=search_fields,
                        case_sensitive=resolved_config.case_sensitive,
                        max_matches=resolved_config.max_matches,
                    )
                    if not field_matches:
                        continue
                    first_match = dict(field_matches[0])
                    matched_values_by_field[requested_field_name] = first_match.get("value")
                    matched_paths_by_field[requested_field_name] = str(first_match.get("path", "") or "")
                    if single_field_mode:
                        matched_value = first_match.get("value")
                        matched_path = matched_paths_by_field[requested_field_name]
                    break
                all_matches.extend(
                    {
                        **match,
                        "requested_field": requested_field_name,
                    }
                    for match in field_matches
                )

        if not single_field_mode:
            matched_value = {field_name: matched_values_by_field[field_name] for field_name in requested_field_names if field_name in matched_values_by_field}

        missing_field_names = [field_name for field_name in requested_field_names if field_name not in matched_values_by_field]

        metadata = {
            "contract": "data_envelope",
            "node_kind": self.kind,
            "data_mode": RUNTIME_NORMALIZER_MODE,
            "provider_id": self.provider_id,
            "field_names": list(requested_field_names),
            "field_name": resolved_config.field_name,
            "fallback_field_names": list(resolved_config.fallback_field_names),
            "matched_path": matched_path or None,
            "matched_paths_by_field": matched_paths_by_field,
            "match_count": len(all_matches),
            "matched_field_names": list(matched_values_by_field.keys()),
            "missing_field_names": missing_field_names,
            "case_sensitive": resolved_config.case_sensitive,
        }
        artifacts = {"field_matches": all_matches}
        envelope = MessageEnvelope(
            schema_version="1.0",
            from_node_id=self.id,
            from_category=self.category.value,
            payload=matched_value,
            artifacts=artifacts,
            metadata=metadata,
        )
        error = None
        if single_field_mode:
            summary = (
                f"Extracted '{resolved_config.field_name}' from '{matched_path}'."
                if matched_path
                else f"Field '{resolved_config.field_name}' was not found."
            )
        else:
            summary = (
                f"Extracted {len(matched_values_by_field)} of {len(requested_field_names)} requested fields."
                if not missing_field_names
                else f"Missing {len(missing_field_names)} of {len(requested_field_names)} requested fields."
            )
        status = "success"
        if single_field_mode and matched_path == "":
            error = {
                "type": "field_not_found",
                "message": f"Field '{resolved_config.field_name}' was not found in the incoming envelope data.",
                "field_name": resolved_config.field_name,
                "fallback_field_names": list(resolved_config.fallback_field_names),
            }
            status = "failed"
        elif not single_field_mode and missing_field_names:
            error = {
                "type": "fields_not_found",
                "message": "One or more requested fields were not found in the incoming envelope data.",
                "field_names": list(requested_field_names),
                "missing_field_names": missing_field_names,
            }
            status = "failed"
        return NodeExecutionResult(
            status=status,
            output=envelope.to_dict(),
            error=error,
            summary=summary,
            metadata=metadata,
        )

    def _supabase_data_request(self, context: NodeContext) -> SupabaseDataRequest:
        resolved = context.resolve_graph_env_value(dict(self.config))
        rpc_params_text = str(resolved.get("rpc_params_json", "{}") or "{}").strip() or "{}"
        try:
            rpc_params = json.loads(rpc_params_text)
        except json.JSONDecodeError as exc:
            raise SupabaseDataError(
                "Supabase rpc_params_json must be valid JSON.",
                error_type="invalid_supabase_rpc_params",
            ) from exc
        supabase_url_env_var, supabase_key_env_var = resolve_supabase_runtime_env_var_names(resolved, context.graph)
        return SupabaseDataRequest(
            supabase_url=resolve_graph_process_env(
                supabase_url_env_var,
                context.graph.env_vars,
            ),
            supabase_key=resolve_graph_process_env(
                supabase_key_env_var,
                context.graph.env_vars,
            ),
            schema=str(resolved.get("schema", "public") or "public").strip() or "public",
            source_kind=str(resolved.get("source_kind", "table") or "table").strip().lower() or "table",
            source_name=str(resolved.get("source_name", "") or "").strip(),
            select=str(resolved.get("select", "*") or "*").strip() or "*",
            filters_text=str(resolved.get("filters_text", "") or "").strip(),
            order_by=str(resolved.get("order_by", "") or "").strip(),
            order_desc=_coerce_bool(resolved.get("order_desc"), default=False),
            limit=_coerce_int(resolved.get("limit"), default=25, minimum=1),
            single_row=_coerce_bool(resolved.get("single_row"), default=False),
            output_mode=str(resolved.get("output_mode", "records") or "records").strip().lower() or "records",
            rpc_params=rpc_params if isinstance(rpc_params, dict) else {},
        )

    def _supabase_sql_source_value(self, context: NodeContext) -> Any:
        source_value = context.resolve_binding(self.config.get("input_binding"))
        source_value = _parse_json_object_or_array(source_value)
        if _is_message_envelope_like(source_value):
            source_value = _parse_json_object_or_array(source_value.get("payload"))
        return source_value

    def _resolve_supabase_sql_query_tokens(self, query: str, source_value: Any) -> tuple[str, list[Any]]:
        payload_lookup: Mapping[str, Any] = source_value if isinstance(source_value, Mapping) else {}
        parameters: list[Any] = []
        token_to_index: dict[str, int] = {}
        missing_tokens: list[str] = []

        def _replace(match: "re.Match[str]") -> str:
            key = str(match.group(1) or "")
            if key not in payload_lookup:
                if key not in missing_tokens:
                    missing_tokens.append(key)
                return match.group(0)
            if key not in token_to_index:
                parameters.append(payload_lookup[key])
                token_to_index[key] = len(parameters)
            return f"${token_to_index[key]}"

        parameterized_query = SUPABASE_SQL_TOKEN_PATTERN.sub(_replace, query)
        if missing_tokens:
            raise SupabaseDataError(
                f"Supabase SQL query references {{{missing_tokens[0]}}} but the incoming payload has no field named '{missing_tokens[0]}'.",
                error_type="missing_supabase_sql_parameter",
                details={"missing_fields": missing_tokens},
            )
        return parameterized_query, parameters

    def _supabase_sql_request(self, context: NodeContext) -> tuple[SupabaseSqlQueryRequest, Any]:
        resolved = context.resolve_graph_env_value(dict(self.config))
        source_value = self._supabase_sql_source_value(context)
        raw_query = str(resolved.get("query", "") or "").strip()
        parameterized_query, parameters = self._resolve_supabase_sql_query_tokens(raw_query, source_value)
        project_ref_env_var, access_token_env_var = resolve_supabase_management_runtime_env_var_names(resolved, context.graph)
        raw_management_api_base_url = str(resolved.get("management_api_base_url", "") or "").strip()
        management_api_base_url_env_var = resolve_graph_env_reference_name(
            raw_management_api_base_url,
            context.graph.env_vars,
            default="",
        )
        management_api_base_url = (
            str(os.environ.get(management_api_base_url_env_var, "") or "").strip()
            if management_api_base_url_env_var
            else ""
        ) or (
            resolve_graph_process_env(raw_management_api_base_url, context.graph.env_vars)
            if raw_management_api_base_url
            else ""
        ) or raw_management_api_base_url
        return (
            SupabaseSqlQueryRequest(
                project_ref=resolve_graph_process_env(
                    project_ref_env_var,
                    context.graph.env_vars,
                ),
                access_token=resolve_graph_process_env(
                    access_token_env_var,
                    context.graph.env_vars,
                ),
                query=parameterized_query,
                parameters=parameters,
                read_only=_coerce_bool(resolved.get("read_only"), default=True),
                output_mode=str(resolved.get("output_mode", "records") or "records").strip().lower() or "records",
                management_api_base_url=management_api_base_url,
            ),
            source_value,
        )

    def _supabase_row_write_source_value(self, context: NodeContext) -> Any:
        source_value = context.resolve_binding(self.config.get("input_binding"))
        source_value = _parse_json_object_or_array(source_value)
        if _is_message_envelope_like(source_value):
            source_value = _parse_json_object_or_array(source_value.get("payload"))
        return source_value

    def _supabase_row_write_mapping_specs(self, resolved_config: Mapping[str, Any]) -> dict[str, Any]:
        raw_mapping_text = str(resolved_config.get("column_values_json", "{}") or "{}").strip() or "{}"
        try:
            parsed = json.loads(raw_mapping_text)
        except json.JSONDecodeError as exc:
            raise SupabaseDataError(
                "Supabase column_values_json must be valid JSON.",
                error_type="invalid_supabase_row_mapping",
            ) from exc
        if not isinstance(parsed, Mapping):
            raise SupabaseDataError(
                "Supabase column_values_json must be a JSON object keyed by column name.",
                error_type="invalid_supabase_row_mapping",
            )
        mapping_specs: dict[str, Any] = {}
        for raw_key, raw_value in parsed.items():
            column_name = str(raw_key or "").strip()
            if not column_name:
                raise SupabaseDataError(
                    "Supabase column_values_json cannot contain empty column names.",
                    error_type="invalid_supabase_row_mapping",
                )
            mapping_specs[column_name] = raw_value
        return mapping_specs

    def _resolve_supabase_row_write_spec(
        self,
        column_name: str,
        spec: Any,
        *,
        source_value: Any,
        context: NodeContext,
    ) -> tuple[bool, Any]:
        if not isinstance(spec, Mapping):
            return True, spec

        reserved_keys = {"mode", "source", "path", "template", "value", "on_missing"}
        if not any(key in spec for key in reserved_keys):
            return True, dict(spec)

        mode = str(spec.get("mode", spec.get("source", "literal")) or "literal").strip().lower() or "literal"
        if mode in {"default", "omit"}:
            return False, None
        if mode == "null":
            return True, None
        if mode == "literal":
            return True, spec.get("value")
        if mode == "template":
            template = str(spec.get("template", spec.get("value", "")) or "")
            return True, context.render_template(template)
        if mode == "path":
            path = str(spec.get("path", "$") or "$").strip() or "$"
            found, resolved_value = _deep_get_with_presence(source_value, path)
            if found:
                return True, resolved_value
            on_missing = str(spec.get("on_missing", "error") or "error").strip().lower() or "error"
            if on_missing in {"omit", "default"}:
                return False, None
            if on_missing == "null":
                return True, None
            raise SupabaseDataError(
                f"Supabase row column '{column_name}' could not resolve path '{path}'.",
                error_type="missing_supabase_row_value",
                details={"column_name": column_name, "path": path},
            )
        raise SupabaseDataError(
            f"Supabase row column '{column_name}' uses unsupported mode '{mode}'.",
            error_type="invalid_supabase_row_mapping",
            details={"column_name": column_name},
        )

    def _supabase_row_write_payload(self, context: NodeContext) -> tuple[dict[str, Any], Any]:
        resolved = context.resolve_graph_env_value(dict(self.config))
        source_value = self._supabase_row_write_source_value(context)
        row_payload: dict[str, Any] = {}

        base_row_path = str(resolved.get("base_row_json_path", "") or "").strip()
        if base_row_path:
            found, base_row_value = _deep_get_with_presence(source_value, base_row_path)
            if not found:
                raise SupabaseDataError(
                    f"Supabase base_row_json_path '{base_row_path}' did not resolve to a value.",
                    error_type="missing_supabase_base_row",
                )
            if not isinstance(base_row_value, Mapping):
                raise SupabaseDataError(
                    "Supabase base_row_json_path must resolve to a JSON object.",
                    error_type="invalid_supabase_base_row",
                )
            row_payload = {str(key): value for key, value in base_row_value.items() if str(key).strip()}

        for column_name, spec in self._supabase_row_write_mapping_specs(resolved).items():
            include_value, resolved_value = self._resolve_supabase_row_write_spec(
                column_name,
                spec,
                source_value=source_value,
                context=context,
            )
            if include_value:
                row_payload[column_name] = resolved_value
            else:
                row_payload.pop(column_name, None)

        if not row_payload:
            raise SupabaseDataError(
                "Supabase row payload resolved to an empty object. Add at least one column value or map a base row object.",
                error_type="empty_supabase_row_payload",
            )
        return row_payload, source_value

    def _supabase_row_write_request(self, context: NodeContext) -> tuple[SupabaseRowWriteRequest, Any]:
        resolved = context.resolve_graph_env_value(dict(self.config))
        row_payload, source_value = self._supabase_row_write_payload(context)
        supabase_url_env_var, supabase_key_env_var = resolve_supabase_runtime_env_var_names(resolved, context.graph)
        return (
            SupabaseRowWriteRequest(
                supabase_url=resolve_graph_process_env(
                    supabase_url_env_var,
                    context.graph.env_vars,
                ),
                supabase_key=resolve_graph_process_env(
                    supabase_key_env_var,
                    context.graph.env_vars,
                ),
                schema=str(resolved.get("schema", "public") or "public").strip() or "public",
                table_name=str(resolved.get("table_name", "") or "").strip(),
                row=row_payload,
                write_mode=str(resolved.get("write_mode", "insert") or "insert").strip().lower() or "insert",
                on_conflict=str(resolved.get("on_conflict", "") or "").strip(),
                ignore_duplicates=_coerce_bool(resolved.get("ignore_duplicates"), default=False),
                returning=str(resolved.get("returning", "representation") or "representation").strip().lower() or "representation",
            ),
            source_value,
        )

    def _execute_supabase_row_write(self, context: NodeContext) -> NodeExecutionResult:
        try:
            request, source_value = self._supabase_row_write_request(context)
            result = write_supabase_row(request)
        except SupabaseDataError as exc:
            return NodeExecutionResult(
                status="failed",
                error=exc.to_error_payload(),
                summary=str(exc),
            )

        envelope = MessageEnvelope(
            schema_version="1.0",
            from_node_id=self.id,
            from_category=self.category.value,
            payload=result.payload,
            artifacts={
                "supabase_raw_payload": result.raw_payload,
                "supabase_request_url": result.request_url,
                "supabase_written_row": result.inserted_row,
                "supabase_write_source_value": source_value,
            },
            metadata={
                "contract": "data_envelope",
                "node_kind": self.kind,
                "data_mode": SUPABASE_ROW_WRITE_MODE,
                "provider_id": self.provider_id,
                "schema": result.schema,
                "table_name": result.table_name,
                "write_mode": result.write_mode,
                "returning": result.returning,
                "row_count": result.row_count,
                "written_columns": sorted(result.inserted_row.keys()),
            },
        )
        summary_count = "unknown rows" if result.row_count is None else f"{result.row_count} row(s)"
        return NodeExecutionResult(
            status="success",
            output=envelope.to_dict(),
            summary=f"Wrote {summary_count} to Supabase table '{result.table_name}'.",
            metadata=envelope.metadata,
        )

    def _execute_supabase_data(self, context: NodeContext) -> NodeExecutionResult:
        try:
            request = self._supabase_data_request(context)
            result = fetch_supabase_data(request)
        except SupabaseDataError as exc:
            return NodeExecutionResult(
                status="failed",
                error=exc.to_error_payload(),
                summary=str(exc),
            )

        envelope = MessageEnvelope(
            schema_version="1.0",
            from_node_id=self.id,
            from_category=self.category.value,
            payload=result.payload,
            artifacts={
                "supabase_raw_payload": result.raw_payload,
                "supabase_request_url": result.request_url,
            },
            metadata={
                "contract": "data_envelope",
                "node_kind": self.kind,
                "data_mode": SUPABASE_DATA_MODE,
                "provider_id": self.provider_id,
                "source_kind": result.source_kind,
                "source_name": result.source_name,
                "schema": result.schema,
                "row_count": result.row_count,
                "output_mode": result.output_mode,
            },
        )
        summary_count = "unknown rows" if result.row_count is None else f"{result.row_count} row(s)"
        return NodeExecutionResult(
            status="success",
            output=envelope.to_dict(),
            summary=f"Loaded {summary_count} from Supabase {result.source_kind} '{result.source_name}'.",
            metadata=envelope.metadata,
        )

    def _execute_supabase_sql(self, context: NodeContext) -> NodeExecutionResult:
        try:
            request, source_value = self._supabase_sql_request(context)
            result = execute_supabase_sql_query(request)
        except SupabaseDataError as exc:
            return NodeExecutionResult(
                status="failed",
                error=exc.to_error_payload(),
                summary=str(exc),
            )

        envelope = MessageEnvelope(
            schema_version="1.0",
            from_node_id=self.id,
            from_category=self.category.value,
            payload=result.payload,
            artifacts={
                "supabase_raw_payload": result.raw_payload,
                "supabase_request_url": result.request_url,
                "supabase_sql_query": result.query,
                "supabase_sql_parameters": result.parameters,
                "supabase_sql_source_value": source_value,
            },
            metadata={
                "contract": "data_envelope",
                "node_kind": self.kind,
                "data_mode": SUPABASE_SQL_MODE,
                "provider_id": self.provider_id,
                "read_only": result.read_only,
                "row_count": result.row_count,
                "output_mode": result.output_mode,
            },
        )
        summary_prefix = "Executed read-only" if result.read_only else "Executed"
        summary_count = "unknown rows" if result.row_count is None else f"{result.row_count} row(s)"
        return NodeExecutionResult(
            status="success",
            output=envelope.to_dict(),
            summary=f"{summary_prefix} Supabase SQL query returning {summary_count}.",
            metadata=envelope.metadata,
        )

    def runtime_input_preview(self, context: NodeContext) -> Any:
        mode = "passthrough" if bool(self.config.get("lock_passthrough", False)) else self.config.get("mode", "passthrough")
        if self.provider_id == SPREADSHEET_ROW_PROVIDER_ID:
            return self._spreadsheet_config(context)
        if self.provider_id == WRITE_TEXT_FILE_PROVIDER_ID or mode == "write_text_file":
            source_value = context.resolve_binding(self.config.get("input_binding"))
            if isinstance(source_value, Mapping) and "payload" in source_value:
                source_value = source_value.get("payload")
            is_loop_execution = context.is_loop_execution()
            resolved_exists_behavior = normalize_workspace_text_write_behavior(self.config.get("exists_behavior"))
            configured_relative_path = str(self.config.get("relative_path", "response.txt") or "response.txt").strip() or "response.txt"
            return {
                "relative_path": _resolve_write_text_file_relative_path(configured_relative_path, context=context),
                "configured_path": configured_relative_path,
                "workspace_dir": context.workspace_dir(create=True),
                "content_preview": _render_workspace_file_content(source_value),
                "exists_behavior": resolved_exists_behavior or "overwrite",
                "append_newline": bool(self.config.get("append_newline", True)),
                "loop_execution": is_loop_execution,
            }
        if self.provider_id == STRUCTURED_PAYLOAD_BUILDER_PROVIDER_ID or mode == STRUCTURED_PAYLOAD_BUILDER_MODE:
            resolved_config = self._structured_payload_builder_config(context)
            labeled_roots = self._structured_payload_builder_labeled_source_roots(context)
            source_roots = tuple(value for _, value in labeled_roots)
            source_root_kinds = tuple(kind for kind, _ in labeled_roots)
            preview: dict[str, Any] = {
                "case_sensitive": resolved_config.case_sensitive,
                "max_matches_per_field": resolved_config.max_matches_per_field,
                "default_search_section": resolved_config.default_search_section,
            }
            try:
                template = parse_structured_payload_template(resolved_config.template_json)
            except ValueError as exc:
                preview["error"] = str(exc)
                preview["template_json"] = resolved_config.template_json
                return preview
            result = build_structured_payload(
                template,
                source_roots,
                case_sensitive=resolved_config.case_sensitive,
                max_matches_per_field=resolved_config.max_matches_per_field,
                field_aliases=dict(resolved_config.field_aliases) if resolved_config.field_aliases else None,
                source_root_kinds=source_root_kinds,
                field_search_scopes=dict(resolved_config.field_search_scopes) if resolved_config.field_search_scopes else None,
                default_search_section=resolved_config.default_search_section,
            )
            preview["template"] = template
            preview["source_roots"] = list(source_roots)
            preview["source_root_kinds"] = list(source_root_kinds)
            preview["payload_preview"] = result.payload
            preview["filled_paths"] = list(result.filled_paths)
            preview["unresolved_paths"] = list(result.unresolved_paths)
            preview["field_matches"] = list(result.field_matches)
            if resolved_config.field_aliases:
                preview["field_aliases"] = {
                    field: list(aliases) for field, aliases in resolved_config.field_aliases
                }
            if resolved_config.field_search_scopes:
                preview["field_search_scopes"] = dict(resolved_config.field_search_scopes)
            return preview
        if self.provider_id == APOLLO_EMAIL_LOOKUP_PROVIDER_ID or mode == APOLLO_EMAIL_LOOKUP_MODE:
            resolved_config = self._apollo_email_lookup_config(context)
            source_value = self._apollo_email_lookup_source_value(context)
            request = self._apollo_email_lookup_request(
                context,
                resolved_config=resolved_config,
                source_value=source_value,
            )
            preview: dict[str, Any] = {
                "api_key_env_var": resolved_config["api_key_env_var"],
                "api_key_configured": bool(
                    resolve_graph_process_env(resolved_config["api_key_env_var"], context.graph_env_vars())
                ),
                "use_cache": resolved_config["use_cache"],
                "force_refresh": resolved_config["force_refresh"],
                "lookup": request.to_lookup_fields(),
                "conversation": resolved_config["conversation"],
            }
            request_error = validate_apollo_lookup_request(request)
            if request_error:
                preview["error"] = request_error
                return preview
            cache_info = build_apollo_email_lookup_cache_info(request)
            preview["cache_key"] = cache_info.cache_key
            preview["shared_cache_path"] = str(cache_info.shared_cache_path)
            cached_entry, shared_cache_file = (
                read_cached_apollo_email_lookup(cache_info) if resolved_config["use_cache"] else (None, None)
            )
            preview["cache_hit"] = cached_entry is not None and not resolved_config["force_refresh"]
            if shared_cache_file is not None:
                preview["shared_cache_file"] = shared_cache_file
            if isinstance(cached_entry, Mapping):
                preview["lookup_status"] = cached_entry.get("lookup_status")
                preview["resolved_email"] = cached_entry.get("resolved_email")
            workspace_relative_path = apollo_workspace_cache_relative_path(
                resolved_config["workspace_cache_path_template"],
                cache_key=cache_info.cache_key,
            )
            preview["workspace_cache_path"] = workspace_relative_path
            try:
                workspace_absolute_path, _ = context.resolve_workspace_path(workspace_relative_path)
                preview["workspace_cache_absolute_path"] = workspace_absolute_path
            except Exception as exc:  # noqa: BLE001
                preview["workspace_path_error"] = str(exc)
            return preview
        if self.provider_id == LINKEDIN_PROFILE_FETCH_PROVIDER_ID or mode == LINKEDIN_PROFILE_FETCH_MODE:
            resolved_config = self._linkedin_profile_fetch_config(context)
            source_value = self._linkedin_profile_fetch_source_value(context)
            preview: dict[str, Any] = {
                "url_field": resolved_config["url_field"],
                "use_cache": resolved_config["use_cache"],
                "force_refresh": resolved_config["force_refresh"],
                "headless": resolved_config["headless"],
                "navigation_timeout_ms": resolved_config["navigation_timeout_ms"],
                "page_settle_ms": resolved_config["page_settle_ms"],
                "linkedin_data_dir": resolved_config["linkedin_data_dir"],
                "session_state_path": resolved_config["session_state_path"],
            }
            source_url = extract_linkedin_profile_url(source_value, url_field=resolved_config["url_field"])
            if not source_url:
                preview["error"] = f"Expected a raw string URL or a '{resolved_config['url_field']}' field."
                return preview
            preview["source_url"] = source_url
            try:
                cache_info = build_linkedin_profile_cache_info(source_url)
            except LinkedInFetchError as exc:
                preview["error"] = str(exc)
                return preview
            preview["normalized_url"] = cache_info.normalized_url
            preview["cache_key"] = cache_info.cache_key
            preview["shared_cache_path"] = str(cache_info.shared_cache_path)
            cached_payload, shared_cache_file = read_cached_linkedin_profile(cache_info) if resolved_config["use_cache"] else (None, None)
            preview["cache_hit"] = cached_payload is not None and not resolved_config["force_refresh"]
            if shared_cache_file is not None:
                preview["shared_cache_file"] = shared_cache_file
            if isinstance(cached_payload, Mapping):
                preview["cached_profile_name"] = _deep_get(cached_payload, "person.name")
            workspace_relative_path = workspace_cache_relative_path(
                resolved_config["workspace_cache_path_template"],
                cache_key=cache_info.cache_key,
            )
            preview["workspace_cache_path"] = workspace_relative_path
            try:
                workspace_absolute_path, _ = context.resolve_workspace_path(workspace_relative_path)
                preview["workspace_cache_absolute_path"] = workspace_absolute_path
            except Exception as exc:  # noqa: BLE001
                preview["workspace_path_error"] = str(exc)
            return preview
        if self.provider_id == RUNTIME_NORMALIZER_PROVIDER_ID or mode == RUNTIME_NORMALIZER_MODE:
            return self._runtime_normalizer_source_value(context)
        if self.provider_id == SUPABASE_DATA_PROVIDER_ID or mode == SUPABASE_DATA_MODE:
            try:
                request = self._supabase_data_request(context)
            except SupabaseDataError as exc:
                return {"error": str(exc)}
            return {
                "source_kind": request.source_kind,
                "source_name": request.source_name,
                "schema": request.schema,
                "select": request.select,
                "filters_text": request.filters_text,
                "order_by": request.order_by,
                "order_desc": request.order_desc,
                "limit": request.limit,
                "single_row": request.single_row,
                "output_mode": request.output_mode,
                "supabase_url_present": bool(request.supabase_url),
                "supabase_key_present": bool(request.supabase_key),
                "rpc_params": request.rpc_params,
            }
        if self.provider_id == SUPABASE_SQL_PROVIDER_ID or mode == SUPABASE_SQL_MODE:
            try:
                request, source_value = self._supabase_sql_request(context)
            except SupabaseDataError as exc:
                return {"error": str(exc)}
            return {
                "query": request.query,
                "parameters_preview": request.parameters,
                "source_preview": source_value,
                "read_only": request.read_only,
                "output_mode": request.output_mode,
                "project_ref_present": bool(request.project_ref),
                "access_token_present": bool(request.access_token),
                "management_api_base_url": request.management_api_base_url,
            }
        if self.provider_id == SUPABASE_ROW_WRITE_PROVIDER_ID or mode == SUPABASE_ROW_WRITE_MODE:
            try:
                request, source_value = self._supabase_row_write_request(context)
            except SupabaseDataError as exc:
                return {"error": str(exc)}
            return {
                "schema": request.schema,
                "table_name": request.table_name,
                "write_mode": request.write_mode,
                "returning": request.returning,
                "on_conflict": request.on_conflict,
                "ignore_duplicates": request.ignore_duplicates,
                "row_preview": request.row,
                "source_preview": source_value,
                "supabase_url_present": bool(request.supabase_url),
                "supabase_key_present": bool(request.supabase_key),
            }
        if self.provider_id == OUTBOUND_EMAIL_LOGGER_PROVIDER_ID or mode == OUTBOUND_EMAIL_LOGGER_MODE:
            resolved = context.resolve_graph_env_value(dict(self.config))
            return {
                "schema": str(resolved.get("schema", "public") or "public").strip() or "public",
                "table_name": str(resolved.get("table_name", "") or "").strip(),
                "message_type": str(resolved.get("message_type", "initial") or "initial").strip().lower() or "initial",
                "outreach_step": _coerce_int(resolved.get("outreach_step"), default=0, minimum=0),
                "sales_approach": str(resolved.get("sales_approach", "") or "").strip(),
                "sales_approach_version": str(resolved.get("sales_approach_version", "") or "").strip(),
                "supabase_url_present": bool(
                    resolve_graph_process_env(
                        str(resolved.get("supabase_url_env_var", "GRAPH_AGENT_SUPABASE_URL") or "GRAPH_AGENT_SUPABASE_URL"),
                        context.graph.env_vars,
                    )
                ),
                "supabase_key_present": bool(
                    resolve_graph_process_env(
                        str(resolved.get("supabase_key_env_var", "GRAPH_AGENT_SUPABASE_SECRET_KEY") or "GRAPH_AGENT_SUPABASE_SECRET_KEY"),
                        context.graph.env_vars,
                    )
                ),
            }
        if self.provider_id == PROMPT_BLOCK_PROVIDER_ID or mode == PROMPT_BLOCK_MODE:
            return context.prompt_block_payload_for_node(self.id)
        if mode == "context_builder":
            preview_bindings: list[dict[str, Any]] = []
            for binding in self._context_builder_bindings(context):
                source_binding = binding.get("binding")
                value = context.resolve_binding(source_binding if isinstance(source_binding, Mapping) else None)
                if isinstance(value, Mapping) and "payload" in value:
                    value = value.get("payload")
                preview_bindings.append(
                    {
                        "source_node_id": str(binding.get("source_node_id", "")),
                        "header": str(binding.get("header", "")),
                        "placeholder": str(binding.get("placeholder", "")),
                        "value": _render_prompt_block_text(value) if _is_prompt_block_payload(value) else value,
                    }
                )
            return preview_bindings
        source_value, display_value = self._display_source_value(context)
        if bool(self.config.get("show_input_envelope", False)):
            return display_value
        return source_value

    def is_ready(self, context: NodeContext) -> bool:
        mode = "passthrough" if bool(self.config.get("lock_passthrough", False)) else self.config.get("mode", "passthrough")
        if mode == "context_builder":
            return self._is_context_builder_ready(context)
        return True

    def execute(self, context: NodeContext) -> NodeExecutionResult:
        mode = "passthrough" if bool(self.config.get("lock_passthrough", False)) else self.config.get("mode", "passthrough")
        if self.provider_id == SPREADSHEET_ROW_PROVIDER_ID:
            return self._execute_spreadsheet_rows(context)
        if self.provider_id == WRITE_TEXT_FILE_PROVIDER_ID or mode == "write_text_file":
            return self._execute_write_text_file(context)
        if self.provider_id == STRUCTURED_PAYLOAD_BUILDER_PROVIDER_ID or mode == STRUCTURED_PAYLOAD_BUILDER_MODE:
            return self._execute_structured_payload_builder(context)
        if self.provider_id == APOLLO_EMAIL_LOOKUP_PROVIDER_ID or mode == APOLLO_EMAIL_LOOKUP_MODE:
            return self._execute_apollo_email_lookup(context)
        if self.provider_id == PYTHON_SCRIPT_RUNNER_PROVIDER_ID or mode == PYTHON_SCRIPT_RUNNER_MODE:
            return self._execute_python_script_runner(context)
        if self.provider_id == LINKEDIN_PROFILE_FETCH_PROVIDER_ID or mode == LINKEDIN_PROFILE_FETCH_MODE:
            return self._execute_linkedin_profile_fetch(context)
        if self.provider_id == RUNTIME_NORMALIZER_PROVIDER_ID or mode == RUNTIME_NORMALIZER_MODE:
            return self._execute_runtime_normalizer(context)
        if self.provider_id == SUPABASE_DATA_PROVIDER_ID or mode == SUPABASE_DATA_MODE:
            return self._execute_supabase_data(context)
        if self.provider_id == SUPABASE_SQL_PROVIDER_ID or mode == SUPABASE_SQL_MODE:
            return self._execute_supabase_sql(context)
        if self.provider_id == SUPABASE_ROW_WRITE_PROVIDER_ID or mode == SUPABASE_ROW_WRITE_MODE:
            return self._execute_supabase_row_write(context)
        if self.provider_id == OUTBOUND_EMAIL_LOGGER_PROVIDER_ID or mode == OUTBOUND_EMAIL_LOGGER_MODE:
            resolved = context.resolve_graph_env_value(dict(self.config))
            return NodeExecutionResult(
                status="success",
                output={
                    "schema": str(resolved.get("schema", "public") or "public").strip() or "public",
                    "table_name": str(resolved.get("table_name", "") or "").strip(),
                    "message_type": str(resolved.get("message_type", "initial") or "initial").strip().lower() or "initial",
                },
                summary="Outbound email logger binding is available for Outlook draft nodes.",
                metadata={"binding_only": True, "outbound_email_logger": True},
            )
        if self.provider_id == PROMPT_BLOCK_PROVIDER_ID or mode == PROMPT_BLOCK_MODE:
            return self._execute_prompt_block(context)
        source_value, display_value = self._display_source_value(context)
        source_envelope: MessageEnvelope | None = None
        if isinstance(display_value, Mapping) and "metadata" in display_value:
            try:
                source_envelope = MessageEnvelope.from_dict(display_value)
            except Exception:  # noqa: BLE001
                source_envelope = None
        display_only = bool(self.config.get("show_input_envelope", False))
        if mode == "context_builder":
            return self._execute_context_builder(context)
        if mode == "template":
            payload = context.render_template(str(self.config.get("template", "{input_payload}")), {"source": source_value})
        else:
            payload = source_value
        artifacts: dict[str, Any] = {}
        if display_only:
            artifacts["display_envelope"] = display_value
        metadata = {
            "contract": "data_envelope",
            "node_kind": self.kind,
            "display_only": display_only,
        }
        errors: list[dict[str, Any]] = []
        tool_calls: list[dict[str, Any]] = []
        schema_version = "1.0"
        if source_envelope is not None:
            schema_version = source_envelope.schema_version
            artifacts = {**source_envelope.artifacts, **artifacts}
            errors = list(source_envelope.errors)
            tool_calls = list(source_envelope.tool_calls)
            metadata = {
                **dict(source_envelope.metadata),
                "contract": str(source_envelope.metadata.get("contract", "data_envelope")),
                "node_kind": self.kind,
                "display_only": display_only,
                **({"display_provider_id": self.provider_id} if display_only else {}),
            }
        envelope = MessageEnvelope(
            schema_version=schema_version,
            from_node_id=self.id,
            from_category=self.category.value,
            payload=payload,
            artifacts=artifacts,
            errors=errors,
            tool_calls=tool_calls,
            metadata=metadata,
        )
        return NodeExecutionResult(
            status="success",
            output=envelope.to_dict(),
            summary="Display node captured the upstream envelope." if display_only else "Data node completed.",
        )


class ControlFlowNode(BaseNode):
    kind = "control_flow_unit"

    def __init__(
        self,
        node_id: str,
        label: str,
        provider_id: str = SPREADSHEET_ROW_PROVIDER_ID,
        provider_label: str = "Control Flow Unit",
        description: str = "",
        config: Mapping[str, Any] | None = None,
        position: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(
            node_id=node_id,
            label=label,
            category=NodeCategory.CONTROL_FLOW_UNIT,
            provider_id=provider_id,
            provider_label=provider_label,
            description=description,
            config=config,
            position=position,
        )

    def _spreadsheet_config(self, context: NodeContext) -> dict[str, Any]:
        resolved = context.resolve_graph_env_value(dict(self.config))
        file_format = str(resolved.get("file_format", "auto") or "auto").strip().lower() or "auto"
        file_path = str(resolved.get("file_path", "") or "").strip()
        if not file_path:
            file_path = resolve_spreadsheet_path_from_run_documents(
                context.state.documents,
                run_document_id=str(resolved.get("run_document_id", "") or ""),
                run_document_name=str(resolved.get("run_document_name", "") or ""),
            )
        sheet_name = str(resolved.get("sheet_name", "") or "").strip()
        empty_row_policy = str(resolved.get("empty_row_policy", "skip") or "skip").strip().lower() or "skip"
        start_row_index = _coerce_start_row_index(resolved.get("start_row_index"))
        return {
            "file_format": file_format,
            "file_path": file_path,
            "sheet_name": sheet_name,
            "header_row_index": SPREADSHEET_HEADER_ROW_INDEX,
            "start_row_index": start_row_index,
            "empty_row_policy": empty_row_policy,
        }

    def _spreadsheet_iterator_state(self, parse_result: Any) -> dict[str, Any]:
        return {
            "iterator_type": "spreadsheet_rows",
            "status": "ready" if parse_result.row_count > 0 else "completed",
            "current_row_index": 0,
            "total_rows": parse_result.row_count,
            "headers": list(parse_result.headers),
            "sheet_name": parse_result.sheet_name,
            "source_file": parse_result.source_file,
            "file_format": parse_result.file_format,
        }

    def _spreadsheet_row_envelopes(self, parse_result: Any) -> list[dict[str, Any]]:
        row_envelopes: list[dict[str, Any]] = []
        total_rows = parse_result.row_count
        for position, row in enumerate(parse_result.rows, start=1):
            envelope = MessageEnvelope(
                schema_version="1.0",
                from_node_id=self.id,
                from_category=self.category.value,
                payload={
                    "row_index": position,
                    "row_number": row.row_number,
                    "row_data": dict(row.row_data),
                    "sheet_name": parse_result.sheet_name,
                    "source_file": parse_result.source_file,
                },
                metadata={
                    "contract": "data_envelope",
                    "node_kind": self.kind,
                    "data_mode": "spreadsheet_row",
                    "provider_id": self.provider_id,
                    "iterator_type": "spreadsheet_rows",
                    "row_index": position,
                    "row_number": row.row_number,
                    "total_rows": total_rows,
                    "headers": list(parse_result.headers),
                    "sheet_name": parse_result.sheet_name,
                    "source_file": parse_result.source_file,
                    "file_format": parse_result.file_format,
                },
            )
            row_envelopes.append(envelope.to_dict())
        return row_envelopes

    def _execute_spreadsheet_rows(self, context: NodeContext) -> NodeExecutionResult:
        resolved_config = self._spreadsheet_config(context)
        try:
            parse_result = parse_spreadsheet(**resolved_config)
        except SpreadsheetParseError as exc:
            return NodeExecutionResult(
                status="failed",
                error={"type": "spreadsheet_parse_error", "message": str(exc)},
                summary=str(exc),
            )
        iterator_state = self._spreadsheet_iterator_state(parse_result)
        summary_envelope = MessageEnvelope(
            schema_version="1.0",
            from_node_id=self.id,
            from_category=self.category.value,
            payload={
                "source_file": parse_result.source_file,
                "file_format": parse_result.file_format,
                "sheet_name": parse_result.sheet_name,
                "headers": list(parse_result.headers),
                "row_count": parse_result.row_count,
            },
            metadata={
                "contract": "data_envelope",
                "node_kind": self.kind,
                "control_flow_mode": "spreadsheet_rows",
                "provider_id": self.provider_id,
                "iterator_type": "spreadsheet_rows",
                "headers": list(parse_result.headers),
                "sheet_name": parse_result.sheet_name,
                "source_file": parse_result.source_file,
                "file_format": parse_result.file_format,
                "total_rows": parse_result.row_count,
            },
        )
        return NodeExecutionResult(
            status="success",
            output=summary_envelope.to_dict(),
            summary=f"Prepared {parse_result.row_count} spreadsheet row(s).",
            metadata={
                "iterator_state": iterator_state,
                "control_flow_handle_id": CONTROL_FLOW_LOOP_BODY_HANDLE_ID,
                "_internal": {
                    "iterator_envelopes": self._spreadsheet_row_envelopes(parse_result),
                    "iterator_type": "spreadsheet_rows",
                    "iterator_item_label": "spreadsheet row",
                    "iterator_handle_id": CONTROL_FLOW_LOOP_BODY_HANDLE_ID,
                },
            },
        )

    def _supabase_table_rows_config(self, context: NodeContext) -> dict[str, Any]:
        resolved = context.resolve_graph_env_value(dict(self.config))
        supabase_url_env_var, supabase_key_env_var = resolve_supabase_runtime_env_var_names(resolved, context.graph)
        return {
            "supabase_url": resolve_graph_process_env(supabase_url_env_var, context.graph.env_vars),
            "supabase_key": resolve_graph_process_env(supabase_key_env_var, context.graph.env_vars),
            "supabase_connection_id": str(resolved.get("supabase_connection_id", "") or "").strip(),
            "schema": str(resolved.get("schema", "public") or "public").strip() or "public",
            "table_name": str(resolved.get("table_name", "") or "").strip(),
            "select": (
                "*"
                if resolved.get("select") is None
                else str(resolved.get("select", "") or "").strip()
            ),
            "filters_text": str(resolved.get("filters_text", "") or "").strip(),
            "cursor_column": str(resolved.get("cursor_column", "") or "").strip(),
            "row_id_column": str(resolved.get("row_id_column", "id") or "id").strip() or "id",
            "page_size": _coerce_int(resolved.get("page_size"), default=500, minimum=1),
            "include_previously_processed_rows": _coerce_bool(
                resolved.get("include_previously_processed_rows"),
                default=False,
            ),
        }

    def _supabase_table_rows_scope(
        self,
        context: NodeContext,
        resolved_config: Mapping[str, Any],
    ) -> SupabaseTableRowsCursorScope:
        connection_identity = str(resolved_config.get("supabase_url", "") or "").strip()
        if not connection_identity:
            connection_identity = str(resolved_config.get("supabase_connection_id", "") or "").strip()
        return SupabaseTableRowsCursorScope(
            graph_id=context.graph.graph_id,
            agent_id=str(context.state.agent_id or ""),
            node_id=self.id,
            connection_identity=connection_identity,
            schema=str(resolved_config.get("schema", "public") or "public").strip() or "public",
            table_name=str(resolved_config.get("table_name", "") or "").strip(),
            filters_text=str(resolved_config.get("filters_text", "") or "").strip(),
            cursor_column=str(resolved_config.get("cursor_column", "") or "").strip(),
            row_id_column=str(resolved_config.get("row_id_column", "id") or "id").strip() or "id",
        )

    def _supabase_table_rows_request(
        self,
        context: NodeContext,
        *,
        resolved_config: Mapping[str, Any],
        watermark: SupabaseTableRowsWatermark | None,
    ) -> SupabaseTableRowsRequest:
        return SupabaseTableRowsRequest(
            supabase_url=str(resolved_config.get("supabase_url", "") or "").strip(),
            supabase_key=str(resolved_config.get("supabase_key", "") or "").strip(),
            schema=str(resolved_config.get("schema", "public") or "public").strip() or "public",
            table_name=str(resolved_config.get("table_name", "") or "").strip(),
            select=(
                "*"
                if resolved_config.get("select") is None
                else str(resolved_config.get("select", "") or "").strip()
            ),
            filters_text=str(resolved_config.get("filters_text", "") or "").strip(),
            cursor_column=str(resolved_config.get("cursor_column", "") or "").strip(),
            row_id_column=str(resolved_config.get("row_id_column", "id") or "id").strip() or "id",
            page_size=_coerce_int(resolved_config.get("page_size"), default=500, minimum=1),
            include_previously_processed_rows=_coerce_bool(
                resolved_config.get("include_previously_processed_rows"),
                default=False,
            ),
            last_cursor_value=(
                ""
                if _coerce_bool(resolved_config.get("include_previously_processed_rows"), default=False)
                else watermark.last_cursor_value if watermark is not None else ""
            ),
            last_row_id=(
                ""
                if _coerce_bool(resolved_config.get("include_previously_processed_rows"), default=False)
                else watermark.last_row_id if watermark is not None else ""
            ),
        )

    def _supabase_table_rows_iterator_state(
        self,
        result: Any,
        *,
        watermark: SupabaseTableRowsWatermark | None,
    ) -> dict[str, Any]:
        return {
            "iterator_type": "supabase_table_rows",
            "status": "ready" if result.row_count > 0 else "completed",
            "current_row_index": 0,
            "total_rows": result.row_count,
            "schema": result.schema,
            "table_name": result.table_name,
            "cursor_column": result.cursor_column,
            "row_id_column": result.row_id_column,
            "include_previously_processed_rows": result.include_previously_processed_rows,
            "last_cached_cursor_value": watermark.last_cursor_value if watermark is not None else None,
            "last_cached_row_id": watermark.last_row_id if watermark is not None else None,
        }

    def _supabase_table_row_envelopes(self, result: Any) -> list[dict[str, Any]]:
        row_envelopes: list[dict[str, Any]] = []
        total_rows = result.row_count
        for position, row in enumerate(result.rows, start=1):
            row_id = row.get(result.row_id_column)
            cursor_value = row.get(result.cursor_column)
            output_row_data = filter_supabase_table_row_output(row, result.select)
            envelope = MessageEnvelope(
                schema_version="1.0",
                from_node_id=self.id,
                from_category=self.category.value,
                payload={
                    "row_data": output_row_data,
                },
                metadata={
                    "contract": "data_envelope",
                    "node_kind": self.kind,
                    "data_mode": "supabase_table_row",
                    "provider_id": self.provider_id,
                    "iterator_type": "supabase_table_rows",
                    "row_index": position,
                    "row_id": row_id,
                    "cursor_column": result.cursor_column,
                    "cursor_value": cursor_value,
                    "row_id_column": result.row_id_column,
                    "total_rows": total_rows,
                    "schema": result.schema,
                    "table_name": result.table_name,
                },
            )
            row_envelopes.append(envelope.to_dict())
        return row_envelopes

    def _execute_supabase_table_rows(self, context: NodeContext) -> NodeExecutionResult:
        try:
            resolved_config = self._supabase_table_rows_config(context)
            scope = self._supabase_table_rows_scope(context, resolved_config)
            cursor_store = SupabaseTableRowsCursorStore()
            watermark = cursor_store.load_watermark(scope)
            request = self._supabase_table_rows_request(
                context,
                resolved_config=resolved_config,
                watermark=watermark,
            )
            result = materialize_supabase_table_rows(request)
        except SupabaseDataError as exc:
            return NodeExecutionResult(
                status="failed",
                error=exc.to_error_payload(),
                summary=str(exc),
            )

        iterator_state = self._supabase_table_rows_iterator_state(result, watermark=watermark)
        warnings: list[dict[str, Any]] = []
        if result.truncated:
            warnings.append(
                {
                    "type": "supabase_iterator_row_cap_reached",
                    "message": (
                        f"Supabase iterator stopped after {result.row_count} rows from table "
                        f"'{result.table_name}' (cap: {result.max_rows}). Increase "
                        "GRAPH_AGENT_SUPABASE_MAX_ITERATOR_ROWS or narrow filters/select."
                    ),
                    "table_name": result.table_name,
                    "schema": result.schema,
                    "row_count": result.row_count,
                    "max_rows": result.max_rows,
                    "include_previously_processed_rows": result.include_previously_processed_rows,
                }
            )
        summary_envelope = MessageEnvelope(
            schema_version="1.0",
            from_node_id=self.id,
            from_category=self.category.value,
            payload={
                "schema": result.schema,
                "table_name": result.table_name,
                "row_count": result.row_count,
                "cursor_column": result.cursor_column,
                "row_id_column": result.row_id_column,
                "include_previously_processed_rows": result.include_previously_processed_rows,
                "truncated": result.truncated,
                "max_rows": result.max_rows,
            },
            artifacts={
                "supabase_request_urls": list(result.request_urls),
            },
            metadata={
                "contract": "data_envelope",
                "node_kind": self.kind,
                "control_flow_mode": SUPABASE_TABLE_ROWS_MODE,
                "provider_id": self.provider_id,
                "iterator_type": "supabase_table_rows",
                "schema": result.schema,
                "table_name": result.table_name,
                "cursor_column": result.cursor_column,
                "row_id_column": result.row_id_column,
                "include_previously_processed_rows": result.include_previously_processed_rows,
                "total_rows": result.row_count,
                "truncated": result.truncated,
                "max_rows": result.max_rows,
                "warnings": warnings,
            },
        )

        def _mark_completed() -> None:
            cursor_store.mark_completed(
                scope=scope,
                last_cursor_value=result.last_cursor_value,
                last_row_id=result.last_row_id,
                run_id=context.state.run_id,
            )

        summary_text = (
            f"Prepared {result.row_count} Supabase row(s) from table '{result.table_name}'"
            + (f" (capped at {result.max_rows})." if result.truncated else ".")
        )
        return NodeExecutionResult(
            status="success",
            output=summary_envelope.to_dict(),
            summary=summary_text,
            metadata={
                "iterator_state": iterator_state,
                "control_flow_handle_id": CONTROL_FLOW_LOOP_BODY_HANDLE_ID,
                "warnings": warnings,
                "_internal": {
                    "iterator_envelopes": self._supabase_table_row_envelopes(result),
                    "iterator_type": "supabase_table_rows",
                    "iterator_item_label": "Supabase row",
                    "iterator_handle_id": CONTROL_FLOW_LOOP_BODY_HANDLE_ID,
                    "iterator_on_completed": _mark_completed,
                },
            },
        )

    def _source_envelope(self, context: NodeContext) -> MessageEnvelope:
        source_value = context.resolve_binding(self.config.get("input_binding"))
        if isinstance(source_value, Mapping) and "schema_version" in source_value and "payload" in source_value:
            return MessageEnvelope.from_dict(source_value)
        return MessageEnvelope(
            schema_version="1.0",
            from_node_id="",
            from_category="",
            payload=source_value,
            metadata={"contract": "data_envelope", "node_kind": self.kind},
        )

    def _logic_clauses(self) -> list[dict[str, Any]]:
        raw_clauses = self.config.get("clauses", [])
        if not isinstance(raw_clauses, Sequence) or isinstance(raw_clauses, (str, bytes)):
            return []
        clauses: list[dict[str, Any]] = []
        for index, raw_clause in enumerate(raw_clauses):
            if not isinstance(raw_clause, Mapping):
                continue
            raw_source_contracts = raw_clause.get("source_contracts", [])
            source_contracts = (
                [str(contract).strip() for contract in raw_source_contracts if str(contract).strip()]
                if isinstance(raw_source_contracts, Sequence) and not isinstance(raw_source_contracts, (str, bytes))
                else []
            )
            clauses.append(
                {
                    "id": str(raw_clause.get("id", f"clause-{index + 1}")).strip() or f"clause-{index + 1}",
                    "label": str(raw_clause.get("label", "If")).strip() or "If",
                    "path": None if raw_clause.get("path") in {None, ""} else str(raw_clause.get("path")),
                    "operator": str(raw_clause.get("operator", "equals") or "equals").strip(),
                    "value": raw_clause.get("value"),
                    "source_contracts": source_contracts,
                    "output_handle_id": str(
                        raw_clause.get("output_handle_id", CONTROL_FLOW_IF_HANDLE_ID) or CONTROL_FLOW_IF_HANDLE_ID
                    ).strip()
                    or CONTROL_FLOW_IF_HANDLE_ID,
                }
            )
        return clauses

    def _logic_branches(self) -> list[dict[str, Any]]:
        raw_branches = self.config.get("branches", [])
        if isinstance(raw_branches, Sequence) and not isinstance(raw_branches, (str, bytes)):
            branches: list[dict[str, Any]] = []
            for index, raw_branch in enumerate(raw_branches):
                if not isinstance(raw_branch, Mapping):
                    continue
                branch_id = str(raw_branch.get("id", f"branch-{index + 1}")).strip() or f"branch-{index + 1}"
                branch_label = str(raw_branch.get("label", f"Branch {index + 1}"))
                output_handle_id = (
                    str(raw_branch.get("output_handle_id", CONTROL_FLOW_IF_HANDLE_ID) or CONTROL_FLOW_IF_HANDLE_ID).strip()
                    or CONTROL_FLOW_IF_HANDLE_ID
                )
                branches.append(
                    {
                        "id": branch_id,
                        "label": branch_label,
                        "output_handle_id": output_handle_id,
                        "root_group": self._normalize_logic_group(raw_branch.get("root_group"), fallback_id=f"group-{branch_id}"),
                    }
                )
            if branches:
                return branches

        branches = []
        for clause in self._logic_clauses():
            branch_id = str(clause.get("id", "clause")).strip() or "clause"
            branches.append(
                {
                    "id": branch_id,
                    "label": clause.get("label", ""),
                    "output_handle_id": clause.get("output_handle_id", CONTROL_FLOW_IF_HANDLE_ID),
                    "root_group": {
                        "id": f"group-{branch_id}",
                        "type": "group",
                        "combinator": "all",
                        "negated": False,
                        "children": [
                            {
                                "id": f"rule-{branch_id}",
                                "type": "rule",
                                "path": clause.get("path"),
                                "operator": clause.get("operator", "equals"),
                                "value": clause.get("value"),
                                "source_contracts": list(clause.get("source_contracts", [])),
                            }
                        ],
                    },
                }
            )
        return branches

    def _normalize_logic_group(self, candidate: Any, fallback_id: str) -> dict[str, Any]:
        if not isinstance(candidate, Mapping):
            return {
                "id": fallback_id,
                "type": "group",
                "combinator": "all",
                "negated": False,
                "children": [self._normalize_logic_rule({}, fallback_id=f"{fallback_id}-rule-1")],
            }
        raw_children = candidate.get("children", [])
        children = []
        if isinstance(raw_children, Sequence) and not isinstance(raw_children, (str, bytes)):
            for index, child in enumerate(raw_children):
                if isinstance(child, Mapping) and child.get("type") == "group":
                    children.append(self._normalize_logic_group(child, fallback_id=f"{fallback_id}-group-{index + 1}"))
                else:
                    children.append(self._normalize_logic_rule(child, fallback_id=f"{fallback_id}-rule-{index + 1}"))
        return {
            "id": str(candidate.get("id", fallback_id)).strip() or fallback_id,
            "type": "group",
            "combinator": "any" if candidate.get("combinator") == "any" else "all",
            "negated": candidate.get("negated") is True,
            "children": children or [self._normalize_logic_rule({}, fallback_id=f"{fallback_id}-rule-1")],
        }

    def _normalize_logic_rule(self, candidate: Any, fallback_id: str) -> dict[str, Any]:
        record = candidate if isinstance(candidate, Mapping) else {}
        raw_source_contracts = record.get("source_contracts", [])
        source_contracts = (
            [str(contract).strip() for contract in raw_source_contracts if str(contract).strip()]
            if isinstance(raw_source_contracts, Sequence) and not isinstance(raw_source_contracts, (str, bytes))
            else []
        )
        return {
            "id": str(record.get("id", fallback_id)).strip() or fallback_id,
            "type": "rule",
            "path": None if record.get("path") in {None, ""} else str(record.get("path")),
            "operator": str(record.get("operator", "equals") or "equals").strip(),
            "value": record.get("value"),
            "source_contracts": source_contracts,
        }

    def _evaluate_logic_rule(self, payload: Any, rule: Mapping[str, Any], incoming_contract: str) -> tuple[bool, dict[str, Any]]:
        source_contracts = rule.get("source_contracts", [])
        normalized_contracts: list[str] = []
        if isinstance(source_contracts, Sequence) and not isinstance(source_contracts, (str, bytes)):
            normalized_contracts = [str(contract).strip() for contract in source_contracts if str(contract).strip()]
            if normalized_contracts and incoming_contract not in normalized_contracts:
                return (
                    False,
                    {
                        "id": rule.get("id"),
                        "type": "rule",
                        "path": rule.get("path"),
                        "operator": rule.get("operator"),
                        "expected_value": rule.get("value"),
                        "actual_value": None,
                        "matched": False,
                        "skipped_for_contract": True,
                        "incoming_contract": incoming_contract,
                        "source_contracts": normalized_contracts,
                    },
                )
        actual_value = _deep_get(payload, rule.get("path")) if rule.get("path") not in {None, "", "$"} else payload
        operator = str(rule.get("operator", "equals") or "equals").strip()
        expected_value = rule.get("value")
        matched = False
        if operator == "exists":
            matched = actual_value is not None
        elif operator == "equals":
            matched = actual_value == expected_value
        elif operator == "not_equals":
            matched = actual_value != expected_value
        elif operator == "contains":
            if isinstance(actual_value, str):
                matched = str(expected_value) in actual_value
            elif isinstance(actual_value, Sequence) and not isinstance(actual_value, (str, bytes)):
                matched = expected_value in actual_value
            else:
                matched = False
        elif operator == "gt":
            comparable_actual = _coerce_logic_order_operand(actual_value)
            comparable_expected = _coerce_logic_order_operand(expected_value)
            try:
                matched = comparable_actual is not None and comparable_expected is not None and comparable_actual > comparable_expected
            except TypeError:
                matched = False
        elif operator == "gte":
            comparable_actual = _coerce_logic_order_operand(actual_value)
            comparable_expected = _coerce_logic_order_operand(expected_value)
            try:
                matched = comparable_actual is not None and comparable_expected is not None and comparable_actual >= comparable_expected
            except TypeError:
                matched = False
        elif operator == "lt":
            comparable_actual = _coerce_logic_order_operand(actual_value)
            comparable_expected = _coerce_logic_order_operand(expected_value)
            try:
                matched = comparable_actual is not None and comparable_expected is not None and comparable_actual < comparable_expected
            except TypeError:
                matched = False
        elif operator == "lte":
            comparable_actual = _coerce_logic_order_operand(actual_value)
            comparable_expected = _coerce_logic_order_operand(expected_value)
            try:
                matched = comparable_actual is not None and comparable_expected is not None and comparable_actual <= comparable_expected
            except TypeError:
                matched = False
        return (
            matched,
            {
                "id": rule.get("id"),
                "type": "rule",
                "path": rule.get("path"),
                "operator": operator,
                "expected_value": expected_value,
                "actual_value": actual_value,
                "matched": matched,
                "skipped_for_contract": False,
                "incoming_contract": incoming_contract,
                "source_contracts": normalized_contracts,
            },
        )

    def _evaluate_logic_group(self, payload: Any, group: Mapping[str, Any], incoming_contract: str) -> tuple[bool, dict[str, Any]]:
        child_evaluations: list[dict[str, Any]] = []
        child_matches: list[bool] = []
        raw_children = group.get("children", [])
        if isinstance(raw_children, Sequence) and not isinstance(raw_children, (str, bytes)):
            for child in raw_children:
                if isinstance(child, Mapping) and child.get("type") == "group":
                    matched, evaluation = self._evaluate_logic_group(payload, child, incoming_contract)
                else:
                    matched, evaluation = self._evaluate_logic_rule(payload, child if isinstance(child, Mapping) else {}, incoming_contract)
                child_matches.append(matched)
                child_evaluations.append(evaluation)
        combinator = "any" if group.get("combinator") == "any" else "all"
        matched = any(child_matches) if combinator == "any" else all(child_matches)
        if group.get("negated") is True:
            matched = not matched
        return (
            matched,
            {
                "id": group.get("id"),
                "type": "group",
                "combinator": combinator,
                "negated": group.get("negated") is True,
                "matched": matched,
                "children": child_evaluations,
            },
        )

    def _flatten_logic_evaluations(self, evaluation: Mapping[str, Any]) -> list[dict[str, Any]]:
        evaluation_type = str(evaluation.get("type", "rule") or "rule").strip()
        if evaluation_type != "group":
            return [dict(evaluation)]
        flattened: list[dict[str, Any]] = []
        raw_children = evaluation.get("children", [])
        if isinstance(raw_children, Sequence) and not isinstance(raw_children, (str, bytes)):
            for child in raw_children:
                if isinstance(child, Mapping):
                    flattened.extend(self._flatten_logic_evaluations(child))
        return flattened

    def _evaluate_logic_clause(self, payload: Any, clause: Mapping[str, Any], incoming_contract: str) -> tuple[bool, dict[str, Any]]:
        return self._evaluate_logic_rule(payload, clause, incoming_contract)

    def _execute_parallel_splitter(self, context: NodeContext) -> NodeExecutionResult:
        source_envelope = self._source_envelope(context)
        incoming_contract = str(source_envelope.metadata.get("contract", "") or "").strip() or "data_envelope"
        forwarded_envelope = MessageEnvelope(
            schema_version=source_envelope.schema_version,
            from_node_id=self.id,
            from_category=self.category.value,
            payload=source_envelope.payload,
            artifacts=dict(source_envelope.artifacts),
            errors=list(source_envelope.errors),
            tool_calls=list(source_envelope.tool_calls),
            metadata={
                **dict(source_envelope.metadata),
                "contract": incoming_contract,
                "node_kind": self.kind,
                "control_flow_mode": "parallel_splitter",
                "fan_out_parallel": True,
            },
        )
        return NodeExecutionResult(
            status="success",
            output=forwarded_envelope.to_dict(),
            summary="Parallel splitter forwarded the incoming envelope to all downstream branches.",
            metadata={
                "control_flow_mode": "parallel_splitter",
                "fan_out_parallel": True,
                "incoming_contract": incoming_contract,
            },
        )

    def _execute_logic_conditions(self, context: NodeContext) -> NodeExecutionResult:
        source_envelope = self._source_envelope(context)
        incoming_contract = str(source_envelope.metadata.get("contract", "") or "").strip()
        matched_branch: dict[str, Any] | None = None
        clause_evaluations: list[dict[str, Any]] = []
        branch_evaluations: list[dict[str, Any]] = []
        for branch in self._logic_branches():
            matched, evaluation = self._evaluate_logic_group(source_envelope.payload, branch.get("root_group", {}), incoming_contract)
            clause_evaluations.extend(self._flatten_logic_evaluations(evaluation))
            branch_evaluations.append(
                {
                    "id": branch.get("id"),
                    "label": branch.get("label"),
                    "output_handle_id": branch.get("output_handle_id"),
                    "matched": matched,
                    "trace": evaluation,
                }
            )
            if matched:
                matched_branch = branch
                break
        selected_handle_id = (
            str(matched_branch.get("output_handle_id", CONTROL_FLOW_IF_HANDLE_ID))
            if matched_branch is not None
            else str(self.config.get("else_output_handle_id", CONTROL_FLOW_ELSE_HANDLE_ID) or CONTROL_FLOW_ELSE_HANDLE_ID)
        ).strip() or CONTROL_FLOW_ELSE_HANDLE_ID
        forwarded_envelope = MessageEnvelope(
            schema_version=source_envelope.schema_version,
            from_node_id=self.id,
            from_category=self.category.value,
            payload=source_envelope.payload,
            artifacts=dict(source_envelope.artifacts),
            errors=list(source_envelope.errors),
            tool_calls=list(source_envelope.tool_calls),
            metadata={
                **dict(source_envelope.metadata),
                "contract": incoming_contract or str(source_envelope.metadata.get("contract", "data_envelope")),
                "node_kind": self.kind,
                "control_flow_mode": "logic_conditions",
                "selected_handle_id": selected_handle_id,
                "matched_branch_id": matched_branch.get("id") if matched_branch is not None else None,
                "matched_branch_label": matched_branch.get("label") if matched_branch is not None else "Else",
                "matched_clause_id": matched_branch.get("id") if matched_branch is not None else None,
                "matched_clause_label": matched_branch.get("label") if matched_branch is not None else "Else",
                "condition_evaluations": clause_evaluations,
                "branch_evaluations": branch_evaluations,
            },
        )
        route_output = forwarded_envelope.to_dict()
        return NodeExecutionResult(
            status="success",
            output=route_output,
            summary=(
                f"Logic conditions matched '{matched_branch.get('label', 'if')}'."
                if matched_branch is not None
                else "Logic conditions fell through to else."
            ),
            metadata={
                "control_flow_mode": "logic_conditions",
                "control_flow_handle_id": selected_handle_id,
                "incoming_contract": incoming_contract or None,
                "matched_branch_id": matched_branch.get("id") if matched_branch is not None else None,
                "matched_clause_id": matched_branch.get("id") if matched_branch is not None else None,
                "condition_evaluations": clause_evaluations,
                "branch_evaluations": branch_evaluations,
            },
            route_outputs={selected_handle_id: route_output},
        )

    def runtime_input_preview(self, context: NodeContext) -> Any:
        if self.provider_id == SPREADSHEET_ROW_PROVIDER_ID:
            return self._spreadsheet_config(context)
        if self.provider_id == SUPABASE_TABLE_ROWS_PROVIDER_ID:
            resolved_config = self._supabase_table_rows_config(context)
            scope = self._supabase_table_rows_scope(context, resolved_config)
            watermark = SupabaseTableRowsCursorStore().load_watermark(scope)
            return {
                "schema": resolved_config["schema"],
                "table_name": resolved_config["table_name"],
                "select": resolved_config["select"],
                "filters_text": resolved_config["filters_text"],
                "cursor_column": resolved_config["cursor_column"],
                "row_id_column": resolved_config["row_id_column"],
                "page_size": resolved_config["page_size"],
                "include_previously_processed_rows": resolved_config["include_previously_processed_rows"],
                "supabase_url_present": bool(resolved_config["supabase_url"]),
                "supabase_key_present": bool(resolved_config["supabase_key"]),
                "watermark": None
                if watermark is None
                else {
                    "last_cursor_value": watermark.last_cursor_value,
                    "last_row_id": watermark.last_row_id,
                    "updated_at": watermark.updated_at,
                },
            }
        source_envelope = self._source_envelope(context)
        return {
            "incoming_contract": source_envelope.metadata.get("contract"),
            "payload": source_envelope.payload,
        }

    def execute(self, context: NodeContext) -> NodeExecutionResult:
        if self.provider_id == SPREADSHEET_ROW_PROVIDER_ID:
            return self._execute_spreadsheet_rows(context)
        if self.provider_id == SUPABASE_TABLE_ROWS_PROVIDER_ID:
            return self._execute_supabase_table_rows(context)
        if self.provider_id == PARALLEL_SPLITTER_PROVIDER_ID:
            return self._execute_parallel_splitter(context)
        if self.provider_id == LOGIC_CONDITIONS_PROVIDER_ID:
            return self._execute_logic_conditions(context)
        envelope = self._source_envelope(context)
        return NodeExecutionResult(
            status="success",
            output=envelope.to_dict(),
            summary="Control flow node forwarded the incoming envelope.",
        )


class ProviderNode(BaseNode):
    kind = "provider"

    def __init__(
        self,
        node_id: str,
        label: str,
        provider_name: str,
        provider_id: str = "provider.generic",
        provider_label: str = "Generic Model Provider",
        description: str = "",
        config: Mapping[str, Any] | None = None,
        position: Mapping[str, Any] | None = None,
    ) -> None:
        merged_config = {"provider_name": provider_name, **dict(config or {})}
        super().__init__(
            node_id=node_id,
            label=label,
            category=NodeCategory.PROVIDER,
            provider_id=provider_id,
            provider_label=provider_label,
            description=description,
            config=merged_config,
            position=position,
        )
        self.provider_name = provider_name

    def execute(self, context: NodeContext) -> NodeExecutionResult:
        return NodeExecutionResult(
            status="success",
            output={
                "provider_name": self.provider_name,
                "provider_id": self.provider_id,
                "provider_label": self.provider_label,
                "config": dict(self.config),
            },
            summary=f"Provider node '{self.label}' is available for API node bindings.",
            metadata={"provider_name": self.provider_name, "binding_only": True},
        )

    def to_dict(self) -> dict[str, Any]:
        payload = super().to_dict()
        payload["model_provider_name"] = self.provider_name
        return payload


class ModelNode(BaseNode):
    kind = "model"

    _RUNTIME_CONFIG_KEYS = {
        "allowed_tool_names",
        "metadata_bindings",
        "mode",
        "prompt_block_node_ids",
        "preferred_tool_name",
        "prompt_name",
        "provider_config",
        "provider_name",
        "response_mode",
        "response_schema",
        "response_schema_text",
        "system_prompt",
        "tool_target_node_ids",
        "user_message_template",
    }

    def __init__(
        self,
        node_id: str,
        label: str,
        provider_name: str,
        prompt_name: str,
        node_provider_id: str = "model.generic",
        node_provider_label: str = "Generic Model Node",
        description: str = "",
        config: Mapping[str, Any] | None = None,
        position: Mapping[str, Any] | None = None,
    ) -> None:
        merged_config = {"provider_name": provider_name, "prompt_name": prompt_name, **dict(config or {})}
        super().__init__(
            node_id=node_id,
            label=label,
            category=NodeCategory.API,
            provider_id=node_provider_id,
            provider_label=node_provider_label,
            description=description,
            config=merged_config,
            position=position,
        )
        self.provider_name = provider_name
        self.prompt_name = prompt_name

    def _prompt_only_mcp_tool_decision_contract(
        self,
        *,
        mcp_tool_context: Mapping[str, Any] | None,
        mcp_available_tool_names: Sequence[str],
    ) -> str:
        if mcp_tool_context is None or mcp_available_tool_names:
            return ""
        return "\n".join(
            [
                "MCP Tool Decision Output",
                "When MCP tool metadata is present in prompt context but no MCP tools are directly callable, you must respond using this exact structure:",
                "",
                "Uses Tool: True|False",
                'Tool Call Schema: {"tool_name":"<tool name>","arguments":{...}} or NA',
                "DELIMITER",
                "<Explain why the tool schema is needed or why no tool is needed, and describe the next step required to finish the user's request.>",
                "",
                "Rules",
                "- Set `Uses Tool` to `True` only when one of the tools described in the MCP Tool Context is required.",
                "- When `Uses Tool` is `True`, `Tool Call Schema` must be a single JSON object containing exactly `tool_name` and `arguments`.",
                "- When `Uses Tool` is `False`, `Tool Call Schema` must be `NA`.",
                "- Do not claim that you already called a tool unless you were given an actual tool result.",
                "- The content after `DELIMITER` must be plain-language guidance for the next processing step.",
            ]
        )

    def _mcp_tool_guidance_block(
        self,
        *,
        mcp_available_tool_names: Sequence[str],
        mcp_tool_guidance: str,
    ) -> str:
        guidance_lines: list[str] = []
        if mcp_available_tool_names:
            guidance_lines = [
                "MCP Tool Guidance",
                "Use MCP tools only when a listed live capability is needed to answer the request or complete the task.",
                "Call only MCP tools that are explicitly exposed to you and follow their schemas exactly.",
                "Do not invent MCP tool names or arguments.",
                "If no exposed MCP tool is necessary, continue without calling one.",
            ]
        if mcp_tool_guidance:
            if guidance_lines:
                guidance_lines.extend(["", "Connected MCP Tool Notes:", mcp_tool_guidance])
            else:
                guidance_lines = ["MCP Tool Guidance", mcp_tool_guidance]
        return "\n".join(guidance_lines).strip()

    def _build_request(self, context: NodeContext, bound_provider_node: ProviderNode | None = None) -> ModelRequest:
        if bound_provider_node is None:
            bound_provider_node = context.bound_provider_node(self.id)
        metadata_bindings = dict(self.config.get("metadata_bindings", {}))
        metadata: dict[str, Any] = {}
        for key, binding in metadata_bindings.items():
            if isinstance(binding, Mapping):
                metadata[key] = context.resolve_binding(binding)
            else:
                metadata[key] = binding
        for key, value in context.context_builder_section_variables_for_current_input().items():
            metadata.setdefault(key, value)
        mcp_tool_context = context.mcp_tool_context_for_model(self.id)
        if mcp_tool_context is not None and "mcp_tool_context" not in metadata:
            metadata["mcp_tool_context"] = mcp_tool_context
            metadata["mcp_tool_context_prompt"] = mcp_tool_context.get("rendered_prompt_text", "")
        if "mcp_tool_guidance" not in metadata:
            metadata["mcp_tool_guidance"] = ""
        if mcp_tool_context is not None and not str(metadata.get("mcp_tool_guidance", "") or "").strip():
            metadata["mcp_tool_guidance"] = str(mcp_tool_context.get("usage_hints_text", "") or "")
        if mcp_tool_context is not None:
            for placeholder_block in mcp_tool_context.get("placeholder_blocks", []):
                if not isinstance(placeholder_block, Mapping):
                    continue
                token = str(placeholder_block.get("token", "") or "").strip()
                if not token or token in metadata:
                    continue
                metadata[token] = str(placeholder_block.get("prompt_text", "") or "")
        prompt_block_payloads = context.prompt_block_payloads_for_node(self.id)
        if prompt_block_payloads and "prompt_blocks" not in metadata:
            metadata["prompt_blocks"] = prompt_block_payloads
        allowed_tool_names = list(self.config.get("allowed_tool_names", []))
        available_tool_payloads = context.available_tool_definitions(allowed_tool_names)
        for tool_definition in context.mcp_tool_definitions_for_model(self.id):
            tool_name = str(tool_definition.get("name", "")).strip()
            if not tool_name:
                continue
            if any(str(existing.get("name", "")).strip() == tool_name for existing in available_tool_payloads):
                continue
            available_tool_payloads.append(tool_definition)
        available_tools = [
            ModelToolDefinition(
                name=str(tool.get("name", "")),
                description=str(tool.get("description", "")),
                input_schema=dict(tool.get("input_schema", {})),
            )
            for tool in available_tool_payloads
            if isinstance(tool, Mapping) and isinstance(tool.get("name"), str) and isinstance(tool.get("input_schema"), Mapping)
        ]
        response_mode = infer_model_response_mode(context.graph, self)
        mcp_available_tool_names = sorted(
            {
                str(tool.get("name", "")).strip()
                for tool in available_tool_payloads
                if str(tool.get("source_type", "")).strip() == "mcp" and str(tool.get("name", "")).strip()
            }
        )
        preferred_tool_name = str(self.config.get("preferred_tool_name", "") or "").strip()
        resolved_preferred_tool_name = (
            context.services.tool_registry.canonical_name_for(preferred_tool_name) if preferred_tool_name else None
        )
        metadata["available_tools"] = available_tool_payloads
        metadata["mcp_available_tool_names"] = mcp_available_tool_names
        metadata["mode"] = self.config.get("mode", self.prompt_name)
        metadata["preferred_tool_name"] = resolved_preferred_tool_name
        metadata["response_mode"] = response_mode
        system_prompt_template = str(self.config.get("system_prompt", ""))
        user_template = str(self.config.get("user_message_template", "{input_payload}"))
        mcp_tool_guidance = str(metadata.get("mcp_tool_guidance", "") or "").strip()
        mcp_tool_prompt = str(metadata.get("mcp_tool_context_prompt", "") or "").strip()
        mcp_tool_guidance_block = self._mcp_tool_guidance_block(
            mcp_available_tool_names=mcp_available_tool_names,
            mcp_tool_guidance=mcp_tool_guidance,
        )
        mcp_tool_context_block = f"MCP Tool Context\n{mcp_tool_prompt}" if mcp_tool_prompt else ""
        metadata.setdefault("mcp_tool_guidance_block", mcp_tool_guidance_block)
        metadata.setdefault("mcp_tool_context_block", mcp_tool_context_block)
        placeholder_tokens = [
            str(placeholder_block.get("token", "") or "").strip()
            for placeholder_block in (mcp_tool_context or {}).get("placeholder_blocks", [])
            if isinstance(placeholder_block, Mapping) and str(placeholder_block.get("token", "") or "").strip()
        ]
        has_inline_mcp_context_placeholder = "{mcp_tool_context_prompt}" in system_prompt_template or "{mcp_tool_context_block}" in system_prompt_template
        has_complete_inline_mcp_tool_placeholders = bool(placeholder_tokens) and all(
            f"{{{token}}}" in system_prompt_template for token in placeholder_tokens
        )
        has_inline_mcp_guidance_block = "{mcp_tool_guidance_block}" in system_prompt_template
        system_prompt = context.render_template(system_prompt_template, metadata)
        mcp_prompt_sections: list[str] = []
        if mcp_tool_guidance_block and not has_inline_mcp_guidance_block:
            mcp_prompt_sections.append(mcp_tool_guidance_block)
        if mcp_tool_context_block and not (has_inline_mcp_context_placeholder or has_complete_inline_mcp_tool_placeholders):
            mcp_prompt_sections.append(mcp_tool_context_block)
        prompt_only_tool_contract = self._prompt_only_mcp_tool_decision_contract(
            mcp_tool_context=mcp_tool_context,
            mcp_available_tool_names=mcp_available_tool_names,
        )
        if prompt_only_tool_contract:
            mcp_prompt_sections.append(prompt_only_tool_contract)
        if mcp_prompt_sections:
            appended_prompt = "\n\n".join(section.strip() for section in mcp_prompt_sections if section.strip())
            if system_prompt.strip():
                system_prompt = f"{system_prompt.rstrip()}\n\n{appended_prompt}"
            else:
                system_prompt = appended_prompt
        provider_config = self._provider_config(context, bound_provider_node)
        decision_response_schema = api_decision_response_schema(
            final_message_schema=self.config.get("response_schema")
            if isinstance(self.config.get("response_schema"), Mapping)
            else None,
            available_tools=available_tools,
            allow_tool_calls=bool(available_tools),
            response_mode=response_mode,
        )
        return ModelRequest(
            prompt_name=self.prompt_name,
            messages=[
                ModelMessage(role="system", content=system_prompt),
                *context.prompt_block_messages_for_model(self.id),
                ModelMessage(role="user", content=context.render_template(user_template, metadata)),
            ],
            response_schema=decision_response_schema,
            provider_config=provider_config,
            available_tools=available_tools,
            preferred_tool_name=resolved_preferred_tool_name,
            response_mode=response_mode,
            metadata=metadata,
        )

    def execute(self, context: NodeContext) -> NodeExecutionResult:
        bound_provider_node = context.bound_provider_node(self.id)
        provider_name = str(
            (
                bound_provider_node.config.get("provider_name")
                if bound_provider_node is not None
                else self.config.get("provider_name", self.provider_name)
            )
            or self.provider_name
        )
        provider = context.services.model_providers[provider_name]
        request = self._build_request(context, bound_provider_node)
        prompt_trace_artifacts = _request_prompt_trace_artifacts(request.messages)
        metadata = request.metadata
        response_mode_hint = request.response_mode
        available_tool_payloads = list(metadata.get("available_tools", []))
        callable_tool_names = {
            str(tool.get("name", "")).strip()
            for tool in available_tool_payloads
            if isinstance(tool, Mapping) and str(tool.get("name", "")).strip()
        }
        tool_payload_lookup = {
            str(tool.get("name", "")).strip(): tool
            for tool in available_tool_payloads
            if isinstance(tool, Mapping) and str(tool.get("name", "")).strip()
        }
        decision_validation_tools = [
            tool
            for tool in request.available_tools
            if str(tool_payload_lookup.get(tool.name, {}).get("source_type", "")).strip() != "mcp"
        ]
        decision_validation_schema = (
            request.response_schema
            if len(decision_validation_tools) == len(request.available_tools)
            else api_decision_response_schema(
                final_message_schema=self.config.get("response_schema")
                if isinstance(self.config.get("response_schema"), Mapping)
                else None,
                available_tools=decision_validation_tools or None,
                allow_tool_calls=bool(available_tool_payloads),
                response_mode=response_mode_hint,
            )
        )
        response = provider.generate(request)
        try:
            normalized_decision_output = _canonicalize_api_decision_tool_names(
                normalize_api_decision_output(
                    response.structured_output,
                    content=response.content,
                    tool_calls=response.tool_calls,
                ),
                context.services.tool_registry,
            )
            decision_output = validate_api_decision_output(
                normalized_decision_output,
                decision_schema=decision_validation_schema if isinstance(decision_validation_schema, Mapping) else None,
                available_tools=decision_validation_tools or None,
                callable_tool_names=callable_tool_names,
                response_mode=response_mode_hint,
            )
        except ValueError as exc:
            error = {"message": str(exc), "type": "structured_api_output_error"}
            details = getattr(exc, "details", None)
            if isinstance(details, Mapping):
                error["details"] = dict(details)
            envelope = MessageEnvelope(
                schema_version="1.0",
                from_node_id=self.id,
                from_category=self.category.value,
                payload=None,
                artifacts=dict(prompt_trace_artifacts),
                errors=[error],
                metadata={
                    "contract": "message_envelope",
                    "node_kind": self.kind,
                    "provider": provider.name,
                    "prompt_name": self.prompt_name,
                    "response_mode": response_mode_hint,
                    **response.metadata,
                },
            )
            return NodeExecutionResult(
                status="validation_error",
                output=envelope.to_dict(),
                error=error,
                summary=f"Model node '{self.label}' returned invalid structured API output.",
                metadata=envelope.metadata,
            )

        normalized_tool_calls = list(decision_output["tool_calls"])
        emit_tool_call_envelope = bool(decision_output["should_call_tools"])
        emit_message_envelope = decision_output.get("message") is not None

        base_metadata = {
            "node_kind": self.kind,
            "provider": provider.name,
            "prompt_name": self.prompt_name,
            "response_mode": response_mode_hint,
            "should_call_tools": bool(decision_output["should_call_tools"]),
            "tool_call_count": len(response.tool_calls),
            **response.metadata,
        }
        route_outputs: dict[str, Any] = {}

        tool_envelope: MessageEnvelope | None = None
        if emit_tool_call_envelope:
            tool_artifacts: dict[str, Any] = dict(prompt_trace_artifacts)
            source_input = context.resolve_binding(None)
            source_input_envelope: MessageEnvelope | None = None
            if isinstance(source_input, Mapping) and "metadata" in source_input:
                try:
                    source_input_envelope = MessageEnvelope.from_dict(source_input)
                except Exception:  # noqa: BLE001
                    source_input_envelope = None
            if source_input_envelope is not None:
                tool_artifacts["source_input_payload"] = source_input_envelope.payload
                tool_artifacts["source_input_metadata"] = dict(source_input_envelope.metadata)
            elif source_input is not None:
                tool_artifacts["source_input_payload"] = source_input
            tool_envelope = MessageEnvelope(
                schema_version="1.0",
                from_node_id=self.id,
                from_category=self.category.value,
                payload=None,
                artifacts=tool_artifacts,
                tool_calls=normalized_tool_calls,
                metadata={
                    "contract": "tool_call_envelope",
                    **base_metadata,
                },
            )
            route_outputs[API_TOOL_CALL_HANDLE_ID] = tool_envelope.to_dict()

        if not emit_tool_call_envelope and callable_tool_names:
            tool_artifacts_nt: dict[str, Any] = dict(prompt_trace_artifacts)
            source_input_nt = context.resolve_binding(None)
            source_input_envelope_nt: MessageEnvelope | None = None
            if isinstance(source_input_nt, Mapping) and "metadata" in source_input_nt:
                try:
                    source_input_envelope_nt = MessageEnvelope.from_dict(source_input_nt)
                except Exception:  # noqa: BLE001
                    source_input_envelope_nt = None
            if source_input_envelope_nt is not None:
                tool_artifacts_nt["source_input_payload"] = source_input_envelope_nt.payload
                tool_artifacts_nt["source_input_metadata"] = dict(source_input_envelope_nt.metadata)
            elif source_input_nt is not None:
                tool_artifacts_nt["source_input_payload"] = source_input_nt
            no_tool_call_envelope = MessageEnvelope(
                schema_version="1.0",
                from_node_id=self.id,
                from_category=self.category.value,
                payload=NO_TOOL_CALL_MESSAGE,
                artifacts=tool_artifacts_nt,
                tool_calls=[],
                metadata={
                    "contract": "no_tool_call_envelope",
                    "no_tool_call": True,
                    **base_metadata,
                },
            )
            route_outputs[API_TOOL_CALL_HANDLE_ID] = no_tool_call_envelope.to_dict()

        message_envelope: MessageEnvelope | None = None
        if emit_message_envelope:
            message_payload = decision_output["message"]
            message_envelope = MessageEnvelope(
                schema_version="1.0",
                from_node_id=self.id,
                from_category=self.category.value,
                payload=message_payload,
                artifacts=dict(prompt_trace_artifacts),
                metadata={
                    "contract": "message_envelope",
                    **base_metadata,
                },
            )
            route_outputs[API_MESSAGE_HANDLE_ID] = message_envelope.to_dict()

        envelope = tool_envelope or message_envelope or MessageEnvelope(
            schema_version="1.0",
            from_node_id=self.id,
            from_category=self.category.value,
            payload=decision_output["message"],
            artifacts=dict(prompt_trace_artifacts),
            metadata={
                "contract": "message_envelope",
                **base_metadata,
            },
        )
        return NodeExecutionResult(
            status="success",
            output=envelope.to_dict(),
            summary=f"Model node '{self.label}' completed.",
            metadata=envelope.metadata,
            route_outputs=route_outputs,
        )

    def runtime_input_preview(self, context: NodeContext) -> Any:
        request = self._build_request(context)
        return {
            "messages": [{"role": message.role, "content": message.content} for message in request.messages],
            "response_mode": request.response_mode,
        }

    def to_dict(self) -> dict[str, Any]:
        payload = super().to_dict()
        payload["model_provider_name"] = self.provider_name
        payload["prompt_name"] = self.prompt_name
        return payload

    def _provider_config(self, context: NodeContext, bound_provider_node: ProviderNode | None = None) -> dict[str, Any]:
        raw_provider_config = self.config.get("provider_config", {})
        provider_config = dict(raw_provider_config) if isinstance(raw_provider_config, Mapping) else {}
        for key, value in self.config.items():
            if key not in self._RUNTIME_CONFIG_KEYS:
                provider_config[key] = value
        if bound_provider_node is not None:
            for key, value in bound_provider_node.config.items():
                if key != "provider_binding_node_id":
                    # Bound provider-node configuration is authoritative when present.
                    provider_config[key] = value
        resolved_provider_config = context.resolve_graph_env_value(provider_config)
        api_key_env_var = str(resolved_provider_config.get("api_key_env_var", "") or "").strip()
        if api_key_env_var and not str(resolved_provider_config.get("api_key", "") or "").strip():
            graph_env_vars = context.graph_env_vars()
            graph_env_api_key = str(graph_env_vars.get(api_key_env_var, "") or "").strip()
            if graph_env_api_key and graph_env_api_key != api_key_env_var:
                api_key = graph_env_api_key
            elif ENV_VAR_NAME_PATTERN.fullmatch(api_key_env_var):
                api_key = resolve_graph_process_env(api_key_env_var, graph_env_vars)
            else:
                api_key = api_key_env_var
            if api_key:
                resolved_provider_config["api_key"] = api_key
        return resolved_provider_config


class SpreadsheetMatrixDecisionNode(ModelNode):
    _RUNTIME_CONFIG_KEYS = ModelNode._RUNTIME_CONFIG_KEYS | {
        "file_format",
        "file_path",
        "project_file_id",
        "project_file_name",
        "sheet_name",
        "run_document_id",
        "run_document_name",
    }

    def _spreadsheet_config(self, context: NodeContext) -> dict[str, Any]:
        resolved = context.resolve_graph_env_value(dict(self.config))
        file_format = str(resolved.get("file_format", "auto") or "auto").strip().lower() or "auto"
        file_path = str(resolved.get("file_path", "") or "").strip()
        if not file_path:
            file_path = resolve_spreadsheet_path_from_run_documents(
                context.state.documents,
                run_document_id=str(resolved.get("run_document_id", "") or ""),
                run_document_name=str(resolved.get("run_document_name", "") or ""),
            )
        return {
            "file_format": file_format,
            "file_path": file_path,
            "sheet_name": str(resolved.get("sheet_name", "") or "").strip() or None,
        }

    def _resolved_provider_name(self, bound_provider_node: ProviderNode | None) -> str:
        return str(
            (
                bound_provider_node.config.get("provider_name")
                if bound_provider_node is not None
                else self.config.get("provider_name", self.provider_name)
            )
            or self.provider_name
        )

    def _build_request(
        self,
        context: NodeContext,
        bound_provider_node: ProviderNode | None = None,
    ) -> ModelRequest:
        request, _ = self._build_matrix_request(context, bound_provider_node)
        return request

    def _matrix_request_cache_key(self, context: NodeContext) -> tuple[str, int, str | None, str | None]:
        iteration_id = str(context.state.current_iteration_context.get("iteration_id", "") or "").strip() or None
        incoming_edge_id = str(context.state.current_edge_id or "").strip() or None
        visit_count = int(context.state.visit_counts.get(self.id, 0) or 0)
        return (self.id, visit_count, incoming_edge_id, iteration_id)

    def _cache_matrix_request(
        self,
        context: NodeContext,
        request: ModelRequest,
        matrix: SpreadsheetMatrixParseResult,
    ) -> None:
        context.state.runtime_preview_cache[self.id] = {
            "key": self._matrix_request_cache_key(context),
            "request": request,
            "matrix": matrix,
        }

    def _consume_cached_matrix_request(
        self,
        context: NodeContext,
    ) -> tuple[ModelRequest, SpreadsheetMatrixParseResult] | None:
        cached = context.state.runtime_preview_cache.get(self.id)
        if not isinstance(cached, Mapping):
            return None
        cached_key = cached.get("key")
        if not isinstance(cached_key, tuple) or cached_key != self._matrix_request_cache_key(context):
            context.state.runtime_preview_cache.pop(self.id, None)
            return None
        request = cached.get("request")
        matrix = cached.get("matrix")
        if not isinstance(request, ModelRequest) or not isinstance(matrix, SpreadsheetMatrixParseResult):
            context.state.runtime_preview_cache.pop(self.id, None)
            return None
        context.state.runtime_preview_cache.pop(self.id, None)
        return request, matrix

    def _build_matrix_request(
        self,
        context: NodeContext,
        bound_provider_node: ProviderNode | None = None,
    ) -> tuple[ModelRequest, SpreadsheetMatrixParseResult]:
        if bound_provider_node is None:
            bound_provider_node = context.bound_provider_node(self.id)
        matrix = parse_spreadsheet_matrix(**self._spreadsheet_config(context))
        metadata_bindings = dict(self.config.get("metadata_bindings", {}))
        metadata: dict[str, Any] = {}
        for key, binding in metadata_bindings.items():
            if isinstance(binding, Mapping):
                metadata[key] = context.resolve_binding(binding)
            else:
                metadata[key] = binding
        metadata.update(
            {
                "mode": self.config.get("mode", SPREADSHEET_MATRIX_DECISION_MODE),
                "spreadsheet_matrix_source_file": matrix.source_file,
                "spreadsheet_matrix_sheet_name": matrix.sheet_name or "",
                "spreadsheet_matrix_corner_label": matrix.corner_label,
                "spreadsheet_matrix_row_labels": list(matrix.row_labels),
                "spreadsheet_matrix_column_labels": list(matrix.column_labels),
                "spreadsheet_matrix_markdown": _render_spreadsheet_matrix_markdown(matrix),
            }
        )
        if context.prompt_block_payloads_for_node(self.id) and "prompt_blocks" not in metadata:
            metadata["prompt_blocks"] = context.prompt_block_payloads_for_node(self.id)
        system_prompt = context.render_template(str(self.config.get("system_prompt", "") or ""), metadata).strip()
        matrix_instruction = "\n".join(
            [
                "You are selecting one cell from a spreadsheet decision matrix.",
                "The first row contains the column-axis labels and the first column contains the row-axis labels.",
                "Choose exactly one row_label and one column_label that best answer the user's request.",
                "Use the full context to infer the strongest fit, not shallow keyword overlap or title matching.",
                "When the input describes a person, role, or account, reason from responsibilities, incentives, technical depth, seniority, and likely response behavior.",
                "If a headline title conflicts with the role description, prioritize the day-to-day work, tools, outcomes, and scope in the description.",
                "Distinguish closely related profiles when the evidence supports it, such as technical versus managerial PMs, operators versus strategists, or practitioners versus decision-makers.",
                "Prefer the single best-supported row and column rather than averaging across multiple plausible categories.",
                "Return only structured JSON matching the schema. The row_label and column_label must exactly match the provided labels.",
            ]
        )
        final_system_prompt = "\n\n".join(section for section in [system_prompt, matrix_instruction] if section.strip())
        rendered_user_message = context.render_template(
            str(self.config.get("user_message_template", "{input_payload}") or "{input_payload}"),
            metadata,
        ).strip()
        decision_guidance = "\n".join(
            [
                "Selection guidance:",
                "- Match on the underlying role, intent, pressure, and likely response pattern, not just literal word overlap.",
                "- If the input is about a person, infer what they are most likely to respond to from their scope, current responsibilities, technical fluency, seniority, and business context.",
                "- When a current title sounds broad or generic, use the role description and evidence of day-to-day work to narrow the fit.",
                "- If title and description point in different directions, trust the detailed responsibilities over the headline title.",
                "- Make the sharpest supported distinction available, for example technical PM versus people manager or hands-on operator versus executive sponsor.",
            ]
        )
        matrix_context = "\n".join(
            [
                "Spreadsheet Matrix",
                f"Source file: {matrix.source_file}",
                f"Sheet: {matrix.sheet_name or 'first sheet'}",
                f"Corner label: {matrix.corner_label or '(blank)'}",
                "",
                "Available row labels:",
                *[f"- {label}" for label in matrix.row_labels],
                "",
                "Available column labels:",
                *[f"- {label}" for label in matrix.column_labels],
                "",
                "Matrix:",
                _render_spreadsheet_matrix_markdown(matrix),
            ]
        )
        final_user_message = "\n\n".join(
            section
            for section in [
                rendered_user_message and f"User question or context:\n{rendered_user_message}",
                decision_guidance,
                matrix_context,
            ]
            if section
        )
        return (
            ModelRequest(
                prompt_name=self.prompt_name,
                messages=[
                    ModelMessage(role="system", content=final_system_prompt),
                    *context.prompt_block_messages_for_model(self.id),
                    ModelMessage(role="user", content=final_user_message),
                ],
                response_schema=_spreadsheet_matrix_selection_response_schema(matrix),
                provider_config=self._provider_config(context, bound_provider_node),
                available_tools=[],
                preferred_tool_name=None,
                response_mode="message",
                metadata=metadata,
            ),
            matrix,
        )

    def _selection_from_response(
        self,
        response: ModelResponse,
        matrix: SpreadsheetMatrixParseResult,
    ) -> dict[str, str]:
        candidate = response.structured_output
        # Providers may normalize structured outputs into the
        # {message, need_tool, tool_calls} decision envelope. For spreadsheet
        # matrix nodes, only the wrapped message payload is relevant.
        if isinstance(candidate, Mapping) and (
            "need_tool" in candidate
            or "tool_calls" in candidate
            or "message" in candidate
        ):
            candidate = candidate.get("message")
        if not isinstance(candidate, Mapping) and response.content.strip():
            try:
                parsed = json.loads(response.content)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, Mapping):
                candidate = parsed
        if not isinstance(candidate, Mapping):
            raise ValueError("Spreadsheet matrix decision node requires a structured JSON object response.")
        row_label = str(candidate.get("row_label", "") or "").strip()
        column_label = str(candidate.get("column_label", "") or "").strip()
        if row_label not in matrix.row_labels:
            raise ValueError(
                f"Spreadsheet matrix decision returned unknown row_label '{row_label}'."
            )
        if column_label not in matrix.column_labels:
            raise ValueError(
                f"Spreadsheet matrix decision returned unknown column_label '{column_label}'."
            )
        reasoning = str(candidate.get("reasoning", "") or "").strip()
        return {
            "row_label": row_label,
            "column_label": column_label,
            "reasoning": reasoning,
        }

    def execute(self, context: NodeContext) -> NodeExecutionResult:
        bound_provider_node = context.bound_provider_node(self.id)
        try:
            cached_request = self._consume_cached_matrix_request(context)
            if cached_request is not None:
                request, matrix = cached_request
            else:
                request, matrix = self._build_matrix_request(context, bound_provider_node)
        except SpreadsheetParseError as exc:
            return NodeExecutionResult(
                status="failed",
                error={"type": "spreadsheet_parse_error", "message": str(exc)},
                summary=str(exc),
            )

        provider_name = self._resolved_provider_name(bound_provider_node)
        provider = context.services.model_providers[provider_name]
        response = provider.generate(request)
        prompt_trace_artifacts = _request_prompt_trace_artifacts(request.messages)
        try:
            selection = self._selection_from_response(response, matrix)
        except ValueError as exc:
            error = {"message": str(exc), "type": "spreadsheet_matrix_selection_error"}
            envelope = MessageEnvelope(
                schema_version="1.0",
                from_node_id=self.id,
                from_category=self.category.value,
                payload=None,
                artifacts=dict(prompt_trace_artifacts),
                errors=[error],
                metadata={
                    "contract": "message_envelope",
                    "node_kind": self.kind,
                    "provider": provider.name,
                    "prompt_name": self.prompt_name,
                    "response_mode": "message",
                    **response.metadata,
                },
            )
            return NodeExecutionResult(
                status="validation_error",
                output=envelope.to_dict(),
                error=error,
                summary=f"Spreadsheet matrix node '{self.label}' returned an invalid selection.",
                metadata=envelope.metadata,
            )

        selected_row = matrix.row_by_label(selection["row_label"])
        selected_value = selected_row.values.get(selection["column_label"])
        selection_payload = {
            "row_label": selection["row_label"],
            "column_label": selection["column_label"],
            "row_number": selected_row.row_number,
            "column_number": matrix.column_number_for_label(selection["column_label"]),
            "value": selected_value,
            "reasoning": selection["reasoning"],
            "sheet_name": matrix.sheet_name,
            "source_file": matrix.source_file,
        }
        artifacts = dict(prompt_trace_artifacts)
        artifacts["spreadsheet_matrix_selection"] = selection_payload
        envelope = MessageEnvelope(
            schema_version="1.0",
            from_node_id=self.id,
            from_category=self.category.value,
            payload=selected_value,
            artifacts=artifacts,
            metadata={
                "contract": "message_envelope",
                "node_kind": self.kind,
                "provider": provider.name,
                "prompt_name": self.prompt_name,
                "response_mode": "message",
                "mode": self.config.get("mode", SPREADSHEET_MATRIX_DECISION_MODE),
                "row_label": selection["row_label"],
                "column_label": selection["column_label"],
                "row_number": selected_row.row_number,
                "column_number": matrix.column_number_for_label(selection["column_label"]),
                "sheet_name": matrix.sheet_name,
                "source_file": matrix.source_file,
                **response.metadata,
            },
        )
        return NodeExecutionResult(
            status="success",
            output=envelope.to_dict(),
            summary=(
                f"Spreadsheet matrix node '{self.label}' selected row '{selection['row_label']}' "
                f"and column '{selection['column_label']}'."
            ),
            metadata=envelope.metadata,
            route_outputs={API_MESSAGE_HANDLE_ID: envelope.to_dict()},
        )

    def runtime_input_preview(self, context: NodeContext) -> Any:
        try:
            request, matrix = self._build_matrix_request(context)
        except SpreadsheetParseError as exc:
            context.state.runtime_preview_cache.pop(self.id, None)
            return {"error": str(exc)}
        self._cache_matrix_request(context, request, matrix)
        return {
            "messages": [{"role": message.role, "content": message.content} for message in request.messages],
            "response_mode": request.response_mode,
            "matrix": matrix.preview(limit=3),
        }


class ToolNode(BaseNode):
    kind = "tool"

    def __init__(
        self,
        node_id: str,
        label: str,
        tool_name: str,
        provider_id: str = "tool.registry",
        provider_label: str = "Registry Tool Node",
        description: str = "",
        config: Mapping[str, Any] | None = None,
        position: Mapping[str, Any] | None = None,
    ) -> None:
        merged_config = {"tool_name": tool_name, **dict(config or {})}
        super().__init__(
            node_id=node_id,
            label=label,
            category=NodeCategory.TOOL,
            provider_id=provider_id,
            provider_label=provider_label,
            description=description,
            config=merged_config,
            position=position,
        )
        self.tool_name = tool_name

    def execute(self, context: NodeContext) -> NodeExecutionResult:
        bound_value = context.resolve_binding(self.config.get("input_binding"))
        payload: Any = bound_value
        if isinstance(bound_value, Mapping) and "tool_calls" in bound_value:
            tool_calls = list(bound_value.get("tool_calls", []))
            if tool_calls:
                payload = tool_calls[0].get("arguments")
            else:
                raw_payload = bound_value.get("payload")
                payload = raw_payload if isinstance(raw_payload, Mapping) else {}
        elif isinstance(bound_value, Mapping) and "payload" in bound_value:
            payload = bound_value.get("payload")
        if not isinstance(payload, Mapping):
            payload = {}
        tool_context = ToolContext(
            run_id=context.state.run_id,
            graph_id=context.state.graph_id,
            node_id=context.node_id,
            state_snapshot=context.state.snapshot(),
        )
        tool_result = context.services.tool_registry.invoke(self.tool_name, payload, tool_context)
        envelope = MessageEnvelope(
            schema_version="1.0",
            from_node_id=self.id,
            from_category=self.category.value,
            payload=tool_result.output,
            errors=[tool_result.error] if tool_result.error else [],
            metadata={
                "contract": "tool_result_envelope",
                "node_kind": self.kind,
                "tool_name": self.tool_name,
                **tool_result.metadata,
            },
        )
        return NodeExecutionResult(
            status=tool_result.status,
            output=envelope.to_dict(),
            error=tool_result.error,
            summary=tool_result.summary or f"Tool '{self.tool_name}' completed.",
            metadata=envelope.metadata,
        )

    def runtime_input_preview(self, context: NodeContext) -> Any:
        bound_value = context.resolve_binding(self.config.get("input_binding"))
        payload: Any = bound_value
        if isinstance(bound_value, Mapping) and "tool_calls" in bound_value:
            tool_calls = list(bound_value.get("tool_calls", []))
            if tool_calls:
                payload = tool_calls[0].get("arguments")
        elif isinstance(bound_value, Mapping) and "payload" in bound_value:
            payload = bound_value.get("payload")
        return payload if isinstance(payload, Mapping) else {}

    def to_dict(self) -> dict[str, Any]:
        payload = super().to_dict()
        payload["tool_name"] = self.tool_name
        return payload


class McpContextProviderNode(BaseNode):
    kind = "mcp_context_provider"

    def __init__(
        self,
        node_id: str,
        label: str,
        provider_id: str = "tool.mcp_context_provider",
        provider_label: str = "MCP Context Provider",
        description: str = "",
        config: Mapping[str, Any] | None = None,
        position: Mapping[str, Any] | None = None,
    ) -> None:
        tool_names: list[str] = []
        raw_config = dict(config or {})
        raw_tool_names = raw_config.get("tool_names", [])
        if isinstance(raw_tool_names, Sequence) and not isinstance(raw_tool_names, (str, bytes)):
            tool_names.extend(str(tool_name).strip() for tool_name in raw_tool_names if str(tool_name).strip())
        fallback_tool_name = str(raw_config.get("tool_name", "")).strip()
        if fallback_tool_name and fallback_tool_name not in tool_names:
            tool_names.append(fallback_tool_name)
        merged_config = {
            "tool_names": tool_names,
            "include_mcp_tool_context": bool(raw_config.get("include_mcp_tool_context", False)),
            "expose_mcp_tools": bool(raw_config.get("expose_mcp_tools", True)),
            **raw_config,
        }
        super().__init__(
            node_id=node_id,
            label=label,
            category=NodeCategory.TOOL,
            provider_id=provider_id,
            provider_label=provider_label,
            description=description,
            config=merged_config,
            position=position,
        )

    def execute(self, context: NodeContext) -> NodeExecutionResult:
        output = {
            "tool_names": list(self.config.get("tool_names", [])),
            "include_mcp_tool_context": bool(self.config.get("include_mcp_tool_context", False)),
            "expose_mcp_tools": bool(self.config.get("expose_mcp_tools", True)),
        }
        envelope = MessageEnvelope(
            schema_version="1.0",
            from_node_id=self.id,
            from_category=self.category.value,
            payload=output,
            metadata={
                "contract": "data_envelope",
                "node_kind": self.kind,
                "binding_only": True,
            },
        )
        return NodeExecutionResult(
            status="success",
            output=envelope.to_dict(),
            summary=f"MCP context provider '{self.label}' prepared context metadata.",
            metadata=envelope.metadata,
        )

    def runtime_input_preview(self, context: NodeContext) -> Any:
        return {
            "tool_names": list(self.config.get("tool_names", [])),
            "include_mcp_tool_context": bool(self.config.get("include_mcp_tool_context", False)),
            "expose_mcp_tools": bool(self.config.get("expose_mcp_tools", True)),
        }


class McpToolExecutorNode(BaseNode):
    kind = "mcp_tool_executor"
    _RUNTIME_CONFIG_KEYS = {
        "allow_retries",
        "allowed_tool_names",
        "enable_follow_up_decision",
        "input_binding",
        "metadata_bindings",
        "mode",
        "preferred_tool_name",
        "prompt_name",
        "provider_name",
        "response_mode",
        "response_schema",
        "response_schema_text",
        "system_prompt",
        "tool_target_node_ids",
        "user_message_template",
        "validate_last_tool_success",
    }
    _FOLLOW_UP_STATE_CONTRACT = "mcp_executor_follow_up_envelope"

    def __init__(
        self,
        node_id: str,
        label: str,
        provider_id: str = "tool.mcp_tool_executor",
        provider_label: str = "MCP Tool Executor",
        description: str = "",
        config: Mapping[str, Any] | None = None,
        position: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(
            node_id=node_id,
            label=label,
            category=NodeCategory.TOOL,
            provider_id=provider_id,
            provider_label=provider_label,
            description=description,
            config=config,
            position=position,
        )

    def _source_envelope_from_value(self, bound_value: Any) -> MessageEnvelope | None:
        return _message_envelope_from_value(bound_value)

    def _extract_tool_call_from_value(self, value: Any) -> tuple[MessageEnvelope | None, str, dict[str, Any]]:
        source_envelope = self._source_envelope_from_value(value)
        tool_call: Mapping[str, Any] | None = None
        if isinstance(value, Mapping):
            raw_tool_calls = value.get("tool_calls", [])
            if isinstance(raw_tool_calls, Sequence) and not isinstance(raw_tool_calls, (str, bytes)):
                for candidate in raw_tool_calls:
                    if isinstance(candidate, Mapping):
                        tool_call = candidate
                        break
        if tool_call is None and source_envelope is not None and source_envelope.tool_calls:
            tool_call = source_envelope.tool_calls[0]
        tool_name = str((tool_call or {}).get("tool_name", "")).strip()
        payload = (tool_call or {}).get("arguments", {})
        if isinstance(value, Mapping) and "payload" in value and not tool_name:
            payload = value.get("payload")
        if not isinstance(payload, Mapping):
            payload = {}
        return source_envelope, tool_name, dict(payload)

    def _resolve_tool_call_input(self, context: NodeContext) -> tuple[MessageEnvelope | None, str, dict[str, Any]]:
        input_binding = self.config.get("input_binding")
        bound_value = context.resolve_binding(input_binding)
        source_envelope, tool_name, payload = self._extract_tool_call_from_value(bound_value)
        if tool_name:
            return source_envelope, tool_name, payload

        fallback_candidates: list[Any] = []
        current_edge = context.current_input_edge()
        if current_edge is not None:
            fallback_candidates.append(context.state.edge_outputs.get(current_edge.id))
            fallback_candidates.append(context.latest_output(current_edge.source_id))
        if isinstance(input_binding, Mapping):
            binding_source = str(input_binding.get("source", "")).strip()
            if binding_source:
                fallback_candidates.append(context.latest_output(binding_source))

        for candidate in fallback_candidates:
            _, candidate_tool_name, candidate_payload = self._extract_tool_call_from_value(candidate)
            if candidate_tool_name:
                candidate_envelope = self._source_envelope_from_value(candidate)
                return candidate_envelope or source_envelope, candidate_tool_name, candidate_payload
        return source_envelope, tool_name, payload

    def _provider_config(self, context: NodeContext) -> dict[str, Any]:
        provider_config: dict[str, Any] = {}
        for key, value in self.config.items():
            if key not in self._RUNTIME_CONFIG_KEYS:
                provider_config[key] = value
        return context.resolve_graph_env_value(provider_config)

    def _tool_call_signature(self, tool_name: str, arguments: Any) -> str:
        try:
            normalized_arguments = json.dumps(arguments, sort_keys=True, separators=(",", ":"))
        except TypeError:
            normalized_arguments = json.dumps(_json_safe(arguments), sort_keys=True, separators=(",", ":"))
        return f"{tool_name}:{normalized_arguments}"

    def _successful_tool_call_signatures(self, normalized_payload: Mapping[str, Any]) -> set[str]:
        successful_signatures: set[str] = set()
        raw_tool_history = normalized_payload.get("tool_history", [])
        if not isinstance(raw_tool_history, Sequence) or isinstance(raw_tool_history, (str, bytes)):
            return successful_signatures
        for entry in raw_tool_history:
            if not isinstance(entry, Mapping):
                continue
            if str(entry.get("tool_status", "")).strip() != "success":
                continue
            tool_name = str(entry.get("tool_name", "")).strip()
            if tool_name:
                successful_signatures.add(self._tool_call_signature(tool_name, entry.get("tool_arguments", {})))
        return successful_signatures

    def _normalize_tool_calls(self, raw_tool_calls: Any) -> list[dict[str, Any]]:
        if not isinstance(raw_tool_calls, Sequence) or isinstance(raw_tool_calls, (str, bytes)):
            return []
        normalized: list[dict[str, Any]] = []
        for candidate in raw_tool_calls:
            if not isinstance(candidate, Mapping):
                continue
            tool_name = str(candidate.get("tool_name", "")).strip()
            if not tool_name:
                continue
            normalized_call = dict(candidate)
            normalized_call["tool_name"] = tool_name
            normalized_call["arguments"] = (
                dict(candidate.get("arguments", {})) if isinstance(candidate.get("arguments"), Mapping) else {}
            )
            normalized.append(normalized_call)
        return normalized

    def _split_requested_tool_calls(
        self,
        requested_tool_calls: Sequence[Mapping[str, Any]],
        *,
        tool_name: str,
        arguments: Mapping[str, Any],
    ) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
        remaining_tool_calls: list[dict[str, Any]] = []
        matched_tool_call: dict[str, Any] | None = None
        target_signature = self._tool_call_signature(tool_name, arguments)
        for candidate in requested_tool_calls:
            normalized_candidate = dict(candidate)
            normalized_candidate["arguments"] = (
                dict(candidate.get("arguments", {})) if isinstance(candidate.get("arguments"), Mapping) else {}
            )
            candidate_signature = self._tool_call_signature(
                str(normalized_candidate.get("tool_name", "")).strip(),
                normalized_candidate.get("arguments", {}),
            )
            if matched_tool_call is None and candidate_signature == target_signature:
                matched_tool_call = normalized_candidate
                continue
            remaining_tool_calls.append(normalized_candidate)
        if matched_tool_call is None and tool_name:
            matched_tool_call = {
                "tool_name": tool_name,
                "arguments": dict(arguments),
                "provider_tool_id": None,
                "metadata": {},
            }
        return matched_tool_call, remaining_tool_calls

    def _configured_follow_up_response_mode(self) -> str:
        response_mode = str(self.config.get("response_mode", "auto") or "auto").strip()
        if response_mode not in {"message", "tool_call", "auto"}:
            return "auto"
        return response_mode

    def _retries_enabled(self) -> bool:
        return bool(self.config.get("allow_retries", True))

    def _dispatch_tool_call(
        self,
        context: NodeContext,
        *,
        tool_name: str,
        payload: Mapping[str, Any],
        source_envelope: MessageEnvelope | None,
    ) -> NodeExecutionResult:
        payload_dict = dict(payload)
        if not tool_name:
            error = {"type": "missing_tool_call", "node_id": self.id, "message": "No MCP tool call was available to dispatch."}
            envelope = MessageEnvelope(
                schema_version="1.0",
                from_node_id=self.id,
                from_category=self.category.value,
                payload=None,
                errors=[error],
                metadata={"contract": "tool_result_envelope", "node_kind": self.kind},
            )
            return NodeExecutionResult(
                status="failed",
                output=envelope.to_dict(),
                error=error,
                summary=f"MCP executor '{self.label}' did not receive a tool call.",
                metadata=envelope.metadata,
            )
        try:
            tool_definition = context.services.tool_registry.require_invocable(tool_name)
            if tool_definition.source_type != "mcp":
                raise ValueError(f"Tool '{tool_name}' is not an MCP tool.")
            resolved_tool_name = tool_definition.canonical_name
            tool_context = ToolContext(
                run_id=context.state.run_id,
                graph_id=context.state.graph_id,
                node_id=context.node_id,
                state_snapshot=context.state.snapshot(),
            )
            tool_result = context.services.tool_registry.invoke(resolved_tool_name, payload_dict, tool_context)
            route_outputs: dict[str, Any] = {}
            execution_summary = tool_result.summary or f"MCP tool '{resolved_tool_name}' completed."
            tool_metadata = dict(tool_result.metadata)
            terminal_output = tool_metadata.pop("terminal_output", None)
            tool_artifacts: dict[str, Any] = {}
            if source_envelope is not None:
                tool_artifacts["source_tool_call_envelope"] = source_envelope.to_dict()
                normalized_requested_tool_calls = self._normalize_tool_calls(source_envelope.tool_calls)
                if normalized_requested_tool_calls:
                    requested_tool_call, pending_tool_calls = self._split_requested_tool_calls(
                        normalized_requested_tool_calls,
                        tool_name=resolved_tool_name,
                        arguments=payload_dict,
                    )
                    if requested_tool_call is not None:
                        tool_artifacts["requested_tool_call"] = requested_tool_call
                    tool_artifacts["requested_tool_calls"] = [dict(tool_call) for tool_call in normalized_requested_tool_calls]
                    tool_artifacts["pending_tool_calls"] = [dict(tool_call) for tool_call in pending_tool_calls]
                assistant_message = source_envelope.artifacts.get("assistant_message")
                if assistant_message:
                    tool_artifacts["assistant_message"] = assistant_message
            if isinstance(terminal_output, Mapping):
                terminal_envelope = MessageEnvelope(
                    schema_version="1.0",
                    from_node_id=self.id,
                    from_category=self.category.value,
                    payload=dict(terminal_output),
                    errors=[tool_result.error] if tool_result.error else [],
                    metadata={
                        "contract": "terminal_output_envelope",
                        "node_kind": self.kind,
                        "tool_name": resolved_tool_name,
                    },
                )
                route_outputs[MCP_TERMINAL_OUTPUT_HANDLE_ID] = terminal_envelope.to_dict()
                tool_artifacts["terminal_output"] = dict(terminal_output)
                tool_artifacts["terminal_output_envelope"] = terminal_envelope.to_dict()
            envelope = MessageEnvelope(
                schema_version="1.0",
                from_node_id=self.id,
                from_category=self.category.value,
                payload=tool_result.output,
                artifacts=tool_artifacts,
                errors=[tool_result.error] if tool_result.error else [],
                metadata={
                    "contract": "tool_result_envelope",
                    "node_kind": self.kind,
                    "tool_name": resolved_tool_name,
                    **tool_metadata,
                    "tool_status": tool_result.status,
                    "tool_summary": execution_summary,
                    "terminal_output_present": isinstance(terminal_output, Mapping),
                },
            )
            return NodeExecutionResult(
                status=tool_result.status,
                output=envelope.to_dict(),
                error=tool_result.error,
                summary=execution_summary,
                metadata=envelope.metadata,
                route_outputs=route_outputs,
            )
        except (KeyError, ValueError) as exc:
            error = {"type": "mcp_tool_dispatch_error", "node_id": self.id, "tool_name": tool_name, "message": str(exc)}
            envelope = MessageEnvelope(
                schema_version="1.0",
                from_node_id=self.id,
                from_category=self.category.value,
                payload=None,
                errors=[error],
                metadata={
                    "contract": "tool_result_envelope",
                    "node_kind": self.kind,
                    "tool_name": tool_name,
                },
            )
            return NodeExecutionResult(
                status="failed",
                output=envelope.to_dict(),
                error=error,
                summary=f"MCP executor '{self.label}' could not dispatch '{tool_name}'.",
                metadata=envelope.metadata,
            )

    def _invalid_follow_up_result(
        self,
        message: str,
        *,
        route_outputs: Mapping[str, Any] | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> NodeExecutionResult:
        error = {"type": "mcp_executor_follow_up_error", "node_id": self.id, "message": message}
        if isinstance(details, Mapping):
            error["details"] = dict(details)
        envelope = MessageEnvelope(
            schema_version="1.0",
            from_node_id=self.id,
            from_category=self.category.value,
            payload=None,
            errors=[error],
            metadata={"contract": "tool_result_envelope", "node_kind": self.kind},
        )
        return NodeExecutionResult(
            status="failed",
            output=envelope.to_dict(),
            error=error,
            summary=f"MCP executor '{self.label}' could not prepare follow-up context.",
            metadata=envelope.metadata,
            route_outputs=dict(route_outputs or {}),
        )

    def _validation_details(self, value: Any) -> dict[str, Any]:
        if isinstance(value, Mapping):
            return dict(value)
        return {}

    def _should_halt_after_tool_result(self, normalized_payload: Mapping[str, Any]) -> bool:
        if not bool(self.config.get("validate_last_tool_success", True)):
            return False
        return str(normalized_payload.get("tool_status", "")).strip() not in {"success", "validation_error"}

    def _repair_context_payload(
        self,
        normalized_payload: Mapping[str, Any],
        *,
        repair_type: str,
        message: str,
        validation_details: Mapping[str, Any] | None = None,
        attempted_decision: Mapping[str, Any] | None = None,
        attempted_tool_call: Mapping[str, Any] | None = None,
        available_tool_names: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        updated_payload = dict(normalized_payload)
        follow_up_context = (
            dict(updated_payload.get("follow_up_context", {}))
            if isinstance(updated_payload.get("follow_up_context"), Mapping)
            else {}
        )
        repair_context: dict[str, Any] = {
            "repair_type": repair_type,
            "message": message,
            "allowed_tool_names": [str(name).strip() for name in (available_tool_names or []) if str(name).strip()],
        }
        if isinstance(validation_details, Mapping):
            repair_context.update(self._validation_details(validation_details))
        if isinstance(attempted_decision, Mapping):
            repair_context["attempted_decision"] = dict(attempted_decision)
        if isinstance(attempted_tool_call, Mapping):
            repair_context["attempted_tool_call"] = dict(attempted_tool_call)
        follow_up_context["repair_attempt_count"] = int(follow_up_context.get("repair_attempt_count", 0) or 0) + 1
        updated_payload["follow_up_context"] = follow_up_context
        updated_payload["repair_context"] = repair_context
        return updated_payload

    def _clear_repair_context(self, normalized_payload: Mapping[str, Any]) -> dict[str, Any]:
        updated_payload = dict(normalized_payload)
        updated_payload.pop("repair_context", None)
        return updated_payload

    def _repair_payload_for_validation_error(
        self,
        normalized_payload: Mapping[str, Any],
        *,
        available_tool_names: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        if str(normalized_payload.get("tool_status", "")).strip() != "validation_error":
            return normalized_payload
        tool_error = self._validation_details(normalized_payload.get("tool_error"))
        attempted_tool_call = (
            dict(normalized_payload.get("tool_call", {}))
            if isinstance(normalized_payload.get("tool_call"), Mapping)
            else None
        )
        message = str(tool_error.get("message", "") or "The last MCP tool call failed schema validation.")
        return self._repair_context_payload(
            normalized_payload,
            repair_type="tool_call_validation_error",
            message=message,
            validation_details=tool_error,
            attempted_tool_call=attempted_tool_call,
            available_tool_names=available_tool_names,
        )

    def _build_follow_up_payload(
        self,
        context: NodeContext,
        source_envelope: MessageEnvelope,
    ) -> tuple[dict[str, Any], Mapping[str, Any] | None, Mapping[str, Any] | None]:
        requested_tool_call = source_envelope.artifacts.get("requested_tool_call")
        source_tool_call_envelope = source_envelope.artifacts.get("source_tool_call_envelope")
        source_tool_call_artifacts = (
            dict(source_tool_call_envelope.get("artifacts", {}))
            if isinstance(source_tool_call_envelope, Mapping) and isinstance(source_tool_call_envelope.get("artifacts"), Mapping)
            else {}
        )
        source_input_metadata = (
            dict(source_tool_call_artifacts.get("source_input_metadata", {}))
            if isinstance(source_tool_call_artifacts.get("source_input_metadata"), Mapping)
            else {}
        )
        source_input_payload = source_tool_call_artifacts.get("source_input_payload")
        previous_follow_up_payload = (
            dict(source_input_payload)
            if isinstance(source_input_payload, Mapping)
            and str(source_input_metadata.get("contract", "")).strip() == self._FOLLOW_UP_STATE_CONTRACT
            else None
        )
        requested_tool_calls = (
            self._normalize_tool_calls(previous_follow_up_payload.get("requested_tool_calls"))
            if previous_follow_up_payload is not None and "requested_tool_calls" in previous_follow_up_payload
            else []
        )
        if "requested_tool_calls" in source_envelope.artifacts:
            requested_tool_calls = self._normalize_tool_calls(source_envelope.artifacts.get("requested_tool_calls"))
        elif not requested_tool_calls and isinstance(source_tool_call_envelope, Mapping):
            requested_tool_calls = self._normalize_tool_calls(source_tool_call_envelope.get("tool_calls"))
        pending_tool_calls = (
            self._normalize_tool_calls(previous_follow_up_payload.get("pending_tool_calls"))
            if previous_follow_up_payload is not None and "pending_tool_calls" in previous_follow_up_payload
            else []
        )
        if "pending_tool_calls" in source_envelope.artifacts:
            pending_tool_calls = self._normalize_tool_calls(source_envelope.artifacts.get("pending_tool_calls"))
        elif not pending_tool_calls and requested_tool_calls:
            _, pending_tool_calls = self._split_requested_tool_calls(
                requested_tool_calls,
                tool_name=str(source_envelope.metadata.get("tool_name", "")).strip(),
                arguments=dict(requested_tool_call.get("arguments", {})) if isinstance(requested_tool_call, Mapping) else {},
            )
        terminal_output = source_envelope.artifacts.get("terminal_output")
        terminal_output_envelope = source_envelope.artifacts.get("terminal_output_envelope")
        tool_name = str(source_envelope.metadata.get("tool_name", "")).strip()
        tool_error = source_envelope.errors[0] if source_envelope.errors else None
        prior_tool_history = []
        if previous_follow_up_payload is not None:
            raw_tool_history = previous_follow_up_payload.get("tool_history", [])
            if isinstance(raw_tool_history, Sequence) and not isinstance(raw_tool_history, (str, bytes)):
                prior_tool_history = [dict(entry) for entry in raw_tool_history if isinstance(entry, Mapping)]
        current_tool_entry = {
            "tool_name": tool_name,
            "tool_status": str(source_envelope.metadata.get("tool_status", "") or ("failed" if tool_error else "success")),
            "tool_summary": str(source_envelope.metadata.get("tool_summary", "") or ""),
            "tool_arguments": dict(requested_tool_call.get("arguments", {})) if isinstance(requested_tool_call, Mapping) else {},
            "tool_call": dict(requested_tool_call) if isinstance(requested_tool_call, Mapping) else None,
            "tool_output": source_envelope.payload,
            "tool_error": tool_error,
            "tool_errors": list(source_envelope.errors),
            "tool_metadata": dict(source_envelope.metadata),
            "terminal_output": dict(terminal_output) if isinstance(terminal_output, Mapping) else None,
        }
        normalized_payload = {
            "original_input_payload": (
                previous_follow_up_payload.get("original_input_payload")
                if previous_follow_up_payload is not None and "original_input_payload" in previous_follow_up_payload
                else source_input_payload if source_input_payload is not None else context.state.input_payload
            ),
            "requested_tool_calls": [dict(tool_call) for tool_call in requested_tool_calls],
            "pending_tool_calls": [dict(tool_call) for tool_call in pending_tool_calls],
            "tool_name": tool_name,
            "tool_history": [*prior_tool_history, current_tool_entry],
            **current_tool_entry,
            "follow_up_context": {
                "executor_node_id": self.id,
                "run_id": context.state.run_id,
                "graph_id": context.state.graph_id,
                "tool_history_length": len(prior_tool_history) + 1,
                "pending_tool_call_count": len(pending_tool_calls),
            },
        }
        return normalized_payload, source_tool_call_envelope, terminal_output_envelope

    def _build_follow_up_request(
        self,
        context: NodeContext,
        normalized_payload: Mapping[str, Any],
        *,
        forbidden_tool_call_signatures: set[str] | None = None,
        force_response_mode: str | None = None,
        include_available_tools: bool = True,
    ) -> ModelRequest:
        forbidden_signatures = forbidden_tool_call_signatures or set()
        metadata_bindings = dict(self.config.get("metadata_bindings", {}))
        metadata: dict[str, Any] = {}
        for key, binding in metadata_bindings.items():
            if isinstance(binding, Mapping):
                metadata[key] = context.resolve_binding(binding)
            else:
                metadata[key] = binding
        available_tool_payloads: list[dict[str, Any]] = []
        if include_available_tools:
            allowed_tool_names = list(self.config.get("allowed_tool_names", []))
            available_tool_payloads = context.available_tool_definitions(allowed_tool_names)
        available_tools = [
            ModelToolDefinition(
                name=str(tool.get("name", "")),
                description=str(tool.get("description", "")),
                input_schema=dict(tool.get("input_schema", {})),
            )
            for tool in available_tool_payloads
            if isinstance(tool, Mapping) and isinstance(tool.get("name"), str) and isinstance(tool.get("input_schema"), Mapping)
        ]
        response_mode = force_response_mode or self._configured_follow_up_response_mode()
        mcp_available_tool_names = sorted(
            {
                str(tool.get("name", "")).strip()
                for tool in available_tool_payloads
                if str(tool.get("source_type", "")).strip() == "mcp" and str(tool.get("name", "")).strip()
            }
        )
        preferred_tool_name = str(self.config.get("preferred_tool_name", "") or "").strip()
        resolved_preferred_tool_name = (
            context.services.tool_registry.canonical_name_for(preferred_tool_name) if preferred_tool_name else None
        )
        if response_mode != "message" and not available_tools:
            response_mode = "message"
        metadata["available_tools"] = available_tool_payloads
        metadata["mcp_available_tool_names"] = mcp_available_tool_names
        metadata["forbidden_tool_call_signatures"] = sorted(forbidden_signatures)
        metadata["mode"] = self.config.get("mode", self.config.get("prompt_name", "mcp_executor_follow_up"))
        metadata["preferred_tool_name"] = resolved_preferred_tool_name
        metadata["response_mode"] = response_mode
        metadata["original_input_payload"] = normalized_payload.get("original_input_payload")
        metadata["tool_history"] = normalized_payload.get("tool_history", [])
        metadata["last_tool_name"] = normalized_payload.get("tool_name")
        metadata["last_tool_status"] = normalized_payload.get("tool_status")
        metadata["input_payload"] = normalized_payload
        system_prompt_template = str(self.config.get("system_prompt", ""))
        user_template = str(self.config.get("user_message_template", "{input_payload}"))
        system_prompt = context.render_template(system_prompt_template, metadata)
        mcp_tool_prompt = str(metadata.get("mcp_tool_context_prompt", "") or "").strip()
        mcp_prompt_sections: list[str] = []
        if mcp_available_tool_names:
            guidance_lines = [
                "MCP Tool Guidance",
                "Use MCP tools only when a listed live capability is needed to answer the request or complete the task.",
                "Call only MCP tools that are explicitly exposed to you and follow their schemas exactly.",
                "Do not invent MCP tool names or arguments.",
                "Do not repeat a successful MCP tool call already present in tool_history.",
                "If no exposed MCP tool is necessary, continue without calling one.",
            ]
            mcp_prompt_sections.append("\n".join(guidance_lines))
        if forbidden_signatures:
            mcp_prompt_sections.append(
                "Do not repeat any already satisfied MCP tool call signatures from tool_history. "
                + "Forbidden successful call signatures for this step: "
                + ", ".join(sorted(forbidden_signatures))
            )
        if isinstance(normalized_payload.get("repair_context"), Mapping):
            mcp_prompt_sections.append(
                "\n".join(
                    [
                        "MCP Tool Repair",
                        "input_payload.repair_context contains validation feedback for an invalid MCP tool decision or tool payload.",
                        "Use that feedback to repair the schema and return exactly one corrected exposed MCP tool call when more live MCP data is required.",
                        "Do not repeat malformed arguments unchanged.",
                    ]
                )
            )
        elif str(normalized_payload.get("tool_status", "")).strip() == "validation_error":
            mcp_prompt_sections.append(
                "\n".join(
                    [
                        "MCP Tool Repair",
                        "The last MCP tool call failed schema validation.",
                        "Inspect tool_error and tool_history carefully, then return exactly one corrected exposed MCP tool call if a repair is needed.",
                    ]
                )
            )
        if not include_available_tools:
            mcp_prompt_sections.append(
                "No further tool calls are allowed in this step. Use tool_history to produce the final answer."
            )
        if mcp_tool_prompt:
            mcp_prompt_sections.append(f"MCP Tool Context\n{mcp_tool_prompt}")
        if mcp_prompt_sections:
            appended_prompt = "\n\n".join(section.strip() for section in mcp_prompt_sections if section.strip())
            if system_prompt.strip():
                system_prompt = f"{system_prompt.rstrip()}\n\n{appended_prompt}"
            else:
                system_prompt = appended_prompt
        decision_response_schema = api_decision_response_schema(
            final_message_schema=self.config.get("response_schema")
            if isinstance(self.config.get("response_schema"), Mapping)
            else None,
            available_tools=available_tools,
            allow_tool_calls=response_mode != "message" and bool(available_tools),
            response_mode=response_mode,
        )
        return ModelRequest(
            prompt_name=str(self.config.get("prompt_name", "mcp_executor_follow_up")),
            messages=[
                ModelMessage(role="system", content=system_prompt),
                ModelMessage(role="user", content=context.render_template(user_template, metadata)),
            ],
            response_schema=decision_response_schema,
            provider_config=self._provider_config(context),
            available_tools=available_tools,
            preferred_tool_name=resolved_preferred_tool_name,
            response_mode=response_mode,
            metadata=metadata,
        )

    def _follow_up_failure_result(
        self,
        normalized_payload: Mapping[str, Any],
        source_envelope: MessageEnvelope,
        *,
        route_outputs: Mapping[str, Any] | None = None,
    ) -> NodeExecutionResult:
        output = source_envelope.to_dict()
        if isinstance(output.get("artifacts"), Mapping):
            output["artifacts"] = {
                **dict(output["artifacts"]),
                "follow_up_payload": dict(normalized_payload),
                "validation_message": (
                    f"Skipping further MCP tool checks because '{normalized_payload.get('tool_name', 'tool')}' "
                    "did not complete successfully."
                ),
            }
        return NodeExecutionResult(
            status="failed",
            output=output,
            error=source_envelope.errors[0] if source_envelope.errors else {
                "type": "mcp_executor_follow_up_failed_tool",
                "node_id": self.id,
                "tool_name": normalized_payload.get("tool_name"),
                "message": "An MCP tool call failed during executor follow-up.",
            },
            summary=f"MCP executor '{self.label}' halted after failed MCP execution.",
            metadata=dict(source_envelope.metadata),
            route_outputs=dict(route_outputs or {}),
        )

    def _follow_up_iteration_limit(self) -> int:
        raw_limit = self.config.get("max_turns", 3)
        try:
            limit = int(raw_limit)
        except (TypeError, ValueError):
            limit = 3
        return max(1, min(limit, 12))

    def _follow_up_tool_call_envelope(
        self,
        *,
        tool_call: Mapping[str, Any],
        normalized_payload: Mapping[str, Any],
        source_tool_result_envelope: MessageEnvelope,
        source_tool_call_envelope: Mapping[str, Any] | None,
        terminal_output_envelope: Mapping[str, Any] | None,
        metadata: Mapping[str, Any],
        assistant_message: str | None,
    ) -> MessageEnvelope:
        tool_artifacts: dict[str, Any] = {
            "follow_up_payload": normalized_payload,
            "source_input_payload": normalized_payload,
            "source_input_metadata": {"contract": self._FOLLOW_UP_STATE_CONTRACT, "node_kind": self.kind},
            "source_tool_result_envelope": source_tool_result_envelope.to_dict(),
            "source_tool_call_envelope": dict(source_tool_call_envelope) if isinstance(source_tool_call_envelope, Mapping) else None,
            "terminal_output_envelope": dict(terminal_output_envelope) if isinstance(terminal_output_envelope, Mapping) else None,
        }
        if assistant_message:
            tool_artifacts["assistant_message"] = assistant_message
        return MessageEnvelope(
            schema_version="1.0",
            from_node_id=self.id,
            from_category=self.category.value,
            payload=None,
            artifacts=tool_artifacts,
            tool_calls=[dict(tool_call)],
            metadata={
                "contract": "tool_call_envelope",
                **dict(metadata),
            },
        )

    def _follow_up_output_payload(
        self,
        normalized_payload: Mapping[str, Any],
        *,
        tool_calls: Sequence[Mapping[str, Any]] | None = None,
    ) -> dict[str, Any]:
        raw_tool_history = normalized_payload.get("tool_history", [])
        tool_payloads: list[dict[str, Any]] = []
        if isinstance(raw_tool_history, Sequence) and not isinstance(raw_tool_history, (str, bytes)):
            for entry in raw_tool_history:
                if not isinstance(entry, Mapping):
                    continue
                tool_payload: dict[str, Any] = {
                    "tool_name": str(entry.get("tool_name", "")).strip(),
                    "tool_arguments": dict(entry.get("tool_arguments", {}))
                    if isinstance(entry.get("tool_arguments"), Mapping)
                    else {},
                    "tool_output": entry.get("tool_output"),
                    "tool_status": str(entry.get("tool_status", "")).strip(),
                }
                if entry.get("tool_error") is not None:
                    tool_payload["tool_error"] = entry.get("tool_error")
                terminal_output = entry.get("terminal_output")
                if isinstance(terminal_output, Mapping):
                    tool_payload["terminal_output"] = dict(terminal_output)
                tool_payloads.append(tool_payload)
        return {
            "should_call_tool": bool(tool_calls),
            "tool_calls": [
                dict(tool_call)
                for tool_call in list(tool_calls or [])
                if isinstance(tool_call, Mapping)
            ],
            "tool_payloads": tool_payloads,
        }

    def _follow_up_result(
        self,
        *,
        normalized_payload: Mapping[str, Any],
        source_tool_result_envelope: MessageEnvelope,
        source_tool_call_envelope: Mapping[str, Any] | None,
        terminal_output_envelope: Mapping[str, Any] | None,
        metadata: Mapping[str, Any],
        summary: str,
        route_outputs: Mapping[str, Any] | None = None,
        tool_calls: Sequence[Mapping[str, Any]] | None = None,
    ) -> NodeExecutionResult:
        result_envelope = MessageEnvelope(
            schema_version="1.0",
            from_node_id=self.id,
            from_category=self.category.value,
            payload=self._follow_up_output_payload(normalized_payload, tool_calls=tool_calls),
            artifacts={
                "follow_up_payload": normalized_payload,
                "source_tool_result_envelope": source_tool_result_envelope.to_dict(),
                "source_tool_call_envelope": dict(source_tool_call_envelope) if isinstance(source_tool_call_envelope, Mapping) else None,
                "terminal_output_envelope": dict(terminal_output_envelope) if isinstance(terminal_output_envelope, Mapping) else None,
            },
            metadata={
                "contract": "tool_result_envelope",
                **dict(metadata),
            },
        )
        return NodeExecutionResult(
            status="success",
            output=result_envelope.to_dict(),
            summary=summary,
            metadata=result_envelope.metadata,
            route_outputs=dict(route_outputs or {}),
        )

    def _execute_follow_up(self, context: NodeContext, dispatch_result: NodeExecutionResult) -> NodeExecutionResult:
        source_envelope = self._source_envelope_from_value(dispatch_result.output)
        if source_envelope is None:
            return self._invalid_follow_up_result(
                "No MCP tool result envelope was available for follow-up evaluation.",
                route_outputs=dispatch_result.route_outputs,
            )
        if str(source_envelope.metadata.get("contract", "")).strip() != "tool_result_envelope":
            return self._invalid_follow_up_result(
                "MCP follow-up evaluation requires a tool_result_envelope.",
                route_outputs=dispatch_result.route_outputs,
            )
        normalized_payload, source_tool_call_envelope, terminal_output_envelope = self._build_follow_up_payload(context, source_envelope)
        if self._should_halt_after_tool_result(normalized_payload):
            return self._follow_up_failure_result(normalized_payload, source_envelope, route_outputs=dispatch_result.route_outputs)

        provider_name = str(self.config.get("provider_name", "") or "claude_code")
        provider = context.services.model_providers[provider_name]
        route_outputs = dict(dispatch_result.route_outputs)
        iteration_budget = self._follow_up_iteration_limit()
        allowed_tool_names = sorted(str(name).strip() for name in self.config.get("allowed_tool_names", []) if str(name).strip())
        normalized_payload = self._repair_payload_for_validation_error(
            normalized_payload,
            available_tool_names=allowed_tool_names,
        )

        while True:
            pending_tool_calls = (
                []
                if str(normalized_payload.get("tool_status", "")).strip() == "validation_error"
                else self._normalize_tool_calls(normalized_payload.get("pending_tool_calls", []))
            )
            if pending_tool_calls:
                if iteration_budget <= 0:
                    return self._invalid_follow_up_result(
                        "MCP executor follow-up exceeded the configured iteration limit.",
                        route_outputs=route_outputs,
                        details=self._validation_details(normalized_payload.get("repair_context")),
                    )
                iteration_budget -= 1
                next_tool_call = pending_tool_calls[0]
                queued_tool_metadata = {
                    "node_kind": self.kind,
                    "provider": (
                        str(source_tool_call_envelope.get("metadata", {}).get("provider", "")).strip()
                        if isinstance(source_tool_call_envelope, Mapping) and isinstance(source_tool_call_envelope.get("metadata"), Mapping)
                        else ""
                    ),
                    "prompt_name": (
                        str(source_tool_call_envelope.get("metadata", {}).get("prompt_name", "")).strip()
                        if isinstance(source_tool_call_envelope, Mapping) and isinstance(source_tool_call_envelope.get("metadata"), Mapping)
                        else ""
                    ),
                    "response_mode": "tool_call",
                    "should_call_tools": True,
                    "tool_call_count": len(pending_tool_calls),
                    "tool_name": normalized_payload.get("tool_name"),
                    "tool_status": normalized_payload.get("tool_status"),
                }
                tool_call_envelope = self._follow_up_tool_call_envelope(
                    tool_call=next_tool_call,
                    normalized_payload=normalized_payload,
                    source_tool_result_envelope=source_envelope,
                    source_tool_call_envelope=source_tool_call_envelope,
                    terminal_output_envelope=terminal_output_envelope,
                    metadata=queued_tool_metadata,
                    assistant_message=None,
                )
                dispatch_result = self._dispatch_tool_call(
                    context,
                    tool_name=str(next_tool_call.get("tool_name", "")).strip(),
                    payload=dict(next_tool_call.get("arguments", {}))
                    if isinstance(next_tool_call.get("arguments", {}), Mapping)
                    else {},
                    source_envelope=tool_call_envelope,
                )
                route_outputs.update(dispatch_result.route_outputs)
                source_envelope = self._source_envelope_from_value(dispatch_result.output)
                if source_envelope is None:
                    return self._invalid_follow_up_result(
                        "MCP executor follow-up lost the downstream tool result envelope.",
                        route_outputs=route_outputs,
                    )
                normalized_payload, source_tool_call_envelope, terminal_output_envelope = self._build_follow_up_payload(
                    context,
                    source_envelope,
                )
                if self._should_halt_after_tool_result(normalized_payload):
                    return self._follow_up_failure_result(
                        normalized_payload,
                        source_envelope,
                        route_outputs=route_outputs,
                    )
                normalized_payload = self._repair_payload_for_validation_error(
                    normalized_payload,
                    available_tool_names=allowed_tool_names,
                )
                continue
            successful_tool_call_signatures = self._successful_tool_call_signatures(normalized_payload)
            if iteration_budget <= 0:
                return self._invalid_follow_up_result(
                    "MCP executor follow-up exceeded the configured iteration limit.",
                    route_outputs=route_outputs,
                    details=self._validation_details(normalized_payload.get("repair_context")),
                )
            iteration_budget -= 1
            request = self._build_follow_up_request(
                context,
                normalized_payload,
                forbidden_tool_call_signatures=successful_tool_call_signatures,
                include_available_tools=self._retries_enabled(),
            )
            metadata = request.metadata
            response_mode = request.response_mode
            available_tool_payloads = list(metadata.get("available_tools", []))
            callable_tool_names = {
                str(tool.get("name", "")).strip()
                for tool in available_tool_payloads
                if isinstance(tool, Mapping) and str(tool.get("name", "")).strip()
            }
            response = provider.generate(request)
            normalized_decision_output: Mapping[str, Any] | None = None
            try:
                normalized_decision_output = _canonicalize_api_decision_tool_names(
                    normalize_api_decision_output(
                        response.structured_output,
                        content=response.content,
                        tool_calls=response.tool_calls,
                    ),
                    context.services.tool_registry,
                )
                decision_output = validate_api_decision_output(
                    normalized_decision_output,
                    decision_schema=request.response_schema if isinstance(request.response_schema, Mapping) else None,
                    available_tools=request.available_tools,
                    callable_tool_names=callable_tool_names,
                    response_mode=response_mode,
                )
            except ValueError as exc:
                normalized_payload = self._repair_context_payload(
                    normalized_payload,
                    repair_type="follow_up_decision_validation_error",
                    message=str(exc),
                    validation_details=self._validation_details(getattr(exc, "details", None)),
                    attempted_decision=normalized_decision_output,
                    attempted_tool_call=(
                        normalized_decision_output["tool_calls"][0]
                        if isinstance(normalized_decision_output, Mapping)
                        and isinstance(normalized_decision_output.get("tool_calls"), list)
                        and normalized_decision_output.get("tool_calls")
                        and isinstance(normalized_decision_output["tool_calls"][0], Mapping)
                        else None
                    ),
                    available_tool_names=sorted(callable_tool_names),
                )
                continue
            normalized_payload = self._clear_repair_context(normalized_payload)
            normalized_tool_calls = [
                tool_call
                for tool_call in list(decision_output["tool_calls"])
                if self._tool_call_signature(str(tool_call["tool_name"]), tool_call.get("arguments"))
                not in successful_tool_call_signatures
            ]
            base_metadata = {
                "node_kind": self.kind,
                "provider": provider.name,
                "prompt_name": request.prompt_name,
                "response_mode": response_mode,
                "should_call_tools": bool(normalized_tool_calls),
                "tool_call_count": len(normalized_tool_calls),
                "tool_name": normalized_payload.get("tool_name"),
                "tool_status": normalized_payload.get("tool_status"),
                **response.metadata,
            }
            if normalized_tool_calls:
                next_tool_call = normalized_tool_calls[0]
                tool_call_envelope = self._follow_up_tool_call_envelope(
                    tool_call=next_tool_call,
                    normalized_payload=normalized_payload,
                    source_tool_result_envelope=source_envelope,
                    source_tool_call_envelope=source_tool_call_envelope,
                    terminal_output_envelope=terminal_output_envelope,
                    metadata=base_metadata,
                    assistant_message=response.content,
                )
                dispatch_result = self._dispatch_tool_call(
                    context,
                    tool_name=str(next_tool_call.get("tool_name", "")).strip(),
                    payload=dict(next_tool_call.get("arguments", {})) if isinstance(next_tool_call.get("arguments", {}), Mapping) else {},
                    source_envelope=tool_call_envelope,
                )
                route_outputs.update(dispatch_result.route_outputs)
                source_envelope = self._source_envelope_from_value(dispatch_result.output)
                if source_envelope is None:
                    return self._invalid_follow_up_result(
                        "MCP executor follow-up lost the downstream tool result envelope.",
                        route_outputs=route_outputs,
                    )
                normalized_payload, source_tool_call_envelope, terminal_output_envelope = self._build_follow_up_payload(
                    context,
                    source_envelope,
                )
                if self._should_halt_after_tool_result(normalized_payload):
                    return self._follow_up_failure_result(
                        normalized_payload,
                        source_envelope,
                        route_outputs=route_outputs,
                    )
                normalized_payload = self._repair_payload_for_validation_error(
                    normalized_payload,
                    available_tool_names=sorted(callable_tool_names),
                )
                continue

            return self._follow_up_result(
                normalized_payload=normalized_payload,
                source_tool_result_envelope=source_envelope,
                source_tool_call_envelope=source_tool_call_envelope,
                terminal_output_envelope=terminal_output_envelope,
                metadata=base_metadata,
                summary=f"MCP executor '{self.label}' completed follow-up evaluation.",
                route_outputs=route_outputs,
                tool_calls=normalized_tool_calls,
            )

    def execute(self, context: NodeContext) -> NodeExecutionResult:
        source_envelope, tool_name, payload = self._resolve_tool_call_input(context)
        dispatch_result = self._dispatch_tool_call(
            context,
            tool_name=tool_name,
            payload=payload,
            source_envelope=source_envelope,
        )
        if not bool(self.config.get("enable_follow_up_decision", False)) or not self._retries_enabled():
            return dispatch_result
        return self._execute_follow_up(context, dispatch_result)

    def runtime_input_preview(self, context: NodeContext) -> Any:
        _, tool_name, payload = self._resolve_tool_call_input(context)
        return {
            "tool_name": tool_name,
            "arguments": payload,
            "follow_up_enabled": bool(self.config.get("enable_follow_up_decision", False)),
            "retries_enabled": self._retries_enabled(),
            "provider_name": self.config.get("provider_name", "claude_code"),
            "response_mode": self._configured_follow_up_response_mode(),
        }


def _payload_from_bound_value(bound_value: Any) -> Any:
    if isinstance(bound_value, Mapping) and "payload" in bound_value:
        return bound_value.get("payload")
    return bound_value


def _output_source_value_for_prompt_capture(context: NodeContext, binding: Mapping[str, Any] | None) -> Any:
    source_value = context.resolve_binding(binding)
    if _generation_prompt_capture_from_value(source_value) is not None:
        return source_value
    if not isinstance(binding, Mapping):
        return source_value
    binding_type = str(binding.get("type", "") or "").strip()
    if binding_type == "latest_payload":
        source_id = str(binding.get("source", "") or "").strip()
        if source_id:
            envelope = context.latest_envelope(source_id)
            if envelope is not None:
                return envelope.to_dict()
        return source_value
    if binding_type == "first_available_payload":
        for source_id in context._binding_sources_in_resolution_order(binding.get("sources", [])):
            envelope = context.latest_envelope(source_id)
            if envelope is None:
                continue
            captured_value = envelope.to_dict()
            if _generation_prompt_capture_from_value(captured_value) is not None:
                return captured_value
    return source_value


def _resolve_output_payload(context: NodeContext, binding: Mapping[str, Any] | None) -> Any:
    return _payload_from_bound_value(context.resolve_binding(binding))


def _coerce_discord_message_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, Mapping):
        for key in ("message", "content", "text", "summary"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
    return _json_safe(value).strip()


def _coerce_outlook_body_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, Mapping):
        for key in ("body", "content", "text", "message", "summary"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
    return _json_safe(value).strip()


def _normalize_outlook_text_formatting(value: str) -> str:
    normalized = value.replace("\r\n", "\n").replace("\r", "\n")
    normalized = normalized.replace("\\r\\n", "\n").replace("\\n", "\n").replace("\\t", "\t")
    return normalized.strip()


def _parse_outlook_message_content(value: Any) -> tuple[str, str]:
    subject = ""
    body_source = value
    if isinstance(value, Mapping):
        subject_candidate = value.get("subject")
        if isinstance(subject_candidate, str) and subject_candidate.strip():
            subject = _normalize_outlook_text_formatting(subject_candidate)
        for key in ("body", "content", "text", "message", "summary"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                body_source = candidate
                break
    body = _normalize_outlook_text_formatting(_coerce_outlook_body_text(body_source))
    subject_match = re.match(r"(?is)^subject:\s*(.+?)(?:\n+|$)", body)
    if subject_match is not None:
        parsed_subject = _normalize_outlook_text_formatting(subject_match.group(1))
        if parsed_subject:
            subject = parsed_subject
        body = body[subject_match.end() :].lstrip("\n").strip()
    return subject, body


def _outlook_template_variables(payload: Any) -> dict[str, Any]:
    variables: dict[str, Any] = {
        "message_payload": payload,
        "message_json": _json_safe(payload),
    }
    if isinstance(payload, Mapping):
        for key, value in payload.items():
            normalized_key = str(key).strip()
            if normalized_key and normalized_key not in variables:
                variables[normalized_key] = value
    return variables


def _extract_outlook_recipient_value(payload: Any) -> Any | None:
    if isinstance(payload, Mapping):
        metadata = payload.get("metadata")
        if isinstance(metadata, Mapping):
            metadata_email = metadata.get("resolved_email")
            if metadata_email:
                return metadata_email
        nested_payload = payload.get("payload")
        nested_candidate = _extract_outlook_recipient_value(nested_payload)
        if nested_candidate is not None:
            return nested_candidate
        for key in ("to_recipients", "toRecipients", "to", "email", "recipient_email"):
            candidate = payload.get(key)
            if candidate is not None:
                return candidate
        for key in ("recipient", "contact", "person"):
            nested_candidate = _extract_outlook_recipient_value(payload.get(key))
            if nested_candidate is not None:
                return nested_candidate
    return None


class OutputNode(BaseNode):
    kind = "output"

    def __init__(
        self,
        node_id: str,
        label: str,
        provider_id: str = "core.output",
        provider_label: str = "Core Output Node",
        description: str = "",
        config: Mapping[str, Any] | None = None,
        position: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(
            node_id=node_id,
            label=label,
            category=NodeCategory.END,
            provider_id=provider_id,
            provider_label=provider_label,
            description=description,
            config=config,
            position=position,
        )

    def execute(self, context: NodeContext) -> NodeExecutionResult:
        output = _resolve_output_payload(context, self.config.get("source_binding"))
        return NodeExecutionResult(status="success", output=output, summary="Output prepared.")

    def runtime_input_preview(self, context: NodeContext) -> Any:
        return _resolve_output_payload(context, self.config.get("source_binding"))


class EndAgentRunNode(OutputNode):
    def __init__(
        self,
        node_id: str,
        label: str,
        provider_id: str = END_AGENT_RUN_PROVIDER_ID,
        provider_label: str = "End Agent Run",
        description: str = "",
        config: Mapping[str, Any] | None = None,
        position: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(
            node_id=node_id,
            label=label,
            provider_id=provider_id,
            provider_label=provider_label,
            description=description,
            config=config,
            position=position,
        )

    def execute(self, context: NodeContext) -> NodeExecutionResult:
        output = _resolve_output_payload(context, self.config.get("source_binding"))
        return NodeExecutionResult(
            status="success",
            output=output,
            summary="Run termination prepared.",
            metadata={"terminate_run": True},
        )


class DiscordOutputNode(OutputNode):
    def __init__(
        self,
        node_id: str,
        label: str,
        provider_id: str = DISCORD_END_PROVIDER_ID,
        provider_label: str = "Discord Message End",
        description: str = "",
        config: Mapping[str, Any] | None = None,
        position: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(
            node_id=node_id,
            label=label,
            provider_id=provider_id,
            provider_label=provider_label,
            description=description,
            config=config,
            position=position,
        )

    def _resolved_channel_id(self, context: NodeContext) -> str:
        return str(context.resolve_graph_env_value(self.config.get("discord_channel_id", ""))).strip()

    def _resolved_bot_token(self, context: NodeContext) -> str:
        token_reference = str(self.config.get("discord_bot_token_env_var", DEFAULT_DISCORD_BOT_TOKEN_ENV_VAR))
        return resolve_graph_process_env(token_reference, context.graph_env_vars())

    def _render_message_content(self, context: NodeContext, payload: Any) -> str:
        template = str(self.config.get("message_template", "") or "").strip()
        if template:
            rendered = context.render_template(
                template,
                {
                    "message_payload": payload,
                    "message_json": _json_safe(payload),
                    "discord_channel_id": self._resolved_channel_id(context),
                },
            ).strip()
            if rendered:
                return rendered
        content = _coerce_discord_message_text(payload)
        if content:
            return content
        raise ValueError("Discord output node could not derive message content from the resolved payload.")

    def execute(self, context: NodeContext) -> NodeExecutionResult:
        payload = _resolve_output_payload(context, self.config.get("source_binding"))
        channel_id = self._resolved_channel_id(context)
        sender = context.services.discord_message_sender or DiscordMessageSender()
        delivery = sender.send_message(
            token=self._resolved_bot_token(context),
            channel_id=channel_id,
            content=self._render_message_content(context, payload),
        )
        return NodeExecutionResult(
            status="success",
            output={
                "delivery_status": "sent",
                "channel_id": delivery.channel_id,
                "message_id": delivery.message_id,
                "content": delivery.content,
                "timestamp": delivery.timestamp,
                "source_payload": payload,
            },
            summary=f"Sent Discord message to channel '{delivery.channel_id}'.",
            metadata={"skip_final_output_promotion": True, "discord_delivery": True},
        )

    def runtime_input_preview(self, context: NodeContext) -> Any:
        payload = _resolve_output_payload(context, self.config.get("source_binding"))
        return {
            "channel_id": self._resolved_channel_id(context),
            "content": self._render_message_content(context, payload),
        }


class OutlookDraftOutputNode(OutputNode):
    def __init__(
        self,
        node_id: str,
        label: str,
        provider_id: str = OUTLOOK_DRAFT_PROVIDER_ID,
        provider_label: str = "Outlook Draft End",
        description: str = "",
        config: Mapping[str, Any] | None = None,
        position: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(
            node_id=node_id,
            label=label,
            provider_id=provider_id,
            provider_label=provider_label,
            description=description,
            config=config,
            position=position,
        )

    def _auth_service(self, context: NodeContext) -> MicrosoftAuthService:
        return context.services.microsoft_auth_service or MicrosoftAuthService()

    def _acquire_access_token(self, auth_service: MicrosoftAuthService) -> str:
        max_retries = _coerce_int(self.config.get("auth_max_retries"), default=2, minimum=0)
        backoff_seconds = _coerce_float(self.config.get("auth_retry_backoff_seconds"), default=1.0, minimum=0.0)
        attempt = 0
        while True:
            try:
                return auth_service.acquire_access_token()
            except Exception as exc:  # noqa: BLE001
                if not _is_timeout_like_exception(exc) or attempt >= max_retries:
                    raise
                time.sleep(backoff_seconds * (2 ** attempt))
                attempt += 1

    def _require_to(self) -> bool:
        return bool(self.config.get("require_to", True))

    def _require_subject(self) -> bool:
        return bool(self.config.get("require_subject", True))

    def _require_body(self) -> bool:
        return bool(self.config.get("require_body", True))

    def _resolved_to_recipients(self, context: NodeContext, payload: Any) -> list[str]:
        configured_to = str(self.config.get("to", "") or "").strip()
        candidate_value: Any
        if configured_to:
            candidate_value = context.render_template(
                configured_to,
                _outlook_template_variables(payload),
            ).strip()
        else:
            candidate_value = _extract_outlook_recipient_value(payload)
        return parse_outlook_recipient_addresses(
            candidate_value,
            required=self._require_to(),
        )

    def _render_subject(self, context: NodeContext, payload: Any) -> str:
        subject_template = str(self.config.get("subject", "") or "").strip()
        derived_subject, _ = _parse_outlook_message_content(payload)
        if subject_template:
            subject = context.render_template(
                subject_template,
                _outlook_template_variables(payload),
            ).strip()
        else:
            subject = derived_subject
        if not subject:
            if not self._require_subject():
                return ""
            raise ValueError("Outlook draft node requires a subject.")
        return subject

    def _render_body(self, payload: Any) -> str:
        _, body = _parse_outlook_message_content(payload)
        if not body:
            if not self._require_body():
                return ""
            raise ValueError("Outlook draft node could not derive body text from the resolved payload.")
        return body

    def _resolve_iteration_source_file(self, context: NodeContext, payload: Any) -> str:
        iteration_context = context.current_iteration_context()
        iterator_node_id = str(iteration_context.get("iterator_node_id", "") or "").strip()
        if iterator_node_id:
            iterator_state = context.state.iterator_states.get(iterator_node_id)
            if isinstance(iterator_state, Mapping):
                source_file = str(iterator_state.get("source_file", "") or "").strip()
                if source_file:
                    return source_file
        if isinstance(payload, Mapping):
            source_file = str(payload.get("source_file", "") or "").strip()
            if source_file:
                return source_file
        return ""

    def _dedupe_scope(
        self,
        context: NodeContext,
        *,
        payload: Any,
        recipients: list[str],
        subject: str,
        body: str,
    ) -> OutlookDraftDeduplicationScope | None:
        iteration_context = context.current_iteration_context()
        iterator_node_id = str(iteration_context.get("iterator_node_id", "") or "").strip()
        iteration_id = str(iteration_context.get("iteration_id", "") or "").strip()
        if not iterator_node_id or not iteration_id:
            return None
        return OutlookDraftDeduplicationScope(
            graph_id=context.state.graph_id,
            node_id=self.id,
            iterator_node_id=iterator_node_id,
            iteration_id=iteration_id,
            source_file=self._resolve_iteration_source_file(context, payload),
            recipients=list(recipients),
            agent_id=str(context.state.agent_id or ""),
            subject=subject,
            body=body,
        )

    def _outbound_email_logger_binding(self, context: NodeContext) -> OutboundEmailLoggerBinding | None:
        for edge in context.graph.get_incoming_edges(self.id):
            if edge.kind != "binding":
                continue
            candidate = context.graph.nodes.get(edge.source_id)
            if candidate is None or candidate.provider_id != OUTBOUND_EMAIL_LOGGER_PROVIDER_ID:
                continue
            supabase_url_env_var, supabase_key_env_var = resolve_supabase_runtime_env_var_names(candidate.config, context.graph)
            return OutboundEmailLoggerBinding(
                node_id=candidate.id,
                schema=str(candidate.config.get("schema", "public") or "public").strip() or "public",
                table_name=str(candidate.config.get("table_name", "") or "").strip(),
                supabase_url=resolve_graph_process_env(
                    supabase_url_env_var,
                    context.graph.env_vars,
                ),
                supabase_key=resolve_graph_process_env(
                    supabase_key_env_var,
                    context.graph.env_vars,
                ),
                message_type=str(candidate.config.get("message_type", "initial") or "initial").strip().lower() or "initial",
                outreach_step=_coerce_int(candidate.config.get("outreach_step"), default=0, minimum=0),
                sales_approach_template=str(candidate.config.get("sales_approach", "") or ""),
                sales_approach_version_template=str(candidate.config.get("sales_approach_version", "") or ""),
                parent_outbound_email_id_template=str(candidate.config.get("parent_outbound_email_id", "") or ""),
                root_outbound_email_id_template=str(candidate.config.get("root_outbound_email_id", "") or ""),
                metadata_json_template=str(candidate.config.get("metadata_json", "{}") or "{}"),
            )
        return None

    def _validate_outbound_email_logger_binding(self, binding: OutboundEmailLoggerBinding) -> dict[str, Any]:
        sources = fetch_supabase_schema_catalog(
            supabase_url=binding.supabase_url,
            supabase_key=binding.supabase_key,
            schema=binding.schema,
        )
        validation = validate_outbound_email_log_schema(
            sources=sources,
            schema=binding.schema,
            table_name=binding.table_name,
        )
        if validation.valid:
            return validation.to_dict()
        details: list[str] = []
        if validation.missing_required_columns:
            details.append("missing required columns: " + ", ".join(validation.missing_required_columns))
        if validation.type_mismatches:
            details.append(
                "type mismatches: "
                + ", ".join(
                    f"{mismatch.column_name} expected {'/'.join(mismatch.expected_types)} but found {mismatch.actual_type}"
                    for mismatch in validation.type_mismatches
                )
            )
        if validation.warnings:
            details.extend(validation.warnings)
        raise SupabaseDataError(
            (
                f"Outbound email logger binding cannot use Supabase table '{binding.table_name}'. "
                + ("; ".join(details) if details else "Choose a table that matches the outbound email schema.")
            ).strip(),
            error_type="invalid_outbound_email_log_table",
            details=validation.to_dict(),
        )

    def _render_outbound_email_logger_value(self, context: NodeContext, template: str, extra: Mapping[str, Any]) -> str:
        normalized_template = str(template or "")
        if not normalized_template.strip():
            return ""
        return context.render_template(normalized_template, extra).strip()

    def _render_outbound_email_logger_metadata_value(
        self,
        context: NodeContext,
        value: Any,
        extra: Mapping[str, Any],
    ) -> Any:
        if isinstance(value, str):
            return context.render_template(value, extra)
        if isinstance(value, Mapping):
            rendered: dict[str, Any] = {}
            for raw_key, raw_value in value.items():
                rendered_key = context.render_template(str(raw_key), extra)
                rendered[rendered_key] = self._render_outbound_email_logger_metadata_value(context, raw_value, extra)
            return rendered
        if isinstance(value, list):
            return [self._render_outbound_email_logger_metadata_value(context, item, extra) for item in value]
        return value

    def _render_outbound_email_logger_metadata(
        self,
        context: NodeContext,
        binding: OutboundEmailLoggerBinding,
        extra: Mapping[str, Any],
    ) -> dict[str, Any]:
        metadata_template = str(binding.metadata_json_template or "").strip()
        if not metadata_template:
            return {}
        try:
            parsed_template = json.loads(metadata_template)
        except json.JSONDecodeError:
            parsed_template = None
        if isinstance(parsed_template, Mapping):
            try:
                rendered_metadata = self._render_outbound_email_logger_metadata_value(context, parsed_template, extra)
            except ValueError as exc:
                raise SupabaseDataError(
                    "Outbound email logger metadata_json contains an invalid template expression.",
                    error_type="invalid_outbound_email_logger_metadata",
                    details={"node_id": binding.node_id},
                ) from exc
            return {str(key): value for key, value in rendered_metadata.items()}
        if parsed_template is not None:
            raise SupabaseDataError(
                "Outbound email logger metadata_json must resolve to a JSON object.",
                error_type="invalid_outbound_email_logger_metadata",
                details={"node_id": binding.node_id},
            )
        rendered = context.render_template(metadata_template, extra).strip()
        if not rendered:
            return {}
        try:
            parsed = json.loads(rendered)
        except json.JSONDecodeError as exc:
            raise SupabaseDataError(
                "Outbound email logger metadata_json must be valid JSON after template rendering.",
                error_type="invalid_outbound_email_logger_metadata",
                details={"node_id": binding.node_id},
            ) from exc
        if not isinstance(parsed, Mapping):
            raise SupabaseDataError(
                "Outbound email logger metadata_json must resolve to a JSON object.",
                error_type="invalid_outbound_email_logger_metadata",
                details={"node_id": binding.node_id},
            )
        return {str(key): value for key, value in parsed.items()}

    def _write_outbound_email_log(
        self,
        context: NodeContext,
        *,
        source_value: Any,
        payload: Any,
        auth_status: MicrosoftAuthStatus,
        draft: Any,
        binding: OutboundEmailLoggerBinding,
        validation: Mapping[str, Any],
    ) -> dict[str, Any]:
        available_columns = set(validation.get("available_columns", []))
        raw_provider_payload = dict(draft.raw_response) if isinstance(draft.raw_response, Mapping) else {}
        provider_message_id = str(raw_provider_payload.get("id", draft.draft_id) or draft.draft_id or "").strip()
        internet_message_id = str(raw_provider_payload.get("internetMessageId", "") or "").strip()
        conversation_id = str(raw_provider_payload.get("conversationId", "") or "").strip()
        original_to_recipients = list(draft.to_recipients) if draft.to_recipients else []
        primary_recipient = str(draft.to_recipients[0] if draft.to_recipients else "").strip().lower()
        recipient_missing = not primary_recipient
        if recipient_missing:
            # Track the draft anyway with a non-deliverable sentinel so the outbound log
            # has a record of every draft attempt. `.invalid` is RFC 6761 reserved and is
            # guaranteed not to resolve, so this can never accidentally route real mail.
            primary_recipient = MISSING_RECIPIENT_SENTINEL_EMAIL

        generation_prompt = _generation_prompt_capture_from_value(source_value)
        metadata_extra = {
            "message_payload": payload,
            "message_json": _json_safe(payload),
            "draft_id": draft.draft_id,
            "provider_message_id": provider_message_id,
            "internet_message_id": internet_message_id,
            "conversation_id": conversation_id,
            "recipient_email": primary_recipient,
            "to_recipients": original_to_recipients,
            "mailbox_account": auth_status.account_username,
            "draft_created_at": draft.created_at or utc_now_iso(),
            "run_id": context.state.run_id,
            "graph_id": context.state.graph_id,
            "recipient_missing": recipient_missing,
        }
        if generation_prompt is not None:
            metadata_extra["generation_prompt"] = generation_prompt
            metadata_extra["generation_prompt_name"] = generation_prompt.get("prompt_name", "")
            metadata_extra["generation_source_node_id"] = generation_prompt.get("source_node_id", "")
            metadata_extra["generation_system_prompt"] = generation_prompt.get("system_prompt", "")
            metadata_extra["generation_user_prompt"] = generation_prompt.get("user_prompt", "")
        sales_approach = self._render_outbound_email_logger_value(context, binding.sales_approach_template, metadata_extra)
        if binding.message_type not in {"initial", "follow_up"}:
            raise SupabaseDataError(
                f"Outbound email logger uses unsupported message_type '{binding.message_type}'.",
                error_type="invalid_outbound_email_log_message_type",
                details={"node_id": binding.node_id},
            )
        sales_approach_version = self._render_outbound_email_logger_value(
            context,
            binding.sales_approach_version_template,
            metadata_extra,
        )
        parent_outbound_email_id = self._render_outbound_email_logger_value(
            context,
            binding.parent_outbound_email_id_template,
            metadata_extra,
        )
        root_outbound_email_id = self._render_outbound_email_logger_value(
            context,
            binding.root_outbound_email_id_template,
            metadata_extra,
        )
        metadata = {
            "run_id": context.state.run_id,
            "graph_id": context.state.graph_id,
            "logger_node_id": binding.node_id,
            "to_recipients": original_to_recipients,
            "web_link": draft.web_link,
            "last_modified_at": draft.last_modified_at,
            "source_payload": payload,
        }
        if recipient_missing:
            metadata["recipient_missing"] = True
            metadata["recipient_substitution"] = primary_recipient
        metadata.update(self._render_outbound_email_logger_metadata(context, binding, metadata_extra))
        if generation_prompt is not None:
            metadata["generation_prompt"] = generation_prompt

        row: dict[str, Any] = {}

        def include(column_name: str, value: Any) -> None:
            if column_name in available_columns:
                row[column_name] = value

        def include_if_present(column_name: str, value: Any) -> None:
            if value is None:
                return
            if isinstance(value, str) and not value.strip():
                return
            include(column_name, value)

        include("provider", "outlook")
        include_if_present("source_run_id", context.state.run_id)
        include_if_present("mailbox_account", auth_status.account_username)
        include("recipient_email", primary_recipient)
        include("subject", draft.subject)
        include("body_text", draft.body)
        include("message_type", binding.message_type)
        include("outreach_step", binding.outreach_step)
        if sales_approach:
            include("sales_approach", sales_approach)
        include_if_present("provider_draft_id", draft.draft_id)
        include_if_present("provider_message_id", provider_message_id)
        include_if_present("internet_message_id", internet_message_id)
        include_if_present("conversation_id", conversation_id)
        include("drafted_at", draft.created_at or utc_now_iso())
        include("metadata", metadata)
        include("raw_provider_payload", raw_provider_payload)
        if sales_approach_version:
            include("sales_approach_version", sales_approach_version)
        if parent_outbound_email_id:
            include("parent_outbound_email_id", parent_outbound_email_id)
        if root_outbound_email_id:
            include("root_outbound_email_id", root_outbound_email_id)

        result = self._write_outbound_email_log_row(
            binding=binding,
            row=row,
        )
        return {
            "schema": result.schema,
            "table_name": result.table_name,
            "row_count": result.row_count,
            "inserted_row": result.inserted_row,
            "written_columns": sorted(result.inserted_row.keys()),
            "recipient_missing": recipient_missing,
        }

    def _write_outbound_email_log_row(
        self,
        *,
        binding: OutboundEmailLoggerBinding,
        row: Mapping[str, Any],
    ) -> SupabaseRowWriteResult:
        try:
            return write_supabase_row(
                SupabaseRowWriteRequest(
                    supabase_url=binding.supabase_url,
                    supabase_key=binding.supabase_key,
                    schema=binding.schema,
                    table_name=binding.table_name,
                    row=dict(row),
                    write_mode="insert",
                    returning="representation",
                )
            )
        except SupabaseDataError as exc:
            if not self._should_retry_outbound_email_log_without_source_run_id(exc, row):
                raise
            fallback_row = dict(row)
            fallback_row.pop("source_run_id", None)
            return write_supabase_row(
                SupabaseRowWriteRequest(
                    supabase_url=binding.supabase_url,
                    supabase_key=binding.supabase_key,
                    schema=binding.schema,
                    table_name=binding.table_name,
                    row=fallback_row,
                    write_mode="insert",
                    returning="representation",
                )
            )

    def _should_retry_outbound_email_log_without_source_run_id(
        self,
        error: SupabaseDataError,
        row: Mapping[str, Any],
    ) -> bool:
        if "source_run_id" not in row:
            return False
        if error.error_type != "supabase_write_request_failed":
            return False
        message = str(error)
        return (
            ("23503" in message or "foreign key constraint" in message)
            and "source_run_id" in message
        )

    def _build_skipped_draft_relative_path(self, context: NodeContext) -> str:
        iteration_context = context.current_iteration_context()
        iteration_id = str(iteration_context.get("iteration_id", "") or "").strip()
        if iteration_id:
            suffix = _sanitize_workspace_path_suffix(iteration_id)
        else:
            row_index = iteration_context.get("iterator_row_index")
            if isinstance(row_index, int) and row_index > 0:
                suffix = f"row-{row_index}"
            else:
                suffix = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        node_suffix = _sanitize_workspace_path_suffix(self.id, fallback="outlook-draft")
        return f"outputs/skipped-drafts/{node_suffix}-{suffix}.json"

    def _record_skipped_invalid_recipient(
        self,
        context: NodeContext,
        payload: Any,
        exc: ValueError,
    ) -> NodeExecutionResult:
        reason = str(exc)
        skipped_record: dict[str, Any] = {
            "delivery_status": "draft_skipped_invalid_recipient",
            "reason": reason,
            "node_id": self.id,
            "graph_id": context.state.graph_id,
            "run_id": context.state.run_id,
            "agent_id": context.state.agent_id,
            "skipped_at": datetime.now(timezone.utc).isoformat(),
            "source_payload": payload,
        }
        relative_path = self._build_skipped_draft_relative_path(context)
        try:
            content = json.dumps(skipped_record, indent=2, default=str, sort_keys=True)
        except TypeError:
            content = json.dumps({**skipped_record, "source_payload": str(payload)}, indent=2, default=str, sort_keys=True)
        skipped_file_record: dict[str, Any] | None = None
        try:
            skipped_file_record = write_agent_workspace_text_file(
                context.state.run_id,
                context.state.agent_id,
                relative_path,
                content,
            )
        except Exception:  # noqa: BLE001
            skipped_file_record = None
        output_payload: dict[str, Any] = {
            "delivery_status": "draft_skipped_invalid_recipient",
            "reason": reason,
            "to_recipients": [],
            "subject": "",
            "body": "",
            "source_payload": payload,
            "skipped_draft_file": skipped_file_record,
        }
        return NodeExecutionResult(
            status="success",
            output=output_payload,
            summary=f"Skipped Outlook draft (invalid recipient): {reason}",
            metadata={
                "outlook_draft_saved": False,
                "outlook_draft_skipped": True,
                "outlook_draft_skipped_reason": "invalid_recipient",
                "outbound_email_logged": False,
                "outbound_email_log_skipped": False,
            },
        )

    def execute(self, context: NodeContext) -> NodeExecutionResult:
        source_binding = self.config.get("source_binding")
        source_value = _output_source_value_for_prompt_capture(context, source_binding)
        payload = _payload_from_bound_value(source_value)
        try:
            recipients = self._resolved_to_recipients(context, payload)
        except ValueError as exc:
            return self._record_skipped_invalid_recipient(context, payload, exc)
        subject = self._render_subject(context, payload)
        body = self._render_body(payload)
        logger_binding = self._outbound_email_logger_binding(context)
        logger_validation: dict[str, Any] | None = None
        if logger_binding is not None:
            logger_validation = self._validate_outbound_email_logger_binding(logger_binding)
        auth_service = self._auth_service(context)
        auth_status = auth_service.connection_status()
        dedupe_scope = self._dedupe_scope(
            context,
            payload=payload,
            recipients=recipients,
            subject=subject,
            body=body,
        )
        dedupe_store = OutlookDraftDedupeStore() if dedupe_scope is not None else None
        if dedupe_store is not None and dedupe_scope is not None:
            dedupe_action, dedupe_row = dedupe_store.begin_attempt(
                scope=dedupe_scope,
                run_id=context.state.run_id,
                parent_run_id=context.state.parent_run_id,
            )
            if dedupe_action == "deduped_success":
                cached_output = dedupe_store.decode_success_output(dedupe_row) or {}
                output_payload = dict(cached_output)
                output_payload["delivery_status"] = "draft_saved_deduped"
                output_payload.setdefault("subject", subject)
                output_payload.setdefault("body", body)
                output_payload.setdefault("to_recipients", list(recipients))
                output_payload.setdefault("account_username", auth_status.account_username)
                output_payload.setdefault("source_payload", payload)
                logged_outbound_email = output_payload.get("outbound_email_log")
                logged_mapping = logged_outbound_email if isinstance(logged_outbound_email, Mapping) else {}
                summary = (
                    f"Skipped duplicate Outlook draft for {', '.join(recipients)}."
                    if recipients
                    else "Skipped duplicate Outlook draft."
                )
                return NodeExecutionResult(
                    status="success",
                    output=output_payload,
                    summary=summary,
                    metadata={
                        "outlook_draft_saved": True,
                        "outlook_draft_deduped": True,
                        "outbound_email_logged": bool(logged_mapping and not logged_mapping.get("skipped")),
                        "outbound_email_log_skipped": bool(logged_mapping and logged_mapping.get("skipped")),
                    },
                )
            if dedupe_action == "deduped_in_progress":
                summary = (
                    f"Skipped duplicate Outlook draft for {', '.join(recipients)} while another attempt is in progress."
                    if recipients
                    else "Skipped duplicate Outlook draft while another attempt is in progress."
                )
                return NodeExecutionResult(
                    status="success",
                    output={
                        "delivery_status": "draft_deduped_in_progress",
                        "subject": subject,
                        "body": body,
                        "to_recipients": list(recipients),
                        "account_username": auth_status.account_username,
                        "source_payload": payload,
                    },
                    summary=summary,
                    metadata={
                        "outlook_draft_saved": False,
                        "outlook_draft_deduped": True,
                        "outlook_draft_pending": True,
                        "outbound_email_logged": False,
                        "outbound_email_log_skipped": False,
                    },
                )
        access_token = self._acquire_access_token(auth_service)
        draft_client = context.services.outlook_draft_client or OutlookDraftClient()
        signature = str(self.config.get("signature", "") or "")
        try:
            draft = draft_client.create_draft(
                access_token=access_token,
                to_recipients=recipients,
                subject=subject,
                body=body,
                signature=signature,
            )
        except Exception as exc:  # noqa: BLE001
            if dedupe_store is not None and dedupe_scope is not None:
                dedupe_store.mark_failure(
                    scope=dedupe_scope,
                    error_payload={
                        "type": type(exc).__name__,
                        "message": str(exc),
                        "run_id": context.state.run_id,
                    },
                )
            raise
        raw_provider_payload = dict(draft.raw_response) if isinstance(draft.raw_response, Mapping) else {}
        provider_message_id = str(raw_provider_payload.get("id", draft.draft_id) or draft.draft_id or "").strip()
        internet_message_id = str(raw_provider_payload.get("internetMessageId", "") or "").strip()
        conversation_id = str(raw_provider_payload.get("conversationId", "") or "").strip()
        output_payload: dict[str, Any] = {
            "delivery_status": "draft_saved",
            "draft_id": draft.draft_id,
            "provider_message_id": provider_message_id,
            "internet_message_id": internet_message_id,
            "conversation_id": conversation_id,
            "web_link": draft.web_link,
            "subject": draft.subject,
            "body": draft.body,
            "to_recipients": draft.to_recipients,
            "created_at": draft.created_at,
            "last_modified_at": draft.last_modified_at,
            "account_username": auth_status.account_username,
            "raw_provider_payload": raw_provider_payload,
            "outbound_email_log": None,
            "source_payload": payload,
        }
        if dedupe_store is not None and dedupe_scope is not None:
            dedupe_store.mark_success(
                scope=dedupe_scope,
                output_payload=output_payload,
            )
        logged_outbound_email: dict[str, Any] | None = None
        if logger_binding is not None and logger_validation is not None:
            try:
                logged_outbound_email = self._write_outbound_email_log(
                    context,
                    source_value=source_value,
                    payload=payload,
                    auth_status=auth_status,
                    draft=draft,
                    binding=logger_binding,
                    validation=logger_validation,
                )
            except SupabaseDataError as exc:
                if exc.error_type == "missing_outbound_email_log_recipient":
                    logged_outbound_email = {
                        "skipped": True,
                        "reason": "missing_recipient_email",
                        "message": str(exc),
                        "node_id": logger_binding.node_id,
                        "table_name": logger_binding.table_name,
                        "schema": logger_binding.schema,
                    }
                else:
                    error_payload = exc.to_error_payload()
                    error_payload["draft_id"] = draft.draft_id
                    error_payload["provider_draft_saved"] = True
                    return NodeExecutionResult(
                        status="failed",
                        error=error_payload,
                        summary=str(exc),
                        metadata={"outlook_draft_saved": True, "outbound_email_log_failed": True},
                    )
        output_payload["outbound_email_log"] = logged_outbound_email
        if dedupe_store is not None and dedupe_scope is not None:
            dedupe_store.mark_success(
                scope=dedupe_scope,
                output_payload=output_payload,
            )
        recipient_missing_logged = bool(logged_outbound_email and logged_outbound_email.get("recipient_missing"))
        if draft.to_recipients:
            summary = f"Saved Outlook draft for {', '.join(draft.to_recipients)}."
        elif recipient_missing_logged:
            summary = "Saved Outlook draft (no recipient attached; logged with sentinel address)."
        else:
            summary = "Saved Outlook draft."
        return NodeExecutionResult(
            status="success",
            output=output_payload,
            summary=summary,
            metadata={
                "outlook_draft_saved": True,
                "outlook_draft_deduped": False,
                "outbound_email_logged": bool(logged_outbound_email and not logged_outbound_email.get("skipped")),
                "outbound_email_log_skipped": bool(logged_outbound_email and logged_outbound_email.get("skipped")),
                "outbound_email_recipient_missing": recipient_missing_logged,
            },
        )

    def runtime_input_preview(self, context: NodeContext) -> Any:
        payload = _resolve_output_payload(context, self.config.get("source_binding"))
        auth_status = self._auth_service(context).connection_status()
        logger_binding = self._outbound_email_logger_binding(context)
        return {
            "microsoft_auth": auth_status.to_dict(),
            "to_recipients": self._resolved_to_recipients(context, payload),
            "subject": self._render_subject(context, payload),
            "body": self._render_body(payload),
            "outbound_email_logger": {
                "node_id": logger_binding.node_id,
                "schema": logger_binding.schema,
                "table_name": logger_binding.table_name,
                "message_type": logger_binding.message_type,
                "outreach_step": logger_binding.outreach_step,
            } if logger_binding is not None else None,
        }


class GraphValidationError(ValueError):
    pass


def _node_from_dict(payload: Mapping[str, Any]) -> BaseNode:
    kind = str(payload["kind"])
    provider_id = str(payload.get("provider_id", ""))
    if provider_id == SPREADSHEET_ROW_PROVIDER_ID and kind == "data":
        kind = "control_flow_unit"
    common = {
        "node_id": str(payload["id"]),
        "label": str(payload.get("label", payload["id"])),
        "description": str(payload.get("description", "")),
        "position": payload.get("position"),
        "config": payload.get("config"),
    }
    if kind == "input":
        return InputNode(
            provider_id=str(payload.get("provider_id", "core.input")),
            provider_label=str(payload.get("provider_label", "Core Input Node")),
            **common,
        )
    if kind == "data":
        return DataNode(
            provider_id=str(payload.get("provider_id", "core.data")),
            provider_label=str(payload.get("provider_label", "Core Data Node")),
            **common,
        )
    if kind == "control_flow_unit":
        return ControlFlowNode(
            provider_id=str(payload.get("provider_id", SPREADSHEET_ROW_PROVIDER_ID)),
            provider_label=str(payload.get("provider_label", "Control Flow Unit")),
            **common,
        )
    if kind == "provider":
        return ProviderNode(
            provider_name=str(payload.get("model_provider_name") or payload.get("config", {}).get("provider_name", "")),
            provider_id=str(payload.get("provider_id", "provider.generic")),
            provider_label=str(payload.get("provider_label", "Generic Model Provider")),
            **common,
        )
    if kind == "model":
        if provider_id == SPREADSHEET_MATRIX_DECISION_PROVIDER_ID:
            return SpreadsheetMatrixDecisionNode(
                provider_name=str(payload.get("model_provider_name") or payload.get("config", {}).get("provider_name", "")),
                prompt_name=str(payload.get("prompt_name") or payload.get("config", {}).get("prompt_name", "")),
                node_provider_id=provider_id,
                node_provider_label=str(payload.get("provider_label", "Spreadsheet Matrix Decision")),
                **common,
            )
        return ModelNode(
            provider_name=str(payload.get("model_provider_name") or payload.get("config", {}).get("provider_name", "")),
            prompt_name=str(payload.get("prompt_name") or payload.get("config", {}).get("prompt_name", "")),
            node_provider_id=str(payload.get("provider_id", "model.generic")),
            node_provider_label=str(payload.get("provider_label", "Generic Model Node")),
            **common,
        )
    if kind == "tool":
        config_payload = payload.get("config", {})
        include_mcp_context = bool(config_payload.get("include_mcp_tool_context", False)) if isinstance(config_payload, Mapping) else False
        if include_mcp_context:
            return McpContextProviderNode(
                provider_id=str(payload.get("provider_id", "tool.mcp_context_provider")),
                provider_label=str(payload.get("provider_label", "MCP Context Provider")),
                **common,
            )
        return ToolNode(
            tool_name=str(payload.get("tool_name") or payload.get("config", {}).get("tool_name", "")),
            provider_id=str(payload.get("provider_id", "tool.registry")),
            provider_label=str(payload.get("provider_label", "Registry Tool Node")),
            **common,
        )
    if kind == "mcp_context_provider":
        return McpContextProviderNode(
            provider_id=str(payload.get("provider_id", "tool.mcp_context_provider")),
            provider_label=str(payload.get("provider_label", "MCP Context Provider")),
            **common,
        )
    if kind == "mcp_tool_executor":
        return McpToolExecutorNode(
            provider_id=str(payload.get("provider_id", "tool.mcp_tool_executor")),
            provider_label=str(payload.get("provider_label", "MCP Tool Executor")),
            **common,
        )
    if kind == "output":
        provider_id = str(payload.get("provider_id", "core.output"))
        provider_label = str(payload.get("provider_label", "Core Output Node"))
        if provider_id == END_AGENT_RUN_PROVIDER_ID:
            return EndAgentRunNode(
                provider_id=provider_id,
                provider_label=provider_label,
                **common,
            )
        if provider_id == DISCORD_END_PROVIDER_ID:
            return DiscordOutputNode(
                provider_id=provider_id,
                provider_label=provider_label,
                **common,
            )
        if provider_id == OUTLOOK_DRAFT_PROVIDER_ID:
            return OutlookDraftOutputNode(
                provider_id=provider_id,
                provider_label=provider_label,
                **common,
            )
        return OutputNode(
            provider_id=provider_id,
            provider_label=provider_label,
            **common,
        )
    raise GraphValidationError(f"Unsupported node kind '{kind}'.")


class GraphDefinition:
    def __init__(
        self,
        graph_id: str,
        name: str,
        start_node_id: str,
        nodes: list[BaseNode],
        edges: list[Edge],
        *,
        description: str = "",
        version: str = "1.0",
        default_input: str = "",
        env_vars: Mapping[str, Any] | None = None,
        supabase_connections: Sequence[SupabaseConnectionDefinition] | None = None,
        default_supabase_connection_id: str = "",
        run_store_supabase_connection_id: str = "",
    ) -> None:
        node_ids = [node.id for node in nodes]
        if len(node_ids) != len(set(node_ids)):
            raise GraphValidationError("Graph node identifiers must be unique.")
        self.graph_id = graph_id
        self.name = name
        self.description = description
        self.version = version
        self.default_input = default_input
        self.start_node_id = start_node_id
        self.env_vars = _normalize_graph_env_vars(env_vars)
        self.supabase_connections = list(supabase_connections or [])
        self.default_supabase_connection_id = str(default_supabase_connection_id or "").strip()
        self.run_store_supabase_connection_id = str(run_store_supabase_connection_id or "").strip()
        for node in nodes:
            node.attach_graph_env_vars(self.env_vars)
        self.nodes = {node.id: node for node in nodes}
        self.edges = edges
        self.validate()

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> GraphDefinition:
        nodes = [_node_from_dict(node) for node in payload.get("nodes", [])]
        edges = [Edge.from_dict(edge) for edge in payload.get("edges", [])]
        return cls(
            graph_id=str(payload["graph_id"]),
            name=str(payload["name"]),
            description=str(payload.get("description", "")),
            version=str(payload.get("version", "1.0")),
            default_input=str(payload.get("default_input", "")),
            start_node_id=str(payload["start_node_id"]),
            env_vars=payload.get("env_vars"),
            supabase_connections=_normalize_supabase_connections(payload.get("supabase_connections")),
            default_supabase_connection_id=str(payload.get("default_supabase_connection_id", "") or "").strip(),
            run_store_supabase_connection_id=str(payload.get("run_store_supabase_connection_id", "") or "").strip(),
            nodes=nodes,
            edges=edges,
        )

    def validate(self) -> None:
        if self.start_node_id not in self.nodes:
            raise GraphValidationError(f"Unknown start node '{self.start_node_id}'.")
        if not self.nodes:
            raise GraphValidationError("Graphs must contain at least one node.")
        seen_supabase_connection_ids: set[str] = set()
        for connection in self.supabase_connections:
            if connection.connection_id in seen_supabase_connection_ids:
                raise GraphValidationError(f"Duplicate Supabase connection '{connection.connection_id}'.")
            seen_supabase_connection_ids.add(connection.connection_id)
        if self.default_supabase_connection_id and self.get_supabase_connection(self.default_supabase_connection_id) is None:
            raise GraphValidationError(
                f"Unknown default Supabase connection '{self.default_supabase_connection_id}'."
            )
        if self.run_store_supabase_connection_id and self.get_supabase_connection(self.run_store_supabase_connection_id) is None:
            raise GraphValidationError(
                f"Unknown run-store Supabase connection '{self.run_store_supabase_connection_id}'."
            )
        start_node = self.nodes[self.start_node_id]
        if start_node.category != NodeCategory.START:
            raise GraphValidationError("The graph start node must use the 'start' category.")

        output_nodes = [node for node in self.nodes.values() if node.category == NodeCategory.END]
        if not output_nodes:
            raise GraphValidationError("Graphs must include at least one 'end' category node.")
        for node in self.nodes.values():
            if node.provider_id not in {
                SUPABASE_DATA_PROVIDER_ID,
                SUPABASE_SQL_PROVIDER_ID,
                SUPABASE_TABLE_ROWS_PROVIDER_ID,
                SUPABASE_ROW_WRITE_PROVIDER_ID,
                OUTBOUND_EMAIL_LOGGER_PROVIDER_ID,
            }:
                continue
            connection_id = str(node.config.get("supabase_connection_id", "") or "").strip()
            if connection_id and self.get_supabase_connection(connection_id) is None:
                raise GraphValidationError(
                    f"Node '{node.id}' references unknown Supabase connection '{connection_id}'."
                )

        for node in self.nodes.values():
            if node.provider_id != PYTHON_SCRIPT_RUNNER_PROVIDER_ID:
                continue
            script_file_id = str(node.config.get("script_file_id", "") or "").strip()
            if not script_file_id:
                raise GraphValidationError(
                    f"Python script runner '{node.id}' requires a script_file_id."
                )
            try:
                timeout_value = float(node.config.get("timeout_seconds") or DEFAULT_SCRIPT_TIMEOUT_SECONDS)
            except (TypeError, ValueError):
                timeout_value = 0.0
            if timeout_value <= 0:
                raise GraphValidationError(
                    f"Python script runner '{node.id}' requires timeout_seconds > 0."
                )

        standard_edge_counts: dict[str, int] = {}
        for edge in self.edges:
            if edge.source_id not in self.nodes:
                raise GraphValidationError(f"Edge '{edge.id}' references unknown source '{edge.source_id}'.")
            if edge.target_id not in self.nodes:
                raise GraphValidationError(f"Edge '{edge.id}' references unknown target '{edge.target_id}'.")
            source_node = self.nodes[edge.source_id]
            target_node = self.nodes[edge.target_id]
            if not is_valid_category_connection(source_node.category, target_node.category):
                raise GraphValidationError(
                    f"Edge '{edge.id}' uses an invalid category connection: "
                    f"{source_node.category.value} -> {target_node.category.value}."
                )
            if source_node.category == NodeCategory.END:
                raise GraphValidationError(f"End node '{source_node.id}' cannot have outgoing edges.")
            if source_node.provider_id == OUTBOUND_EMAIL_LOGGER_PROVIDER_ID:
                if edge.kind != "binding":
                    raise GraphValidationError(
                        f"Outbound email logger '{source_node.id}' can only create binding edges."
                    )
                if target_node.provider_id != OUTLOOK_DRAFT_PROVIDER_ID:
                    raise GraphValidationError(
                        f"Outbound email logger '{source_node.id}' can only bind into Outlook draft end nodes."
                    )
            if target_node.provider_id == OUTBOUND_EMAIL_LOGGER_PROVIDER_ID:
                raise GraphValidationError(
                    f"Outbound email logger '{target_node.id}' is source-only and cannot receive incoming edges."
                )
            if source_node.kind == "data" and source_node.provider_id == PROMPT_BLOCK_PROVIDER_ID:
                if edge.kind != "binding":
                    raise GraphValidationError(
                        f"Prompt block '{source_node.id}' can only create binding edges."
                    )
                if target_node.kind not in {"model", "data"}:
                    raise GraphValidationError(
                        f"Prompt block '{source_node.id}' can only bind into model or data nodes."
                    )
            if target_node.kind == "data" and target_node.provider_id == PROMPT_BLOCK_PROVIDER_ID:
                raise GraphValidationError(
                    f"Prompt block '{target_node.id}' is source-only and cannot receive incoming edges."
                )
            if edge.kind == "conditional" and edge.condition is None:
                raise GraphValidationError(f"Edge '{edge.id}' is conditional but missing a condition.")
            if edge.kind not in {"conditional", "binding"}:
                if source_node.category == NodeCategory.PROVIDER:
                    continue
                if edge.source_handle_id is not None:
                    continue
                standard_edge_counts[edge.source_id] = standard_edge_counts.get(edge.source_id, 0) + 1
                if standard_edge_counts[edge.source_id] > 1:
                    if source_node.provider_id == PARALLEL_SPLITTER_PROVIDER_ID:
                        continue
                    standard_outgoing_edges = [
                        candidate for candidate in self.get_outgoing_edges(edge.source_id) if candidate.kind == "standard"
                    ]
                    if not all(
                        (target := self.nodes.get(candidate.target_id)) is not None and target.kind == "output"
                        for candidate in standard_outgoing_edges
                    ):
                        raise GraphValidationError(
                            f"Node '{edge.source_id}' has more than one standard outgoing edge."
                        )

    def start_node(self) -> BaseNode:
        return self.nodes[self.start_node_id]

    def start_node_config(self) -> dict[str, Any]:
        return dict(self.start_node().raw_config)

    def resolved_start_node_config(self) -> dict[str, Any]:
        return resolve_graph_env_value(self.start_node_config(), self.env_vars)

    def validate_against_services(self, services: RuntimeServices) -> None:
        for node in self.nodes.values():
            try:
                provider_definition = services.node_provider_registry.get(node.provider_id)
            except KeyError as exc:
                raise GraphValidationError(str(exc)) from exc
            if provider_definition.category != node.category:
                raise GraphValidationError(
                    f"Node '{node.id}' uses provider '{node.provider_id}' with category "
                    f"'{provider_definition.category.value}', but the node is '{node.category.value}'."
                )
            if provider_definition.node_kind != node.kind:
                raise GraphValidationError(
                    f"Node '{node.id}' uses provider '{node.provider_id}' for kind "
                    f"'{provider_definition.node_kind}', but the node kind is '{node.kind}'."
                )
            contract = get_category_contract(node.category)
            if not contract.produced_outputs:
                raise GraphValidationError(f"Node category '{node.category.value}' is missing a contract.")

            if node.kind == "provider":
                provider_name = str(node.config.get("provider_name", "") or getattr(node, "provider_name", ""))
                if provider_name not in services.model_providers:
                    raise GraphValidationError(
                        f"Provider node '{node.id}' references unknown model provider '{provider_name}'."
                    )
            if node.provider_id == OUTBOUND_EMAIL_LOGGER_PROVIDER_ID:
                incoming_edges = self.get_incoming_edges(node.id)
                if incoming_edges:
                    raise GraphValidationError(
                        f"Outbound email logger '{node.id}' cannot receive incoming edges."
                    )
                outgoing_edges = self.get_outgoing_edges(node.id)
                if any(edge.kind != "binding" for edge in outgoing_edges):
                    raise GraphValidationError(
                        f"Outbound email logger '{node.id}' can only create binding edges."
                    )
                message_type = str(node.config.get("message_type", "initial") or "initial").strip().lower() or "initial"
                if message_type not in {"initial", "follow_up"}:
                    raise GraphValidationError(
                        f"Outbound email logger '{node.id}' uses unsupported message_type '{message_type}'."
                    )
            if node.provider_id in {
                SUPABASE_DATA_PROVIDER_ID,
                SUPABASE_SQL_PROVIDER_ID,
                SUPABASE_TABLE_ROWS_PROVIDER_ID,
                SUPABASE_ROW_WRITE_PROVIDER_ID,
                OUTBOUND_EMAIL_LOGGER_PROVIDER_ID,
            }:
                connection_id = str(node.config.get("supabase_connection_id", "") or "").strip()
                if connection_id and self.get_supabase_connection(connection_id) is None:
                    raise GraphValidationError(
                        f"Node '{node.id}' references unknown Supabase connection '{connection_id}'."
                    )
            if node.kind == "model":
                bound_provider = self._resolve_provider_binding(node)
                model_provider_name = (
                    str(bound_provider.config.get("provider_name", ""))
                    if bound_provider is not None
                    else str(node.config.get("provider_name", ""))
                )
                if model_provider_name not in services.model_providers:
                    raise GraphValidationError(
                        f"Model node '{node.id}' references unknown model provider '{model_provider_name}'."
                    )
                if node.provider_id == SPREADSHEET_MATRIX_DECISION_PROVIDER_ID:
                    if node.config.get("tool_target_node_ids"):
                        raise GraphValidationError(
                            f"Spreadsheet matrix decision node '{node.id}' cannot declare tool_target_node_ids."
                        )
                    if any(str(tool_name).strip() for tool_name in node.config.get("allowed_tool_names", [])):
                        raise GraphValidationError(
                            f"Spreadsheet matrix decision node '{node.id}' cannot expose allowed_tool_names."
                        )
                    if str(node.config.get("preferred_tool_name", "") or "").strip():
                        raise GraphValidationError(
                            f"Spreadsheet matrix decision node '{node.id}' cannot declare preferred_tool_name."
                        )
                    response_mode = str(node.config.get("response_mode", "message") or "message").strip()
                    if response_mode not in {"", "message"}:
                        raise GraphValidationError(
                            f"Spreadsheet matrix decision node '{node.id}' must use response_mode 'message'."
                        )
                allowed_tool_names = [str(tool_name) for tool_name in node.config.get("allowed_tool_names", [])]
                resolved_allowed_tool_names: list[str] = []
                for tool_name in allowed_tool_names:
                    try:
                        tool_definition = services.tool_registry.require_graph_reference(str(tool_name))
                    except (KeyError, ValueError) as exc:
                        raise GraphValidationError(str(exc)) from exc
                    resolved_allowed_tool_names.append(tool_definition.canonical_name)
                mcp_context_tool_names: list[str] = []
                response_mode = infer_model_response_mode(self, node)
                candidate_context_nodes: list[McpContextProviderNode] = []
                seen_context_node_ids: set[str] = set()
                tool_target_node_ids = node.config.get("tool_target_node_ids", [])
                if tool_target_node_ids:
                    if not isinstance(tool_target_node_ids, Sequence) or isinstance(tool_target_node_ids, (str, bytes)):
                        raise GraphValidationError(
                            f"Model node '{node.id}' must declare tool_target_node_ids as a list of tool node ids."
                        )
                    for target_node_id in tool_target_node_ids:
                        target_node = self.nodes.get(str(target_node_id))
                        if not isinstance(target_node, McpContextProviderNode):
                            raise GraphValidationError(
                                f"Model node '{node.id}' references unknown MCP context provider node '{target_node_id}'."
                            )
                        if target_node.id not in seen_context_node_ids:
                            candidate_context_nodes.append(target_node)
                            seen_context_node_ids.add(target_node.id)
                prompt_block_node_ids = node.config.get("prompt_block_node_ids", [])
                if prompt_block_node_ids:
                    if not isinstance(prompt_block_node_ids, Sequence) or isinstance(prompt_block_node_ids, (str, bytes)):
                        raise GraphValidationError(
                            f"Model node '{node.id}' must declare prompt_block_node_ids as a list of prompt block node ids."
                        )
                    for prompt_block_node_id in prompt_block_node_ids:
                        prompt_block_node_id_str = str(prompt_block_node_id).strip()
                        if not prompt_block_node_id_str:
                            continue
                        target_node = self.nodes.get(prompt_block_node_id_str)
                        if target_node is None or target_node.kind != "data" or target_node.provider_id != PROMPT_BLOCK_PROVIDER_ID:
                            raise GraphValidationError(
                                f"Model node '{node.id}' references unknown prompt block node '{prompt_block_node_id_str}'."
                            )
                for edge in self.get_incoming_edges(node.id):
                    if edge.kind != "binding":
                        continue
                    source_node = self.nodes.get(edge.source_id)
                    if (
                        node.provider_id == SPREADSHEET_MATRIX_DECISION_PROVIDER_ID
                        and isinstance(source_node, McpContextProviderNode)
                    ):
                        raise GraphValidationError(
                            f"Spreadsheet matrix decision node '{node.id}' cannot bind MCP context provider '{source_node.id}'."
                        )
                    if isinstance(source_node, McpContextProviderNode) and source_node.id not in seen_context_node_ids:
                        candidate_context_nodes.append(source_node)
                        seen_context_node_ids.add(source_node.id)
                for target_node in candidate_context_nodes:
                    tool_names = target_node.config.get("tool_names", [])
                    if not isinstance(tool_names, Sequence) or isinstance(tool_names, (str, bytes)):
                        raise GraphValidationError(
                            f"MCP context provider '{target_node.id}' must declare tool_names as a list."
                        )
                    for tool_name in tool_names:
                        tool_name_str = str(tool_name).strip()
                        if not tool_name_str:
                            continue
                        try:
                            tool_definition = services.tool_registry.require_graph_reference(tool_name_str)
                        except (KeyError, ValueError) as exc:
                            raise GraphValidationError(str(exc)) from exc
                        if tool_definition.source_type != "mcp":
                            raise GraphValidationError(
                                f"MCP context provider '{target_node.id}' references non-MCP tool '{tool_name_str}'."
                            )
                        if bool(target_node.config.get("expose_mcp_tools", True)):
                            mcp_context_tool_names.append(tool_definition.canonical_name)
                combined_tool_names = [
                    *resolved_allowed_tool_names,
                    *[tool_name for tool_name in mcp_context_tool_names if tool_name not in resolved_allowed_tool_names],
                ]
                if response_mode == "tool_call" and not combined_tool_names and not isinstance(node.config.get("response_schema"), Mapping):
                    raise GraphValidationError(
                        f"Model node '{node.id}' uses tool_call mode but does not expose any allowed tools."
                    )
                preferred_tool_name = str(node.config.get("preferred_tool_name", "") or "").strip()
                if preferred_tool_name and combined_tool_names:
                    try:
                        resolved_preferred_tool_name = services.tool_registry.canonical_name_for(preferred_tool_name)
                    except (KeyError, ValueError) as exc:
                        raise GraphValidationError(str(exc)) from exc
                    if resolved_preferred_tool_name not in combined_tool_names:
                        raise GraphValidationError(
                            f"Model node '{node.id}' prefers tool '{preferred_tool_name}', but it is not exposed to the node."
                        )
                model_outgoing_edges = [edge for edge in self.get_outgoing_edges(node.id) if edge.kind != "binding"]
                for edge in model_outgoing_edges:
                    target_node = self.nodes.get(edge.target_id)
                    if target_node is None:
                        continue
                    if (
                        node.provider_id == SPREADSHEET_MATRIX_DECISION_PROVIDER_ID
                        and edge.source_handle_id == API_TOOL_CALL_HANDLE_ID
                    ):
                        raise GraphValidationError(
                            f"Spreadsheet matrix decision node '{node.id}' cannot use tool-call output edge '{edge.id}'."
                        )
                    if edge.source_handle_id == API_TOOL_CALL_HANDLE_ID:
                        if target_node.category != NodeCategory.TOOL and not (
                            target_node.category == NodeCategory.DATA and target_node.provider_id == "core.data_display"
                        ):
                            raise GraphValidationError(
                                f"Model node '{node.id}' tool-call output must route to a tool node, but '{target_node.id}' is '{target_node.category.value}'."
                            )
                        if edge.kind != "conditional" or not _is_tool_call_contract_condition(edge.condition):
                            raise GraphValidationError(
                                f"Model node '{node.id}' tool-call output edge '{edge.id}' must match tool_call_envelope."
                            )
                    if edge.source_handle_id == API_MESSAGE_HANDLE_ID:
                        if target_node.category not in {
                            NodeCategory.API,
                            NodeCategory.CONTROL_FLOW_UNIT,
                            NodeCategory.DATA,
                            NodeCategory.END,
                        }:
                            raise GraphValidationError(
                                f"Model node '{node.id}' message output must route to api, control_flow_unit, data, or end nodes, but '{target_node.id}' is '{target_node.category.value}'."
                            )
                        if edge.kind != "conditional" or not _is_message_contract_condition(edge.condition):
                            raise GraphValidationError(
                                f"Model node '{node.id}' message output edge '{edge.id}' must match message_envelope."
                            )
            if node.kind == "tool":
                try:
                    services.tool_registry.require_graph_reference(str(node.config.get("tool_name", "")), require_executor=True)
                except (KeyError, ValueError) as exc:
                    raise GraphValidationError(str(exc)) from exc
            if node.kind == "mcp_context_provider":
                tool_names = node.config.get("tool_names", [])
                if not isinstance(tool_names, Sequence) or isinstance(tool_names, (str, bytes)):
                    raise GraphValidationError(f"MCP context provider '{node.id}' must declare tool_names as a list.")
                if not [str(tool_name).strip() for tool_name in tool_names if str(tool_name).strip()]:
                    raise GraphValidationError(f"MCP context provider '{node.id}' must register at least one MCP tool.")
                for tool_name in tool_names:
                    tool_name_str = str(tool_name).strip()
                    if not tool_name_str:
                        continue
                    try:
                        tool_definition = services.tool_registry.require_graph_reference(tool_name_str)
                    except (KeyError, ValueError) as exc:
                        raise GraphValidationError(str(exc)) from exc
                    if tool_definition.source_type != "mcp":
                        raise GraphValidationError(
                            f"MCP context provider '{node.id}' references non-MCP tool '{tool_name_str}'."
                        )
            if node.kind == "mcp_tool_executor":
                tool_target_node_ids = node.config.get("tool_target_node_ids", [])
                if tool_target_node_ids:
                    raise GraphValidationError(
                        f"MCP tool executor '{node.id}' cannot declare tool_target_node_ids; configure allowed_tool_names directly."
                    )
                input_binding = node.config.get("input_binding")
                if input_binding is not None:
                    if not isinstance(input_binding, Mapping):
                        raise GraphValidationError(
                            f"MCP tool executor '{node.id}' must declare input_binding as an object."
                        )
                    binding_type = str(input_binding.get("type", "latest_output"))
                    if binding_type not in {"latest_output", "latest_envelope", "first_available_envelope"}:
                        raise GraphValidationError(
                            f"MCP tool executor '{node.id}' uses unsupported input binding '{binding_type}'."
                        )
                    if binding_type == "first_available_envelope":
                        raw_sources = input_binding.get("sources", [])
                        if not isinstance(raw_sources, Sequence) or isinstance(raw_sources, (str, bytes)):
                            raise GraphValidationError(
                                f"MCP tool executor '{node.id}' must declare sources as a list for first_available_envelope bindings."
                            )
                        binding_source_ids = [str(source_id).strip() for source_id in raw_sources if str(source_id).strip()]
                    else:
                        binding_source_id = str(input_binding.get("source", "")).strip()
                        binding_source_ids = [binding_source_id] if binding_source_id else []
                    if not binding_source_ids:
                        raise GraphValidationError(
                            f"MCP tool executor '{node.id}' must reference at least one upstream source."
                        )
                    for source_id in binding_source_ids:
                        source_node = self.nodes.get(source_id)
                        if source_node is None:
                            raise GraphValidationError(
                                f"MCP tool executor '{node.id}' references missing source node '{source_id}'."
                            )
                        if isinstance(source_node, McpContextProviderNode):
                            raise GraphValidationError(
                                f"MCP tool executor '{node.id}' cannot bind directly to MCP context provider '{source_id}'; configure allowed_tool_names directly."
                            )
                        if isinstance(source_node, ModelNode):
                            source_response_mode = infer_model_response_mode(self, source_node)
                            if source_response_mode == "message":
                                raise GraphValidationError(
                                    f"MCP tool executor '{node.id}' must bind to a tool_call model output, but '{source_id}' uses response mode '{source_response_mode}'."
                                )
                else:
                    incoming_edges = self.get_incoming_edges(node.id)
                    if not incoming_edges:
                        raise GraphValidationError(
                            f"MCP tool executor '{node.id}' must have an incoming edge or an explicit input_binding."
                        )
                    for edge in incoming_edges:
                        source_node = self.nodes.get(edge.source_id)
                        if edge.kind == "binding" and isinstance(source_node, McpContextProviderNode):
                            raise GraphValidationError(
                                f"MCP tool executor '{node.id}' cannot receive MCP context provider binding '{edge.id}'; configure allowed_tool_names directly."
                            )
                    valid_tool_call_routes = 0
                    for edge in incoming_edges:
                        if edge.kind == "binding":
                            continue
                        source_node = self.nodes.get(edge.source_id)
                        if source_node is None:
                            continue
                        if not isinstance(source_node, ModelNode):
                            valid_tool_call_routes += 1
                            continue
                        source_response_mode = infer_model_response_mode(self, source_node)
                        if edge.source_handle_id == API_MESSAGE_HANDLE_ID:
                            continue
                        if edge.source_handle_id == API_TOOL_CALL_HANDLE_ID:
                            valid_tool_call_routes += 1
                            continue
                        if source_response_mode == "tool_call":
                            valid_tool_call_routes += 1
                            continue
                        if source_response_mode == "auto" and _is_tool_call_contract_condition(edge.condition):
                            valid_tool_call_routes += 1
                            continue
                    if valid_tool_call_routes == 0:
                        raise GraphValidationError(
                            f"MCP tool executor '{node.id}' must receive a tool_call envelope from at least one upstream model node."
                        )
                    for edge in incoming_edges:
                        source_node = self.nodes.get(edge.source_id)
                        if not isinstance(source_node, ModelNode):
                            continue
                        source_response_mode = infer_model_response_mode(self, source_node)
                        if edge.source_handle_id == API_MESSAGE_HANDLE_ID:
                            raise GraphValidationError(
                                f"MCP tool executor '{node.id}' cannot receive api-message output from model node '{source_node.id}'."
                            )
                        if edge.source_handle_id == API_TOOL_CALL_HANDLE_ID:
                            if edge.kind != "conditional" or not _is_tool_call_contract_condition(edge.condition):
                                raise GraphValidationError(
                                    f"MCP tool executor '{node.id}' must use a tool_call_envelope condition on api-tool-call edge '{edge.id}'."
                                )
                            continue
                        if source_response_mode == "message":
                            raise GraphValidationError(
                                f"MCP tool executor '{node.id}' cannot receive direct message-mode output from model node '{source_node.id}'."
                            )
                        if source_response_mode == "auto" and not _is_tool_call_contract_condition(edge.condition):
                            raise GraphValidationError(
                                f"MCP tool executor '{node.id}' must use a tool_call_envelope condition on edges from auto-mode model node '{source_node.id}'."
                            )
                terminal_output_edges = [
                    edge
                    for edge in self.get_outgoing_edges(node.id)
                    if edge.kind != "binding" and edge.source_handle_id == MCP_TERMINAL_OUTPUT_HANDLE_ID
                ]
                if len(terminal_output_edges) > 1:
                    raise GraphValidationError(
                        f"MCP tool executor '{node.id}' can only declare one terminal output route."
                    )
                for edge in terminal_output_edges:
                    if edge.kind != "conditional" or not _is_terminal_output_contract_condition(edge.condition):
                        raise GraphValidationError(
                            f"MCP tool executor '{node.id}' terminal output edge '{edge.id}' must match terminal_output_envelope."
                        )
                if bool(node.config.get("enable_follow_up_decision", False)):
                    provider_name = str(node.config.get("provider_name", "") or "claude_code")
                    if provider_name not in services.model_providers:
                        raise GraphValidationError(
                            f"MCP tool executor '{node.id}' references unknown follow-up model provider '{provider_name}'."
                        )
                    response_mode = str(node.config.get("response_mode", "auto") or "auto").strip()
                    if response_mode not in {"message", "tool_call", "auto"}:
                        raise GraphValidationError(
                            f"MCP tool executor '{node.id}' uses unsupported follow-up response_mode '{response_mode}'."
                        )
                    allowed_tool_names = [str(tool_name) for tool_name in node.config.get("allowed_tool_names", [])]
                    resolved_allowed_tool_names: list[str] = []
                    for tool_name in allowed_tool_names:
                        try:
                            tool_definition = services.tool_registry.require_graph_reference(str(tool_name))
                        except (KeyError, ValueError) as exc:
                            raise GraphValidationError(str(exc)) from exc
                        resolved_allowed_tool_names.append(tool_definition.canonical_name)
                    for tool_name in allowed_tool_names:
                        tool_definition = services.tool_registry.require_graph_reference(str(tool_name))
                        if tool_definition.source_type != "mcp":
                            raise GraphValidationError(
                                f"MCP tool executor '{node.id}' references non-MCP tool '{tool_name}'."
                            )
                    if response_mode == "tool_call" and not resolved_allowed_tool_names and not isinstance(node.config.get("response_schema"), Mapping):
                        raise GraphValidationError(
                            f"MCP tool executor '{node.id}' uses tool_call follow-up mode but does not expose any allowed tools."
                        )
                    preferred_tool_name = str(node.config.get("preferred_tool_name", "") or "").strip()
                    if preferred_tool_name and resolved_allowed_tool_names:
                        try:
                            resolved_preferred_tool_name = services.tool_registry.canonical_name_for(preferred_tool_name)
                        except (KeyError, ValueError) as exc:
                            raise GraphValidationError(str(exc)) from exc
                        if resolved_preferred_tool_name not in resolved_allowed_tool_names:
                            raise GraphValidationError(
                                f"MCP tool executor '{node.id}' prefers tool '{preferred_tool_name}', but it is not exposed to the node."
                            )
            if node.kind == "data" and node.provider_id == "core.context_builder":
                raw_bindings = node.config.get("input_bindings", [])
                if not isinstance(raw_bindings, Sequence) or isinstance(raw_bindings, (str, bytes)):
                    raise GraphValidationError(f"Context builder '{node.id}' must declare input_bindings as a list.")
                incoming_source_ids = {edge.source_id for edge in self.get_incoming_edges(node.id)}
                seen_placeholders: set[str] = set()
                for index, raw_binding in enumerate(raw_bindings):
                    if not isinstance(raw_binding, Mapping):
                        raise GraphValidationError(
                            f"Context builder '{node.id}' binding at index {index} must be an object."
                        )
                    source_node_id = str(raw_binding.get("source_node_id") or raw_binding.get("source") or "").strip()
                    if source_node_id and source_node_id not in incoming_source_ids:
                        continue
                    placeholder = _slugify_context_builder_placeholder(
                        raw_binding.get("placeholder"),
                        fallback=f"source_{index + 1}",
                    )
                    if not CONTEXT_BUILDER_IDENTIFIER_PATTERN.match(placeholder):
                        raise GraphValidationError(
                            f"Context builder '{node.id}' uses invalid placeholder '{placeholder}'."
                        )
                    if placeholder in seen_placeholders:
                        raise GraphValidationError(
                            f"Context builder '{node.id}' uses duplicate placeholder '{placeholder}'."
                        )
                    seen_placeholders.add(placeholder)
            if node.kind == "data" and node.provider_id == PROMPT_BLOCK_PROVIDER_ID:
                raw_role = str(node.config.get("role", "user") or "user").strip().lower()
                if raw_role not in PROMPT_BLOCK_ROLES:
                    raise GraphValidationError(
                        f"Prompt block '{node.id}' uses unsupported role '{raw_role}'."
                    )
            if node.kind == "output" and node.provider_id == OUTLOOK_DRAFT_PROVIDER_ID:
                logger_binding_edges = [
                    edge
                    for edge in self.get_incoming_edges(node.id)
                    if edge.kind == "binding"
                    and (source_node := self.nodes.get(edge.source_id)) is not None
                    and source_node.provider_id == OUTBOUND_EMAIL_LOGGER_PROVIDER_ID
                ]
                if len(logger_binding_edges) > 1:
                    raise GraphValidationError(
                        f"Outlook draft node '{node.id}' can only have one outbound email logger binding."
                    )
            if node.kind == "data" and node.provider_id == APOLLO_EMAIL_LOOKUP_PROVIDER_ID:
                workspace_template = str(
                    node.config.get("workspace_cache_path_template", "cache/apollo-email/{cache_key}.json")
                    or "cache/apollo-email/{cache_key}.json"
                ).strip()
                if not workspace_template:
                    raise GraphValidationError(
                        f"Apollo email lookup node '{node.id}' must declare workspace_cache_path_template."
                    )
                try:
                    preview_path = apollo_workspace_cache_relative_path(workspace_template, cache_key="preview")
                    resolve_agent_workspace_path("preview-run", "preview-agent", preview_path)
                except Exception as exc:  # noqa: BLE001
                    raise GraphValidationError(
                        f"Apollo email lookup node '{node.id}' uses an invalid workspace cache path template."
                    ) from exc
            if node.kind == "data" and node.provider_id == STRUCTURED_PAYLOAD_BUILDER_PROVIDER_ID:
                try:
                    parse_structured_payload_template(node.config.get("template_json", "{}"))
                except ValueError as exc:
                    raise GraphValidationError(
                        f"Structured payload builder node '{node.id}' has invalid template_json."
                    ) from exc
                try:
                    if int(node.config.get("max_matches_per_field", 25)) < 1:
                        raise ValueError("max_matches_per_field")
                except (TypeError, ValueError) as exc:
                    raise GraphValidationError(
                        f"Structured payload builder node '{node.id}' must declare a positive max_matches_per_field value."
                    ) from exc
            if node.kind == "data" and node.provider_id == LINKEDIN_PROFILE_FETCH_PROVIDER_ID:
                url_field = str(node.config.get("url_field", "url") or "").strip()
                if not url_field:
                    raise GraphValidationError(f"LinkedIn profile fetch node '{node.id}' must declare a url_field.")
                linkedin_data_dir = str(node.config.get("linkedin_data_dir", "") or "").strip()
                if not linkedin_data_dir:
                    raise GraphValidationError(
                        f"LinkedIn profile fetch node '{node.id}' must declare linkedin_data_dir."
                    )
                workspace_template = str(
                    node.config.get("workspace_cache_path_template", "cache/linkedin/{cache_key}.json")
                    or "cache/linkedin/{cache_key}.json"
                ).strip()
                if not workspace_template:
                    raise GraphValidationError(
                        f"LinkedIn profile fetch node '{node.id}' must declare workspace_cache_path_template."
                    )
                try:
                    preview_path = workspace_cache_relative_path(workspace_template, cache_key="preview")
                    resolve_agent_workspace_path("preview-run", "preview-agent", preview_path)
                except Exception as exc:  # noqa: BLE001
                    raise GraphValidationError(
                        f"LinkedIn profile fetch node '{node.id}' uses an invalid workspace cache path template."
                    ) from exc
            if node.kind == "data" and node.provider_id == RUNTIME_NORMALIZER_PROVIDER_ID:
                field_names = parse_field_name_list(node.config.get("field_names"))
                if not field_names:
                    field_names = parse_field_name_list(node.config.get("field_name"))
                if not field_names:
                    raise GraphValidationError(
                        f"Runtime field extractor node '{node.id}' must declare at least one field name."
                    )
                try:
                    if int(node.config.get("max_matches", 25)) < 1:
                        raise ValueError("max_matches")
                except (TypeError, ValueError) as exc:
                    raise GraphValidationError(
                        f"Runtime field extractor node '{node.id}' must declare a positive max_matches value."
                    ) from exc
            if node.kind == "data" and node.provider_id == SUPABASE_DATA_PROVIDER_ID:
                source_name = str(node.config.get("source_name", "") or "").strip()
                source_kind = str(node.config.get("source_kind", "table") or "table").strip().lower() or "table"
                if source_kind not in {"table", "rpc"}:
                    raise GraphValidationError(
                        f"Supabase data source node '{node.id}' uses unsupported source_kind '{source_kind}'."
                    )
                output_mode = str(node.config.get("output_mode", "records") or "records").strip().lower() or "records"
                if output_mode not in {"records", "markdown"}:
                    raise GraphValidationError(
                        f"Supabase data source node '{node.id}' uses unsupported output_mode '{output_mode}'."
                    )
                try:
                    if int(node.config.get("limit", 25)) < 1:
                        raise ValueError("limit")
                except (TypeError, ValueError) as exc:
                    raise GraphValidationError(
                        f"Supabase data source node '{node.id}' must declare a positive limit value."
                    ) from exc
            if node.kind == "data" and node.provider_id == SUPABASE_SQL_PROVIDER_ID:
                output_mode = str(node.config.get("output_mode", "records") or "records").strip().lower() or "records"
                if output_mode not in {"records", "markdown"}:
                    raise GraphValidationError(
                        f"Supabase SQL node '{node.id}' uses unsupported output_mode '{output_mode}'."
                    )
            if node.kind == "data" and node.provider_id == SUPABASE_ROW_WRITE_PROVIDER_ID:
                write_mode = str(node.config.get("write_mode", "insert") or "insert").strip().lower() or "insert"
                if write_mode not in {"insert", "upsert"}:
                    raise GraphValidationError(
                        f"Supabase row write node '{node.id}' uses unsupported write_mode '{write_mode}'."
                    )
                returning = str(node.config.get("returning", "representation") or "representation").strip().lower() or "representation"
                if returning not in {"representation", "minimal"}:
                    raise GraphValidationError(
                        f"Supabase row write node '{node.id}' uses unsupported returning mode '{returning}'."
                    )
            if node.kind == "control_flow_unit":
                if node.provider_id == SPREADSHEET_ROW_PROVIDER_ID:
                    allowed_handles = {CONTROL_FLOW_LOOP_BODY_HANDLE_ID, None}
                    invalid_handles = [
                        edge.source_handle_id
                        for edge in self.get_outgoing_edges(node.id)
                        if edge.kind != "binding" and edge.source_handle_id not in allowed_handles
                    ]
                    if invalid_handles:
                        raise GraphValidationError(
                            f"Spreadsheet rows node '{node.id}' uses unsupported output handle(s): {', '.join(str(handle) for handle in invalid_handles)}."
                        )
                if node.provider_id == SUPABASE_TABLE_ROWS_PROVIDER_ID:
                    table_name = str(node.config.get("table_name", "") or "").strip()
                    cursor_column = str(node.config.get("cursor_column", "") or "").strip()
                    row_id_column = str(node.config.get("row_id_column", "id") or "id").strip() or "id"
                    if not table_name:
                        raise GraphValidationError(
                            f"Supabase table rows node '{node.id}' must declare a table_name."
                        )
                    if not cursor_column:
                        raise GraphValidationError(
                            f"Supabase table rows node '{node.id}' must declare a cursor_column."
                        )
                    if not row_id_column:
                        raise GraphValidationError(
                            f"Supabase table rows node '{node.id}' must declare a row_id_column."
                        )
                    try:
                        if int(node.config.get("page_size", 500)) < 1:
                            raise ValueError("page_size")
                    except (TypeError, ValueError) as exc:
                        raise GraphValidationError(
                            f"Supabase table rows node '{node.id}' must declare a positive page_size value."
                        ) from exc
                    allowed_handles = {CONTROL_FLOW_LOOP_BODY_HANDLE_ID, None}
                    invalid_handles = [
                        edge.source_handle_id
                        for edge in self.get_outgoing_edges(node.id)
                        if edge.kind != "binding" and edge.source_handle_id not in allowed_handles
                    ]
                    if invalid_handles:
                        raise GraphValidationError(
                            f"Supabase table rows node '{node.id}' uses unsupported output handle(s): {', '.join(str(handle) for handle in invalid_handles)}."
                        )
                if node.provider_id == LOGIC_CONDITIONS_PROVIDER_ID:
                    branch_handles: set[str] = set()

                    def validate_logic_group(raw_group: Any, branch_label: str, path: str) -> None:
                        if not isinstance(raw_group, Mapping):
                            raise GraphValidationError(
                                f"Logic conditions node '{node.id}' branch '{branch_label}' has invalid group at {path}."
                            )
                        raw_children = raw_group.get("children", [])
                        if not isinstance(raw_children, Sequence) or isinstance(raw_children, (str, bytes)):
                            raise GraphValidationError(
                                f"Logic conditions node '{node.id}' branch '{branch_label}' group at {path} must declare children as a list."
                            )
                        for child_index, raw_child in enumerate(raw_children):
                            child_path = f"{path}.{child_index + 1}"
                            if not isinstance(raw_child, Mapping):
                                raise GraphValidationError(
                                    f"Logic conditions node '{node.id}' branch '{branch_label}' has invalid child at {child_path}."
                                )
                            if raw_child.get("type") == "group":
                                validate_logic_group(raw_child, branch_label, child_path)
                                continue
                            operator = str(raw_child.get("operator", "equals") or "equals").strip()
                            if operator not in {"exists", "equals", "not_equals", "contains", "gt", "gte", "lt", "lte"}:
                                raise GraphValidationError(
                                    f"Logic conditions node '{node.id}' branch '{branch_label}' uses unsupported operator '{operator}'."
                                )

                    raw_branches = node.config.get("branches")
                    if isinstance(raw_branches, Sequence) and not isinstance(raw_branches, (str, bytes)):
                        for index, raw_branch in enumerate(raw_branches):
                            if not isinstance(raw_branch, Mapping):
                                raise GraphValidationError(
                                    f"Logic conditions node '{node.id}' branch at index {index} must be an object."
                                )
                            branch_label = str(raw_branch.get("label", f"Branch {index + 1}")).strip() or f"Branch {index + 1}"
                            output_handle_id = str(
                                raw_branch.get("output_handle_id", CONTROL_FLOW_IF_HANDLE_ID) or CONTROL_FLOW_IF_HANDLE_ID
                            ).strip()
                            if not output_handle_id:
                                raise GraphValidationError(
                                    f"Logic conditions node '{node.id}' branch '{branch_label}' must declare an output_handle_id."
                                )
                            branch_handles.add(output_handle_id)
                            validate_logic_group(raw_branch.get("root_group", {}), branch_label, "root")
                    else:
                        raw_clauses = node.config.get("clauses", [])
                        if not isinstance(raw_clauses, Sequence) or isinstance(raw_clauses, (str, bytes)):
                            raise GraphValidationError(
                                f"Logic conditions node '{node.id}' must declare clauses as a list."
                            )
                        for index, raw_clause in enumerate(raw_clauses):
                            if not isinstance(raw_clause, Mapping):
                                raise GraphValidationError(
                                    f"Logic conditions node '{node.id}' clause at index {index} must be an object."
                                )
                            operator = str(raw_clause.get("operator", "equals") or "equals").strip()
                            if operator not in {"exists", "equals", "not_equals", "contains", "gt", "gte", "lt", "lte"}:
                                raise GraphValidationError(
                                    f"Logic conditions node '{node.id}' uses unsupported operator '{operator}'."
                                )
                            output_handle_id = str(
                                raw_clause.get("output_handle_id", CONTROL_FLOW_IF_HANDLE_ID) or CONTROL_FLOW_IF_HANDLE_ID
                            ).strip()
                            if not output_handle_id:
                                raise GraphValidationError(
                                    f"Logic conditions node '{node.id}' clause at index {index} must declare an output_handle_id."
                                )
                            branch_handles.add(output_handle_id)
                    branch_handles.add(str(node.config.get("else_output_handle_id", CONTROL_FLOW_ELSE_HANDLE_ID) or CONTROL_FLOW_ELSE_HANDLE_ID))
                    invalid_handles = [
                        edge.source_handle_id
                        for edge in self.get_outgoing_edges(node.id)
                        if edge.kind != "binding" and edge.source_handle_id not in branch_handles
                    ]
                    if invalid_handles:
                        raise GraphValidationError(
                            f"Logic conditions node '{node.id}' uses unsupported output handle(s): {', '.join(str(handle) for handle in invalid_handles)}."
                        )

    def _resolve_provider_binding(self, node: BaseNode) -> ProviderNode | None:
        binding_node_id = str(node.config.get("provider_binding_node_id", "")).strip()
        if binding_node_id:
            bound_node = self.nodes.get(binding_node_id)
            if bound_node is None:
                raise GraphValidationError(
                    f"Node '{node.id}' references missing provider binding node '{binding_node_id}'."
                )
            if not isinstance(bound_node, ProviderNode):
                raise GraphValidationError(
                    f"Node '{node.id}' references '{binding_node_id}', but it is not a provider node."
                )
            if not any(edge.source_id == binding_node_id and edge.target_id == node.id for edge in self.get_incoming_edges(node.id)):
                raise GraphValidationError(
                    f"Node '{node.id}' is missing a provider edge from '{binding_node_id}'."
                )
            return bound_node

        for edge in self.get_incoming_edges(node.id):
            bound_node = self.nodes.get(edge.source_id)
            if isinstance(bound_node, ProviderNode):
                return bound_node
        return None

    def get_node(self, node_id: str) -> BaseNode:
        return self.nodes[node_id]

    def get_outgoing_edges(self, node_id: str) -> list[Edge]:
        return sorted(
            [edge for edge in self.edges if edge.source_id == node_id],
            key=lambda edge: edge.priority,
        )

    def get_incoming_edges(self, node_id: str) -> list[Edge]:
        return [edge for edge in self.edges if edge.target_id == node_id]

    def get_supabase_connection(self, connection_id: str) -> SupabaseConnectionDefinition | None:
        normalized_connection_id = str(connection_id or "").strip()
        if not normalized_connection_id:
            return None
        return next(
            (connection for connection in self.supabase_connections if connection.connection_id == normalized_connection_id),
            None,
        )

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "graph_id": self.graph_id,
            "name": self.name,
            "description": self.description,
            "version": self.version,
            "default_input": self.default_input,
            "start_node_id": self.start_node_id,
            "env_vars": dict(self.env_vars),
            "nodes": [node.to_dict() for node in self.nodes.values()],
            "edges": [edge.to_dict() for edge in self.edges],
        }
        if self.supabase_connections:
            payload["supabase_connections"] = [connection.to_dict() for connection in self.supabase_connections]
        if self.default_supabase_connection_id:
            payload["default_supabase_connection_id"] = self.default_supabase_connection_id
        if self.run_store_supabase_connection_id:
            payload["run_store_supabase_connection_id"] = self.run_store_supabase_connection_id
        return payload
