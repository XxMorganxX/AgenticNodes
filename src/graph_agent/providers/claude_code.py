from __future__ import annotations

import json
import logging
import os
import re
import selectors
import subprocess
import time
from collections.abc import Mapping, Sequence
from typing import Any

from graph_agent.providers.base import (
    ModelMessage,
    ModelProvider,
    ModelRequest,
    ModelResponse,
    api_decision_response_schema,
    ModelToolCall,
    ModelToolDefinition,
    ProviderPreflightResult,
    normalize_api_decision_output,
)

_HEALTHCHECK_MIN_TURNS = 2
_MIN_REQUEST_TURNS = 2
_ANTHROPIC_API_KEY_ENV_VAR = "ANTHROPIC_API_KEY"
_LOG_PREVIEW_LIMIT = 240

LOGGER = logging.getLogger(__name__)


def _is_mapping(value: Any) -> bool:
    return isinstance(value, Mapping)


def _strict_json_schema(schema: Any) -> Any:
    if isinstance(schema, Mapping):
        normalized: dict[str, Any] = {}
        for key, value in schema.items():
            normalized[key] = _strict_json_schema(value)
        type_value = normalized.get("type")
        if type_value == "object" or (
            isinstance(type_value, Sequence)
            and not isinstance(type_value, (str, bytes))
            and "object" in {str(entry).strip() for entry in type_value}
        ) or "properties" in normalized:
            normalized.setdefault("additionalProperties", False)
        if isinstance(type_value, Sequence) and not isinstance(type_value, (str, bytes)):
            union_types = [entry for entry in type_value if isinstance(entry, str) and entry.strip()]
            if len(union_types) == 1:
                normalized["type"] = union_types[0]
            elif len(union_types) > 1:
                normalized.pop("type", None)
                normalized["anyOf"] = [{"type": union_type} for union_type in union_types]
        return normalized
    if isinstance(schema, Sequence) and not isinstance(schema, (str, bytes)):
        return [_strict_json_schema(entry) for entry in schema]
    return schema


def _friendly_cli_error_detail(detail: str) -> str:
    normalized_detail = detail.strip()
    if not normalized_detail:
        return detail
    try:
        parsed = json.loads(normalized_detail)
    except json.JSONDecodeError:
        return detail
    if not _is_mapping(parsed):
        return detail

    result_text = parsed.get("result")
    message_text = result_text.strip() if isinstance(result_text, str) else ""
    if not message_text:
        return detail

    lowered = message_text.lower()
    if "hit your limit" in lowered:
        return f"Claude Code usage limit reached. {message_text}"
    if parsed.get("subtype") == "error_max_turns":
        return (
            "Claude Code stopped after reaching the configured max turns. "
            f"{message_text} Increase max_turns to allow the CLI to finish after tool use."
        )
    if parsed.get("is_error") is True:
        return f"Claude Code request failed. {message_text}"
    return detail


def _schema_type_includes(schema: Mapping[str, Any], expected_type: str) -> bool:
    type_value = schema.get("type")
    if type_value == expected_type:
        return True
    if isinstance(type_value, Sequence) and not isinstance(type_value, (str, bytes)):
        return expected_type in {str(entry).strip() for entry in type_value}
    return False


def _string_like_schema(schema: Any) -> bool:
    return isinstance(schema, Mapping) and _schema_type_includes(schema, "string")


def _decision_message_object_schema(response_schema: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
    if not _is_mapping(response_schema):
        return None
    properties = response_schema.get("properties")
    if not isinstance(properties, Mapping):
        return None
    message_schema = properties.get("message")
    if not isinstance(message_schema, Mapping):
        return None
    if not (_schema_type_includes(message_schema, "object") or isinstance(message_schema.get("properties"), Mapping)):
        return None
    return message_schema


def _normalized_label_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def _candidate_field_labels(field_name: str) -> set[str]:
    normalized = _normalized_label_key(field_name)
    labels = {
        normalized,
        normalized.replace("_", " "),
    }
    if normalized == "recipient":
        labels.update({"to", "recipient", "email recipient", "recipient email"})
    elif normalized == "body":
        labels.update({"body", "message", "email body", "content", "text"})
    elif normalized == "subject":
        labels.update({"subject", "title", "email subject"})
    return {label for label in labels if label}


def _clean_labeled_value(value: str) -> str:
    cleaned = value.strip()
    cleaned = re.sub(r"^\*+\s*", "", cleaned)
    cleaned = re.sub(r"\s*\*+$", "", cleaned)
    return cleaned.strip()


def _coerce_labeled_message_object(
    content_text: str,
    message_schema: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if not content_text.strip() or not isinstance(message_schema, Mapping):
        return None
    properties = message_schema.get("properties")
    if not isinstance(properties, Mapping) or not properties:
        return None

    field_aliases: dict[str, set[str]] = {}
    for field_name, field_schema in properties.items():
        normalized_name = str(field_name).strip()
        if not normalized_name or not _string_like_schema(field_schema):
            continue
        field_aliases[normalized_name] = _candidate_field_labels(normalized_name)
    if not field_aliases:
        return None

    required_fields = [
        str(field_name).strip()
        for field_name in message_schema.get("required", [])
        if str(field_name).strip()
    ]
    if any(field_name not in field_aliases for field_name in required_fields):
        return None

    normalized_text = content_text.replace("\r\n", "\n").replace("\r", "\n").strip()
    heading_pattern = re.compile(r"(?im)^\s*(?:\*\*)?\s*([A-Za-z][A-Za-z0-9 _/\-]{0,60})\s*(?:\*\*)?\s*:\s*")
    matches: list[tuple[int, int, str]] = []
    for match in heading_pattern.finditer(normalized_text):
        normalized_label = _normalized_label_key(match.group(1))
        for field_name, aliases in field_aliases.items():
            if normalized_label in aliases:
                matches.append((match.start(), match.end(), field_name))
                break

    extracted_values: dict[str, str] = {}
    if matches:
        matches.sort(key=lambda entry: entry[0])
        for index, (_start, value_start, field_name) in enumerate(matches):
            if field_name in extracted_values:
                continue
            next_start = matches[index + 1][0] if index + 1 < len(matches) else len(normalized_text)
            extracted_values[field_name] = _clean_labeled_value(normalized_text[value_start:next_start])
    elif len(field_aliases) == 1:
        only_field = next(iter(field_aliases))
        extracted_values[only_field] = _clean_labeled_value(normalized_text)

    if not extracted_values:
        return None

    coerced: dict[str, Any] = {}
    for field_name in field_aliases:
        if field_name in extracted_values and extracted_values[field_name]:
            coerced[field_name] = extracted_values[field_name]
        elif field_name in required_fields:
            coerced[field_name] = ""
    if not all(field_name in coerced for field_name in required_fields):
        return None
    return coerced


def _string_config(config: Mapping[str, Any], key: str, default: str) -> str:
    value = config.get(key, default)
    return value if isinstance(value, str) and value.strip() else default


def _number_config(config: Mapping[str, Any], key: str) -> float | int | None:
    value = config.get(key)
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value
    return None


def _bool_config(config: Mapping[str, Any], key: str, default: bool = False) -> bool:
    value = config.get(key)
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


def _truncate_for_log(value: str, limit: int = _LOG_PREVIEW_LIMIT) -> str:
    normalized = " ".join(value.split())
    if len(normalized) <= limit:
        return normalized
    return f"{normalized[:limit]}..."


def _command_preview(command: Sequence[str]) -> str:
    preview_parts: list[str] = []
    for index, part in enumerate(command):
        if index > 0 and command[index - 1] in {"-p", "--system-prompt", "--json-schema"}:
            preview_parts.append(f"<{command[index - 1].lstrip('-')}>")
            continue
        preview_parts.append(part)
    return " ".join(preview_parts)


def _format_timeout_seconds(timeout_seconds: float) -> str:
    return str(int(timeout_seconds)) if float(timeout_seconds).is_integer() else f"{timeout_seconds:g}"


def _resolved_max_turns(config: Mapping[str, Any]) -> int | None:
    raw_value = _number_config(config, "max_turns")
    if raw_value is None:
        return _MIN_REQUEST_TURNS
    configured = int(raw_value)
    if configured <= 0:
        return None
    # Claude Code can stop on an intermediate tool_use before returning JSON.
    return max(configured, _MIN_REQUEST_TURNS)


class ClaudeCodeCLIModelProvider(ModelProvider):
    name = "claude_code"
    default_cli_path = "claude"
    default_model = "sonnet"

    def generate(self, request: ModelRequest) -> ModelResponse:
        started_at = time.perf_counter()
        provider_config = self._provider_config(request)
        tools = self._tool_definitions(request)
        response_schema = self._response_schema(request, tools)
        payload = self._run_command(
            command=self._build_command(request, provider_config, tools, response_schema),
            cwd=self._working_directory(provider_config),
            timeout_seconds=float(_number_config(provider_config, "timeout_seconds") or 60),
        )
        latency_ms = int((time.perf_counter() - started_at) * 1000)
        return self._parse_response(payload, tools, response_schema, provider_config, latency_ms)

    def _provider_config(self, request: ModelRequest) -> Mapping[str, Any]:
        return request.provider_config if _is_mapping(request.provider_config) else {}

    def preflight(self, provider_config: Mapping[str, Any] | None = None) -> ProviderPreflightResult:
        config = provider_config if _is_mapping(provider_config) else {}
        cli_path = _string_config(config, "cli_path", self.default_cli_path)
        working_directory = self._working_directory(config)
        timeout_seconds = float(_number_config(config, "timeout_seconds") or 15)
        warnings = self._billing_warnings()
        child_env = self._child_env()
        LOGGER.info(
            "claude_code preflight starting: cli_path=%s cwd=%s timeout_seconds=%s check_auth=%s anthropic_api_key_present=%s sanitized_env=%s",
            cli_path,
            working_directory or "<workspace>",
            timeout_seconds,
            _bool_config(config, "check_auth", False),
            self._anthropic_api_key_present(),
            _ANTHROPIC_API_KEY_ENV_VAR not in child_env,
        )
        try:
            completed = subprocess.run(
                [cli_path, "--version"],
                capture_output=True,
                text=True,
                check=False,
                cwd=working_directory,
                env=child_env,
                timeout=min(timeout_seconds, 15),
            )
        except FileNotFoundError:
            LOGGER.warning("claude_code preflight failed: cli binary not found at '%s'", cli_path)
            return ProviderPreflightResult(
                status="missing_cli",
                ok=False,
                message="Claude Code CLI was not found. Install `claude` or set `cli_path`.",
                warnings=warnings,
                details={"cli_path": cli_path},
            )
        except subprocess.TimeoutExpired:
            LOGGER.warning("claude_code preflight failed: version check timed out after %s seconds", min(timeout_seconds, 15))
            return ProviderPreflightResult(
                status="timeout",
                ok=False,
                message="Claude Code CLI version check timed out.",
                warnings=warnings,
                details={"cli_path": cli_path},
            )
        except OSError as exc:
            LOGGER.warning("claude_code preflight failed to start CLI: %s", exc)
            return ProviderPreflightResult(
                status="unavailable",
                ok=False,
                message=f"Claude Code CLI could not be started: {exc}",
                warnings=warnings,
                details={
                    "cli_path": cli_path,
                    "os_error_errno": exc.errno,
                    "os_error_message": str(exc),
                },
            )

        LOGGER.info(
            "claude_code preflight version check finished: returncode=%s stdout=%s stderr=%s",
            completed.returncode,
            _truncate_for_log(completed.stdout.strip()),
            _truncate_for_log(completed.stderr.strip()),
        )

        if completed.returncode != 0:
            detail = completed.stderr.strip() or completed.stdout.strip() or f"exit code {completed.returncode}"
            LOGGER.warning("claude_code preflight unhealthy version check: %s", _truncate_for_log(detail))
            return ProviderPreflightResult(
                status="unavailable",
                ok=False,
                message=f"Claude Code CLI is installed but not healthy: {detail}",
                warnings=warnings,
                details={"cli_path": cli_path},
            )

        if not _bool_config(config, "check_auth", False):
            version_text = completed.stdout.strip() or completed.stderr.strip()
            LOGGER.info("claude_code preflight completed without live auth check")
            return ProviderPreflightResult(
                status="installed",
                ok=True,
                message="Claude Code CLI is installed. Run live verification to confirm auth/subscription access.",
                warnings=warnings,
                details={
                    "backend_type": "claude_code",
                    "auth_mode": "claude_code_subscription",
                    "cli_path": cli_path,
                    "version": version_text,
                    "anthropic_api_key_present": self._anthropic_api_key_present(),
                    "sanitized_child_env": True,
                    "sanitized_env_removed_vars": [_ANTHROPIC_API_KEY_ENV_VAR],
                },
            )

        try:
            self._run_command(
                command=self._healthcheck_command(config),
                cwd=working_directory,
                timeout_seconds=timeout_seconds,
            )
        except RuntimeError as exc:
            detail = str(exc)
            LOGGER.warning("claude_code live preflight failed: %s", _truncate_for_log(detail))
            if self._is_auth_error(detail):
                return ProviderPreflightResult(
                    status="unauthenticated",
                    ok=False,
                    message="Claude Code CLI is installed but is not authenticated or lacks subscription access.",
                    warnings=warnings,
                    details={
                        "backend_type": "claude_code",
                        "auth_mode": "claude_code_subscription",
                        "cli_path": cli_path,
                        "error": detail,
                        "anthropic_api_key_present": self._anthropic_api_key_present(),
                        "sanitized_child_env": True,
                        "sanitized_env_removed_vars": [_ANTHROPIC_API_KEY_ENV_VAR],
                    },
                )
            return ProviderPreflightResult(
                status="unavailable",
                ok=False,
                message=detail,
                warnings=warnings,
                details={
                    "backend_type": "claude_code",
                    "auth_mode": "claude_code_subscription",
                    "cli_path": cli_path,
                    "anthropic_api_key_present": self._anthropic_api_key_present(),
                    "sanitized_child_env": True,
                    "sanitized_env_removed_vars": [_ANTHROPIC_API_KEY_ENV_VAR],
                },
            )

        LOGGER.info("claude_code live preflight succeeded")
        return ProviderPreflightResult(
            status="available",
            ok=True,
            message="Claude Code CLI is installed and responded successfully.",
            warnings=warnings,
            details={
                "backend_type": "claude_code",
                "auth_mode": "claude_code_subscription",
                "cli_path": cli_path,
                "anthropic_api_key_present": self._anthropic_api_key_present(),
                "sanitized_child_env": True,
                "sanitized_env_removed_vars": [_ANTHROPIC_API_KEY_ENV_VAR],
            },
        )

    def _tool_definitions(self, request: ModelRequest) -> list[ModelToolDefinition]:
        if request.available_tools:
            return list(request.available_tools)
        if request.response_mode != "tool_call" or not _is_mapping(request.response_schema):
            return []
        tool_name = request.preferred_tool_name or "emit_structured_output"
        return [
            ModelToolDefinition(
                name=tool_name,
                description="Return the structured payload for this graph node.",
                input_schema=_strict_json_schema(request.response_schema),
            )
        ]

    def _preferred_tool_name(self, request: ModelRequest, tools: list[ModelToolDefinition]) -> str | None:
        preferred_name = str(request.preferred_tool_name or "").strip()
        if not preferred_name:
            return None
        if any(tool.name == preferred_name for tool in tools):
            return preferred_name
        return None

    def _response_schema(
        self,
        request: ModelRequest,
        tools: list[ModelToolDefinition],
    ) -> Mapping[str, Any] | None:
        if _is_mapping(request.response_schema):
            return _strict_json_schema(request.response_schema)
        if tools:
            return _strict_json_schema(api_decision_response_schema(available_tools=tools, allow_tool_calls=True))
        return None

    def _build_command(
        self,
        request: ModelRequest,
        provider_config: Mapping[str, Any],
        tools: list[ModelToolDefinition],
        response_schema: Mapping[str, Any] | None,
    ) -> list[str]:
        command = [_string_config(provider_config, "cli_path", self.default_cli_path)]

        model = _string_config(provider_config, "model", self.default_model)
        if model:
            command.extend(["--model", model])

        system_prompt = self._system_prompt(request.messages)
        if system_prompt:
            command.extend(["--system-prompt", system_prompt])

        max_turns = _resolved_max_turns(provider_config)
        if max_turns is not None:
            command.extend(["--max-turns", str(max_turns)])

        command.extend(
            [
                "-p",
                self._prompt_text(request.messages),
                "--output-format",
                "json",
                "--tools",
                "",
                "--no-session-persistence",
            ]
        )

        if response_schema is not None:
            command.extend(["--json-schema", json.dumps(dict(response_schema), separators=(",", ":"))])

        return command

    def _healthcheck_command(self, provider_config: Mapping[str, Any]) -> list[str]:
        healthcheck_config = dict(provider_config)
        configured_max_turns = _resolved_max_turns(provider_config)
        healthcheck_config["max_turns"] = max(configured_max_turns or 0, _HEALTHCHECK_MIN_TURNS)
        return self._build_command(
            ModelRequest(
                prompt_name="claude_code_healthcheck",
                messages=[ModelMessage(role="user", content="Reply with a JSON object containing {\"status\":\"ok\"}.")],
                response_schema={
                    "type": "object",
                    "properties": {"status": {"type": "string", "const": "ok"}},
                    "required": ["status"],
                    "additionalProperties": False,
                },
                provider_config=healthcheck_config,
                response_mode="message",
            ),
            healthcheck_config,
            [],
            {
                "type": "object",
                "properties": {"status": {"type": "string", "const": "ok"}},
                "required": ["status"],
                "additionalProperties": False,
            },
        )

    def _is_auth_error(self, detail: str) -> bool:
        normalized = detail.lower()
        auth_markers = [
            "login",
            "logged in",
            "not authenticated",
            "authentication",
            "subscription",
            "plan",
            "access denied",
        ]
        return any(marker in normalized for marker in auth_markers)

    def _system_prompt(self, messages: Sequence[ModelMessage]) -> str:
        parts = [message.content for message in messages if message.role == "system" and message.content]
        return "\n\n".join(parts)

    def _prompt_text(self, messages: Sequence[ModelMessage]) -> str:
        conversation = [message for message in messages if message.role != "system" and message.content]
        if not conversation:
            return ""
        if len(conversation) == 1 and conversation[0].role == "user":
            return conversation[0].content

        parts: list[str] = []
        for message in conversation:
            if message.role == "user":
                role = "User"
            elif message.role == "assistant":
                role = "Assistant"
            else:
                role = message.role.capitalize()
            parts.append(f"{role}:\n{message.content}")
        return "\n\n".join(parts)

    def _working_directory(self, provider_config: Mapping[str, Any]) -> str | None:
        working_directory = provider_config.get("working_directory")
        if isinstance(working_directory, str) and working_directory.strip():
            return working_directory.strip()
        return None

    def _anthropic_api_key_present(self) -> bool:
        return bool(os.environ.get(_ANTHROPIC_API_KEY_ENV_VAR, "").strip())

    def _billing_warnings(self) -> list[str]:
        if not self._anthropic_api_key_present():
            return []
        return [
            "ANTHROPIC_API_KEY is set in the host environment. Claude Code can switch to API-key billing when it sees this variable, so this app removes it from Claude Code child processes to preserve subscription-backed auth."
        ]

    def _child_env(self) -> dict[str, str]:
        child_env = dict(os.environ)
        child_env.pop(_ANTHROPIC_API_KEY_ENV_VAR, None)
        return child_env

    def _run_command(self, command: Sequence[str], cwd: str | None, timeout_seconds: float) -> Mapping[str, Any]:
        child_env = self._child_env()
        started_at = time.perf_counter()
        LOGGER.info(
            "claude_code subprocess starting: command=%s cwd=%s timeout_seconds=%s anthropic_api_key_present=%s sanitized_env=%s",
            _command_preview(command),
            cwd or "<workspace>",
            timeout_seconds,
            self._anthropic_api_key_present(),
            _ANTHROPIC_API_KEY_ENV_VAR not in child_env,
        )
        try:
            process = subprocess.Popen(
                list(command),
                text=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=cwd,
                env=child_env,
                stdin=subprocess.DEVNULL,
            )
        except FileNotFoundError as exc:
            LOGGER.warning("claude_code subprocess failed: cli binary not found")
            raise RuntimeError(
                "claude_code provider could not find the Claude Code CLI. "
                "Install `claude` or set `cli_path` in the provider config."
            ) from exc
        except OSError as exc:
            LOGGER.warning("claude_code subprocess failed to start: %s", exc)
            raise RuntimeError(f"claude_code provider failed to start: {exc}") from exc

        selector = selectors.DefaultSelector()
        stdout_chunks: list[bytes] = []
        stderr_chunks: list[bytes] = []
        if process.stdout is not None:
            selector.register(process.stdout, selectors.EVENT_READ, data="stdout")
        if process.stderr is not None:
            selector.register(process.stderr, selectors.EVENT_READ, data="stderr")

        inactivity_deadline = time.monotonic() + timeout_seconds
        try:
            while selector.get_map():
                remaining_seconds = inactivity_deadline - time.monotonic()
                if remaining_seconds <= 0:
                    raise subprocess.TimeoutExpired(list(command), timeout_seconds)
                events = selector.select(timeout=remaining_seconds)
                if not events:
                    raise subprocess.TimeoutExpired(list(command), timeout_seconds)
                made_progress = False
                for key, _mask in events:
                    chunk = key.fileobj.read1(4096)
                    if chunk:
                        made_progress = True
                        if key.data == "stdout":
                            stdout_chunks.append(chunk)
                        else:
                            stderr_chunks.append(chunk)
                        continue
                    selector.unregister(key.fileobj)
                    key.fileobj.close()
                if made_progress:
                    inactivity_deadline = time.monotonic() + timeout_seconds
            completed = process.wait(timeout=1)
        except subprocess.TimeoutExpired as exc:
            process.kill()
            try:
                remaining_stdout, remaining_stderr = process.communicate(timeout=1)
            except subprocess.TimeoutExpired:
                remaining_stdout, remaining_stderr = (b"", b"")
            if remaining_stdout:
                stdout_chunks.append(remaining_stdout)
            if remaining_stderr:
                stderr_chunks.append(remaining_stderr)
            timeout_label = _format_timeout_seconds(timeout_seconds)
            LOGGER.warning("claude_code subprocess timed out after %s seconds without output progress", timeout_label)
            raise RuntimeError(
                f"claude_code provider timed out after {timeout_label} seconds without output progress."
            ) from exc
        finally:
            selector.close()

        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        stdout = b"".join(stdout_chunks).decode("utf-8", errors="replace").strip()
        stderr = b"".join(stderr_chunks).decode("utf-8", errors="replace").strip()
        LOGGER.info(
            "claude_code subprocess finished: returncode=%s elapsed_ms=%s stdout=%s stderr=%s",
            completed,
            elapsed_ms,
            _truncate_for_log(stdout),
            _truncate_for_log(stderr),
        )
        if completed != 0:
            detail = stderr or stdout or f"exit code {completed}"
            friendly_detail = _friendly_cli_error_detail(detail)
            raise RuntimeError(f"claude_code provider request failed: {friendly_detail}")

        if not stdout:
            LOGGER.warning("claude_code subprocess returned no stdout")
            raise RuntimeError("claude_code provider returned no output.")

        try:
            payload = json.loads(stdout)
        except json.JSONDecodeError as exc:
            LOGGER.warning("claude_code subprocess returned invalid JSON stdout=%s", _truncate_for_log(stdout))
            raise RuntimeError("claude_code provider returned invalid JSON output.") from exc

        if not _is_mapping(payload):
            LOGGER.warning("claude_code subprocess returned unexpected payload type=%s", type(payload).__name__)
            raise RuntimeError("claude_code provider returned an unexpected response shape.")
        LOGGER.info(
            "claude_code payload parsed: keys=%s session_id=%s stop_reason=%s subtype=%s is_error=%s",
            sorted(str(key) for key in payload.keys()),
            payload.get("session_id"),
            payload.get("stop_reason"),
            payload.get("subtype"),
            payload.get("is_error"),
        )
        return payload

    def _parse_response(
        self,
        payload: Mapping[str, Any],
        tools: list[ModelToolDefinition],
        response_schema: Mapping[str, Any] | None,
        provider_config: Mapping[str, Any],
        latency_ms: int,
    ) -> ModelResponse:
        content = payload.get("result")
        content_text = content if isinstance(content, str) else ""
        structured_output = payload.get("structured_output") if response_schema is not None else None
        if structured_output is None and response_schema is not None and content_text.strip():
            try:
                structured_output = json.loads(content_text)
            except json.JSONDecodeError:
                structured_output = None
        if structured_output is None and response_schema is not None and content_text.strip() and not tools:
            message_schema = _decision_message_object_schema(response_schema)
            coerced_message = _coerce_labeled_message_object(content_text, message_schema)
            if coerced_message is not None:
                structured_output = {
                    "message": coerced_message,
                    "need_tool": False,
                    "tool_calls": [],
                }

        normalized_tool_calls: list[ModelToolCall] = []
        if _is_mapping(structured_output):
            if isinstance(structured_output.get("tool_name"), str) and "arguments" in structured_output:
                normalized_tool_calls.append(
                    ModelToolCall(
                        tool_name=str(structured_output["tool_name"]),
                        arguments=structured_output.get("arguments"),
                    )
                )
            elif len(tools) == 1:
                normalized_tool_calls.append(
                    ModelToolCall(
                        tool_name=tools[0].name,
                        arguments=dict(structured_output),
                    )
                )

        decision_output = normalize_api_decision_output(
            structured_output,
            content=content_text,
            tool_calls=normalized_tool_calls,
        )
        normalized_decision_tool_calls = [
            ModelToolCall(
                tool_name=str(tool_call["tool_name"]),
                arguments=tool_call.get("arguments"),
                provider_tool_id=tool_call.get("provider_tool_id"),
                metadata=dict(tool_call.get("metadata", {})),
            )
            for tool_call in decision_output["tool_calls"]
        ]
        requested_model = _string_config(provider_config, "model", self.default_model)
        reported_model_value = payload.get("model")
        reported_model = str(reported_model_value).strip() if reported_model_value is not None else ""
        resolved_reported_model = reported_model or None
        resolved_vendor_model = resolved_reported_model or requested_model

        return ModelResponse(
            content=content_text,
            structured_output=decision_output,
            tool_calls=normalized_decision_tool_calls,
            metadata={
                "latency_ms": latency_ms,
                "requested_model": requested_model,
                "reported_model": resolved_reported_model,
                "vendor_model": resolved_vendor_model,
                "session_id": payload.get("session_id"),
                "total_cost_usd": payload.get("total_cost_usd"),
                "duration_ms": payload.get("duration_ms"),
                "num_turns": payload.get("num_turns"),
                "stop_reason": payload.get("stop_reason"),
                "terminal_reason": payload.get("terminal_reason"),
                "errors": payload.get("errors"),
                "usage": payload.get("usage"),
            },
        )
