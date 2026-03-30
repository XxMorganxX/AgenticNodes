from __future__ import annotations

import json
import subprocess
import time
from collections.abc import Mapping, Sequence
from typing import Any

from graph_agent.providers.base import ModelMessage, ModelProvider, ModelRequest, ModelResponse


def _is_mapping(value: Any) -> bool:
    return isinstance(value, Mapping)


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


class ClaudeCodeCLIModelProvider(ModelProvider):
    name = "claude_code"
    default_cli_path = "claude"
    default_model = "sonnet"

    def generate(self, request: ModelRequest) -> ModelResponse:
        started_at = time.perf_counter()
        provider_config = self._provider_config(request)
        response_schema = self._resolve_response_schema(request)
        payload = self._run_command(
            command=self._build_command(request, provider_config, response_schema),
            cwd=self._working_directory(provider_config),
            timeout_seconds=float(_number_config(provider_config, "timeout_seconds") or 60),
        )
        latency_ms = int((time.perf_counter() - started_at) * 1000)
        return self._parse_response(payload, response_schema, provider_config, latency_ms)

    def _provider_config(self, request: ModelRequest) -> Mapping[str, Any]:
        return request.provider_config if _is_mapping(request.provider_config) else {}

    def _resolve_response_schema(self, request: ModelRequest) -> Mapping[str, Any] | None:
        if _is_mapping(request.response_schema):
            return request.response_schema

        if str(request.metadata.get("response_mode", "message")) != "tool_call":
            return None

        available_tools = request.metadata.get("available_tools", [])
        if not isinstance(available_tools, list):
            return None

        preferred_name = request.metadata.get("preferred_tool_name")
        preferred_tool = None
        if isinstance(preferred_name, str) and preferred_name:
            preferred_tool = next(
                (
                    tool
                    for tool in available_tools
                    if _is_mapping(tool) and tool.get("name") == preferred_name and _is_mapping(tool.get("input_schema"))
                ),
                None,
            )
        if preferred_tool is None:
            preferred_tool = next(
                (tool for tool in available_tools if _is_mapping(tool) and _is_mapping(tool.get("input_schema"))),
                None,
            )
        if preferred_tool is None:
            return None
        return preferred_tool["input_schema"]

    def _build_command(
        self,
        request: ModelRequest,
        provider_config: Mapping[str, Any],
        response_schema: Mapping[str, Any] | None,
    ) -> list[str]:
        command = [_string_config(provider_config, "cli_path", self.default_cli_path)]

        model = _string_config(provider_config, "model", self.default_model)
        if model:
            command.extend(["--model", model])

        system_prompt = self._system_prompt(request.messages)
        if system_prompt:
            command.extend(["--system-prompt", system_prompt])

        max_turns = int(_number_config(provider_config, "max_turns") or 1)
        if max_turns > 0:
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

    def _run_command(self, command: Sequence[str], cwd: str | None, timeout_seconds: float) -> Mapping[str, Any]:
        try:
            completed = subprocess.run(
                list(command),
                capture_output=True,
                text=True,
                check=False,
                cwd=cwd,
                timeout=timeout_seconds,
            )
        except FileNotFoundError as exc:
            raise RuntimeError(
                "claude_code provider could not find the Claude Code CLI. "
                "Install `claude` or set `cli_path` in the provider config."
            ) from exc
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(
                f"claude_code provider timed out after {int(timeout_seconds)} seconds."
            ) from exc
        except OSError as exc:
            raise RuntimeError(f"claude_code provider failed to start: {exc}") from exc

        stdout = completed.stdout.strip()
        stderr = completed.stderr.strip()
        if completed.returncode != 0:
            detail = stderr or stdout or f"exit code {completed.returncode}"
            raise RuntimeError(f"claude_code provider request failed: {detail}")

        if not stdout:
            raise RuntimeError("claude_code provider returned no output.")

        try:
            payload = json.loads(stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError("claude_code provider returned invalid JSON output.") from exc

        if not _is_mapping(payload):
            raise RuntimeError("claude_code provider returned an unexpected response shape.")
        return payload

    def _parse_response(
        self,
        payload: Mapping[str, Any],
        response_schema: Mapping[str, Any] | None,
        provider_config: Mapping[str, Any],
        latency_ms: int,
    ) -> ModelResponse:
        content = payload.get("result")
        content_text = content if isinstance(content, str) else ""
        structured_output = payload.get("structured_output") if response_schema is not None else None
        if structured_output is None and response_schema is not None and content_text.strip():
            structured_output = json.loads(content_text)

        return ModelResponse(
            content=content_text,
            structured_output=structured_output,
            metadata={
                "latency_ms": latency_ms,
                "vendor_model": payload.get("model") or _string_config(provider_config, "model", self.default_model),
                "session_id": payload.get("session_id"),
                "total_cost_usd": payload.get("total_cost_usd"),
                "duration_ms": payload.get("duration_ms"),
                "usage": payload.get("usage"),
            },
        )
