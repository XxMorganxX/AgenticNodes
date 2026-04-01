from __future__ import annotations

from datetime import datetime
import json
import sys
from typing import Any


TOOLS = [
    {
        "name": "time_current_minute",
        "description": "Return the current local time rounded down to the minute.",
        "inputSchema": {
            "type": "object",
            "properties": {},
        },
    }
]


def _send(payload: dict[str, Any]) -> None:
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    sys.stdout.buffer.write(f"Content-Length: {len(body)}\r\n\r\n".encode("ascii"))
    sys.stdout.buffer.write(body)
    sys.stdout.buffer.flush()


def _read() -> dict[str, Any] | None:
    headers: dict[str, str] = {}
    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            return None
        if line in {b"\r\n", b"\n"}:
            break
        key, _, value = line.decode("ascii", errors="replace").partition(":")
        headers[key.strip().lower()] = value.strip()
    content_length = int(headers.get("content-length", "0") or "0")
    if content_length <= 0:
        return None
    payload = sys.stdin.buffer.read(content_length)
    if len(payload) != content_length:
        return None
    message = json.loads(payload.decode("utf-8"))
    return message if isinstance(message, dict) else None


def _response(message_id: Any, result: dict[str, Any]) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": message_id, "result": result}


def _error(message_id: Any, code: int, message: str) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": message_id, "error": {"code": code, "message": message}}


def _current_minute_payload() -> dict[str, Any]:
    current = datetime.now().astimezone().replace(second=0, microsecond=0)
    return {
        "local_iso_minute": current.isoformat(timespec="minutes"),
        "hour_24": current.hour,
        "minute": current.minute,
        "timezone": current.tzname() or "",
        "utc_offset": current.strftime("%z"),
    }


def _call_tool(name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    if name != "time_current_minute":
        return {
            "content": [{"type": "text", "text": f"Unknown tool '{name}'."}],
            "structuredContent": {"message": f"Unknown tool '{name}'."},
            "isError": True,
        }
    if arguments:
        return {
            "content": [{"type": "text", "text": "This tool does not accept arguments."}],
            "structuredContent": {"message": "This tool does not accept arguments."},
            "isError": True,
        }
    payload = _current_minute_payload()
    return {
        "content": [{"type": "text", "text": f"Current local minute: {payload['local_iso_minute']}"}],
        "structuredContent": payload,
        "isError": False,
    }


def main() -> int:
    while True:
        message = _read()
        if message is None:
            return 0
        message_id = message.get("id")
        method = message.get("method")
        params = message.get("params", {})
        if not isinstance(method, str):
            if message_id is not None:
                _send(_error(message_id, -32600, "Invalid request."))
            continue
        if method == "initialize":
            if message_id is not None:
                _send(
                    _response(
                        message_id,
                        {
                            "protocolVersion": "2024-11-05",
                            "capabilities": {"tools": {"listChanged": False}},
                            "serverInfo": {"name": "time-mcp-server", "version": "0.1.0"},
                        },
                    )
                )
            continue
        if method == "notifications/initialized":
            continue
        if method == "shutdown":
            return 0
        if method == "tools/list":
            if message_id is not None:
                _send(_response(message_id, {"tools": TOOLS}))
            continue
        if method == "tools/call":
            if not isinstance(params, dict):
                if message_id is not None:
                    _send(_error(message_id, -32602, "Invalid params."))
                continue
            name = params.get("name")
            arguments = params.get("arguments", {})
            if not isinstance(name, str) or not isinstance(arguments, dict):
                if message_id is not None:
                    _send(_error(message_id, -32602, "Invalid tool call params."))
                continue
            if message_id is not None:
                _send(_response(message_id, _call_tool(name, arguments)))
            continue
        if message_id is not None:
            _send(_error(message_id, -32601, f"Method '{method}' not found."))


if __name__ == "__main__":
    raise SystemExit(main())
