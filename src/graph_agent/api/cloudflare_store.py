from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
from typing import Any


DEFAULT_TOKEN_ENV_VAR = "CLOUDFLARE_TUNNEL_TOKEN"


def _sanitize(payload: Any) -> dict[str, Any]:
    """Coerce a raw config dict into the canonical persisted shape.

    `tunnel_token_env_var` references the env-var holding the secret (the secret
    itself is never stored on disk). `public_hostname` is the externally
    reachable URL configured on the Cloudflare tunnel.
    """
    if not isinstance(payload, dict):
        return {"tunnel_token_env_var": DEFAULT_TOKEN_ENV_VAR, "public_hostname": ""}
    token_env_var = str(payload.get("tunnel_token_env_var") or DEFAULT_TOKEN_ENV_VAR).strip() or DEFAULT_TOKEN_ENV_VAR
    public_hostname = str(payload.get("public_hostname") or "").strip()
    return {
        "tunnel_token_env_var": token_env_var,
        "public_hostname": public_hostname,
    }


class CloudflareConfigStore:
    """Single-record JSON store for the Cloudflare tunnel configuration.

    Mirrors the GraphStore pattern: persistence sits under `.graph-agent/`
    (ignored by git), the secret token is referenced by env-var name only.
    """

    def __init__(self, path: Path | None = None) -> None:
        self.path = path or Path(__file__).resolve().parents[3] / ".graph-agent" / "cloudflare_config.json"

    def get(self) -> dict[str, Any]:
        if not self.path.exists():
            return _sanitize(None)
        try:
            data = json.loads(self.path.read_text())
        except (OSError, json.JSONDecodeError):
            return _sanitize(None)
        return _sanitize(data)

    def set(self, payload: dict[str, Any]) -> dict[str, Any]:
        sanitized = _sanitize(payload)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(sanitized, indent=2))
        return deepcopy(sanitized)

    def clear(self) -> None:
        if self.path.exists():
            self.path.unlink()
