from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
from typing import Any, Mapping
from urllib.parse import urlparse, urlunparse

from graph_agent.runtime.agent_filesystem import resolve_agent_filesystem_root, write_agent_workspace_text_file


LINKEDIN_PROFILE_URL_PATTERN = re.compile(r"^/(?:public-profile/)?in/([^/]+)/?$", re.IGNORECASE)
SAFE_CACHE_KEY_SEGMENT_PATTERN = re.compile(r"[^A-Za-z0-9._-]+")
LINKEDIN_RAW_TEXT_FIELD_NAMES = {
    "filteredtextlines",
    "rawline",
    "rawlines",
    "rawtext",
    "rawtexts",
    "textline",
    "textlines",
}


class LinkedInFetchError(RuntimeError):
    def __init__(self, error_type: str, message: str, *, details: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.error_type = error_type
        self.details = dict(details or {})

    def to_error_dict(self) -> dict[str, Any]:
        return {
            "type": self.error_type,
            "message": str(self),
            **self.details,
        }


@dataclass(frozen=True)
class LinkedInProfileCacheInfo:
    source_url: str
    normalized_url: str
    cache_key: str
    shared_cache_root: Path
    shared_cache_path: Path


class _SafeFormatDict(dict[str, Any]):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def _normalize_linkedin_field_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def _is_linkedin_raw_text_field_name(value: Any) -> bool:
    normalized = _normalize_linkedin_field_name(value)
    if normalized in LINKEDIN_RAW_TEXT_FIELD_NAMES:
        return True
    return normalized.startswith("raw") and ("text" in normalized or "line" in normalized)


def _linkedin_value_has_content(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, Mapping):
        return any(_linkedin_value_has_content(candidate) for candidate in value.values())
    if isinstance(value, (list, tuple)):
        return any(_linkedin_value_has_content(candidate) for candidate in value)
    if isinstance(value, bool):
        return value
    return True


def _sanitize_linkedin_profile_value(value: Any, *, parent_key: str | None = None) -> Any:
    if isinstance(value, Mapping):
        sanitized_items = {
            str(key): _sanitize_linkedin_profile_value(item, parent_key=str(key))
            for key, item in value.items()
        }
        raw_text_keys = [key for key in sanitized_items if _is_linkedin_raw_text_field_name(key)]
        if raw_text_keys:
            has_structured_content = any(
                _linkedin_value_has_content(item)
                for key, item in sanitized_items.items()
                if key not in raw_text_keys
            )
            if has_structured_content:
                for key in raw_text_keys:
                    sanitized_items.pop(key, None)
        if "skills" in sanitized_items and _normalize_linkedin_field_name(parent_key) == "sections":
            sanitized_items.pop("skills", None)
        return sanitized_items
    if isinstance(value, list):
        return [_sanitize_linkedin_profile_value(item, parent_key=parent_key) for item in value]
    if isinstance(value, tuple):
        return tuple(_sanitize_linkedin_profile_value(item, parent_key=parent_key) for item in value)
    return value


def sanitize_linkedin_profile_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    sanitized = _sanitize_linkedin_profile_value(payload)
    return dict(sanitized) if isinstance(sanitized, Mapping) else dict(payload)


def extract_linkedin_profile_url(value: Any, *, url_field: str = "url") -> str | None:
    if isinstance(value, str):
        candidate = value.strip()
        return candidate or None
    if isinstance(value, Mapping):
        candidate = value.get(str(url_field or "url"))
        if isinstance(candidate, str):
            resolved = candidate.strip()
            return resolved or None
    return None


def normalize_linkedin_profile_url(url: str) -> str:
    candidate = str(url or "").strip()
    if not candidate:
        raise LinkedInFetchError("invalid_linkedin_profile_url", "A LinkedIn profile URL is required.")
    if "://" not in candidate:
        candidate = f"https://{candidate.lstrip('/')}"
    parsed = urlparse(candidate)
    host = parsed.netloc.strip().lower()
    if not host:
        raise LinkedInFetchError("invalid_linkedin_profile_url", f"'{url}' is not a valid LinkedIn profile URL.")
    host = host.split("@")[-1].split(":")[0]
    if host == "linkedin.com":
        host = "www.linkedin.com"
    elif not host.endswith(".linkedin.com"):
        raise LinkedInFetchError("invalid_linkedin_profile_url", f"'{url}' is not a LinkedIn URL.")

    path_segments = [segment for segment in parsed.path.split("/") if segment]
    normalized_path = ""
    if len(path_segments) >= 2 and path_segments[0].lower() == "in":
        normalized_path = f"/in/{path_segments[1].strip()}/"
    elif len(path_segments) >= 3 and path_segments[0].lower() == "public-profile" and path_segments[1].lower() == "in":
        normalized_path = f"/public-profile/in/{path_segments[2].strip()}/"
    if not normalized_path:
        raise LinkedInFetchError(
            "invalid_linkedin_profile_url",
            f"'{url}' is not a supported LinkedIn profile URL. Expected a profile path like /in/<slug>/.",
        )
    if not LINKEDIN_PROFILE_URL_PATTERN.match(normalized_path):
        raise LinkedInFetchError(
            "invalid_linkedin_profile_url",
            f"'{url}' is not a supported LinkedIn profile URL. Expected a profile path like /in/<slug>/.",
        )
    return urlunparse(("https", "www.linkedin.com", normalized_path, "", "", ""))


def build_linkedin_profile_cache_info(url: str) -> LinkedInProfileCacheInfo:
    normalized_url = normalize_linkedin_profile_url(url)
    slug_match = LINKEDIN_PROFILE_URL_PATTERN.match(urlparse(normalized_url).path)
    slug = slug_match.group(1) if slug_match else "profile"
    safe_slug = SAFE_CACHE_KEY_SEGMENT_PATTERN.sub("-", slug.lower()).strip("-.") or "profile"
    digest = hashlib.sha256(normalized_url.encode("utf-8")).hexdigest()[:16]
    cache_key = f"{safe_slug}-{digest}"
    shared_cache_root = resolve_shared_linkedin_cache_root()
    return LinkedInProfileCacheInfo(
        source_url=url,
        normalized_url=normalized_url,
        cache_key=cache_key,
        shared_cache_root=shared_cache_root,
        shared_cache_path=shared_cache_root / f"{cache_key}.json",
    )


def resolve_shared_linkedin_cache_root() -> Path:
    return resolve_agent_filesystem_root().parent / "cache" / "linkedin"


def describe_linkedin_cache_file(path: Path, *, root: Path | None = None) -> dict[str, Any]:
    resolved_path = path.resolve()
    stat = resolved_path.stat()
    relative_path = resolved_path.relative_to((root or resolved_path.parent).resolve()).as_posix()
    return {
        "path": relative_path,
        "name": resolved_path.name,
        "size_bytes": stat.st_size,
        "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
        "mime_type": "application/json",
        "absolute_path": str(resolved_path),
    }


def read_cached_linkedin_profile(cache_info: LinkedInProfileCacheInfo) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    try:
        raw = cache_info.shared_cache_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None, None
    except OSError:
        return None, None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None, None
    if not isinstance(payload, dict):
        return None, None
    return sanitize_linkedin_profile_payload(payload), describe_linkedin_cache_file(
        cache_info.shared_cache_path,
        root=cache_info.shared_cache_root,
    )


def write_cached_linkedin_profile(
    cache_info: LinkedInProfileCacheInfo,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    cache_info.shared_cache_root.mkdir(parents=True, exist_ok=True)
    cache_info.shared_cache_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return describe_linkedin_cache_file(cache_info.shared_cache_path, root=cache_info.shared_cache_root)


def workspace_cache_relative_path(template: str, *, cache_key: str) -> str:
    resolved = str(template or "cache/linkedin/{cache_key}.json").format_map(_SafeFormatDict({"cache_key": cache_key}))
    return resolved.strip() or f"cache/linkedin/{cache_key}.json"


def write_linkedin_profile_workspace_copy(
    run_id: str,
    agent_id: str | None,
    template: str,
    *,
    cache_key: str,
    payload: Mapping[str, Any],
) -> tuple[str, dict[str, Any]]:
    relative_path = workspace_cache_relative_path(template, cache_key=cache_key)
    file_record = write_agent_workspace_text_file(
        run_id,
        agent_id,
        relative_path,
        json.dumps(payload, indent=2, sort_keys=True),
        exists_behavior="overwrite",
        append_newline=False,
    )
    return relative_path, file_record


def is_cacheable_linkedin_profile(payload: Mapping[str, Any]) -> bool:
    status = str(payload.get("status", "") or "").strip().lower()
    return status == "ok"


def error_from_linkedin_profile_payload(
    payload: Mapping[str, Any],
    *,
    source_url: str,
    normalized_url: str,
) -> dict[str, Any]:
    status = str(payload.get("status", "unknown") or "unknown").strip().lower() or "unknown"
    page_type = str(payload.get("pageType", "unknown") or "unknown").strip().lower() or "unknown"
    error_type = {
        "blocked": "linkedin_fetch_blocked",
        "not_found": "linkedin_profile_not_found",
        "unavailable": "linkedin_profile_unavailable",
    }.get(status, "linkedin_fetch_failed")
    return {
        "type": error_type,
        "message": f"LinkedIn fetch returned status '{status}' for profile '{normalized_url}'.",
        "status": status,
        "page_type": page_type,
        "source_url": source_url,
        "normalized_url": normalized_url,
    }


def fetch_linkedin_profile_live(
    *,
    url: str,
    linkedin_data_dir: str,
    session_state_path: str = "",
    headless: bool = False,
    navigation_timeout_ms: int = 45000,
    page_settle_ms: int = 3000,
) -> dict[str, Any]:
    raw_data_dir = str(linkedin_data_dir or "").strip()
    if not raw_data_dir:
        raise LinkedInFetchError(
            "linkedin_fetch_assets_missing",
            "LinkedIn data directory is not configured.",
            details={"linkedin_data_dir": ""},
        )

    data_dir = Path(raw_data_dir).expanduser()
    if not data_dir.exists() or not data_dir.is_dir():
        raise LinkedInFetchError(
            "linkedin_fetch_assets_missing",
            f"LinkedIn data directory '{data_dir}' does not exist.",
            details={"linkedin_data_dir": str(data_dir)},
        )

    bridge_path = Path(__file__).resolve().with_name("linkedin_fetch_bridge.js")
    node_binary = shutil.which("node")
    if not node_binary:
        raise LinkedInFetchError("linkedin_fetch_node_missing", "The 'node' executable is required to fetch LinkedIn profiles.")

    payload = {
        "url": url,
        "linkedinDataDir": str(data_dir),
        "sessionStatePath": str(session_state_path or ""),
        "headless": bool(headless),
        "navigationTimeoutMs": int(navigation_timeout_ms),
        "pageSettleMs": int(page_settle_ms),
    }

    completed = subprocess.run(
        [node_binary, str(bridge_path)],
        input=json.dumps(payload),
        text=True,
        capture_output=True,
        check=False,
        cwd=str(data_dir),
    )

    stdout = completed.stdout.strip()
    stderr = completed.stderr.strip()
    bridge_response: dict[str, Any] | None = None
    if stdout:
        try:
            parsed = json.loads(stdout)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, dict):
            bridge_response = parsed

    if completed.returncode != 0:
        if bridge_response is not None and bridge_response.get("error"):
            error = bridge_response["error"]
            if isinstance(error, Mapping):
                raise LinkedInFetchError(
                    str(error.get("type") or "linkedin_fetch_bridge_error"),
                    str(error.get("message") or "LinkedIn fetch bridge failed."),
                    details={k: v for k, v in error.items() if k not in {"type", "message"}},
                )
        raise LinkedInFetchError(
            "linkedin_fetch_bridge_error",
            stderr or stdout or "LinkedIn fetch bridge failed.",
            details={"returncode": completed.returncode},
        )

    if bridge_response is None:
        raise LinkedInFetchError("linkedin_fetch_bridge_error", "LinkedIn fetch bridge returned no JSON payload.")

    extracted = bridge_response.get("extracted")
    if not isinstance(extracted, Mapping):
        raise LinkedInFetchError("linkedin_fetch_invalid_response", "LinkedIn fetch bridge did not return a valid parsed payload.")

    return {
        "extracted": sanitize_linkedin_profile_payload(extracted),
        "final_page_url": str(bridge_response.get("finalPageUrl", "") or ""),
        "storage_state_path": str(bridge_response.get("storageStatePath", "") or ""),
    }
