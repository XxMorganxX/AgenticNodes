from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Mapping
import urllib.error
import urllib.parse
import urllib.request

from graph_agent.runtime.agent_filesystem import resolve_agent_filesystem_root, write_agent_workspace_text_file


APOLLO_MATCH_URL = "https://api.apollo.io/api/v1/people/match"
APOLLO_EMAIL_LOOKUP_FIELDS = (
    "name",
    "domain",
    "organization_name",
    "first_name",
    "last_name",
    "linkedin_url",
    "email",
    "twitter_url",
)
APOLLO_LOOKUP_STATUSES = {"matched", "no_email", "no_match"}
SAFE_CACHE_KEY_SEGMENT_PATTERN = re.compile(r"[^A-Za-z0-9._-]+")


class ApolloLookupError(RuntimeError):
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
class ApolloEmailLookupRequest:
    name: str = ""
    domain: str = ""
    organization_name: str = ""
    first_name: str = ""
    last_name: str = ""
    linkedin_url: str = ""
    email: str = ""
    twitter_url: str = ""
    reveal_personal_emails: bool = False

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> ApolloEmailLookupRequest:
        payload = dict(value or {})
        return cls(
            name=_normalize_optional_text(payload.get("name")),
            domain=_normalize_optional_text(payload.get("domain")),
            organization_name=_normalize_optional_text(payload.get("organization_name")),
            first_name=_normalize_optional_text(payload.get("first_name")),
            last_name=_normalize_optional_text(payload.get("last_name")),
            linkedin_url=_normalize_optional_text(payload.get("linkedin_url")),
            email=_normalize_optional_text(payload.get("email")),
            twitter_url=_normalize_optional_text(payload.get("twitter_url")),
            reveal_personal_emails=_normalize_bool(payload.get("reveal_personal_emails"), default=False),
        )

    def to_lookup_fields(self) -> dict[str, Any]:
        payload = {
            "name": self.name,
            "domain": self.domain,
            "organization_name": self.organization_name,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "linkedin_url": self.linkedin_url,
            "email": self.email,
            "twitter_url": self.twitter_url,
            "reveal_personal_emails": self.reveal_personal_emails,
        }
        return {key: value for key, value in payload.items() if value not in {"", None}}

    def to_query_params(self) -> dict[str, str]:
        params = {
            "reveal_personal_emails": str(self.reveal_personal_emails).lower(),
            "reveal_phone_number": "false",
        }
        for key in APOLLO_EMAIL_LOOKUP_FIELDS:
            value = getattr(self, key)
            if value:
                params[key] = value
        return params

    def direct_identifier(self) -> str:
        for key in ("linkedin_url", "email", "twitter_url"):
            value = getattr(self, key)
            if value:
                return value
        return ""

    def has_person_identity(self) -> bool:
        return bool(self.name or (self.first_name and self.last_name))

    def has_organization_hint(self) -> bool:
        return bool(self.domain or self.organization_name)


@dataclass(frozen=True)
class ApolloEmailLookupCacheInfo:
    request: ApolloEmailLookupRequest
    cache_key: str
    shared_cache_root: Path
    shared_cache_path: Path


class _SafeFormatDict(dict[str, Any]):
    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def _normalize_optional_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_bool(value: Any, *, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "yes", "on"}:
        return True
    if normalized in {"false", "0", "no", "off"}:
        return False
    return default


def extract_apollo_lookup_fields(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    extracted: dict[str, Any] = {}
    for key in APOLLO_EMAIL_LOOKUP_FIELDS:
        candidate = _normalize_optional_text(value.get(key))
        if candidate:
            extracted[key] = candidate
    if "reveal_personal_emails" in value:
        extracted["reveal_personal_emails"] = _normalize_bool(value.get("reveal_personal_emails"), default=False)
    return extracted


def validate_apollo_lookup_request(request: ApolloEmailLookupRequest) -> str | None:
    if request.direct_identifier():
        return None
    if request.has_person_identity() and request.has_organization_hint():
        return None
    return (
        "Apollo email lookup requires either linkedin_url, email, or twitter_url, or a person identity "
        "plus domain/organization_name."
    )


def _lookup_label(request: ApolloEmailLookupRequest) -> str:
    if request.linkedin_url:
        candidate = urllib.parse.urlparse(request.linkedin_url).path.rsplit("/", 2)[-2:]
        label = "-".join(part for part in candidate if part)
        if label:
            return label
    if request.email:
        return request.email.split("@", 1)[0]
    if request.name:
        return request.name
    if request.first_name or request.last_name:
        return " ".join(part for part in (request.first_name, request.last_name) if part)
    if request.organization_name:
        return request.organization_name
    return "lookup"


def build_apollo_email_lookup_cache_info(request: ApolloEmailLookupRequest) -> ApolloEmailLookupCacheInfo:
    normalized_lookup = {
        "name": request.name,
        "domain": request.domain.lower(),
        "organization_name": request.organization_name,
        "first_name": request.first_name,
        "last_name": request.last_name,
        "linkedin_url": request.linkedin_url,
        "email": request.email.lower(),
        "twitter_url": request.twitter_url,
        "reveal_personal_emails": request.reveal_personal_emails,
    }
    serialized = json.dumps(normalized_lookup, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:16]
    safe_label = SAFE_CACHE_KEY_SEGMENT_PATTERN.sub("-", _lookup_label(request).lower()).strip("-.") or "lookup"
    cache_key = f"{safe_label}-{digest}"
    shared_cache_root = resolve_shared_apollo_email_cache_root()
    return ApolloEmailLookupCacheInfo(
        request=request,
        cache_key=cache_key,
        shared_cache_root=shared_cache_root,
        shared_cache_path=shared_cache_root / f"{cache_key}.json",
    )


def resolve_shared_apollo_email_cache_root() -> Path:
    return resolve_agent_filesystem_root().parent / "cache" / "apollo-email"


def describe_apollo_email_cache_file(path: Path, *, root: Path | None = None) -> dict[str, Any]:
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


def extract_apollo_email(payload: Mapping[str, Any]) -> str | None:
    person = payload.get("person")
    if not isinstance(person, Mapping):
        return None
    direct_email = _normalize_optional_text(person.get("email"))
    if direct_email:
        return direct_email
    contact = person.get("contact")
    if not isinstance(contact, Mapping):
        return None
    contact_email = _normalize_optional_text(contact.get("email"))
    return contact_email or None


def determine_apollo_lookup_status(payload: Mapping[str, Any]) -> str:
    if extract_apollo_email(payload):
        return "matched"
    person = payload.get("person")
    if isinstance(person, Mapping) and person:
        return "no_email"
    return "no_match"


def is_cacheable_apollo_response(payload: Mapping[str, Any]) -> bool:
    if payload.get("error") or payload.get("errors"):
        return False
    return determine_apollo_lookup_status(payload) in APOLLO_LOOKUP_STATUSES


def build_apollo_email_cache_entry(
    request: ApolloEmailLookupRequest,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    lookup_status = determine_apollo_lookup_status(payload)
    return {
        "schema_version": "1.0",
        "source": "apollo_people_match",
        "lookup": request.to_lookup_fields(),
        "lookup_status": lookup_status,
        "resolved_email": extract_apollo_email(payload),
        "payload": dict(payload),
    }


def read_cached_apollo_email_lookup(
    cache_info: ApolloEmailLookupCacheInfo,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
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
    cached_payload = payload.get("payload")
    lookup_status = str(payload.get("lookup_status", "") or "").strip()
    if not isinstance(cached_payload, dict) or lookup_status not in APOLLO_LOOKUP_STATUSES:
        return None, None
    return payload, describe_apollo_email_cache_file(
        cache_info.shared_cache_path,
        root=cache_info.shared_cache_root,
    )


def write_cached_apollo_email_lookup(
    cache_info: ApolloEmailLookupCacheInfo,
    cache_entry: Mapping[str, Any],
) -> dict[str, Any]:
    cache_info.shared_cache_root.mkdir(parents=True, exist_ok=True)
    cache_info.shared_cache_path.write_text(json.dumps(cache_entry, indent=2, sort_keys=True), encoding="utf-8")
    return describe_apollo_email_cache_file(cache_info.shared_cache_path, root=cache_info.shared_cache_root)


def workspace_cache_relative_path(template: str, *, cache_key: str) -> str:
    resolved = str(template or "cache/apollo-email/{cache_key}.json").format_map(_SafeFormatDict({"cache_key": cache_key}))
    return resolved.strip() or f"cache/apollo-email/{cache_key}.json"


def write_apollo_email_lookup_workspace_copy(
    run_id: str,
    agent_id: str | None,
    template: str,
    *,
    cache_key: str,
    cache_entry: Mapping[str, Any],
) -> tuple[str, dict[str, Any]]:
    relative_path = workspace_cache_relative_path(template, cache_key=cache_key)
    file_record = write_agent_workspace_text_file(
        run_id,
        agent_id,
        relative_path,
        json.dumps(cache_entry, indent=2, sort_keys=True),
        exists_behavior="overwrite",
        append_newline=False,
    )
    return relative_path, file_record


def fetch_apollo_person_match_live(
    *,
    request: ApolloEmailLookupRequest,
    api_key: str,
) -> dict[str, Any]:
    resolved_api_key = _normalize_optional_text(api_key)
    if not resolved_api_key:
        raise ApolloLookupError("apollo_api_key_missing", "Missing Apollo API key.")

    http_request = urllib.request.Request(
        f"{APOLLO_MATCH_URL}?{urllib.parse.urlencode(request.to_query_params())}",
        data=b"{}",
        method="POST",
        headers={
            "accept": "application/json",
            "Cache-Control": "no-cache",
            "Content-Type": "application/json",
            "x-api-key": resolved_api_key,
            "User-Agent": "graph-agent-apollo-email-lookup/1.0",
        },
    )

    try:
        with urllib.request.urlopen(http_request) as response:
            raw_payload = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise ApolloLookupError(
            "apollo_http_error",
            f"Apollo API returned HTTP {exc.code}.",
            details={"status_code": exc.code, "response_body": body},
        ) from exc
    except urllib.error.URLError as exc:
        raise ApolloLookupError(
            "apollo_network_error",
            "Could not reach Apollo API.",
            details={"reason": str(exc.reason)},
        ) from exc

    try:
        payload = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        raise ApolloLookupError("apollo_invalid_response", "Apollo API returned invalid JSON.") from exc
    if not isinstance(payload, dict):
        raise ApolloLookupError("apollo_invalid_response", "Apollo API returned a non-object JSON payload.")
    return payload
