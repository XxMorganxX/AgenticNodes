from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
import html as html_module
import json
import re
import socket
import time
from typing import Any, Sequence
from urllib import error as urllib_error, request as urllib_request


MICROSOFT_GRAPH_API_BASE_URL = "https://graph.microsoft.com/v1.0"
OUTLOOK_DRAFT_USER_AGENT = "graph-agent-outlook-draft/1.0"
RECIPIENT_SPLIT_PATTERN = re.compile(r"[\n,;]+")
HTML_TAG_PATTERN = re.compile(r"<[a-zA-Z][^>]*>")


def _looks_like_html(text: str) -> bool:
    return bool(HTML_TAG_PATTERN.search(text))


def _plain_text_to_html(text: str) -> str:
    if not text:
        return ""
    return html_module.escape(text, quote=False).replace("\n", "<br>")


def compose_outlook_draft_body(body: str, signature: str) -> tuple[str, str]:
    """Merge a body and signature into a single Microsoft Graph body payload.

    Returns ``(content_type, combined_content)`` where ``content_type`` is
    either ``"Text"`` or ``"HTML"``. HTML mode kicks in whenever either the
    body or signature contains HTML markup; the plain-text half is escaped
    and newline-wrapped so it renders correctly alongside the HTML half.
    """

    normalized_body = body.strip()
    normalized_signature = signature.strip()
    if not normalized_signature:
        return ("Text", normalized_body)
    body_is_html = _looks_like_html(normalized_body)
    signature_is_html = _looks_like_html(normalized_signature)
    if body_is_html or signature_is_html:
        body_html = normalized_body if body_is_html else _plain_text_to_html(normalized_body)
        signature_html = (
            normalized_signature if signature_is_html else _plain_text_to_html(normalized_signature)
        )
        separator = "<br><br>" if body_html and signature_html else ""
        return ("HTML", f"{body_html}{separator}{signature_html}")
    separator = "\n\n" if normalized_body and normalized_signature else ""
    return ("Text", f"{normalized_body}{separator}{normalized_signature}")


def _flatten_outlook_recipient_candidates(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [candidate.strip() for candidate in RECIPIENT_SPLIT_PATTERN.split(value)]
    if isinstance(value, Mapping):
        email_address = value.get("emailAddress")
        if email_address is not None:
            return _flatten_outlook_recipient_candidates(email_address)
        for key in ("address", "email", "recipient_email"):
            candidate = value.get(key)
            if candidate is not None:
                return _flatten_outlook_recipient_candidates(candidate)
        return []
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
        candidates: list[str] = []
        for candidate in value:
            candidates.extend(_flatten_outlook_recipient_candidates(candidate))
        return candidates
    return [str(value).strip()]


def parse_outlook_recipient_addresses(value: Any, *, required: bool = True) -> list[str]:
    candidates = _flatten_outlook_recipient_candidates(value)
    recipients = [candidate for candidate in candidates if candidate]
    if not recipients:
        if not required:
            return []
        raise ValueError("Outlook draft node requires at least one recipient email address.")
    invalid_recipients = [candidate for candidate in recipients if "@" not in candidate]
    if invalid_recipients:
        raise ValueError(
            "Outlook draft node received invalid recipient email addresses: "
            + ", ".join(invalid_recipients)
        )
    return recipients


@dataclass(frozen=True)
class OutlookDraftResult:
    draft_id: str
    subject: str
    body: str
    to_recipients: list[str]
    web_link: str
    created_at: str
    last_modified_at: str
    raw_response: dict[str, Any] = field(default_factory=dict)


class OutlookDraftClient:
    def __init__(
        self,
        *,
        api_base_url: str = MICROSOFT_GRAPH_API_BASE_URL,
        timeout_seconds: float = 15.0,
        max_retries: int = 2,
        retry_backoff_seconds: float = 1.0,
    ) -> None:
        self._api_base_url = api_base_url.rstrip("/")
        self._timeout_seconds = max(timeout_seconds, 1.0)
        self._max_retries = max(int(max_retries), 0)
        self._retry_backoff_seconds = max(float(retry_backoff_seconds), 0.0)

    def create_draft(
        self,
        *,
        access_token: str,
        to_recipients: Sequence[str],
        subject: str = "",
        body: str = "",
        signature: str = "",
    ) -> OutlookDraftResult:
        normalized_token = access_token.strip()
        normalized_recipients = parse_outlook_recipient_addresses(list(to_recipients), required=False)
        normalized_subject = subject.strip()
        content_type, combined_body = compose_outlook_draft_body(body, signature)

        if not normalized_token:
            raise ValueError("Microsoft Graph access token is required to create an Outlook draft.")
        payload: dict[str, Any] = {}
        if normalized_subject:
            payload["subject"] = normalized_subject
        if combined_body:
            payload["body"] = {
                "contentType": content_type,
                "content": combined_body,
            }
        if normalized_recipients:
            payload["toRecipients"] = [
                {
                    "emailAddress": {
                        "address": recipient,
                    }
                }
                for recipient in normalized_recipients
            ]
        request_body = json.dumps(payload).encode("utf-8")
        request = urllib_request.Request(
            url=f"{self._api_base_url}/me/messages",
            data=request_body,
            headers={
                "Authorization": f"Bearer {normalized_token}",
                "Content-Type": "application/json",
                "User-Agent": OUTLOOK_DRAFT_USER_AGENT,
            },
            method="POST",
        )

        attempt = 0
        while True:
            try:
                with urllib_request.urlopen(request, timeout=self._timeout_seconds) as response:
                    payload = json.loads(response.read().decode("utf-8") or "{}")
                break
            except urllib_error.HTTPError as exc:
                response_text = exc.read().decode("utf-8", errors="replace")
                raise RuntimeError(
                    "Microsoft Graph draft creation failed with status "
                    f"{exc.code}: {response_text}"
                ) from exc
            except (TimeoutError, socket.timeout) as exc:
                if attempt >= self._max_retries:
                    raise RuntimeError(
                        "Microsoft Graph draft creation timed out after "
                        f"{self._max_retries + 1} attempts ({self._timeout_seconds:.0f}s each): {exc}"
                    ) from exc
                time.sleep(self._retry_backoff_seconds * (2 ** attempt))
                attempt += 1
            except urllib_error.URLError as exc:
                if (
                    isinstance(exc.reason, (TimeoutError, socket.timeout))
                    or "timed out" in str(exc.reason).lower()
                ):
                    if attempt >= self._max_retries:
                        raise RuntimeError(
                            "Microsoft Graph draft creation timed out after "
                            f"{self._max_retries + 1} attempts ({self._timeout_seconds:.0f}s each): {exc.reason}"
                        ) from exc
                    time.sleep(self._retry_backoff_seconds * (2 ** attempt))
                    attempt += 1
                    continue
                raise RuntimeError(f"Microsoft Graph draft creation failed: {exc.reason}") from exc

        payload_dict = payload if isinstance(payload, dict) else {}
        return OutlookDraftResult(
            draft_id=str(payload_dict.get("id", "")),
            subject=str(payload_dict.get("subject", normalized_subject)),
            body=combined_body,
            to_recipients=normalized_recipients,
            web_link=str(payload_dict.get("webLink", "")),
            created_at=str(payload_dict.get("createdDateTime", "")),
            last_modified_at=str(payload_dict.get("lastModifiedDateTime", "")),
            raw_response=payload_dict,
        )
