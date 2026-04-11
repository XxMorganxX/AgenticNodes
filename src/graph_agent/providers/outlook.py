from __future__ import annotations

from dataclasses import dataclass, field
import json
import re
from typing import Any, Sequence
from urllib import error as urllib_error, request as urllib_request


MICROSOFT_GRAPH_API_BASE_URL = "https://graph.microsoft.com/v1.0"
OUTLOOK_DRAFT_USER_AGENT = "graph-agent-outlook-draft/1.0"
RECIPIENT_SPLIT_PATTERN = re.compile(r"[\n,;]+")


def parse_outlook_recipient_addresses(value: Any, *, required: bool = True) -> list[str]:
    if isinstance(value, str):
        candidates = [candidate.strip() for candidate in RECIPIENT_SPLIT_PATTERN.split(value)]
    elif isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
        candidates = [str(candidate).strip() for candidate in value]
    else:
        candidates = [str(value).strip()] if value is not None else []
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
    def __init__(self, *, api_base_url: str = MICROSOFT_GRAPH_API_BASE_URL, timeout_seconds: float = 10.0) -> None:
        self._api_base_url = api_base_url.rstrip("/")
        self._timeout_seconds = max(timeout_seconds, 1.0)

    def create_draft(
        self,
        *,
        access_token: str,
        to_recipients: Sequence[str],
        subject: str = "",
        body: str = "",
    ) -> OutlookDraftResult:
        normalized_token = access_token.strip()
        normalized_recipients = parse_outlook_recipient_addresses(list(to_recipients), required=False)
        normalized_subject = subject.strip()
        normalized_body = body.strip()

        if not normalized_token:
            raise ValueError("Microsoft Graph access token is required to create an Outlook draft.")
        payload: dict[str, Any] = {}
        if normalized_subject:
            payload["subject"] = normalized_subject
        if normalized_body:
            payload["body"] = {
                "contentType": "Text",
                "content": normalized_body,
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

        try:
            with urllib_request.urlopen(request, timeout=self._timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8") or "{}")
        except urllib_error.HTTPError as exc:
            response_text = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                "Microsoft Graph draft creation failed with status "
                f"{exc.code}: {response_text}"
            ) from exc
        except urllib_error.URLError as exc:
            raise RuntimeError(f"Microsoft Graph draft creation failed: {exc.reason}") from exc

        payload_dict = payload if isinstance(payload, dict) else {}
        return OutlookDraftResult(
            draft_id=str(payload_dict.get("id", "")),
            subject=str(payload_dict.get("subject", normalized_subject)),
            body=normalized_body,
            to_recipients=normalized_recipients,
            web_link=str(payload_dict.get("webLink", "")),
            created_at=str(payload_dict.get("createdDateTime", "")),
            last_modified_at=str(payload_dict.get("lastModifiedDateTime", "")),
            raw_response=payload_dict,
        )
