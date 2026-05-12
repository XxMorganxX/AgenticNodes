from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
import html as html_module
import json
import re
import socket
import time
from typing import Any, Sequence
from urllib import error as urllib_error, parse as urllib_parse, request as urllib_request


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


def _merge_reply_body_content(
    *,
    new_content_type: str,
    new_content: str,
    auto_content_type: str,
    auto_content: str,
) -> dict[str, str] | None:
    """Merge new reply text with the auto-composed body returned by ``createReply``.

    Microsoft Graph's ``createReply`` populates the draft body with the quoted
    parent message and the threading markup Outlook uses for the in-message
    "switch between current and previous" affordance. PATCH-replacing that body
    breaks that affordance even when ``conversationId`` still matches. This
    helper prepends the caller's new content to the auto-composed body so the
    quoted parent is preserved end-to-end.
    """

    if not new_content.strip():
        return None
    if not auto_content.strip():
        return {
            "contentType": "HTML" if new_content_type.upper() == "HTML" else "Text",
            "content": new_content,
        }
    auto_is_html = auto_content_type.lower() == "html"
    new_is_html = new_content_type.upper() == "HTML"
    if auto_is_html or new_is_html:
        new_html = new_content if new_is_html else _plain_text_to_html(new_content)
        auto_html = auto_content if auto_is_html else _plain_text_to_html(auto_content)
        return {"contentType": "HTML", "content": f"{new_html}<br><br>{auto_html}"}
    return {"contentType": "Text", "content": f"{new_content}\n\n{auto_content}"}


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


def _graph_to_recipients_from_message(message: Mapping[str, Any]) -> list[str]:
    recipients: list[str] = []
    raw = message.get("toRecipients")
    if not isinstance(raw, list):
        return recipients
    for entry in raw:
        if not isinstance(entry, Mapping):
            continue
        addr = entry.get("emailAddress")
        if isinstance(addr, Mapping):
            address = str(addr.get("address", "") or "").strip()
            if address:
                recipients.append(address)
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


def _outlook_draft_result_from_message(
    *,
    payload_dict: dict[str, Any],
    normalized_subject: str,
    combined_body: str,
    normalized_recipients: list[str],
) -> OutlookDraftResult:
    body_obj = payload_dict.get("body")
    resolved_body = combined_body
    if isinstance(body_obj, Mapping):
        content = str(body_obj.get("content", "") or "")
        if content.strip():
            resolved_body = content
    subject = str(payload_dict.get("subject", normalized_subject) or normalized_subject)
    to_from_graph = _graph_to_recipients_from_message(payload_dict)
    to_recipients = normalized_recipients if normalized_recipients else to_from_graph
    return OutlookDraftResult(
        draft_id=str(payload_dict.get("id", "")),
        subject=subject,
        body=resolved_body,
        to_recipients=to_recipients,
        web_link=str(payload_dict.get("webLink", "")),
        created_at=str(payload_dict.get("createdDateTime", "")),
        last_modified_at=str(payload_dict.get("lastModifiedDateTime", "")),
        raw_response=payload_dict,
    )


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

    def _message_not_found(self, exc: RuntimeError) -> bool:
        text = str(exc)
        return "status 404" in text and "ErrorItemNotFound" in text

    def _graph_request_json(
        self,
        *,
        method: str,
        path: str,
        access_token: str,
        body: dict[str, Any] | None,
    ) -> dict[str, Any]:
        normalized_token = access_token.strip()
        if not normalized_token:
            raise ValueError("Microsoft Graph access token is required.")
        url = f"{self._api_base_url}{path}"
        data: bytes | None
        headers: dict[str, str] = {
            "Authorization": f"Bearer {normalized_token}",
            "User-Agent": OUTLOOK_DRAFT_USER_AGENT,
        }
        if body is None:
            data = None
        else:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib_request.Request(url, data=data, headers=headers, method=method.upper())

        attempt = 0
        while True:
            try:
                with urllib_request.urlopen(request, timeout=self._timeout_seconds) as response:
                    raw = response.read().decode("utf-8") or "{}"
                    payload = json.loads(raw)
                break
            except urllib_error.HTTPError as exc:
                response_text = exc.read().decode("utf-8", errors="replace")
                raise RuntimeError(
                    "Microsoft Graph request failed with status "
                    f"{exc.code}: {response_text}"
                ) from exc
            except (TimeoutError, socket.timeout) as exc:
                if attempt >= self._max_retries:
                    raise RuntimeError(
                        "Microsoft Graph request timed out after "
                        f"{self._max_retries + 1} attempts ({self._timeout_seconds:.0f}s each): {exc}"
                    ) from exc
                time.sleep(self._retry_backoff_seconds * (2**attempt))
                attempt += 1
            except urllib_error.URLError as exc:
                if (
                    isinstance(exc.reason, (TimeoutError, socket.timeout))
                    or "timed out" in str(exc.reason).lower()
                ):
                    if attempt >= self._max_retries:
                        raise RuntimeError(
                            "Microsoft Graph request timed out after "
                            f"{self._max_retries + 1} attempts ({self._timeout_seconds:.0f}s each): {exc.reason}"
                        ) from exc
                    time.sleep(self._retry_backoff_seconds * (2**attempt))
                    attempt += 1
                    continue
                raise RuntimeError(f"Microsoft Graph request failed: {exc.reason}") from exc

        return payload if isinstance(payload, dict) else {}

    def _resolve_message_id_by_internet_message_id(
        self,
        *,
        access_token: str,
        internet_message_id: str,
    ) -> str:
        normalized_id = str(internet_message_id or "").strip()
        if not normalized_id:
            return ""
        escaped_id = normalized_id.replace("'", "''")
        query = urllib_parse.urlencode(
            {
                "$select": "id,internetMessageId",
                "$top": "1",
                "$filter": f"internetMessageId eq '{escaped_id}'",
            }
        )
        payload = self._graph_request_json(
            method="GET",
            path=f"/me/messages?{query}",
            access_token=access_token,
            body=None,
        )
        rows = payload.get("value")
        if not isinstance(rows, list) or not rows:
            return ""
        first = rows[0]
        if not isinstance(first, Mapping):
            return ""
        return str(first.get("id", "") or "").strip()

    def create_draft(
        self,
        *,
        access_token: str,
        to_recipients: Sequence[str],
        subject: str = "",
        body: str = "",
        signature: str = "",
    ) -> OutlookDraftResult:
        normalized_recipients = parse_outlook_recipient_addresses(list(to_recipients), required=False)
        normalized_subject = subject.strip()
        content_type, combined_body = compose_outlook_draft_body(body, signature)

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

        payload_dict = self._graph_request_json(
            method="POST",
            path="/me/messages",
            access_token=access_token,
            body=payload if payload else {},
        )
        return _outlook_draft_result_from_message(
            payload_dict=payload_dict,
            normalized_subject=normalized_subject,
            combined_body=combined_body,
            normalized_recipients=normalized_recipients,
        )

    def create_reply_draft(
        self,
        *,
        access_token: str,
        reply_to_message_id: str,
        reply_to_internet_message_id: str = "",
        reply_mode: str = "reply",
        to_recipients: Sequence[str],
        subject: str = "",
        body: str = "",
        signature: str = "",
    ) -> OutlookDraftResult:
        parent_id = str(reply_to_message_id or "").strip()
        if not parent_id:
            raise ValueError("reply_to_message_id is required to create a reply draft.")
        mode = str(reply_mode or "reply").strip().lower() or "reply"
        if mode not in {"reply", "reply_all"}:
            raise ValueError(f"Unsupported reply_mode '{reply_mode}'. Use 'reply' or 'reply_all'.")

        normalized_recipients = parse_outlook_recipient_addresses(list(to_recipients), required=False)
        normalized_subject = subject.strip()
        content_type, combined_body = compose_outlook_draft_body(body, signature)

        action = "createReplyAll" if mode == "reply_all" else "createReply"
        effective_parent_id = parent_id
        encoded_parent = urllib_parse.quote(effective_parent_id, safe="")
        path = f"/me/messages/{encoded_parent}/{action}"
        try:
            payload_dict = self._graph_request_json(
                method="POST",
                path=path,
                access_token=access_token,
                body=None,
            )
        except RuntimeError as exc:
            if not self._message_not_found(exc):
                raise
            resolved_parent_id = self._resolve_message_id_by_internet_message_id(
                access_token=access_token,
                internet_message_id=reply_to_internet_message_id,
            )
            if not resolved_parent_id or resolved_parent_id == parent_id:
                raise RuntimeError(
                    "Microsoft Graph could not find the reply target message id. "
                    "The stored provider_message_id may be stale after the draft was sent or moved, "
                    "and no current message could be resolved from internet_message_id."
                ) from exc
            effective_parent_id = resolved_parent_id
            encoded_parent = urllib_parse.quote(effective_parent_id, safe="")
            payload_dict = self._graph_request_json(
                method="POST",
                path=f"/me/messages/{encoded_parent}/{action}",
                access_token=access_token,
                body=None,
            )

        auto_body_obj = payload_dict.get("body") if isinstance(payload_dict, Mapping) else None
        auto_content_type = ""
        auto_content = ""
        if isinstance(auto_body_obj, Mapping):
            auto_content_type = str(auto_body_obj.get("contentType", "") or "")
            auto_content = str(auto_body_obj.get("content", "") or "")
        merged_body = _merge_reply_body_content(
            new_content_type=content_type,
            new_content=combined_body,
            auto_content_type=auto_content_type,
            auto_content=auto_content,
        )

        patch: dict[str, Any] = {}
        if normalized_subject:
            patch["subject"] = normalized_subject
        if merged_body is not None:
            patch["body"] = merged_body
        if normalized_recipients:
            patch["toRecipients"] = [
                {"emailAddress": {"address": recipient}}
                for recipient in normalized_recipients
            ]

        if patch:
            draft_id = str(payload_dict.get("id", "") or "").strip()
            if not draft_id:
                raise RuntimeError("Microsoft Graph createReply response did not include a message id.")
            encoded_draft = urllib_parse.quote(draft_id, safe="")
            payload_dict = self._graph_request_json(
                method="PATCH",
                path=f"/me/messages/{encoded_draft}",
                access_token=access_token,
                body=patch,
            )

        return _outlook_draft_result_from_message(
            payload_dict=payload_dict,
            normalized_subject=normalized_subject,
            combined_body=combined_body,
            normalized_recipients=normalized_recipients,
        )
