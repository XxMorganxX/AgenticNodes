from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
from threading import Lock, Thread
from typing import Any, Callable, Sequence
from uuid import uuid4


MICROSOFT_GRAPH_DEVICE_CODE_SCOPES = ("Mail.ReadWrite",)
MICROSOFT_AUTH_STATE_DIR = Path.home() / ".graph-agent" / "microsoft-auth"


def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


@dataclass(frozen=True)
class MicrosoftAuthStatus:
    status: str
    connected: bool
    pending: bool
    client_id: str = ""
    tenant_id: str = ""
    account_username: str = ""
    request_id: str = ""
    user_code: str = ""
    verification_uri: str = ""
    verification_uri_complete: str = ""
    message: str = ""
    expires_at: str = ""
    connected_at: str = ""
    last_error: str = ""
    scopes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "connected": self.connected,
            "pending": self.pending,
            "client_id": self.client_id,
            "tenant_id": self.tenant_id,
            "account_username": self.account_username,
            "request_id": self.request_id,
            "user_code": self.user_code,
            "verification_uri": self.verification_uri,
            "verification_uri_complete": self.verification_uri_complete,
            "message": self.message,
            "expires_at": self.expires_at,
            "connected_at": self.connected_at,
            "last_error": self.last_error,
            "scopes": list(self.scopes),
        }


class MicrosoftAuthService:
    def __init__(
        self,
        *,
        state_dir: Path | None = None,
        application_factory: Callable[[str, str, Any], Any] | None = None,
        token_cache_factory: Callable[[Path], Any] | None = None,
    ) -> None:
        self._state_dir = state_dir or MICROSOFT_AUTH_STATE_DIR
        self._settings_path = self._state_dir / "settings.json"
        self._cache_path = self._state_dir / "token-cache.bin"
        self._application_factory = application_factory or self._default_application_factory
        self._token_cache_factory = token_cache_factory or self._default_token_cache_factory
        self._lock = Lock()
        self._pending_flow: dict[str, Any] | None = None

    def start_device_code(
        self,
        *,
        client_id: str,
        tenant_id: str,
        scopes: Sequence[str] | None = None,
    ) -> MicrosoftAuthStatus:
        normalized_client_id = client_id.strip()
        normalized_tenant_id = tenant_id.strip()
        normalized_scopes = self._normalize_scopes(scopes)
        if not normalized_client_id:
            raise ValueError("Microsoft client id is required.")
        if not normalized_tenant_id:
            raise ValueError("Microsoft tenant id is required.")
        with self._lock:
            if self._pending_flow is not None:
                raise ValueError("A Microsoft device sign-in is already in progress.")
        self._write_settings(
            {
                "client_id": normalized_client_id,
                "tenant_id": normalized_tenant_id,
                "scopes": normalized_scopes,
            }
        )
        application = self._build_application(normalized_client_id, normalized_tenant_id)
        flow = dict(application.initiate_device_flow(scopes=normalized_scopes))
        if "user_code" not in flow:
            detail = str(flow.get("error_description") or flow.get("error") or "Unable to start Microsoft device sign-in.")
            raise ValueError(detail)

        pending_flow = {
            "request_id": str(uuid4()),
            "client_id": normalized_client_id,
            "tenant_id": normalized_tenant_id,
            "scopes": normalized_scopes,
            "flow": flow,
            "last_error": "",
            "thread": None,
        }
        thread = Thread(
            target=self._complete_device_code_flow,
            args=(pending_flow["request_id"], application, flow, normalized_client_id, normalized_tenant_id, normalized_scopes),
            daemon=True,
            name="microsoft-device-code-flow",
        )
        pending_flow["thread"] = thread
        with self._lock:
            self._pending_flow = pending_flow
        thread.start()
        return self.connection_status()

    def connection_status(self) -> MicrosoftAuthStatus:
        with self._lock:
            pending_flow = dict(self._pending_flow) if self._pending_flow is not None else None
        if pending_flow is not None:
            flow = dict(pending_flow.get("flow", {}))
            return MicrosoftAuthStatus(
                status="pending",
                connected=False,
                pending=True,
                client_id=str(pending_flow.get("client_id", "")),
                tenant_id=str(pending_flow.get("tenant_id", "")),
                request_id=str(pending_flow.get("request_id", "")),
                user_code=str(flow.get("user_code", "")),
                verification_uri=str(flow.get("verification_uri", "")),
                verification_uri_complete=str(flow.get("verification_uri_complete", "")),
                message=str(flow.get("message", "")),
                expires_at=str(flow.get("expires_at", "")),
                last_error=str(pending_flow.get("last_error", "")),
                scopes=list(pending_flow.get("scopes", [])),
            )

        settings = self._read_settings()
        client_id = str(settings.get("client_id", "") or "").strip()
        tenant_id = str(settings.get("tenant_id", "") or "").strip()
        scopes = self._normalize_scopes(settings.get("scopes"))
        if not client_id or not tenant_id:
            return MicrosoftAuthStatus(status="disconnected", connected=False, pending=False)

        try:
            application = self._build_application(client_id, tenant_id)
            accounts = list(application.get_accounts())
        except Exception as exc:  # noqa: BLE001
            return MicrosoftAuthStatus(
                status="error",
                connected=False,
                pending=False,
                client_id=client_id,
                tenant_id=tenant_id,
                last_error=str(exc),
                scopes=scopes,
            )

        if not accounts:
            return MicrosoftAuthStatus(
                status="disconnected",
                connected=False,
                pending=False,
                client_id=client_id,
                tenant_id=tenant_id,
                last_error=str(settings.get("last_error", "") or ""),
                scopes=scopes,
            )

        account = self._select_account(accounts, settings)
        return MicrosoftAuthStatus(
            status="connected",
            connected=True,
            pending=False,
            client_id=client_id,
            tenant_id=tenant_id,
            account_username=str(settings.get("account_username") or account.get("username") or ""),
            connected_at=str(settings.get("connected_at", "") or ""),
            scopes=scopes,
        )

    def acquire_access_token(self, *, scopes: Sequence[str] | None = None) -> str:
        settings = self._read_settings()
        client_id = str(settings.get("client_id", "") or "").strip()
        tenant_id = str(settings.get("tenant_id", "") or "").strip()
        if not client_id or not tenant_id:
            raise RuntimeError("No Microsoft account is connected. Open the Environment panel and connect Microsoft first.")

        normalized_scopes = self._normalize_scopes(scopes or settings.get("scopes"))
        application = self._build_application(client_id, tenant_id)
        accounts = list(application.get_accounts())
        if not accounts:
            raise RuntimeError("The Microsoft token cache is empty. Reconnect the Microsoft account.")
        account = self._select_account(accounts, settings)
        result = application.acquire_token_silent(normalized_scopes, account=account)
        if isinstance(result, dict) and str(result.get("access_token", "")).strip():
            return str(result["access_token"])

        result_with_error = None
        acquire_with_error = getattr(application, "acquire_token_silent_with_error", None)
        if callable(acquire_with_error):
            result_with_error = acquire_with_error(normalized_scopes, account=account)
        detail = ""
        if isinstance(result_with_error, dict):
            detail = str(result_with_error.get("error_description") or result_with_error.get("error") or "").strip()
        raise RuntimeError(
            detail
            or "The cached Microsoft token could not be refreshed silently. Disconnect and reconnect the Microsoft account."
        )

    def disconnect(self) -> MicrosoftAuthStatus:
        with self._lock:
            pending_flow = self._pending_flow
            self._pending_flow = None
        if pending_flow is not None:
            flow = pending_flow.get("flow")
            if isinstance(flow, dict):
                flow["expires_at"] = 0
        self._clear_settings()
        try:
            self._cache_path.unlink(missing_ok=True)
        except OSError:
            pass
        return MicrosoftAuthStatus(status="disconnected", connected=False, pending=False)

    def _complete_device_code_flow(
        self,
        request_id: str,
        application: Any,
        flow: dict[str, Any],
        client_id: str,
        tenant_id: str,
        scopes: list[str],
    ) -> None:
        try:
            result = application.acquire_token_by_device_flow(flow)
        except Exception as exc:  # noqa: BLE001
            self._finish_pending_flow(request_id, error=str(exc))
            return

        if not isinstance(result, dict) or not str(result.get("access_token", "")).strip():
            detail = str(result.get("error_description") or result.get("error") or "Microsoft device sign-in failed.")
            self._finish_pending_flow(request_id, error=detail)
            return

        accounts = list(application.get_accounts())
        account = self._select_account(accounts, {"account_username": self._username_from_result(result)})
        self._write_settings(
            {
                "client_id": client_id,
                "tenant_id": tenant_id,
                "scopes": scopes,
                "account_username": self._username_from_account(account) or self._username_from_result(result),
                "connected_at": _utc_now_iso(),
                "last_error": "",
            }
        )
        self._finish_pending_flow(request_id)

    def _finish_pending_flow(self, request_id: str, *, error: str = "") -> None:
        with self._lock:
            if self._pending_flow is None or self._pending_flow.get("request_id") != request_id:
                return
            if error:
                settings = self._read_settings()
                settings["last_error"] = error
                self._write_settings(settings)
            self._pending_flow = None

    def _build_application(self, client_id: str, tenant_id: str) -> Any:
        token_cache = self._token_cache_factory(self._cache_path)
        return self._application_factory(client_id, self._authority(tenant_id), token_cache)

    def _default_application_factory(self, client_id: str, authority: str, token_cache: Any) -> Any:
        try:
            import msal
        except ImportError as exc:  # pragma: no cover - exercised only when dependency is missing
            raise RuntimeError("Microsoft auth requires the `msal` package. Install project dependencies and restart.") from exc
        return msal.PublicClientApplication(client_id, authority=authority, token_cache=token_cache)

    def _default_token_cache_factory(self, cache_path: Path) -> Any:
        try:
            from msal_extensions import PersistedTokenCache, build_encrypted_persistence
        except ImportError as exc:  # pragma: no cover - exercised only when dependency is missing
            raise RuntimeError(
                "Microsoft auth requires the `msal-extensions` package for secure token storage. "
                "Install project dependencies and restart."
            ) from exc
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            persistence = build_encrypted_persistence(str(cache_path))
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(
                "Secure OS-backed token storage is unavailable on this machine. "
                f"Microsoft auth cannot continue safely: {exc}"
            ) from exc
        return PersistedTokenCache(persistence)

    def _authority(self, tenant_id: str) -> str:
        return f"https://login.microsoftonline.com/{tenant_id}"

    def _normalize_scopes(self, scopes: Sequence[str] | None) -> list[str]:
        if scopes is None:
            return list(MICROSOFT_GRAPH_DEVICE_CODE_SCOPES)
        normalized: list[str] = []
        for scope in scopes:
            candidate = str(scope).strip()
            if candidate and candidate not in normalized:
                normalized.append(candidate)
        return normalized or list(MICROSOFT_GRAPH_DEVICE_CODE_SCOPES)

    def _read_settings(self) -> dict[str, Any]:
        try:
            payload = json.loads(self._settings_path.read_text())
        except (FileNotFoundError, OSError, json.JSONDecodeError):
            return {}
        return dict(payload) if isinstance(payload, dict) else {}

    def _write_settings(self, payload: dict[str, Any]) -> None:
        self._settings_path.parent.mkdir(parents=True, exist_ok=True)
        self._settings_path.write_text(json.dumps(payload, indent=2))

    def _clear_settings(self) -> None:
        try:
            self._settings_path.unlink(missing_ok=True)
        except OSError:
            pass

    def _select_account(self, accounts: Sequence[dict[str, Any]], settings: dict[str, Any]) -> dict[str, Any]:
        preferred_username = str(settings.get("account_username", "") or "").strip().lower()
        for account in accounts:
            username = self._username_from_account(account).lower()
            if preferred_username and username == preferred_username:
                return dict(account)
        return dict(accounts[0]) if accounts else {}

    def _username_from_account(self, account: dict[str, Any]) -> str:
        return str(account.get("username") or account.get("preferred_username") or "").strip()

    def _username_from_result(self, result: dict[str, Any]) -> str:
        claims = result.get("id_token_claims")
        if isinstance(claims, dict):
            for key in ("preferred_username", "email", "upn"):
                candidate = str(claims.get(key, "") or "").strip()
                if candidate:
                    return candidate
        return ""
