import { useEffect, useState } from "react";
import type { MouseEvent } from "react";

import { disconnectMicrosoftAuth, fetchMicrosoftAuthStatus, startMicrosoftDeviceCode } from "../lib/api";
import type { MicrosoftAuthStatus } from "../lib/types";

type MicrosoftAuthModalProps = {
  initialStatus: MicrosoftAuthStatus | null;
  onStatusChange: (status: MicrosoftAuthStatus) => void;
  onClose: () => void;
};

const MICROSOFT_GRAPH_SCOPES = ["Mail.ReadWrite"];

function disconnectedStatus(): MicrosoftAuthStatus {
  return {
    status: "disconnected",
    connected: false,
    pending: false,
    client_id: "",
    tenant_id: "",
    account_username: "",
    request_id: "",
    user_code: "",
    verification_uri: "",
    verification_uri_complete: "",
    message: "",
    expires_at: "",
    connected_at: "",
    last_error: "",
    scopes: [],
  };
}

export function MicrosoftAuthModal({ initialStatus, onStatusChange, onClose }: MicrosoftAuthModalProps) {
  const [clientId, setClientId] = useState(initialStatus?.client_id ?? "");
  const [tenantId, setTenantId] = useState(initialStatus?.tenant_id ?? "");
  const [status, setStatus] = useState<MicrosoftAuthStatus>(initialStatus ?? disconnectedStatus());
  const [error, setError] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isDisconnecting, setIsDisconnecting] = useState(false);

  useEffect(() => {
    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        onClose();
      }
    }
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [onClose]);

  useEffect(() => {
    if (!status.pending) {
      return;
    }
    const timeoutId = window.setTimeout(() => {
      fetchMicrosoftAuthStatus()
        .then((nextStatus) => {
          setStatus(nextStatus);
          onStatusChange(nextStatus);
          if (nextStatus.client_id) {
            setClientId(nextStatus.client_id);
          }
          if (nextStatus.tenant_id) {
            setTenantId(nextStatus.tenant_id);
          }
        })
        .catch((pollError: Error) => {
          setError(pollError.message);
        });
    }, 2000);
    return () => window.clearTimeout(timeoutId);
  }, [onStatusChange, status.pending]);

  function handleOverlayClick(event: MouseEvent<HTMLDivElement>) {
    if (event.target === event.currentTarget) {
      onClose();
    }
  }

  async function handleStart() {
    setIsSubmitting(true);
    setError(null);
    try {
      const nextStatus = await startMicrosoftDeviceCode({
        client_id: clientId,
        tenant_id: tenantId,
        scopes: MICROSOFT_GRAPH_SCOPES,
      });
      setStatus(nextStatus);
      onStatusChange(nextStatus);
    } catch (startError) {
      setError(startError instanceof Error ? startError.message : "Unable to start Microsoft device sign-in.");
    } finally {
      setIsSubmitting(false);
    }
  }

  async function handleDisconnect() {
    setIsDisconnecting(true);
    setError(null);
    try {
      const nextStatus = await disconnectMicrosoftAuth();
      setStatus(nextStatus);
      onStatusChange(nextStatus);
    } catch (disconnectError) {
      setError(disconnectError instanceof Error ? disconnectError.message : "Unable to disconnect Microsoft account.");
    } finally {
      setIsDisconnecting(false);
    }
  }

  return (
    <div className="tool-details-modal-backdrop" onClick={handleOverlayClick} role="presentation">
      <section className="tool-details-modal supabase-auth-modal" role="dialog" aria-modal="true" aria-labelledby="microsoft-auth-modal-title">
        <div className="tool-details-modal-header">
          <div>
            <div className="tool-details-modal-eyebrow">Environment</div>
            <h3 id="microsoft-auth-modal-title">Microsoft Authentication</h3>
            <p>Use Microsoft Entra device-code sign-in to securely cache Graph tokens in OS-backed storage for Outlook draft nodes.</p>
          </div>
          <button type="button" className="secondary-button" onClick={onClose}>
            Close
          </button>
        </div>

        <div className="tool-details-modal-body supabase-auth-modal-body">
          <div className="supabase-auth-modal-grid">
            <label>
              Application (Client) ID
              <input
                type="text"
                value={clientId}
                placeholder="00000000-0000-0000-0000-000000000000"
                onChange={(event) => setClientId(event.target.value)}
              />
            </label>
            <label>
              Directory (Tenant) ID
              <input
                type="text"
                value={tenantId}
                placeholder="common or your tenant GUID"
                onChange={(event) => setTenantId(event.target.value)}
              />
            </label>
          </div>

          <div className="tool-details-modal-help">
            The app requests the delegated Microsoft Graph scope <code>Mail.ReadWrite</code>. Tokens are stored in secure OS-backed local storage, not in graph env vars or browser localStorage.
          </div>

          {status.pending ? (
            <div className="tool-details-modal-help supabase-auth-modal-status">
              <strong>Device code ready.</strong>
              <div>{status.message || "Open the verification URL, enter the code, and finish sign-in."}</div>
              {status.verification_uri ? <div>URL: <code>{status.verification_uri}</code></div> : null}
              {status.user_code ? <div>Code: <code>{status.user_code}</code></div> : null}
            </div>
          ) : null}

          {status.connected ? (
            <div className="tool-details-modal-help supabase-auth-modal-status">
              <strong>Connected.</strong>
              <div>{status.account_username ? `Signed in as ${status.account_username}.` : "Microsoft account connected."}</div>
              <div>Tenant: <code>{status.tenant_id}</code></div>
              <div>Client: <code>{status.client_id}</code></div>
            </div>
          ) : null}

          {status.last_error ? (
            <div className="tool-details-modal-help supabase-auth-modal-status supabase-auth-modal-status--error">
              {status.last_error}
            </div>
          ) : null}

          {error ? (
            <div className="tool-details-modal-help supabase-auth-modal-status supabase-auth-modal-status--error">
              {error}
            </div>
          ) : null}
        </div>

        <div className="tool-details-modal-footer">
          <button
            type="button"
            className="primary-button"
            onClick={() => void handleStart()}
            disabled={isSubmitting || isDisconnecting || status.pending || !clientId.trim() || !tenantId.trim()}
          >
            {isSubmitting ? "Starting..." : status.connected ? "Reconnect with Device Code" : "Start Device Sign-In"}
          </button>
          <button
            type="button"
            className="secondary-button"
            onClick={() => void handleDisconnect()}
            disabled={isDisconnecting || isSubmitting || (!status.connected && !status.pending)}
          >
            {isDisconnecting ? "Disconnecting..." : "Disconnect"}
          </button>
        </div>
      </section>
    </div>
  );
}
