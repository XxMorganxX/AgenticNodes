import { useEffect, useState } from "react";
import type { MouseEvent } from "react";

import { verifySupabaseAuth } from "../lib/api";
import type { SupabaseAuthVerificationResult } from "../lib/types";

type SupabaseAuthValues = {
  supabaseUrl: string;
  supabaseKey: string;
  projectRef: string;
  accessToken: string;
};

type SupabaseAuthModalProps = {
  initialValues: SupabaseAuthValues;
  onSave: (values: SupabaseAuthValues, verification?: SupabaseAuthVerificationResult | null) => void;
  onClose: () => void;
};

function isSecretVisible(visible: boolean): "text" | "password" {
  return visible ? "text" : "password";
}

function deriveSupabaseProjectRef(rawUrl: string): string {
  const trimmedUrl = rawUrl.trim();
  if (!trimmedUrl) {
    return "";
  }
  try {
    const parsed = new URL(trimmedUrl);
    const match = parsed.hostname.match(/^([a-z0-9-]+)\.supabase\.co$/i);
    return match?.[1] ?? "";
  } catch {
    return "";
  }
}

function SupabaseFieldLabel({
  label,
  tooltipId,
  tooltipText,
}: {
  label: string;
  tooltipId: string;
  tooltipText: string;
}) {
  return (
    <span className="supabase-auth-modal-label-with-hint">
      <span>{label}</span>
      <span className="supabase-auth-modal-hint">
        <button type="button" className="supabase-auth-modal-hint-button" aria-label={`${label} help`} aria-describedby={tooltipId}>
          ?
        </button>
        <span id={tooltipId} role="tooltip" className="supabase-auth-modal-tooltip">
          {tooltipText}
        </span>
      </span>
    </span>
  );
}

export function SupabaseAuthModal({ initialValues, onSave, onClose }: SupabaseAuthModalProps) {
  const [supabaseUrl, setSupabaseUrl] = useState(initialValues.supabaseUrl);
  const [supabaseKey, setSupabaseKey] = useState(initialValues.supabaseKey);
  const [accessToken, setAccessToken] = useState(initialValues.accessToken);
  const [showSupabaseKey, setShowSupabaseKey] = useState(false);
  const [showAccessToken, setShowAccessToken] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [verification, setVerification] = useState<SupabaseAuthVerificationResult | null>(null);
  const derivedProjectRef = deriveSupabaseProjectRef(supabaseUrl);

  useEffect(() => {
    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        onClose();
      }
    }

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [onClose]);

  function handleOverlayClick(event: MouseEvent<HTMLDivElement>) {
    if (event.target === event.currentTarget) {
      onClose();
    }
  }

  async function handleSave() {
    setIsSaving(true);
    setError(null);
    const nextValues = {
      supabaseUrl,
      supabaseKey,
      projectRef: derivedProjectRef,
      accessToken,
    };
    onSave(nextValues, null);
    try {
      const result = await verifySupabaseAuth({
        supabase_url: supabaseUrl,
        supabase_key: supabaseKey,
        schema: "public",
        project_ref: derivedProjectRef,
        access_token: accessToken,
      });
      setVerification(result);
      onSave(nextValues, result);
      onClose();
    } catch (verificationError) {
      setVerification(null);
      setError(
        verificationError instanceof Error
          ? `Saved locally, but verification failed: ${verificationError.message}`
          : "Saved locally, but verification failed.",
      );
    } finally {
      setIsSaving(false);
    }
  }

  return (
    <div className="tool-details-modal-backdrop" onClick={handleOverlayClick} role="presentation">
      <section className="tool-details-modal supabase-auth-modal" role="dialog" aria-modal="true" aria-labelledby="supabase-auth-modal-title">
        <div className="tool-details-modal-header">
          <div>
            <div className="tool-details-modal-eyebrow">Environment</div>
            <h3 id="supabase-auth-modal-title">Supabase Authentication</h3>
            <p>Store the Supabase credentials for this graph, verify them immediately, and unlock schema-aware Supabase nodes.</p>
          </div>
          <button type="button" className="secondary-button" onClick={onClose}>
            Close
          </button>
        </div>

        <div className="tool-details-modal-body supabase-auth-modal-body">
          <div className="supabase-auth-modal-grid">
            <label>
              <SupabaseFieldLabel
                label="Supabase URL"
                tooltipId="supabase-url-tooltip"
                tooltipText="Supabase dashboard -> Settings -> API -> Project URL."
              />
              <input
                type="text"
                value={supabaseUrl}
                placeholder="https://your-project-ref.supabase.co"
                onChange={(event) => setSupabaseUrl(event.target.value)}
              />
            </label>
            <label>
              <SupabaseFieldLabel
                label="Service Role Key"
                tooltipId="supabase-service-role-tooltip"
                tooltipText="Supabase dashboard -> Settings -> API -> service_role secret key."
              />
              <div className="env-tile-value-row">
                <input
                  type={isSecretVisible(showSupabaseKey)}
                  value={supabaseKey}
                  placeholder="service_role key"
                  onChange={(event) => setSupabaseKey(event.target.value)}
                />
                <button
                  type="button"
                  className="secondary-button env-tile-visibility-toggle"
                  onClick={() => setShowSupabaseKey((current) => !current)}
                >
                  {showSupabaseKey ? "Hide" : "Show"}
                </button>
              </div>
            </label>
            <label>
              <SupabaseFieldLabel
                label="Project Ref"
                tooltipId="supabase-project-ref-tooltip"
                tooltipText="Parsed automatically from the Supabase URL."
              />
              <input type="text" value={derivedProjectRef} placeholder="Parsed from Supabase URL" readOnly />
            </label>
            <label>
              <SupabaseFieldLabel
                label="Access Token"
                tooltipId="supabase-access-token-tooltip"
                tooltipText="Supabase dashboard -> Account menu -> Access Tokens -> Generate new token."
              />
              <div className="env-tile-value-row">
                <input
                  type={isSecretVisible(showAccessToken)}
                  value={accessToken}
                  placeholder="Optional for hosted MCP"
                  onChange={(event) => setAccessToken(event.target.value)}
                />
                <button
                  type="button"
                  className="secondary-button env-tile-visibility-toggle"
                  onClick={() => setShowAccessToken((current) => !current)}
                >
                  {showAccessToken ? "Hide" : "Show"}
                </button>
              </div>
            </label>
          </div>

          <div className="tool-details-modal-help">
            Saving verifies the Supabase URL and service key. The project ref is parsed from the URL automatically. If an access token is also provided, the modal verifies hosted Supabase MCP access too.
          </div>

          {accessToken.trim() && !derivedProjectRef ? (
            <div className="tool-details-modal-help supabase-auth-modal-status supabase-auth-modal-status--error">
              We could not parse a Supabase project ref from that URL, so hosted MCP authentication cannot be verified yet.
            </div>
          ) : null}

          {verification ? (
            <div className="tool-details-modal-help supabase-auth-modal-status">
              <strong>Verification passed.</strong>
              <div>{verification.source_count} sources discovered from schema <code>{verification.schema}</code>.</div>
              {verification.mcp_auth_checked ? <div>Hosted MCP authentication also succeeded.</div> : null}
              {verification.warnings.map((warning) => (
                <div key={warning}>{warning}</div>
              ))}
            </div>
          ) : null}

          {error ? <div className="tool-details-modal-help supabase-auth-modal-status supabase-auth-modal-status--error">{error}</div> : null}

          <div className="preferences-modal-actions">
            <button type="button" className="secondary-button" onClick={onClose} disabled={isSaving}>
              Cancel
            </button>
            <button
              type="button"
              className="primary-button"
              onClick={() => void handleSave()}
              disabled={isSaving || !supabaseUrl.trim() || !supabaseKey.trim()}
            >
              {isSaving ? "Verifying..." : "Save and Verify"}
            </button>
          </div>
        </div>
      </section>
    </div>
  );
}
