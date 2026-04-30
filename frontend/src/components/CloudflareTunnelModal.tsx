import { useEffect, useState } from "react";
import type { MouseEvent } from "react";

import { clearCloudflareConfig, saveCloudflareConfig } from "../lib/cloudflare";
import type { CloudflareConfig } from "../lib/types";

type CloudflareTunnelModalProps = {
  initialConfig: CloudflareConfig | null;
  onConfigChange: (config: CloudflareConfig) => void;
  onClose: () => void;
};

const DEFAULT_TOKEN_ENV_VAR = "CLOUDFLARE_TUNNEL_TOKEN";

function emptyConfig(): CloudflareConfig {
  return {
    tunnel_token_env_var: DEFAULT_TOKEN_ENV_VAR,
    public_hostname: "",
    token_configured: false,
  };
}

export function CloudflareTunnelModal({ initialConfig, onConfigChange, onClose }: CloudflareTunnelModalProps) {
  const startingConfig = initialConfig ?? emptyConfig();
  const [tokenEnvVar, setTokenEnvVar] = useState(startingConfig.tunnel_token_env_var || DEFAULT_TOKEN_ENV_VAR);
  const [publicHostname, setPublicHostname] = useState(startingConfig.public_hostname);
  const [config, setConfig] = useState<CloudflareConfig>(startingConfig);
  const [error, setError] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isClearing, setIsClearing] = useState(false);

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
    setIsSubmitting(true);
    setError(null);
    try {
      const next = await saveCloudflareConfig({
        tunnel_token_env_var: tokenEnvVar.trim() || DEFAULT_TOKEN_ENV_VAR,
        public_hostname: publicHostname.trim(),
      });
      setConfig(next);
      setTokenEnvVar(next.tunnel_token_env_var || DEFAULT_TOKEN_ENV_VAR);
      setPublicHostname(next.public_hostname);
      onConfigChange(next);
    } catch (saveError) {
      setError(saveError instanceof Error ? saveError.message : "Unable to save Cloudflare configuration.");
    } finally {
      setIsSubmitting(false);
    }
  }

  async function handleClear() {
    setIsClearing(true);
    setError(null);
    try {
      const next = await clearCloudflareConfig();
      setConfig(next);
      setTokenEnvVar(next.tunnel_token_env_var || DEFAULT_TOKEN_ENV_VAR);
      setPublicHostname(next.public_hostname);
      onConfigChange(next);
    } catch (clearError) {
      setError(clearError instanceof Error ? clearError.message : "Unable to clear Cloudflare configuration.");
    } finally {
      setIsClearing(false);
    }
  }

  const hasSavedHostname = Boolean(config.public_hostname.trim());

  return (
    <div className="tool-details-modal-backdrop" onClick={handleOverlayClick} role="presentation">
      <section
        className="tool-details-modal supabase-auth-modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="cloudflare-tunnel-modal-title"
      >
        <div className="tool-details-modal-header">
          <div>
            <div className="tool-details-modal-eyebrow">Environment</div>
            <h3 id="cloudflare-tunnel-modal-title">Cloudflare Tunnel</h3>
            <p>
              Configure a Cloudflare tunnel so inbound-webhook listener start nodes can be reached from the public internet.
              The actual tunnel token stays in your <code>.env</code> file — only the env-var name is recorded here.
            </p>
          </div>
          <button type="button" className="secondary-button" onClick={onClose}>
            Close
          </button>
        </div>

        <div className="tool-details-modal-body supabase-auth-modal-body">
          <div className="supabase-auth-modal-grid">
            <label>
              Tunnel Token Env Var
              <input
                type="text"
                value={tokenEnvVar}
                placeholder={DEFAULT_TOKEN_ENV_VAR}
                onChange={(event) => setTokenEnvVar(event.target.value)}
              />
            </label>
            <label>
              Public Hostname
              <input
                type="text"
                value={publicHostname}
                placeholder="example.trycloudflare.com"
                onChange={(event) => setPublicHostname(event.target.value)}
              />
            </label>
          </div>

          <div className="tool-details-modal-help">
            Set the secret value of <code>{tokenEnvVar.trim() || DEFAULT_TOKEN_ENV_VAR}</code> in your <code>.env</code> (or
            deployment env). The status indicator below reports whether that env-var is currently populated in the running
            backend.
          </div>

          <div className="tool-details-modal-help supabase-auth-modal-status">
            <strong>{config.token_configured ? "Token detected." : "Token not configured."}</strong>
            <div>
              {config.token_configured
                ? `Backend sees ${config.tunnel_token_env_var || DEFAULT_TOKEN_ENV_VAR} populated.`
                : `Set ${config.tunnel_token_env_var || DEFAULT_TOKEN_ENV_VAR} in .env and restart the backend.`}
            </div>
            {hasSavedHostname ? (
              <div>
                Hostname: <code>{config.public_hostname}</code>
              </div>
            ) : null}
          </div>

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
            onClick={() => void handleSave()}
            disabled={isSubmitting || isClearing}
          >
            {isSubmitting ? "Saving..." : "Save Configuration"}
          </button>
          <button
            type="button"
            className="secondary-button"
            onClick={() => void handleClear()}
            disabled={isClearing || isSubmitting || (!hasSavedHostname && !config.tunnel_token_env_var)}
          >
            {isClearing ? "Clearing..." : "Clear"}
          </button>
        </div>
      </section>
    </div>
  );
}
