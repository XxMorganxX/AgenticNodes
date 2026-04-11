import { useEffect, useMemo, useState } from "react";

import { fetchMicrosoftAuthStatus, verifySupabaseAuth } from "../lib/api";
import { getGraphEnvVars, NON_PERSISTED_GRAPH_ENV_KEYS, STANDARD_GRAPH_ENV_FIELDS, sanitizeGraphEnvVars } from "../lib/graphEnv";
import { saveSessionSupabaseSchema } from "../lib/sessionSupabaseSchema";
import type { GraphDocument, MicrosoftAuthStatus, SupabaseAuthVerificationResult } from "../lib/types";
import { MicrosoftAuthModal } from "./MicrosoftAuthModal";
import { SupabaseAuthModal } from "./SupabaseAuthModal";

type GraphEnvEditorProps = {
  graph: GraphDocument | null;
  onGraphChange: (graph: GraphDocument) => void;
  onMicrosoftAuthChanged?: () => void | Promise<void>;
};

function EnvFieldLabel({
  label,
  tooltipText,
  tooltipId,
}: {
  label: string;
  tooltipText?: string;
  tooltipId: string;
}) {
  if (!tooltipText) {
    return <label className="env-tile-label">{label}</label>;
  }
  return (
    <label className="env-tile-label env-tile-label-with-hint">
      <span>{label}</span>
      <span className="env-tile-hint">
        <button type="button" className="env-tile-hint-button" aria-label={`${label} help`} aria-describedby={tooltipId}>
          ?
        </button>
        <span id={tooltipId} role="tooltip" className="env-tile-tooltip">
          {tooltipText}
        </span>
      </span>
    </label>
  );
}

const GRAPH_ENV_KEY_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;
const STANDARD_GRAPH_ENV_KEYS: ReadonlySet<string> = new Set(STANDARD_GRAPH_ENV_FIELDS.map((field) => field.key));
const SUPABASE_ENV_KEYS = new Set([
  "GRAPH_AGENT_SUPABASE_URL",
  "GRAPH_AGENT_SUPABASE_SECRET_KEY",
  "SUPABASE_PROJECT_REF",
  "SUPABASE_ACCESS_TOKEN",
]);
const SENSITIVE_ENV_KEY_PATTERN = /(password|passwd|passphrase|secret|token|api[_-]?key|private[_-]?key|credential)/i;

function isSensitiveEnvKey(key: string): boolean {
  return SENSITIVE_ENV_KEY_PATTERN.test(key);
}

function isEnvValueVisible(revealedEnvKeys: Record<string, boolean>, key: string): boolean {
  return revealedEnvKeys[key] ?? !isSensitiveEnvKey(key);
}

function normalizedSupabaseValue(envVars: Record<string, string>, key: string): string {
  const value = String(envVars[key] ?? "");
  return value === key ? "" : value;
}

function updateGraphEnvVars(
  graph: GraphDocument,
  updater: (envVars: Record<string, string>) => Record<string, string>,
): GraphDocument {
  return {
    ...graph,
    env_vars: sanitizeGraphEnvVars(updater(getGraphEnvVars(graph))),
  };
}

function disconnectedMicrosoftAuthStatus(): MicrosoftAuthStatus {
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

export function GraphEnvEditor({ graph, onGraphChange, onMicrosoftAuthChanged }: GraphEnvEditorProps) {
  const [newEnvKey, setNewEnvKey] = useState("");
  const [newEnvValue, setNewEnvValue] = useState("");
  const [revealedEnvKeys, setRevealedEnvKeys] = useState<Record<string, boolean>>({});
  const [newEnvValueVisible, setNewEnvValueVisible] = useState(false);
  const [supabaseModalOpen, setSupabaseModalOpen] = useState(false);
  const [microsoftAuthModalOpen, setMicrosoftAuthModalOpen] = useState(false);
  const [lastSupabaseVerification, setLastSupabaseVerification] = useState<SupabaseAuthVerificationResult | null>(null);
  const [supabaseVerificationError, setSupabaseVerificationError] = useState<string | null>(null);
  const [isVerifyingSupabase, setIsVerifyingSupabase] = useState(false);
  const [microsoftAuthStatus, setMicrosoftAuthStatus] = useState<MicrosoftAuthStatus>(disconnectedMicrosoftAuthStatus());
  const [microsoftAuthError, setMicrosoftAuthError] = useState<string | null>(null);

  const envVars = useMemo(() => getGraphEnvVars(graph), [graph]);
  const visibleStandardFields = useMemo(
    () => STANDARD_GRAPH_ENV_FIELDS.filter((field) => !SUPABASE_ENV_KEYS.has(field.key)),
    [],
  );
  const customEnvEntries = useMemo(
    () => Object.entries(envVars).filter(([key]) => !STANDARD_GRAPH_ENV_KEYS.has(key) && !SUPABASE_ENV_KEYS.has(key)),
    [envVars],
  );
  const trimmedNewEnvKey = newEnvKey.trim();
  const newEnvValueInputVisible = newEnvValueVisible || !isSensitiveEnvKey(trimmedNewEnvKey);
  const supabaseUrl = normalizedSupabaseValue(envVars, "GRAPH_AGENT_SUPABASE_URL");
  const supabaseKey = normalizedSupabaseValue(envVars, "GRAPH_AGENT_SUPABASE_SECRET_KEY");
  const supabaseProjectRef = normalizedSupabaseValue(envVars, "SUPABASE_PROJECT_REF");
  const supabaseAccessToken = normalizedSupabaseValue(envVars, "SUPABASE_ACCESS_TOKEN");
  const supabaseConfigured = Boolean(supabaseUrl.trim() && supabaseKey.trim());
  const supabaseMcpConfigured = Boolean(supabaseProjectRef.trim() && supabaseAccessToken.trim());
  const newEnvKeyError =
    trimmedNewEnvKey.length === 0
      ? null
      : NON_PERSISTED_GRAPH_ENV_KEYS.has(trimmedNewEnvKey)
        ? "Use the Microsoft Auth connection flow instead of storing this token in graph env vars."
      : GRAPH_ENV_KEY_PATTERN.test(trimmedNewEnvKey)
        ? null
        : "Use letters, numbers, and underscores only.";

  useEffect(() => {
    let cancelled = false;
    fetchMicrosoftAuthStatus()
      .then((status) => {
        if (!cancelled) {
          setMicrosoftAuthStatus(status);
          setMicrosoftAuthError(null);
        }
      })
      .catch((error: Error) => {
        if (!cancelled) {
          setMicrosoftAuthError(error.message);
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  if (!graph) {
    return null;
  }

  async function handleVerifySupabase() {
    setIsVerifyingSupabase(true);
    setSupabaseVerificationError(null);
    setLastSupabaseVerification(null);
    try {
      const result = await verifySupabaseAuth({
        supabase_url: supabaseUrl,
        supabase_key: supabaseKey,
        schema: "public",
        project_ref: supabaseProjectRef,
        access_token: supabaseAccessToken,
      });
      saveSessionSupabaseSchema(graph, {
        schema: result.schema,
        source_count: result.source_count,
        sources: result.sources,
      });
      setLastSupabaseVerification(result);
    } catch (error) {
      setSupabaseVerificationError(error instanceof Error ? error.message : "Unable to verify Supabase authentication.");
    } finally {
      setIsVerifyingSupabase(false);
    }
  }

  return (
    <>
      <div className="env-supabase-launcher">
        <div className="env-supabase-launcher-copy">
          <div className="env-supabase-launcher-heading">
            <strong>Supabase Auth</strong>
            <span className={`env-integration-status${supabaseConfigured ? " is-ready" : ""}`}>
              {supabaseConfigured ? "Saved" : "Not set"}
            </span>
          </div>
          <p>
            Store the Supabase URL, service role key, and optional hosted MCP token here. Saving opens verification immediately so invalid auth is rejected before it is kept.
          </p>
        </div>
        <div className="env-supabase-launcher-actions">
          <button type="button" className="primary-button env-supabase-launcher-button" onClick={() => setSupabaseModalOpen(true)}>
            Manage Supabase Auth
          </button>
          <button
            type="button"
            className="secondary-button env-supabase-verify-button"
            onClick={() => void handleVerifySupabase()}
            disabled={isVerifyingSupabase || !supabaseUrl.trim() || !supabaseKey.trim()}
          >
            {isVerifyingSupabase ? "Verifying..." : "Verify"}
          </button>
        </div>
        <div className="env-supabase-launcher-meta">
          <span className={`env-integration-status${supabaseConfigured ? " is-ready" : ""}`}>{supabaseConfigured ? "Static auth saved" : "Static auth not set"}</span>
          {supabaseMcpConfigured ? <span className="env-integration-status is-ready">MCP auth saved</span> : null}
        </div>
        {lastSupabaseVerification ? (
          <div className="env-supabase-launcher-verification">
            Verified schema <code>{lastSupabaseVerification.schema}</code> with {lastSupabaseVerification.source_count} discovered source
            {lastSupabaseVerification.source_count === 1 ? "" : "s"}.
            {lastSupabaseVerification.mcp_auth_checked ? " Hosted MCP auth also passed." : ""}
          </div>
        ) : null}
        {supabaseVerificationError ? <div className="env-supabase-launcher-error">{supabaseVerificationError}</div> : null}
      </div>

      <div className="env-supabase-launcher">
        <div className="env-supabase-launcher-copy">
          <div className="env-supabase-launcher-heading">
            <strong>Microsoft Auth</strong>
            <span className={`env-integration-status${microsoftAuthStatus.connected ? " is-ready" : ""}`}>
              {microsoftAuthStatus.connected ? "Connected" : microsoftAuthStatus.pending ? "Pending" : "Not connected"}
            </span>
          </div>
          <p>
            Connect a Microsoft account once with device-code sign-in. Outlook draft nodes use the secure local token cache instead of storing Graph access tokens in graph env vars.
          </p>
        </div>
        <div className="env-supabase-launcher-actions">
          <button
            type="button"
            className="primary-button env-supabase-launcher-button"
            onClick={() => setMicrosoftAuthModalOpen(true)}
          >
            {microsoftAuthStatus.connected ? "Manage Microsoft Auth" : "Connect Microsoft Account"}
          </button>
        </div>
        <div className="env-supabase-launcher-meta">
          {microsoftAuthStatus.account_username ? (
            <span className="env-integration-status is-ready">{microsoftAuthStatus.account_username}</span>
          ) : null}
          {microsoftAuthStatus.client_id ? <span className="env-integration-status">Client configured</span> : null}
        </div>
        {microsoftAuthStatus.pending && microsoftAuthStatus.user_code ? (
          <div className="env-supabase-launcher-verification">
            Finish Microsoft sign-in with code <code>{microsoftAuthStatus.user_code}</code>
            {microsoftAuthStatus.verification_uri ? (
              <>
                {" "}
                at <code>{microsoftAuthStatus.verification_uri}</code>.
              </>
            ) : (
              "."
            )}
          </div>
        ) : null}
        {microsoftAuthStatus.last_error ? <div className="env-supabase-launcher-error">{microsoftAuthStatus.last_error}</div> : null}
        {microsoftAuthError ? <div className="env-supabase-launcher-error">{microsoftAuthError}</div> : null}
      </div>

      <div className="env-tiles">
        {visibleStandardFields.map((field) => (
          <div key={field.key} className="env-tile">
            <EnvFieldLabel label={field.label} tooltipText={field.tooltipText} tooltipId={`env-field-tooltip-${field.key.toLowerCase()}`} />
            <div className="env-tile-value-row">
              <input
                type={isEnvValueVisible(revealedEnvKeys, field.key) ? "text" : "password"}
                value={envVars[field.key] ?? ""}
                placeholder={field.placeholder}
                onChange={(event) =>
                  onGraphChange(
                    updateGraphEnvVars(graph, (currentEnvVars) => ({
                      ...currentEnvVars,
                      [field.key]: event.target.value,
                    })),
                  )
                }
              />
              <button
                type="button"
                className="secondary-button env-tile-visibility-toggle"
                onClick={() =>
                  setRevealedEnvKeys((currentValue) => ({
                    ...currentValue,
                    [field.key]: !isEnvValueVisible(currentValue, field.key),
                  }))
                }
                aria-label={`${isEnvValueVisible(revealedEnvKeys, field.key) ? "Hide" : "Show"} value for ${field.key}`}
                aria-pressed={isEnvValueVisible(revealedEnvKeys, field.key)}
              >
                {isEnvValueVisible(revealedEnvKeys, field.key) ? "Hide" : "Show"}
              </button>
            </div>
          </div>
        ))}
        {customEnvEntries.map(([key, value]) => (
          <div key={key} className="env-tile">
            <label className="env-tile-label">
              <code>{`{${key}}`}</code>
            </label>
            <div className="env-tile-value-row">
              <input
                type={isEnvValueVisible(revealedEnvKeys, key) ? "text" : "password"}
                value={value}
                onChange={(event) =>
                  onGraphChange(
                    updateGraphEnvVars(graph, (currentEnvVars) => ({
                      ...currentEnvVars,
                      [key]: event.target.value,
                    })),
                  )
                }
              />
              <button
                type="button"
                className="secondary-button env-tile-visibility-toggle"
                onClick={() =>
                  setRevealedEnvKeys((currentValue) => ({
                    ...currentValue,
                    [key]: !isEnvValueVisible(currentValue, key),
                  }))
                }
                aria-label={`${isEnvValueVisible(revealedEnvKeys, key) ? "Hide" : "Show"} value for ${key}`}
                aria-pressed={isEnvValueVisible(revealedEnvKeys, key)}
              >
                {isEnvValueVisible(revealedEnvKeys, key) ? "Hide" : "Show"}
              </button>
              <button
                type="button"
                className="secondary-button env-tile-remove"
                onClick={() =>
                  onGraphChange(
                    updateGraphEnvVars(graph, (currentEnvVars) => {
                      const nextEnvVars = { ...currentEnvVars };
                      delete nextEnvVars[key];
                      return nextEnvVars;
                    }),
                  )
                }
              >
                &times;
              </button>
            </div>
          </div>
        ))}
        <div className="env-tile env-tile--add">
          <div className="env-tile-add-inputs">
            <input
              value={newEnvKey}
              placeholder="VAR_NAME"
              onChange={(event) => setNewEnvKey(event.target.value)}
            />
            <input
              type={newEnvValueInputVisible ? "text" : "password"}
              value={newEnvValue}
              placeholder="value"
              onChange={(event) => setNewEnvValue(event.target.value)}
            />
            <button
              type="button"
              className="secondary-button env-tile-visibility-toggle"
              onClick={() => setNewEnvValueVisible((currentValue) => !currentValue)}
              aria-label={`${newEnvValueInputVisible ? "Hide" : "Show"} new environment variable value`}
              aria-pressed={newEnvValueInputVisible}
            >
              {newEnvValueInputVisible ? "Hide" : "Show"}
            </button>
          </div>
          <button
            type="button"
            className="secondary-button"
            onClick={() => {
              if (!trimmedNewEnvKey || newEnvKeyError) {
                return;
              }
              onGraphChange(
                updateGraphEnvVars(graph, (currentEnvVars) => ({
                  ...currentEnvVars,
                  [trimmedNewEnvKey]: newEnvValue,
                })),
              );
              setNewEnvKey("");
              setNewEnvValue("");
              setNewEnvValueVisible(false);
            }}
            disabled={!trimmedNewEnvKey || Boolean(newEnvKeyError)}
          >
            + Add
          </button>
          {newEnvKeyError ? <p className="env-tile-error">{newEnvKeyError}</p> : null}
        </div>
      </div>

      {supabaseModalOpen ? (
        <SupabaseAuthModal
          initialValues={{
            supabaseUrl,
            supabaseKey,
            projectRef: supabaseProjectRef,
            accessToken: supabaseAccessToken,
          }}
          onSave={(values, verification) => {
            setLastSupabaseVerification(verification ?? null);
            setSupabaseVerificationError(null);
            if (verification) {
              saveSessionSupabaseSchema(graph, {
                schema: verification.schema,
                source_count: verification.source_count,
                sources: verification.sources,
              });
            }
            onGraphChange(
              updateGraphEnvVars(graph, (currentEnvVars) => ({
                ...currentEnvVars,
                GRAPH_AGENT_SUPABASE_URL: values.supabaseUrl,
                GRAPH_AGENT_SUPABASE_SECRET_KEY: values.supabaseKey,
                SUPABASE_PROJECT_REF: values.projectRef,
                SUPABASE_ACCESS_TOKEN: values.accessToken,
              })),
            );
          }}
          onClose={() => setSupabaseModalOpen(false)}
        />
      ) : null}
      {microsoftAuthModalOpen ? (
        <MicrosoftAuthModal
          initialStatus={microsoftAuthStatus}
          onStatusChange={(status) => {
            setMicrosoftAuthStatus(status);
            setMicrosoftAuthError(null);
            void onMicrosoftAuthChanged?.();
          }}
          onClose={() => setMicrosoftAuthModalOpen(false)}
        />
      ) : null}
    </>
  );
}
