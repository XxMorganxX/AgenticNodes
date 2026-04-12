import { useEffect, useMemo, useState } from "react";

import { fetchMicrosoftAuthStatus } from "../lib/api";
import { getGraphEnvVars, NON_PERSISTED_GRAPH_ENV_KEYS, STANDARD_GRAPH_ENV_FIELDS, sanitizeGraphEnvVars } from "../lib/graphEnv";
import { saveSessionSupabaseSchema } from "../lib/sessionSupabaseSchema";
import type { GraphDocument, MicrosoftAuthStatus, SupabaseAuthVerificationResult } from "../lib/types";
import { collectReferencedSupabaseConnectionIds, getSupabaseConnectionById, getSupabaseConnections, managedSupabaseEnvKeys } from "../lib/supabaseConnections";
import { MicrosoftAuthModal } from "./MicrosoftAuthModal";
import { SupabaseConnectionsModal } from "./SupabaseConnectionsModal";

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
  const [microsoftAuthStatus, setMicrosoftAuthStatus] = useState<MicrosoftAuthStatus>(disconnectedMicrosoftAuthStatus());
  const [microsoftAuthError, setMicrosoftAuthError] = useState<string | null>(null);

  const envVars = useMemo(() => getGraphEnvVars(graph), [graph]);
  const managedConnectionEnvKeys = useMemo(() => managedSupabaseEnvKeys(graph), [graph]);
  const supabaseConnections = useMemo(() => getSupabaseConnections(graph), [graph]);
  const explicitSupabaseConnections = useMemo(() => supabaseConnections.filter((connection) => !connection.isImplicit), [supabaseConnections]);
  const referencedSupabaseConnectionIds = useMemo(() => collectReferencedSupabaseConnectionIds(graph), [graph]);
  const defaultSupabaseConnection = useMemo(
    () => getSupabaseConnectionById(graph, String(graph?.default_supabase_connection_id ?? "")),
    [graph],
  );
  const visibleStandardFields = useMemo(
    () => STANDARD_GRAPH_ENV_FIELDS.filter((field) => !managedConnectionEnvKeys.has(field.key)),
    [managedConnectionEnvKeys],
  );
  const customEnvEntries = useMemo(
    () => Object.entries(envVars).filter(([key]) => !STANDARD_GRAPH_ENV_KEYS.has(key) && !managedConnectionEnvKeys.has(key)),
    [envVars, managedConnectionEnvKeys],
  );
  const trimmedNewEnvKey = newEnvKey.trim();
  const newEnvValueInputVisible = newEnvValueVisible || !isSensitiveEnvKey(trimmedNewEnvKey);
  const supabaseConfiguredCount = explicitSupabaseConnections.filter((connection) => {
    const url = normalizedSupabaseValue(envVars, connection.supabase_url_env_var).trim();
    const key = normalizedSupabaseValue(envVars, connection.supabase_key_env_var).trim();
    return Boolean(url && key);
  }).length;
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

  return (
    <>
      <div className="env-supabase-launcher">
        <div className="env-supabase-launcher-copy">
          <div className="env-supabase-launcher-heading">
            <strong>Supabase Connections</strong>
            <span className={`env-integration-status${explicitSupabaseConnections.length ? " is-ready" : ""}`}>
              {explicitSupabaseConnections.length ? `${explicitSupabaseConnections.length} saved` : "Not set"}
            </span>
          </div>
          <p>
            Manage multiple Supabase projects for this graph. Nodes can target a named connection while secrets continue to live in graph env vars behind the scenes.
          </p>
        </div>
        <div className="env-supabase-launcher-actions">
          <button type="button" className="primary-button env-supabase-launcher-button" onClick={() => setSupabaseModalOpen(true)}>
            Manage Supabase Connections
          </button>
        </div>
        <div className="env-supabase-launcher-meta">
          <span className={`env-integration-status${supabaseConfiguredCount ? " is-ready" : ""}`}>
            {supabaseConfiguredCount ? `${supabaseConfiguredCount} ready for static auth` : "No connection values saved yet"}
          </span>
          {defaultSupabaseConnection ? <span className="env-integration-status is-ready">Default: {defaultSupabaseConnection.name}</span> : null}
        </div>
        {lastSupabaseVerification ? (
          <div className="env-supabase-launcher-verification">
            Verified schema <code>{lastSupabaseVerification.schema}</code> with {lastSupabaseVerification.source_count} discovered source
            {lastSupabaseVerification.source_count === 1 ? "" : "s"}.
            {lastSupabaseVerification.mcp_auth_checked ? " Hosted MCP auth also passed." : ""}
          </div>
        ) : null}
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
        <SupabaseConnectionsModal
          connections={supabaseConnections}
          envVars={envVars}
          defaultConnectionId={String(graph.default_supabase_connection_id ?? "")}
          referencedConnectionIds={referencedSupabaseConnectionIds}
          onSave={({ connections, defaultConnectionId, envVars: nextEnvVars, verification }) => {
            setLastSupabaseVerification(verification);
            if (verification) {
              saveSessionSupabaseSchema(graph, {
                schema: verification.schema,
                source_count: verification.source_count,
                sources: verification.sources,
              }, {
                connectionScope: defaultConnectionId || "graph-connections",
                schemaName: verification.schema,
              });
            }
            onGraphChange(
              {
                ...graph,
                env_vars: sanitizeGraphEnvVars(nextEnvVars),
                supabase_connections: connections,
                default_supabase_connection_id: defaultConnectionId,
              },
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
