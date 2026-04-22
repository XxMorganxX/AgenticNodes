import { useEffect, useMemo, useState } from "react";
import type { MouseEvent } from "react";

import { verifySupabaseAuth } from "../lib/api";
import {
  createSupabaseConnectionIdentity,
  IMPLICIT_LEGACY_SUPABASE_CONNECTION_ID,
  LEGACY_SUPABASE_ACCESS_TOKEN_ENV_VAR,
  LEGACY_SUPABASE_KEY_ENV_VAR,
  LEGACY_SUPABASE_PROJECT_REF_ENV_VAR,
  LEGACY_SUPABASE_URL_ENV_VAR,
} from "../lib/supabaseConnections";
import type { DerivedSupabaseConnection } from "../lib/supabaseConnections";
import type { SupabaseAuthVerificationResult, SupabaseConnectionDefinition } from "../lib/types";

type EditableSupabaseConnection = {
  draft_id: string;
  connection_id: string;
  name: string;
  supabase_url_env_var: string;
  supabase_key_env_var: string;
  project_ref_env_var: string;
  access_token_env_var: string;
  supabaseUrl: string;
  supabaseKey: string;
  accessToken: string;
  isImplicit: boolean;
};

type SupabaseConnectionsModalProps = {
  connections: DerivedSupabaseConnection[];
  envVars: Record<string, string>;
  defaultConnectionId: string;
  runStoreConnectionId: string;
  referencedConnectionIds: Set<string>;
  onSave: (payload: {
    connections: SupabaseConnectionDefinition[];
    defaultConnectionId: string;
    runStoreConnectionId: string;
    envVars: Record<string, string>;
    verification: SupabaseAuthVerificationResult | null;
  }) => void;
  onClose: () => void;
};

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

function createDraftFromConnection(connection: DerivedSupabaseConnection, envVars: Record<string, string>): EditableSupabaseConnection {
  return {
    draft_id: connection.connection_id || `draft-${Date.now()}`,
    connection_id: connection.connection_id,
    name: connection.name,
    supabase_url_env_var: connection.supabase_url_env_var,
    supabase_key_env_var: connection.supabase_key_env_var,
    project_ref_env_var: connection.project_ref_env_var,
    access_token_env_var: connection.access_token_env_var,
    supabaseUrl: String(envVars[connection.supabase_url_env_var] ?? ""),
    supabaseKey: String(envVars[connection.supabase_key_env_var] ?? ""),
    accessToken: String(envVars[connection.access_token_env_var] ?? ""),
    isImplicit: Boolean(connection.isImplicit),
  };
}

function normalizeConnectionName(value: string): string {
  return value.trim() || "Supabase Connection";
}

export function SupabaseConnectionsModal({
  connections,
  envVars,
  defaultConnectionId,
  runStoreConnectionId,
  referencedConnectionIds,
  onSave,
  onClose,
}: SupabaseConnectionsModalProps) {
  const [drafts, setDrafts] = useState<EditableSupabaseConnection[]>(() => connections.map((connection) => createDraftFromConnection(connection, envVars)));
  const [selectedDraftId, setSelectedDraftId] = useState<string>(() => connections[0]?.connection_id || "new-connection");
  const [selectedDefaultConnectionId, setSelectedDefaultConnectionId] = useState<string>(defaultConnectionId);
  const [selectedRunStoreConnectionId, setSelectedRunStoreConnectionId] = useState<string>(runStoreConnectionId);
  const [isVerifying, setIsVerifying] = useState(false);
  const [verificationError, setVerificationError] = useState<string | null>(null);
  const [verification, setVerification] = useState<SupabaseAuthVerificationResult | null>(null);
  const [saveError, setSaveError] = useState<string | null>(null);

  useEffect(() => {
    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        onClose();
      }
    }
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [onClose]);

  const selectedDraft = drafts.find((draft) => draft.draft_id === selectedDraftId) ?? drafts[0] ?? null;
  const selectableConnections = useMemo(
    () =>
      drafts.map((draft) => ({
        connection_id: draft.connection_id.trim() || draft.draft_id,
        name: normalizeConnectionName(draft.name),
      })),
    [drafts],
  );

  function updateDraft(draftId: string, updater: (draft: EditableSupabaseConnection) => EditableSupabaseConnection): void {
    setDrafts((current) => current.map((draft) => (draft.draft_id === draftId ? updater(draft) : draft)));
  }

  function handleOverlayClick(event: MouseEvent<HTMLDivElement>) {
    if (event.target === event.currentTarget) {
      onClose();
    }
  }

  function addDraftConnection(): void {
    const draftId = `draft-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    setDrafts((current) => [
      ...current,
      {
        draft_id: draftId,
        connection_id: "",
        name: "",
        supabase_url_env_var: "",
        supabase_key_env_var: "",
        project_ref_env_var: "",
        access_token_env_var: "",
        supabaseUrl: "",
        supabaseKey: "",
        accessToken: "",
        isImplicit: false,
      },
    ]);
    setSelectedDraftId(draftId);
    setVerification(null);
    setVerificationError(null);
  }

  function deleteDraftConnection(draftId: string): void {
    const draft = drafts.find((candidate) => candidate.draft_id === draftId);
    if (!draft) {
      return;
    }
    const effectiveConnectionId = draft.connection_id.trim();
    if (effectiveConnectionId && referencedConnectionIds.has(effectiveConnectionId)) {
      setSaveError(`Connection "${draft.name}" is still used by one or more nodes.`);
      return;
    }
    const remainingDrafts = drafts.filter((candidate) => candidate.draft_id !== draftId);
    setDrafts(remainingDrafts);
    if (selectedDraftId === draftId) {
      setSelectedDraftId(remainingDrafts[0]?.draft_id ?? "new-connection");
    }
    if (selectedDefaultConnectionId === effectiveConnectionId) {
      setSelectedDefaultConnectionId(remainingDrafts[0]?.connection_id ?? "");
    }
    if (selectedRunStoreConnectionId === effectiveConnectionId) {
      setSelectedRunStoreConnectionId("");
    }
    setVerification(null);
    setVerificationError(null);
    setSaveError(null);
  }

  async function handleVerifySelected(): Promise<void> {
    if (!selectedDraft) {
      return;
    }
    setIsVerifying(true);
    setVerification(null);
    setVerificationError(null);
    try {
      const result = await verifySupabaseAuth({
        supabase_url: selectedDraft.supabaseUrl,
        supabase_key: selectedDraft.supabaseKey,
        schema: "public",
        project_ref: deriveSupabaseProjectRef(selectedDraft.supabaseUrl),
        access_token: selectedDraft.accessToken,
      });
      setVerification(result);
    } catch (error) {
      setVerificationError(error instanceof Error ? error.message : "Unable to verify the selected Supabase connection.");
    } finally {
      setIsVerifying(false);
    }
  }

  function materializeDrafts(): {
    connections: SupabaseConnectionDefinition[];
    nextEnvVars: Record<string, string>;
    defaultConnectionId: string;
    runStoreConnectionId: string;
  } {
    const nextEnvVars = { ...envVars };
    const previousExplicitConnections = connections.filter((connection) => !connection.isImplicit);
    const retainedConnections: SupabaseConnectionDefinition[] = [];
    const connectionIdsByDraftId = new Map<string, string>();

    for (const draft of drafts) {
      const trimmedName = draft.name.trim();
      const hasAnyValues = Boolean(trimmedName || draft.supabaseUrl.trim() || draft.supabaseKey.trim() || draft.accessToken.trim());
      if (!hasAnyValues) {
        continue;
      }
      if (!trimmedName) {
        throw new Error("Each Supabase connection needs a name before it can be saved.");
      }
      let connection: SupabaseConnectionDefinition;
      if (draft.connection_id && draft.connection_id !== IMPLICIT_LEGACY_SUPABASE_CONNECTION_ID) {
        connection = {
          connection_id: draft.connection_id,
          name: normalizeConnectionName(draft.name),
          supabase_url_env_var: draft.supabase_url_env_var,
          supabase_key_env_var: draft.supabase_key_env_var,
          project_ref_env_var: draft.project_ref_env_var,
          access_token_env_var: draft.access_token_env_var,
        };
      } else if (draft.isImplicit) {
        connection = {
          connection_id: "default-supabase",
          name: normalizeConnectionName(draft.name || "Default Supabase"),
          supabase_url_env_var: LEGACY_SUPABASE_URL_ENV_VAR,
          supabase_key_env_var: LEGACY_SUPABASE_KEY_ENV_VAR,
          project_ref_env_var: LEGACY_SUPABASE_PROJECT_REF_ENV_VAR,
          access_token_env_var: LEGACY_SUPABASE_ACCESS_TOKEN_ENV_VAR,
        };
        if (retainedConnections.some((candidate) => candidate.connection_id === connection.connection_id)) {
          connection = {
            ...createSupabaseConnectionIdentity(retainedConnections, connection.name),
            supabase_url_env_var: LEGACY_SUPABASE_URL_ENV_VAR,
            supabase_key_env_var: LEGACY_SUPABASE_KEY_ENV_VAR,
            project_ref_env_var: LEGACY_SUPABASE_PROJECT_REF_ENV_VAR,
            access_token_env_var: LEGACY_SUPABASE_ACCESS_TOKEN_ENV_VAR,
          };
        }
      } else {
        connection = createSupabaseConnectionIdentity(retainedConnections, normalizeConnectionName(draft.name));
      }
      retainedConnections.push(connection);
      connectionIdsByDraftId.set(draft.draft_id, connection.connection_id);
      nextEnvVars[connection.supabase_url_env_var] = draft.supabaseUrl;
      nextEnvVars[connection.supabase_key_env_var] = draft.supabaseKey;
      nextEnvVars[connection.project_ref_env_var] = deriveSupabaseProjectRef(draft.supabaseUrl);
      nextEnvVars[connection.access_token_env_var] = draft.accessToken;
    }

    const retainedIds = new Set(retainedConnections.map((connection) => connection.connection_id));
    for (const connection of previousExplicitConnections) {
      if (retainedIds.has(connection.connection_id)) {
        continue;
      }
      delete nextEnvVars[connection.supabase_url_env_var];
      delete nextEnvVars[connection.supabase_key_env_var];
      delete nextEnvVars[connection.project_ref_env_var];
      delete nextEnvVars[connection.access_token_env_var];
    }

    let nextDefaultConnectionId = selectedDefaultConnectionId.trim();
    if (nextDefaultConnectionId === IMPLICIT_LEGACY_SUPABASE_CONNECTION_ID) {
      nextDefaultConnectionId = retainedConnections.find(
        (connection) =>
          connection.supabase_url_env_var === LEGACY_SUPABASE_URL_ENV_VAR
          && connection.supabase_key_env_var === LEGACY_SUPABASE_KEY_ENV_VAR,
      )?.connection_id ?? "";
    }
    nextDefaultConnectionId = connectionIdsByDraftId.get(nextDefaultConnectionId) ?? nextDefaultConnectionId;
    if (nextDefaultConnectionId && !retainedConnections.some((connection) => connection.connection_id === nextDefaultConnectionId)) {
      nextDefaultConnectionId = retainedConnections[0]?.connection_id ?? "";
    }
    let nextRunStoreConnectionId = selectedRunStoreConnectionId.trim();
    if (nextRunStoreConnectionId === IMPLICIT_LEGACY_SUPABASE_CONNECTION_ID) {
      nextRunStoreConnectionId = retainedConnections.find(
        (connection) =>
          connection.supabase_url_env_var === LEGACY_SUPABASE_URL_ENV_VAR
          && connection.supabase_key_env_var === LEGACY_SUPABASE_KEY_ENV_VAR,
      )?.connection_id ?? "";
    }
    nextRunStoreConnectionId = connectionIdsByDraftId.get(nextRunStoreConnectionId) ?? nextRunStoreConnectionId;
    if (nextRunStoreConnectionId && !retainedConnections.some((connection) => connection.connection_id === nextRunStoreConnectionId)) {
      nextRunStoreConnectionId = "";
    }
    return {
      connections: retainedConnections,
      nextEnvVars,
      defaultConnectionId: nextDefaultConnectionId,
      runStoreConnectionId: nextRunStoreConnectionId,
    };
  }

  function handleSaveChanges(): void {
    try {
      const nextState = materializeDrafts();
      onSave({
        connections: nextState.connections,
        envVars: nextState.nextEnvVars,
        defaultConnectionId: nextState.defaultConnectionId,
        runStoreConnectionId: nextState.runStoreConnectionId,
        verification,
      });
      onClose();
    } catch (error) {
      setSaveError(error instanceof Error ? error.message : "Unable to save Supabase connections.");
    }
  }

  return (
    <div className="tool-details-modal-backdrop" onClick={handleOverlayClick} role="presentation">
      <section className="tool-details-modal supabase-connections-modal" role="dialog" aria-modal="true" aria-labelledby="supabase-connections-modal-title">
        <div className="tool-details-modal-header">
          <div>
            <div className="tool-details-modal-eyebrow">Environment</div>
            <h3 id="supabase-connections-modal-title">Supabase Connections</h3>
            <p>Manage named Supabase projects for this graph. Connection values stay in graph env vars, while nodes can target a connection by name.</p>
          </div>
          <button type="button" className="secondary-button" onClick={onClose}>
            Close
          </button>
        </div>

        <div className="tool-details-modal-body supabase-connections-modal-body">
          <aside className="supabase-connections-sidebar">
            <div className="supabase-connections-sidebar-header">
              <strong>Connections</strong>
              <span>{drafts.length} total</span>
            </div>
            <div className="supabase-connections-list" role="list" aria-label="Supabase connections">
              {drafts.map((draft) => {
                const isSelected = selectedDraftId === draft.draft_id;
                const isDefault =
                  selectedDefaultConnectionId === draft.connection_id
                  || (selectedDefaultConnectionId === draft.draft_id && !draft.connection_id.trim());
                const hasValues = Boolean(draft.supabaseUrl.trim() && draft.supabaseKey.trim());
                return (
                  <button
                    key={draft.draft_id}
                    type="button"
                    className={`supabase-connection-list-item${isSelected ? " is-active" : ""}`}
                    onClick={() => {
                      setSelectedDraftId(draft.draft_id);
                      setSaveError(null);
                      setVerification(null);
                      setVerificationError(null);
                    }}
                  >
                    <div className="supabase-connection-list-item-header">
                      <strong>{normalizeConnectionName(draft.name || (draft.isImplicit ? "Default Supabase" : "New Connection"))}</strong>
                      {draft.isImplicit ? <span className="supabase-connection-pill">Legacy</span> : null}
                    </div>
                    <div className="supabase-connection-list-item-meta">
                      <span>{hasValues ? "Credentials saved" : "Credentials incomplete"}</span>
                      {isDefault ? <span className="supabase-connection-pill is-default">Default</span> : null}
                    </div>
                  </button>
                );
              })}
            </div>
            <button type="button" className="secondary-button supabase-connections-add-button" onClick={addDraftConnection}>
              + Add Connection
            </button>
          </aside>

          <div className="supabase-connections-detail">
            {selectedDraft ? (
              <>
                <div className="supabase-connections-detail-header">
                  <div>
                    <strong>{normalizeConnectionName(selectedDraft.name || (selectedDraft.isImplicit ? "Default Supabase" : "New Connection"))}</strong>
                    <p>
                      Env vars stay stable after creation, so renaming a connection does not break node bindings.
                      {selectedDraft.isImplicit ? " Saving this legacy connection will persist it explicitly for this graph." : ""}
                    </p>
                  </div>
                  <div className="supabase-connections-detail-selects">
                    <label className="supabase-connections-default-select">
                      <span>Default for new nodes</span>
                      <select
                        value={selectedDefaultConnectionId}
                        onChange={(event) => setSelectedDefaultConnectionId(event.target.value)}
                      >
                        <option value="">No default</option>
                        {selectableConnections.map((connection) => (
                          <option key={connection.connection_id} value={connection.connection_id}>
                            {connection.name}
                          </option>
                        ))}
                      </select>
                    </label>
                    <label className="supabase-connections-default-select">
                      <span>Run history tables project</span>
                      <select
                        value={selectedRunStoreConnectionId}
                        onChange={(event) => setSelectedRunStoreConnectionId(event.target.value)}
                      >
                        <option value="">Not configured</option>
                        {selectableConnections.map((connection) => (
                          <option key={connection.connection_id} value={connection.connection_id}>
                            {connection.name}
                          </option>
                        ))}
                      </select>
                    </label>
                  </div>
                </div>

                <div className="supabase-connections-form-grid">
                  <label>
                    <span>Name</span>
                    <input
                      type="text"
                      value={selectedDraft.name}
                      placeholder="Production DB"
                      onChange={(event) => {
                        const nextValue = event.target.value;
                        updateDraft(selectedDraft.draft_id, (draft) => ({ ...draft, name: nextValue }));
                      }}
                    />
                  </label>
                  <label>
                    <span>Supabase URL</span>
                    <input
                      type="text"
                      value={selectedDraft.supabaseUrl}
                      placeholder="https://your-project-ref.supabase.co"
                      onChange={(event) => {
                        const nextValue = event.target.value;
                        updateDraft(selectedDraft.draft_id, (draft) => ({ ...draft, supabaseUrl: nextValue }));
                      }}
                    />
                  </label>
                  <label className="supabase-connections-field-span">
                    <span>Supabase Secret Key or Legacy service_role Key</span>
                    <input
                      type="password"
                      value={selectedDraft.supabaseKey}
                      placeholder="sb_secret_... or service_role"
                      onChange={(event) => {
                        const nextValue = event.target.value;
                        updateDraft(selectedDraft.draft_id, (draft) => ({ ...draft, supabaseKey: nextValue }));
                      }}
                    />
                  </label>
                  <label>
                    <span>Project Ref</span>
                    <input type="text" value={deriveSupabaseProjectRef(selectedDraft.supabaseUrl)} placeholder="Parsed from URL" readOnly />
                  </label>
                  <label>
                    <span>Hosted MCP Access Token</span>
                    <input
                      type="password"
                      value={selectedDraft.accessToken}
                      placeholder="Optional for hosted MCP"
                      onChange={(event) => {
                        const nextValue = event.target.value;
                        updateDraft(selectedDraft.draft_id, (draft) => ({ ...draft, accessToken: nextValue }));
                      }}
                    />
                  </label>
                </div>

                <div className="supabase-connections-note">
                  Nodes can use this connection by name, and existing bindings will keep working even if you later rotate the credentials.
                </div>
              </>
            ) : (
              <div className="tool-details-modal-help">Add a Supabase connection to get started.</div>
            )}

            {verification ? (
              <div className="tool-details-modal-help supabase-auth-modal-status">
                Verified schema <code>{verification.schema}</code> with {verification.source_count} discovered source{verification.source_count === 1 ? "" : "s"}.
                {verification.mcp_auth_checked ? " Hosted MCP auth also succeeded." : ""}
              </div>
            ) : null}
            {verificationError ? <div className="tool-details-modal-help supabase-auth-modal-status supabase-auth-modal-status--error">{verificationError}</div> : null}
            {saveError ? <div className="tool-details-modal-help supabase-auth-modal-status supabase-auth-modal-status--error">{saveError}</div> : null}

            <div className="supabase-connections-actions">
              <div className="supabase-connections-actions-left">
                {selectedDraft ? (
                  <button type="button" className="secondary-button" onClick={() => deleteDraftConnection(selectedDraft.draft_id)}>
                    Delete Connection
                  </button>
                ) : null}
              </div>
              <div className="supabase-connections-actions-right">
                {selectedDraft ? (
                  <button
                    type="button"
                    className="secondary-button"
                    onClick={() => void handleVerifySelected()}
                    disabled={isVerifying || !selectedDraft.supabaseUrl.trim() || !selectedDraft.supabaseKey.trim()}
                  >
                    {isVerifying ? "Verifying..." : "Verify Selected"}
                  </button>
                ) : null}
                <button type="button" className="primary-button" onClick={handleSaveChanges}>
                  Save Changes
                </button>
              </div>
            </div>
          </div>
        </div>
      </section>
    </div>
  );
}
