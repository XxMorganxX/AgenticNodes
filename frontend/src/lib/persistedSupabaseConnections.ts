import type { SupabaseConnectionDefinition } from "./types";

const STORAGE_KEY = "agentic-nodes-supabase-connections";

type PersistedSupabaseConnectionState = {
  supabase_connections: SupabaseConnectionDefinition[];
  default_supabase_connection_id: string;
  run_store_supabase_connection_id: string;
};

function sanitizeConnection(connection: SupabaseConnectionDefinition | null | undefined): SupabaseConnectionDefinition | null {
  if (!connection || typeof connection !== "object") {
    return null;
  }
  const connectionId = String(connection.connection_id ?? "").trim();
  const name = String(connection.name ?? "").trim();
  const supabaseUrlEnvVar = String(connection.supabase_url_env_var ?? "").trim();
  const supabaseKeyEnvVar = String(connection.supabase_key_env_var ?? "").trim();
  const projectRefEnvVar = String(connection.project_ref_env_var ?? "").trim();
  const accessTokenEnvVar = String(connection.access_token_env_var ?? "").trim();
  if (!connectionId || !name || !supabaseUrlEnvVar || !supabaseKeyEnvVar || !projectRefEnvVar || !accessTokenEnvVar) {
    return null;
  }
  return {
    connection_id: connectionId,
    name,
    supabase_url_env_var: supabaseUrlEnvVar,
    supabase_key_env_var: supabaseKeyEnvVar,
    project_ref_env_var: projectRefEnvVar,
    access_token_env_var: accessTokenEnvVar,
  };
}

function sanitizeState(rawState: unknown): PersistedSupabaseConnectionState | null {
  if (!rawState || typeof rawState !== "object" || Array.isArray(rawState)) {
    return null;
  }
  const record = rawState as Partial<PersistedSupabaseConnectionState>;
  const seenIds = new Set<string>();
  const connections = Array.isArray(record.supabase_connections)
    ? record.supabase_connections.flatMap((connection) => {
        const sanitized = sanitizeConnection(connection);
        if (!sanitized || seenIds.has(sanitized.connection_id)) {
          return [];
        }
        seenIds.add(sanitized.connection_id);
        return [sanitized];
      })
    : [];
  const defaultConnectionId = String(record.default_supabase_connection_id ?? "").trim();
  const runStoreConnectionId = String(record.run_store_supabase_connection_id ?? "").trim();
  return {
    supabase_connections: connections,
    default_supabase_connection_id: defaultConnectionId,
    run_store_supabase_connection_id: runStoreConnectionId,
  };
}

function loadPersistedStateMap(): Record<string, PersistedSupabaseConnectionState> {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) {
      return {};
    }
    const parsed = JSON.parse(raw) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return {};
    }
    return Object.fromEntries(
      Object.entries(parsed).flatMap(([graphKey, value]) => {
        const trimmedKey = graphKey.trim();
        const sanitized = sanitizeState(value);
        if (!trimmedKey || !sanitized) {
          return [];
        }
        return [[trimmedKey, sanitized]];
      }),
    );
  } catch {
    return {};
  }
}

function savePersistedStateMap(nextMap: Record<string, PersistedSupabaseConnectionState>): void {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(nextMap));
  } catch {
    // Ignore local persistence failures and keep the in-memory draft state.
  }
}

export function loadPersistedSupabaseConnectionState(graphKey: string | null | undefined): PersistedSupabaseConnectionState | null {
  if (!graphKey?.trim()) {
    return null;
  }
  return loadPersistedStateMap()[graphKey] ?? null;
}

export function savePersistedSupabaseConnectionState(
  graphKey: string | null | undefined,
  state: {
    supabase_connections?: SupabaseConnectionDefinition[] | null;
    default_supabase_connection_id?: string | null;
    run_store_supabase_connection_id?: string | null;
  },
): void {
  if (!graphKey?.trim()) {
    return;
  }
  const sanitized = sanitizeState({
    supabase_connections: state.supabase_connections ?? [],
    default_supabase_connection_id: state.default_supabase_connection_id ?? "",
    run_store_supabase_connection_id: state.run_store_supabase_connection_id ?? "",
  });
  if (!sanitized) {
    return;
  }
  const current = loadPersistedStateMap();
  if (
    sanitized.supabase_connections.length === 0
    && !sanitized.default_supabase_connection_id
    && !sanitized.run_store_supabase_connection_id
  ) {
    delete current[graphKey];
    savePersistedStateMap(current);
    return;
  }
  current[graphKey] = sanitized;
  savePersistedStateMap(current);
}

export function clearPersistedSupabaseConnectionState(graphKey: string | null | undefined): void {
  if (!graphKey?.trim()) {
    return;
  }
  const current = loadPersistedStateMap();
  if (!(graphKey in current)) {
    return;
  }
  delete current[graphKey];
  savePersistedStateMap(current);
}
