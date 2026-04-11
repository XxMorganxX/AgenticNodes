const STORAGE_KEY = "agentic-nodes-graph-env-vars";
const DRAFT_GRAPH_ENV_KEY = "__draft__";

function loadPersistedEnvMap(): Record<string, Record<string, string>> {
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
      Object.entries(parsed).flatMap(([graphKey, envVars]) => {
        if (!graphKey.trim() || !envVars || typeof envVars !== "object" || Array.isArray(envVars)) {
          return [];
        }
        const normalizedEnvVars = Object.fromEntries(
          Object.entries(envVars).flatMap(([envKey, envValue]) =>
            envKey.trim() ? [[envKey.trim(), typeof envValue === "string" ? envValue : String(envValue ?? "")]] : [],
          ),
        );
        return [[graphKey, normalizedEnvVars]];
      }),
    );
  } catch {
    return {};
  }
}

function savePersistedEnvMap(nextMap: Record<string, Record<string, string>>): void {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(nextMap));
  } catch {
    // Ignore local persistence failures and keep the in-memory env vars.
  }
}

export function persistedGraphEnvStorageKey(graphId: string | null | undefined, selectedGraphId: string | null | undefined): string {
  return selectedGraphId?.trim() || graphId?.trim() || DRAFT_GRAPH_ENV_KEY;
}

export function loadPersistedGraphEnvVars(graphKey: string | null | undefined): Record<string, string> | null {
  if (!graphKey?.trim()) {
    return null;
  }
  return loadPersistedEnvMap()[graphKey] ?? null;
}

export function savePersistedGraphEnvVars(graphKey: string | null | undefined, envVars: Record<string, string>): void {
  if (!graphKey?.trim()) {
    return;
  }
  const current = loadPersistedEnvMap();
  current[graphKey] = { ...envVars };
  savePersistedEnvMap(current);
}

export function clearPersistedGraphEnvVars(graphKey: string | null | undefined): void {
  if (!graphKey?.trim()) {
    return;
  }
  const current = loadPersistedEnvMap();
  if (!(graphKey in current)) {
    return;
  }
  delete current[graphKey];
  savePersistedEnvMap(current);
}

export function draftGraphEnvStorageKey(): string {
  return DRAFT_GRAPH_ENV_KEY;
}
