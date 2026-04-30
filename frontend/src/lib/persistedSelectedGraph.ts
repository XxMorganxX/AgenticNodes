const STORAGE_KEY = "agentic-nodes-selected-graph-id";

export function loadPersistedSelectedGraphId(): string | null {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) {
      return null;
    }
    const trimmed = raw.trim();
    return trimmed || null;
  } catch {
    return null;
  }
}

export function savePersistedSelectedGraphId(graphId: string | null | undefined): void {
  try {
    if (!graphId || !graphId.trim()) {
      localStorage.removeItem(STORAGE_KEY);
      return;
    }
    localStorage.setItem(STORAGE_KEY, graphId.trim());
  } catch {
    // Ignore local persistence failures.
  }
}
