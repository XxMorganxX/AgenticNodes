import type { GraphDefinition, SupabaseSchemaPreviewResult, SupabaseSchemaSource } from "./types";

const STORAGE_KEY = "agentic-nodes-supabase-schema-cache";

type CachedSupabaseSchemaRecord = {
  schema: string;
  source_count: number;
  sources: SupabaseSchemaSource[];
  saved_at: string;
};

function storageKeyForGraph(graph: Pick<GraphDefinition, "graph_id"> | null | undefined): string {
  return String(graph?.graph_id ?? "").trim() || "__draft__";
}

function loadCacheMap(): Record<string, CachedSupabaseSchemaRecord> {
  try {
    const raw = window.sessionStorage.getItem(STORAGE_KEY);
    if (!raw) {
      return {};
    }
    const parsed = JSON.parse(raw) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return {};
    }
    return Object.fromEntries(
      Object.entries(parsed).flatMap(([graphKey, value]) => {
        if (!graphKey.trim() || !value || typeof value !== "object" || Array.isArray(value)) {
          return [];
        }
        const record = value as Partial<CachedSupabaseSchemaRecord>;
        const schema = typeof record.schema === "string" ? record.schema : "public";
        const sourceCount = Number(record.source_count ?? 0);
        const sources = Array.isArray(record.sources) ? (record.sources as SupabaseSchemaSource[]) : [];
        const savedAt = typeof record.saved_at === "string" ? record.saved_at : "";
        return [[graphKey, { schema, source_count: Number.isFinite(sourceCount) ? sourceCount : sources.length, sources, saved_at: savedAt }]];
      }),
    );
  } catch {
    return {};
  }
}

function saveCacheMap(nextMap: Record<string, CachedSupabaseSchemaRecord>): void {
  try {
    window.sessionStorage.setItem(STORAGE_KEY, JSON.stringify(nextMap));
  } catch {
    // Ignore client-side cache failures and continue without temporary schema memory.
  }
}

export function loadSessionSupabaseSchema(graph: Pick<GraphDefinition, "graph_id"> | null | undefined): SupabaseSchemaPreviewResult | null {
  const cached = loadCacheMap()[storageKeyForGraph(graph)];
  if (!cached) {
    return null;
  }
  return {
    schema: cached.schema,
    source_count: cached.source_count,
    sources: cached.sources,
  };
}

export function saveSessionSupabaseSchema(
  graph: Pick<GraphDefinition, "graph_id"> | null | undefined,
  payload: SupabaseSchemaPreviewResult,
): void {
  const current = loadCacheMap();
  current[storageKeyForGraph(graph)] = {
    schema: payload.schema,
    source_count: payload.source_count,
    sources: payload.sources,
    saved_at: new Date().toISOString(),
  };
  saveCacheMap(current);
}

export function clearSessionSupabaseSchema(graph: Pick<GraphDefinition, "graph_id"> | null | undefined): void {
  const current = loadCacheMap();
  const key = storageKeyForGraph(graph);
  if (!(key in current)) {
    return;
  }
  delete current[key];
  saveCacheMap(current);
}
