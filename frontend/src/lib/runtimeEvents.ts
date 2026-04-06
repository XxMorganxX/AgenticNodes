import type { RunDocument, RunState, RuntimeEvent } from "./types";

export const RUNTIME_EVENT_SCHEMA_VERSION = "runtime.v1";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function normalizeNullableString(value: unknown): string | null {
  if (value == null) {
    return null;
  }
  return typeof value === "string" ? value : String(value);
}

function normalizeRunDocuments(value: unknown): RunDocument[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .filter((candidate) => isRecord(candidate))
    .map((candidate) => ({
      ...candidate,
      document_id: typeof candidate.document_id === "string" ? candidate.document_id : String(candidate.document_id ?? ""),
      name: typeof candidate.name === "string" ? candidate.name : String(candidate.name ?? ""),
      mime_type: typeof candidate.mime_type === "string" ? candidate.mime_type : String(candidate.mime_type ?? ""),
      size_bytes: typeof candidate.size_bytes === "number" ? candidate.size_bytes : Number(candidate.size_bytes ?? 0),
      storage_path: typeof candidate.storage_path === "string" ? candidate.storage_path : String(candidate.storage_path ?? ""),
      text_content: typeof candidate.text_content === "string" ? candidate.text_content : String(candidate.text_content ?? ""),
      text_excerpt: typeof candidate.text_excerpt === "string" ? candidate.text_excerpt : String(candidate.text_excerpt ?? ""),
      status: typeof candidate.status === "string" ? candidate.status : String(candidate.status ?? ""),
      error: candidate.error == null ? null : String(candidate.error),
    }));
}

function normalizeLoopRegions(value: unknown): RunState["loop_regions"] {
  if (!isRecord(value)) {
    return {};
  }
  return Object.fromEntries(
    Object.entries(value).map(([iteratorNodeId, loopRegion]) => {
      const normalizedRegion = isRecord(loopRegion) ? loopRegion : {};
      const memberNodeIds = Array.isArray(normalizedRegion.member_node_ids)
        ? normalizedRegion.member_node_ids.filter((entry): entry is string => typeof entry === "string" && entry.length > 0)
        : [];
      const iterationIds = Array.isArray(normalizedRegion.iteration_ids)
        ? normalizedRegion.iteration_ids.filter((entry): entry is string => typeof entry === "string" && entry.length > 0)
        : [];
      return [
        iteratorNodeId,
        {
          ...normalizedRegion,
          iterator_node_id:
            typeof normalizedRegion.iterator_node_id === "string" && normalizedRegion.iterator_node_id.length > 0
              ? normalizedRegion.iterator_node_id
              : iteratorNodeId,
          member_node_ids: memberNodeIds,
          iteration_ids: iterationIds,
        },
      ];
    }),
  );
}

export function normalizeRuntimeEvent(event: unknown): RuntimeEvent {
  const record = isRecord(event) ? event : {};
  return {
    schema_version:
      typeof record.schema_version === "string" && record.schema_version.trim().length > 0
        ? record.schema_version
        : RUNTIME_EVENT_SCHEMA_VERSION,
    event_type: typeof record.event_type === "string" ? record.event_type : String(record.event_type ?? ""),
    summary: typeof record.summary === "string" ? record.summary : String(record.summary ?? ""),
    payload: isRecord(record.payload) ? record.payload : {},
    run_id: typeof record.run_id === "string" ? record.run_id : String(record.run_id ?? ""),
    agent_id: normalizeNullableString(record.agent_id),
    parent_run_id: normalizeNullableString(record.parent_run_id),
    timestamp: typeof record.timestamp === "string" ? record.timestamp : String(record.timestamp ?? ""),
  };
}

export function normalizeRuntimeEventHistory(events: unknown): RuntimeEvent[] {
  return Array.isArray(events) ? events.map((event) => normalizeRuntimeEvent(event)) : [];
}

export function normalizeRunState(runState: RunState | null | undefined): RunState | null {
  if (!isRecord(runState)) {
    return null;
  }
  const normalized = runState as RunState & Record<string, unknown>;
  return {
    ...normalized,
    event_history: normalizeRuntimeEventHistory(normalized.event_history),
    documents: normalizeRunDocuments(normalized.documents),
    node_statuses: isRecord(normalized.node_statuses)
      ? Object.fromEntries(Object.entries(normalized.node_statuses).map(([nodeId, status]) => [nodeId, String(status ?? "")]))
      : {},
    iterator_states: isRecord(normalized.iterator_states)
      ? Object.fromEntries(
          Object.entries(normalized.iterator_states).map(([nodeId, iteratorState]) => [
            nodeId,
            isRecord(iteratorState) ? iteratorState : {},
          ]),
        )
      : {},
    loop_regions: normalizeLoopRegions(normalized.loop_regions),
    agent_runs:
      isRecord(normalized.agent_runs)
        ? Object.fromEntries(
            Object.entries(normalized.agent_runs).map(([agentId, agentRun]) => [
              agentId,
              normalizeRunState(agentRun as RunState) ?? (agentRun as RunState),
            ]),
          )
        : normalized.agent_runs,
  };
}

export function isTerminalRuntimeEvent(event: RuntimeEvent): boolean {
  const normalizedType = event.event_type.replace(/^agent\./, "");
  return (
    normalizedType === "run.completed" ||
    normalizedType === "run.failed" ||
    normalizedType === "run.cancelled" ||
    normalizedType === "run.interrupted"
  );
}
