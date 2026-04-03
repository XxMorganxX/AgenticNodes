import type { RunState, RuntimeEvent } from "./types";

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
