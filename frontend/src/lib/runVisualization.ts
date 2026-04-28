import type {
  GraphDefinition,
  GraphDocument,
  GraphNode,
  LoopRegionState,
  RunState,
  RuntimeEvent,
  TestEnvironmentDefinition,
} from "./types";
import { buildNodeInstanceLabelMap } from "./nodeInstanceLabels";
import { latestRuntimeResolvedNodeOutputs } from "./runtimeNodeOutputs";

export type EnvironmentRunSummary = {
  runId: string | null;
  status: string;
  totalAgents: number;
  completedAgents: number;
  runningAgents: number;
  failedAgents: number;
  cancelledAgents: number;
  interruptedAgents: number;
  queuedAgents: number;
  activeAgentNames: string[];
  focusedAgentId: string | null;
  focusedAgentName: string | null;
  elapsedLabel: string;
  lastHeartbeatLabel: string;
};

export type AgentRunMilestone = {
  id: string;
  label: string;
  eventType: string;
  nodeTypeLabel: string | null;
  timestamp: string;
  timestampLabel: string;
  timestampDetail: string;
  relativeTimestampLabel: string | null;
  deltaLabel: string | null;
  tone: "idle" | "info" | "running" | "success" | "danger";
  nodeId: string | null;
  details: AgentRunMilestoneDetail[];
  dataSections: AgentRunMilestoneDataSection[];
};

export type AgentRunMilestoneDetail = {
  label: string;
  value: string;
};

export type AgentRunMilestoneDataSection = {
  label: string;
  value: unknown;
};

export type AgentRunErrorSummary = {
  id: string;
  nodeId: string | null;
  nodeLabel: string;
  errorTypeLabel: string | null;
  message: string;
  metadata: string[];
};

export type AgentRunLane = {
  agentId: string;
  agentName: string;
  status: string;
  runId: string | null;
  currentNodeId: string | null;
  currentNodeLabel: string;
  completedNodes: number;
  totalNodes: number;
  transitionCount: number;
  errorCount: number;
  errorSummaries: AgentRunErrorSummary[];
  retryCount: number;
  elapsedLabel: string;
  milestones: AgentRunMilestone[];
};

export type FocusedEventGroup = {
  id: string;
  title: string;
  subtitle: string;
  tone: "info" | "running" | "success" | "danger";
  eventCount: number;
  startedAt: string | null;
  endedAt: string | null;
  nodeId: string | null;
  lines: string[];
};

export type FocusedRunSummary = {
  runId: string | null;
  status: string;
  currentNodeId: string | null;
  currentNodeLabel: string;
  completedNodes: number;
  totalNodes: number;
  transitionCount: number;
  errorCount: number;
  retryCount: number;
  elapsedLabel: string;
  lastHeartbeatLabel: string;
  finalOutput: unknown;
  nodeErrors: Record<string, unknown>;
};

export type FocusedRunNodeState = {
  nodeId: string;
  isActive: boolean;
  wasVisited: boolean;
  hasError: boolean;
  latestOutput: unknown;
  latestError: unknown;
  visitCount: number;
};

export type FocusedLoopRegion = {
  id: string;
  iteratorNodeId: string;
  iteratorNodeLabel: string;
  iteratorType: string | null;
  status: string | null;
  currentRowIndex: number | null;
  spreadsheetRowNumber: number | null;
  totalRows: number | null;
  activeIterationId: string | null;
  memberNodeIds: string[];
  iterationIds: string[];
  sheetName: string | null;
  sourceFile: string | null;
  fileFormat: string | null;
};

export type FocusedRunProjection = {
  normalizedEvents: RuntimeEvent[];
  completedNodeIds: Set<string>;
  nodeStates: Record<string, FocusedRunNodeState>;
  loopRegions: FocusedLoopRegion[];
  errorSummaries: AgentRunErrorSummary[];
  runSummary: FocusedRunSummary;
  eventGroups: FocusedEventGroup[];
};

export function formatRunStatusLabel(status: string | null | undefined): string {
  const normalized = (status ?? "idle").trim().toLowerCase();
  if (normalized === "idle") {
    return "Not started";
  }
  return normalized
    .split(/[\s_-]+/)
    .filter(Boolean)
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ");
}

function normalizeEventType(eventType: string): string {
  return eventType.replace(/^agent\./, "");
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function cleanInlineText(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

function truncateText(value: string, limit = 220): string {
  return value.length > limit ? `${value.slice(0, limit - 1)}...` : value;
}

function firstNonEmptyString(...values: unknown[]): string | null {
  for (const value of values) {
    if (typeof value === "string") {
      const cleaned = cleanInlineText(value);
      if (cleaned.length > 0) {
        return cleaned;
      }
    }
  }
  return null;
}

function formatIdentifierLabel(value: string): string {
  const ACRONYMS = new Set(["api", "http", "https", "id", "json", "mcp", "sse", "stderr", "stdout", "ui", "url"]);
  return value
    .split(/[.\s_-]+/)
    .filter(Boolean)
    .map((word) => {
      const normalized = word.toLowerCase();
      if (ACRONYMS.has(normalized)) {
        return normalized.toUpperCase();
      }
      return normalized.charAt(0).toUpperCase() + normalized.slice(1);
    })
    .join(" ");
}

function stringifyCompactValue(value: unknown): string {
  if (typeof value === "string") {
    return value;
  }
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function parseJsonRecord(candidate: string): Record<string, unknown> | null {
  try {
    const parsed = JSON.parse(candidate);
    return isRecord(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

function extractProviderPayloadFromErrorMessage(message: unknown): Record<string, unknown> | null {
  if (typeof message !== "string" || message.trim().length === 0) {
    return null;
  }
  const direct = parseJsonRecord(message.trim());
  if (direct) {
    return direct;
  }
  const start = message.indexOf("{");
  const end = message.lastIndexOf("}");
  if (start === -1 || end <= start) {
    return null;
  }
  return parseJsonRecord(message.slice(start, end + 1));
}

function normalizeProviderIterations(value: unknown): Record<string, unknown>[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.map((entry, index) => {
    if (isRecord(entry)) {
      return {
        turn: index + 1,
        ...entry,
      };
    }
    return {
      turn: index + 1,
      value: entry,
    };
  });
}

function buildProviderTurnTranscript(providerPayload: Record<string, unknown>): Record<string, unknown> | null {
  const usage = isRecord(providerPayload.usage) ? providerPayload.usage : null;
  const iterations = normalizeProviderIterations(usage?.iterations);
  const errors = Array.isArray(providerPayload.errors) ? providerPayload.errors : [];
  const rawNumTurns = providerPayload.num_turns;
  const numTurns =
    typeof rawNumTurns === "number" && Number.isFinite(rawNumTurns)
      ? rawNumTurns
      : iterations.length > 0
        ? iterations.length
        : null;
  if (iterations.length === 0 && errors.length === 0 && numTurns == null) {
    return null;
  }
  const transcript: Record<string, unknown> = {};
  if (typeof providerPayload.session_id === "string" && providerPayload.session_id.trim().length > 0) {
    transcript.session_id = providerPayload.session_id.trim();
  }
  if (typeof providerPayload.stop_reason === "string" && providerPayload.stop_reason.trim().length > 0) {
    transcript.stop_reason = providerPayload.stop_reason.trim();
  }
  if (typeof providerPayload.terminal_reason === "string" && providerPayload.terminal_reason.trim().length > 0) {
    transcript.terminal_reason = providerPayload.terminal_reason.trim();
  }
  if (numTurns != null) {
    transcript.num_turns = numTurns;
  }
  if (errors.length > 0) {
    transcript.errors = errors;
  }
  if (iterations.length > 0) {
    transcript.iterations = iterations;
  }
  return transcript;
}

function providerTurnTranscriptFromPayload(payload: Record<string, unknown>): Record<string, unknown> | null {
  const metadata = isRecord(payload.metadata) ? payload.metadata : null;
  if (metadata) {
    const metadataTranscript = buildProviderTurnTranscript(metadata);
    if (metadataTranscript) {
      return metadataTranscript;
    }
  }

  const output = isRecord(payload.output) ? payload.output : null;
  const outputMetadata = output && isRecord(output.metadata) ? output.metadata : null;
  if (outputMetadata) {
    const outputTranscript = buildProviderTurnTranscript(outputMetadata);
    if (outputTranscript) {
      return outputTranscript;
    }
  }

  const error = isRecord(payload.error) ? payload.error : null;
  if (!error) {
    return null;
  }
  const parsedErrorPayload =
    extractProviderPayloadFromErrorMessage(error.message) ??
    extractProviderPayloadFromErrorMessage(error.detail) ??
    extractProviderPayloadFromErrorMessage(error.stderr) ??
    extractProviderPayloadFromErrorMessage(error.stdout);
  return parsedErrorPayload ? buildProviderTurnTranscript(parsedErrorPayload) : null;
}

function extractErrorMessage(value: unknown): string | null {
  if (typeof value === "string") {
    return firstNonEmptyString(value);
  }
  if (Array.isArray(value)) {
    return value.map((entry) => extractErrorMessage(entry)).find((entry): entry is string => Boolean(entry)) ?? null;
  }
  if (isRecord(value)) {
    return (
      firstNonEmptyString(value.message, value.detail, value.reason, value.stderr, value.stdout, value.summary) ??
      extractErrorMessage(value.error) ??
      extractErrorMessage(value.errors)
    );
  }
  return null;
}

function buildErrorMetadata(error: Record<string, unknown>): string[] {
  const metadata = [
    typeof error.tool_name === "string" ? `Tool ${error.tool_name}` : null,
    typeof error.result_status === "string" ? `Status ${formatIdentifierLabel(error.result_status)}` : null,
  ].filter((entry): entry is string => Boolean(entry));
  return [...new Set(metadata)];
}

function summarizeErrorEntry(nodeId: string, nodeLabel: string, value: unknown): AgentRunErrorSummary {
  if (isRecord(value)) {
    const message =
      extractErrorMessage(value) ?? truncateText(cleanInlineText(stringifyCompactValue(value)), 220);
    return {
      id: `${nodeId}-${message}`,
      nodeId,
      nodeLabel,
      errorTypeLabel: typeof value.type === "string" ? formatIdentifierLabel(value.type) : null,
      message,
      metadata: buildErrorMetadata(value),
    };
  }
  const message = extractErrorMessage(value) ?? truncateText(cleanInlineText(stringifyCompactValue(value)), 220);
  return {
    id: `${nodeId}-${message}`,
    nodeId,
    nodeLabel,
    errorTypeLabel: null,
    message,
    metadata: [],
  };
}

function summarizeNodeErrors(nodeErrors: Record<string, unknown>, labels: Map<string, string>): AgentRunErrorSummary[] {
  const seen = new Set<string>();
  return Object.entries(nodeErrors).flatMap(([nodeId, value]) => {
    const nodeLabel = labels.get(nodeId) ?? nodeId;
    const entries = Array.isArray(value) ? value : [value];
    return entries.flatMap((entry, index) => {
      if (entry == null) {
        return [];
      }
      const summary = summarizeErrorEntry(nodeId, nodeLabel, entry);
      const key = `${nodeId}:${summary.errorTypeLabel ?? "none"}:${summary.message}`;
      if (seen.has(key)) {
        return [];
      }
      seen.add(key);
      return [{ ...summary, id: `${nodeId}-${index}-${summary.message}` }];
    });
  });
}

function nodeLabelMap(graph: GraphDefinition | null): Map<string, string> {
  return buildNodeInstanceLabelMap(graph);
}

function completedNodeIds(events: RuntimeEvent[]): Set<string> {
  return new Set(
    events
      .filter((event) => normalizeEventType(event.event_type) === "node.completed")
      .map((event) => (typeof event.payload.node_id === "string" ? event.payload.node_id : ""))
      .filter((nodeId) => nodeId.length > 0),
  );
}

function hasOwnRecordValue(record: Record<string, unknown> | null | undefined, key: string): boolean {
  return Boolean(record && Object.prototype.hasOwnProperty.call(record, key));
}

function appendUniqueString(values: string[], candidate: unknown): string[] {
  if (typeof candidate !== "string" || candidate.length === 0 || values.includes(candidate)) {
    return values;
  }
  return [...values, candidate];
}

function buildIterationId(iteratorNodeId: unknown, iteratorRowIndex: unknown): string | null {
  if (typeof iteratorNodeId !== "string" || iteratorNodeId.length === 0) {
    return null;
  }
  if (typeof iteratorRowIndex !== "number" || !Number.isInteger(iteratorRowIndex) || iteratorRowIndex <= 0) {
    return null;
  }
  return `${iteratorNodeId}:row:${iteratorRowIndex}`;
}

function extractSpreadsheetRowNumber(value: unknown): number | null {
  if (!isRecord(value)) {
    return null;
  }
  if (typeof value.row_number === "number" && Number.isInteger(value.row_number) && value.row_number > 0) {
    return value.row_number;
  }
  if (isRecord(value.payload)) {
    return extractSpreadsheetRowNumber(value.payload);
  }
  return null;
}

function createFocusedLoopRegion(iteratorNodeId: string, labels: Map<string, string>): FocusedLoopRegion {
  return {
    id: iteratorNodeId,
    iteratorNodeId,
    iteratorNodeLabel: labels.get(iteratorNodeId) ?? iteratorNodeId,
    iteratorType: null,
    status: null,
    currentRowIndex: null,
    spreadsheetRowNumber: null,
    totalRows: null,
    activeIterationId: null,
    memberNodeIds: [],
    iterationIds: [],
    sheetName: null,
    sourceFile: null,
    fileFormat: null,
  };
}

function upsertFocusedLoopRegion(
  regions: Map<string, FocusedLoopRegion>,
  iteratorNodeId: string,
  labels: Map<string, string>,
): FocusedLoopRegion {
  const existing = regions.get(iteratorNodeId);
  if (existing) {
    return existing;
  }
  const nextRegion = createFocusedLoopRegion(iteratorNodeId, labels);
  regions.set(iteratorNodeId, nextRegion);
  return nextRegion;
}

function mergeLoopRegionState(
  region: FocusedLoopRegion,
  iteratorNodeId: string,
  loopRegion: LoopRegionState | Record<string, unknown>,
): FocusedLoopRegion {
  const memberNodeIds = Array.isArray(loopRegion.member_node_ids)
    ? loopRegion.member_node_ids.filter((entry): entry is string => typeof entry === "string" && entry.length > 0)
    : [];
  const iterationIds = Array.isArray(loopRegion.iteration_ids)
    ? loopRegion.iteration_ids.filter((entry): entry is string => typeof entry === "string" && entry.length > 0)
    : [];
  return {
    ...region,
    id: iteratorNodeId,
    iteratorNodeId,
    iteratorType: typeof loopRegion.iterator_type === "string" ? loopRegion.iterator_type : region.iteratorType,
    status: typeof loopRegion.status === "string" ? loopRegion.status : region.status,
    currentRowIndex: typeof loopRegion.current_row_index === "number" ? loopRegion.current_row_index : region.currentRowIndex,
    totalRows: typeof loopRegion.total_rows === "number" ? loopRegion.total_rows : region.totalRows,
    activeIterationId: typeof loopRegion.active_iteration_id === "string" ? loopRegion.active_iteration_id : region.activeIterationId,
    memberNodeIds: Array.from(new Set([...region.memberNodeIds, ...memberNodeIds])),
    iterationIds: Array.from(new Set([...region.iterationIds, ...iterationIds])),
    sheetName: typeof loopRegion.sheet_name === "string" ? loopRegion.sheet_name : region.sheetName,
    sourceFile: typeof loopRegion.source_file === "string" ? loopRegion.source_file : region.sourceFile,
    fileFormat: typeof loopRegion.file_format === "string" ? loopRegion.file_format : region.fileFormat,
  };
}

function buildFocusedLoopRegions(
  graph: GraphDefinition | null,
  runState: RunState | null,
  normalizedEvents: RuntimeEvent[],
): FocusedLoopRegion[] {
  const labels = nodeLabelMap(graph);
  const regions = new Map<string, FocusedLoopRegion>();
  for (const event of normalizedEvents) {
    const eventType = event.event_type;
    const iteratorNodeId = typeof event.payload.iterator_node_id === "string" ? event.payload.iterator_node_id : null;
    if (!iteratorNodeId) {
      continue;
    }
    let region = upsertFocusedLoopRegion(regions, iteratorNodeId, labels);
    if (eventType === "node.started" || eventType === "node.completed") {
      const nodeId = typeof event.payload.node_id === "string" ? event.payload.node_id : null;
      const iterationId = typeof event.payload.iteration_id === "string"
        ? event.payload.iteration_id
        : buildIterationId(iteratorNodeId, event.payload.iterator_row_index);
      region = {
        ...region,
        memberNodeIds:
          nodeId && nodeId !== iteratorNodeId ? appendUniqueString(region.memberNodeIds, nodeId) : region.memberNodeIds,
        iterationIds: appendUniqueString(region.iterationIds, iterationId),
        currentRowIndex:
          typeof event.payload.iterator_row_index === "number" ? event.payload.iterator_row_index : region.currentRowIndex,
        spreadsheetRowNumber:
          extractSpreadsheetRowNumber(event.payload.received_input) ??
          extractSpreadsheetRowNumber(event.payload.output) ??
          region.spreadsheetRowNumber,
        totalRows:
          typeof event.payload.iterator_total_rows === "number" ? event.payload.iterator_total_rows : region.totalRows,
        activeIterationId: iterationId ?? region.activeIterationId,
      };
      regions.set(iteratorNodeId, region);
      continue;
    }
    if (eventType === "node.iterator.updated") {
      region = {
        ...region,
        iteratorType: typeof event.payload.iterator_type === "string" ? event.payload.iterator_type : region.iteratorType,
        status: typeof event.payload.status === "string" ? event.payload.status : region.status,
        currentRowIndex:
          typeof event.payload.current_row_index === "number" ? event.payload.current_row_index : region.currentRowIndex,
        totalRows: typeof event.payload.total_rows === "number" ? event.payload.total_rows : region.totalRows,
        activeIterationId:
          typeof event.payload.iteration_id === "string"
            ? event.payload.iteration_id
            : buildIterationId(iteratorNodeId, event.payload.current_row_index) ?? region.activeIterationId,
        sheetName: typeof event.payload.sheet_name === "string" ? event.payload.sheet_name : region.sheetName,
        sourceFile: typeof event.payload.source_file === "string" ? event.payload.source_file : region.sourceFile,
        fileFormat: typeof event.payload.file_format === "string" ? event.payload.file_format : region.fileFormat,
      };
      regions.set(iteratorNodeId, region);
    }
  }

  Object.entries(runState?.loop_regions ?? {}).forEach(([iteratorNodeId, loopRegion]) => {
    const region = upsertFocusedLoopRegion(regions, iteratorNodeId, labels);
    const mergedRegion = mergeLoopRegionState(region, iteratorNodeId, loopRegion);
    regions.set(
      iteratorNodeId,
      mergedRegion.iteratorType === "spreadsheet_rows"
        ? {
            ...mergedRegion,
            spreadsheetRowNumber:
              mergedRegion.spreadsheetRowNumber
              ?? extractSpreadsheetRowNumber(runState?.node_outputs?.[iteratorNodeId]),
          }
        : mergedRegion,
    );
  });

  return Array.from(regions.values()).filter((region) => region.memberNodeIds.length > 0);
}

function graphByAgent(environment: TestEnvironmentDefinition): Map<string, GraphDefinition> {
  const mergeEnvVars = (parentEnvVars: Record<string, string> | undefined, childEnvVars: Record<string, string> | undefined): Record<string, string> => {
    const merged: Record<string, string> = { ...(parentEnvVars ?? {}) };
    for (const [key, rawValue] of Object.entries(childEnvVars ?? {})) {
      const value = String(rawValue ?? "");
      const normalizedKey = String(key ?? "").trim();
      const isPlaceholder = normalizedKey.length > 0 && value.trim() === normalizedKey;
      const parentValue = String(merged[key] ?? "").trim();
      if (isPlaceholder && parentValue && parentValue !== normalizedKey) {
        continue;
      }
      merged[key] = value;
    }
    return merged;
  };
  return new Map(
    environment.agents.map((agent) => [
      agent.agent_id,
      {
        graph_id: environment.graph_id,
        name: agent.name,
        description: agent.description,
        version: agent.version,
        graph_type: "graph",
        start_node_id: agent.start_node_id,
        env_vars: mergeEnvVars(environment.env_vars, agent.env_vars),
        nodes: agent.nodes,
        edges: agent.edges,
        node_providers: environment.node_providers,
      },
    ]),
  );
}

function formatElapsed(startedAt: string | null | undefined, endedAt: string | null | undefined): string {
  if (!startedAt) {
    return "Not started";
  }
  const startMs = Date.parse(startedAt);
  const endMs = endedAt ? Date.parse(endedAt) : Date.now();
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) {
    return "n/a";
  }
  const durationMs = Math.max(0, endMs - startMs);
  if (durationMs < 1000) {
    return "<1s";
  }
  const seconds = Math.round(durationMs / 1000);
  if (seconds < 60) {
    return `${seconds}s`;
  }
  const minutes = Math.floor(seconds / 60);
  const remainingSeconds = seconds % 60;
  return remainingSeconds > 0 ? `${minutes}m ${remainingSeconds}s` : `${minutes}m`;
}

function formatHeartbeat(timestamp: string | null | undefined): string {
  if (!timestamp) {
    return "n/a";
  }
  return formatTimestamp(timestamp, true);
}

function formatDurationMs(durationMs: number): string {
  if (!Number.isFinite(durationMs) || durationMs < 0) {
    return "n/a";
  }
  if (durationMs < 1000) {
    return `${Math.round(durationMs)}ms`;
  }
  if (durationMs < 10_000) {
    return `${(durationMs / 1000).toFixed(1)}s`;
  }
  const seconds = Math.round(durationMs / 1000);
  if (seconds < 60) {
    return `${seconds}s`;
  }
  const minutes = Math.floor(seconds / 60);
  const remainingSeconds = seconds % 60;
  return remainingSeconds > 0 ? `${minutes}m ${remainingSeconds}s` : `${minutes}m`;
}

function formatTimestamp(timestamp: string, detail = false): string {
  const parsed = Date.parse(timestamp);
  if (!Number.isFinite(parsed)) {
    return timestamp;
  }
  const date = new Date(parsed);
  const dateLabel = detail ? `${date.toLocaleDateString(undefined, { month: "short", day: "numeric" })} ` : "";
  const timeLabel = date.toLocaleTimeString(undefined, {
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit",
  });
  return `${dateLabel}${timeLabel}.${String(date.getMilliseconds()).padStart(3, "0")}`;
}

function formatRelativeTimestamp(timestamp: string, startedAt: string | null | undefined): string | null {
  if (!startedAt) {
    return null;
  }
  const timestampMs = Date.parse(timestamp);
  const startMs = Date.parse(startedAt);
  if (!Number.isFinite(timestampMs) || !Number.isFinite(startMs)) {
    return null;
  }
  return `T+${formatDurationMs(Math.max(0, timestampMs - startMs))}`;
}

function formatDeltaLabel(timestamp: string, previousTimestamp: string | null): string | null {
  if (!previousTimestamp) {
    return null;
  }
  const timestampMs = Date.parse(timestamp);
  const previousMs = Date.parse(previousTimestamp);
  if (!Number.isFinite(timestampMs) || !Number.isFinite(previousMs)) {
    return null;
  }
  return `+${formatDurationMs(Math.max(0, timestampMs - previousMs))}`;
}

function nodeIdFromEvent(event: RuntimeEvent): string | null {
  const payloadNodeId = event.payload.node_id;
  return typeof payloadNodeId === "string" && payloadNodeId.length > 0 ? payloadNodeId : null;
}

function sessionIdFromEventPayload(payload: Record<string, unknown>): string | null {
  if (typeof payload.session_id === "string" && payload.session_id.trim().length > 0) {
    return payload.session_id.trim();
  }
  const metadata = isRecord(payload.metadata) ? payload.metadata : null;
  if (metadata && typeof metadata.session_id === "string" && metadata.session_id.trim().length > 0) {
    return metadata.session_id.trim();
  }
  const output = isRecord(payload.output) ? payload.output : null;
  const outputMetadata = output && isRecord(output.metadata) ? output.metadata : null;
  if (outputMetadata && typeof outputMetadata.session_id === "string" && outputMetadata.session_id.trim().length > 0) {
    return outputMetadata.session_id.trim();
  }
  return null;
}

function nodeFromEvent(event: RuntimeEvent, graph: GraphDefinition | null): GraphNode | null {
  const nodeId = nodeIdFromEvent(event);
  if (nodeId) {
    return nodeById(graph, nodeId);
  }
  if (event.event_type === "edge.selected") {
    const targetId = typeof event.payload.target_id === "string" && event.payload.target_id.length > 0 ? event.payload.target_id : null;
    if (targetId) {
      return nodeById(graph, targetId);
    }
    const sourceId = typeof event.payload.source_id === "string" && event.payload.source_id.length > 0 ? event.payload.source_id : null;
    if (sourceId) {
      return nodeById(graph, sourceId);
    }
  }
  return null;
}

function milestoneNodeTypeLabel(event: RuntimeEvent, graph: GraphDefinition | null): string | null {
  if (typeof event.payload.node_provider_label === "string" && event.payload.node_provider_label.trim().length > 0) {
    return event.payload.node_provider_label.trim();
  }
  const node = nodeFromEvent(event, graph);
  const providerLabel = typeof node?.provider_label === "string" ? node.provider_label.trim() : "";
  if (providerLabel.length > 0) {
    return providerLabel;
  }
  return null;
}

function incomingSourceNodeLabels(nodeId: string | null, graph: GraphDefinition | null, labels: Map<string, string>): string[] {
  if (!nodeId || !graph) {
    return [];
  }
  const sourceNodeIds = graph.edges
    .filter((edge) => edge.target_id === nodeId)
    .map((edge) => edge.source_id)
    .filter((sourceId, index, values) => values.indexOf(sourceId) === index);
  return sourceNodeIds.map((sourceId) => {
    const sourceLabel = labels.get(sourceId) ?? sourceId;
    return sourceLabel === sourceId ? sourceId : `${sourceLabel} (${sourceId})`;
  });
}

function hasNoOutgoingExecutionEdges(nodeId: string | null, graph: GraphDefinition | null): boolean {
  if (!nodeId || !graph) {
    return false;
  }
  return !graph.edges.some((edge) => edge.source_id === nodeId && edge.kind !== "binding");
}

function eventTone(eventType: string): AgentRunMilestone["tone"] {
  if (eventType === "run.completed" || eventType === "node.completed") {
    return "success";
  }
  if (eventType === "run.failed" || eventType === "run.cancelled" || eventType === "run.interrupted") {
    return "danger";
  }
  if (eventType === "run.started" || eventType === "node.started" || eventType === "retry.triggered") {
    return "running";
  }
  return "info";
}

function milestoneLabel(event: RuntimeEvent, graph: GraphDefinition | null): string {
  const eventType = normalizeEventType(event.event_type);
  const labels = nodeLabelMap(graph);
  const nodeId = nodeIdFromEvent(event);
  const nodeLabel = nodeId ? (labels.get(nodeId) ?? nodeId) : null;
  if (eventType === "run.started") {
    return "Run started";
  }
  if (eventType === "run.completed") {
    return "Run completed";
  }
  if (eventType === "run.failed") {
    return "Run failed";
  }
  if (eventType === "run.cancelled") {
    return "Run cancelled";
  }
  if (eventType === "run.interrupted") {
    return "Run interrupted";
  }
  if (eventType === "node.started") {
    return nodeLabel ? `${nodeLabel} started` : "Node started";
  }
  if (eventType === "node.completed") {
    return nodeLabel ? `${nodeLabel} completed` : "Node completed";
  }
  if (eventType === "retry.triggered") {
    return nodeLabel ? `Retry from ${nodeLabel}` : "Retry triggered";
  }
  if (eventType === "edge.selected") {
    return "Transition";
  }
  return event.summary;
}

function formatEventTypeLabel(eventType: string): string {
  return eventType
    .split(".")
    .flatMap((segment) => segment.split(/[\s_-]+/))
    .filter(Boolean)
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ");
}

function nodeInputPreview(
  nodeId: string | null,
  graph: GraphDefinition | null,
  inputPayload: unknown,
  knownNodeOutputs: Record<string, unknown>,
  labels: Map<string, string>,
): unknown {
  if (!nodeId) {
    return undefined;
  }
  const node = graph?.nodes.find((candidate) => candidate.id === nodeId) ?? null;
  if (node?.kind === "input") {
    return inputPayload;
  }
  const incomingEdges = (graph?.edges ?? []).filter((edge) => edge.target_id === nodeId);
  const sources = incomingEdges
    .filter((edge, index, edges) => edges.findIndex((candidate) => candidate.source_id === edge.source_id) === index)
    .map((edge) => ({
      sourceLabel: labels.get(edge.source_id) ?? edge.source_id,
      value: knownNodeOutputs[edge.source_id],
    }))
    .filter((entry) => entry.value !== undefined);
  if (sources.length === 0) {
    return undefined;
  }
  if (sources.length === 1) {
    return {
      source: sources[0].sourceLabel,
      value: sources[0].value,
    };
  }
  return Object.fromEntries(sources.map((entry) => [entry.sourceLabel, entry.value]));
}

function appendSection(
  sections: AgentRunMilestoneDataSection[],
  label: string,
  value: unknown,
  options: { allowNull?: boolean } = {},
): void {
  if (value === undefined) {
    return;
  }
  if (value === null && !options.allowNull) {
    return;
  }
  sections.push({ label, value });
}

function appendOutputSections(sections: AgentRunMilestoneDataSection[], output: unknown): void {
  if (!isRecord(output)) {
    appendSection(sections, "Output", output, { allowNull: true });
    return;
  }
  let added = false;
  if ("payload" in output && output.payload !== undefined) {
    sections.push({ label: "Output payload", value: output.payload });
    added = true;
  }
  if (Array.isArray(output.tool_calls) && output.tool_calls.length > 0) {
    sections.push({ label: "Tool calls", value: output.tool_calls });
    added = true;
  }
  if (Array.isArray(output.errors) && output.errors.length > 0) {
    sections.push({ label: "Output errors", value: output.errors });
    added = true;
  }
  if (isRecord(output.artifacts) && "display_envelope" in output.artifacts) {
    sections.push({ label: "Display envelope", value: output.artifacts.display_envelope });
    added = true;
  } else if ("artifacts" in output && output.artifacts !== undefined && !isRecord(output.artifacts)) {
    sections.push({ label: "Output artifacts", value: output.artifacts });
    added = true;
  }
  if ("metadata" in output && output.metadata !== undefined) {
    sections.push({ label: "Output metadata", value: output.metadata });
    added = true;
  }
  if (!added) {
    sections.push({ label: "Output", value: output });
  }
}

function buildMilestoneDetails(
  event: RuntimeEvent,
  graph: GraphDefinition | null,
  labels: Map<string, string>,
): AgentRunMilestoneDetail[] {
  const payload = event.payload;
  const nodeId = nodeIdFromEvent(event);
  const executionMetadata = isRecord(payload.metadata) ? payload.metadata : null;
  const details: AgentRunMilestoneDetail[] = [{ label: "State", value: formatEventTypeLabel(event.event_type) }];
  const node = nodeId ? (graph?.nodes.find((candidate) => candidate.id === nodeId) ?? null) : null;
  if (nodeId) {
    details.push({ label: "Node", value: labels.get(nodeId) ?? nodeId });
  }
  if (typeof payload.node_kind === "string") {
    details.push({ label: "Kind", value: payload.node_kind });
  }
  if (typeof payload.node_category === "string") {
    details.push({ label: "Category", value: payload.node_category });
  }
  if (typeof payload.node_provider_label === "string") {
    details.push({ label: "Provider", value: payload.node_provider_label });
  }
  if (typeof executionMetadata?.requested_model === "string" && executionMetadata.requested_model.trim().length > 0) {
    details.push({ label: "Requested model", value: executionMetadata.requested_model.trim() });
  }
  if (typeof executionMetadata?.reported_model === "string" && executionMetadata.reported_model.trim().length > 0) {
    details.push({ label: "Reported model", value: executionMetadata.reported_model.trim() });
  } else if (typeof executionMetadata?.vendor_model === "string" && executionMetadata.vendor_model.trim().length > 0) {
    details.push({ label: "Model", value: executionMetadata.vendor_model.trim() });
  }
  if (typeof payload.status === "string") {
    details.push({ label: "Status", value: payload.status });
  }
  if (typeof payload.iteration_id === "string" && payload.iteration_id.trim().length > 0) {
    details.push({ label: "Iteration", value: payload.iteration_id.trim() });
  }
  const sessionId = sessionIdFromEventPayload(payload);
  if (sessionId) {
    details.push({ label: "Session", value: sessionId });
  }
  const providerTranscript = providerTurnTranscriptFromPayload(payload);
  if (providerTranscript && typeof providerTranscript.num_turns === "number") {
    details.push({ label: "Turns", value: String(providerTranscript.num_turns) });
  }
  if (providerTranscript && typeof providerTranscript.stop_reason === "string") {
    details.push({ label: "Stop reason", value: providerTranscript.stop_reason });
  }
  if (typeof payload.visit_count === "number") {
    details.push({ label: "Visit", value: `#${payload.visit_count}` });
  }
  if (event.event_type === "node.started") {
    const sourceLabels = incomingSourceNodeLabels(nodeId, graph, labels);
    if (sourceLabels.length === 1) {
      details.push({ label: "Input source", value: sourceLabels[0] });
    } else if (sourceLabels.length > 1) {
      details.push({ label: "Input sources", value: sourceLabels.join(", ") });
    }
  }
  if (typeof payload.source_id === "string" && typeof payload.target_id === "string") {
    const sourceLabel = labels.get(payload.source_id) ?? payload.source_id;
    const targetLabel = labels.get(payload.target_id) ?? payload.target_id;
    details.push({ label: "Route", value: `${sourceLabel} -> ${targetLabel}` });
  }
  if (typeof payload.matched === "boolean") {
    details.push({ label: "Matched", value: payload.matched ? "yes" : "no" });
  }
  if (typeof payload.result_status === "string") {
    details.push({ label: "Result", value: payload.result_status });
  }
  if (typeof payload.agent_name === "string") {
    details.push({ label: "Agent", value: payload.agent_name });
  }
  if (
    event.event_type === "node.completed" &&
    node?.provider_id === "core.data_display" &&
    hasNoOutgoingExecutionEdges(nodeId, graph)
  ) {
    details.push({ label: "Routing", value: "No outgoing execution edge (display only)" });
  }
  return details;
}

function buildMilestoneDataSections(
  event: RuntimeEvent,
  graph: GraphDefinition | null,
  inputPayload: unknown,
  knownNodeOutputs: Record<string, unknown>,
  labels: Map<string, string>,
): AgentRunMilestoneDataSection[] {
  const payload = event.payload;
  const nodeId = nodeIdFromEvent(event);
  const sections: AgentRunMilestoneDataSection[] = [];
  if (event.event_type === "run.started") {
    appendSection(sections, "Input", inputPayload, { allowNull: true });
    return sections;
  }
  if (event.event_type === "node.started") {
    if ("received_input" in payload) {
      appendSection(sections, "Received input", payload.received_input, { allowNull: true });
    } else {
      appendSection(sections, "Received input", nodeInputPreview(nodeId, graph, inputPayload, knownNodeOutputs, labels), {
        allowNull: true,
      });
    }
    return sections;
  }
  if (event.event_type === "node.completed") {
    if ("output" in payload) {
      appendOutputSections(sections, payload.output);
    }
    if ("route_outputs" in payload) {
      appendSection(sections, "Route outputs", payload.route_outputs, { allowNull: true });
    }
    if ("error" in payload) {
      appendSection(sections, "Error", payload.error, { allowNull: true });
    }
    if ("metadata" in payload) {
      appendSection(sections, "Execution metadata", payload.metadata);
    }
    const providerTranscript = providerTurnTranscriptFromPayload(payload);
    if (providerTranscript) {
      appendSection(sections, "Provider turn transcript", providerTranscript);
    }
    return sections;
  }
  if (event.event_type === "run.completed") {
    appendSection(sections, "Final output", payload.final_output, { allowNull: true });
    return sections;
  }
  if (event.event_type === "run.failed") {
    appendSection(sections, "Failure", payload.error, { allowNull: true });
    const providerTranscript = providerTurnTranscriptFromPayload(payload);
    if (providerTranscript) {
      appendSection(sections, "Provider turn transcript", providerTranscript);
    }
    if ("final_output" in payload) {
      appendSection(sections, "Final output", payload.final_output, { allowNull: true });
    }
    return sections;
  }
  if (event.event_type === "run.cancelled") {
    appendSection(sections, "Cancellation", payload.error, { allowNull: true });
    const providerTranscript = providerTurnTranscriptFromPayload(payload);
    if (providerTranscript) {
      appendSection(sections, "Provider turn transcript", providerTranscript);
    }
    if ("final_output" in payload) {
      appendSection(sections, "Final output", payload.final_output, { allowNull: true });
    }
    return sections;
  }
  if (event.event_type === "run.interrupted") {
    appendSection(sections, "Interruption", payload.error, { allowNull: true });
    const providerTranscript = providerTurnTranscriptFromPayload(payload);
    if (providerTranscript) {
      appendSection(sections, "Provider turn transcript", providerTranscript);
    }
    return sections;
  }
  if (event.event_type === "condition.evaluated") {
    appendSection(sections, "Condition payload", payload);
    return sections;
  }
  if (event.event_type === "retry.triggered") {
    appendSection(sections, "Retry context", payload);
    return sections;
  }
  if (event.event_type === "edge.selected") {
    appendSection(sections, "Transition payload", payload);
    return sections;
  }
  return sections;
}

function isEnvironmentGraph(graph: GraphDocument | null): graph is TestEnvironmentDefinition {
  return Boolean(graph && "agents" in graph && Array.isArray(graph.agents));
}

function focusedGraphName(graph: GraphDocument | null, selectedAgentId: string | null): string | null {
  if (!isEnvironmentGraph(graph) || !selectedAgentId) {
    return null;
  }
  return graph.agents.find((agent) => agent.agent_id === selectedAgentId)?.name ?? null;
}

export function buildEnvironmentRunSummary(
  graph: GraphDocument | null,
  runState: RunState | null,
  selectedAgentId: string | null,
): EnvironmentRunSummary | null {
  if (!graph || !isEnvironmentGraph(graph)) {
    return null;
  }
  const agentStates = Object.values(runState?.agent_runs ?? {});
  const totalAgents = runState?.agent_runs ? agentStates.length : graph.agents.length;
  const runningAgents = agentStates.filter((state) => state.status === "running").length;
  const completedAgents = agentStates.filter((state) => state.status === "completed").length;
  const failedAgents = agentStates.filter((state) => state.status === "failed").length;
  const cancelledAgents = agentStates.filter((state) => state.status === "cancelled").length;
  const interruptedAgents = agentStates.filter((state) => state.status === "interrupted").length;
  const queuedAgents = Math.max(
    0,
    totalAgents - runningAgents - completedAgents - failedAgents - cancelledAgents - interruptedAgents,
  );
  return {
    runId: runState?.run_id ?? null,
    status: runState?.status ?? "idle",
    totalAgents,
    completedAgents,
    runningAgents,
    failedAgents,
    cancelledAgents,
    interruptedAgents,
    queuedAgents,
    activeAgentNames: graph.agents
      .filter((agent) => runState?.agent_runs?.[agent.agent_id]?.status === "running")
      .map((agent) => agent.name),
    focusedAgentId: selectedAgentId,
    focusedAgentName: focusedGraphName(graph, selectedAgentId),
    elapsedLabel: formatElapsed(runState?.started_at, runState?.ended_at),
    lastHeartbeatLabel: formatHeartbeat(runState?.last_heartbeat_at),
  };
}

export function buildAgentRunLanes(
  graph: GraphDocument | null,
  runState: RunState | null,
  events: RuntimeEvent[],
): AgentRunLane[] {
  if (!graph || !isEnvironmentGraph(graph)) {
    return [];
  }
  const graphsByAgent = graphByAgent(graph);
  return graph.agents.map((agent) => {
    const agentState = runState?.agent_runs?.[agent.agent_id] ?? null;
    const currentGraph = graphsByAgent.get(agent.agent_id) ?? null;
    const labels = nodeLabelMap(currentGraph);
    const agentEvents = events
      .filter((event) => event.agent_id === agent.agent_id)
      .map((event) => ({ ...event, event_type: normalizeEventType(event.event_type) }));
    const projection = buildFocusedRunProjection(currentGraph, agentState, agentEvents);
    const errorSummaries = projection.errorSummaries;
    const knownNodeOutputs: Record<string, unknown> = {};
    let previousTimestamp: string | null = null;
    return {
      agentId: agent.agent_id,
      agentName: agent.name,
      status: projection.runSummary.status,
      runId: projection.runSummary.runId,
      currentNodeId: projection.runSummary.currentNodeId,
      currentNodeLabel: projection.runSummary.currentNodeLabel,
      completedNodes: projection.runSummary.completedNodes,
      totalNodes: projection.runSummary.totalNodes,
      transitionCount: projection.runSummary.transitionCount,
      errorCount: errorSummaries.length,
      errorSummaries,
      retryCount: projection.runSummary.retryCount,
      elapsedLabel: projection.runSummary.elapsedLabel,
      milestones: agentEvents.map((event, index) => {
        const milestone = {
          id: `${agent.agent_id}-${event.timestamp}-${index}`,
          label: milestoneLabel(event, currentGraph),
          eventType: event.event_type,
          nodeTypeLabel: milestoneNodeTypeLabel(event, currentGraph),
          timestamp: event.timestamp,
          timestampLabel: formatTimestamp(event.timestamp),
          timestampDetail: formatTimestamp(event.timestamp, true),
          relativeTimestampLabel: formatRelativeTimestamp(event.timestamp, agentState?.started_at),
          deltaLabel: formatDeltaLabel(event.timestamp, previousTimestamp),
          tone: eventTone(event.event_type),
          nodeId: nodeIdFromEvent(event),
          details: buildMilestoneDetails(event, currentGraph, labels),
          dataSections: buildMilestoneDataSections(
            event,
            currentGraph,
            agentState?.input_payload ?? runState?.input_payload ?? null,
            knownNodeOutputs,
            labels,
          ),
        };
        const completedNodeId = event.event_type === "node.completed" ? nodeIdFromEvent(event) : null;
        if (completedNodeId && "output" in event.payload) {
          knownNodeOutputs[completedNodeId] = event.payload.output;
        }
        previousTimestamp = event.timestamp;
        return milestone;
      }),
    };
  });
}

function normalizeFocusedEvents(events: RuntimeEvent[]): RuntimeEvent[] {
  return events.map((event) => ({ ...event, event_type: normalizeEventType(event.event_type) }));
}

function deriveRunStatus(runState: RunState | null, normalizedEvents: RuntimeEvent[]): string {
  const currentStatus = runState?.status?.trim();
  if (currentStatus) {
    return currentStatus;
  }
  const lastEventType = normalizedEvents[normalizedEvents.length - 1]?.event_type ?? null;
  if (lastEventType === "run.completed") {
    return "completed";
  }
  if (lastEventType === "run.failed") {
    return "failed";
  }
  if (lastEventType === "run.interrupted") {
    return "interrupted";
  }
  if (normalizedEvents.some((event) => event.event_type === "run.started")) {
    return "running";
  }
  return "idle";
}

function deriveCurrentNodeId(runState: RunState | null, normalizedEvents: RuntimeEvent[]): string | null {
  return runState?.current_node_id ?? null;
}

function wasNodeVisited(
  node: GraphNode,
  runState: RunState | null,
  completedNodeIdSet: Set<string>,
  latestOutput: unknown,
): boolean {
  if (!runState) {
    return completedNodeIdSet.has(node.id);
  }
  const status = runState.node_statuses?.[node.id] ?? "";
  if (status === "active" || status === "success" || status === "failed") {
    return true;
  }
  return (
    (runState.visit_counts?.[node.id] ?? 0) > 0 ||
    hasOwnRecordValue(runState.node_outputs, node.id) ||
    hasOwnRecordValue(runState.node_errors, node.id) ||
    (node.provider_id === "core.data_display" && latestOutput !== undefined)
  );
}

/** Latest `node.completed` output per node id (successive completions overwrite; supports progressive nodes). */
export function latestOutputsFromCompletedNodeEvents(normalizedEvents: RuntimeEvent[]): Record<string, unknown> {
  const outputs: Record<string, unknown> = {};
  for (const event of normalizedEvents) {
    if (event.event_type !== "node.completed") {
      continue;
    }
    const nodeId = nodeIdFromEvent(event);
    if (!nodeId || !Object.prototype.hasOwnProperty.call(event.payload, "output")) {
      continue;
    }
    outputs[nodeId] = event.payload.output;
  }
  return outputs;
}

function latestNodeOutputsByEvent(
  graph: GraphDefinition | null,
  runState: RunState | null,
  normalizedEvents: RuntimeEvent[],
): Record<string, unknown> {
  return latestRuntimeResolvedNodeOutputs(graph, runState, normalizedEvents);
}

function buildFocusedNodeStates(
  graph: GraphDefinition | null,
  runState: RunState | null,
  normalizedEvents: RuntimeEvent[],
  completedNodeIdSet: Set<string>,
  currentNodeId: string | null,
): Record<string, FocusedRunNodeState> {
  const latestEventOutputs = latestNodeOutputsByEvent(graph, runState, normalizedEvents);
  return Object.fromEntries(
    (graph?.nodes ?? []).map((node) => {
      const latestError = runState?.node_errors?.[node.id];
      const latestOutput = Object.prototype.hasOwnProperty.call(latestEventOutputs, node.id)
        ? latestEventOutputs[node.id]
        : runState?.node_outputs?.[node.id];
      const effectiveStatus = runState?.node_statuses?.[node.id] ?? "idle";
      return [
        node.id,
        {
          nodeId: node.id,
          isActive: effectiveStatus === "active" || currentNodeId === node.id,
          wasVisited: wasNodeVisited(node, runState, completedNodeIdSet, latestOutput),
          hasError: effectiveStatus === "failed" || latestError != null,
          latestOutput,
          latestError,
          visitCount: runState?.visit_counts?.[node.id] ?? 0,
        },
      ] satisfies [string, FocusedRunNodeState];
    }),
  );
}

function buildFocusedEventGroupsFromNormalizedEvents(
  graph: GraphDefinition | null,
  normalizedEvents: RuntimeEvent[],
): FocusedEventGroup[] {
  return normalizedEvents
    .filter((event) => event.event_type !== "edge.selected")
    .map((event, index) => buildSingleEventGroup(`event-${event.timestamp}-${index}`, event, graph))
    .reverse();
}

export function buildFocusedRunProjection(
  graph: GraphDefinition | null,
  runState: RunState | null,
  events: RuntimeEvent[],
): FocusedRunProjection {
  const labels = nodeLabelMap(graph);
  const normalizedEvents = normalizeFocusedEvents(events);
  const completedNodeIdSet = new Set(
    Object.entries(runState?.node_statuses ?? {})
      .filter(([, status]) => status === "success" || status === "failed")
      .map(([nodeId]) => nodeId),
  );
  const currentNodeId = deriveCurrentNodeId(runState, normalizedEvents);
  const errorSummaries = summarizeNodeErrors(runState?.node_errors ?? {}, labels);
  const nodeStates = buildFocusedNodeStates(graph, runState, normalizedEvents, completedNodeIdSet, currentNodeId);
  const loopRegions = buildFocusedLoopRegions(graph, runState, normalizedEvents);
  return {
    normalizedEvents,
    completedNodeIds: completedNodeIdSet,
    nodeStates,
    loopRegions,
    errorSummaries,
    runSummary: {
      runId: runState?.run_id ?? null,
      status: deriveRunStatus(runState, normalizedEvents),
      currentNodeId,
      currentNodeLabel: (currentNodeId ? labels.get(currentNodeId) : null) ?? currentNodeId ?? "n/a",
      completedNodes: Object.values(nodeStates).filter((nodeState) => nodeState.wasVisited).length,
      totalNodes: graph?.nodes.length ?? 0,
      transitionCount: runState?.transition_count ?? runState?.transition_history.length ?? 0,
      errorCount: errorSummaries.length,
      retryCount: normalizedEvents.filter((event) => event.event_type === "retry.triggered").length,
      elapsedLabel: formatElapsed(runState?.started_at, runState?.ended_at),
      lastHeartbeatLabel: formatHeartbeat(runState?.last_heartbeat_at),
      finalOutput: runState?.final_output ?? null,
      nodeErrors: runState?.node_errors ?? {},
    },
    eventGroups: buildFocusedEventGroupsFromNormalizedEvents(graph, normalizedEvents),
  };
}

export function buildFocusedRunSummary(
  graph: GraphDefinition | null,
  runState: RunState | null,
  events: RuntimeEvent[],
): FocusedRunSummary {
  return buildFocusedRunProjection(graph, runState, events).runSummary;
}

function buildSingleEventGroup(id: string, event: RuntimeEvent, graph: GraphDefinition | null): FocusedEventGroup {
  const eventType = normalizeEventType(event.event_type);
  const tone = eventTone(eventType);
  const iterationId =
    typeof event.payload.iteration_id === "string" && event.payload.iteration_id.trim().length > 0
      ? event.payload.iteration_id.trim()
      : null;
  const sessionId = sessionIdFromEventPayload(event.payload);
  const diagnosticsLine = iterationId && sessionId
    ? `Iteration ${iterationId} | Session ${sessionId}`
    : iterationId
      ? `Iteration ${iterationId}`
      : sessionId
        ? `Session ${sessionId}`
        : null;
  return {
    id,
    title: milestoneLabel(event, graph),
    subtitle: eventType,
    tone: tone === "idle" ? "info" : tone,
    eventCount: 1,
    startedAt: event.timestamp,
    endedAt: null,
    nodeId: nodeIdFromEvent(event),
    lines: diagnosticsLine ? [event.summary, diagnosticsLine] : [event.summary],
  };
}

export function buildFocusedEventGroups(
  graph: GraphDefinition | null,
  events: RuntimeEvent[],
): FocusedEventGroup[] {
  const normalizedEvents = normalizeFocusedEvents(events);
  return buildFocusedEventGroupsFromNormalizedEvents(graph, normalizedEvents);
}

export function nodeById(graph: GraphDefinition | null, nodeId: string | null): GraphNode | null {
  if (!graph || !nodeId) {
    return null;
  }
  return graph.nodes.find((node) => node.id === nodeId) ?? null;
}
