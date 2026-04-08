import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { AgentRunSwimlanes } from "./components/AgentRunSwimlanes";
import { GraphCanvas } from "./components/GraphCanvas";
import { GraphEnvEditor } from "./components/GraphEnvEditor";
import { McpServerModal } from "./components/McpServerModal";
import { UserPreferencesModal } from "./components/UserPreferencesModal";
import {
  bootMcpServer,
  createMcpServer,
  createGraph,
  deleteProjectFile,
  deleteMcpServer,
  deleteGraph,
  eventStreamUrl,
  fetchEditorCatalog,
  fetchProjectFiles,
  fetchRunFileContent,
  fetchRunFiles,
  fetchGraph,
  fetchGraphs,
  fetchRun,
  refreshMcpServer,
  resetRuntime,
  setMcpToolEnabled,
  startRun,
  stopMcpServer,
  testMcpServer,
  uploadProjectFiles,
  updateMcpServer,
  updateGraph,
} from "./lib/api";
import { createBlankGraph, layoutGraphLR, normalizeGraphDocument } from "./lib/editor";
import type { GraphLayoutNodeDimensions } from "./lib/editor";
import { filterEventsForAgent, getCanvasGraph, getDefaultAgentId, getSelectedRunId, getSelectedRunState, isTestEnvironment, updateSelectedAgentGraph } from "./lib/graphDocuments";
import { clearAllPersistedRunSnapshots, clearPersistedRunSnapshot, loadPersistedRunSnapshot, savePersistedRunSnapshot } from "./lib/runSnapshots";
import type { PersistedRunSnapshot } from "./lib/runSnapshots";
import { isTerminalRuntimeEvent, normalizeRunState, normalizeRuntimeEvent } from "./lib/runtimeEvents";
import { buildAgentRunLanes, buildEnvironmentRunSummary, buildFocusedRunProjection } from "./lib/runVisualization";
import type {
  EditorCatalog,
  GraphDefinition,
  GraphDocument,
  McpServerDraft,
  McpServerStatus,
  ProjectFile,
  RunDocument,
  RunFilesystemFileContent,
  RunFilesystemListing,
  RunState,
  RuntimeEvent,
  ToolDefinition,
} from "./lib/types";
import { getUserPreferences, resetUserPreferences, saveUserPreferences } from "./lib/userPreferences";
import type { UserPreferences } from "./lib/userPreferences";
import { useGraphHistory } from "./lib/useGraphHistory";

const DEFAULT_INPUT = "Find graph-agent references for a schema repair workflow.";
const DEFAULT_TEST_ENVIRONMENT_ID = "test-environment";
const ENVIRONMENT_AGENT_SELECTION_STORAGE_KEY = "agentic-nodes-environment-agent-selection";
const SELECTED_AGENT_ID_STORAGE_KEY = "agentic-nodes-selected-agent-id";

function isTerminalRunStatus(status: string | null | undefined): boolean {
  return status === "completed" || status === "failed" || status === "cancelled" || status === "interrupted";
}

function buildIdleNodeStatuses(
  startNodeId: string,
  nodes: Array<{ id: string }>,
  edges: Array<{ kind: string; source_id: string; target_id: string }>,
): Record<string, string> {
  const executionNodeIds = new Set<string>();
  if (startNodeId) {
    executionNodeIds.add(startNodeId);
  }
  for (const edge of edges) {
    if (edge.kind === "binding") {
      continue;
    }
    executionNodeIds.add(edge.source_id);
    executionNodeIds.add(edge.target_id);
  }
  return Object.fromEntries(nodes.filter((node) => executionNodeIds.has(node.id)).map((node) => [node.id, "idle"]));
}

function formatDocumentSize(sizeBytes: number): string {
  if (!Number.isFinite(sizeBytes) || sizeBytes <= 0) {
    return "0 B";
  }
  if (sizeBytes < 1024) {
    return `${sizeBytes} B`;
  }
  if (sizeBytes < 1024 * 1024) {
    return `${(sizeBytes / 1024).toFixed(1)} KB`;
  }
  return `${(sizeBytes / (1024 * 1024)).toFixed(1)} MB`;
}

function formatTimestamp(value: string): string {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return parsed.toLocaleString();
}

function normalizeShortcutToken(token: string): string {
  const normalized = token.trim().toLowerCase();
  if (!normalized) {
    return "";
  }
  if (normalized === "cmd" || normalized === "command" || normalized === "ctrl" || normalized === "control" || normalized === "mod") {
    return "mod";
  }
  if (normalized === "option") {
    return "alt";
  }
  return normalized;
}

function eventMatchesAccelerator(event: KeyboardEvent, accelerator: string): boolean {
  const parts = accelerator
    .split("+")
    .map((part) => normalizeShortcutToken(part))
    .filter(Boolean);
  if (parts.length === 0) {
    return false;
  }
  const keyToken = parts[parts.length - 1];
  const modifierTokens = new Set(parts.slice(0, -1));
  const requiresMod = modifierTokens.has("mod");
  const requiresAlt = modifierTokens.has("alt");
  const requiresShift = modifierTokens.has("shift");
  if (Boolean(event.metaKey || event.ctrlKey) !== requiresMod) {
    return false;
  }
  if (Boolean(event.altKey) !== requiresAlt) {
    return false;
  }
  if (Boolean(event.shiftKey) !== requiresShift) {
    return false;
  }
  return normalizeShortcutToken(event.key) === keyToken;
}

function markTerminalNodeStatuses(
  nodeStatuses: Record<string, string> | undefined,
  visitCounts: Record<string, number>,
  nodeErrors: Record<string, unknown>,
  terminalStatus: string,
): Record<string, string> {
  return Object.fromEntries(
    Object.entries(nodeStatuses ?? {}).map(([nodeId, status]) => {
      let nextStatus = status || "idle";
      if (nextStatus === "active") {
        if (Object.prototype.hasOwnProperty.call(nodeErrors, nodeId)) {
          nextStatus = "failed";
        } else if (terminalStatus === "failed") {
          nextStatus = "failed";
        } else if ((visitCounts[nodeId] ?? 0) > 0) {
          nextStatus = "success";
        } else {
          nextStatus = "idle";
        }
      }
      if (nextStatus === "idle") {
        nextStatus = "unreached";
      }
      return [nodeId, nextStatus];
    }),
  );
}

function getSavedInputPrompt(graph: GraphDocument | null | undefined): string {
  const savedPrompt = typeof graph?.default_input === "string" ? graph.default_input.trim() : "";
  return savedPrompt || DEFAULT_INPUT;
}

function buildEnvironmentAgentSelection(
  graph: GraphDocument | null | undefined,
  previous: Record<string, boolean> = {},
): Record<string, boolean> {
  if (!isTestEnvironment(graph)) {
    return {};
  }
  return Object.fromEntries(graph.agents.map((agent) => [agent.agent_id, previous[agent.agent_id] ?? true]));
}

function getSelectedEnvironmentAgentIds(
  graph: GraphDocument | null | undefined,
  selection: Record<string, boolean>,
): string[] {
  if (!isTestEnvironment(graph)) {
    return [];
  }
  return graph.agents.filter((agent) => selection[agent.agent_id] !== false).map((agent) => agent.agent_id);
}

function loadPersistedEnvironmentAgentSelections(): Record<string, Record<string, boolean>> {
  try {
    const raw = localStorage.getItem(ENVIRONMENT_AGENT_SELECTION_STORAGE_KEY);
    if (!raw) {
      return {};
    }
    const parsed = JSON.parse(raw) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return {};
    }
    return Object.fromEntries(
      Object.entries(parsed as Record<string, unknown>).map(([graphId, selection]) => [
        graphId,
        Object.fromEntries(
          Object.entries(selection && typeof selection === "object" && !Array.isArray(selection) ? selection : {}).map(([agentId, enabled]) => [
            agentId,
            enabled !== false,
          ]),
        ),
      ]),
    );
  } catch {
    return {};
  }
}

function loadEnvironmentAgentSelection(graphId: string): Record<string, boolean> {
  return loadPersistedEnvironmentAgentSelections()[graphId] ?? {};
}

function saveEnvironmentAgentSelection(graphId: string, selection: Record<string, boolean>): void {
  try {
    const storedSelections = loadPersistedEnvironmentAgentSelections();
    storedSelections[graphId] = selection;
    localStorage.setItem(ENVIRONMENT_AGENT_SELECTION_STORAGE_KEY, JSON.stringify(storedSelections));
  } catch {
    // Ignore local persistence failures and keep the in-memory selection.
  }
}

function loadPersistedSelectedAgentIds(): Record<string, string> {
  try {
    const raw = localStorage.getItem(SELECTED_AGENT_ID_STORAGE_KEY);
    if (!raw) {
      return {};
    }
    const parsed = JSON.parse(raw) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return {};
    }
    return Object.fromEntries(
      Object.entries(parsed as Record<string, unknown>)
        .map(([graphId, agentId]) => [graphId, typeof agentId === "string" ? agentId.trim() : ""])
        .filter(([, agentId]) => agentId.length > 0),
    );
  } catch {
    return {};
  }
}

function loadSelectedAgentId(graph: GraphDocument | null | undefined): string | null {
  if (!isTestEnvironment(graph) || !graph.graph_id) {
    return null;
  }
  const persistedAgentId = loadPersistedSelectedAgentIds()[graph.graph_id];
  if (persistedAgentId && graph.agents.some((agent) => agent.agent_id === persistedAgentId)) {
    return persistedAgentId;
  }
  return getDefaultAgentId(graph);
}

function saveSelectedAgentId(graph: GraphDocument | null | undefined, selectedAgentId: string | null | undefined): void {
  if (!isTestEnvironment(graph) || !graph.graph_id) {
    return;
  }
  try {
    const storedSelections = loadPersistedSelectedAgentIds();
    const normalizedAgentId = typeof selectedAgentId === "string" ? selectedAgentId.trim() : "";
    if (normalizedAgentId) {
      storedSelections[graph.graph_id] = normalizedAgentId;
    } else {
      delete storedSelections[graph.graph_id];
    }
    localStorage.setItem(SELECTED_AGENT_ID_STORAGE_KEY, JSON.stringify(storedSelections));
  } catch {
    // Ignore local persistence failures and keep the in-memory selection.
  }
}

function createEmptyRunState(runId: string, graphId: string, input: string, documents: RunDocument[] = []): RunState {
  return {
    run_id: runId,
    graph_id: graphId,
    agent_id: null,
    agent_name: null,
    parent_run_id: null,
    current_node_id: null,
    current_edge_id: null,
    status: "queued",
    status_reason: null,
    started_at: null,
    ended_at: null,
    runtime_instance_id: null,
    last_heartbeat_at: null,
    input_payload: input,
    documents,
    node_inputs: {},
    node_outputs: {},
    edge_outputs: {},
    node_errors: {},
    node_statuses: {},
    iterator_states: {},
    visit_counts: {},
    transition_history: [],
    event_history: [],
    final_output: null,
    terminal_error: null,
    agent_runs: {},
  };
}

function createPendingRunState(
  graph: GraphDocument,
  runId: string,
  input: string,
  agentIds?: string[],
  documents: RunDocument[] = [],
): RunState {
  const next = createEmptyRunState(runId, graph.graph_id, input, documents);
  if (!isTestEnvironment(graph)) {
    return {
      ...next,
      node_statuses: buildIdleNodeStatuses(graph.start_node_id, graph.nodes, graph.edges),
    };
  }
  const selectedAgentIds = new Set(agentIds ?? graph.agents.map((agent) => agent.agent_id));
  return {
    ...next,
    agent_runs: Object.fromEntries(
      graph.agents
        .filter((agent) => selectedAgentIds.has(agent.agent_id))
        .map((agent) => [
        agent.agent_id,
        {
          ...createEmptyRunState(`${runId}:${agent.agent_id}`, agent.agent_id, input, documents),
          node_statuses: buildIdleNodeStatuses(agent.start_node_id, agent.nodes, agent.edges),
          agent_id: agent.agent_id,
          agent_name: agent.name,
          parent_run_id: runId,
        } satisfies RunState,
        ]),
    ),
  };
}

function omitRunStateEntry<T>(record: Record<string, T> | undefined, key: string): Record<string, T> {
  if (!record || !Object.prototype.hasOwnProperty.call(record, key)) {
    return record ?? {};
  }
  const next = { ...record };
  delete next[key];
  return next;
}

function resolveEdgeOutputFromEventHistory(previous: RunState, edgePayload: Record<string, unknown>): unknown {
  const sourceNodeId = typeof edgePayload.source_id === "string" ? edgePayload.source_id : null;
  const sourceHandleId = typeof edgePayload.source_handle_id === "string" ? edgePayload.source_handle_id : null;
  if (!sourceNodeId) {
    return undefined;
  }
  for (let index = previous.event_history.length - 1; index >= 0; index -= 1) {
    const candidate = previous.event_history[index];
    if (candidate.event_type !== "node.completed") {
      continue;
    }
    const candidateNodeId = typeof candidate.payload.node_id === "string" ? candidate.payload.node_id : null;
    if (candidateNodeId !== sourceNodeId) {
      continue;
    }
    if (sourceHandleId) {
      const routeOutputs =
        typeof candidate.payload.route_outputs === "object" && candidate.payload.route_outputs !== null
          ? (candidate.payload.route_outputs as Record<string, unknown>)
          : null;
      if (routeOutputs && Object.prototype.hasOwnProperty.call(routeOutputs, sourceHandleId)) {
        return routeOutputs[sourceHandleId];
      }
    }
    if (Object.prototype.hasOwnProperty.call(candidate.payload, "output")) {
      return candidate.payload.output;
    }
    return undefined;
  }
  return undefined;
}

function applySingleRunEvent(previous: RunState, event: RuntimeEvent): RunState {
  const next: RunState = {
    ...previous,
    event_history: [...previous.event_history, event],
  };

  if (event.event_type === "run.started") {
    next.status = "running";
    next.status_reason = null;
    next.started_at = event.timestamp;
  }

  if (event.event_type === "node.started") {
    const payload = event.payload as { node_id: string; visit_count: number; received_input?: unknown };
    next.current_node_id = payload.node_id;
    next.current_edge_id = null;
    next.visit_counts = {
      ...next.visit_counts,
      [payload.node_id]: payload.visit_count,
    };
    next.node_inputs = {
      ...(next.node_inputs ?? {}),
      [payload.node_id]: payload.received_input,
    };
    next.node_errors = omitRunStateEntry(next.node_errors, payload.node_id);
    next.node_statuses = {
      ...(next.node_statuses ?? {}),
      [payload.node_id]: "active",
    };
  }

  if (event.event_type === "node.completed") {
    const payload = event.payload as { node_id: string; output?: unknown; error?: unknown };
    if (next.current_node_id === payload.node_id) {
      next.current_node_id = null;
    }
    if (payload.output !== undefined) {
      next.node_outputs = {
        ...next.node_outputs,
        [payload.node_id]: payload.output,
      };
    }
    if (payload.error != null) {
      next.node_errors = {
        ...next.node_errors,
        [payload.node_id]: payload.error,
      };
    } else {
      next.node_errors = omitRunStateEntry(next.node_errors, payload.node_id);
    }
    next.node_statuses = {
      ...(next.node_statuses ?? {}),
      [payload.node_id]: payload.error != null ? "failed" : "success",
    };
  }

  if (event.event_type === "node.iterator.updated") {
    const payload = event.payload as Record<string, unknown>;
    const nodeId = typeof payload.node_id === "string" ? payload.node_id : null;
    if (nodeId) {
      next.iterator_states = {
        ...(next.iterator_states ?? {}),
        [nodeId]: {
          iterator_type: payload.iterator_type,
          status: payload.status,
          current_row_index: payload.current_row_index,
          total_rows: payload.total_rows,
          headers: payload.headers,
          sheet_name: payload.sheet_name,
          source_file: payload.source_file,
          file_format: payload.file_format,
        },
      };
    }
  }

  if (event.event_type === "edge.selected") {
    const payload = event.payload as Record<string, unknown>;
    next.current_edge_id = typeof payload.id === "string" ? payload.id : null;
    const selectedEdgeId = typeof payload.id === "string" ? payload.id : null;
    const selectedEdgeOutput = resolveEdgeOutputFromEventHistory(previous, payload);
    if (selectedEdgeId && selectedEdgeOutput !== undefined) {
      next.edge_outputs = {
        ...(next.edge_outputs ?? {}),
        [selectedEdgeId]: selectedEdgeOutput,
      };
    }
    next.transition_history = [
      ...next.transition_history,
      {
        edge_id: payload.id,
        source_id: payload.source_id,
        target_id: payload.target_id,
        timestamp: event.timestamp,
      },
    ];
  }

  if (event.event_type === "run.completed") {
    next.status = "completed";
    next.status_reason = null;
    next.current_node_id = null;
    next.current_edge_id = null;
    next.ended_at = event.timestamp;
    next.final_output = event.payload.final_output;
    next.node_statuses = markTerminalNodeStatuses(next.node_statuses, next.visit_counts, next.node_errors, "completed");
  }

  if (event.event_type === "run.failed") {
    next.status = "failed";
    next.status_reason = null;
    next.current_node_id = null;
    next.current_edge_id = null;
    next.ended_at = event.timestamp;
    next.terminal_error = (event.payload.error ?? null) as Record<string, unknown> | null;
    if ("final_output" in event.payload) {
      next.final_output = event.payload.final_output;
    }
    next.node_statuses = markTerminalNodeStatuses(next.node_statuses, next.visit_counts, next.node_errors, "failed");
  }

  if (event.event_type === "run.cancelled") {
    next.status = "cancelled";
    next.status_reason = null;
    next.current_node_id = null;
    next.current_edge_id = null;
    next.ended_at = event.timestamp;
    next.terminal_error = (event.payload.error ?? null) as Record<string, unknown> | null;
    if ("final_output" in event.payload) {
      next.final_output = event.payload.final_output;
    }
    next.node_statuses = markTerminalNodeStatuses(next.node_statuses, next.visit_counts, next.node_errors, "cancelled");
  }

  if (event.event_type === "run.interrupted") {
    next.status = "interrupted";
    next.status_reason = typeof event.payload.reason === "string" ? event.payload.reason : null;
    next.current_node_id = null;
    next.current_edge_id = null;
    next.ended_at = event.timestamp;
    next.terminal_error = (event.payload.error ?? null) as Record<string, unknown> | null;
    if ("final_output" in event.payload) {
      next.final_output = event.payload.final_output;
    }
    next.node_statuses = markTerminalNodeStatuses(next.node_statuses, next.visit_counts, next.node_errors, "interrupted");
  }

  return next;
}

function applyEvent(
  previous: RunState | null,
  event: RuntimeEvent,
  graphId: string,
  input: string,
  documents: RunDocument[] = [],
): RunState {
  const next = previous ?? createEmptyRunState(event.run_id, graphId, input, documents);
  if (event.agent_id) {
    const agentId = event.agent_id;
    const payload = event.payload as { child_run_id?: string; agent_name?: string };
    const priorAgentRun =
      next.agent_runs?.[agentId] ??
      {
        ...createEmptyRunState(payload.child_run_id ?? `${event.run_id}:${agentId}`, graphId, input, next.documents ?? documents),
        agent_id: agentId,
        agent_name: payload.agent_name ?? agentId,
        parent_run_id: event.run_id,
      };
    const normalizedEvent: RuntimeEvent = {
      ...event,
      event_type: event.event_type.replace(/^agent\./, ""),
      run_id: priorAgentRun.run_id,
    };
    return {
      ...next,
      status: next.status === "queued" ? "running" : next.status,
      event_history: [...next.event_history, event],
      agent_runs: {
        ...(next.agent_runs ?? {}),
        [agentId]: applySingleRunEvent(priorAgentRun, normalizedEvent),
      },
    };
  }
  return applySingleRunEvent(next, event);
}

function pickDefaultGraphId(graphs: GraphDocument[]): string {
  return graphs.find((graph) => graph.graph_id === DEFAULT_TEST_ENVIRONMENT_ID)?.graph_id ?? graphs[0]?.graph_id ?? "";
}

function serializeGraphSnapshot(graph: GraphDocument | null): string {
  return graph ? JSON.stringify(normalizeGraphDocument(graph)) : "";
}

function mergeCatalogServerStatus(catalog: EditorCatalog | null, serverStatus: McpServerStatus): EditorCatalog | null {
  if (!catalog) {
    return catalog;
  }
  return {
    ...catalog,
    mcp_servers: (catalog.mcp_servers ?? []).map((server) => (server.server_id === serverStatus.server_id ? serverStatus : server)),
  };
}

function mergeCatalogTool(catalog: EditorCatalog | null, toolDefinition: ToolDefinition): EditorCatalog | null {
  if (!catalog) {
    return catalog;
  }
  const nextToolName = toolDefinition.canonical_name ?? toolDefinition.name;
  return {
    ...catalog,
    tools: catalog.tools.map((tool) => ((tool.canonical_name ?? tool.name) === nextToolName ? toolDefinition : tool)),
  };
}

export default function App() {
  const [graphs, setGraphs] = useState<GraphDocument[]>([]);
  const [selectedGraphId, setSelectedGraphId] = useState<string>("");
  const [selectedAgentId, setSelectedAgentId] = useState<string | null>(null);
  const [environmentAgentSelection, setEnvironmentAgentSelection] = useState<Record<string, boolean>>({});
  const history = useGraphHistory();
  const { graph: draftGraph, set: setDraftGraph, setQuiet: setDraftGraphQuiet, reset: resetHistory } = history;
  const [savedGraphSnapshot, setSavedGraphSnapshot] = useState("");
  const [catalog, setCatalog] = useState<EditorCatalog | null>(null);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [selectedEdgeId, setSelectedEdgeId] = useState<string | null>(null);
  const [visualizerResetVersion, setVisualizerResetVersion] = useState(0);
  const [input, setInput] = useState(DEFAULT_INPUT);
  const [savedInputPrompt, setSavedInputPrompt] = useState(DEFAULT_INPUT);
  const [projectFiles, setProjectFiles] = useState<ProjectFile[]>([]);
  const [runFileListing, setRunFileListing] = useState<RunFilesystemListing | null>(null);
  const [selectedRunFilePath, setSelectedRunFilePath] = useState<string | null>(null);
  const [selectedRunFileContent, setSelectedRunFileContent] = useState<RunFilesystemFileContent | null>(null);
  const [isRunFilesLoading, setIsRunFilesLoading] = useState(false);
  const [isRunFileContentLoading, setIsRunFileContentLoading] = useState(false);
  const [runFilesError, setRunFilesError] = useState<string | null>(null);
  const [runFileContentError, setRunFileContentError] = useState<string | null>(null);
  const [isUploadingProjectFiles, setIsUploadingProjectFiles] = useState(false);
  const [isProjectFilesLoading, setIsProjectFilesLoading] = useState(false);
  const [projectFileError, setProjectFileError] = useState<string | null>(null);
  const [events, setEvents] = useState<RuntimeEvent[]>([]);
  const [runState, setRunState] = useState<RunState | null>(null);
  const [activeRunId, setActiveRunId] = useState<string | null>(null);
  const [isRunning, setIsRunning] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [isResettingRuntime, setIsResettingRuntime] = useState(false);
  const [mcpPendingKey, setMcpPendingKey] = useState<string | null>(null);
  const [mcpPanelOpen, setMcpPanelOpen] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [userPreferences, setUserPreferences] = useState<UserPreferences>(() => getUserPreferences());
  const [userPreferencesOpen, setUserPreferencesOpen] = useState(false);
  const sourceRef = useRef<EventSource | null>(null);
  const runPollTimeoutRef = useRef<number | null>(null);
  const runStateRef = useRef<RunState | null>(null);
  const inputRef = useRef(input);
  const pendingRunSnapshotRef = useRef<PersistedRunSnapshot | null>(null);
  const persistRunSnapshotTimeoutRef = useRef<number | null>(null);
  const executionBoxRef = useRef<HTMLDivElement | null>(null);

  const canvasGraph = useMemo(() => getCanvasGraph(draftGraph, selectedAgentId), [draftGraph, selectedAgentId]);
  const selectedRunState = useMemo(() => getSelectedRunState(runState, selectedAgentId), [runState, selectedAgentId]);
  const selectedRunId = useMemo(() => getSelectedRunId(runState, activeRunId, selectedAgentId), [runState, activeRunId, selectedAgentId]);
  const filteredEvents = useMemo(() => filterEventsForAgent(events, selectedAgentId), [events, selectedAgentId]);
  const persistedGraphIds = useMemo(() => new Set(graphs.map((graph) => graph.graph_id)), [graphs]);
  const isEnvironment = isTestEnvironment(draftGraph);
  const selectedEnvironmentAgentIds = useMemo(
    () => getSelectedEnvironmentAgentIds(draftGraph, environmentAgentSelection),
    [draftGraph, environmentAgentSelection],
  );
  const environmentRunSummary = useMemo(
    () => buildEnvironmentRunSummary(draftGraph, runState, selectedAgentId),
    [draftGraph, runState, selectedAgentId],
  );
  const agentRunLanes = useMemo(() => buildAgentRunLanes(draftGraph, runState, events), [draftGraph, runState, events]);
  const focusedRunProjection = useMemo(
    () => buildFocusedRunProjection(canvasGraph, selectedRunState, filteredEvents),
    [canvasGraph, selectedRunState, filteredEvents],
  );
  const focusedRunSummary = focusedRunProjection.runSummary;
  const focusedEventGroups = focusedRunProjection.eventGroups;
  const draftGraphSnapshot = useMemo(() => serializeGraphSnapshot(draftGraph), [draftGraph]);
  const hasUnsavedChanges = (Boolean(draftGraph) && draftGraphSnapshot !== savedGraphSnapshot) || input !== savedInputPrompt;
  const projectFileGraphId = selectedGraphId || draftGraph?.graph_id || "";
  const readyProjectFiles = useMemo(() => projectFiles.filter((file) => file.status === "ready"), [projectFiles]);
  const visibleRunFiles = useMemo(() => runFileListing?.files ?? [], [runFileListing]);
  const selectedRunFile = useMemo(
    () => visibleRunFiles.find((file) => file.path === selectedRunFilePath) ?? null,
    [visibleRunFiles, selectedRunFilePath],
  );

  useEffect(() => {
    runStateRef.current = runState;
  }, [runState]);

  useEffect(() => {
    inputRef.current = input;
  }, [input]);

  const refreshRunFiles = useCallback(async (runId: string) => {
    setRunFilesError(null);
    setIsRunFilesLoading(true);
    try {
      const listing = await fetchRunFiles(runId);
      setRunFileListing(listing);
      setSelectedRunFilePath((current) => {
        if (current && listing.files.some((file) => file.path === current)) {
          return current;
        }
        return listing.files[0]?.path ?? null;
      });
    } catch (loadError) {
      const message = loadError instanceof Error ? loadError.message : "Unable to load run files.";
      setRunFilesError(message);
      setRunFileListing(null);
      setSelectedRunFilePath(null);
    } finally {
      setIsRunFilesLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!selectedRunId) {
      setRunFileListing(null);
      setSelectedRunFilePath(null);
      setSelectedRunFileContent(null);
      setRunFilesError(null);
      setRunFileContentError(null);
      return;
    }
    void refreshRunFiles(selectedRunId);
  }, [refreshRunFiles, selectedRunId, selectedRunState?.event_history.length]);

  useEffect(() => {
    if (!selectedRunId || !selectedRunFilePath) {
      setSelectedRunFileContent(null);
      setRunFileContentError(null);
      return;
    }
    let cancelled = false;
    setRunFileContentError(null);
    setIsRunFileContentLoading(true);
    void fetchRunFileContent(selectedRunId, selectedRunFilePath)
      .then((content) => {
        if (!cancelled) {
          setSelectedRunFileContent(content);
        }
      })
      .catch((loadError) => {
        if (cancelled) {
          return;
        }
        const message = loadError instanceof Error ? loadError.message : "Unable to load file content.";
        setRunFileContentError(message);
        setSelectedRunFileContent(null);
      })
      .finally(() => {
        if (!cancelled) {
          setIsRunFileContentLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [selectedRunFilePath, selectedRunId]);

  const refreshCatalog = useCallback(async () => {
    const loadedCatalog = await fetchEditorCatalog();
    setCatalog(loadedCatalog);
    return loadedCatalog;
  }, []);

  const hydrateSelectedGraph = useCallback((graph: GraphDocument) => {
    const nextGraph = normalizeGraphDocument(graph);
    resetHistory(nextGraph);
    setSavedGraphSnapshot(serializeGraphSnapshot(nextGraph));
    const nextInput = getSavedInputPrompt(nextGraph);
    setInput(nextInput);
    setSavedInputPrompt(nextInput);
    setSelectedAgentId(loadSelectedAgentId(nextGraph));
    setSelectedNodeId(null);
    setSelectedEdgeId(null);
    return nextGraph;
  }, [resetHistory]);

  const refreshProjectFiles = useCallback(async (graphId: string) => {
    if (!graphId.trim()) {
      setProjectFiles([]);
      setProjectFileError(null);
      return;
    }
    setProjectFileError(null);
    setIsProjectFilesLoading(true);
    try {
      const files = await fetchProjectFiles(graphId);
      setProjectFiles(files);
    } catch (loadError) {
      const message = loadError instanceof Error ? loadError.message : "Unable to load project files.";
      setProjectFileError(message);
      setProjectFiles([]);
    } finally {
      setIsProjectFilesLoading(false);
    }
  }, []);

  const handleProjectFileUpload = useCallback(async (graphId: string, fileList: FileList | null) => {
    if (!graphId.trim() || !fileList || fileList.length === 0) {
      return;
    }
    setProjectFileError(null);
    setIsUploadingProjectFiles(true);
    try {
      const uploadedFiles = await uploadProjectFiles(graphId, Array.from(fileList));
      setProjectFiles((current) => [...current, ...uploadedFiles]);
    } catch (uploadError) {
      const message = uploadError instanceof Error ? uploadError.message : "Unable to upload project files.";
      setProjectFileError(message);
    } finally {
      setIsUploadingProjectFiles(false);
    }
  }, []);

  const removeProjectFile = useCallback(async (graphId: string, fileId: string) => {
    if (!graphId.trim()) {
      return;
    }
    setProjectFileError(null);
    try {
      await deleteProjectFile(graphId, fileId);
      setProjectFiles((current) => current.filter((file) => file.file_id !== fileId));
    } catch (removeError) {
      const message = removeError instanceof Error ? removeError.message : "Unable to remove project file.";
      setProjectFileError(message);
    }
  }, []);

  useEffect(() => {
    if (!projectFileGraphId) {
      setProjectFiles([]);
      setProjectFileError(null);
      setIsProjectFilesLoading(false);
      return;
    }
    void refreshProjectFiles(projectFileGraphId);
  }, [projectFileGraphId, refreshProjectFiles]);

  useEffect(() => {
    function handleKeyDown(event: KeyboardEvent) {
      const saveGraphAccelerator = userPreferences.keyboardShortcuts.saveGraph.accelerator.trim();
      if (saveGraphAccelerator && eventMatchesAccelerator(event, saveGraphAccelerator)) {
        event.preventDefault();
        event.stopPropagation();
        event.stopImmediatePropagation();
        if (!draftGraph || isSaving) {
          return;
        }
        void saveCurrentGraph();
        return;
      }
      const runGraphAccelerator = userPreferences.keyboardShortcuts.runGraph.accelerator.trim();
      if (runGraphAccelerator && eventMatchesAccelerator(event, runGraphAccelerator)) {
        event.preventDefault();
        event.stopPropagation();
        event.stopImmediatePropagation();
        if (!draftGraph || isSaving || isRunning || isResettingRuntime) {
          return;
        }
        void handleRun();
      }
    }

    window.addEventListener("keydown", handleKeyDown, { capture: true });
    return () => window.removeEventListener("keydown", handleKeyDown, { capture: true });
  }, [draftGraph, handleRun, isResettingRuntime, isRunning, isSaving, saveCurrentGraph, userPreferences.keyboardShortcuts]);

  const clearRunPolling = useCallback(() => {
    if (runPollTimeoutRef.current === null) {
      return;
    }
    window.clearTimeout(runPollTimeoutRef.current);
    runPollTimeoutRef.current = null;
  }, []);

  const cancelPersistedRunSnapshot = useCallback((graphId?: string) => {
    if (persistRunSnapshotTimeoutRef.current !== null) {
      window.clearTimeout(persistRunSnapshotTimeoutRef.current);
      persistRunSnapshotTimeoutRef.current = null;
    }
    if (!graphId || pendingRunSnapshotRef.current?.graphId === graphId) {
      pendingRunSnapshotRef.current = null;
    }
  }, []);

  const flushPersistedRunSnapshot = useCallback((snapshot?: PersistedRunSnapshot | null) => {
    if (persistRunSnapshotTimeoutRef.current !== null) {
      window.clearTimeout(persistRunSnapshotTimeoutRef.current);
      persistRunSnapshotTimeoutRef.current = null;
    }
    const nextSnapshot = snapshot ?? pendingRunSnapshotRef.current;
    pendingRunSnapshotRef.current = null;
    if (!nextSnapshot) {
      return;
    }
    savePersistedRunSnapshot(nextSnapshot);
  }, []);

  const schedulePersistedRunSnapshot = useCallback((snapshot: PersistedRunSnapshot) => {
    pendingRunSnapshotRef.current = snapshot;
    if (persistRunSnapshotTimeoutRef.current !== null) {
      return;
    }
    persistRunSnapshotTimeoutRef.current = window.setTimeout(() => {
      persistRunSnapshotTimeoutRef.current = null;
      const nextSnapshot = pendingRunSnapshotRef.current;
      pendingRunSnapshotRef.current = null;
      if (nextSnapshot) {
        savePersistedRunSnapshot(nextSnapshot);
      }
    }, 250);
  }, []);

  const applyFetchedRunState = useCallback((nextRunState: RunState) => {
    const normalizedRunState = normalizeRunState(nextRunState) as RunState;
    setActiveRunId(normalizedRunState.run_id);
    setRunState(normalizedRunState);
    setEvents(normalizedRunState.event_history ?? []);
    setIsRunning(!isTerminalRunStatus(normalizedRunState.status));
  }, []);

  const markRecoveredRunInterrupted = useCallback((
    graphId: string,
    nextRunState: RunState | null,
    fallbackRunId: string | null,
    savedAt?: string,
  ) => {
    clearRunPolling();
    sourceRef.current?.close();
    sourceRef.current = null;
    if (!nextRunState) {
      cancelPersistedRunSnapshot(graphId);
      clearPersistedRunSnapshot(graphId);
      setActiveRunId(null);
      setEvents([]);
      setRunState(null);
      setIsRunning(false);
      return;
    }
    const interruptedState: RunState = {
      ...nextRunState,
      run_id: nextRunState.run_id ?? fallbackRunId ?? nextRunState.run_id,
      status: "interrupted",
      status_reason: nextRunState.status_reason ?? "recovery_unavailable",
      ended_at: nextRunState.ended_at ?? savedAt ?? new Date().toISOString(),
      terminal_error:
        nextRunState.terminal_error ??
        ({
          type: "run_state_unavailable",
          message: "The UI could not reconnect to the backend for this persisted run.",
        } as Record<string, unknown>),
    };
    setActiveRunId(interruptedState.run_id);
    setRunState(interruptedState);
    setEvents(interruptedState.event_history ?? []);
    setIsRunning(false);
    flushPersistedRunSnapshot({
      graphId,
      activeRunId: interruptedState.run_id,
      events: interruptedState.event_history ?? [],
      runState: interruptedState,
      savedAt: interruptedState.ended_at ?? new Date().toISOString(),
    });
  }, [cancelPersistedRunSnapshot, clearRunPolling, flushPersistedRunSnapshot]);

  const scheduleRunPoll = useCallback((runId: string, graphId: string) => {
    clearRunPolling();
    runPollTimeoutRef.current = window.setTimeout(() => {
      void fetchRun(runId)
        .then((nextRunState) => {
          applyFetchedRunState(nextRunState);
          if (isTerminalRunStatus(nextRunState.status)) {
            cancelPersistedRunSnapshot(graphId);
            clearPersistedRunSnapshot(graphId);
            clearRunPolling();
            return;
          }
          scheduleRunPoll(runId, graphId);
        })
        .catch(() => {
          markRecoveredRunInterrupted(graphId, runStateRef.current, runId);
        });
    }, 1500);
  }, [applyFetchedRunState, clearRunPolling, markRecoveredRunInterrupted]);

  const connectToRunStream = useCallback((runId: string, graphId: string, inputValue: string, documents: RunDocument[] = []) => {
    clearRunPolling();
    sourceRef.current?.close();
    const source = new EventSource(eventStreamUrl(runId));
    sourceRef.current = source;

    source.onmessage = (message) => {
      const event = normalizeRuntimeEvent(JSON.parse(message.data) as RuntimeEvent);
      setEvents((previous) => [...previous, event]);
      setRunState((previous) => applyEvent(previous, event, graphId, inputValue, documents));

      if (!event.agent_id && isTerminalRuntimeEvent(event)) {
        source.close();
        sourceRef.current = null;
        setIsRunning(false);
        cancelPersistedRunSnapshot(graphId);
        clearPersistedRunSnapshot(graphId);
        void fetchRun(runId)
          .then((nextRunState) => {
            applyFetchedRunState(nextRunState);
          })
          .catch(() => {
            // The local reducer already has the terminal event; keep that snapshot.
          });
      }
    };

    source.onerror = () => {
      source.close();
      sourceRef.current = null;
      setIsRunning(false);
      scheduleRunPoll(runId, graphId);
    };
  }, [applyFetchedRunState, clearRunPolling, scheduleRunPoll]);

  const restorePersistedRunSnapshot = useCallback(async (graphId: string) => {
    const snapshot = loadPersistedRunSnapshot(graphId);
    const snapshotRunId = snapshot?.activeRunId ?? snapshot?.runState?.run_id ?? null;
    const snapshotRunState = snapshot?.runState ?? null;
    const shouldHydrateLocalSnapshot = Boolean(snapshotRunId && snapshotRunState && !isTerminalRunStatus(snapshotRunState.status));
    setActiveRunId(shouldHydrateLocalSnapshot ? snapshotRunId : null);
    setEvents(shouldHydrateLocalSnapshot ? (snapshot?.events ?? []) : []);
    setRunState(shouldHydrateLocalSnapshot ? snapshotRunState : null);
    setIsRunning(false);
    clearRunPolling();
    sourceRef.current?.close();
    sourceRef.current = null;
    if (!snapshotRunId) {
      return;
    }
    try {
      const recoveredRunState = await fetchRun(snapshotRunId);
      if (isTerminalRunStatus(recoveredRunState.status)) {
        cancelPersistedRunSnapshot(graphId);
        clearPersistedRunSnapshot(graphId);
        setActiveRunId(null);
        setEvents([]);
        setRunState(null);
        setIsRunning(false);
        return;
      }
      applyFetchedRunState(recoveredRunState);
      connectToRunStream(recoveredRunState.run_id, graphId, inputRef.current, recoveredRunState.documents ?? []);
    } catch {
      markRecoveredRunInterrupted(graphId, shouldHydrateLocalSnapshot ? snapshotRunState : null, snapshotRunId, snapshot?.savedAt);
    }
  }, [applyFetchedRunState, clearRunPolling, connectToRunStream, markRecoveredRunInterrupted]);

  useEffect(() => {
    Promise.all([fetchGraphs(), refreshCatalog()])
      .then(([loadedGraphs, loadedCatalog]) => {
        setGraphs(loadedGraphs);
        setCatalog(loadedCatalog);
        if (loadedGraphs.length > 0) {
          const defaultGraphId = pickDefaultGraphId(loadedGraphs);
          const defaultGraph = loadedGraphs.find((graph) => graph.graph_id === defaultGraphId) ?? null;
          if (defaultGraph) {
            const nextGraph = hydrateSelectedGraph(defaultGraph);
            setSelectedGraphId(defaultGraphId);
            void restorePersistedRunSnapshot(nextGraph.graph_id);
            return;
          }
          setSelectedGraphId(defaultGraphId);
        } else {
          const blankGraph = createBlankGraph();
          resetHistory(blankGraph);
          setSavedGraphSnapshot(serializeGraphSnapshot(blankGraph));
          setInput(DEFAULT_INPUT);
          setSavedInputPrompt(DEFAULT_INPUT);
          setActiveRunId(null);
          setEvents([]);
          setRunState(null);
          setIsRunning(false);
        }
      })
      .catch((loadError: Error) => {
        setError(loadError.message);
      });
  }, [hydrateSelectedGraph, refreshCatalog, resetHistory, restorePersistedRunSnapshot]);

  useEffect(() => {
    return () => {
      flushPersistedRunSnapshot();
      sourceRef.current?.close();
      if (runPollTimeoutRef.current !== null) {
        window.clearTimeout(runPollTimeoutRef.current);
      }
    };
  }, [flushPersistedRunSnapshot]);

  useEffect(() => {
    if (!selectedGraphId) {
      return;
    }
    if (draftGraph?.graph_id === selectedGraphId) {
      return;
    }
    fetchGraph(selectedGraphId)
      .then((graph) => {
        const nextGraph = hydrateSelectedGraph(graph);
        void restorePersistedRunSnapshot(nextGraph.graph_id);
      })
      .catch((loadError: Error) => setError(loadError.message));
  }, [draftGraph?.graph_id, hydrateSelectedGraph, restorePersistedRunSnapshot, selectedGraphId]);

  useEffect(() => {
    if (!isTestEnvironment(draftGraph) || !draftGraph.graph_id) {
      setEnvironmentAgentSelection({});
      return;
    }
    setEnvironmentAgentSelection(
      buildEnvironmentAgentSelection(draftGraph, loadEnvironmentAgentSelection(draftGraph.graph_id)),
    );
  }, [draftGraph]);

  useEffect(() => {
    if (!isTestEnvironment(draftGraph) || !draftGraph.graph_id) {
      return;
    }
    saveEnvironmentAgentSelection(draftGraph.graph_id, buildEnvironmentAgentSelection(draftGraph, environmentAgentSelection));
  }, [draftGraph, environmentAgentSelection]);

  useEffect(() => {
    if (!isTestEnvironment(draftGraph) || !draftGraph.graph_id) {
      return;
    }
    const normalizedAgentId =
      selectedAgentId && draftGraph.agents.some((agent) => agent.agent_id === selectedAgentId)
        ? selectedAgentId
        : getDefaultAgentId(draftGraph);
    if (normalizedAgentId !== selectedAgentId) {
      setSelectedAgentId(normalizedAgentId);
      return;
    }
    saveSelectedAgentId(draftGraph, normalizedAgentId);
  }, [draftGraph, selectedAgentId]);

  useEffect(() => {
    setSelectedNodeId(null);
    setSelectedEdgeId(null);
  }, [selectedAgentId]);

  useEffect(() => {
    if (!hasUnsavedChanges || isSaving) {
      return;
    }
    const handleBeforeUnload = (event: BeforeUnloadEvent) => {
      event.preventDefault();
      event.returnValue = "";
    };
    window.addEventListener("beforeunload", handleBeforeUnload);
    return () => {
      window.removeEventListener("beforeunload", handleBeforeUnload);
    };
  }, [hasUnsavedChanges, isSaving]);

  useEffect(() => {
    if (!draftGraph?.graph_id) {
      return;
    }
    if (!activeRunId && !runState && events.length === 0) {
      cancelPersistedRunSnapshot(draftGraph.graph_id);
      clearPersistedRunSnapshot(draftGraph.graph_id);
      return;
    }
    schedulePersistedRunSnapshot({
      graphId: draftGraph.graph_id,
      activeRunId,
      events,
      runState,
      savedAt: new Date().toISOString(),
    });
  }, [activeRunId, cancelPersistedRunSnapshot, draftGraph?.graph_id, events, runState, schedulePersistedRunSnapshot]);

  async function refreshGraphs(nextSelectedGraphId?: string) {
    const loadedGraphs = await fetchGraphs();
    setGraphs(loadedGraphs);
    if (nextSelectedGraphId) {
      setSelectedGraphId(nextSelectedGraphId);
    } else if (loadedGraphs.length === 0) {
      const blankGraph = createBlankGraph();
      setSelectedGraphId("");
      resetHistory(blankGraph);
      setSavedGraphSnapshot(serializeGraphSnapshot(blankGraph));
      setInput(DEFAULT_INPUT);
      setSavedInputPrompt(DEFAULT_INPUT);
      setSelectedAgentId(null);
      setActiveRunId(null);
      setEvents([]);
      setRunState(null);
      setIsRunning(false);
    }
  }

  async function runMcpAction<T>(actionKey: string, callback: () => Promise<T>, applyResult?: (result: T) => void): Promise<T | null> {
    setMcpPendingKey(actionKey);
    setError(null);
    try {
      const result = await callback();
      applyResult?.(result);
      await refreshCatalog();
      return result;
    } catch (actionError) {
      const message = actionError instanceof Error ? actionError.message : "Unable to update MCP state.";
      setError(message);
      return null;
    } finally {
      setMcpPendingKey(null);
    }
  }

  async function saveCurrentGraph(): Promise<GraphDocument | null> {
    if (!draftGraph) {
      return null;
    }
    setIsSaving(true);
    setError(null);
    try {
      const normalized = {
        ...normalizeGraphDocument(draftGraph),
        default_input: input,
      } satisfies GraphDocument;
      const savedGraph =
        selectedGraphId && persistedGraphIds.has(selectedGraphId)
          ? await updateGraph(selectedGraphId, normalized)
          : await createGraph(normalized);
      await refreshGraphs(savedGraph.graph_id);
      setSavedGraphSnapshot(serializeGraphSnapshot(savedGraph));
      setSavedInputPrompt(getSavedInputPrompt(savedGraph));
      setDraftGraph(savedGraph);
      if (isTestEnvironment(savedGraph)) {
        setSelectedAgentId((current) => current ?? getDefaultAgentId(savedGraph));
      }
      return savedGraph;
    } catch (saveError) {
      const message = saveError instanceof Error ? saveError.message : "Unable to save graph.";
      setError(message);
      return null;
    } finally {
      setIsSaving(false);
    }
  }

  function clearLiveRunState() {
    clearRunPolling();
    sourceRef.current?.close();
    sourceRef.current = null;
    setActiveRunId(null);
    setEvents([]);
    setRunState(null);
    setIsRunning(false);
    setSelectedNodeId(null);
    setSelectedEdgeId(null);
    setVisualizerResetVersion((current) => current + 1);
  }

  function handleCreateGraph() {
    const blankGraph = createBlankGraph();
    clearLiveRunState();
    setSelectedGraphId("");
    setSelectedAgentId(null);
    resetHistory(blankGraph);
    setSavedGraphSnapshot(serializeGraphSnapshot(blankGraph));
    setInput(DEFAULT_INPUT);
    setSavedInputPrompt(DEFAULT_INPUT);
    setSelectedNodeId(null);
    setSelectedEdgeId(null);
    setError(null);
  }

  async function handleDeleteGraph() {
    if (!selectedGraphId) {
      handleCreateGraph();
      return;
    }
    try {
      cancelPersistedRunSnapshot(selectedGraphId);
      clearPersistedRunSnapshot(selectedGraphId);
      await deleteGraph(selectedGraphId);
      const loadedGraphs = await fetchGraphs();
      setGraphs(loadedGraphs);
      if (loadedGraphs.length > 0) {
        setSelectedGraphId(pickDefaultGraphId(loadedGraphs));
      } else {
        const blankGraph = createBlankGraph();
        setSelectedGraphId("");
        setSelectedAgentId(null);
        resetHistory(blankGraph);
        setSavedGraphSnapshot(serializeGraphSnapshot(blankGraph));
        setInput(DEFAULT_INPUT);
        setSavedInputPrompt(DEFAULT_INPUT);
        clearLiveRunState();
      }
      setSelectedNodeId(null);
      setSelectedEdgeId(null);
      setError(null);
    } catch (deleteError) {
      const message = deleteError instanceof Error ? deleteError.message : "Unable to delete graph.";
      setError(message);
    }
  }

  async function handleRun() {
    if (!draftGraph) {
      return;
    }
    const agentIdsToRun = isTestEnvironment(draftGraph) ? getSelectedEnvironmentAgentIds(draftGraph, environmentAgentSelection) : undefined;
    if (isTestEnvironment(draftGraph) && (!agentIdsToRun || agentIdsToRun.length === 0)) {
      setError("Turn on at least one agent before running the environment.");
      return;
    }

    const savedGraph = await saveCurrentGraph();
    if (!savedGraph) {
      return;
    }

    clearRunPolling();
    sourceRef.current?.close();
    sourceRef.current = null;
    setError(null);
    cancelPersistedRunSnapshot(savedGraph.graph_id);
    clearPersistedRunSnapshot(savedGraph.graph_id);
    setActiveRunId(null);
    setEvents([]);
    setRunState(null);
    setVisualizerResetVersion((current) => current + 1);
    setIsRunning(true);
    if (isTestEnvironment(savedGraph) && agentIdsToRun && agentIdsToRun.length > 0 && !agentIdsToRun.includes(selectedAgentId ?? "")) {
      setSelectedAgentId(agentIdsToRun[0] ?? null);
    }

    try {
      const runId = await startRun(savedGraph.graph_id, input, { agent_ids: agentIdsToRun });
      setActiveRunId(runId);
      setRunState(createPendingRunState(savedGraph, runId, input, agentIdsToRun));
      connectToRunStream(runId, savedGraph.graph_id, input);
    } catch (runError) {
      const message = runError instanceof Error ? runError.message : "Unable to start run.";
      setError(message);
      setIsRunning(false);
    }
  }

  async function handleResetRuntime() {
    if (!window.confirm("Reset runtime? This will stop active runs, disconnect runtime services, and clear live run state.")) {
      return;
    }
    setIsResettingRuntime(true);
    setError(null);
    try {
      await resetRuntime();
      clearAllPersistedRunSnapshots();
      clearLiveRunState();
      await refreshCatalog();
    } catch (resetError) {
      const message = resetError instanceof Error ? resetError.message : "Unable to reset runtime.";
      setError(message);
    } finally {
      setIsResettingRuntime(false);
    }
  }

  function scrollToExecutionBox() {
    executionBoxRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
  }

  function handleCanvasGraphChange(nextGraph: GraphDefinition) {
    if (!canvasGraph || !draftGraph) {
      return;
    }
    setDraftGraph(updateSelectedAgentGraph(draftGraph, selectedAgentId, nextGraph));
  }

  function handleCanvasGraphDrag(nextGraph: GraphDefinition) {
    if (!canvasGraph || !draftGraph) {
      return;
    }
    setDraftGraphQuiet(updateSelectedAgentGraph(draftGraph, selectedAgentId, nextGraph));
  }

  function handleFormatGraph(nodeDimensions: Record<string, GraphLayoutNodeDimensions>) {
    if (!canvasGraph) {
      return;
    }
    handleCanvasGraphChange(layoutGraphLR(canvasGraph, { nodeDimensions }));
  }

  function handleUpdateUserPreferences(nextPreferences: UserPreferences) {
    setUserPreferences(saveUserPreferences(nextPreferences));
  }

  function handleResetUserPreferences() {
    setUserPreferences(resetUserPreferences());
  }

  return (
    <main className="app-shell">
      <div ref={executionBoxRef} className="hero-section">
        <div className="hero-mosaic">
          <div className="hero-main-column">
            <div className="hero-main-row">
              <div className="mosaic-tile panel mosaic-title">
                <div className="mosaic-title-header">
                  <div className="mosaic-title-copy">
                    <h1>Graph Agent Studio</h1>
                    <p>{isEnvironment ? "Compose a test environment with isolated agents and drill into each run." : "Drag nodes into the canvas, wire edges, and launch your agent."}</p>
                  </div>
                  <button
                    type="button"
                    className="secondary-button mosaic-title-settings-button"
                    aria-label="Open user preferences"
                    title="User preferences"
                    onClick={() => setUserPreferencesOpen(true)}
                  >
                    <svg viewBox="0 0 24 24" aria-hidden="true">
                      <path d="M10.3 3.2h3.4l.5 2.2c.5.2 1 .4 1.5.8l2.1-.8 1.7 2.9-1.7 1.5c.1.3.1.8.1 1.2s0 .8-.1 1.2l1.7 1.5-1.7 2.9-2.1-.8c-.5.3-1 .6-1.5.8l-.5 2.2h-3.4l-.5-2.2c-.5-.2-1-.4-1.5-.8l-2.1.8-1.7-2.9 1.7-1.5a6 6 0 0 1 0-2.4L4.7 8.3 6.4 5.4l2.1.8c.5-.3 1-.6 1.5-.8z" />
                      <circle cx="12" cy="12" r="3.1" />
                    </svg>
                  </button>
                </div>
                <div className="mosaic-title-actions">
                  <button
                    type="button"
                    className="primary-button"
                    onClick={() => void handleRun()}
                    disabled={!draftGraph || isRunning || isSaving || isResettingRuntime}
                  >
                    {isRunning ? "Running..." : isEnvironment ? "Run Environment" : "Run Graph"}
                  </button>
                  <button type="button" className="secondary-button" onClick={handleCreateGraph}>
                    New Agent
                  </button>
                  <button type="button" className="secondary-button" onClick={() => void saveCurrentGraph()} disabled={!draftGraph || isSaving}>
                    {isSaving ? "Saving..." : "Save"}
                  </button>
                  <button type="button" className="secondary-button" onClick={history.undo} disabled={!history.canUndo} title="Undo (⌘Z)">
                    Undo
                  </button>
                  <button type="button" className="secondary-button" onClick={history.redo} disabled={!history.canRedo} title="Redo (⌘⇧Z)">
                    Redo
                  </button>
                  <button type="button" className="danger-button" onClick={() => void handleResetRuntime()} disabled={isResettingRuntime}>
                    {isResettingRuntime ? "Resetting..." : "Reset Runtime"}
                  </button>
                  <button type="button" className="danger-button" onClick={() => void handleDeleteGraph()} disabled={!draftGraph}>
                    Delete
                  </button>
                </div>
                {selectedRunId ? (
                  <code className="mosaic-title-run-id">Run ID: {selectedRunId}</code>
                ) : null}
                {error ? <p className="error-text mosaic-title-error">{error}</p> : null}
              </div>

              <div className="mosaic-tile panel mosaic-graph">
                <label>
                  Graph
                  <select
                    value={selectedGraphId || "__draft__"}
                    onChange={(event) => {
                      if (event.target.value === "__draft__") {
                        handleCreateGraph();
                        return;
                      }
                      setSelectedGraphId(event.target.value);
                    }}
                  >
                    {!selectedGraphId ? <option value="__draft__">Unsaved Draft</option> : null}
                    {graphs.map((graph) => (
                      <option key={graph.graph_id} value={graph.graph_id}>
                        {graph.name}
                      </option>
                    ))}
                  </select>
                </label>
              </div>

              <div className="mosaic-tile panel mosaic-env">
                <h2>Environment</h2>
                <GraphEnvEditor graph={draftGraph} onGraphChange={setDraftGraph} />
              </div>

              <div className="mosaic-tile panel mosaic-execution">
                <label className="mosaic-execution-input">
                  Input
                  <textarea value={input} onChange={(event) => setInput(event.target.value)} rows={7} />
                </label>
              </div>
            </div>

            <div className={`hero-mosaic-row ${isEnvironment && draftGraph ? "" : "hero-mosaic-row--two-up"}`}>
              {isEnvironment && draftGraph ? (
                <div className="mosaic-tile panel mosaic-agents-toggle">
                  <div className="mosaic-section-heading">
                    <span className="mosaic-section-kicker">Execution</span>
                    <div className="environment-run-toggle-header">
                      <strong>Agents To Run</strong>
                      <span>
                        {selectedEnvironmentAgentIds.length} of {draftGraph.agents.length} enabled
                      </span>
                    </div>
                  </div>
                  <div className="environment-run-toggle-actions">
                    <button
                      type="button"
                      className="secondary-button"
                      onClick={() =>
                        setEnvironmentAgentSelection(
                          Object.fromEntries(draftGraph.agents.map((agent) => [agent.agent_id, true])),
                        )
                      }
                      disabled={isRunning || isSaving || isResettingRuntime}
                    >
                      All On
                    </button>
                    <button
                      type="button"
                      className="secondary-button"
                      onClick={() =>
                        setEnvironmentAgentSelection(
                          Object.fromEntries(draftGraph.agents.map((agent) => [agent.agent_id, false])),
                        )
                      }
                      disabled={isRunning || isSaving || isResettingRuntime}
                    >
                      All Off
                    </button>
                  </div>
                  <div className="environment-run-toggle-list">
                    {draftGraph.agents.map((agent) => {
                      const enabled = environmentAgentSelection[agent.agent_id] !== false;
                      return (
                        <button
                          key={agent.agent_id}
                          type="button"
                          className={`environment-run-toggle ${enabled ? "is-enabled" : "is-disabled"}`}
                          aria-pressed={enabled}
                          onClick={() =>
                            setEnvironmentAgentSelection((current) => ({
                              ...current,
                              [agent.agent_id]: current[agent.agent_id] === false,
                            }))
                          }
                          disabled={isRunning || isSaving || isResettingRuntime}
                        >
                          <span>{agent.name}</span>
                          <strong>{enabled ? "On" : "Off"}</strong>
                        </button>
                      );
                    })}
                  </div>
                </div>
              ) : null}
              <div className="mosaic-tile panel mosaic-documents">
                <div className="mosaic-section-heading">
                  <span className="mosaic-section-kicker">Inputs</span>
                  <div className="execution-documents-header">
                    <strong>Project Files</strong>
                    <span>
                      {readyProjectFiles.length} ready
                      {projectFiles.length !== readyProjectFiles.length ? ` / ${projectFiles.length} uploaded` : ""}
                    </span>
                  </div>
                </div>
                <p>
                  Upload reusable files into this project so spreadsheet-backed nodes can select them later.
                </p>
                <label className="execution-documents-picker">
                  <span>{isUploadingProjectFiles ? "Uploading..." : "Add Project Files"}</span>
                  <input
                    type="file"
                    accept=".txt,.md,.markdown,.json,.csv,.xlsx,.pdf,text/plain,text/markdown,text/csv,application/json,application/pdf,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    multiple
                    disabled={!projectFileGraphId || isSaving || isResettingRuntime || isUploadingProjectFiles}
                    onChange={(event) => {
                      void handleProjectFileUpload(projectFileGraphId, event.target.files);
                      event.target.value = "";
                    }}
                  />
                </label>
                {projectFiles.length > 0 ? (
                  <div className="execution-documents-list">
                    {projectFiles.map((file) => (
                      <div key={file.file_id} className={`execution-document-card is-${file.status}`}>
                        <div className="execution-document-card-header">
                          <div>
                            <strong>{file.name}</strong>
                            <span>
                              {formatDocumentSize(file.size_bytes)} · {file.mime_type || "file"}
                            </span>
                          </div>
                          <button
                            type="button"
                            className="secondary-button execution-document-remove"
                            onClick={() => void removeProjectFile(projectFileGraphId, file.file_id)}
                            disabled={!projectFileGraphId || isSaving || isResettingRuntime}
                          >
                            Remove
                          </button>
                        </div>
                        <span className={`execution-document-status execution-document-status--${file.status}`}>{file.status}</span>
                        <pre className="execution-document-excerpt">{file.storage_path}</pre>
                        {file.error ? <span className="execution-document-error">{file.error}</span> : null}
                      </div>
                    ))}
                  </div>
                ) : (
                  <p>{isProjectFilesLoading ? "Loading project files..." : "No project files uploaded yet."}</p>
                )}
                {projectFileError ? <p className="error-text">{projectFileError}</p> : null}
              </div>

              <div className="mosaic-tile panel mosaic-files">
                <div className="execution-files-header">
                  <div className="mosaic-section-heading">
                    <span className="mosaic-section-kicker">Workspace</span>
                    <div className="execution-documents-header">
                      <strong>Agent Files</strong>
                      <span>{visibleRunFiles.length} file{visibleRunFiles.length === 1 ? "" : "s"}</span>
                    </div>
                  </div>
                  <div className="execution-files-actions">
                    <button
                      type="button"
                      className="secondary-button"
                      onClick={() => {
                        if (selectedRunId) {
                          void refreshRunFiles(selectedRunId);
                        }
                      }}
                      disabled={!selectedRunId || isRunFilesLoading}
                    >
                      {isRunFilesLoading ? "Refreshing..." : "Refresh Files"}
                    </button>
                  </div>
                </div>
                <p className="execution-files-intro">
                  Inspect files created in the sandboxed workspace for the selected run
                  {selectedRunState?.agent_name ? ` (${selectedRunState.agent_name})` : ""}.
                </p>
                {runFileListing?.workspace_root ? (
                  <div className="execution-files-workspace">
                    <span>Workspace root</span>
                    <code>{runFileListing.workspace_root}</code>
                  </div>
                ) : null}
                {visibleRunFiles.length > 0 ? (
                  <div className="execution-files-browser">
                    <div className="execution-files-list-panel">
                      <div className="execution-files-browser-header">
                        <strong>Workspace Files</strong>
                        <span>{visibleRunFiles.length} total</span>
                      </div>
                      <div className="execution-files-list" role="list" aria-label="Agent workspace files">
                        {visibleRunFiles.map((file) => (
                          <button
                            key={file.path}
                            type="button"
                            className={`execution-file-row ${selectedRunFilePath === file.path ? "is-selected" : ""}`}
                            onClick={() => setSelectedRunFilePath(file.path)}
                          >
                            <strong>{file.name}</strong>
                            <span className="execution-file-row-path">{file.path}</span>
                            <span>
                              {formatDocumentSize(file.size_bytes)} · {formatTimestamp(file.modified_at)}
                            </span>
                          </button>
                        ))}
                      </div>
                    </div>
                    <div className="execution-file-preview">
                      {selectedRunFile ? (
                        <>
                          <div className="execution-files-browser-header execution-files-browser-header--preview">
                            <div>
                              <strong>{selectedRunFile.name}</strong>
                              <span className="execution-file-preview-path">{selectedRunFile.path}</span>
                            </div>
                            <span>
                              {selectedRunFile.mime_type} · {formatDocumentSize(selectedRunFile.size_bytes)}
                            </span>
                          </div>
                          {isRunFileContentLoading ? <p className="execution-file-preview-empty">Loading file preview...</p> : null}
                          {selectedRunFileContent ? <pre className="execution-file-preview-content">{selectedRunFileContent.content}</pre> : null}
                          {selectedRunFileContent?.truncated ? (
                            <p className="execution-file-preview-note">Preview truncated for large files.</p>
                          ) : null}
                        </>
                      ) : (
                        <p className="execution-file-preview-empty">Select a file to preview it.</p>
                      )}
                    </div>
                  </div>
                ) : (
                  <p className="execution-file-preview-empty">
                    {selectedRunId ? "No files have been written in this run yet." : "Run the graph to inspect workspace files."}
                  </p>
                )}
                {runFilesError ? <p className="error-text">{runFilesError}</p> : null}
                {runFileContentError ? <p className="error-text">{runFileContentError}</p> : null}
              </div>
            </div>
          </div>

        </div>

      </div>

      <section className="content-grid">
        <GraphCanvas
          key={`graph-canvas-${selectedGraphId || "draft"}-${selectedAgentId ?? "all"}-${visualizerResetVersion}`}
          graph={canvasGraph}
          runState={selectedRunState}
          events={filteredEvents}
          activeRunId={selectedRunId}
          isRunning={isRunning}
          runButtonLabel={isEnvironment ? "Run Environment" : "Run Graph"}
          focusedAgentName={isEnvironment ? (environmentRunSummary?.focusedAgentName ?? null) : null}
          focusedAgentStatus={isEnvironment ? focusedRunSummary.status : null}
          environmentAgents={isEnvironment ? agentRunLanes : []}
          selectedAgentId={selectedAgentId}
          onSelectAgent={(agentId) => {
            setSelectedAgentId(agentId);
            setSelectedNodeId(null);
            setSelectedEdgeId(null);
          }}
          runProjection={focusedRunProjection}
          runSummary={focusedRunSummary}
          eventGroups={focusedEventGroups}
          catalog={catalog}
          availableProjectFiles={projectFiles}
          selectedNodeId={selectedNodeId}
          selectedEdgeId={selectedEdgeId}
          onGraphChange={handleCanvasGraphChange}
          onGraphDrag={handleCanvasGraphDrag}
          onFormatGraph={handleFormatGraph}
          onRunGraph={() => void handleRun()}
          onSaveGraph={() => saveCurrentGraph()}
          isSavingGraph={isSaving}
          onScrollToTop={scrollToExecutionBox}
          isMcpPanelOpen={mcpPanelOpen}
          onToggleMcpPanel={() => setMcpPanelOpen((current) => !current)}
          backgroundDragSensitivity={userPreferences.backgroundDragSensitivityPercent / 100}
          onSelectionChange={(nodeId, edgeId) => {
            setSelectedNodeId(nodeId);
            setSelectedEdgeId(edgeId);
          }}
        />
      </section>

      {isTestEnvironment(draftGraph) ? (
        <AgentRunSwimlanes
          key={`agent-swimlanes-${selectedGraphId || "draft"}-${visualizerResetVersion}`}
          lanes={agentRunLanes}
          selectedAgentId={selectedAgentId}
          environmentRunSummary={environmentRunSummary}
          onSelectAgent={(agentId) => setSelectedAgentId(agentId)}
          onSelectNode={(agentId, nodeId) => {
            setSelectedAgentId(agentId);
            setSelectedNodeId(nodeId);
            setSelectedEdgeId(null);
          }}
        />
      ) : null}
      {userPreferencesOpen ? (
        <UserPreferencesModal
          preferences={userPreferences}
          onUpdatePreferences={handleUpdateUserPreferences}
          onResetPreferences={handleResetUserPreferences}
          onClose={() => setUserPreferencesOpen(false)}
        />
      ) : null}
      {mcpPanelOpen ? (
        <McpServerModal
          catalog={catalog}
          onBootMcpServer={(serverId) =>
            void runMcpAction(`boot:${serverId}`, () => bootMcpServer(serverId), (serverStatus) => {
              setCatalog((current) => mergeCatalogServerStatus(current, serverStatus));
            })
          }
          onStopMcpServer={(serverId) =>
            void runMcpAction(`stop:${serverId}`, () => stopMcpServer(serverId), (serverStatus) => {
              setCatalog((current) => mergeCatalogServerStatus(current, serverStatus));
            })
          }
          onRefreshMcpServer={(serverId) =>
            void runMcpAction(`refresh:${serverId}`, () => refreshMcpServer(serverId), (serverStatus) => {
              setCatalog((current) => mergeCatalogServerStatus(current, serverStatus));
            })
          }
          onToggleMcpTool={(toolName, enabled) =>
            void runMcpAction(`tool:${toolName}`, () => setMcpToolEnabled(toolName, enabled), (toolDefinition) => {
              setCatalog((current) => mergeCatalogTool(current, toolDefinition));
            })
          }
          onCreateMcpServer={(server: McpServerDraft) => runMcpAction(`create:${server.server_id}`, () => createMcpServer(server))}
          onUpdateMcpServer={(serverId: string, server: McpServerDraft) =>
            runMcpAction(`update:${serverId}`, () => updateMcpServer(serverId, server))
          }
          onDeleteMcpServer={(serverId: string) => runMcpAction(`delete:${serverId}`, () => deleteMcpServer(serverId))}
          onTestMcpServer={async (server: McpServerDraft) => {
            return runMcpAction(`test:${server.server_id || "draft"}`, () => testMcpServer(server));
          }}
          mcpPendingKey={mcpPendingKey}
          title="Project MCP"
          description="Manage project-level MCP servers. Tool and model nodes can consume these tools, but they do not own the server lifecycle."
          onClose={() => setMcpPanelOpen(false)}
        />
      ) : null}
    </main>
  );
}
