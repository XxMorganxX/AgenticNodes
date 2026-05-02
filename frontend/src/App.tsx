import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { AgentRunSwimlanes } from "./components/AgentRunSwimlanes";
import { DocumentPreviewModal } from "./components/DocumentPreviewModal";
import { GraphCanvas } from "./components/GraphCanvas";
import { GraphDeleteConfirmModal } from "./components/GraphDeleteConfirmModal";
import { GraphEnvEditor } from "./components/GraphEnvEditor";
import { McpServerModal } from "./components/McpServerModal";
import { ProductionRunConfirmModal } from "./components/ProductionRunConfirmModal";
import { RunFilesExplorerModal } from "./components/RunFilesExplorerModal";
import { UserPreferencesModal } from "./components/UserPreferencesModal";
import { WorkflowRemoveConfirmModal } from "./components/WorkflowRemoveConfirmModal";
import {
  bootMcpServer,
  createMcpServer,
  createGraph,
  deleteProjectFile,
  deleteMcpServer,
  deleteGraph,
  eventStreamUrl,
  fetchEditorCatalog,
  fetchProjectFileContent,
  fetchProjectFiles,
  fetchRunFileContent,
  fetchRunFiles,
  fetchGraph,
  fetchGraphs,
  fetchRun,
  fetchRunStatus,
  refreshMcpServer,
  resetRuntime,
  setMcpToolEnabled,
  startListenerSession,
  startRun,
  stopListenerSession,
  stopRuntime,
  stopMcpServer,
  testMcpServer,
  uploadRunDocuments,
  uploadProjectFiles,
  updateMcpServer,
  updateGraph,
} from "./lib/api";
import { createBlankGraph, layoutGraphLR, normalizeGraphDocument } from "./lib/editor";
import type { GraphLayoutNodeDimensions } from "./lib/editor";
import { applyEmailRoutingMode, resolveEmailRoutingMode, syncEmailTableSuffixEnvVar } from "./lib/emailTableRouting";
import {
  filterEventsForAgent,
  getCanvasGraph,
  getDefaultAgentId,
  getListenerStartProvider,
  getSelectedRunFilesRequest,
  getSelectedRunId,
  getSelectedRunState,
  getWebhookPathSlugsForDocument,
  isMultiAgent,
  isTestEnvironment,
  updateSelectedAgentGraph,
} from "./lib/graphDocuments";
import {
  clearPersistedGraphEnvVars,
  draftGraphEnvStorageKey,
  loadPersistedGraphEnvVars,
  persistedGraphEnvStorageKey,
  savePersistedGraphEnvVars,
} from "./lib/persistedGraphEnv";
import { getGraphEnvVars } from "./lib/graphEnv";
import {
  clearPersistedSupabaseConnectionState,
  loadPersistedSupabaseConnectionState,
  savePersistedSupabaseConnectionState,
} from "./lib/persistedSupabaseConnections";
import { loadPersistedSelectedGraphId, savePersistedSelectedGraphId } from "./lib/persistedSelectedGraph";
import { getExplicitSupabaseConnections } from "./lib/supabaseConnections";
import { clearSessionSupabaseSchema } from "./lib/sessionSupabaseSchema";
import { buildWebhookTriggerUrls } from "./lib/webhookUrls";
import { clearAllPersistedRunSnapshots, clearPersistedRunSnapshot, loadPersistedRunSnapshot, savePersistedRunSnapshot } from "./lib/runSnapshots";
import type { PersistedRunSnapshot } from "./lib/runSnapshots";
import { isTerminalRuntimeEvent, normalizeRunState, normalizeRuntimeEvent } from "./lib/runtimeEvents";
import { buildAgentRunLanes, buildEnvironmentRunSummary, buildFocusedRunProjection } from "./lib/runVisualization";
import type {
  AgentDefinition,
  EditorCatalog,
  GraphDefinition,
  GraphDocument,
  LoopRegionState,
  McpServerDraft,
  McpServerStatus,
  ProjectFile,
  RunDocument,
  RunFilesystemFileContent,
  RunFilesystemListing,
  RunState,
  RuntimeEvent,
  SupabaseConnectionDefinition,
  TestEnvironmentDefinition,
  ToolDefinition,
} from "./lib/types";
import { getUserPreferences, resetUserPreferences, saveUserPreferences } from "./lib/userPreferences";
import type { UserPreferences } from "./lib/userPreferences";
import { useGraphHistory } from "./lib/useGraphHistory";
import { usePageVisibility } from "./lib/visibility";

const DEFAULT_INPUT = "Find graph-agent references for a schema repair workflow.";
const DEFAULT_TEST_ENVIRONMENT_ID = "test-environment";
const ENVIRONMENT_AGENT_SELECTION_STORAGE_KEY = "agentic-nodes-environment-agent-selection";
const SELECTED_AGENT_ID_STORAGE_KEY = "agentic-nodes-selected-agent-id";
const SPREADSHEET_ROW_PROVIDER_ID = "core.spreadsheet_rows";

type GraphDeleteTarget = {
  graph_id: string;
  name: string;
};

type WorkflowRemoveTarget = {
  agent_id: string;
  name: string;
};

function getGraphDisplayName(graph: Pick<GraphDocument, "graph_id" | "name">): string {
  return graph.name.trim() || graph.graph_id;
}

function createAgentId(existingAgentIds: Set<string>): string {
  const baseId = `agent-${Date.now()}`;
  if (!existingAgentIds.has(baseId)) {
    return baseId;
  }
  let suffix = 2;
  while (existingAgentIds.has(`${baseId}-${suffix}`)) {
    suffix += 1;
  }
  return `${baseId}-${suffix}`;
}

function graphToAgent(graph: GraphDefinition): AgentDefinition {
  return {
    agent_id: createAgentId(new Set()),
    name: graph.name.trim() || "Agent 1",
    description: graph.description,
    version: graph.version,
    start_node_id: graph.start_node_id,
    env_vars: {},
    nodes: graph.nodes,
    edges: graph.edges,
  };
}

function createBlankAgent(agentName: string, existingAgentIds: Set<string>): AgentDefinition {
  return {
    agent_id: createAgentId(existingAgentIds),
    name: agentName.trim() || `Agent ${existingAgentIds.size + 1}`,
    description: "",
    version: "1.0",
    start_node_id: "",
    env_vars: {},
    nodes: [],
    edges: [],
  };
}

function addAgentToDocument(graph: GraphDocument, agentName: string): { graph: TestEnvironmentDefinition; agentId: string } {
  if (isTestEnvironment(graph)) {
    const existingAgentIds = new Set(graph.agents.map((agent) => agent.agent_id));
    const newAgent = createBlankAgent(agentName, existingAgentIds);
    return {
      graph: {
        ...graph,
        graph_type: "test_environment",
        agents: [...graph.agents, newAgent],
      },
      agentId: newAgent.agent_id,
    };
  }

  const firstAgent = graphToAgent(graph);
  const existingAgentIds = new Set([firstAgent.agent_id]);
  const newAgent = createBlankAgent(agentName, existingAgentIds);
  return {
    graph: {
      graph_id: graph.graph_id,
      name: graph.name,
      description: graph.description,
      version: graph.version,
      graph_type: "test_environment",
      email_routing_mode: graph.email_routing_mode,
      default_input: graph.default_input,
      env_vars: graph.env_vars,
      supabase_connections: graph.supabase_connections,
      default_supabase_connection_id: graph.default_supabase_connection_id,
      run_store_supabase_connection_id: graph.run_store_supabase_connection_id,
      agents: [firstAgent, newAgent],
      node_providers: graph.node_providers,
    },
    agentId: newAgent.agent_id,
  };
}

function removeAgentFromDocument(graph: GraphDocument, agentId: string): { graph: GraphDocument; selectedAgentId: string | null } | null {
  if (!isTestEnvironment(graph)) {
    return null;
  }
  const nextAgents = graph.agents.filter((agent) => agent.agent_id !== agentId);
  if (nextAgents.length === graph.agents.length) {
    return null;
  }
  if (nextAgents.length === 1) {
    const [agent] = nextAgents;
    return {
      graph: {
        graph_id: graph.graph_id,
        name: graph.name,
        description: graph.description,
        version: graph.version,
        graph_type: "graph",
        email_routing_mode: graph.email_routing_mode,
        default_input: graph.default_input,
        env_vars: { ...(graph.env_vars ?? {}), ...(agent.env_vars ?? {}) },
        supabase_connections: graph.supabase_connections,
        default_supabase_connection_id: graph.default_supabase_connection_id,
        run_store_supabase_connection_id: graph.run_store_supabase_connection_id,
        start_node_id: agent.start_node_id,
        nodes: agent.nodes,
        edges: agent.edges,
        node_providers: graph.node_providers,
      },
      selectedAgentId: null,
    };
  }
  return {
    graph: {
      ...graph,
      agents: nextAgents,
    },
    selectedAgentId: nextAgents[0]?.agent_id ?? null,
  };
}

function graphNodes(graph: GraphDocument): GraphDefinition["nodes"] {
  return isTestEnvironment(graph) ? graph.agents.flatMap((agent) => agent.nodes) : graph.nodes;
}

function spreadsheetStartRowValidationMessage(graph: GraphDocument): string | null {
  for (const node of graphNodes(graph)) {
    if (node.provider_id !== SPREADSHEET_ROW_PROVIDER_ID) {
      continue;
    }
    const label = String(node.label ?? node.id);
    const rawValue = node.config.start_row_index;
    if (rawValue == null) {
      continue;
    }
    if (typeof rawValue === "string" && rawValue.trim().length === 0) {
      return `Set a Starting Row Index for '${label}' before running.`;
    }
    const parsed =
      typeof rawValue === "number"
        ? rawValue
        : typeof rawValue === "string"
          ? Number(rawValue.trim())
          : Number.NaN;
    if (!Number.isInteger(parsed)) {
      return `Starting Row Index for '${label}' must be a whole number.`;
    }
    if (parsed < 2) {
      return `Starting Row Index for '${label}' must be 2 or greater.`;
    }
  }
  return null;
}

function collectReferencedSupabaseConnectionIds(graph: GraphDocument): Set<string> {
  const nodes = graphNodes(graph);
  return new Set(
    nodes
      .map((node) => String(node.config.supabase_connection_id ?? "").trim())
      .filter((connectionId) => connectionId.length > 0),
  );
}

function reconcileSupabaseConnections(graph: GraphDocument, ...fallbackGraphs: Array<GraphDocument | null | undefined>): GraphDocument {
  const referencedConnectionIds = collectReferencedSupabaseConnectionIds(graph);
  const currentConnections = getExplicitSupabaseConnections(graph);
  const resolvedConnectionIds = new Set(
    currentConnections.map((connection) => String(connection.connection_id ?? "").trim()).filter(Boolean),
  );
  const rescuedConnections: SupabaseConnectionDefinition[] = [];
  for (const fallbackGraph of fallbackGraphs) {
    if (!fallbackGraph) {
      continue;
    }
    const fallbackConnections = getExplicitSupabaseConnections(fallbackGraph);
    for (const connection of fallbackConnections) {
      const connectionId = String(connection.connection_id ?? "").trim();
      if (connectionId && referencedConnectionIds.has(connectionId) && !resolvedConnectionIds.has(connectionId)) {
        rescuedConnections.push(connection);
        resolvedConnectionIds.add(connectionId);
      }
    }
  }
  const mergedConnections = rescuedConnections.length > 0 ? [...currentConnections, ...rescuedConnections] : currentConnections;
  const currentDefaultConnectionId = String(graph.default_supabase_connection_id ?? "").trim();
  const fallbackDefaultConnectionId = fallbackGraphs.reduce<string>((found, fallback) => {
    if (found) return found;
    const candidate = String(fallback?.default_supabase_connection_id ?? "").trim();
    return candidate && resolvedConnectionIds.has(candidate) ? candidate : "";
  }, "");
  const resolvedDefaultConnectionId =
    currentDefaultConnectionId && resolvedConnectionIds.has(currentDefaultConnectionId)
      ? currentDefaultConnectionId
      : fallbackDefaultConnectionId || "";
  if (
    rescuedConnections.length === 0 &&
    resolvedDefaultConnectionId === currentDefaultConnectionId
  ) {
    return graph;
  }
  return {
    ...graph,
    supabase_connections: mergedConnections,
    default_supabase_connection_id: resolvedDefaultConnectionId,
  };
}

function findMissingSupabaseConnectionIds(graph: GraphDocument): string[] {
  const referencedIds = collectReferencedSupabaseConnectionIds(graph);
  const definedIds = new Set(
    getExplicitSupabaseConnections(graph)
      .map((c) => String(c.connection_id ?? "").trim())
      .filter(Boolean),
  );
  return [...referencedIds].filter((id) => !definedIds.has(id));
}

function isTerminalRunStatus(status: string | null | undefined): boolean {
  return status === "completed" || status === "failed" || status === "cancelled" || status === "interrupted";
}

function formatListenerInputPayload(payload: unknown): string {
  if (payload === null || payload === undefined) {
    return "";
  }
  if (typeof payload === "string") {
    return payload;
  }
  try {
    return JSON.stringify(payload, null, 2);
  } catch {
    return String(payload);
  }
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

function buildIterationId(iteratorNodeId: unknown, iteratorRowIndex: unknown): string | null {
  if (typeof iteratorNodeId !== "string" || !iteratorNodeId) {
    return null;
  }
  if (typeof iteratorRowIndex !== "number" || !Number.isInteger(iteratorRowIndex) || iteratorRowIndex <= 0) {
    return null;
  }
  return `${iteratorNodeId}:row:${iteratorRowIndex}`;
}

function appendUniqueString(values: string[], candidate: unknown): string[] {
  if (typeof candidate !== "string" || !candidate || values.includes(candidate)) {
    return values;
  }
  return [...values, candidate];
}

function resolveIteratorNodeId(payload: Record<string, unknown>): string | null {
  const iteratorNodeId = typeof payload.iterator_node_id === "string" ? payload.iterator_node_id : null;
  if (iteratorNodeId) {
    return iteratorNodeId;
  }
  const nodeId = typeof payload.node_id === "string" ? payload.node_id : null;
  const looksLikeIteratorUpdate =
    payload.iterator_type != null || payload.current_row_index != null || payload.total_rows != null;
  if (nodeId && looksLikeIteratorUpdate) {
    return nodeId;
  }
  return null;
}

function updateLoopRegionState(
  previousRegions: RunState["loop_regions"] | undefined,
  payload: Record<string, unknown>,
  includeStatus = false,
): RunState["loop_regions"] | undefined {
  const iteratorNodeId = resolveIteratorNodeId(payload);
  if (!iteratorNodeId) {
    return previousRegions;
  }
  const nextRegions = { ...(previousRegions ?? {}) };
  const existingRegion = nextRegions[iteratorNodeId];
  const currentRegion = (existingRegion ?? {}) as LoopRegionState;
  let memberNodeIds = Array.isArray(currentRegion.member_node_ids)
    ? currentRegion.member_node_ids.filter((entry): entry is string => typeof entry === "string" && entry.length > 0)
    : [];
  let iterationIds = Array.isArray(currentRegion.iteration_ids)
    ? currentRegion.iteration_ids.filter((entry): entry is string => typeof entry === "string" && entry.length > 0)
    : [];

  const nodeId = typeof payload.node_id === "string" ? payload.node_id : null;
  if (nodeId && nodeId !== iteratorNodeId) {
    memberNodeIds = appendUniqueString(memberNodeIds, nodeId);
  }

  const iteratorRowIndex =
    typeof payload.iterator_row_index === "number"
      ? payload.iterator_row_index
      : typeof payload.current_row_index === "number"
        ? payload.current_row_index
        : null;
  const iterationId =
    typeof payload.iteration_id === "string" && payload.iteration_id
      ? payload.iteration_id
      : buildIterationId(iteratorNodeId, iteratorRowIndex);
  iterationIds = appendUniqueString(iterationIds, iterationId);

  const currentRowIndex =
    typeof payload.current_row_index === "number"
      ? payload.current_row_index
      : iteratorRowIndex ?? currentRegion.current_row_index ?? null;
  const totalRows =
    typeof payload.total_rows === "number"
      ? payload.total_rows
      : typeof payload.iterator_total_rows === "number"
        ? payload.iterator_total_rows
        : currentRegion.total_rows ?? null;
  const status =
    includeStatus && typeof payload.status === "string" && payload.status
      ? payload.status
      : currentRegion.status ?? null;

  nextRegions[iteratorNodeId] = {
    iterator_node_id: iteratorNodeId,
    iterator_type:
      typeof payload.iterator_type === "string"
        ? payload.iterator_type
        : currentRegion.iterator_type ?? null,
    status,
    current_row_index: currentRowIndex,
    total_rows: totalRows,
    active_iteration_id:
      typeof iterationId === "string" && iterationId
        ? iterationId
        : currentRegion.active_iteration_id ?? null,
    member_node_ids: memberNodeIds,
    iteration_ids: iterationIds,
    sheet_name: typeof payload.sheet_name === "string" ? payload.sheet_name : (currentRegion.sheet_name ?? null),
    source_file: typeof payload.source_file === "string" ? payload.source_file : (currentRegion.source_file ?? null),
    file_format: typeof payload.file_format === "string" ? payload.file_format : (currentRegion.file_format ?? null),
  };
  return nextRegions;
}

function resetLoopMemberNodeVisualizerState(
  runState: RunState,
  iteratorNodeId: string,
  previousIterationId: string | null,
  nextIterationId: string | null,
): RunState {
  if (!previousIterationId || !nextIterationId || previousIterationId === nextIterationId) {
    return runState;
  }
  const loopRegion = runState.loop_regions?.[iteratorNodeId];
  const memberNodeIds = Array.isArray(loopRegion?.member_node_ids)
    ? loopRegion.member_node_ids.filter((entry): entry is string => typeof entry === "string" && entry.length > 0)
    : [];
  if (memberNodeIds.length === 0) {
    return runState;
  }
  let nodeInputs = runState.node_inputs ?? {};
  let nodeOutputs = runState.node_outputs ?? {};
  let nodeErrors = runState.node_errors ?? {};
  let visitCounts = runState.visit_counts ?? {};
  const nextNodeStatuses = { ...(runState.node_statuses ?? {}) };
  let didMutate = false;

  for (const nodeId of memberNodeIds) {
    if (nodeId === iteratorNodeId) {
      continue;
    }
    if (nextNodeStatuses[nodeId] !== "idle") {
      nextNodeStatuses[nodeId] = "idle";
      didMutate = true;
    }
    if (Object.prototype.hasOwnProperty.call(nodeInputs, nodeId)) {
      nodeInputs = omitRunStateEntry(nodeInputs, nodeId);
      didMutate = true;
    }
    if (Object.prototype.hasOwnProperty.call(nodeOutputs, nodeId)) {
      nodeOutputs = omitRunStateEntry(nodeOutputs, nodeId);
      didMutate = true;
    }
    if (Object.prototype.hasOwnProperty.call(nodeErrors, nodeId)) {
      nodeErrors = omitRunStateEntry(nodeErrors, nodeId);
      didMutate = true;
    }
    if (Object.prototype.hasOwnProperty.call(visitCounts, nodeId)) {
      visitCounts = omitRunStateEntry(visitCounts, nodeId);
      didMutate = true;
    }
  }

  if (!didMutate) {
    return runState;
  }
  return {
    ...runState,
    node_statuses: nextNodeStatuses,
    node_inputs: nodeInputs,
    node_outputs: nodeOutputs,
    node_errors: nodeErrors,
    visit_counts: visitCounts,
  };
}

function getSavedInputPrompt(graph: GraphDocument | null | undefined): string {
  const savedPrompt = typeof graph?.default_input === "string" ? graph.default_input.trim() : "";
  return savedPrompt || DEFAULT_INPUT;
}

function serializePersistedGraphDocument(graph: GraphDocument): string {
  return JSON.stringify(normalizeGraphDocument(graph));
}

function buildEnvironmentAgentSelection(
  graph: GraphDocument | null | undefined,
  previous: Record<string, boolean> = {},
): Record<string, boolean> {
  if (!isMultiAgent(graph)) {
    return {};
  }
  return Object.fromEntries(graph.agents.map((agent) => [agent.agent_id, previous[agent.agent_id] ?? true]));
}

function getSelectedEnvironmentAgentIds(
  graph: GraphDocument | null | undefined,
  selection: Record<string, boolean>,
): string[] {
  if (!isMultiAgent(graph)) {
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
  if (!isMultiAgent(graph) || !graph.graph_id) {
    return null;
  }
  const persistedAgentId = loadPersistedSelectedAgentIds()[graph.graph_id];
  if (persistedAgentId && graph.agents.some((agent) => agent.agent_id === persistedAgentId)) {
    return persistedAgentId;
  }
  return getDefaultAgentId(graph);
}

function saveSelectedAgentId(graph: GraphDocument | null | undefined, selectedAgentId: string | null | undefined): void {
  if (!isMultiAgent(graph) || !graph.graph_id) {
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

const RUN_STATE_EVENT_HISTORY_LIMIT = 500;
const RUN_STATE_TRANSITION_HISTORY_LIMIT = 500;
const MAX_LIVE_EVENTS = 1000;
const BACKGROUND_BUFFER_LIMIT = 5000;

function appendBoundedHistory<T>(history: T[] | undefined, entry: T, limit: number): T[] {
  const next = [...(history ?? []), entry];
  const overflow = next.length - limit;
  return overflow > 0 ? next.slice(overflow) : next;
}

function appendBoundedEntries<T>(history: T[] | undefined, entries: T[], limit: number): T[] {
  if (entries.length === 0) {
    return history ?? [];
  }
  const next = [...(history ?? []), ...entries];
  const overflow = next.length - limit;
  return overflow > 0 ? next.slice(overflow) : next;
}

function nextEventCount(runState: RunState): number {
  const currentCount =
    typeof runState.event_count === "number" && Number.isFinite(runState.event_count) && runState.event_count >= 0
      ? runState.event_count
      : runState.event_history.length;
  return currentCount + 1;
}

function nextTransitionCount(runState: RunState): number {
  const currentCount =
    typeof runState.transition_count === "number" &&
    Number.isFinite(runState.transition_count) &&
    runState.transition_count >= 0
      ? runState.transition_count
      : runState.transition_history.length;
  return currentCount + 1;
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
    transition_count: 0,
    transition_history: [],
    event_count: 0,
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
  if (!isMultiAgent(graph)) {
    if (isTestEnvironment(graph) && graph.agents.length > 0) {
      const agent = graph.agents[0];
      return {
        ...next,
        node_statuses: buildIdleNodeStatuses(agent.start_node_id, agent.nodes, agent.edges),
      };
    }
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
    event_count: nextEventCount(previous),
    event_history: appendBoundedHistory(previous.event_history, event, RUN_STATE_EVENT_HISTORY_LIMIT),
  };

  if (event.event_type === "run.started") {
    next.status = "running";
    next.status_reason = null;
    next.started_at = event.timestamp;
  }

  if (event.event_type === "node.started") {
    const payload = event.payload as { node_id: string; visit_count: number; received_input?: unknown } & Record<string, unknown>;
    next.current_node_id = payload.node_id;
    next.current_edge_id = null;
    const previousNodeVisitCount = Number(next.visit_counts?.[payload.node_id] ?? 0);
    const expectedVisitCount = previousNodeVisitCount + 1;
    const resolvedVisitCount = payload.visit_count === expectedVisitCount ? payload.visit_count : expectedVisitCount;
    next.visit_counts = {
      ...next.visit_counts,
      [payload.node_id]: resolvedVisitCount,
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
    const nextLoopRegions = updateLoopRegionState(next.loop_regions, payload);
    if (nextLoopRegions) {
      next.loop_regions = nextLoopRegions;
    }
  }

  if (event.event_type === "node.completed") {
    const payload = event.payload as { node_id: string; output?: unknown; error?: unknown } & Record<string, unknown>;
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
    const nextLoopRegions = updateLoopRegionState(next.loop_regions, payload);
    if (nextLoopRegions) {
      next.loop_regions = nextLoopRegions;
    }
  }

  if (event.event_type === "node.iterator.updated") {
    const payload = event.payload as Record<string, unknown>;
    const nodeId = typeof payload.node_id === "string" ? payload.node_id : null;
    const iteratorNodeId = resolveIteratorNodeId(payload);
    const previousIterationId =
      iteratorNodeId && next.loop_regions?.[iteratorNodeId] && typeof next.loop_regions[iteratorNodeId]?.active_iteration_id === "string"
        ? next.loop_regions[iteratorNodeId]?.active_iteration_id ?? null
        : null;
    if (nodeId) {
      next.iterator_states = {
        ...(next.iterator_states ?? {}),
        [nodeId]: Object.fromEntries(
          Object.entries(payload).filter(
            ([key]) => !["node_id", "iterator_node_id", "iterator_row_index", "iterator_total_rows", "iteration_id"].includes(key),
          ),
        ),
      };
    }
    const nextLoopRegions = updateLoopRegionState(next.loop_regions, payload, true);
    if (nextLoopRegions) {
      next.loop_regions = nextLoopRegions;
    }
    const nextIterationId =
      iteratorNodeId && next.loop_regions?.[iteratorNodeId] && typeof next.loop_regions[iteratorNodeId]?.active_iteration_id === "string"
        ? next.loop_regions[iteratorNodeId]?.active_iteration_id ?? null
        : null;
    if (iteratorNodeId) {
      Object.assign(next, resetLoopMemberNodeVisualizerState(next, iteratorNodeId, previousIterationId, nextIterationId));
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
    next.transition_count = nextTransitionCount(next);
    next.transition_history = appendBoundedHistory(
      next.transition_history,
      {
        edge_id: payload.id,
        source_id: payload.source_id,
        target_id: payload.target_id,
        timestamp: event.timestamp,
      },
      RUN_STATE_TRANSITION_HISTORY_LIMIT,
    );
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
      event_count: nextEventCount(next),
      event_history: appendBoundedHistory(next.event_history, event, RUN_STATE_EVENT_HISTORY_LIMIT),
      agent_runs: {
        ...(next.agent_runs ?? {}),
        [agentId]: applySingleRunEvent(priorAgentRun, normalizedEvent),
      },
    };
  }
  return applySingleRunEvent(next, event);
}

function pickDefaultGraphId(graphs: GraphDocument[], preferredId?: string | null): string {
  if (preferredId && graphs.some((graph) => graph.graph_id === preferredId)) {
    return preferredId;
  }
  return graphs.find((graph) => graph.graph_id === DEFAULT_TEST_ENVIRONMENT_ID)?.graph_id ?? graphs[0]?.graph_id ?? "";
}

function applyPersistedEnvVars(graph: GraphDocument, storageKey: string | null | undefined): GraphDocument {
  const persistedEnvVars = loadPersistedGraphEnvVars(storageKey);
  if (!persistedEnvVars) {
    return syncEmailTableSuffixEnvVar(graph);
  }
  return syncEmailTableSuffixEnvVar({
    ...graph,
    env_vars: {
      ...(graph.env_vars ?? {}),
      ...persistedEnvVars,
    },
  });
}

function applyPersistedSupabaseConnectionState(graph: GraphDocument, storageKey: string | null | undefined): GraphDocument {
  const persistedState = loadPersistedSupabaseConnectionState(storageKey);
  if (!persistedState) {
    return graph;
  }
  const referencedConnectionIds = collectReferencedSupabaseConnectionIds(graph);
  const savedConnections = Array.isArray(graph.supabase_connections) ? graph.supabase_connections : [];
  const savedConnectionIds = new Set(
    savedConnections.map((connection) => String(connection.connection_id ?? "").trim()).filter(Boolean),
  );
  const savedDefaultConnectionId = String(graph.default_supabase_connection_id ?? "").trim();
  const persistedDefaultConnectionId = String(persistedState.default_supabase_connection_id ?? "").trim();
  const savedRunStoreConnectionId = String(graph.run_store_supabase_connection_id ?? "").trim();
  const persistedRunStoreConnectionId = String(persistedState.run_store_supabase_connection_id ?? "").trim();
  const supplementalPersistedConnections = persistedState.supabase_connections.filter((connection) => {
    const connectionId = String(connection.connection_id ?? "").trim();
    if (!connectionId || savedConnectionIds.has(connectionId)) {
      return false;
    }
    if (savedConnections.length === 0) {
      return true;
    }
    return referencedConnectionIds.has(connectionId) || connectionId === persistedDefaultConnectionId;
  });
  const mergedConnections = [...savedConnections, ...supplementalPersistedConnections];
  const mergedConnectionIds = new Set(
    mergedConnections.map((connection) => String(connection.connection_id ?? "").trim()).filter(Boolean),
  );
  const resolvedDefaultConnectionId =
    savedDefaultConnectionId && mergedConnectionIds.has(savedDefaultConnectionId)
      ? savedDefaultConnectionId
      : persistedDefaultConnectionId && mergedConnectionIds.has(persistedDefaultConnectionId)
        ? persistedDefaultConnectionId
        : "";
  const resolvedRunStoreConnectionId =
    savedRunStoreConnectionId && mergedConnectionIds.has(savedRunStoreConnectionId)
      ? savedRunStoreConnectionId
      : persistedRunStoreConnectionId && mergedConnectionIds.has(persistedRunStoreConnectionId)
        ? persistedRunStoreConnectionId
        : "";
  if (
    supplementalPersistedConnections.length === 0
    && resolvedDefaultConnectionId === savedDefaultConnectionId
    && resolvedRunStoreConnectionId === savedRunStoreConnectionId
  ) {
    return graph;
  }
  return {
    ...graph,
    supabase_connections: mergedConnections,
    default_supabase_connection_id: resolvedDefaultConnectionId,
    run_store_supabase_connection_id: resolvedRunStoreConnectionId,
  };
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
  const { graph: draftGraph, stateId: draftGraphStateId, set: setDraftGraph, setQuiet: setDraftGraphQuiet, reset: resetHistory } = history;
  const [savedGraphStateId, setSavedGraphStateId] = useState(0);
  const [catalog, setCatalog] = useState<EditorCatalog | null>(null);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [selectedEdgeId, setSelectedEdgeId] = useState<string | null>(null);
  const [visualizerResetVersion, setVisualizerResetVersion] = useState(0);
  const [input, setInput] = useState(DEFAULT_INPUT);
  const [savedInputPrompt, setSavedInputPrompt] = useState(DEFAULT_INPUT);
  const [projectFiles, setProjectFiles] = useState<ProjectFile[]>([]);
  const [runDocuments, setRunDocuments] = useState<RunDocument[]>([]);
  const [runFileListing, setRunFileListing] = useState<RunFilesystemListing | null>(null);
  const [selectedRunFilePath, setSelectedRunFilePath] = useState<string | null>(null);
  const [selectedRunFileContent, setSelectedRunFileContent] = useState<RunFilesystemFileContent | null>(null);
  const [followLatestRunFile, setFollowLatestRunFile] = useState(true);
  const [isRunFilesLoading, setIsRunFilesLoading] = useState(false);
  const [isRunFileContentLoading, setIsRunFileContentLoading] = useState(false);
  const [runFilesError, setRunFilesError] = useState<string | null>(null);
  const [runFileContentError, setRunFileContentError] = useState<string | null>(null);
  const [isRunFilesExplorerOpen, setIsRunFilesExplorerOpen] = useState(false);
  const [isUploadingProjectFiles, setIsUploadingProjectFiles] = useState(false);
  const [isUploadingRunDocuments, setIsUploadingRunDocuments] = useState(false);
  const [isProjectFilesLoading, setIsProjectFilesLoading] = useState(false);
  const [projectFileError, setProjectFileError] = useState<string | null>(null);
  const [expandedProjectFileSources, setExpandedProjectFileSources] = useState<Record<string, boolean>>({
    upload: true,
    scripts: true,
  });
  const [runDocumentError, setRunDocumentError] = useState<string | null>(null);
  const [documentPreview, setDocumentPreview] = useState<{
    title: string;
    subtitle: string;
    content: string;
    isLoading: boolean;
    truncated: boolean;
    error: string | null;
  } | null>(null);
  const [events, setEvents] = useState<RuntimeEvent[]>([]);
  const [runState, setRunState] = useState<RunState | null>(null);
  const [activeRunId, setActiveRunId] = useState<string | null>(null);
  const [selectedListenerChildRunId, setSelectedListenerChildRunId] = useState<string | null>(null);
  const [listenerEndpointCopyKey, setListenerEndpointCopyKey] = useState<string | null>(null);
  const [listenerChildRunStates, setListenerChildRunStates] = useState<Record<string, RunState>>({});
  const [listenerChildRunError, setListenerChildRunError] = useState<string | null>(null);
  const [isLoadingListenerChildRun, setIsLoadingListenerChildRun] = useState(false);
  const [isRunning, setIsRunning] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [isStoppingRuntime, setIsStoppingRuntime] = useState(false);
  const [isResettingRuntime, setIsResettingRuntime] = useState(false);
  const [productionRunConfirmOpen, setProductionRunConfirmOpen] = useState(false);
  const [deleteGraphTarget, setDeleteGraphTarget] = useState<GraphDeleteTarget | null>(null);
  const [isDeletingGraph, setIsDeletingGraph] = useState(false);
  const [workflowRemoveTarget, setWorkflowRemoveTarget] = useState<WorkflowRemoveTarget | null>(null);
  const [isRenamingGraph, setIsRenamingGraph] = useState(false);
  const [graphNameDraft, setGraphNameDraft] = useState("");
  const [graphNameError, setGraphNameError] = useState<string | null>(null);
  const [mcpPendingKey, setMcpPendingKey] = useState<string | null>(null);
  const [mcpPanelOpen, setMcpPanelOpen] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [userPreferences, setUserPreferences] = useState<UserPreferences>(() => getUserPreferences());
  const [userPreferencesOpen, setUserPreferencesOpen] = useState(false);
  const sourceRef = useRef<EventSource | null>(null);
  const pendingEventsRef = useRef<RuntimeEvent[]>([]);
  const flushFrameRef = useRef<number | null>(null);
  const isTabHiddenRef = useRef<boolean>(typeof document !== "undefined" ? document.hidden : false);
  const flushPendingEventsRef = useRef<() => void>(() => {});
  const runPollTimeoutRef = useRef<number | null>(null);
  const runPollDelayMsRef = useRef<number>(5000);
  const runStateRef = useRef<RunState | null>(null);
  const draftGraphRef = useRef<GraphDocument | null>(draftGraph);
  const graphsRef = useRef<GraphDocument[]>(graphs);
  const selectedAgentIdRef = useRef<string | null>(selectedAgentId);
  const inputRef = useRef(input);
  const pendingRunSnapshotRef = useRef<PersistedRunSnapshot | null>(null);
  const persistRunSnapshotTimeoutRef = useRef<number | null>(null);
  const pendingBackgroundPersistGraphRef = useRef<GraphDocument | null>(null);
  const backgroundPersistInFlightRef = useRef(false);
  const backgroundPersistedSnapshotRef = useRef("");
  const executionBoxRef = useRef<HTMLDivElement | null>(null);

  const canvasGraph = useMemo(() => getCanvasGraph(draftGraph, selectedAgentId), [draftGraph, selectedAgentId]);
  const selectedRunState = useMemo(() => getSelectedRunState(runState, selectedAgentId), [runState, selectedAgentId]);
  const selectedRunId = useMemo(() => getSelectedRunId(runState, activeRunId, selectedAgentId), [runState, activeRunId, selectedAgentId]);
  const selectedRunFilesRequest = useMemo(
    () => getSelectedRunFilesRequest(runState, activeRunId, selectedAgentId),
    [runState, activeRunId, selectedAgentId],
  );
  const filteredEvents = useMemo(() => filterEventsForAgent(events, selectedAgentId), [events, selectedAgentId]);
  const fileRefreshTrigger = useMemo(
    () =>
      filteredEvents.reduce((count, event) => {
        const eventType = event.event_type;
        return eventType === "node.completed" || eventType === "agent.node.completed" ? count + 1 : count;
      }, 0),
    [filteredEvents],
  );
  const persistedGraphIds = useMemo(() => new Set(graphs.map((graph) => graph.graph_id)), [graphs]);
  const isEnvironment = isMultiAgent(draftGraph);
  const listenerStartProvider = useMemo(
    () => getListenerStartProvider(canvasGraph, catalog),
    [canvasGraph, catalog],
  );
  const isListenerGraph = listenerStartProvider !== null;
  const isListeningSession = isListenerGraph && isRunning;
  const isWebhookListenerSessionActive =
    isListeningSession && listenerStartProvider?.provider_id === "start.webhook";
  const listenerWebhookEndpoints = useMemo(() => {
    if (!isListeningSession || !draftGraph) {
      return [] as Array<{ slug: string; localUrl: string; publicUrl: string | null }>;
    }
    const slugs = getWebhookPathSlugsForDocument(draftGraph);
    const host = catalog?.cloudflare?.public_hostname;
    return slugs.map((slug) => ({ slug, ...buildWebhookTriggerUrls(slug, host) }));
  }, [isListeningSession, draftGraph, catalog]);
  const childRunSummaries = useMemo(() => {
    if (!runState) {
      return [] as Array<{ run_id: string; received_at: string; payload: Record<string, unknown> }>;
    }
    return (runState.event_history ?? [])
      .filter((event) => event.event_type === "listener.event.received")
      .map((event) => {
        const payload = (event.payload ?? {}) as Record<string, unknown>;
        const childRunId = String(payload.child_run_id ?? "");
        return {
          run_id: childRunId,
          received_at: event.timestamp ?? "",
          payload,
        };
      })
      .filter((entry) => entry.run_id.length > 0);
  }, [runState]);
  const selectedListenerChildRunState = selectedListenerChildRunId
    ? listenerChildRunStates[selectedListenerChildRunId] ?? null
    : null;
  const selectedListenerChildRunInput = formatListenerInputPayload(selectedListenerChildRunState?.input_payload);
  const emailRoutingMode = useMemo(() => resolveEmailRoutingMode(draftGraph), [draftGraph]);
  const selectedEnvironmentAgentIds = useMemo(
    () => getSelectedEnvironmentAgentIds(draftGraph, environmentAgentSelection),
    [draftGraph, environmentAgentSelection],
  );
  const environmentRunSummary = useMemo(
    () => buildEnvironmentRunSummary(draftGraph, runState, selectedAgentId),
    [draftGraph, runState, selectedAgentId],
  );
  const agentRunLanes = useMemo(() => buildAgentRunLanes(draftGraph, runState, events), [draftGraph, runState, events]);
  const canvasRunState = isListenerGraph && selectedListenerChildRunState ? selectedListenerChildRunState : selectedRunState;
  const canvasEvents =
    isListenerGraph && selectedListenerChildRunState ? selectedListenerChildRunState.event_history ?? [] : filteredEvents;
  const canvasActiveRunId =
    isListenerGraph && selectedListenerChildRunState ? selectedListenerChildRunState.run_id : selectedRunId;
  const focusedRunProjection = useMemo(
    () => buildFocusedRunProjection(canvasGraph, canvasRunState, canvasEvents),
    [canvasGraph, canvasRunState, canvasEvents],
  );
  const focusedRunSummary = focusedRunProjection.runSummary;
  const focusedEventGroups = focusedRunProjection.eventGroups;
  useEffect(() => {
    if (!isListenerGraph || childRunSummaries.length === 0) {
      setSelectedListenerChildRunId(null);
      return;
    }
    setSelectedListenerChildRunId(childRunSummaries[childRunSummaries.length - 1]?.run_id ?? null);
  }, [childRunSummaries, isListenerGraph]);

  useEffect(() => {
    if (!isListenerGraph || !selectedListenerChildRunId) {
      setListenerChildRunError(null);
      setIsLoadingListenerChildRun(false);
      return;
    }

    let cancelled = false;
    let refreshTimeout: number | null = null;

    const loadChildRun = async () => {
      setIsLoadingListenerChildRun(true);
      try {
        const nextRunState = await fetchRun(selectedListenerChildRunId);
        if (cancelled) {
          return;
        }
        setListenerChildRunStates((previous) => ({
          ...previous,
          [nextRunState.run_id]: nextRunState,
        }));
        setListenerChildRunError(null);
        if (!isTerminalRunStatus(nextRunState.status)) {
          refreshTimeout = window.setTimeout(loadChildRun, 1500);
        }
      } catch (childRunError) {
        if (!cancelled) {
          setListenerChildRunError(
            childRunError instanceof Error ? childRunError.message : "Unable to load child run data.",
          );
        }
      } finally {
        if (!cancelled) {
          setIsLoadingListenerChildRun(false);
        }
      }
    };

    void loadChildRun();

    return () => {
      cancelled = true;
      if (refreshTimeout !== null) {
        window.clearTimeout(refreshTimeout);
      }
    };
  }, [isListenerGraph, selectedListenerChildRunId]);

  const hasUnsavedChanges = (Boolean(draftGraph) && draftGraphStateId !== savedGraphStateId) || input !== savedInputPrompt;
  const projectFileGraphId = selectedGraphId || draftGraph?.graph_id || "";
  const readyProjectFiles = useMemo(() => projectFiles.filter((file) => file.status === "ready"), [projectFiles]);
  const uploadedProjectFiles = useMemo(
    () => projectFiles.filter((file) => (file.source ?? "upload") === "upload"),
    [projectFiles],
  );
  const scriptsProjectFiles = useMemo(
    () => projectFiles.filter((file) => file.source === "scripts"),
    [projectFiles],
  );
  const readyRunDocuments = useMemo(() => runDocuments.filter((document) => document.status === "ready"), [runDocuments]);
  const visibleRunFiles = useMemo(() => {
    const files = runFileListing?.files ?? [];
    const agentId = selectedRunFilesRequest.agentId;
    if (!agentId) {
      return files;
    }
    return files.filter((file) => file.agent_id === agentId);
  }, [runFileListing, selectedRunFilesRequest.agentId]);
  const selectedRunFile = useMemo(
    () => visibleRunFiles.find((file) => file.path === selectedRunFilePath) ?? null,
    [visibleRunFiles, selectedRunFilePath],
  );
  const recentRunFiles = useMemo(
    () =>
      [...visibleRunFiles]
        .sort((left, right) => new Date(right.modified_at).getTime() - new Date(left.modified_at).getTime())
        .slice(0, 3),
    [visibleRunFiles],
  );

  useEffect(() => {
    runStateRef.current = runState;
  }, [runState]);

  useEffect(() => {
    draftGraphRef.current = draftGraph;
  }, [draftGraph]);

  useEffect(() => {
    graphsRef.current = graphs;
  }, [graphs]);

  useEffect(() => {
    selectedAgentIdRef.current = selectedAgentId;
  }, [selectedAgentId]);

  useEffect(() => {
    inputRef.current = input;
  }, [input]);

  useEffect(() => {
    if (!isRunning) {
      setIsStoppingRuntime(false);
    }
  }, [isRunning]);

  const refreshRunFiles = useCallback(async (runId: string, agentId: string | null = null) => {
    setRunFilesError(null);
    setIsRunFilesLoading(true);
    try {
      const listing = await fetchRunFiles(runId);
      const candidateFiles = agentId ? listing.files.filter((file) => file.agent_id === agentId) : listing.files;
      const latestFilePath =
        [...candidateFiles]
          .sort((left, right) => new Date(right.modified_at).getTime() - new Date(left.modified_at).getTime())[0]?.path ?? null;
      setRunFileListing(listing);
      setSelectedRunFilePath((current) => {
        if (followLatestRunFile) {
          return latestFilePath;
        }
        if (current && candidateFiles.some((file) => file.path === current)) {
          return current;
        }
        return latestFilePath;
      });
    } catch (loadError) {
      const message = loadError instanceof Error ? loadError.message : "Unable to load run files.";
      setRunFilesError(message);
      setRunFileListing(null);
      setSelectedRunFilePath(null);
    } finally {
      setIsRunFilesLoading(false);
    }
  }, [followLatestRunFile]);

  useEffect(() => {
    if (!selectedRunFilesRequest.runId) {
      setRunFileListing(null);
      setSelectedRunFilePath(null);
      setSelectedRunFileContent(null);
      setFollowLatestRunFile(true);
      setRunFilesError(null);
      setRunFileContentError(null);
      setIsRunFilesExplorerOpen(false);
      return;
    }
    void refreshRunFiles(selectedRunFilesRequest.runId, selectedRunFilesRequest.agentId);
  }, [refreshRunFiles, selectedRunFilesRequest.runId, selectedRunFilesRequest.agentId, fileRefreshTrigger]);

  useEffect(() => {
    if (!selectedRunFilesRequest.runId || !selectedRunFilePath) {
      setSelectedRunFileContent(null);
      setRunFileContentError(null);
      return;
    }
    let cancelled = false;
    setRunFileContentError(null);
    setIsRunFileContentLoading(true);
    void fetchRunFileContent(selectedRunFilesRequest.runId, selectedRunFilePath)
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
  }, [selectedRunFilePath, selectedRunFilesRequest.runId]);

  const refreshCatalog = useCallback(async () => {
    const loadedCatalog = await fetchEditorCatalog();
    setCatalog(loadedCatalog);
    return loadedCatalog;
  }, []);

  const buildPersistableGraphDocument = useCallback((sourceGraph: GraphDocument): GraphDocument => {
    const storageKey = persistedGraphEnvStorageKey(sourceGraph.graph_id, sourceGraph.graph_id);
    const savedGraphFallback = graphsRef.current.find((graph) => graph.graph_id === sourceGraph.graph_id) ?? null;
    const hydratedGraph = reconcileSupabaseConnections(
      applyPersistedSupabaseConnectionState(
        applyPersistedEnvVars(sourceGraph, storageKey),
        storageKey,
      ),
      savedGraphFallback,
      ...graphsRef.current.filter((graph) => graph.graph_id !== sourceGraph.graph_id),
    );
    const missingConnectionIds = findMissingSupabaseConnectionIds(hydratedGraph);
    if (missingConnectionIds.length > 0) {
      const label = missingConnectionIds.length === 1
        ? `Supabase connection "${missingConnectionIds[0]}" is`
        : `Supabase connections ${missingConnectionIds.map((id) => `"${id}"`).join(", ")} are`;
      throw new Error(
        `${label} referenced by nodes but not defined on this graph. `
        + "Open the Supabase Connections panel in the Environment section to add the missing connection, "
        + "or switch affected nodes to a different connection.",
      );
    }
    return {
      ...normalizeGraphDocument(hydratedGraph),
      default_input: inputRef.current,
    } satisfies GraphDocument;
  }, []);

  const persistPendingGraphInBackground = useCallback(async () => {
    if (backgroundPersistInFlightRef.current) {
      return;
    }
    const pendingGraph = pendingBackgroundPersistGraphRef.current;
    if (!pendingGraph) {
      return;
    }
    if (!pendingGraph.graph_id || !graphsRef.current.some((graph) => graph.graph_id === pendingGraph.graph_id)) {
      pendingBackgroundPersistGraphRef.current = null;
      return;
    }

    pendingBackgroundPersistGraphRef.current = null;
    backgroundPersistInFlightRef.current = true;

    try {
      const normalized = buildPersistableGraphDocument(pendingGraph);
      const savedGraph = await updateGraph(pendingGraph.graph_id, normalized);
      backgroundPersistedSnapshotRef.current = serializePersistedGraphDocument(normalized);
      setGraphs((current) => current.map((graph) => (graph.graph_id === savedGraph.graph_id ? savedGraph : graph)));
    } catch (error) {
      console.error("Background graph persistence failed.", error);
    } finally {
      backgroundPersistInFlightRef.current = false;
      if (pendingBackgroundPersistGraphRef.current) {
        void persistPendingGraphInBackground();
      }
    }
  }, [buildPersistableGraphDocument]);

  const hydrateSelectedGraph = useCallback((graph: GraphDocument) => {
    const storageKey = persistedGraphEnvStorageKey(graph.graph_id, graph.graph_id);
    const nextGraph = reconcileSupabaseConnections(
      applyPersistedSupabaseConnectionState(
        applyPersistedEnvVars(normalizeGraphDocument(graph), storageKey),
        storageKey,
      ),
      graph,
    );
    const nextStateId = resetHistory(nextGraph);
    setSavedGraphStateId(nextStateId);
    const nextInput = getSavedInputPrompt(nextGraph);
    setInput(nextInput);
    setSavedInputPrompt(nextInput);
    setSelectedAgentId(loadSelectedAgentId(nextGraph));
    setSelectedNodeId(null);
    setSelectedEdgeId(null);
    pendingBackgroundPersistGraphRef.current = null;
    backgroundPersistedSnapshotRef.current = serializePersistedGraphDocument(nextGraph);
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

  const handleRunDocumentUpload = useCallback(async (fileList: FileList | null) => {
    if (!fileList || fileList.length === 0) {
      return;
    }
    setRunDocumentError(null);
    setIsUploadingRunDocuments(true);
    try {
      const uploadedDocuments = await uploadRunDocuments(Array.from(fileList));
      setRunDocuments((current) => [...current, ...uploadedDocuments]);
    } catch (uploadError) {
      const message = uploadError instanceof Error ? uploadError.message : "Unable to upload run documents.";
      setRunDocumentError(message);
    } finally {
      setIsUploadingRunDocuments(false);
    }
  }, []);

  const removeRunDocument = useCallback((documentId: string) => {
    setRunDocumentError(null);
    setRunDocuments((current) => current.filter((document) => document.document_id !== documentId));
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

  const viewRunDocument = useCallback((documentId: string) => {
    setRunDocuments((current) => {
      const match = current.find((document) => document.document_id === documentId);
      if (match) {
        setDocumentPreview({
          title: match.name,
          subtitle: `${match.mime_type || "file"} · ${formatDocumentSize(match.size_bytes)}`,
          content: match.text_content || match.text_excerpt || "",
          isLoading: false,
          truncated: false,
          error: match.error ?? null,
        });
      }
      return current;
    });
  }, []);

  const viewProjectFile = useCallback(async (graphId: string, fileId: string) => {
    if (!graphId.trim()) {
      return;
    }
    const seed = projectFiles.find((file) => file.file_id === fileId);
    setDocumentPreview({
      title: seed?.name ?? "Project file",
      subtitle: seed ? `${seed.mime_type || "file"} · ${formatDocumentSize(seed.size_bytes)}` : "",
      content: "",
      isLoading: true,
      truncated: false,
      error: null,
    });
    try {
      const payload = await fetchProjectFileContent(graphId, fileId);
      setDocumentPreview({
        title: payload.name,
        subtitle: `${payload.mime_type || "file"} · ${formatDocumentSize(payload.size_bytes)}`,
        content: payload.content,
        isLoading: false,
        truncated: payload.truncated,
        error: payload.error ?? null,
      });
    } catch (viewError) {
      const message = viewError instanceof Error ? viewError.message : "Unable to load project file content.";
      setDocumentPreview((current) =>
        current
          ? { ...current, isLoading: false, error: message }
          : { title: seed?.name ?? "Project file", subtitle: "", content: "", isLoading: false, truncated: false, error: message },
      );
    }
  }, [projectFiles]);

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
        if (!draftGraph || isSaving || isRunning || isResettingRuntime || isUploadingRunDocuments) {
          return;
        }
        void handleRun();
      }
    }

    window.addEventListener("keydown", handleKeyDown, { capture: true });
    return () => window.removeEventListener("keydown", handleKeyDown, { capture: true });
  }, [draftGraph, handleRun, isResettingRuntime, isRunning, isSaving, isUploadingRunDocuments, saveCurrentGraph, userPreferences.keyboardShortcuts]);

  const clearRunPolling = useCallback(() => {
    runPollDelayMsRef.current = 5000;
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
    if (runPollTimeoutRef.current !== null) {
      window.clearTimeout(runPollTimeoutRef.current);
      runPollTimeoutRef.current = null;
    }
    const delayMs = runPollDelayMsRef.current;
    // Exponential backoff capped at 30s. Each successive poll on a stuck run
    // doubles the wait — this caps Supabase egress in the SSE-fallback path.
    runPollDelayMsRef.current = Math.min(delayMs * 2, 30000);
    runPollTimeoutRef.current = window.setTimeout(() => {
      void fetchRunStatus(runId)
        .then((status) => {
          if (status.is_terminal) {
            // Terminal — fetch full state once so the UI reflects final outputs.
            return fetchRun(runId).then((nextRunState) => {
              applyFetchedRunState(nextRunState);
              cancelPersistedRunSnapshot(graphId);
              clearPersistedRunSnapshot(graphId);
              clearRunPolling();
            });
          }
          scheduleRunPoll(runId, graphId);
          return undefined;
        })
        .catch(() => {
          markRecoveredRunInterrupted(graphId, runStateRef.current, runId);
        });
    }, delayMs);
  }, [applyFetchedRunState, clearRunPolling, markRecoveredRunInterrupted, cancelPersistedRunSnapshot]);

  const connectToRunStream = useCallback((runId: string, graphId: string, inputValue: string, documents: RunDocument[] = []) => {
    clearRunPolling();
    sourceRef.current?.close();
    if (flushFrameRef.current !== null) {
      cancelAnimationFrame(flushFrameRef.current);
      flushFrameRef.current = null;
    }
    pendingEventsRef.current = [];

    const source = new EventSource(eventStreamUrl(runId));
    sourceRef.current = source;

    const flushPendingEvents = () => {
      flushFrameRef.current = null;
      const buffered = pendingEventsRef.current;
      if (buffered.length === 0) {
        return;
      }
      pendingEventsRef.current = [];

      setEvents((previous) => appendBoundedEntries(previous, buffered, MAX_LIVE_EVENTS));
      setRunState((previous) =>
        buffered.reduce(
          (acc, event) => applyEvent(acc, event, graphId, inputValue, documents),
          previous,
        ),
      );

      const terminal = buffered.find((event) => !event.agent_id && isTerminalRuntimeEvent(event));
      if (terminal && sourceRef.current === source) {
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

    flushPendingEventsRef.current = flushPendingEvents;

    const scheduleFlush = () => {
      if (isTabHiddenRef.current) {
        // Tab is backgrounded — keep accumulating in the buffer (capped) and flush on refocus.
        if (pendingEventsRef.current.length > BACKGROUND_BUFFER_LIMIT) {
          pendingEventsRef.current = pendingEventsRef.current.slice(-BACKGROUND_BUFFER_LIMIT);
        }
        return;
      }
      if (flushFrameRef.current === null) {
        flushFrameRef.current = requestAnimationFrame(flushPendingEvents);
      }
    };

    source.onmessage = (message) => {
      const event = normalizeRuntimeEvent(JSON.parse(message.data) as RuntimeEvent);
      pendingEventsRef.current.push(event);
      scheduleFlush();
    };

    source.onerror = () => {
      source.close();
      sourceRef.current = null;
      if (flushFrameRef.current !== null) {
        cancelAnimationFrame(flushFrameRef.current);
        flushFrameRef.current = null;
      }
      if (pendingEventsRef.current.length > 0) {
        flushPendingEvents();
      }
      setIsRunning(false);
      scheduleRunPoll(runId, graphId);
    };
  }, [applyFetchedRunState, cancelPersistedRunSnapshot, clearRunPolling, scheduleRunPoll]);

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
    // Cheap pre-check: ask the backend only for status. If the run is
    // already terminal we never need to pull the (potentially huge) full
    // state + event history from Supabase.
    try {
      const status = await fetchRunStatus(snapshotRunId);
      if (status.is_terminal) {
        cancelPersistedRunSnapshot(graphId);
        clearPersistedRunSnapshot(graphId);
        setActiveRunId(null);
        setEvents([]);
        setRunState(null);
        setIsRunning(false);
        return;
      }
      const recoveredRunState = await fetchRun(snapshotRunId);
      applyFetchedRunState(recoveredRunState);
      connectToRunStream(recoveredRunState.run_id, graphId, inputRef.current, recoveredRunState.documents ?? []);
    } catch {
      markRecoveredRunInterrupted(graphId, shouldHydrateLocalSnapshot ? snapshotRunState : null, snapshotRunId, snapshot?.savedAt);
    }
  }, [applyFetchedRunState, clearRunPolling, connectToRunStream, markRecoveredRunInterrupted, cancelPersistedRunSnapshot]);

  useEffect(() => {
    const persistedSelectedGraphId = loadPersistedSelectedGraphId();
    Promise.all([fetchGraphs(), refreshCatalog()])
      .then(([loadedGraphs, loadedCatalog]) => {
        setGraphs(loadedGraphs);
        setCatalog(loadedCatalog);
        if (loadedGraphs.length > 0) {
          const defaultGraphId = pickDefaultGraphId(loadedGraphs, persistedSelectedGraphId);
          const defaultGraph = loadedGraphs.find((graph) => graph.graph_id === defaultGraphId) ?? null;
          if (defaultGraph) {
            const nextGraph = hydrateSelectedGraph(defaultGraph);
            setSelectedGraphId(defaultGraphId);
            void restorePersistedRunSnapshot(nextGraph.graph_id);
            return;
          }
          setSelectedGraphId(defaultGraphId);
        } else {
          const blankGraph = applyPersistedSupabaseConnectionState(
            applyPersistedEnvVars(createBlankGraph(), draftGraphEnvStorageKey()),
            draftGraphEnvStorageKey(),
          );
          const nextStateId = resetHistory(blankGraph);
          setSavedGraphStateId(nextStateId);
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
      if (flushFrameRef.current !== null) {
        cancelAnimationFrame(flushFrameRef.current);
        flushFrameRef.current = null;
      }
      pendingEventsRef.current = [];
      if (runPollTimeoutRef.current !== null) {
        window.clearTimeout(runPollTimeoutRef.current);
      }
    };
  }, [flushPersistedRunSnapshot]);

  usePageVisibility(useCallback((hidden: boolean) => {
    isTabHiddenRef.current = hidden;
    if (hidden) {
      // Pause the rAF flush loop while hidden; events keep accumulating in the buffer (capped).
      if (flushFrameRef.current !== null) {
        cancelAnimationFrame(flushFrameRef.current);
        flushFrameRef.current = null;
      }
      return;
    }
    // Tab became visible — drain the buffer in a single batched render.
    if (pendingEventsRef.current.length > 0) {
      flushPendingEventsRef.current();
    }
  }, []));

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
    if (!selectedGraphId) {
      return;
    }
    savePersistedSelectedGraphId(selectedGraphId);
  }, [selectedGraphId]);

  useEffect(() => {
    if (!isMultiAgent(draftGraph) || !draftGraph.graph_id) {
      setEnvironmentAgentSelection({});
      return;
    }
    setEnvironmentAgentSelection(
      buildEnvironmentAgentSelection(draftGraph, loadEnvironmentAgentSelection(draftGraph.graph_id)),
    );
  }, [draftGraph]);

  useEffect(() => {
    if (!isMultiAgent(draftGraph) || !draftGraph.graph_id) {
      return;
    }
    saveEnvironmentAgentSelection(draftGraph.graph_id, buildEnvironmentAgentSelection(draftGraph, environmentAgentSelection));
  }, [draftGraph, environmentAgentSelection]);

  useEffect(() => {
    if (!isMultiAgent(draftGraph) || !draftGraph.graph_id) {
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
    if (!draftGraph?.env_vars) {
      return;
    }
    savePersistedGraphEnvVars(
      persistedGraphEnvStorageKey(draftGraph.graph_id, selectedGraphId),
      Object.fromEntries(Object.entries(draftGraph.env_vars).map(([key, value]) => [key, String(value ?? "")])),
    );
  }, [draftGraph?.env_vars, draftGraph?.graph_id, selectedGraphId]);

  useEffect(() => {
    if (!draftGraph) {
      return;
    }
    savePersistedSupabaseConnectionState(
      persistedGraphEnvStorageKey(draftGraph.graph_id, selectedGraphId),
      {
        supabase_connections: draftGraph.supabase_connections ?? [],
        default_supabase_connection_id: draftGraph.default_supabase_connection_id ?? "",
        run_store_supabase_connection_id: draftGraph.run_store_supabase_connection_id ?? "",
      },
    );
  }, [draftGraph, draftGraph?.default_supabase_connection_id, draftGraph?.run_store_supabase_connection_id, draftGraph?.supabase_connections, selectedGraphId]);

  useEffect(() => {
    setSelectedNodeId(null);
    setSelectedEdgeId(null);
  }, [selectedAgentId]);

  useEffect(() => {
    setIsRenamingGraph(false);
    setGraphNameDraft(draftGraph?.name ?? "");
    setGraphNameError(null);
  }, [draftGraph?.graph_id, selectedGraphId]);

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
    if (!activeRunId && !runState) {
      cancelPersistedRunSnapshot(draftGraph.graph_id);
      clearPersistedRunSnapshot(draftGraph.graph_id);
      return;
    }
    // Persist only the bounded event_history from runState (capped at RUN_STATE_EVENT_HISTORY_LIMIT)
    // rather than the full live events array. This prevents localStorage growth proportional to
    // run length and silent quota errors during long-running sessions.
    schedulePersistedRunSnapshot({
      graphId: draftGraph.graph_id,
      activeRunId,
      events: runState?.event_history ?? [],
      runState,
      savedAt: new Date().toISOString(),
    });
  }, [activeRunId, cancelPersistedRunSnapshot, draftGraph?.graph_id, runState, schedulePersistedRunSnapshot]);

  async function refreshGraphs(nextSelectedGraphId?: string) {
    const loadedGraphs = await fetchGraphs();
    setGraphs(loadedGraphs);
    if (nextSelectedGraphId) {
      setSelectedGraphId(nextSelectedGraphId);
    } else if (loadedGraphs.length === 0) {
      const blankGraph = createBlankGraph();
      setSelectedGraphId("");
      const nextStateId = resetHistory(blankGraph);
      setSavedGraphStateId(nextStateId);
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
      const graphToPersist = pendingBackgroundPersistGraphRef.current ?? draftGraph;
      const normalized = buildPersistableGraphDocument(graphToPersist);
      const savedGraph =
        selectedGraphId && persistedGraphIds.has(selectedGraphId)
          ? await updateGraph(selectedGraphId, normalized)
          : await createGraph(normalized);
      pendingBackgroundPersistGraphRef.current = null;
      backgroundPersistedSnapshotRef.current = serializePersistedGraphDocument(normalized);
      if (!selectedGraphId) {
        const draftStorageKey = draftGraphEnvStorageKey();
        const draftEnvVars = loadPersistedGraphEnvVars(draftStorageKey);
        if (draftEnvVars) {
          savePersistedGraphEnvVars(savedGraph.graph_id, draftEnvVars);
          clearPersistedGraphEnvVars(draftStorageKey);
        }
        const draftSupabaseState = loadPersistedSupabaseConnectionState(draftStorageKey);
        if (draftSupabaseState) {
          savePersistedSupabaseConnectionState(savedGraph.graph_id, draftSupabaseState);
          clearPersistedSupabaseConnectionState(draftStorageKey);
        }
      }
      await refreshGraphs(savedGraph.graph_id);
      const nextSavedInput = getSavedInputPrompt(savedGraph);
      const nextStateId = setDraftGraph(savedGraph);
      setSavedGraphStateId(nextStateId);
      setSavedInputPrompt(nextSavedInput);
      if (isMultiAgent(savedGraph)) {
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
    clearPersistedGraphEnvVars(draftGraphEnvStorageKey());
    clearPersistedSupabaseConnectionState(draftGraphEnvStorageKey());
    clearSessionSupabaseSchema({ graph_id: draftGraphEnvStorageKey() });
    setDeleteGraphTarget(null);
    const blankGraph = createBlankGraph();
    clearLiveRunState();
    setSelectedGraphId("");
    setSelectedAgentId(null);
    const nextStateId = resetHistory(blankGraph);
    setSavedGraphStateId(nextStateId);
    setInput(DEFAULT_INPUT);
    setSavedInputPrompt(DEFAULT_INPUT);
    setSelectedNodeId(null);
    setSelectedEdgeId(null);
    setError(null);
  }

  function handleRequestDeleteGraph() {
    if (!selectedGraphId) {
      handleCreateGraph();
      return;
    }
    const selectedGraph = draftGraph?.graph_id === selectedGraphId
      ? draftGraph
      : graphs.find((graph) => graph.graph_id === selectedGraphId);
    setDeleteGraphTarget({
      graph_id: selectedGraphId,
      name: selectedGraph ? getGraphDisplayName(selectedGraph) : selectedGraphId,
    });
    setError(null);
  }

  async function handleDeleteGraph(target: GraphDeleteTarget) {
    setIsDeletingGraph(true);
    try {
      cancelPersistedRunSnapshot(target.graph_id);
      clearPersistedRunSnapshot(target.graph_id);
      clearPersistedGraphEnvVars(target.graph_id);
      clearPersistedSupabaseConnectionState(target.graph_id);
      clearSessionSupabaseSchema({ graph_id: target.graph_id });
      await deleteGraph(target.graph_id);
      const loadedGraphs = await fetchGraphs();
      setGraphs(loadedGraphs);
      if (loadedGraphs.length > 0) {
        setSelectedGraphId(pickDefaultGraphId(loadedGraphs));
      } else {
        const blankGraph = createBlankGraph();
        setSelectedGraphId("");
        setSelectedAgentId(null);
        const nextStateId = resetHistory(blankGraph);
        setSavedGraphStateId(nextStateId);
        setInput(DEFAULT_INPUT);
        setSavedInputPrompt(DEFAULT_INPUT);
        clearLiveRunState();
      }
      setSelectedNodeId(null);
      setSelectedEdgeId(null);
      setDeleteGraphTarget(null);
      setError(null);
    } catch (deleteError) {
      const message = deleteError instanceof Error ? deleteError.message : "Unable to delete graph.";
      setError(message);
    } finally {
      setIsDeletingGraph(false);
    }
  }

  async function executeRun() {
    if (!draftGraph) {
      return;
    }
    const startRowError = spreadsheetStartRowValidationMessage(draftGraph);
    if (startRowError) {
      setError(startRowError);
      return;
    }
    const agentIdsToRun = isMultiAgent(draftGraph) ? getSelectedEnvironmentAgentIds(draftGraph, environmentAgentSelection) : undefined;
    if (isMultiAgent(draftGraph) && (!agentIdsToRun || agentIdsToRun.length === 0)) {
      setError("Turn on at least one agent before running the environment.");
      return;
    }

    const savedGraph = await saveCurrentGraph();
    if (!savedGraph) {
      return;
    }
    const storageKey = persistedGraphEnvStorageKey(savedGraph.graph_id, selectedGraphId);
    const runGraphEnvVars = getGraphEnvVars(applyPersistedEnvVars(savedGraph, storageKey));
    const runDocumentsForRun = readyRunDocuments;

    clearRunPolling();
    sourceRef.current?.close();
    sourceRef.current = null;
    setError(null);
    cancelPersistedRunSnapshot(savedGraph.graph_id);
    clearPersistedRunSnapshot(savedGraph.graph_id);
    setActiveRunId(null);
    setEvents([]);
    setRunState(null);
    setSelectedListenerChildRunId(null);
    setListenerChildRunStates({});
    setListenerChildRunError(null);
    setIsLoadingListenerChildRun(false);
    setVisualizerResetVersion((current) => current + 1);
    setIsStoppingRuntime(false);
    setIsRunning(true);
    if (isMultiAgent(savedGraph) && agentIdsToRun && agentIdsToRun.length > 0 && !agentIdsToRun.includes(selectedAgentId ?? "")) {
      setSelectedAgentId(agentIdsToRun[0] ?? null);
    }

    try {
      const runId = await startRun(savedGraph.graph_id, input, {
        agent_ids: agentIdsToRun,
        documents: runDocumentsForRun,
        graph_env_vars: runGraphEnvVars,
      });
      setActiveRunId(runId);
      setRunState(createPendingRunState(savedGraph, runId, input, agentIdsToRun, runDocumentsForRun));
      connectToRunStream(runId, savedGraph.graph_id, input, runDocumentsForRun);
    } catch (runError) {
      const message = runError instanceof Error ? runError.message : "Unable to start run.";
      setError(message);
      setIsRunning(false);
    }
  }

  async function executeListenerSession() {
    if (!draftGraph) {
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
    setIsStoppingRuntime(false);
    setIsRunning(true);

    try {
      const runId = await startListenerSession(savedGraph.graph_id);
      setActiveRunId(runId);
      setRunState(createPendingRunState(savedGraph, runId, input, undefined, []));
      connectToRunStream(runId, savedGraph.graph_id, input, []);
    } catch (runError) {
      const message = runError instanceof Error ? runError.message : "Unable to start listener session.";
      setError(message);
      setIsRunning(false);
    }
  }

  async function handleStopListenerSession() {
    if (!activeRunId) {
      return;
    }
    setIsStoppingRuntime(true);
    setError(null);
    try {
      await stopListenerSession(activeRunId);
      sourceRef.current?.close();
      sourceRef.current = null;
      setIsRunning(false);
    } catch (stopError) {
      const message = stopError instanceof Error ? stopError.message : "Unable to stop listener session.";
      setError(message);
    } finally {
      setIsStoppingRuntime(false);
    }
  }

  async function handleRun() {
    if (!draftGraph) {
      return;
    }
    if (isListenerGraph) {
      if (isListeningSession) {
        await handleStopListenerSession();
      } else {
        await executeListenerSession();
      }
      return;
    }
    const startRowError = spreadsheetStartRowValidationMessage(draftGraph);
    if (startRowError) {
      setError(startRowError);
      return;
    }
    const agentIdsToRun = isMultiAgent(draftGraph) ? getSelectedEnvironmentAgentIds(draftGraph, environmentAgentSelection) : undefined;
    if (isMultiAgent(draftGraph) && (!agentIdsToRun || agentIdsToRun.length === 0)) {
      setError("Turn on at least one agent before running the environment.");
      return;
    }
    if (emailRoutingMode === "production") {
      setProductionRunConfirmOpen(true);
      return;
    }
    await executeRun();
  }

  async function handleStopRuntime() {
    if (!window.confirm("Stop active execution? The current run state will stay visible, but active runs will be cancelled.")) {
      return;
    }
    setIsStoppingRuntime(true);
    setError(null);
    try {
      await stopRuntime();
    } catch (stopError) {
      const message = stopError instanceof Error ? stopError.message : "Unable to stop runtime.";
      setError(message);
      setIsStoppingRuntime(false);
    }
  }

  async function handleResetRuntime() {
    if (!window.confirm("Reset runtime? This will stop active runs, disconnect runtime services, and clear live run state.")) {
      return;
    }
    setIsResettingRuntime(true);
    setIsStoppingRuntime(false);
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
    const nextDocument = updateSelectedAgentGraph(draftGraph, selectedAgentId, nextGraph);
    const nextStateId = setDraftGraph(nextDocument);
    try {
      if (
        backgroundPersistedSnapshotRef.current
        && serializePersistedGraphDocument(buildPersistableGraphDocument(nextDocument)) === backgroundPersistedSnapshotRef.current
      ) {
        setSavedGraphStateId(nextStateId);
      }
    } catch {
      // Preserve the in-memory commit even when background-persistence validation cannot be recomputed.
    }
  }

  function handleCanvasGraphQuietChange(nextGraph: GraphDefinition) {
    if (!canvasGraph || !draftGraph) {
      return;
    }
    setDraftGraphQuiet(updateSelectedAgentGraph(draftGraph, selectedAgentId, nextGraph));
  }

  function handleCanvasGraphDrag(nextGraph: GraphDefinition) {
    handleCanvasGraphQuietChange(nextGraph);
  }

  // Memoized so the modal receives a stable `onBackgroundPersistGraph` prop
  // across renders. This reads draft graph and selected agent from refs so it
  // can never drift, and it intentionally does NOT touch `draftGraph` state —
  // background persist must stay off the rendered graph path.
  const handleCanvasBackgroundPersist = useCallback((nextGraph: GraphDefinition) => {
    const currentDraftGraph = draftGraphRef.current;
    if (!currentDraftGraph) {
      return;
    }
    pendingBackgroundPersistGraphRef.current = updateSelectedAgentGraph(
      currentDraftGraph,
      selectedAgentIdRef.current,
      nextGraph,
    );
    void persistPendingGraphInBackground();
  }, [persistPendingGraphInBackground]);

  function applyDraftGraphName(nextName: string) {
    if (!draftGraph) {
      return;
    }
    setDraftGraph({
      ...draftGraph,
      name: nextName,
    });
  }

  function beginGraphRename() {
    setGraphNameDraft(draftGraph?.name ?? "");
    setGraphNameError(null);
    setIsRenamingGraph(true);
  }

  function cancelGraphRename() {
    setGraphNameDraft(draftGraph?.name ?? "");
    setGraphNameError(null);
    setIsRenamingGraph(false);
  }

  function commitGraphRename() {
    const nextName = graphNameDraft.trim();
    if (!nextName) {
      setGraphNameError("Grouping name is required.");
      return;
    }
    applyDraftGraphName(nextName);
    setGraphNameDraft(nextName);
    setGraphNameError(null);
    setIsRenamingGraph(false);
  }

  function handleCreateAgent(agentName: string) {
    if (!draftGraph) {
      return;
    }
    const result = addAgentToDocument(draftGraph, agentName);
    setDraftGraph(result.graph);
    setSelectedAgentId(result.agentId);
    setSelectedNodeId(null);
    setSelectedEdgeId(null);
    setVisualizerResetVersion((current) => current + 1);
    setError(null);
  }

  function handleRequestRemoveAgent(agentId: string) {
    if (!isTestEnvironment(draftGraph)) {
      return;
    }
    const targetAgent = draftGraph.agents.find((agent) => agent.agent_id === agentId);
    if (!targetAgent) {
      return;
    }
    setWorkflowRemoveTarget({
      agent_id: targetAgent.agent_id,
      name: targetAgent.name.trim() || targetAgent.agent_id,
    });
  }

  function handleConfirmRemoveAgent() {
    if (!draftGraph || !workflowRemoveTarget) {
      return;
    }
    const result = removeAgentFromDocument(draftGraph, workflowRemoveTarget.agent_id);
    if (!result) {
      setWorkflowRemoveTarget(null);
      return;
    }
    setDraftGraph(result.graph);
    setSelectedAgentId(result.selectedAgentId);
    setSelectedNodeId(null);
    setSelectedEdgeId(null);
    setWorkflowRemoveTarget(null);
    setVisualizerResetVersion((current) => current + 1);
    setError(null);
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
                    disabled={
                      !draftGraph
                      || isSaving
                      || isResettingRuntime
                      || isUploadingRunDocuments
                      || (isListenerGraph
                        ? isStoppingRuntime
                        : isRunning || isStoppingRuntime)
                    }
                  >
                    {isListenerGraph
                      ? isStoppingRuntime
                        ? "Stopping..."
                        : isListeningSession
                          ? "Stop Listening"
                          : "Start Listening"
                      : isRunning
                        ? "Running..."
                        : isEnvironment
                          ? "Run Grouping"
                          : "Run Workflow"}
                  </button>
                  <button
                    type="button"
                    className="danger-button"
                    onClick={() => void handleStopRuntime()}
                    disabled={!isRunning || isStoppingRuntime || isResettingRuntime || isListenerGraph}
                  >
                    {isStoppingRuntime ? "Stopping..." : "Stop Runtime"}
                  </button>
                  <button type="button" className="secondary-button" onClick={handleCreateGraph}>
                    New Grouping
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
                  <button type="button" className="danger-button" onClick={() => void handleResetRuntime()} disabled={isStoppingRuntime || isResettingRuntime}>
                    {isResettingRuntime ? "Resetting..." : "Reset Runtime"}
                  </button>
                  <button type="button" className="danger-button" onClick={handleRequestDeleteGraph} disabled={!draftGraph || isDeletingGraph}>
                    {isDeletingGraph ? "Deleting..." : "Delete"}
                  </button>
                </div>
                <div className="mosaic-title-toggle-row">
                  <span className="mosaic-title-toggle-label">Email mode</span>
                  <div className="email-routing-mode-toggle email-routing-mode-toggle--compact" role="group" aria-label="Email table mode">
                    <button
                      type="button"
                      className={`secondary-button${emailRoutingMode === "development" ? " is-active" : ""}`}
                      onClick={() => {
                        if (!draftGraph) {
                          return;
                        }
                        setDraftGraph(applyEmailRoutingMode(draftGraph, "development"));
                      }}
                      disabled={!draftGraph || isRunning || isSaving}
                    >
                      Dev
                    </button>
                    <button
                      type="button"
                      className={`secondary-button${emailRoutingMode === "production" ? " is-active" : ""}`}
                      onClick={() => {
                        if (!draftGraph) {
                          return;
                        }
                        setDraftGraph(applyEmailRoutingMode(draftGraph, "production"));
                      }}
                      disabled={!draftGraph || isRunning || isSaving}
                    >
                      Prod
                    </button>
                  </div>
                  {emailRoutingMode === "production" ? (
                    <span className="env-integration-status">Confirmation required</span>
                  ) : (
                    <span className="env-integration-status is-ready">Using *_dev tables</span>
                  )}
                </div>
                {selectedRunId ? (
                  <code className="mosaic-title-run-id">Run ID: {selectedRunId}</code>
                ) : null}
                {error ? <p className="error-text mosaic-title-error">{error}</p> : null}
              </div>

              <div className="mosaic-tile panel mosaic-graph">
                <section className="graph-name-editor" aria-label="Grouping selector">
                  <div className="graph-name-editor-label">Grouping</div>
                  {isRenamingGraph ? (
                    <div className="graph-name-editor-edit">
                      <input
                        type="text"
                        value={graphNameDraft}
                        onChange={(event) => {
                          setGraphNameDraft(event.target.value);
                          if (graphNameError) {
                            setGraphNameError(null);
                          }
                        }}
                        onKeyDown={(event) => {
                          if (event.key === "Enter") {
                            event.preventDefault();
                            commitGraphRename();
                          }
                          if (event.key === "Escape") {
                            event.preventDefault();
                            cancelGraphRename();
                          }
                        }}
                        disabled={!draftGraph || isSaving}
                        placeholder="Name this grouping"
                        autoFocus
                      />
                      <div className="graph-name-editor-actions">
                        <button type="button" className="secondary-button" onClick={cancelGraphRename} disabled={isSaving}>
                          Cancel
                        </button>
                        <button type="button" className="primary-button" onClick={commitGraphRename} disabled={!draftGraph || isSaving}>
                          Save Name
                        </button>
                      </div>
                    </div>
                  ) : (
                    <div className="graph-name-editor-display">
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
                      <button type="button" className="secondary-button" onClick={beginGraphRename} disabled={!draftGraph || isSaving}>
                        Rename
                      </button>
                    </div>
                  )}
                  {graphNameError ? <p className="error-text graph-name-editor-error">{graphNameError}</p> : null}
                  {hasUnsavedChanges ? <p className="graph-name-editor-hint">Use Save to persist changes.</p> : null}
                </section>
              </div>

              <div className="mosaic-tile panel mosaic-env">
                <h2>Environment</h2>
                <GraphEnvEditor
                  graph={draftGraph}
                  onGraphChange={setDraftGraph}
                  onMicrosoftAuthChanged={async () => {
                    await refreshCatalog();
                  }}
                />
              </div>

              <div className="mosaic-tile panel mosaic-execution">
                {isListenerGraph ? (
                  <div className="listener-session-panel">
                    <div className="mosaic-section-heading">
                      <span className="mosaic-section-kicker">Listener</span>
                      <strong>{listenerStartProvider?.display_name ?? "Listener"}</strong>
                    </div>
                    <p className="listener-session-status">
                      Status:{" "}
                      <strong>
                        {isListeningSession
                          ? "Listening — incoming events spawn child runs."
                          : "Idle. Click Start Listening to open a session."}
                      </strong>
                    </p>
                    {listenerWebhookEndpoints.length > 0 ? (
                      <div className="listener-session-endpoints" aria-live="polite">
                        <span className="mosaic-section-kicker">Webhook endpoints</span>
                        {listenerWebhookEndpoints.map((row) => (
                          <div key={row.slug} className="listener-session-endpoint-block">
                            {listenerWebhookEndpoints.length > 1 ? (
                              <div className="listener-session-endpoint-slug">Slug: {row.slug}</div>
                            ) : null}
                            <div className="listener-session-endpoint-line">
                              <span className="listener-session-endpoint-label">Local (this machine / Vite)</span>
                              <div className="inspector-webhook-url-row">
                                <code className="inspector-webhook-url">{row.localUrl}</code>
                                <button
                                  type="button"
                                  className="secondary-button"
                                  onClick={() =>
                                    void navigator.clipboard
                                      .writeText(row.localUrl)
                                      .then(
                                        () => setListenerEndpointCopyKey(`local:${row.slug}`),
                                        () => undefined,
                                      )
                                  }
                                >
                                  {listenerEndpointCopyKey === `local:${row.slug}` ? "Copied" : "Copy"}
                                </button>
                              </div>
                            </div>
                            {row.publicUrl ? (
                              <div className="listener-session-endpoint-line">
                                <span className="listener-session-endpoint-label">Tunneled (HTTPS)</span>
                                <div className="inspector-webhook-url-row">
                                  <code className="inspector-webhook-url">{row.publicUrl}</code>
                                  <button
                                    type="button"
                                    className="secondary-button"
                                    onClick={() =>
                                      void navigator.clipboard
                                        .writeText(row.publicUrl!)
                                        .then(
                                          () => setListenerEndpointCopyKey(`public:${row.slug}`),
                                          () => undefined,
                                        )
                                    }
                                  >
                                    {listenerEndpointCopyKey === `public:${row.slug}` ? "Copied" : "Copy"}
                                  </button>
                                </div>
                              </div>
                            ) : (
                              <p className="listener-session-endpoint-hint">
                                Add a <strong>public hostname</strong> under Environment → Cloudflare Tunnel to show the
                                tunneled URL for callers off this machine.
                              </p>
                            )}
                          </div>
                        ))}
                      </div>
                    ) : null}
                    <p className="listener-session-meta">
                      Events received: <strong>{childRunSummaries.length}</strong>
                    </p>
                    {childRunSummaries.length > 0 ? (
                      <div className="listener-session-children">
                        <span className="mosaic-section-kicker">Child runs</span>
                        <ul>
                          {childRunSummaries.slice(-10).reverse().map((entry) => (
                            <li key={entry.run_id}>
                              <button
                                type="button"
                                className={`listener-session-child-run ${
                                  selectedListenerChildRunId === entry.run_id ? "is-selected" : ""
                                }`}
                                onClick={() => setSelectedListenerChildRunId(entry.run_id)}
                              >
                                <code>{entry.run_id}</code>
                                {entry.received_at ? (
                                  <span className="listener-session-children-time">{entry.received_at}</span>
                                ) : null}
                              </button>
                            </li>
                          ))}
                        </ul>
                        <div className="listener-session-child-details">
                          <div className="listener-session-child-details-header">
                            <span className="mosaic-section-kicker">Selected Input</span>
                            <span>{isLoadingListenerChildRun ? "Loading..." : selectedListenerChildRunState?.status ?? "Waiting"}</span>
                          </div>
                          {listenerChildRunError ? <p className="error-text">{listenerChildRunError}</p> : null}
                          {selectedListenerChildRunInput ? (
                            <pre>{selectedListenerChildRunInput}</pre>
                          ) : (
                            <p className="listener-session-meta">Select a child run to inspect its input payload.</p>
                          )}
                        </div>
                      </div>
                    ) : null}
                  </div>
                ) : (
                  <label className="mosaic-execution-input">
                    Input
                    <textarea value={input} onChange={(event) => setInput(event.target.value)} rows={2} />
                  </label>
                )}
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
                  Upload reusable files into this project so spreadsheet-backed nodes can select them later. Browse, preview, or remove them from the Workspace panel.
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
                {projectFileError ? <p className="error-text">{projectFileError}</p> : null}
                <div className="execution-documents-header">
                  <strong>Run Documents</strong>
                  <span>
                    {readyRunDocuments.length} ready
                    {runDocuments.length !== readyRunDocuments.length ? ` / ${runDocuments.length} uploaded` : ""}
                  </span>
                </div>
                <p>
                  Upload PDFs and docs for this run. Use <code>{"{documents}"}</code> in a Context Builder template to inject extracted text.
                </p>
                <label className="execution-documents-picker">
                  <span>{isUploadingRunDocuments ? "Uploading..." : "Add Run Documents"}</span>
                  <input
                    type="file"
                    accept=".txt,.md,.markdown,.json,.csv,.xlsx,.pdf,text/plain,text/markdown,text/csv,application/json,application/pdf,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    multiple
                    disabled={isSaving || isResettingRuntime || isUploadingRunDocuments}
                    onChange={(event) => {
                      void handleRunDocumentUpload(event.target.files);
                      event.target.value = "";
                    }}
                  />
                </label>
                {runDocuments.length > 0 ? (
                  <div className="execution-documents-list">
                    {runDocuments.map((document) => (
                      <div key={document.document_id} className={`execution-document-card is-${document.status}`}>
                        <div className="execution-document-card-header">
                          <div>
                            <strong>{document.name}</strong>
                            <span>
                              {formatDocumentSize(document.size_bytes)} · {document.mime_type || "file"}
                            </span>
                          </div>
                          <div className="execution-document-card-actions">
                            <button
                              type="button"
                              className="secondary-button execution-document-view"
                              onClick={() => viewRunDocument(document.document_id)}
                              disabled={document.status !== "ready" && !document.text_content}
                            >
                              View
                            </button>
                            <button
                              type="button"
                              className="secondary-button execution-document-remove"
                              onClick={() => removeRunDocument(document.document_id)}
                              disabled={isSaving || isResettingRuntime}
                            >
                              Remove
                            </button>
                          </div>
                        </div>
                        <span className={`execution-document-status execution-document-status--${document.status}`}>{document.status}</span>
                        <pre className="execution-document-excerpt">{document.text_excerpt || document.storage_path}</pre>
                        {document.error ? <span className="execution-document-error">{document.error}</span> : null}
                      </div>
                    ))}
                  </div>
                ) : (
                  <p>No run documents uploaded yet.</p>
                )}
                {runDocumentError ? <p className="error-text">{runDocumentError}</p> : null}
              </div>

              <div className="mosaic-tile panel mosaic-files mosaic-project-files">
                <div className="mosaic-section-heading">
                  <span className="mosaic-section-kicker">Workspace</span>
                  <div className="execution-documents-header">
                    <strong>Project Files</strong>
                    <span>
                      {readyProjectFiles.length} ready
                      {projectFiles.length !== readyProjectFiles.length ? ` / ${projectFiles.length} total` : ""}
                    </span>
                  </div>
                </div>
                  <p className="workspace-subsection-intro">
                    Reusable files attached to this project. Uploads are per-graph; files in <code>scripts/</code> on disk are shared across every graph.
                  </p>
                  {isProjectFilesLoading ? (
                    <p>Loading project files...</p>
                  ) : projectFiles.length > 0 ? (
                    <div className="project-files-accordion">
                      {([
                        { source: "upload" as const, label: "Uploaded", files: uploadedProjectFiles, emptyHint: "No files uploaded for this graph yet." },
                        { source: "scripts" as const, label: "Scripts folder", files: scriptsProjectFiles, emptyHint: "Drop .py files in scripts/ and they'll show up here." },
                      ]).map((group) => {
                        const expanded = expandedProjectFileSources[group.source] ?? true;
                        return (
                          <div key={group.source} className={`project-files-group project-files-group--${group.source}`}>
                            <button
                              type="button"
                              className="project-files-group-header"
                              aria-expanded={expanded}
                              onClick={() =>
                                setExpandedProjectFileSources((prev) => ({
                                  ...prev,
                                  [group.source]: !(prev[group.source] ?? true),
                                }))
                              }
                            >
                              <span className="project-files-group-chevron">{expanded ? "▾" : "▸"}</span>
                              <span className="project-files-group-label">{group.label}</span>
                              <span className="project-files-group-count">
                                {group.files.length} file{group.files.length === 1 ? "" : "s"}
                              </span>
                            </button>
                            {expanded ? (
                              group.files.length > 0 ? (
                                <div className="execution-documents-list">
                                  {group.files.map((file) => {
                                    const isScripts = (file.source ?? "upload") === "scripts";
                                    return (
                                      <div key={file.file_id} className={`execution-document-card is-${file.status}`}>
                                        <div className="execution-document-card-header">
                                          <div>
                                            <strong>{file.name}</strong>
                                            <span>
                                              {formatDocumentSize(file.size_bytes)} · {file.mime_type || "file"}
                                            </span>
                                          </div>
                                          <div className="execution-document-card-actions">
                                            <button
                                              type="button"
                                              className="secondary-button execution-document-view"
                                              onClick={() => void viewProjectFile(projectFileGraphId, file.file_id)}
                                              disabled={file.status !== "ready" || !projectFileGraphId}
                                            >
                                              View
                                            </button>
                                            {!isScripts ? (
                                              <button
                                                type="button"
                                                className="secondary-button execution-document-remove"
                                                onClick={() => void removeProjectFile(projectFileGraphId, file.file_id)}
                                                disabled={isSaving || isResettingRuntime || !projectFileGraphId}
                                              >
                                                Remove
                                              </button>
                                            ) : null}
                                          </div>
                                        </div>
                                        <span className={`execution-document-status execution-document-status--${file.status}`}>{file.status}</span>
                                        {file.error ? <span className="execution-document-error">{file.error}</span> : null}
                                      </div>
                                    );
                                  })}
                                </div>
                              ) : (
                                <p className="project-files-group-empty">{group.emptyHint}</p>
                              )
                            ) : null}
                          </div>
                        );
                      })}
                    </div>
                  ) : (
                    <p>No project files yet. Upload from the Inputs tile or drop a .py file in scripts/.</p>
                  )}
                  {projectFileError ? <p className="error-text">{projectFileError}</p> : null}
              </div>

              <div className="mosaic-tile panel mosaic-files mosaic-agent-files">
                  <div className="execution-files-header">
                    <div className="execution-documents-header">
                      <strong>Agent Files</strong>
                      <span>{visibleRunFiles.length} file{visibleRunFiles.length === 1 ? "" : "s"}</span>
                    </div>
                    <div className="execution-files-actions">
                      <button
                        type="button"
                        className="secondary-button"
                        onClick={() => setIsRunFilesExplorerOpen(true)}
                        disabled={!selectedRunFilesRequest.runId}
                      >
                        Open Explorer
                      </button>
                      <button
                        type="button"
                        className="secondary-button"
                        onClick={() => {
                          if (selectedRunFilesRequest.runId) {
                            void refreshRunFiles(selectedRunFilesRequest.runId, selectedRunFilesRequest.agentId);
                          }
                        }}
                        disabled={!selectedRunFilesRequest.runId || isRunFilesLoading}
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
                  <div className="execution-files-summary-grid">
                    <div className="execution-files-summary-card">
                      <div className="execution-files-browser-header">
                        <strong>Quick Preview</strong>
                        <span>{selectedRunFile ? selectedRunFile.name : "No file selected"}</span>
                      </div>
                      {selectedRunFile ? (
                        <>
                          <div className="execution-file-summary-meta">
                            <span className="execution-file-preview-path">{selectedRunFile.path}</span>
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
                        <p className="execution-file-preview-empty">Open the explorer to browse files from this run.</p>
                      )}
                    </div>

                    <div className="execution-files-summary-card">
                      <div className="execution-files-browser-header">
                        <strong>Recent Files</strong>
                        <span>{visibleRunFiles.length} total</span>
                      </div>
                      <div className="execution-file-summary-list" role="list" aria-label="Recent agent workspace files">
                        {recentRunFiles.map((file) => (
                          <button
                            key={file.path}
                            type="button"
                            className={`execution-file-row ${selectedRunFilePath === file.path ? "is-selected" : ""}`}
                            onClick={() => {
                              setFollowLatestRunFile(false);
                              setSelectedRunFilePath(file.path);
                              setIsRunFilesExplorerOpen(true);
                            }}
                          >
                            <strong>{file.name}</strong>
                            <span className="execution-file-row-path">{file.path}</span>
                            <span>
                              {formatDocumentSize(file.size_bytes)} · {formatTimestamp(file.modified_at)}
                            </span>
                          </button>
                        ))}
                      </div>
                      <button
                        type="button"
                        className="secondary-button execution-files-open-button"
                        onClick={() => setIsRunFilesExplorerOpen(true)}
                      >
                        Browse all files
                      </button>
                    </div>
                  </div>
                ) : (
                  <p className="execution-file-preview-empty">
                    {selectedRunFilesRequest.runId ? "No files have been written in this run yet." : "Run the graph to inspect workspace files."}
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
          runState={canvasRunState}
          events={canvasEvents}
          activeRunId={canvasActiveRunId}
          isRunning={isRunning}
          runButtonLabel={
            isListenerGraph
              ? isListeningSession
                ? "Stop Listening"
                : "Start Listening"
              : isEnvironment
                ? "Run Grouping"
                : "Run Workflow"
          }
          focusedAgentName={isEnvironment ? (environmentRunSummary?.focusedAgentName ?? null) : null}
          focusedAgentStatus={isEnvironment ? focusedRunSummary.status : null}
          environmentAgents={isEnvironment ? agentRunLanes : []}
          selectedAgentId={selectedAgentId}
          onCreateAgent={handleCreateAgent}
          onRequestRemoveAgent={handleRequestRemoveAgent}
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
          onBackgroundPersistGraph={handleCanvasBackgroundPersist}
          onGraphQuietChange={handleCanvasGraphQuietChange}
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
          isWebhookListenerSessionActive={isWebhookListenerSessionActive}
        />
      </section>

      {isMultiAgent(draftGraph) ? (
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
      {isRunFilesExplorerOpen ? (
        <RunFilesExplorerModal
          listing={runFileListing}
          files={visibleRunFiles}
          selectedFilePath={selectedRunFilePath}
          selectedFile={selectedRunFile}
          selectedFileContent={selectedRunFileContent}
          isRunFilesLoading={isRunFilesLoading}
          isRunFileContentLoading={isRunFileContentLoading}
          runFilesError={runFilesError}
          runFileContentError={runFileContentError}
          onClose={() => setIsRunFilesExplorerOpen(false)}
          onRefresh={() => {
            if (selectedRunFilesRequest.runId) {
              void refreshRunFiles(selectedRunFilesRequest.runId, selectedRunFilesRequest.agentId);
            }
          }}
          onSelectFile={(path) => {
            setFollowLatestRunFile(false);
            setSelectedRunFilePath(path);
          }}
        />
      ) : null}
      {productionRunConfirmOpen ? (
        <ProductionRunConfirmModal
          onClose={() => setProductionRunConfirmOpen(false)}
          onConfirm={() => {
            setProductionRunConfirmOpen(false);
            void executeRun();
          }}
        />
      ) : null}
      {deleteGraphTarget ? (
        <GraphDeleteConfirmModal
          graphName={deleteGraphTarget.name}
          isDeleting={isDeletingGraph}
          onClose={() => {
            if (!isDeletingGraph) {
              setDeleteGraphTarget(null);
            }
          }}
          onConfirm={() => void handleDeleteGraph(deleteGraphTarget)}
        />
      ) : null}
      {documentPreview ? (
        <DocumentPreviewModal
          title={documentPreview.title}
          subtitle={documentPreview.subtitle}
          content={documentPreview.content}
          isLoading={documentPreview.isLoading}
          error={documentPreview.error}
          truncated={documentPreview.truncated}
          onClose={() => setDocumentPreview(null)}
        />
      ) : null}
      {workflowRemoveTarget ? (
        <WorkflowRemoveConfirmModal
          workflowName={workflowRemoveTarget.name}
          onClose={() => setWorkflowRemoveTarget(null)}
          onConfirm={handleConfirmRemoveAgent}
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
