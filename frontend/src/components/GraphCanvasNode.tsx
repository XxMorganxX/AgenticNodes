import { memo, useEffect } from "react";
import type { CSSProperties } from "react";
import { Handle, Position, useUpdateNodeInternals } from "reactflow";
import type { NodeProps } from "reactflow";

import {
  API_FINAL_MESSAGE_HANDLE_ID,
  API_TOOL_CALL_HANDLE_ID,
  API_TOOL_CONTEXT_HANDLE_ID,
  CONTROL_FLOW_ELSE_HANDLE_ID,
  CONTROL_FLOW_IF_HANDLE_ID,
  CONTROL_FLOW_LOOP_BODY_HANDLE_ID,
  createParallelSplitterOutputHandleId,
  getNodeTargetAnchorRatio,
  getParallelSplitterNodeDimensions,
  getParallelSplitterOutputHandles,
  inferModelResponseMode,
  getApiToolContextTargetAnchorRatio,
  getToolSourceHandleAnchorRatio,
  isApiModelNode,
  isControlFlowNode,
  isMcpContextProviderNode,
  isOutboundEmailLoggerNode,
  isPromptBlockNode,
  isRoutableToolNode,
  isWireJunctionNode,
  MCP_TERMINAL_OUTPUT_HANDLE_ID,
  OUTLOOK_DRAFT_EMAIL_LOG_TARGET_HANDLE_ID,
  TOOL_CONTEXT_HANDLE_ID,
  TOOL_FAILURE_HANDLE_ID,
  TOOL_SUCCESS_HANDLE_ID,
} from "../lib/editor";
import { warnGraphDiagnostic } from "../lib/dragDiagnostics";
import { normalizeLogicConditionConfig } from "../lib/logicConditions";
import { buildNodeTooltip } from "../lib/nodeTooltip";
import { getNodeInstanceLabel } from "../lib/nodeInstanceLabels";
import type { NodeTooltipData } from "../lib/nodeTooltip";
import { getContextBuilderBindings } from "../lib/contextBuilderBindings";
import type { ContextBuilderRuntimeView } from "../lib/contextBuilderRuntime";
import { formatRunStatusLabel } from "../lib/runVisualization";
import { getSupabaseConnectionSelectOptions, resolveSupabaseBinding } from "../lib/supabaseConnections";
import type { EditorCatalog, GraphDefinition, GraphNode, ProjectFile, RunState } from "../lib/types";

export type GraphCanvasNodeData = {
  node: GraphNode;
  graph: GraphDefinition | null;
  graphRenderSignature?: string;
  tooltipGraph?: GraphDefinition | null;
  catalog: EditorCatalog | null;
  runState: RunState | null;
  availableProjectFiles?: ProjectFile[];
  displayLabel?: string;
  runtimeOutput?: unknown;
  contextBuilderRuntime?: ContextBuilderRuntimeView | null;
  contextBuilderRuntimeKey?: string;
  kindColor: string;
  status: "idle" | "active" | "success" | "failed" | "unreached";
  isConnectionMagnetized?: boolean;
  preview?: boolean;
  tooltipVisible: boolean;
  onToggleTooltip: (nodeId: string) => void;
  onOpenToolDetails: (nodeId: string) => void;
  onOpenProviderDetails: (nodeId: string) => void;
  onOpenDiscordTriggerConfig: (nodeId: string) => void;
  onOpenCronScheduleConfig: (nodeId: string) => void;
  onToggleExecutorRetries: (nodeId: string) => void;
  onToggleSupabaseIteratorIncludeProcessedRows: (nodeId: string) => void;
  onOpenPromptBlockDetails: (nodeId: string) => void;
  onOpenDisplayResponse: (nodeId: string) => void;
  onOpenContextBuilderPayload: (nodeId: string) => void;
  onOpenConditionResults: (nodeId: string) => void;
  onSelectSpreadsheetFile: (nodeId: string, fileId: string) => void;
  onChangeSpreadsheetStartRowIndex: (nodeId: string, startRowIndex: number | string) => void;
  onSelectPythonScriptFile: (nodeId: string, fileId: string) => void;
  onSelectSupabaseConnection: (nodeId: string, connectionId: string) => void;
  onHandlePointerDown: (nodeId: string, handleType: "source" | "target", handleId: string | null) => boolean;
  onJunctionPointerDown: (nodeId: string, clientPosition: { x: number; y: number }) => void;
};

const SPREADSHEET_ROW_PROVIDER_ID = "core.spreadsheet_rows";
const SUPABASE_SQL_PROVIDER_ID = "core.supabase_sql";
const SUPABASE_TABLE_ROWS_PROVIDER_ID = "core.supabase_table_rows";
const SPREADSHEET_MATRIX_DECISION_PROVIDER_ID = "core.spreadsheet_matrix_decision";
const LOGIC_CONDITIONS_PROVIDER_ID = "core.logic_conditions";
const PARALLEL_SPLITTER_PROVIDER_ID = "core.parallel_splitter";
const STRUCTURED_PAYLOAD_BUILDER_PROVIDER_ID = "core.structured_payload_builder";
const PYTHON_SCRIPT_RUNNER_PROVIDER_ID = "core.python_script_runner";
const PARALLEL_SPLITTER_HANDLE_SLOT_HEIGHT = 28;
const PARALLEL_SPLITTER_HANDLE_GAP = 12;

const KIND_LABELS: Record<string, string> = {
  input: "IN",
  model: "AI",
  tool: "FX",
  mcp_context_provider: "MC",
  mcp_tool_executor: "MX",
  control_flow_unit: "CF",
  data: "DB",
  output: "OUT",
};

const FALLBACK_TOOLTIP: NodeTooltipData = {
  title: "Node details unavailable",
  eyebrow: "graph / node",
  description: "This node could not render its tooltip data.",
  sections: [],
  parameters: [],
  emptyState: "The node is still available in the canvas.",
};

function formatInlineDisplayValue(value: unknown): string {
  if (typeof value === "string") {
    return value;
  }
  if (value === undefined) {
    return "";
  }
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

function formatLogicEvaluationValue(value: unknown): string {
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  if (value === null) {
    return "null";
  }
  if (value === undefined) {
    return "undefined";
  }
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function hasContextConnection(graph: GraphDefinition | null, node: GraphNode): boolean {
  if (!graph || node.kind !== "mcp_context_provider") {
    return false;
  }
  const hasBindingEdge = graph.edges.some(
    (edge) => edge.kind === "binding" && edge.source_id === node.id && edge.target_id !== node.id,
  );
  if (hasBindingEdge) {
    return true;
  }
  return graph.nodes.some((candidate) => {
    if (candidate.kind !== "model") {
      return false;
    }
    const targetIds = Array.isArray(candidate.config.tool_target_node_ids) ? candidate.config.tool_target_node_ids : [];
    return targetIds.some((targetId) => String(targetId) === node.id);
  });
}

function isContextBooted(catalog: EditorCatalog | null, node: GraphNode): boolean {
  const toolNames = Array.isArray(node.config.tool_names) ? node.config.tool_names.map((toolName) => String(toolName)) : [];
  if (!catalog || toolNames.length === 0) {
    return false;
  }
  const selectedTools = catalog.tools.filter((tool) => {
    const identifiers = [tool.canonical_name ?? tool.name, tool.name, ...(tool.aliases ?? [])];
    return toolNames.some((toolName) => identifiers.includes(toolName));
  });
  if (selectedTools.length === 0) {
    return false;
  }
  const serverIds = [...new Set(selectedTools.map((tool) => tool.server_id).filter((serverId): serverId is string => Boolean(serverId)))];
  if (serverIds.length === 0) {
    return selectedTools.every((tool) => tool.available !== false);
  }
  return serverIds.every((serverId) => catalog.mcp_servers?.some((server) => server.server_id === serverId && server.running));
}

function contextBuilderPlaceholderCount(graph: GraphDefinition | null, node: GraphNode): number {
  if (graph && node.provider_id === "core.context_builder") {
    return getContextBuilderBindings(node, graph).length;
  }
  const configuredSourceIds = Array.isArray(node.config.input_bindings)
    ? node.config.input_bindings
        .map((binding) =>
          typeof binding === "object" && binding !== null ? String((binding as { source_node_id?: unknown }).source_node_id ?? "") : "",
        )
        .filter((sourceId) => sourceId.trim().length > 0)
    : [];
  const incomingSourceIds =
    graph?.edges
      .filter((edge) => edge.target_id === node.id)
      .map((edge) => edge.source_id)
      .filter((sourceId, index, sourceIds) => sourceIds.indexOf(sourceId) === index) ?? [];
  return new Set([...configuredSourceIds, ...incomingSourceIds]).size;
}

function configuredResponseMode(node: GraphNode): "auto" | "tool_call" | "message" {
  const value = String(node.config.response_mode ?? "auto").trim();
  return value === "tool_call" || value === "message" || value === "auto" ? value : "auto";
}

function isSpreadsheetProjectFile(file: ProjectFile): boolean {
  const candidate = `${file.name} ${file.storage_path}`.toLowerCase();
  return file.status === "ready" && (candidate.endsWith(".csv") || candidate.endsWith(".xlsx"));
}

function isPythonProjectFile(file: ProjectFile): boolean {
  const candidate = `${file.name} ${file.storage_path}`.toLowerCase();
  return file.status === "ready" && candidate.endsWith(".py");
}

function GraphCanvasNodeComponent({
  data,
  selected,
}: NodeProps<GraphCanvasNodeData>) {
  const updateNodeInternals = useUpdateNodeInternals();
  const {
    node,
    graph,
    displayLabel: providedDisplayLabel,
    tooltipGraph = null,
    catalog,
    runState,
    availableProjectFiles = [],
    runtimeOutput,
    contextBuilderRuntime = null,
    kindColor,
    status,
    isConnectionMagnetized = false,
    preview = false,
    tooltipVisible,
    onToggleTooltip,
    onOpenToolDetails,
    onOpenProviderDetails,
    onOpenDiscordTriggerConfig,
    onOpenCronScheduleConfig,
    onToggleExecutorRetries,
    onToggleSupabaseIteratorIncludeProcessedRows,
    onOpenPromptBlockDetails,
    onOpenDisplayResponse,
    onOpenContextBuilderPayload,
    onOpenConditionResults,
    onSelectSpreadsheetFile,
    onChangeSpreadsheetStartRowIndex,
    onSelectPythonScriptFile,
    onSelectSupabaseConnection,
    onJunctionPointerDown,
  } = data;
  const isWireJunction = isWireJunctionNode(node);
  const isRoutableTool = isRoutableToolNode(node);
  const isContextProviderNode = isMcpContextProviderNode(node);
  const isModelNode = isApiModelNode(node);
  const isControlFlowUnitNode = isControlFlowNode(node);
  const isDisplayNode = node.provider_id === "core.data_display";
  const isContextBuilderNode = node.provider_id === "core.context_builder";
  const isLogicConditionsNode = node.provider_id === LOGIC_CONDITIONS_PROVIDER_ID;
  const isParallelSplitterNode = node.provider_id === PARALLEL_SPLITTER_PROVIDER_ID;
  const isStructuredPayloadBuilderNode = node.provider_id === STRUCTURED_PAYLOAD_BUILDER_PROVIDER_ID;
  const isRuntimeNormalizerNode = node.provider_id === "core.runtime_normalizer";
  const isSupabaseDataNode = node.provider_id === "core.supabase_data";
  const isSupabaseSqlNode = node.provider_id === SUPABASE_SQL_PROVIDER_ID;
  const isSupabaseTableRowsNode = node.provider_id === SUPABASE_TABLE_ROWS_PROVIDER_ID;
  const isSupabaseRowWriteNode = node.provider_id === "core.supabase_row_write";
  const isOutboundEmailLogger = isOutboundEmailLoggerNode(node);
  const isSupabaseNode = isSupabaseSqlNode || isSupabaseDataNode || isSupabaseTableRowsNode || isSupabaseRowWriteNode || isOutboundEmailLogger;
  const isOutlookDraftNode = node.provider_id === "end.outlook_draft";
  const isDiscordStartNode = node.kind === "input" && node.provider_id === "start.discord_message";
  const isCronStartNode = node.kind === "input" && node.provider_id === "start.cron_schedule";
  const displayLabel = providedDisplayLabel ?? getNodeInstanceLabel(graph, node);
  const resolvedSupabaseBinding = isSupabaseNode ? resolveSupabaseBinding(graph, node.config as Record<string, unknown>) : null;
  const supabaseConnectionOptions = isSupabaseNode ? getSupabaseConnectionSelectOptions(graph, node.config as Record<string, unknown>) : [];
  const supabaseConnectionMissing = resolvedSupabaseBinding?.missingConnection ?? false;
  const supabaseSelectValue = String(node.config.supabase_connection_id ?? "");
  const stopInlineControlPropagation = (event: { stopPropagation: () => void }) => {
    event.stopPropagation();
  };
  const displayEnvelope =
    isDisplayNode &&
    runtimeOutput &&
    typeof runtimeOutput === "object" &&
    runtimeOutput !== null &&
    "artifacts" in runtimeOutput &&
    typeof runtimeOutput.artifacts === "object" &&
    runtimeOutput.artifacts !== null &&
    "display_envelope" in runtimeOutput.artifacts
      ? runtimeOutput.artifacts.display_envelope
      : runtimeOutput;
  const displayText = isDisplayNode
    ? status === "active"
      ? "Running..."
      : displayEnvelope !== undefined
        ? formatInlineDisplayValue(displayEnvelope)
        : "Run the graph to inspect the incoming envelope here."
    : null;
  const contextBuilderPayload =
    isContextBuilderNode &&
    runtimeOutput &&
    typeof runtimeOutput === "object" &&
    runtimeOutput !== null &&
    "payload" in runtimeOutput
      ? runtimeOutput.payload
      : runtimeOutput;
  const contextBuilderForwardLine = contextBuilderRuntime
    ? contextBuilderRuntime.isWaitingToForward
      ? "Waiting to forward until every input is resolved and the merge is complete."
      : contextBuilderRuntime.contextBuilderComplete === true
        ? "All inputs resolved — payload forwarded downstream when the run continues."
        : null
    : null;
  const contextBuilderDisplayText = isContextBuilderNode
    ? status === "active" && !contextBuilderRuntime && contextBuilderPayload === undefined
      ? "Running..."
      : contextBuilderPayload !== undefined
        ? [formatInlineDisplayValue(contextBuilderPayload), contextBuilderForwardLine].filter(Boolean).join("\n\n")
        : contextBuilderRuntime && contextBuilderRuntime.totalCount > 0
          ? [
              contextBuilderForwardLine,
              status === "active"
                ? "Streaming inputs as upstream nodes finish."
                : "Run the graph to stream each input into the merged payload.",
            ]
              .filter(Boolean)
              .join("\n\n")
          : "Run the graph to inspect the exact payload produced here."
    : null;
  let tooltip: NodeTooltipData = FALLBACK_TOOLTIP;
  if (tooltipVisible && !preview && !isWireJunction) {
    try {
      tooltip = buildNodeTooltip({ ...node, label: displayLabel }, tooltipGraph, catalog, runState);
    } catch (error) {
      warnGraphDiagnostic("GraphCanvasNode", "tooltip fallback", error, {
        nodeId: node.id,
        nodeKind: node.kind,
        tooltipVisible,
      });
      tooltip = FALLBACK_TOOLTIP;
    }
  }
  const showTargetHandle = !preview && node.category !== "start" && !isOutboundEmailLogger;
  const showPromptBlockTargetHandle = !isPromptBlockNode(node);
  const showSourceHandle = !preview && node.category !== "end";
  const successHandleStyle = {
    top: `${getToolSourceHandleAnchorRatio(TOOL_SUCCESS_HANDLE_ID) * 100}%`,
  } satisfies CSSProperties;
  const failureHandleStyle = {
    top: `${getToolSourceHandleAnchorRatio(TOOL_FAILURE_HANDLE_ID) * 100}%`,
  } satisfies CSSProperties;
  const contextSourceHandleStyle = {
    top: `${getToolSourceHandleAnchorRatio(TOOL_CONTEXT_HANDLE_ID) * 100}%`,
  } satisfies CSSProperties;
  const apiToolCallHandleStyle = {
    top: `${getToolSourceHandleAnchorRatio(API_TOOL_CALL_HANDLE_ID) * 100}%`,
  } satisfies CSSProperties;
  const apiMessageHandleStyle = {
    top: `${getToolSourceHandleAnchorRatio(API_FINAL_MESSAGE_HANDLE_ID) * 100}%`,
  } satisfies CSSProperties;
  const mcpTerminalHandleStyle = {
    top: `${getToolSourceHandleAnchorRatio(MCP_TERMINAL_OUTPUT_HANDLE_ID) * 100}%`,
  } satisfies CSSProperties;
  const controlFlowLoopHandleStyle = {
    top: `${getToolSourceHandleAnchorRatio(CONTROL_FLOW_LOOP_BODY_HANDLE_ID) * 100}%`,
  } satisfies CSSProperties;
  const parallelSplitterOutputHandles = isParallelSplitterNode
    ? getParallelSplitterOutputHandles(graph, node)
    : [{ id: createParallelSplitterOutputHandleId(0), label: "connection 1", index: 0 }];
  const parallelSplitterDimensions = isParallelSplitterNode ? getParallelSplitterNodeDimensions(graph, node) : null;
  const parallelSplitterHandleStyles = parallelSplitterOutputHandles.map((_, index) => {
    const slotCount = parallelSplitterOutputHandles.length;
    const columnHeight =
      slotCount * PARALLEL_SPLITTER_HANDLE_SLOT_HEIGHT + Math.max(0, slotCount - 1) * PARALLEL_SPLITTER_HANDLE_GAP;
    const nodeHeight = parallelSplitterDimensions?.height ?? 356;
    const columnTop = (nodeHeight - columnHeight) / 2;
    const top = columnTop + index * (PARALLEL_SPLITTER_HANDLE_SLOT_HEIGHT + PARALLEL_SPLITTER_HANDLE_GAP) + PARALLEL_SPLITTER_HANDLE_SLOT_HEIGHT / 2;
    return {
      top: `${top}px`,
    } satisfies CSSProperties;
  });
  const parallelSplitterHandleSignature = parallelSplitterOutputHandles.map((handle) => handle.id).join("|");
  const parallelSplitterRenderKey = isParallelSplitterNode
    ? `${node.id}:${parallelSplitterHandleSignature}:${parallelSplitterDimensions?.height ?? "auto"}`
    : node.id;
  useEffect(() => {
    if (!isParallelSplitterNode) {
      return;
    }
    updateNodeInternals(node.id);
  }, [isParallelSplitterNode, node.id, parallelSplitterHandleSignature, parallelSplitterDimensions?.height, updateNodeInternals]);
  const logicConditionConfig = isLogicConditionsNode ? normalizeLogicConditionConfig(node.config).normalized : null;
  const logicConditionOutputHandles = isLogicConditionsNode
    ? [
        ...(logicConditionConfig?.branches ?? []).map((branch, index) => ({
          id: String(branch.output_handle_id ?? `${CONTROL_FLOW_IF_HANDLE_ID}-${index + 1}`),
          label: branch.label.trim() || `Branch ${index + 1}`,
          toneClassName: "graph-node-output-port--success",
        })),
        {
          id: String(logicConditionConfig?.else_output_handle_id ?? node.config.else_output_handle_id ?? CONTROL_FLOW_ELSE_HANDLE_ID),
          label: "Else",
          toneClassName: "graph-node-output-port--failure",
        },
      ]
    : [];
  const logicConditionHandleStyles = logicConditionOutputHandles.map((_, index) => {
    const totalHandles = logicConditionOutputHandles.length;
    const topRatio =
      totalHandles <= 1 ? 0.5 : 0.28 + (index / Math.max(1, totalHandles - 1)) * 0.52;
    return {
      top: `${topRatio * 100}%`,
    } satisfies CSSProperties;
  });
  const primaryTargetHandleStyle = {
    top: `${getApiToolContextTargetAnchorRatio(null) * 100}%`,
  } satisfies CSSProperties;
  const contextTargetHandleStyle = {
    top: `${getApiToolContextTargetAnchorRatio(API_TOOL_CONTEXT_HANDLE_ID) * 100}%`,
  } satisfies CSSProperties;
  const outlookDraftEmailLogTargetHandleStyle = {
    top: `${getNodeTargetAnchorRatio(node, OUTLOOK_DRAFT_EMAIL_LOG_TARGET_HANDLE_ID) * 100}%`,
  } satisfies CSSProperties;
  const iconLabel = KIND_LABELS[node.kind] ?? node.kind.slice(0, 2).toUpperCase();
  const subtitle =
    node.kind === "model"
      ? String(node.config.provider_name ?? node.model_provider_name ?? node.provider_label ?? node.provider_id)
      : node.provider_label ?? node.provider_id;
  const contextBuilderCount = isContextBuilderNode ? contextBuilderPlaceholderCount(graph, node) : 0;
  const contextBuilderSummary = isContextBuilderNode
    ? contextBuilderRuntime && contextBuilderRuntime.totalCount > 0
      ? `${contextBuilderRuntime.fulfilledCount + contextBuilderRuntime.errorCount}/${contextBuilderRuntime.totalCount} inputs settled${
          contextBuilderRuntime.errorCount > 0 ? ` (${contextBuilderRuntime.errorCount} error${contextBuilderRuntime.errorCount === 1 ? "" : "s"})` : ""
        }${contextBuilderRuntime.isWaitingToForward ? " · holding downstream" : ""}`
      : contextBuilderCount > 0
        ? `${contextBuilderCount} named input${contextBuilderCount === 1 ? "" : "s"}`
        : "Connect text inputs to build placeholders"
    : null;
  const isContextConnected = isContextProviderNode ? hasContextConnection(graph, node) : false;
  const contextBooted = isContextProviderNode ? isContextBooted(catalog, node) : false;
  const displayStatus = status;
  const isActive = displayStatus === "active";
  const executorRetriesEnabled = node.kind === "mcp_tool_executor" ? node.config.allow_retries !== false : false;
  const supabaseIteratorIncludesProcessedRows = isSupabaseTableRowsNode && node.config.include_previously_processed_rows === true;
  const modelConfiguredResponseMode = isModelNode ? configuredResponseMode(node) : null;
  const modelEffectiveResponseMode = isModelNode ? inferModelResponseMode(graph, node) : null;
  const executorConfiguredResponseMode = node.kind === "mcp_tool_executor" ? configuredResponseMode(node) : null;
  const isSpreadsheetRowNode = node.provider_id === SPREADSHEET_ROW_PROVIDER_ID;
  const isSpreadsheetMatrixDecisionNode = node.provider_id === SPREADSHEET_MATRIX_DECISION_PROVIDER_ID;
  const isSpreadsheetBackedNode = isSpreadsheetRowNode || isSpreadsheetMatrixDecisionNode;
  const showsModelToolHandles = isModelNode && !isSpreadsheetMatrixDecisionNode;
  const spreadsheetIteratorState = isSpreadsheetRowNode || isSupabaseTableRowsNode ? runState?.iterator_states?.[node.id] : undefined;
  const spreadsheetIteratorStatus =
    spreadsheetIteratorState && typeof spreadsheetIteratorState.status === "string" ? spreadsheetIteratorState.status : null;
  const spreadsheetRowSummary = spreadsheetIteratorState != null
    ? (() => {
        const current = spreadsheetIteratorState.current_row_index;
        const total = spreadsheetIteratorState.total_rows;
        if (typeof total !== "number" || total === 0) return null;
        if (typeof current !== "number") return null;
        if (spreadsheetIteratorStatus === "running") return `Row ${current} / ${total}`;
        if (spreadsheetIteratorStatus === "completed") return `${total} row${total === 1 ? "" : "s"} complete`;
        if (spreadsheetIteratorStatus === "failed") return `Failed at row ${current} / ${total}`;
        return `Row ${current} / ${total}`;
      })()
    : null;
  const availableSpreadsheetFiles = availableProjectFiles.filter(isSpreadsheetProjectFile);
  const selectedSpreadsheetFile =
    availableSpreadsheetFiles.find((file) => file.file_id === String(node.config.project_file_id ?? "").trim()) ??
    availableSpreadsheetFiles.find((file) => file.storage_path === String(node.config.file_path ?? "").trim()) ??
    availableSpreadsheetFiles.find((file) => file.name === String(node.config.project_file_name ?? "").trim()) ??
    null;
  const hasManualSpreadsheetPath = !selectedSpreadsheetFile && String(node.config.file_path ?? "").trim().length > 0;
  const spreadsheetSelectValue = selectedSpreadsheetFile?.file_id ?? (hasManualSpreadsheetPath ? "__manual__" : "");
  const spreadsheetSelectLabel =
    selectedSpreadsheetFile?.name ??
    (hasManualSpreadsheetPath ? String(node.config.file_path ?? "").trim().split("/").pop() ?? "Manual path" : "No spreadsheet");
  const spreadsheetStartRowValue =
    typeof node.config.start_row_index === "number" || typeof node.config.start_row_index === "string"
      ? String(node.config.start_row_index)
      : "2";
  const isPythonScriptRunnerNode = node.provider_id === PYTHON_SCRIPT_RUNNER_PROVIDER_ID;
  const availablePythonScriptFiles = isPythonScriptRunnerNode
    ? availableProjectFiles.filter(isPythonProjectFile)
    : [];
  const selectedPythonScriptFile = isPythonScriptRunnerNode
    ? availablePythonScriptFiles.find((file) => file.file_id === String(node.config.script_file_id ?? "").trim()) ??
      availablePythonScriptFiles.find((file) => file.name === String(node.config.script_file_name ?? "").trim()) ??
      null
    : null;
  const pythonScriptSelectValue = selectedPythonScriptFile?.file_id ?? "";
  const logicBranchSummary =
    isLogicConditionsNode &&
    runtimeOutput &&
    typeof runtimeOutput === "object" &&
    runtimeOutput !== null &&
    "metadata" in runtimeOutput &&
    typeof runtimeOutput.metadata === "object" &&
    runtimeOutput.metadata !== null
      ? String(
          (runtimeOutput.metadata as Record<string, unknown>).matched_branch_label ??
            (runtimeOutput.metadata as Record<string, unknown>).matched_clause_label ??
            "Else",
        )
      : null;
  const logicMetadata =
    isLogicConditionsNode &&
    runtimeOutput &&
    typeof runtimeOutput === "object" &&
    runtimeOutput !== null &&
    "metadata" in runtimeOutput &&
    typeof runtimeOutput.metadata === "object" &&
    runtimeOutput.metadata !== null
      ? (runtimeOutput.metadata as Record<string, unknown>)
      : null;
  const logicConditionSummary =
    logicMetadata && Array.isArray(logicMetadata.branch_evaluations)
      ? (logicMetadata.branch_evaluations as unknown[])
          .filter((entry): entry is Record<string, unknown> => typeof entry === "object" && entry !== null && !Array.isArray(entry))
          .map((entry) => {
            const label = typeof entry.label === "string" && entry.label.trim().length > 0 ? entry.label : "Branch";
            const matched = entry.matched === true ? "true" : "false";
            const trace =
              typeof entry.trace === "object" && entry.trace !== null && !Array.isArray(entry.trace)
                ? (entry.trace as Record<string, unknown>)
                : null;
            const sampleChild =
              trace && Array.isArray(trace.children)
                ? trace.children.find((child) => typeof child === "object" && child !== null && !Array.isArray(child)) as Record<string, unknown> | undefined
                : undefined;
            const actualValue =
              sampleChild && Object.prototype.hasOwnProperty.call(sampleChild, "actual_value")
                ? ` (${formatLogicEvaluationValue(sampleChild.actual_value)})`
                : "";
            return `${label}: ${matched}${actualValue}`;
          })
          .join("\n")
      : logicMetadata && Array.isArray(logicMetadata.condition_evaluations)
        ? (logicMetadata.condition_evaluations as unknown[])
            .filter((entry): entry is Record<string, unknown> => typeof entry === "object" && entry !== null && !Array.isArray(entry))
            .map((entry) => {
              const label = typeof entry.label === "string" && entry.label.trim().length > 0 ? entry.label : "Clause";
              const matched = entry.matched === true ? "true" : "false";
              const actualValue = Object.prototype.hasOwnProperty.call(entry, "actual_value")
                ? ` (${formatLogicEvaluationValue(entry.actual_value)})`
                : "";
              return `${label}: ${matched}${actualValue}`;
            })
            .join("\n")
      : null;
  const logicConditionDisplayText =
    logicConditionSummary != null
      ? `branch: ${logicBranchSummary ?? "Else"}\n${logicConditionSummary}`
      : null;
  const contextBindingLabel = isContextProviderNode
    ? contextBooted && isContextConnected
      ? "Bound and MCP booted"
      : isContextConnected
        ? "Bound but MCP not booted"
        : contextBooted
          ? "MCP booted but not bound"
          : "MCP not booted and not bound"
    : null;
  const statusLabel = contextBindingLabel
    ? `${formatRunStatusLabel(displayStatus)} • ${contextBindingLabel}`
    : formatRunStatusLabel(displayStatus);
  const nodeCardClassName = `graph-node-card graph-node-card--${displayStatus} ${isRoutableTool ? "graph-node-card--tool-outputs" : ""} ${
    isContextProviderNode ? "graph-node-card--tool-context-provider" : ""
  } ${
    isControlFlowUnitNode ? "graph-node-card--control-flow" : ""
  } ${
    isParallelSplitterNode ? "graph-node-card--parallel-splitter" : ""
  } ${
    isLogicConditionsNode ? "graph-node-card--logic-conditions" : ""
  } ${
    isModelNode ? "graph-node-card--model-inputs" : ""
  } ${
    isModelNode ? "graph-node-card--model-outputs" : ""
  } ${
    node.kind === "mcp_tool_executor" ? "graph-node-card--mcp-tool-executor" : ""
  } ${
    isDisplayNode ? "graph-node-card--display-node" : ""
  } ${
    isContextBuilderNode ? "graph-node-card--context-builder" : ""
  } ${selected ? "is-selected" : ""} ${tooltipVisible ? "is-tooltip-visible" : ""} ${preview ? "is-preview" : ""} ${
    isConnectionMagnetized ? "is-connection-magnetized" : ""
  }`;

  if (isWireJunction) {
    return (
      <div
        className={`graph-junction-node graph-junction-node--${status} ${selected ? "is-selected" : ""} ${isConnectionMagnetized ? "is-connection-magnetized" : ""}`}
        tabIndex={preview ? -1 : 0}
        aria-label="Wire junction"
        onMouseDown={(event) => {
          if (preview || event.button !== 0) {
            return;
          }
          event.preventDefault();
          event.stopPropagation();
          onJunctionPointerDown(node.id, { x: event.clientX, y: event.clientY });
        }}
      >
        {showTargetHandle ? (
          <Handle
            type="target"
            position={Position.Left}
            className={`graph-node-handle graph-node-handle-target graph-junction-handle ${isConnectionMagnetized ? "graph-node-handle-valid is-magnetized" : ""}`}
          />
        ) : null}
        {showSourceHandle ? (
          <Handle
            type="source"
            position={Position.Right}
            className="graph-node-handle graph-node-handle-source graph-junction-handle"
          />
        ) : null}
      </div>
    );
  }

  return (
    <div
      key={parallelSplitterRenderKey}
      className={nodeCardClassName}
      style={
        {
          "--node-kind-color": kindColor,
          ...(parallelSplitterDimensions
            ? { "--parallel-splitter-height": `${parallelSplitterDimensions.height}px` }
            : {}),
        } as CSSProperties
      }
      tabIndex={preview ? -1 : 0}
      aria-label={`${displayLabel} ${node.kind} node`}
      onContextMenu={(event) => {
        if (preview) {
          return;
        }
        event.preventDefault();
        event.stopPropagation();
        onToggleTooltip(node.id);
      }}
    >
      {showTargetHandle && showPromptBlockTargetHandle && !isContextProviderNode ? (
        <Handle
          type="target"
          position={Position.Left}
          className={`graph-node-handle graph-node-handle-target ${isConnectionMagnetized ? "graph-node-handle-valid is-magnetized" : ""}`}
          style={isModelNode ? primaryTargetHandleStyle : undefined}
        />
      ) : null}
      {showTargetHandle && showsModelToolHandles ? (
        <>
          <div className="graph-node-input-port graph-node-input-port--context" style={contextTargetHandleStyle} aria-hidden="true">
            <span className="graph-node-output-port-label">Tool Context</span>
          </div>
          <Handle
            id={API_TOOL_CONTEXT_HANDLE_ID}
            type="target"
            position={Position.Left}
            className={`graph-node-handle graph-node-handle-target graph-node-handle-target--context ${isConnectionMagnetized ? "graph-node-handle-valid is-magnetized" : ""}`}
            style={contextTargetHandleStyle}
          />
        </>
      ) : null}
      {showTargetHandle && isOutlookDraftNode ? (
        <>
          <div className="graph-node-input-port graph-node-input-port--email-log" style={outlookDraftEmailLogTargetHandleStyle} aria-hidden="true">
            <span className="graph-node-output-port-label">Email Log</span>
          </div>
          <Handle
            id={OUTLOOK_DRAFT_EMAIL_LOG_TARGET_HANDLE_ID}
            type="target"
            position={Position.Left}
            className={`graph-node-handle graph-node-handle-target graph-node-handle-target--email-log ${isConnectionMagnetized ? "graph-node-handle-valid is-magnetized" : ""}`}
            style={outlookDraftEmailLogTargetHandleStyle}
          />
        </>
      ) : null}
      <div className="graph-node-card-inner">
        <div className="graph-node-header">
          <div className="graph-node-icon" aria-hidden="true">
            {iconLabel}
          </div>
          <div className="graph-node-body">
            <strong className="graph-node-title">{displayLabel}</strong>
            <div className="graph-node-subtitle">{subtitle}</div>
          </div>
          <div className="graph-node-badge" aria-label={`Node status: ${statusLabel}`}>
            {isActive ? (
              <span className="graph-node-badge-spinner">
                <span className="graph-node-badge-spinner-core" />
              </span>
            ) : (
              <span className="graph-node-badge-dot" />
            )}
            <span className="graph-node-status-tooltip" role="status">
              {statusLabel}
            </span>
          </div>
        </div>
        <div className="graph-node-meta">
          <span className="graph-node-chip">{node.category}</span>
          <span className="graph-node-meta-text">{node.kind}</span>
          {isModelNode ? (
            <span className="graph-node-chip graph-node-chip--response-mode">
              response {modelConfiguredResponseMode}
              {modelConfiguredResponseMode === "auto" && modelEffectiveResponseMode && modelEffectiveResponseMode !== "auto"
                ? ` -> ${modelEffectiveResponseMode}`
                : ""}
            </span>
          ) : null}
          {node.kind === "mcp_tool_executor" ? (
            <span className="graph-node-chip graph-node-chip--response-mode">
              follow-up {node.config.enable_follow_up_decision === true ? executorConfiguredResponseMode : "off"}
            </span>
          ) : null}
        </div>
        {node.kind === "mcp_tool_executor" ? (
          <div className="graph-node-inline-toggle-row">
            <span className="graph-node-inline-toggle-label">Retries</span>
            <button
              type="button"
              className={`graph-node-inline-toggle ${executorRetriesEnabled ? "is-enabled" : "is-disabled"}`}
              aria-pressed={executorRetriesEnabled}
              aria-label={`${executorRetriesEnabled ? "Disable" : "Enable"} retries for ${displayLabel}`}
              onMouseDown={(event) => event.stopPropagation()}
              onClick={(event) => {
                event.stopPropagation();
                onToggleExecutorRetries(node.id);
              }}
            >
              <span className="graph-node-inline-toggle-track">
                <span className="graph-node-inline-toggle-thumb" />
              </span>
              <span className="graph-node-inline-toggle-text">{executorRetriesEnabled ? "On" : "Off"}</span>
            </button>
          </div>
        ) : null}
        {isSupabaseTableRowsNode ? (
          <div className="graph-node-inline-toggle-row">
            <span className="graph-node-inline-toggle-label">Include Cached</span>
            <button
              type="button"
              className={`graph-node-inline-toggle ${supabaseIteratorIncludesProcessedRows ? "is-enabled" : "is-disabled"}`}
              aria-pressed={supabaseIteratorIncludesProcessedRows}
              aria-label={`${supabaseIteratorIncludesProcessedRows ? "Disable" : "Enable"} cached row replay for ${displayLabel}`}
              onMouseDown={(event) => event.stopPropagation()}
              onClick={(event) => {
                event.stopPropagation();
                onToggleSupabaseIteratorIncludeProcessedRows(node.id);
              }}
            >
              <span className="graph-node-inline-toggle-track">
                <span className="graph-node-inline-toggle-thumb" />
              </span>
              <span className="graph-node-inline-toggle-text">{supabaseIteratorIncludesProcessedRows ? "On" : "Off"}</span>
            </button>
          </div>
        ) : null}
        {contextBuilderSummary ? <div className="graph-node-summary">{contextBuilderSummary}</div> : null}
        {spreadsheetRowSummary ? (
          <div
            className={`graph-node-summary graph-node-summary--row-progress${
              spreadsheetIteratorStatus === "running" ? " is-active" : ""
            }`}
          >
            {spreadsheetRowSummary}
          </div>
        ) : null}
        {!preview && isSpreadsheetBackedNode && (availableSpreadsheetFiles.length > 0 || hasManualSpreadsheetPath) ? (
          <div className="graph-node-inline-select-row">
            <span className="graph-node-inline-toggle-label">Spreadsheet</span>
            <select
              className="graph-node-inline-select nodrag"
              value={spreadsheetSelectValue}
              aria-label={`Select spreadsheet for ${displayLabel}`}
              onMouseDown={stopInlineControlPropagation}
              onPointerDown={stopInlineControlPropagation}
              onClick={stopInlineControlPropagation}
              onKeyDown={stopInlineControlPropagation}
              onChange={(event) => {
                event.stopPropagation();
                const nextFileId = event.target.value;
                if (!nextFileId || nextFileId === "__manual__") {
                  return;
                }
                onSelectSpreadsheetFile(node.id, nextFileId);
              }}
            >
              {!hasManualSpreadsheetPath ? <option value="">Select spreadsheet</option> : null}
              {hasManualSpreadsheetPath ? <option value="__manual__">Manual: {spreadsheetSelectLabel}</option> : null}
              {availableSpreadsheetFiles.map((file) => (
                <option key={file.file_id} value={file.file_id}>
                  {file.name}
                </option>
              ))}
            </select>
          </div>
        ) : null}
        {!preview && isSpreadsheetRowNode ? (
          <div className="graph-node-inline-select-row">
            <span className="graph-node-inline-toggle-label">Start Row</span>
            <input
              type="number"
              min={2}
              step={1}
              className="graph-node-inline-select nodrag"
              value={spreadsheetStartRowValue}
              aria-label={`Starting row index for ${displayLabel}`}
              onMouseDown={stopInlineControlPropagation}
              onPointerDown={stopInlineControlPropagation}
              onClick={stopInlineControlPropagation}
              onKeyDown={stopInlineControlPropagation}
              onChange={(event) => {
                event.stopPropagation();
                const rawValue = event.target.value;
                if (rawValue === "") {
                  onChangeSpreadsheetStartRowIndex(node.id, "");
                  return;
                }
                const parsed = Number(rawValue);
                if (Number.isInteger(parsed) && parsed >= 2) {
                  onChangeSpreadsheetStartRowIndex(node.id, Math.floor(parsed));
                  return;
                }
                onChangeSpreadsheetStartRowIndex(node.id, rawValue);
              }}
            />
          </div>
        ) : null}
        {!preview && isPythonScriptRunnerNode ? (
          <div className="graph-node-inline-select-row">
            <span className="graph-node-inline-toggle-label">Script</span>
            <select
              className="graph-node-inline-select nodrag"
              value={pythonScriptSelectValue}
              aria-label={`Select Python script for ${displayLabel}`}
              onMouseDown={stopInlineControlPropagation}
              onPointerDown={stopInlineControlPropagation}
              onClick={stopInlineControlPropagation}
              onKeyDown={stopInlineControlPropagation}
              onChange={(event) => {
                event.stopPropagation();
                onSelectPythonScriptFile(node.id, event.target.value);
              }}
            >
              <option value="">
                {availablePythonScriptFiles.length === 0 ? "Upload a .py project file…" : "Select a .py project file…"}
              </option>
              {availablePythonScriptFiles.map((file) => (
                <option key={file.file_id} value={file.file_id}>
                  {file.name}
                </option>
              ))}
            </select>
          </div>
        ) : null}
        {!preview && isSupabaseNode ? (
          <>
            <div className="graph-node-inline-select-row">
              <span className="graph-node-inline-toggle-label">Project</span>
              <select
                className="graph-node-inline-select nodrag"
                value={supabaseSelectValue}
                aria-label={`Select Supabase project for ${displayLabel}`}
                onMouseDown={stopInlineControlPropagation}
                onPointerDown={stopInlineControlPropagation}
                onClick={stopInlineControlPropagation}
                onKeyDown={stopInlineControlPropagation}
                onChange={(event) => {
                  event.stopPropagation();
                  onSelectSupabaseConnection(node.id, event.target.value);
                }}
              >
                <option value="">Compatibility mode</option>
                {supabaseConnectionOptions.map((connection) => (
                  <option key={connection.value} value={connection.value}>
                    {connection.label}
                  </option>
                ))}
              </select>
            </div>
            {supabaseConnectionMissing ? (
              <div className="graph-node-summary graph-node-summary--warning">
                Assigned project is missing. Pick another connection or switch to compatibility mode.
              </div>
            ) : null}
          </>
        ) : null}
        {logicConditionDisplayText ? (
          <div
            role="button"
            tabIndex={preview ? -1 : 0}
            className="graph-node-inline-display graph-node-inline-display--compact graph-node-inline-display--narrow"
            onMouseDown={(event) => event.stopPropagation()}
            onClick={(event) => {
              event.stopPropagation();
              onOpenConditionResults(node.id);
            }}
            onKeyDown={(event) => {
              if (event.key === "Enter" || event.key === " ") {
                event.preventDefault();
                event.stopPropagation();
                onOpenConditionResults(node.id);
              }
            }}
            aria-label={`Open full condition results for ${displayLabel}`}
          >
            <div className="graph-node-inline-display-header">Condition Results</div>
            <pre className="graph-node-inline-display-body">{logicConditionDisplayText}</pre>
            <span className="graph-node-inline-display-hint">Click to expand full evaluation</span>
          </div>
        ) : null}
        {isContextBuilderNode ? (
          <div
            role="button"
            tabIndex={preview ? -1 : 0}
            className="graph-node-inline-display graph-node-inline-display--compact"
            onMouseDown={(event) => event.stopPropagation()}
            onClick={(event) => {
              event.stopPropagation();
              onOpenContextBuilderPayload(node.id);
            }}
            onKeyDown={(event) => {
              if (event.key === "Enter" || event.key === " ") {
                event.preventDefault();
                event.stopPropagation();
                onOpenContextBuilderPayload(node.id);
              }
            }}
            aria-label={`Open full payload for ${displayLabel}`}
          >
            <div className="graph-node-inline-display-header">Resolved Payload</div>
            <pre className="graph-node-inline-display-body">{contextBuilderDisplayText}</pre>
            <span className="graph-node-inline-display-hint">Click to expand full payload</span>
          </div>
        ) : null}
        {isDisplayNode ? (
          <div
            role="button"
            tabIndex={preview ? -1 : 0}
            className="graph-node-inline-display"
            onMouseDown={(event) => event.stopPropagation()}
            onClick={(event) => {
              event.stopPropagation();
              onOpenDisplayResponse(node.id);
            }}
            onKeyDown={(event) => {
              if (event.key === "Enter" || event.key === " ") {
                event.preventDefault();
                event.stopPropagation();
                onOpenDisplayResponse(node.id);
              }
            }}
            aria-label={`Open full response for ${displayLabel}`}
          >
            <div className="graph-node-inline-display-header">Run Envelope</div>
            <pre className="graph-node-inline-display-body">{displayText}</pre>
            <span className="graph-node-inline-display-hint">Click to expand</span>
          </div>
        ) : null}
        {!preview && (node.category === "tool" || node.kind === "model" || isPromptBlockNode(node) || isLogicConditionsNode || isStructuredPayloadBuilderNode || isRuntimeNormalizerNode || isSupabaseSqlNode || isSupabaseDataNode || isSupabaseTableRowsNode || isSupabaseRowWriteNode || isOutboundEmailLogger || isPythonScriptRunnerNode) ? (
          <div className="graph-node-card-actions" aria-hidden="false">
            <button
              type="button"
              className="secondary-button graph-node-card-button"
              onMouseDown={(event) => event.stopPropagation()}
              onClick={(event) => {
                event.stopPropagation();
                if (node.category === "tool") {
                  onOpenToolDetails(node.id);
                  return;
                }
                if (isPromptBlockNode(node)) {
                  onOpenPromptBlockDetails(node.id);
                  return;
                }
                onOpenProviderDetails(node.id);
              }}
            >
              {node.category === "tool"
                ? "Learn More"
                : isLogicConditionsNode
                  ? "Learn More"
                : isStructuredPayloadBuilderNode
                  ? "Learn More"
                : isRuntimeNormalizerNode
                  ? "Learn More"
                : isSupabaseSqlNode
                  ? "Learn More"
                : isSupabaseDataNode
                  ? "Learn More"
                : isSupabaseTableRowsNode
                  ? "Learn More"
                : isSupabaseRowWriteNode
                  ? "Learn More"
                : isOutboundEmailLogger
                  ? "Learn More"
                : isPythonScriptRunnerNode
                  ? "Learn More"
                : isPromptBlockNode(node)
                  ? "More Info"
                  : "Provider Info"}
            </button>
          </div>
        ) : null}
        {!preview && isDiscordStartNode ? (
          <div className="graph-node-card-actions" aria-hidden="false">
            <button
              type="button"
              className="secondary-button graph-node-card-button"
              onMouseDown={(event) => event.stopPropagation()}
              onClick={(event) => {
                event.stopPropagation();
                onOpenDiscordTriggerConfig(node.id);
              }}
            >
              Configure Trigger
            </button>
          </div>
        ) : null}
        {!preview && isCronStartNode ? (
          <div className="graph-node-card-actions" aria-hidden="false">
            <button
              type="button"
              className="secondary-button graph-node-card-button"
              onMouseDown={(event) => event.stopPropagation()}
              onClick={(event) => {
                event.stopPropagation();
                onOpenCronScheduleConfig(node.id);
              }}
            >
              Configure Schedule
            </button>
          </div>
        ) : null}
      </div>
      {!preview ? (
        <div className="graph-node-tooltip" role="tooltip">
          <div className="graph-node-tooltip-eyebrow">{tooltip.eyebrow}</div>
          <strong className="graph-node-tooltip-title">{tooltip.title}</strong>
          {tooltip.description ? <p className="graph-node-tooltip-description">{tooltip.description}</p> : null}
          {tooltip.sections.map((section) => (
            <section key={section.title} className="graph-node-tooltip-section">
              <div className="graph-node-tooltip-section-title">{section.title}</div>
              <div className="graph-node-tooltip-grid">
                {section.rows.map((row) => (
                  <div key={`${section.title}-${row.label}`} className="graph-node-tooltip-row">
                    <span className="graph-node-tooltip-label">{row.label}</span>
                    <span className="graph-node-tooltip-value">{row.value}</span>
                  </div>
                ))}
              </div>
            </section>
          ))}
          {tooltip.parameters.length > 0 ? (
            <section className="graph-node-tooltip-section">
              <div className="graph-node-tooltip-section-title">Parameters</div>
              <div className="graph-node-parameter-list">
                {tooltip.parameters.map((parameter) => (
                  <div key={parameter.name} className="graph-node-parameter">
                    <div className="graph-node-parameter-header">
                      <code>{parameter.name}</code>
                      <span className="graph-node-parameter-type">{parameter.type}</span>
                      {parameter.required ? <span className="graph-node-parameter-required">required</span> : null}
                    </div>
                    {parameter.description ? (
                      <div className="graph-node-parameter-description">{parameter.description}</div>
                    ) : null}
                    {parameter.source ? <div className="graph-node-parameter-source">Source: {parameter.source}</div> : null}
                  </div>
                ))}
              </div>
            </section>
          ) : null}
          {tooltip.emptyState ? <div className="graph-node-tooltip-empty">{tooltip.emptyState}</div> : null}
        </div>
      ) : null}
      {showSourceHandle && isRoutableTool ? (
        <>
          <div className="graph-node-output-port graph-node-output-port--success" style={successHandleStyle} aria-hidden="true">
            <span className="graph-node-output-port-label">
              {node.kind === "mcp_tool_executor" ? "On Finish" : "On Success"}
            </span>
          </div>
          <Handle
            id={TOOL_SUCCESS_HANDLE_ID}
            type="source"
            position={Position.Right}
            className="graph-node-handle graph-node-handle-source graph-node-handle-source--success"
            style={successHandleStyle}
          />
          <div className="graph-node-output-port graph-node-output-port--failure" style={failureHandleStyle} aria-hidden="true">
            <span className="graph-node-output-port-label">On Failure</span>
          </div>
          <Handle
            id={TOOL_FAILURE_HANDLE_ID}
            type="source"
            position={Position.Right}
            className="graph-node-handle graph-node-handle-source graph-node-handle-source--failure"
            style={failureHandleStyle}
          />
          {node.kind === "mcp_tool_executor" ? (
            <>
              <div className="graph-node-output-port graph-node-output-port--context" style={mcpTerminalHandleStyle} aria-hidden="true">
                <span className="graph-node-output-port-label">Terminal</span>
              </div>
              <Handle
                id={MCP_TERMINAL_OUTPUT_HANDLE_ID}
                type="source"
                position={Position.Right}
                className="graph-node-handle graph-node-handle-source graph-node-handle-source--context"
                style={mcpTerminalHandleStyle}
              />
            </>
          ) : null}
        </>
      ) : null}
      {showSourceHandle && isContextProviderNode ? (
        <>
          <div className="graph-node-output-port graph-node-output-port--context" style={contextSourceHandleStyle} aria-hidden="true">
            <span className="graph-node-output-port-label">Context</span>
          </div>
          <Handle
            id={TOOL_CONTEXT_HANDLE_ID}
            type="source"
            position={Position.Right}
            className="graph-node-handle graph-node-handle-source graph-node-handle-source--context"
            style={contextSourceHandleStyle}
          />
        </>
      ) : null}
      {showSourceHandle && showsModelToolHandles ? (
        <>
          <div className="graph-node-output-port graph-node-output-port--tool-call" style={apiToolCallHandleStyle} aria-hidden="true">
            <span className="graph-node-output-port-label">Tool Calls</span>
          </div>
          <Handle
            id={API_TOOL_CALL_HANDLE_ID}
            type="source"
            position={Position.Right}
            className="graph-node-handle graph-node-handle-source graph-node-handle-source--tool-call"
            style={apiToolCallHandleStyle}
          />
          <div className="graph-node-output-port graph-node-output-port--message" style={apiMessageHandleStyle} aria-hidden="true">
            <span className="graph-node-output-port-label">Message</span>
          </div>
          <Handle
            id={API_FINAL_MESSAGE_HANDLE_ID}
            type="source"
            position={Position.Right}
            className="graph-node-handle graph-node-handle-source graph-node-handle-source--message"
            style={apiMessageHandleStyle}
          />
        </>
      ) : null}
      {showSourceHandle && isControlFlowUnitNode && (node.provider_id === SPREADSHEET_ROW_PROVIDER_ID || node.provider_id === SUPABASE_TABLE_ROWS_PROVIDER_ID) ? (
        <>
          <div className="graph-node-output-port graph-node-output-port--success" style={controlFlowLoopHandleStyle} aria-hidden="true">
            <span className="graph-node-output-port-label">Loop Body</span>
          </div>
          <Handle
            id={CONTROL_FLOW_LOOP_BODY_HANDLE_ID}
            type="source"
            position={Position.Right}
            className="graph-node-handle graph-node-handle-source graph-node-handle-source--success"
            style={controlFlowLoopHandleStyle}
          />
        </>
      ) : null}
      {showSourceHandle && isControlFlowUnitNode && isParallelSplitterNode ? (
        <>
          {parallelSplitterOutputHandles.map((handle, index) => {
            const style = parallelSplitterHandleStyles[index];
            return (
              <div key={handle.id}>
                <div className="graph-node-output-port graph-node-output-port--success graph-parallel-splitter-port" style={style} aria-hidden="true">
                  <span className="graph-node-output-port-label">{handle.label}</span>
                </div>
                <Handle
                  id={handle.id}
                  type="source"
                  position={Position.Right}
                  className="graph-node-handle graph-node-handle-source graph-node-handle-source--success graph-parallel-splitter-handle"
                  style={style}
                />
              </div>
            );
          })}
        </>
      ) : null}
      {showSourceHandle && isControlFlowUnitNode && isLogicConditionsNode ? (
        <>
          {logicConditionOutputHandles.map((handle, index) => {
            const style = logicConditionHandleStyles[index];
            const isElseHandle = handle.id === String(logicConditionConfig?.else_output_handle_id ?? node.config.else_output_handle_id ?? CONTROL_FLOW_ELSE_HANDLE_ID);
            return (
              <div key={handle.id}>
                <div className={`graph-node-output-port ${handle.toneClassName}`} style={style} aria-hidden="true">
                  <span className="graph-node-output-port-label">{handle.label}</span>
                </div>
                <Handle
                  id={handle.id}
                  type="source"
                  position={Position.Right}
                  className={`graph-node-handle graph-node-handle-source ${
                    isElseHandle ? "graph-node-handle-source--failure" : "graph-node-handle-source--success"
                  }`}
                  style={style}
                />
              </div>
            );
          })}
        </>
      ) : null}
      {showSourceHandle && !isRoutableTool && !isContextProviderNode && !showsModelToolHandles && !isControlFlowUnitNode ? (
        <Handle
          type="source"
          position={Position.Right}
          className="graph-node-handle graph-node-handle-source"
        />
      ) : null}
    </div>
  );
}

export const GraphCanvasNode = memo(GraphCanvasNodeComponent);
