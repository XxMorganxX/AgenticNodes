import { useEffect, useState } from "react";

import { previewSpreadsheetRows } from "../lib/api";
import {
  CONTROL_FLOW_ELSE_HANDLE_ID,
  defaultModelName,
  findProviderDefinition,
  PARALLEL_SPLITTER_HANDLE_COUNT_CONFIG_KEY,
  getParallelSplitterOutputHandles,
  inferModelResponseMode,
  isControlFlowNode,
  isPromptBlockNode,
  isWireJunctionNode,
  modelProviderDefinitions,
  PROMPT_BLOCK_PROVIDER_ID,
  providerDefaultConfig,
  providerModelName,
} from "../lib/editor";
import { getGraphEnvVars, resolveGraphEnvReferences } from "../lib/graphEnv";
import { normalizeLogicConditionConfig, summarizeLogicGroup } from "../lib/logicConditions";
import {
  parseResponseSchemaText,
  resolveResponseSchemaDetails,
  RESPONSE_SCHEMA_PRESETS,
  RESPONSE_SCHEMA_TEXT_CONFIG_KEY,
} from "../lib/responseSchema";
import {
  getContextBuilderBindings,
  normalizeContextBuilderHeader,
  slugifyContextBuilderPlaceholder,
  type ContextBuilderBindingRow,
} from "../lib/contextBuilderBindings";
import { getModelContextBuilderPromptVariables } from "../lib/contextBuilderPromptVariables";
import { getNodeInstanceLabel } from "../lib/nodeInstanceLabels";
import { insertTokenAtEnd, listPromptBlockAvailableVariables, PROMPT_BLOCK_STARTERS, renderPromptBlockPreview } from "../lib/promptBlockEditor";
import { SPREADSHEET_MATRIX_RECOMMENDED_USER_MESSAGE_TEMPLATE } from "../lib/spreadsheetMatrixPrompt";
import { resolveToolNodeDetails } from "../lib/toolNodeDetails";
import { useRenderDiagnostics } from "../lib/dragDiagnostics";
import { StructuredPayloadBuilderLearnMoreModal } from "./StructuredPayloadBuilderLearnMoreModal";
import type {
  EditorCatalog,
  GraphDefinition,
  GraphEdge,
  GraphNode,
  ProjectFile,
  RunState,
  SpreadsheetPreviewResult,
  ToolDefinition,
} from "../lib/types";

type GraphInspectorProps = {
  graph: GraphDefinition | null;
  catalog: EditorCatalog | null;
  availableProjectFiles?: ProjectFile[];
  runState: RunState | null;
  selectedNodeId: string | null;
  selectedEdgeId: string | null;
  onGraphChange: (graph: GraphDefinition) => void;
  onOpenProviderDetails?: (nodeId: string) => void;
  onSaveNode?: (node: GraphNode) => void;
};

function updateNode(graph: GraphDefinition, nodeId: string, updater: (node: GraphNode) => GraphNode): GraphDefinition {
  return {
    ...graph,
    nodes: graph.nodes.map((node) => (node.id === nodeId ? updater(node) : node)),
  };
}

function updateEdge(graph: GraphDefinition, edgeId: string, updater: (edge: GraphEdge) => GraphEdge): GraphDefinition {
  return {
    ...graph,
    edges: graph.edges.map((edge) => (edge.id === edgeId ? updater(edge) : edge)),
  };
}

function isToolOnline(tool: ToolDefinition): boolean {
  return tool.available !== false;
}

function isToolEnabled(tool: ToolDefinition): boolean {
  return tool.enabled !== false;
}

function toolStatusLabel(tool: ToolDefinition): string {
  if (!isToolEnabled(tool)) {
    return "disabled";
  }
  if (!isToolOnline(tool)) {
    return "offline";
  }
  return "ready";
}

function toolCanonicalName(tool: ToolDefinition): string {
  return tool.canonical_name ?? tool.name;
}

function toolLabel(tool: ToolDefinition): string {
  return tool.display_name ?? tool.name;
}

function toolMatchesReference(tool: ToolDefinition, reference: string): boolean {
  const normalizedReference = reference.trim();
  if (!normalizedReference) {
    return false;
  }
  return [toolCanonicalName(tool), tool.name, ...(tool.aliases ?? [])].includes(normalizedReference);
}

function uniqueStrings(values: string[]): string[] {
  return [...new Set(values.filter((value) => value.trim().length > 0))];
}

function parseConfigStringList(value: unknown): string[] {
  if (Array.isArray(value)) {
    return value.map((item) => String(item));
  }
  if (typeof value === "string") {
    return value
      .replace(/\n/g, ",")
      .split(",")
      .map((item) => item.trim())
      .filter((item) => item.length > 0);
  }
  return [];
}

function serializeLegacyConfigStringList(values: string[]): string {
  return values
    .map((value) => value.trim())
    .filter((value) => value.length > 0)
    .join("\n");
}

type StructuredPayloadTemplateEntry = {
  id: string;
  key: string;
  value: string;
};

function parseStructuredPayloadTemplateEntries(value: unknown): StructuredPayloadTemplateEntry[] {
  const raw = String(value ?? "").trim();
  if (!raw) {
    return [];
  }
  try {
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return [];
    }
    return Object.entries(parsed).map(([key, entryValue], index) => ({
      id: `template-entry-${index + 1}-${key}`,
      key,
      value:
        typeof entryValue === "string"
          ? entryValue
          : entryValue == null
            ? ""
            : JSON.stringify(entryValue),
    }));
  } catch {
    return [];
  }
}

function serializeStructuredPayloadTemplateEntries(entries: StructuredPayloadTemplateEntry[]): string {
  const payload: Record<string, unknown> = {};
  for (const entry of entries) {
    const key = entry.key.trim();
    if (!key) {
      continue;
    }
    const rawValue = entry.value;
    const trimmedValue = rawValue.trim();
    if (!trimmedValue) {
      payload[key] = "";
      continue;
    }
    if (trimmedValue === "null") {
      payload[key] = null;
      continue;
    }
    if (trimmedValue === "{}") {
      payload[key] = {};
      continue;
    }
    if (trimmedValue === "[]") {
      payload[key] = [];
      continue;
    }
    payload[key] = rawValue;
  }
  return JSON.stringify(payload, null, 2);
}

const CONTEXT_BUILDER_PROVIDER_ID = "core.context_builder";
const SPREADSHEET_ROW_PROVIDER_ID = "core.spreadsheet_rows";
const SUPABASE_TABLE_ROWS_PROVIDER_ID = "core.supabase_table_rows";
const SPREADSHEET_MATRIX_DECISION_PROVIDER_ID = "core.spreadsheet_matrix_decision";
const LOGIC_CONDITIONS_PROVIDER_ID = "core.logic_conditions";
const PARALLEL_SPLITTER_PROVIDER_ID = "core.parallel_splitter";
const WRITE_TEXT_FILE_PROVIDER_ID = "core.write_text_file";
const STRUCTURED_PAYLOAD_BUILDER_PROVIDER_ID = "core.structured_payload_builder";
const APOLLO_EMAIL_LOOKUP_PROVIDER_ID = "core.apollo_email_lookup";
const LINKEDIN_PROFILE_FETCH_PROVIDER_ID = "core.linkedin_profile_fetch";
const RUNTIME_NORMALIZER_PROVIDER_ID = "core.runtime_normalizer";
const CONTEXT_BUILDER_IDENTIFIER_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;
const CONTEXT_BUILDER_TOKEN_PATTERN = /\{([A-Za-z_][A-Za-z0-9_]*)\}/g;
const CONTEXT_BUILDER_BASE_VARIABLES = ["current_node_id", "documents", "graph_id", "input_payload", "run_id"];

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function buildContextBuilderTemplate(bindings: ContextBuilderBindingRow[]): string {
  return bindings
    .map((binding) => `# ${binding.header}\n{${binding.placeholder}}`)
    .join("\n\n");
}

function buildContextBuilderStructuredPreview(
  bindings: ContextBuilderBindingRow[],
  previewValues: Record<string, string>,
): string {
  return JSON.stringify(
    bindings.map((binding) => ({
      [binding.header]: previewValues[binding.placeholder] ?? "",
    })),
    null,
    2,
  );
}

function contextBuilderPreviewBlocks(
  bindings: ContextBuilderBindingRow[],
  previewValues: Record<string, string>,
): Array<{ sourceNodeId: string; header: string; body: string }> {
  return bindings.map((binding) => ({
    sourceNodeId: binding.sourceNodeId,
    header: binding.header,
    body: previewValues[binding.placeholder] ?? "",
  }));
}

function extractTemplateTokens(template: string): string[] {
  return uniqueStrings(Array.from(template.matchAll(CONTEXT_BUILDER_TOKEN_PATTERN)).map((match) => match[1] ?? ""));
}

function stringifyPreviewValue(value: unknown): string {
  if (typeof value === "string") {
    return value;
  }
  if (value === null || value === undefined) {
    return "";
  }
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

function truncatePreview(value: string, limit = 180): string {
  return value.length > limit ? `${value.slice(0, limit - 1)}...` : value;
}

function isSpreadsheetProjectFile(file: ProjectFile): boolean {
  const candidate = `${file.name} ${file.storage_path}`.toLowerCase();
  return file.status === "ready" && (candidate.endsWith(".csv") || candidate.endsWith(".xlsx"));
}

function incomingEdgeContractLabel(graph: GraphDefinition, node: GraphNode): string {
  const incomingEdges = graph.edges.filter((edge) => edge.target_id === node.id && edge.kind !== "binding");
  if (incomingEdges.length === 0) {
    return "No incoming execution edge";
  }
  if (incomingEdges.length > 1) {
    return "Multiple possible incoming contracts";
  }
  const incomingEdge = incomingEdges[0];
  const sourceNode = graph.nodes.find((candidate) => candidate.id === incomingEdge.source_id);
  if (!sourceNode) {
    return "Unknown source";
  }
  if (
    incomingEdge.condition?.type === "result_payload_path_equals" &&
    incomingEdge.condition.path === "metadata.contract" &&
    typeof incomingEdge.condition.value === "string"
  ) {
    return incomingEdge.condition.value;
  }
  if (sourceNode.kind === "model" && incomingEdge.source_handle_id === "api-message") {
    return "message_envelope";
  }
  if (sourceNode.kind === "model" && incomingEdge.source_handle_id === "api-tool-call") {
    return "tool_call_envelope";
  }
  if (sourceNode.kind === "tool" || sourceNode.kind === "mcp_tool_executor") {
    return "tool_result_envelope";
  }
  return graph.node_providers?.find((provider) => provider.provider_id === sourceNode.provider_id)?.category === "start"
    ? "message_envelope"
    : sourceNode.category === "data" || sourceNode.category === "control_flow_unit"
      ? "data_envelope"
      : "Envelope inferred from source node";
}

function getContextBuilderSourcePreview(runState: RunState | null, sourceNodeId: string): string | null {
  const rawOutput = runState?.node_outputs?.[sourceNodeId];
  if (rawOutput === undefined) {
    return null;
  }
  const payload = isRecord(rawOutput) && Object.prototype.hasOwnProperty.call(rawOutput, "payload") ? rawOutput.payload : rawOutput;
  return truncatePreview(stringifyPreviewValue(payload));
}

function getPromptBlockPreview(node: GraphNode): string {
  const role = String(node.config.role ?? "user").trim() || "user";
  const name = String(node.config.name ?? "").trim();
  const content = String(node.config.content ?? "").trim();
  const header = name ? `${role} (${name})` : role;
  return truncatePreview(`${header}: ${content}`.trim());
}

function getContextBuilderSourcePreviewFromGraph(graph: GraphDefinition, runState: RunState | null, sourceNodeId: string): string | null {
  const sourceNode = graph.nodes.find((candidate) => candidate.id === sourceNodeId) ?? null;
  if (sourceNode && isPromptBlockNode(sourceNode)) {
    return getPromptBlockPreview(sourceNode);
  }
  return getContextBuilderSourcePreview(runState, sourceNodeId);
}

function getModelPromptBlockNodes(graph: GraphDefinition, modelNode: GraphNode): GraphNode[] {
  const candidateNodeIds = new Set<string>();
  const configuredNodeIds = Array.isArray(modelNode.config.prompt_block_node_ids)
    ? modelNode.config.prompt_block_node_ids.map((nodeId) => String(nodeId))
    : [];
  configuredNodeIds.forEach((nodeId) => candidateNodeIds.add(nodeId));
  graph.edges
    .filter((edge) => edge.kind === "binding" && edge.target_id === modelNode.id)
    .forEach((edge) => candidateNodeIds.add(edge.source_id));
  return [...candidateNodeIds]
    .map((nodeId) => graph.nodes.find((node) => node.id === nodeId) ?? null)
    .filter((node): node is GraphNode => node !== null && isPromptBlockNode(node));
}

function renderContextBuilderPreview(template: string, variables: Record<string, string>): string {
  return template.replace(CONTEXT_BUILDER_TOKEN_PATTERN, (_, token: string) => variables[token] ?? `{${token}}`);
}

function buildPromptOnlyMcpToolDecisionContract(hasToolContext: boolean, callableToolNames: string[]): string {
  if (!hasToolContext || callableToolNames.length > 0) {
    return "";
  }
  return [
    "MCP Tool Decision Output",
    "When MCP tool metadata is present in prompt context but no MCP tools are directly callable, you must respond using this exact structure:",
    "",
    "Uses Tool: True|False",
    'Tool Call Schema: {"tool_name":"<tool name>","arguments":{...}} or NA',
    "DELIMITER",
    "<Explain why the tool schema is needed or why no tool is needed, and describe the next step required to finish the user's request.>",
    "",
    "Rules",
    "- Set `Uses Tool` to `True` only when one of the tools described in the MCP Tool Context is required.",
    "- When `Uses Tool` is `True`, `Tool Call Schema` must be a single JSON object containing exactly `tool_name` and `arguments`.",
    "- When `Uses Tool` is `False`, `Tool Call Schema` must be `NA`.",
    "- Do not claim that you already called a tool unless you were given an actual tool result.",
    "- The content after `DELIMITER` must be plain-language guidance for the next processing step.",
  ].join("\n");
}

function buildMcpToolGuidanceBlock(callableToolNames: string[], guidanceText: string): string {
  let guidanceLines: string[] = [];
  if (callableToolNames.length > 0) {
    guidanceLines = [
      "MCP Tool Guidance",
      "Use MCP tools only when a listed live capability is needed to answer the request or complete the task.",
      "Call only MCP tools that are explicitly exposed to you and follow their schemas exactly.",
      "Do not invent MCP tool names or arguments.",
      "If no exposed MCP tool is necessary, continue without calling one.",
    ];
  }
  if (guidanceText.trim().length > 0) {
    guidanceLines = guidanceLines.length > 0
      ? [...guidanceLines, "", "Connected MCP Tool Notes:", guidanceText]
      : ["MCP Tool Guidance", guidanceText];
  }
  return guidanceLines.join("\n").trim();
}

function mcpToolPlaceholderToken(index: number): string {
  return `MCP_TOOL_${index + 1}`;
}

function buildMcpToolPlaceholderTemplate(
  tools: Array<{
    placeholderToken: string;
    displayName: string;
  }>,
): string {
  if (tools.length === 0) {
    return "";
  }
  return [
    "You are a tool calling assistant.",
    "",
    "{mcp_tool_guidance_block}",
    "",
    "You have these tools:",
    "",
    ...tools.map((tool) => `# ${tool.displayName}\n{${tool.placeholderToken}}`),
  ].join("\n\n");
}

function getModelMcpContextNodes(graph: GraphDefinition, modelNode: GraphNode): GraphNode[] {
  const candidateNodeIds = new Set<string>();
  const configuredTargetIds = Array.isArray(modelNode.config.tool_target_node_ids)
    ? modelNode.config.tool_target_node_ids.map((nodeId) => String(nodeId))
    : [];
  configuredTargetIds.forEach((nodeId) => candidateNodeIds.add(nodeId));
  graph.edges
    .filter((edge) => edge.kind === "binding" && edge.target_id === modelNode.id)
    .forEach((edge) => candidateNodeIds.add(edge.source_id));
  return [...candidateNodeIds]
    .map((nodeId) => graph.nodes.find((node) => node.id === nodeId) ?? null)
    .filter((node): node is GraphNode => node !== null && node.kind === "mcp_context_provider");
}

function describeMcpExecutorBinding(binding: unknown): string {
  if (!binding || typeof binding !== "object") {
    return "implicit latest incoming edge";
  }
  const bindingRecord = binding as Record<string, unknown>;
  const bindingType = String(bindingRecord.type ?? "latest_output");
  if (bindingType === "first_available_envelope") {
    const sources = Array.isArray(bindingRecord.sources)
      ? bindingRecord.sources.map((sourceId) => String(sourceId)).filter((sourceId) => sourceId.trim().length > 0)
      : [];
    return sources.length > 0 ? `${bindingType} from ${sources.join(", ")}` : bindingType;
  }
  const sourceId = String(bindingRecord.source ?? "").trim();
  return sourceId ? `${bindingType} from ${sourceId}` : bindingType;
}

function encodeOutputSourceBinding(binding: unknown): string {
  if (!isRecord(binding)) {
    return "auto";
  }
  const bindingType = String(binding.type ?? "").trim();
  if (bindingType === "input_payload") {
    return "input_payload";
  }
  if ((bindingType === "latest_payload" || bindingType === "latest_envelope") && typeof binding.source === "string") {
    return `${bindingType}:${binding.source}`;
  }
  return "auto";
}

function decodeOutputSourceBinding(value: string): GraphNode["config"]["source_binding"] | undefined {
  if (value === "auto") {
    return undefined;
  }
  if (value === "input_payload") {
    return { type: "input_payload" };
  }
  const [bindingType, sourceId] = value.split(":", 2);
  if ((bindingType === "latest_payload" || bindingType === "latest_envelope") && sourceId?.trim()) {
    return { type: bindingType, source: sourceId };
  }
  return undefined;
}

function buildOutputSourceBindingOptions(
  graph: GraphDefinition,
  node: GraphNode,
): Array<{
  value: string;
  label: string;
}> {
  const incomingSourceIds = uniqueStrings(graph.edges.filter((edge) => edge.target_id === node.id).map((edge) => edge.source_id));
  const options = [
    { value: "auto", label: "Automatic upstream payload" },
    { value: "input_payload", label: "Run input payload" },
  ];
  incomingSourceIds.forEach((sourceId) => {
    const sourceNode = graph.nodes.find((candidate) => candidate.id === sourceId);
    const sourceLabel = sourceNode ? getNodeInstanceLabel(graph, sourceNode) : sourceId;
    options.push(
      { value: `latest_payload:${sourceId}`, label: `Latest payload from ${sourceLabel}` },
      { value: `latest_envelope:${sourceId}`, label: `Latest envelope from ${sourceLabel}` },
    );
  });
  return options;
}

export function GraphInspector({
  graph,
  catalog,
  availableProjectFiles = [],
  runState,
  selectedNodeId,
  selectedEdgeId,
  onGraphChange,
  onOpenProviderDetails,
  onSaveNode,
}: GraphInspectorProps) {
  useRenderDiagnostics(
    "GraphInspector",
    true,
    {
      hasGraph: Boolean(graph),
      selectedNodeId: selectedNodeId ?? "none",
      selectedEdgeId: selectedEdgeId ?? "none",
      nodeCount: graph?.nodes.length ?? 0,
      edgeCount: graph?.edges.length ?? 0,
    },
    12,
  );
  const [spreadsheetPreview, setSpreadsheetPreview] = useState<SpreadsheetPreviewResult | null>(null);
  const [spreadsheetPreviewError, setSpreadsheetPreviewError] = useState<string | null>(null);
  const [isSpreadsheetPreviewLoading, setIsSpreadsheetPreviewLoading] = useState(false);
  const [contextBuilderPreviewFormatted, setContextBuilderPreviewFormatted] = useState(true);
  const [isStructuredPayloadBuilderLearnMoreOpen, setIsStructuredPayloadBuilderLearnMoreOpen] = useState(false);
  const [structuredPayloadTemplateDraftEntries, setStructuredPayloadTemplateDraftEntries] = useState<StructuredPayloadTemplateEntry[]>([]);

  if (!graph) {
    return (
      <section className="panel inspector-panel">
        <div className="panel-header">
          <h2>Inspector</h2>
          <p>Select or create an agent to inspect its settings.</p>
        </div>
      </section>
    );
  }

  const selectedNode = selectedNodeId ? graph.nodes.find((node) => node.id === selectedNodeId) ?? null : null;
  const selectedEdge = selectedEdgeId ? graph.edges.find((edge) => edge.id === selectedEdgeId) ?? null : null;
  const spreadsheetProjectFiles = availableProjectFiles.filter(isSpreadsheetProjectFile);
  const spreadsheetPreviewKey =
    selectedNode?.provider_id === SPREADSHEET_ROW_PROVIDER_ID ||
    selectedNode?.provider_id === SPREADSHEET_MATRIX_DECISION_PROVIDER_ID
      ? JSON.stringify({
          id: selectedNode.id,
          file_format: selectedNode.config.file_format ?? "auto",
          file_path: selectedNode.config.file_path ?? "",
          sheet_name: selectedNode.config.sheet_name ?? "",
          header_row_index: selectedNode.config.header_row_index ?? 1,
          start_row_index: selectedNode.config.start_row_index ?? 2,
          empty_row_policy: selectedNode.config.empty_row_policy ?? "skip",
        })
      : "none";
  useEffect(() => {
    setSpreadsheetPreview(null);
    setSpreadsheetPreviewError(null);
    setIsSpreadsheetPreviewLoading(false);
  }, [spreadsheetPreviewKey]);
  useEffect(() => {
    if (selectedNode?.provider_id !== STRUCTURED_PAYLOAD_BUILDER_PROVIDER_ID) {
      setIsStructuredPayloadBuilderLearnMoreOpen(false);
    }
  }, [selectedNode]);
  useEffect(() => {
    if (selectedNode?.provider_id === STRUCTURED_PAYLOAD_BUILDER_PROVIDER_ID) {
      setStructuredPayloadTemplateDraftEntries(parseStructuredPayloadTemplateEntries(selectedNode.config.template_json));
      return;
    }
    setStructuredPayloadTemplateDraftEntries([]);
  }, [selectedNode?.id, selectedNode?.provider_id]);
  const formatNodeLabel = (node: GraphNode) => getNodeInstanceLabel(graph, node);
  const executorFollowUpEnabled =
    selectedNode?.kind === "mcp_tool_executor" && Boolean(selectedNode.config.enable_follow_up_decision);
  const executorRetriesEnabled =
    selectedNode?.kind === "mcp_tool_executor" && Boolean(selectedNode.config.allow_retries ?? true);
  const isSpreadsheetMatrixDecisionNode =
    selectedNode?.kind === "model" && selectedNode.provider_id === SPREADSHEET_MATRIX_DECISION_PROVIDER_ID;
  const isPromptDrivenNode =
    (selectedNode?.kind === "model" && !isSpreadsheetMatrixDecisionNode) || executorFollowUpEnabled;
  const selectedModelResponseMode =
    selectedNode?.kind === "model"
      ? inferModelResponseMode(graph, selectedNode)
      : executorFollowUpEnabled
        ? String(selectedNode.config.response_mode ?? "auto")
        : null;
  const responseSchemaDetails =
    selectedNode && isPromptDrivenNode ? resolveResponseSchemaDetails(selectedNode.config as Record<string, unknown>) : null;
  const selectedSpreadsheetProjectFile =
    selectedNode?.provider_id === SPREADSHEET_ROW_PROVIDER_ID || selectedNode?.provider_id === SPREADSHEET_MATRIX_DECISION_PROVIDER_ID
      ? spreadsheetProjectFiles.find((file) => file.file_id === String(selectedNode.config.project_file_id ?? "").trim()) ??
        spreadsheetProjectFiles.find((file) => file.storage_path === String(selectedNode.config.file_path ?? "").trim()) ??
        spreadsheetProjectFiles.find((file) => file.name === String(selectedNode.config.project_file_name ?? "").trim()) ??
        null
      : null;
  const hasManualSpreadsheetPath =
    !selectedSpreadsheetProjectFile && selectedNode ? String(selectedNode.config.file_path ?? "").trim().length > 0 : false;
  const spreadsheetProjectFileSelectValue =
    selectedSpreadsheetProjectFile?.file_id ?? (hasManualSpreadsheetPath ? "__manual__" : "");

  const applySpreadsheetProjectFile = (fileId: string): void => {
    if (!selectedNode) {
      return;
    }
    const selectedFile = spreadsheetProjectFiles.find((file) => file.file_id === fileId) ?? null;
    if (!selectedFile) {
      return;
    }
    onGraphChange(
      updateNode(graph, selectedNode.id, (node) => ({
        ...node,
        config: {
          ...node.config,
          project_file_id: selectedFile.file_id,
          project_file_name: selectedFile.name,
          file_path: selectedFile.storage_path,
        },
      })),
    );
  };

  if (selectedNode) {
    if (isWireJunctionNode(selectedNode)) {
      return (
        <section className="panel inspector-panel">
          <div className="panel-header">
            <h2>Wire Point</h2>
            <p>This floating junction anchors a routed wire segment and can be dragged to reshape the path.</p>
          </div>
          <div className="inspector-body">
            <label>
              Node ID
              <input value={selectedNode.id} readOnly />
            </label>
            <label>
              Position X
              <input
                type="number"
                value={selectedNode.position.x}
                onChange={(event) =>
                  onGraphChange(
                    updateNode(graph, selectedNode.id, (node) => ({
                      ...node,
                      position: { ...node.position, x: Number(event.target.value) },
                    })),
                  )
                }
              />
            </label>
            <label>
              Position Y
              <input
                type="number"
                value={selectedNode.position.y}
                onChange={(event) =>
                  onGraphChange(
                    updateNode(graph, selectedNode.id, (node) => ({
                      ...node,
                      position: { ...node.position, y: Number(event.target.value) },
                    })),
                  )
                }
              />
            </label>
            <div className="inspector-meta">
              <span>Kind: floating junction</span>
              <span>Purpose: wire routing</span>
            </div>
          </div>
        </section>
      );
    }

    const contract = catalog?.contracts[selectedNode.category];
    const allowedTools = Array.isArray(selectedNode.config.allowed_tool_names)
      ? (selectedNode.config.allowed_tool_names as string[])
      : [];
    const availableModelProviders = modelProviderDefinitions(catalog);
    const selectedProviderName = String(selectedNode.config.provider_name ?? selectedNode.model_provider_name ?? "mock");
    const selectedProvider = findProviderDefinition(catalog, selectedProviderName);
    const providerConfigFields = selectedProvider?.config_fields ?? [];
    const allModelOptions = (() => {
      const options = new Map<string, { value: string; label: string }>();
      availableModelProviders.forEach((provider) => {
        const modelField = (provider.config_fields ?? []).find((field) => field.key === "model");
        (modelField?.options ?? []).forEach((option) => {
          const value = String(option.value ?? "").trim();
          if (!value || options.has(value)) {
            return;
          }
          const label = String(option.label ?? value).trim() || value;
          options.set(value, { value, label });
        });
      });
      return [...options.values()];
    })();
    const displayedProviderConfigFields = providerConfigFields.map((field) =>
      selectedNode.kind === "model" && field.key === "model" && allModelOptions.length > 0
        ? {
            ...field,
            options: allModelOptions,
          }
        : field,
    );
    const providerStatus = catalog?.provider_statuses?.[selectedProviderName];
    const catalogTools = catalog?.tools ?? [];
    const mcpCatalogTools = catalogTools.filter((tool) => tool.source_type === "mcp");
    const standardCatalogTools = catalogTools.filter((tool) => tool.source_type !== "mcp");
    const followUpSelectableTools = selectedNode.kind === "mcp_tool_executor" ? mcpCatalogTools : standardCatalogTools;
    const mcpToolByName = new Map<string, ToolDefinition>();
    for (const tool of mcpCatalogTools) {
      for (const identifier of [toolCanonicalName(tool), tool.name, ...(tool.aliases ?? [])]) {
        const normalizedIdentifier = String(identifier).trim();
        if (normalizedIdentifier && !mcpToolByName.has(normalizedIdentifier)) {
          mcpToolByName.set(normalizedIdentifier, tool);
        }
      }
    }
    const selectedMcpToolNames = Array.isArray(selectedNode.config.tool_names)
      ? (selectedNode.config.tool_names as string[])
      : [];
    const mcpContextProvidersForModel = selectedNode.kind === "model" ? getModelMcpContextNodes(graph, selectedNode) : [];
    const modelCallableMcpTools =
      isPromptDrivenNode
        ? mcpContextProvidersForModel.flatMap((node) => {
            const nodeToolNames = Array.isArray(node.config.tool_names)
              ? node.config.tool_names.map((toolName) => String(toolName)).filter((toolName) => toolName.trim().length > 0)
              : [];
            if (node.config.expose_mcp_tools === false) {
              return [];
            }
            return nodeToolNames.map((toolName) => {
              const tool = mcpToolByName.get(toolName);
              const status = tool ? toolStatusLabel(tool) : "unknown";
              return `${tool ? toolLabel(tool) : toolName} (${status}) via ${formatNodeLabel(node)}`;
            });
          })
        : [];
    const modelPromptContextProviders =
      isPromptDrivenNode
        ? uniqueStrings(
            mcpContextProvidersForModel
              .filter((node) => Boolean(node.config.include_mcp_tool_context))
              .map((node) => formatNodeLabel(node)),
          )
        : [];
    const modelPromptGuidanceProviders =
      isPromptDrivenNode
        ? uniqueStrings(
            mcpContextProvidersForModel
              .filter(
                (node) =>
                  Boolean(node.config.include_mcp_tool_context) && String(node.config.usage_hint ?? "").trim().length > 0,
              )
              .map((node) => formatNodeLabel(node)),
          )
        : [];
    const modelTargetedMcpNodeIds =
      selectedNode.kind === "model" && Array.isArray(selectedNode.config.tool_target_node_ids)
        ? uniqueStrings(selectedNode.config.tool_target_node_ids.map((nodeId) => String(nodeId)))
        : [];
    const modelPromptBlockNodes = selectedNode.kind === "model" ? getModelPromptBlockNodes(graph, selectedNode) : [];
    const contextBuilderPromptVariables =
      selectedNode.kind === "model" ? getModelContextBuilderPromptVariables(graph, selectedNode) : [];
    const modelMetadataBindingKeys =
      selectedNode.kind === "model" && isRecord(selectedNode.config.metadata_bindings)
        ? uniqueStrings(Object.keys(selectedNode.config.metadata_bindings).map((key) => String(key)))
        : [];
    const graphEnvVars = getGraphEnvVars(graph);
    const modelSystemPromptTemplate = selectedNode.kind === "model" ? String(selectedNode.config.system_prompt ?? "") : "";
    const modelSystemPromptTokens = selectedNode.kind === "model" ? extractTemplateTokens(modelSystemPromptTemplate) : [];
    const runtimeNormalizerFieldNames =
      selectedNode.provider_id === RUNTIME_NORMALIZER_PROVIDER_ID
        ? (() => {
            const configured = parseConfigStringList(selectedNode.config.field_names);
            if (configured.length > 0) {
              return configured;
            }
            const legacy = parseConfigStringList(selectedNode.config.field_name);
            return legacy.length > 0 ? legacy : ["url"];
          })()
        : [];
    const modelDirectRegistryToolSummaries =
      selectedNode.kind === "model"
        ? allowedTools
            .map((toolName) => {
              const tool = standardCatalogTools.find((candidate) => toolMatchesReference(candidate, toolName));
              if (!tool) {
                return {
                  label: toolName,
                  canonicalName: toolName,
                  status: "unknown",
                };
              }
              return {
                label: toolLabel(tool),
                canonicalName: toolCanonicalName(tool),
                status: toolStatusLabel(tool),
              };
            })
            .filter((tool) => tool.canonicalName.trim().length > 0)
        : [];
    const findPromptOverrideNodeForTool = (toolName: string): GraphNode | null => {
      for (const candidate of graph.nodes) {
        if (candidate.kind !== "tool" && candidate.kind !== "mcp_context_provider") {
          continue;
        }
        const configuredToolNames = Array.isArray(candidate.config.tool_names)
          ? candidate.config.tool_names.map((value) => String(value))
          : [];
        const configuredToolName = typeof candidate.config.tool_name === "string" ? [candidate.config.tool_name] : [];
        const candidateToolNames = [...configuredToolNames, ...configuredToolName];
        if (candidateToolNames.some((configuredName) => toolMatchesReference({ name: toolName, description: "", input_schema: {} }, configuredName) || configuredName.trim() === toolName.trim())) {
          return candidate;
        }
        const matchingCatalogTool = catalogTools.find((tool) => toolMatchesReference(tool, toolName));
        if (matchingCatalogTool && candidateToolNames.some((configuredName) => toolMatchesReference(matchingCatalogTool, configuredName))) {
          return candidate;
        }
      }
      return null;
    };
    const modelPromptToolSummaries =
      selectedNode.kind === "model"
        ? mcpContextProvidersForModel.flatMap((node) => {
            const nodeToolNames = Array.isArray(node.config.tool_names)
              ? node.config.tool_names.map((toolName) => String(toolName)).filter((toolName) => toolName.trim().length > 0)
              : [];
            const sourceNodeLabel = formatNodeLabel(node);
            const usageHint = String(node.config.usage_hint ?? "").trim();
            const injectsPromptContext = Boolean(node.config.include_mcp_tool_context);
            const isCallableSource = node.config.expose_mcp_tools !== false;
            return nodeToolNames.map((toolName) => {
              const tool = mcpToolByName.get(toolName) ?? null;
              const canonicalName = tool ? toolCanonicalName(tool) : toolName;
              const overrideNode = findPromptOverrideNodeForTool(canonicalName) ?? node;
              const previewNode: GraphNode = {
                ...overrideNode,
                tool_name: canonicalName,
                config: {
                  ...overrideNode.config,
                  tool_name: canonicalName,
                  tool_names: [canonicalName],
                },
              };
              const resolvedDetails = resolveToolNodeDetails(previewNode, catalog, graph);
              const status = tool ? toolStatusLabel(tool) : "unknown";
              return {
                sourceNodeId: node.id,
                sourceNodeLabel,
                overrideNodeLabel: formatNodeLabel(overrideNode),
                toolName: canonicalName,
                displayName: tool ? toolLabel(tool) : canonicalName,
                status,
                isCallable: isCallableSource && status === "ready",
                injectsPromptContext,
                usageHint,
                renderedPromptText: resolvedDetails.renderedPromptText,
                templateText: resolvedDetails.templateText,
                descriptionText: resolvedDetails.agentDescriptionText,
              };
            });
          })
        : [];
    const modelPromptContextToolSummaries =
      selectedNode.kind === "model"
        ? modelPromptToolSummaries
            .filter((tool) => tool.injectsPromptContext)
            .map((tool, index) => ({
              ...tool,
              placeholderToken: mcpToolPlaceholderToken(index),
            }))
        : [];
    const modelGeneratedMcpPlaceholderTemplate =
      selectedNode.kind === "model" ? buildMcpToolPlaceholderTemplate(modelPromptContextToolSummaries) : "";
    const modelPromptGuidanceBlocks =
      selectedNode.kind === "model"
        ? mcpContextProvidersForModel
            .filter(
              (node) =>
                Boolean(node.config.include_mcp_tool_context) &&
                String(node.config.usage_hint ?? "").trim().length > 0 &&
                Array.isArray(node.config.tool_names) &&
                node.config.tool_names.length > 0,
            )
            .map((node) => {
              const toolNames = (node.config.tool_names as unknown[])
                .map((toolName) => String(toolName))
                .filter((toolName) => toolName.trim().length > 0)
                .map((toolName) => mcpToolByName.get(toolName))
                .filter((tool): tool is ToolDefinition => tool !== undefined)
                .map((tool) => toolLabel(tool));
              const dedupedToolNames = uniqueStrings(toolNames);
              const usageHint = String(node.config.usage_hint ?? "").trim();
              if (!usageHint || dedupedToolNames.length === 0) {
                return "";
              }
              return [`Tools: ${dedupedToolNames.join(", ")}`, "Guidance:", usageHint].join("\n");
            })
            .filter((block) => block.trim().length > 0)
        : [];
    const modelMcpToolContextPrompt =
      selectedNode.kind === "model"
        ? modelPromptContextToolSummaries.map((tool) => tool.renderedPromptText.trim()).filter((text) => text.length > 0).join("\n\n")
        : "";
    const modelCallableMcpToolNames =
      selectedNode.kind === "model"
        ? uniqueStrings(modelPromptToolSummaries.filter((tool) => tool.isCallable).map((tool) => tool.toolName)).sort()
        : [];
    const modelPromptVariables =
      selectedNode.kind === "model"
        ? uniqueStrings([
            ...Object.keys(graphEnvVars),
            "documents",
            "input_payload",
            "run_id",
            "graph_id",
            "current_node_id",
            "available_tools",
            "mcp_available_tool_names",
            "mcp_tool_context",
            "mcp_tool_context_prompt",
            "mcp_tool_context_block",
            "mcp_tool_guidance",
            "mcp_tool_guidance_block",
            "mode",
            "preferred_tool_name",
            "response_mode",
            "prompt_blocks",
            ...contextBuilderPromptVariables.map((variable) => variable.token),
            ...modelPromptContextToolSummaries.map((tool) => tool.placeholderToken),
            ...modelMetadataBindingKeys,
          ]).sort()
        : [];
    const modelPreviewVariableValues: Record<string, string> =
      selectedNode.kind === "model"
        ? {
            documents: stringifyPreviewValue(runState?.documents ?? []),
            input_payload: stringifyPreviewValue(runState?.input_payload ?? ""),
            run_id: runState?.run_id ?? "",
            graph_id: graph.graph_id,
            current_node_id: selectedNode.id,
            available_tools: JSON.stringify(
              [
                ...modelDirectRegistryToolSummaries.map((tool) => ({
                  name: tool.canonicalName,
                  description: tool.label,
                  status: tool.status,
                })),
                ...modelPromptToolSummaries
                  .filter((tool) => tool.isCallable)
                  .map((tool) => ({
                    name: tool.toolName,
                    description: tool.descriptionText,
                    status: tool.status,
                  })),
              ],
              null,
              2,
            ),
            mcp_available_tool_names: JSON.stringify(modelCallableMcpToolNames, null, 2),
            mcp_tool_context: JSON.stringify(
              {
                tool_names: modelPromptContextToolSummaries.map((tool) => tool.toolName),
                prompt_blocks: modelPromptContextToolSummaries.map((tool) => tool.renderedPromptText),
                usage_hints_text: modelPromptGuidanceBlocks.join("\n\n"),
              },
              null,
              2,
            ),
            mcp_tool_context_prompt: modelMcpToolContextPrompt,
            mcp_tool_guidance: modelPromptGuidanceBlocks.join("\n\n"),
            mcp_tool_context_block: modelMcpToolContextPrompt.trim().length > 0
              ? `MCP Tool Context\n${modelMcpToolContextPrompt}`
              : "",
            mcp_tool_guidance_block: buildMcpToolGuidanceBlock(
              modelCallableMcpToolNames,
              modelPromptGuidanceBlocks.join("\n\n"),
            ),
            mode: String(selectedNode.config.mode ?? selectedNode.prompt_name ?? ""),
            preferred_tool_name: String(selectedNode.config.preferred_tool_name ?? ""),
            response_mode: selectedModelResponseMode ?? "message",
            prompt_blocks: JSON.stringify(
              modelPromptBlockNodes.map((node) => ({
                role: String(node.config.role ?? "user"),
                name: String(node.config.name ?? ""),
                content: renderPromptBlockPreview(node, graph, runState),
              })),
              null,
              2,
            ),
            ...Object.fromEntries(modelPromptContextToolSummaries.map((tool) => [tool.placeholderToken, tool.renderedPromptText])),
            ...Object.fromEntries(
              contextBuilderPromptVariables.map((variable) => [
                variable.token,
                `[Context Builder section: ${variable.header}]`,
              ]),
            ),
            ...Object.fromEntries(modelMetadataBindingKeys.map((key) => [key, `[bound at runtime: ${key}]`])),
          }
        : {};
    const modelSystemPromptTemplatePreview =
      selectedNode.kind === "model"
        ? resolveGraphEnvReferences(modelSystemPromptTemplate, graph, modelPreviewVariableValues)
        : "";
    const modelPromptOnlyToolContract =
      selectedNode.kind === "model"
        ? buildPromptOnlyMcpToolDecisionContract(modelPromptContextToolSummaries.length > 0, modelCallableMcpToolNames)
        : "";
    const modelMcpToolPlaceholderTokens =
      selectedNode.kind === "model" ? modelPromptContextToolSummaries.map((tool) => tool.placeholderToken) : [];
    const modelHasInlineMcpGuidanceBlock =
      selectedNode.kind === "model" && modelSystemPromptTemplate.includes("{mcp_tool_guidance_block}");
    const modelHasInlineMcpContextCoverage =
      selectedNode.kind === "model" && (
        modelSystemPromptTemplate.includes("{mcp_tool_context_block}")
        || modelSystemPromptTemplate.includes("{mcp_tool_context_prompt}")
        || (
          modelMcpToolPlaceholderTokens.length > 0
          && modelMcpToolPlaceholderTokens.every((token) => modelSystemPromptTemplate.includes(`{${token}}`))
        )
      );
    const modelOptionalPromptVariables =
      selectedNode.kind === "model"
        ? modelPromptVariables.filter(
            (token) => !["mcp_tool_guidance_block", "mcp_tool_context_block", "mcp_tool_context_prompt", ...modelMcpToolPlaceholderTokens].includes(token),
          )
        : [];
    const modelMcpGuidanceBlock =
      selectedNode.kind === "model"
        ? String(modelPreviewVariableValues.mcp_tool_guidance_block ?? "").trim()
        : "";
    const modelMcpContextBlock =
      selectedNode.kind === "model"
        ? String(modelPreviewVariableValues.mcp_tool_context_block ?? "").trim()
        : "";
    const modelPromptAssemblySections =
      selectedNode.kind === "model"
        ? (() => {
            const sections: string[] = [];
            const hasInlineMcpGuidanceBlock = modelSystemPromptTemplate.includes("{mcp_tool_guidance_block}");
            const hasInlineMcpContextCoverage =
              modelSystemPromptTemplate.includes("{mcp_tool_context_block}")
              || modelSystemPromptTemplate.includes("{mcp_tool_context_prompt}")
              || (
                modelPromptContextToolSummaries.length > 0
                && modelPromptContextToolSummaries.every((tool) => modelSystemPromptTemplate.includes(`{${tool.placeholderToken}}`))
              );
            if (modelMcpGuidanceBlock.length > 0 && !hasInlineMcpGuidanceBlock) {
              sections.push(modelMcpGuidanceBlock);
            }
            if (modelMcpContextBlock.length > 0 && !hasInlineMcpContextCoverage) {
              sections.push(modelMcpContextBlock);
            }
            if (modelPromptOnlyToolContract.trim().length > 0) {
              sections.push(modelPromptOnlyToolContract);
            }
            return sections;
          })()
        : [];
    const modelFinalSystemPromptPreview =
      selectedNode.kind === "model"
        ? [modelSystemPromptTemplatePreview.trim(), ...modelPromptAssemblySections.map((section) => section.trim()).filter((section) => section.length > 0)]
            .filter((section) => section.length > 0)
            .join("\n\n")
        : "";
    const mcpToolExposureEnabled = selectedNode.kind === "mcp_context_provider" ? selectedNode.config.expose_mcp_tools !== false : false;
    const executorBindingSummary = selectedNode.kind === "mcp_tool_executor" ? describeMcpExecutorBinding(selectedNode.config.input_binding) : "";
    const executorFollowUpResponseMode =
      selectedNode.kind === "mcp_tool_executor" ? String(selectedNode.config.response_mode ?? "auto") : "auto";
    const isDiscordStartNode = selectedNode.kind === "input" && selectedNode.provider_id === "start.discord_message";
    const isDiscordEndNode = selectedNode.kind === "output" && selectedNode.provider_id === "end.discord_message";
    const isOutlookDraftEndNode = selectedNode.kind === "output" && selectedNode.provider_id === "end.outlook_draft";
    const microsoftAuthStatus = catalog?.microsoft_auth ?? null;
    const outputSourceBindingValue = selectedNode.kind === "output" ? encodeOutputSourceBinding(selectedNode.config.source_binding) : "auto";
    const outputSourceBindingOptions = selectedNode.kind === "output" ? buildOutputSourceBindingOptions(graph, selectedNode) : [];
    const isManualStartNode =
      selectedNode.kind === "input" &&
      (selectedNode.provider_id === "start.manual_run" || selectedNode.provider_id === "core.input");
    const isContextBuilderNode = selectedNode.kind === "data" && selectedNode.provider_id === CONTEXT_BUILDER_PROVIDER_ID;
    const isPromptBlockDataNode = selectedNode.kind === "data" && selectedNode.provider_id === PROMPT_BLOCK_PROVIDER_ID;
    const isWriteTextFileNode = selectedNode.kind === "data" && selectedNode.provider_id === WRITE_TEXT_FILE_PROVIDER_ID;
    const isStructuredPayloadBuilderNode =
      selectedNode.kind === "data" && selectedNode.provider_id === STRUCTURED_PAYLOAD_BUILDER_PROVIDER_ID;
    const isApolloEmailLookupNode =
      selectedNode.kind === "data" && selectedNode.provider_id === APOLLO_EMAIL_LOOKUP_PROVIDER_ID;
    const isLinkedInProfileFetchNode =
      selectedNode.kind === "data" && selectedNode.provider_id === LINKEDIN_PROFILE_FETCH_PROVIDER_ID;
    const isRuntimeNormalizerNode =
      selectedNode.kind === "data" && selectedNode.provider_id === RUNTIME_NORMALIZER_PROVIDER_ID;
    const isControlFlowUnitNode = isControlFlowNode(selectedNode);
    const isSpreadsheetRowNode = isControlFlowUnitNode && selectedNode.provider_id === SPREADSHEET_ROW_PROVIDER_ID;
    const isSupabaseTableRowsNode = isControlFlowUnitNode && selectedNode.provider_id === SUPABASE_TABLE_ROWS_PROVIDER_ID;
    const isSpreadsheetMatrixNode =
      selectedNode.kind === "model" && selectedNode.provider_id === SPREADSHEET_MATRIX_DECISION_PROVIDER_ID;
    const displayedUserMessageTemplate =
      isSpreadsheetMatrixNode &&
      (!String(selectedNode.config.user_message_template ?? "").trim() ||
        String(selectedNode.config.user_message_template ?? "").trim() === "{input_payload}")
        ? SPREADSHEET_MATRIX_RECOMMENDED_USER_MESSAGE_TEMPLATE
        : String(selectedNode.config.user_message_template ?? "{input_payload}");
    const isLogicConditionsNode = isControlFlowUnitNode && selectedNode.provider_id === LOGIC_CONDITIONS_PROVIDER_ID;
    const isParallelSplitterNode = isControlFlowUnitNode && selectedNode.provider_id === PARALLEL_SPLITTER_PROVIDER_ID;
    const parallelSplitterOutgoingEdges = isParallelSplitterNode ? graph.edges.filter((edge) => edge.source_id === selectedNode.id) : [];
    const parallelSplitterHandles = isParallelSplitterNode ? getParallelSplitterOutputHandles(graph, selectedNode) : [];
    const parallelSplitterConnectionCount = parallelSplitterOutgoingEdges.length;
    const parallelSplitterConfiguredHandleCount = isParallelSplitterNode
      ? Number.parseInt(String(selectedNode.config[PARALLEL_SPLITTER_HANDLE_COUNT_CONFIG_KEY] ?? "1"), 10) || 1
      : 0;
    const spreadsheetNode = isSpreadsheetRowNode || isSpreadsheetMatrixNode ? selectedNode : null;
    const iteratorNode = isSpreadsheetRowNode || isSupabaseTableRowsNode ? selectedNode : null;
    const logicConditionConfig = isLogicConditionsNode ? normalizeLogicConditionConfig(selectedNode.config).normalized : null;
    const logicIncomingContractLabel = isLogicConditionsNode ? incomingEdgeContractLabel(graph, selectedNode) : "";
    const spreadsheetIteratorState =
      iteratorNode && runState?.iterator_states
        ? (runState.iterator_states[selectedNode.id] as Record<string, unknown> | undefined)
        : undefined;
    const spreadsheetLoopRegion = iteratorNode ? runState?.loop_regions?.[selectedNode.id] : undefined;
    const spreadsheetLoopMemberLabels = Array.isArray(spreadsheetLoopRegion?.member_node_ids)
      ? spreadsheetLoopRegion.member_node_ids
          .map((nodeId) => graph.nodes.find((candidate) => candidate.id === nodeId)?.label ?? nodeId)
          .filter((label, index, values) => label.length > 0 && values.indexOf(label) === index)
      : [];
    const spreadsheetResolvedFilePath =
      spreadsheetNode ? String(resolveGraphEnvReferences(String(spreadsheetNode.config.file_path ?? ""), graph) ?? "") : "";
    const spreadsheetPreviewPrimaryHeader = spreadsheetPreview?.headers[0] ?? "";
    const spreadsheetPreviewSampleRowLabels = spreadsheetPreview
      ? spreadsheetPreviewPrimaryHeader.length > 0
        ? spreadsheetPreview.sample_rows
            .map((row) => String(row.row_data[spreadsheetPreviewPrimaryHeader] ?? "").trim())
            .filter((label) => label.length > 0)
        : []
      : [];
    async function handleSpreadsheetPreview(): Promise<void> {
      if (!spreadsheetNode) {
        return;
      }
      setIsSpreadsheetPreviewLoading(true);
      setSpreadsheetPreviewError(null);
      try {
        const preview = await previewSpreadsheetRows({
          file_path: spreadsheetResolvedFilePath,
          file_format: String(spreadsheetNode.config.file_format ?? "auto"),
          sheet_name: String(spreadsheetNode.config.sheet_name ?? "") || null,
          header_row_index: Number(spreadsheetNode.config.header_row_index ?? 1) || 1,
          start_row_index: Number(spreadsheetNode.config.start_row_index ?? 2) || 2,
          empty_row_policy: String(spreadsheetNode.config.empty_row_policy ?? "skip"),
        });
        setSpreadsheetPreview(preview);
      } catch (error) {
        setSpreadsheetPreview(null);
        setSpreadsheetPreviewError(error instanceof Error ? error.message : "Failed to preview spreadsheet rows.");
      } finally {
        setIsSpreadsheetPreviewLoading(false);
      }
    }
    function updateResponseSchemaText(nextText: string): void {
      if (!isPromptDrivenNode || !graph) {
        return;
      }
      const { parsedSchema } = parseResponseSchemaText(nextText);
      onGraphChange(
        updateNode(graph, selectedNode.id, (node) => {
          const nextConfig = { ...node.config } as Record<string, unknown>;
          if (nextText.length > 0) {
            nextConfig[RESPONSE_SCHEMA_TEXT_CONFIG_KEY] = nextText;
          } else {
            delete nextConfig[RESPONSE_SCHEMA_TEXT_CONFIG_KEY];
          }
          if (parsedSchema) {
            nextConfig.response_schema = parsedSchema;
          } else {
            delete nextConfig.response_schema;
          }
          return { ...node, config: nextConfig };
        }),
      );
    }
    const contextBuilderBindings = isContextBuilderNode ? getContextBuilderBindings(selectedNode, graph) : [];
    const generatedContextBuilderTemplate = buildContextBuilderTemplate(contextBuilderBindings);
    const rawContextBuilderTemplate = isContextBuilderNode ? String(selectedNode.config.template ?? "") : "";
    const contextBuilderTemplate = isContextBuilderNode
      ? rawContextBuilderTemplate.trim().length > 0
        ? rawContextBuilderTemplate
        : generatedContextBuilderTemplate
      : "";
    const contextBuilderTemplateTokens = isContextBuilderNode ? extractTemplateTokens(contextBuilderTemplate) : [];
    const contextBuilderDuplicatePlaceholders = isContextBuilderNode
      ? contextBuilderBindings
          .map((binding) => binding.placeholder)
          .filter((placeholder, index, placeholders) => placeholders.indexOf(placeholder) !== index)
      : [];
    const contextBuilderInvalidPlaceholders = isContextBuilderNode
      ? contextBuilderBindings
          .map((binding) => binding.placeholder)
          .filter((placeholder) => !CONTEXT_BUILDER_IDENTIFIER_PATTERN.test(placeholder))
      : [];
    const contextBuilderAvailableVariables = new Set(
      isContextBuilderNode
        ? [
            ...contextBuilderBindings.map((binding) => binding.placeholder),
            ...Object.keys(graph.env_vars ?? {}),
            ...CONTEXT_BUILDER_BASE_VARIABLES,
          ]
        : [],
    );
    const contextBuilderUnmatchedTokens = isContextBuilderNode
      ? contextBuilderTemplateTokens.filter((token) => !contextBuilderAvailableVariables.has(token))
      : [];
    const contextBuilderPreviewVariables = isContextBuilderNode
      ? Object.fromEntries(
          contextBuilderBindings.map((binding) => [
            binding.placeholder,
            getContextBuilderSourcePreviewFromGraph(graph, runState, binding.sourceNodeId) ?? "",
          ]),
        )
      : {};
    const contextBuilderUsesCustomTemplate = isContextBuilderNode ? rawContextBuilderTemplate.trim().length > 0 : false;
    const contextBuilderRenderedPreview = isContextBuilderNode
      ? contextBuilderUsesCustomTemplate
        ? renderContextBuilderPreview(contextBuilderTemplate, {
            ...Object.fromEntries(Object.entries(graph.env_vars ?? {}).map(([key, value]) => [key, String(value)])),
            current_node_id: selectedNode.id,
            documents: stringifyPreviewValue(runState?.documents ?? []),
            graph_id: graph.graph_id,
            input_payload: "",
            run_id: runState?.run_id ?? "",
            ...contextBuilderPreviewVariables,
          })
        : buildContextBuilderStructuredPreview(contextBuilderBindings, contextBuilderPreviewVariables)
      : "";
    const contextBuilderHasPreviewData = isContextBuilderNode
      ? contextBuilderBindings.some((binding) => getContextBuilderSourcePreviewFromGraph(graph, runState, binding.sourceNodeId) !== null)
      : false;
    const promptBlockAvailableVariables = isPromptBlockDataNode ? listPromptBlockAvailableVariables(graph) : [];
    const promptBlockRenderedPreview = isPromptBlockDataNode ? renderPromptBlockPreview(selectedNode, graph, runState) : "";
    const structuredPayloadTemplateEntries = isStructuredPayloadBuilderNode
      ? structuredPayloadTemplateDraftEntries
      : [];
    const updateStructuredPayloadTemplateEntries = (entries: StructuredPayloadTemplateEntry[]) => {
      setStructuredPayloadTemplateDraftEntries(entries);
      onGraphChange(
        updateNode(graph, selectedNode.id, (node) => ({
          ...node,
          config: {
            ...node.config,
            mode: "structured_payload_builder",
            template_json: serializeStructuredPayloadTemplateEntries(entries),
          },
        })),
      );
    };
    const updateContextBuilderBindings = (bindings: ContextBuilderBindingRow[]) =>
      onGraphChange(
        updateNode(graph, selectedNode.id, (node) => ({
          ...node,
          config: {
            ...node.config,
            input_bindings: bindings.map((binding) => ({
              source_node_id: binding.sourceNodeId,
              header: binding.rawHeader,
              placeholder: binding.placeholder,
              binding: binding.binding,
            })),
          },
        })),
      );
    const updateContextBuilderTemplate = (template: string) =>
      onGraphChange(
        updateNode(graph, selectedNode.id, (node) => ({
          ...node,
          config: {
            ...node.config,
            template,
          },
        })),
      );

    return (
      <>
        <section className="panel inspector-panel">
          <div className="panel-header">
            <h2>Node Inspector</h2>
            <p>Edit the selected node and its runtime contract.</p>
          </div>
          <div className="inspector-body">
          <label>
            Node ID
            <input value={selectedNode.id} readOnly />
          </label>
          <label>
            Label
            <input
              value={selectedNode.label}
              onChange={(event) =>
                onGraphChange(updateNode(graph, selectedNode.id, (node) => ({ ...node, label: event.target.value })))
              }
            />
          </label>
          <label>
            Description
            <textarea
              rows={3}
              value={selectedNode.description ?? ""}
              onChange={(event) =>
                onGraphChange(
                  updateNode(graph, selectedNode.id, (node) => ({ ...node, description: event.target.value })),
                )
              }
            />
          </label>
          <label>
            Position X
            <input
              type="number"
              value={selectedNode.position.x}
              onChange={(event) =>
                onGraphChange(
                  updateNode(graph, selectedNode.id, (node) => ({
                    ...node,
                    position: { ...node.position, x: Number(event.target.value) },
                  })),
                )
              }
            />
          </label>
          <label>
            Position Y
            <input
              type="number"
              value={selectedNode.position.y}
              onChange={(event) =>
                onGraphChange(
                  updateNode(graph, selectedNode.id, (node) => ({
                    ...node,
                    position: { ...node.position, y: Number(event.target.value) },
                  })),
                )
              }
            />
          </label>
          <div className="inspector-meta">
            <span>Category: {selectedNode.category}</span>
            <span>Kind: {selectedNode.kind}</span>
            <span>Provider: {selectedNode.provider_label}</span>
          </div>
          {isStructuredPayloadBuilderNode ? (
            <button
              type="button"
              className="secondary-button"
              onClick={() => setIsStructuredPayloadBuilderLearnMoreOpen(true)}
            >
              Learn More About This Node
            </button>
          ) : null}
          {onOpenProviderDetails &&
          selectedNode.provider_id !== "core.data_display" &&
          selectedNode.provider_id !== "core.logic_conditions" ? (
            <button
              type="button"
              className="secondary-button"
              onClick={() => onOpenProviderDetails(selectedNode.id)}
            >
              Learn More About Provider
            </button>
          ) : null}
          {contract ? (
            <div className="contract-card">
              <strong>Contract</strong>
              <span>Accepts: {contract.accepted_inputs.join(", ")}</span>
              <span>Produces: {contract.produced_outputs.join(", ")}</span>
            </div>
          ) : null}
          {selectedNode.kind === "input" ? (
            <>
              <label>
                Start Trigger
                <input value={isDiscordStartNode ? "discord_message" : "manual_run"} readOnly />
              </label>
              {isManualStartNode ? (
                <div className="contract-card">
                  <strong>Manual Run Start</strong>
                  <span>This node is triggered by clicking Run in the editor.</span>
                  <span>Payload source: input payload passed to the run request.</span>
                </div>
              ) : null}
              {isDiscordStartNode ? (
                <>
                  <label>
                    Discord Bot Token Env Var
                    <input
                      value={String(selectedNode.config.discord_bot_token_env_var ?? "{DISCORD_BOT_TOKEN}")}
                      placeholder="{DISCORD_BOT_TOKEN}"
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              trigger_mode: "discord_message",
                              discord_bot_token_env_var: event.target.value,
                            },
                          })),
                        )
                      }
                    />
                  </label>
                  <label>
                    Discord Channel ID
                    <input
                      value={String(selectedNode.config.discord_channel_id ?? "")}
                      placeholder="123456789012345678"
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              trigger_mode: "discord_message",
                              discord_channel_id: event.target.value,
                            },
                          })),
                        )
                      }
                    />
                  </label>
                  <label className="checkbox-option">
                    <input
                      type="checkbox"
                      checked={Boolean(selectedNode.config.ignore_bot_messages ?? true)}
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              trigger_mode: "discord_message",
                              ignore_bot_messages: event.target.checked,
                            },
                          })),
                        )
                      }
                    />
                    <span>Ignore bot-authored messages</span>
                  </label>
                  <label className="checkbox-option">
                    <input
                      type="checkbox"
                      checked={Boolean(selectedNode.config.ignore_self_messages ?? true)}
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              trigger_mode: "discord_message",
                              ignore_self_messages: event.target.checked,
                            },
                          })),
                        )
                      }
                    />
                    <span>Ignore this bot's own messages</span>
                  </label>
                </>
              ) : null}
            </>
          ) : null}
          {selectedNode.kind === "output" ? (
            <>
              <div className="contract-card">
                <strong>
                  {isDiscordEndNode
                    ? "Discord Side-Effect End"
                    : isOutlookDraftEndNode
                      ? "Outlook Draft End"
                      : "Canonical Output End"}
                </strong>
                <span>
                  {isDiscordEndNode
                    ? "Sends the resolved payload to a Discord channel and leaves run final_output unchanged."
                    : isOutlookDraftEndNode
                      ? "Creates a draft email in Outlook using Microsoft Graph and never sends it automatically."
                      : "Promotes the resolved payload into the run final_output when this branch completes."}
                </span>
              </div>
              <label>
                Body Source
                <select
                  value={outputSourceBindingValue}
                  onChange={(event) =>
                    onGraphChange(
                      updateNode(graph, selectedNode.id, (node) => {
                        const nextConfig = { ...node.config };
                        const nextBinding = decodeOutputSourceBinding(event.target.value);
                        if (nextBinding) {
                          nextConfig.source_binding = nextBinding;
                        } else {
                          delete nextConfig.source_binding;
                        }
                        return {
                          ...node,
                          config: nextConfig,
                        };
                      }),
                    )
                  }
                >
                  {outputSourceBindingOptions.map((option) => (
                    <option key={option.value} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>
              {isDiscordEndNode ? (
                <>
                  <label>
                    Discord Bot Token Env Var
                    <input
                      value={String(selectedNode.config.discord_bot_token_env_var ?? "{DISCORD_BOT_TOKEN}")}
                      placeholder="{DISCORD_BOT_TOKEN}"
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              discord_bot_token_env_var: event.target.value,
                            },
                          })),
                        )
                      }
                    />
                  </label>
                  <label>
                    Discord Channel ID
                    <input
                      value={String(selectedNode.config.discord_channel_id ?? "")}
                      placeholder="123456789012345678"
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              discord_channel_id: event.target.value,
                            },
                          })),
                        )
                      }
                    />
                  </label>
                  <label>
                    Message Template
                    <textarea
                      rows={4}
                      value={String(selectedNode.config.message_template ?? "{message_payload}")}
                      placeholder="{message_payload}"
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              message_template: event.target.value,
                            },
                          })),
                        )
                      }
                    />
                  </label>
                  <div className="contract-card">
                    <strong>Template Variables</strong>
                    <span><code>{"{message_payload}"}</code> renders the resolved payload as text.</span>
                    <span><code>{"{message_json}"}</code> renders JSON for structured payloads.</span>
                  </div>
                </>
              ) : null}
              {isOutlookDraftEndNode ? (
                <>
                  <div className="contract-card">
                    <strong>Microsoft Account</strong>
                    <span>
                      {microsoftAuthStatus?.connected
                        ? microsoftAuthStatus.account_username
                          ? `Connected as ${microsoftAuthStatus.account_username}.`
                          : "Connected."
                        : microsoftAuthStatus?.pending
                          ? "Connection pending. Finish device-code sign-in in the Environment panel."
                          : "No Microsoft account connected. Use the Environment panel to connect one before running this node."}
                    </span>
                  </div>
                  <div className="contract-card">
                    <strong>Required Fields</strong>
                    <div className="graph-inspector-inline-toggles">
                      <label className="graph-inspector-inline-toggle">
                        <input
                          type="checkbox"
                          checked={Boolean(selectedNode.config.require_to ?? true)}
                          onChange={(event) =>
                            onGraphChange(
                              updateNode(graph, selectedNode.id, (node) => ({
                                ...node,
                                config: {
                                  ...node.config,
                                  require_to: event.target.checked,
                                },
                              })),
                            )
                          }
                        />
                        <span>Email</span>
                      </label>
                      <label className="graph-inspector-inline-toggle">
                        <input
                          type="checkbox"
                          checked={Boolean(selectedNode.config.require_subject ?? true)}
                          onChange={(event) =>
                            onGraphChange(
                              updateNode(graph, selectedNode.id, (node) => ({
                                ...node,
                                config: {
                                  ...node.config,
                                  require_subject: event.target.checked,
                                },
                              })),
                            )
                          }
                        />
                        <span>Subject</span>
                      </label>
                      <label className="graph-inspector-inline-toggle">
                        <input
                          type="checkbox"
                          checked={Boolean(selectedNode.config.require_body ?? true)}
                          onChange={(event) =>
                            onGraphChange(
                              updateNode(graph, selectedNode.id, (node) => ({
                                ...node,
                                config: {
                                  ...node.config,
                                  require_body: event.target.checked,
                                },
                              })),
                            )
                          }
                        />
                        <span>Body</span>
                      </label>
                    </div>
                  </div>
                  <label>
                    To
                    <input
                      value={String(selectedNode.config.to ?? "")}
                      placeholder="person@example.com, teammate@example.com"
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              to: event.target.value,
                            },
                          })),
                        )
                      }
                    />
                  </label>
                  <label>
                    Subject
                    <input
                      value={String(selectedNode.config.subject ?? "")}
                      placeholder="Draft subject"
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              subject: event.target.value,
                            },
                          })),
                        )
                      }
                    />
                  </label>
                  <div className="contract-card">
                    <strong>Draft Behavior</strong>
                    <span>The body comes from the selected Body Source and is stored as plain text in Outlook.</span>
                    <span>Leave To blank to auto-use a payload email when the resolved data includes fields like <code>email</code>.</span>
                    <span>Use <code>{"{input_payload}"}</code>, <code>{"{message_payload}"}</code>, or <code>{"{message_json}"}</code> in the subject if you want it templated.</span>
                    <span>The To field also supports top-level payload placeholders like <code>{"{email}"}</code>.</span>
                    <span>Authentication is handled globally through Microsoft device-code sign-in, not graph env vars.</span>
                    <span>Only the toggled fields are required before saving the draft.</span>
                  </div>
                </>
              ) : null}
            </>
          ) : null}
          {isPromptDrivenNode ? (
            <>
              <label>
                Model Provider
                <select
                  value={selectedProviderName}
                  onChange={(event) => {
                    const nextProvider = availableModelProviders.find((provider) => {
                      const providerName = provider.provider_id.replace("provider.", "");
                      return providerName === event.target.value;
                    });
                    if (!nextProvider) {
                      return;
                    }
                    const nextProviderName = providerModelName(nextProvider);
                    const nextProviderConfig = providerDefaultConfig(nextProvider);
                    const providerConfigKeys = Array.from(
                      new Set(
                        availableModelProviders.flatMap((provider) => [
                          "provider_name",
                          ...((provider.config_fields ?? []).map((field) => field.key)),
                        ]),
                      ),
                    );
                    onGraphChange(
                      updateNode(graph, selectedNode.id, (node) => {
                        const nextConfig = { ...node.config };
                        providerConfigKeys.forEach((key) => delete nextConfig[key]);
                        return {
                          ...node,
                          model_provider_name: nextProviderName,
                          config: {
                            ...nextConfig,
                            ...nextProviderConfig,
                            provider_name: nextProviderName,
                            model:
                              typeof nextProviderConfig.model === "string"
                                ? nextProviderConfig.model
                                : defaultModelName(nextProviderName, catalog) || node.config.model,
                          },
                        };
                      }),
                    );
                  }}
                >
                  {availableModelProviders.map((provider) => {
                    const providerName = providerModelName(provider);
                    return (
                      <option key={provider.provider_id} value={providerName}>
                        {provider.display_name}
                      </option>
                    );
                  })}
                </select>
              </label>
              {providerStatus ? (
                <div className="contract-card">
                  <strong>Provider Health</strong>
                  <span>{providerStatus.message}</span>
                  {(providerStatus.warnings ?? []).map((warning) => (
                    <span key={warning}>{warning}</span>
                  ))}
                </div>
              ) : null}
              <label>
                Model Provider Name
                <input
                  value={String(selectedNode.config.provider_name ?? selectedNode.model_provider_name ?? "")}
                  onChange={(event) =>
                    onGraphChange(
                      updateNode(graph, selectedNode.id, (node) => ({
                        ...node,
                        model_provider_name: event.target.value,
                        config: { ...node.config, provider_name: event.target.value },
                      })),
                    )
                  }
                />
              </label>
              <label>
                Prompt Name
                <input
                  value={String(selectedNode.config.prompt_name ?? selectedNode.prompt_name ?? "")}
                  onChange={(event) =>
                    onGraphChange(
                      updateNode(graph, selectedNode.id, (node) => ({
                        ...node,
                        prompt_name: event.target.value,
                        config: { ...node.config, prompt_name: event.target.value, mode: event.target.value },
                      })),
                    )
                  }
                />
              </label>
              {displayedProviderConfigFields.map((field) => {
                const value = selectedNode.config[field.key];
                const isNumberField = field.input_type === "number";
                const isSelectField = field.input_type === "select" && (field.options?.length ?? 0) > 0;
                const isModelSelectField = isSelectField && field.key === "model";
                const currentValue = String(value ?? "");
                const selectOptions =
                  isSelectField && currentValue && !field.options?.some((option) => option.value === currentValue)
                    ? [...(field.options ?? []), { value: currentValue, label: `Custom: ${currentValue}` }]
                    : (field.options ?? []);
                const datalistId = `${selectedNode.id}-${field.key}-options`;
                const inputProps = isNumberField ? { type: "number" } : {};
                return (
                  <label key={field.key}>
                    {field.label}
                    {isModelSelectField ? (
                      <>
                        <input
                          list={datalistId}
                          value={currentValue}
                          placeholder={field.placeholder || "Select or type a model id"}
                          onChange={(event) =>
                            onGraphChange(
                              updateNode(graph, selectedNode.id, (node) => ({
                                ...node,
                                config: {
                                  ...node.config,
                                  [field.key]: event.target.value,
                                },
                              })),
                            )
                          }
                        />
                        <datalist id={datalistId}>
                          {selectOptions.map((option) => (
                            <option key={option.value} value={option.value}>
                              {option.label}
                            </option>
                          ))}
                        </datalist>
                      </>
                    ) : isSelectField ? (
                      <select
                        value={currentValue}
                        onChange={(event) =>
                          onGraphChange(
                            updateNode(graph, selectedNode.id, (node) => ({
                              ...node,
                              config: {
                                ...node.config,
                                [field.key]: event.target.value,
                              },
                            })),
                          )
                        }
                      >
                        {selectOptions.map((option) => (
                          <option key={option.value} value={option.value}>
                            {option.label}
                          </option>
                        ))}
                      </select>
                    ) : (
                      <input
                        {...inputProps}
                        value={currentValue}
                        placeholder={field.placeholder || undefined}
                        onChange={(event) =>
                          onGraphChange(
                            updateNode(graph, selectedNode.id, (node) => ({
                              ...node,
                              config: {
                                ...node.config,
                                [field.key]:
                                  isNumberField && event.target.value !== "" ? Number(event.target.value) : event.target.value,
                              },
                            })),
                          )
                        }
                      />
                    )}
                  </label>
                );
              })}
              {isSpreadsheetMatrixNode ? (
                <>
                  <div className="contract-card">
                    <strong>Spreadsheet Matrix Decision</strong>
                    <span>Uses the first row as column choices and the first column as row choices, then asks the configured model to pick the best row/column pair for the current request.</span>
                    <span>The node emits the selected cell value as its message payload and stores the chosen coordinates in the run artifacts.</span>
                  </div>
                  <label>
                    File Format
                    <select
                      value={String(selectedNode.config.file_format ?? "auto")}
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              file_format: event.target.value,
                              mode: "spreadsheet_matrix_decision",
                            },
                          })),
                        )
                      }
                    >
                      <option value="auto">Auto Detect</option>
                      <option value="csv">CSV</option>
                      <option value="xlsx">Excel (.xlsx)</option>
                    </select>
                  </label>
                  <label>
                    Project File
                    <select
                      value={spreadsheetProjectFileSelectValue}
                      onChange={(event) => {
                        const nextFileId = event.target.value;
                        if (!nextFileId || nextFileId === "__manual__") {
                          return;
                        }
                        applySpreadsheetProjectFile(nextFileId);
                      }}
                    >
                      <option value="">Select a saved spreadsheet</option>
                      {hasManualSpreadsheetPath ? <option value="__manual__">Manual path</option> : null}
                      {spreadsheetProjectFiles.map((file) => (
                        <option key={file.file_id} value={file.file_id}>
                          {file.name}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label>
                    File Path
                    <input
                      value={String(selectedNode.config.file_path ?? "")}
                      placeholder="Project file path or graph env reference for the matrix spreadsheet"
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              project_file_id: "",
                              project_file_name: "",
                              file_path: event.target.value,
                              mode: "spreadsheet_matrix_decision",
                            },
                          })),
                        )
                      }
                    />
                  </label>
                  <label>
                    Sheet Name
                    <input
                      value={String(selectedNode.config.sheet_name ?? "")}
                      placeholder="Leave blank to use the first sheet"
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              sheet_name: event.target.value,
                              mode: "spreadsheet_matrix_decision",
                            },
                          })),
                        )
                      }
                    />
                  </label>
                  <div className="context-builder-binding-actions">
                    <button
                      type="button"
                      className="secondary-button context-builder-inline-button"
                      onClick={() => void handleSpreadsheetPreview()}
                      disabled={isSpreadsheetPreviewLoading || spreadsheetResolvedFilePath.trim().length === 0}
                    >
                      {isSpreadsheetPreviewLoading ? "Loading Preview..." : "Preview Matrix"}
                    </button>
                  </div>
                  <div className="inspector-meta">
                    <span>Resolved file path: {spreadsheetResolvedFilePath || "Enter a file path or graph env reference."}</span>
                    <span>Axis parsing: row 1 becomes columns, column 1 becomes rows</span>
                    <span>Output contract: message payload containing the selected cell value</span>
                  </div>
                  {spreadsheetPreviewError ? <div className="tool-details-modal-help">{spreadsheetPreviewError}</div> : null}
                  <div className="contract-card">
                    <strong>Matrix Preview</strong>
                    <span>
                      {spreadsheetPreview
                        ? `${spreadsheetPreview.row_count} row(s) parsed from ${spreadsheetPreview.file_format.toUpperCase()}`
                        : "Run a preview to inspect the matrix headers and sample rows."}
                    </span>
                    <span>
                      Column labels: {spreadsheetPreview ? (spreadsheetPreview.headers.slice(1).join(", ") || "None") : "Unknown"}
                    </span>
                    <span>
                      Sample row labels: {spreadsheetPreview ? (spreadsheetPreviewSampleRowLabels.join(", ") || "None") : "Unknown"}
                    </span>
                    <pre className="context-builder-preview">
                      {spreadsheetPreview
                        ? JSON.stringify(
                            {
                              corner_label: spreadsheetPreview.headers[0] ?? "",
                              column_labels: spreadsheetPreview.headers.slice(1),
                              sample_row_labels: spreadsheetPreviewSampleRowLabels,
                              sheet_name: spreadsheetPreview.sheet_name,
                              row_count: spreadsheetPreview.row_count,
                              sample_rows: spreadsheetPreview.sample_rows,
                            },
                            null,
                            2,
                          )
                        : "Preview output will appear here."}
                    </pre>
                  </div>
                </>
              ) : null}
              <label>
                System Prompt
                <textarea
                  rows={5}
                  value={String(selectedNode.config.system_prompt ?? "")}
                  onChange={(event) =>
                    onGraphChange(
                      updateNode(graph, selectedNode.id, (node) => ({
                        ...node,
                        config: { ...node.config, system_prompt: event.target.value },
                      })),
                    )
                  }
                />
                {selectedNode.kind === "model" && modelPromptContextToolSummaries.length > 0 ? (
                  <div className="context-builder-binding-actions">
                    <button
                      type="button"
                      className="secondary-button context-builder-inline-button"
                      onClick={() =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              system_prompt: modelGeneratedMcpPlaceholderTemplate,
                            },
                          })),
                        )
                      }
                    >
                      Build From Connected MCP Tools
                    </button>
                  </div>
                ) : null}
                {selectedNode.kind === "model" && (modelCallableMcpToolNames.length > 0 || modelPromptGuidanceProviders.length > 0) ? (
                  <small>
                    Connected MCP edges already guarantee which tools are in scope. For full inline MCP coverage, include{" "}
                    <code>{"{mcp_tool_guidance_block}"}</code> and either <code>{"{mcp_tool_context_block}"}</code> or
                    every ordered tool placeholder below. Missing MCP sections are appended automatically.
                  </small>
                ) : null}
                {selectedNode.kind === "model" && modelPromptContextToolSummaries.length > 0 ? (
                  <small>
                    Ordered MCP tool placeholders:{" "}
                    {modelPromptContextToolSummaries.map((tool) => (
                      <code key={tool.placeholderToken}>{`{${tool.placeholderToken}}`}</code>
                    ))}{" "}
                    resolve inline at runtime.
                  </small>
                ) : null}
                {selectedNode.kind === "model" ? (
                  <small>
                    Markdown formatting is supported in prompt templates. Use <code>{"{placeholder}"}</code> tokens for runtime values.
                  </small>
                ) : null}
              </label>
              <label>
                User Message Template
                <textarea
                  rows={5}
                  value={displayedUserMessageTemplate}
                  onChange={(event) =>
                    onGraphChange(
                      updateNode(graph, selectedNode.id, (node) => ({
                        ...node,
                        config: { ...node.config, user_message_template: event.target.value },
                      })),
                    )
                  }
                />
                {selectedNode.kind === "model" && contextBuilderPromptVariables.length > 0 ? (
                  <div className="context-builder-placeholder-bar">
                    <button
                      type="button"
                      className="secondary-button context-builder-token-button"
                      onClick={() =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              user_message_template: insertTokenAtEnd(displayedUserMessageTemplate, "{input_payload}"),
                            },
                          })),
                        )
                      }
                    >
                      {"{input_payload}"}
                    </button>
                    {contextBuilderPromptVariables.map((variable) => (
                      <button
                        key={`${variable.contextBuilderNodeId}-${variable.token}`}
                        type="button"
                        className="secondary-button context-builder-token-button"
                        title={`${variable.header} from ${variable.contextBuilderLabel}`}
                        onClick={() =>
                          onGraphChange(
                            updateNode(graph, selectedNode.id, (node) => ({
                              ...node,
                              config: {
                                ...node.config,
                                user_message_template: insertTokenAtEnd(displayedUserMessageTemplate, `{${variable.token}}`),
                              },
                            })),
                          )
                        }
                      >
                        {`{${variable.token}}`}
                      </button>
                    ))}
                  </div>
                ) : null}
                {selectedNode.kind === "model" && contextBuilderPromptVariables.length > 0 ? (
                  <small>
                    Context Builder section tags:{" "}
                    {contextBuilderPromptVariables.map((variable) => (
                      <code key={`${variable.contextBuilderNodeId}-${variable.token}-label`}>{`{${variable.token}}`}</code>
                    ))}{" "}
                    resolve from the connected Context Builder input at runtime.
                  </small>
                ) : null}
                {selectedNode.kind === "model" ? (
                  <small>
                    Markdown is preserved, including headings, bullets, and fenced code blocks.
                  </small>
                ) : null}
              </label>
              <label>
                Response Mode
                {selectedNode.kind === "model" ? (
                  isSpreadsheetMatrixNode ? (
                    <>
                      <input value="message" readOnly />
                      <small>This node always emits the selected spreadsheet cell as a message payload.</small>
                    </>
                  ) : (
                    <>
                    <select
                      value={String(selectedNode.config.response_mode ?? "auto") || "auto"}
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: { ...node.config, response_mode: event.target.value },
                          })),
                        )
                      }
                    >
                      <option value="auto">auto</option>
                      <option value="tool_call">tool_call</option>
                      <option value="message">message</option>
                    </select>
                    <small>
                      Choose a fixed mode or leave it on `auto` to follow graph wiring. Current effective mode:{" "}
                      <code>{selectedModelResponseMode ?? "message"}</code>.
                    </small>
                    </>
                  )
                ) : (
                  <>
                    <select
                      value={executorFollowUpResponseMode}
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: { ...node.config, response_mode: event.target.value },
                          })),
                        )
                      }
                    >
                      <option value="auto">auto</option>
                      <option value="tool_call">tool_call</option>
                      <option value="message">message</option>
                    </select>
                    <small>Controls whether the executor's follow-up model may request another MCP tool, emit a message, or do either.</small>
                  </>
                )}
              </label>
              {isPromptDrivenNode ? (
                <>
                  <label>
                    Intended Output Schema
                    <div className="context-builder-placeholder-bar">
                      <button
                        type="button"
                        className="secondary-button context-builder-inline-button"
                        onClick={() => updateResponseSchemaText("")}
                      >
                        Clear
                      </button>
                      {RESPONSE_SCHEMA_PRESETS.map((preset) => (
                        <button
                          key={preset.id}
                          type="button"
                          className="secondary-button context-builder-inline-button"
                          onClick={() => updateResponseSchemaText(preset.schemaText)}
                        >
                          {preset.label}
                        </button>
                      ))}
                    </div>
                    <textarea
                      rows={10}
                      className="tool-details-modal-code"
                      value={responseSchemaDetails?.schemaText ?? ""}
                      placeholder='Leave blank to allow any JSON value, or define a JSON Schema object like {"type":"object","properties":{...}}'
                      onChange={(event) => updateResponseSchemaText(event.target.value)}
                      spellCheck={false}
                    />
                    <small>
                      Optional JSON Schema for the final <code>message</code> payload this API block emits. The surrounding
                      decision envelope still includes <code>need_tool</code> and <code>tool_calls</code>.
                    </small>
                  </label>
                  {responseSchemaDetails?.schemaError ? (
                    <p className="error-text">Schema JSON error: {responseSchemaDetails.schemaError}</p>
                  ) : null}
                  <div className="contract-card">
                    <strong>Output Schema</strong>
                    <span>Status: {responseSchemaDetails?.statusLabel ?? "Default flexible payload"}</span>
                    <span>
                      {selectedNode.kind === "model"
                        ? "Applies whenever this API block emits a final message."
                        : "Applies whenever the follow-up model emits a final message instead of another MCP tool call."}
                    </span>
                  </div>
                </>
              ) : null}
              {!isSpreadsheetMatrixNode ? (
                <div className="checkbox-grid">
                <strong>{selectedNode.kind === "mcp_tool_executor" ? "Allowed MCP Tools" : "Direct Registry Tools"}</strong>
                {followUpSelectableTools.map((tool) => {
                  const canonicalName = toolCanonicalName(tool);
                  const isChecked = allowedTools.some((name) => toolMatchesReference(tool, name));
                  const canSelectTool = isToolEnabled(tool) && isToolOnline(tool);
                  return (
                    <label key={canonicalName} className="checkbox-option">
                      <input
                        type="checkbox"
                        checked={isChecked}
                        disabled={!isChecked && !canSelectTool}
                        onChange={(event) => {
                          const nextTools = event.target.checked
                            ? [...allowedTools.filter((name) => !toolMatchesReference(tool, name)), canonicalName]
                            : allowedTools.filter((name) => !toolMatchesReference(tool, name));
                          onGraphChange(
                            updateNode(graph, selectedNode.id, (node) => ({
                              ...node,
                              config: {
                                ...node.config,
                                allowed_tool_names: nextTools,
                                preferred_tool_name:
                                  nextTools.length > 0 ? String(node.config.preferred_tool_name ?? nextTools[0]) : "",
                              },
                            })),
                          );
                        }}
                      />
                      <span>
                        {toolLabel(tool)}
                        {toolLabel(tool) !== canonicalName ? <small><code>{canonicalName}</code></small> : null}
                        <small>{toolStatusLabel(tool)}</small>
                      </span>
                    </label>
                  );
                })}
                </div>
              ) : null}
              {selectedNode.kind === "model" && !isSpreadsheetMatrixNode ? (
                <div className="contract-card">
                  <strong>MCP Tools From Context Providers</strong>
                  <span>
                    Callable MCP tools: {modelCallableMcpTools.length > 0 ? modelCallableMcpTools.join(", ") : "None"}
                  </span>
                  <span>
                    Prompt context sources: {modelPromptContextProviders.length > 0 ? modelPromptContextProviders.join(", ") : "None"}
                  </span>
                  {modelTargetedMcpNodeIds.length > 0 ? (
                    <span>Targeted MCP provider IDs: {modelTargetedMcpNodeIds.join(", ")}</span>
                  ) : (
                    <span>MCP tools are supplied through connected or targeted MCP Context Provider nodes.</span>
                  )}
                </div>
              ) : null}
              {selectedNode.kind === "model" ? (
                <div className="contract-card">
                  <strong>Bound Prompt Blocks</strong>
                  <span>
                    Direct prompt messages:{" "}
                    {modelPromptBlockNodes.length > 0
                      ? modelPromptBlockNodes.map((node) => `${formatNodeLabel(node)} (${String(node.config.role ?? "user")})`).join(", ")
                      : "None"}
                  </span>
                  <span>Bind Prompt Block nodes into the model to inject additional system, user, or assistant messages before the standard user template.</span>
                </div>
              ) : null}
              {selectedNode.kind === "model" ? (
                <>
                  <div className="contract-card">
                    <strong>System Prompt Assembly</strong>
                    <span>
                      Template placeholders: {modelSystemPromptTokens.length > 0 ? modelSystemPromptTokens.join(", ") : "None"}
                    </span>
                    <span>
                      Required MCP guidance:{" "}
                      {modelMcpGuidanceBlock.length > 0
                        ? modelHasInlineMcpGuidanceBlock
                          ? "inline"
                          : "auto-appended"
                        : "not needed"}
                    </span>
                    <span>
                      Required MCP context:{" "}
                      {modelPromptContextToolSummaries.length > 0
                        ? modelHasInlineMcpContextCoverage
                          ? "inline"
                          : "auto-appended"
                        : "not needed"}
                    </span>
                    <span>
                      Prompt block messages stay separate from the system prompt:{" "}
                      {modelPromptBlockNodes.length > 0 ? `${modelPromptBlockNodes.length} bound block${modelPromptBlockNodes.length === 1 ? "" : "s"}` : "None"}
                    </span>
                  </div>
                  {modelGeneratedMcpPlaceholderTemplate ? (
                    <section className="tool-details-modal-preview">
                      <div className="tool-details-modal-preview-header">
                        <strong>Generated MCP Placeholder Template</strong>
                        <span>
                          This scaffold is built from connected MCP prompt-context edges and can replace the system prompt
                          with ordered placeholders before runtime.
                        </span>
                      </div>
                      <pre>{modelGeneratedMcpPlaceholderTemplate}</pre>
                    </section>
                  ) : null}
                  <div className="checkbox-grid">
                    <strong>Required MCP Placeholders</strong>
                    <span className="inspector-meta">
                      Necessary for full inline MCP control. If these are omitted, the runtime appends the missing MCP
                      sections automatically.
                    </span>
                    <div className="context-builder-placeholder-bar">
                      {modelMcpGuidanceBlock.length > 0 ? (
                        <button
                          type="button"
                          className="secondary-button context-builder-token-button context-builder-token-button--required"
                          onClick={() =>
                            onGraphChange(
                              updateNode(graph, selectedNode.id, (node) => ({
                                ...node,
                                config: {
                                  ...node.config,
                                  system_prompt: insertTokenAtEnd(String(node.config.system_prompt ?? ""), "{mcp_tool_guidance_block}"),
                                },
                              })),
                            )
                          }
                        >
                          mcp_tool_guidance_block
                        </button>
                      ) : null}
                      {modelPromptContextToolSummaries.length > 0 ? (
                        <button
                          type="button"
                          className="secondary-button context-builder-token-button context-builder-token-button--required"
                          onClick={() =>
                            onGraphChange(
                              updateNode(graph, selectedNode.id, (node) => ({
                                ...node,
                                config: {
                                  ...node.config,
                                  system_prompt: insertTokenAtEnd(String(node.config.system_prompt ?? ""), "{mcp_tool_context_block}"),
                                },
                              })),
                            )
                          }
                        >
                          mcp_tool_context_block
                        </button>
                      ) : null}
                      {modelMcpToolPlaceholderTokens.map((token) => (
                        <button
                          key={token}
                          type="button"
                          className="secondary-button context-builder-token-button context-builder-token-button--required"
                          onClick={() =>
                            onGraphChange(
                              updateNode(graph, selectedNode.id, (node) => ({
                                ...node,
                                config: {
                                  ...node.config,
                                  system_prompt: insertTokenAtEnd(String(node.config.system_prompt ?? ""), `{${token}}`),
                                },
                              })),
                            )
                          }
                        >
                          {token}
                        </button>
                      ))}
                    </div>
                    {modelPromptContextToolSummaries.length > 0 ? (
                      <span className="inspector-meta">
                        Context coverage can come from <code>{"{mcp_tool_context_block}"}</code> or from all ordered tool
                        placeholders.
                      </span>
                    ) : null}
                  </div>
                  <div className="checkbox-grid">
                    <strong>Optional Runtime Variables</strong>
                    <span className="inspector-meta">
                      These are available if you want to reference other runtime values explicitly.
                    </span>
                    <div className="context-builder-placeholder-bar">
                      {modelOptionalPromptVariables.map((token) => (
                        <button
                          key={token}
                          type="button"
                          className="secondary-button context-builder-token-button context-builder-token-button--optional"
                          onClick={() =>
                            onGraphChange(
                              updateNode(graph, selectedNode.id, (node) => ({
                                ...node,
                                config: {
                                  ...node.config,
                                  system_prompt: insertTokenAtEnd(String(node.config.system_prompt ?? ""), `{${token}}`),
                                },
                              })),
                            )
                          }
                        >
                          {token}
                        </button>
                      ))}
                    </div>
                  </div>
                  <div className="prompt-preview-tool-list">
                    <div className="contract-card">
                      <strong>Connected MCP Tool Info</strong>
                      <span>
                        Callable MCP tools: {modelCallableMcpToolNames.length > 0 ? modelCallableMcpToolNames.join(", ") : "None"}
                      </span>
                      <span>
                        Prompt context tools:{" "}
                        {modelPromptContextToolSummaries.length > 0
                          ? modelPromptContextToolSummaries.map((tool) => tool.displayName).join(", ")
                          : "None"}
                      </span>
                    </div>
                    {modelPromptToolSummaries.length > 0 ? (
                      modelPromptToolSummaries.map((tool) => (
                        <div
                          key={`${tool.sourceNodeId}-${tool.toolName}`}
                          className="contract-card prompt-preview-tool-card"
                        >
                          <strong>
                            {tool.displayName} <code>{tool.toolName}</code>
                          </strong>
                          <span>Source: {tool.sourceNodeLabel}</span>
                          <span>Prompt override source: {tool.overrideNodeLabel}</span>
                          <span>Status: {tool.status}</span>
                          <span>Callable by model: {tool.isCallable ? "Yes" : "No"}</span>
                          <span>Injects prompt context: {tool.injectsPromptContext ? "Yes" : "No"}</span>
                          {"placeholderToken" in tool ? <span>Placeholder: <code>{`{${tool.placeholderToken}}`}</code></span> : null}
                          <span>Usage guidance: {tool.usageHint || "None"}</span>
                          <span>Prompt template:</span>
                          <pre className="context-builder-preview">{tool.templateText || "No prompt template."}</pre>
                          <span>Rendered MCP context block:</span>
                          <pre className="context-builder-preview">{tool.renderedPromptText || "No MCP prompt block."}</pre>
                        </div>
                      ))
                    ) : (
                      <div className="contract-card">
                        <strong>Connected MCP Tool Info</strong>
                        <span>No MCP Context Provider tools are connected to this model.</span>
                      </div>
                    )}
                  </div>
                  <section className="tool-details-modal-preview">
                    <div className="tool-details-modal-preview-header">
                      <strong>System Prompt Template Preview</strong>
                      <span>This resolves graph env references and currently visible runtime variables before auto-appended MCP sections are added.</span>
                    </div>
                    <pre>{modelSystemPromptTemplatePreview || "Add a system prompt template to preview it here."}</pre>
                  </section>
                  <section className="tool-details-modal-preview">
                    <div className="tool-details-modal-preview-header">
                      <strong>Final System Prompt Preview</strong>
                      <span>This mirrors the current runtime assembly path for MCP guidance, MCP tool context, and prompt-only MCP decision instructions.</span>
                    </div>
                    <pre>{modelFinalSystemPromptPreview || "The final assembled system prompt will appear here."}</pre>
                  </section>
                </>
              ) : null}
              {!isSpreadsheetMatrixNode ? (
                <label>
                  Preferred Tool Name
                  <input
                    value={String(selectedNode.config.preferred_tool_name ?? "")}
                    onChange={(event) =>
                      onGraphChange(
                        updateNode(graph, selectedNode.id, (node) => ({
                          ...node,
                          config: { ...node.config, preferred_tool_name: event.target.value },
                        })),
                      )
                    }
                  />
                </label>
              ) : null}
            </>
          ) : null}
          {selectedNode.kind === "tool" ? (
            <>
              <label>
                Tool
                <select
                  value={
                    standardCatalogTools.find((tool) =>
                      toolMatchesReference(tool, String(selectedNode.config.tool_name ?? selectedNode.tool_name ?? "")),
                    )?.canonical_name ??
                    standardCatalogTools.find((tool) =>
                      toolMatchesReference(tool, String(selectedNode.config.tool_name ?? selectedNode.tool_name ?? "")),
                    )?.name ??
                    String(selectedNode.config.tool_name ?? selectedNode.tool_name ?? "")
                  }
                  onChange={(event) =>
                    onGraphChange(
                      updateNode(graph, selectedNode.id, (node) => ({
                        ...node,
                        tool_name: event.target.value,
                        config: { ...node.config, tool_name: event.target.value },
                      })),
                    )
                  }
                >
                  {standardCatalogTools.map((tool) => (
                    <option key={toolCanonicalName(tool)} value={toolCanonicalName(tool)}>
                      {toolLabel(tool)} ({toolStatusLabel(tool)})
                    </option>
                  ))}
                </select>
              </label>
            </>
          ) : null}
          {selectedNode.kind === "mcp_context_provider" ? (
            <>
              <div className="inspector-meta">
                <span>Acts as a source-only context provider. No input connection is required.</span>
              </div>
              <div className="checkbox-grid">
                <strong>Registered MCP Tools</strong>
                {mcpCatalogTools.map((tool) => {
                  const canonicalName = toolCanonicalName(tool);
                  const isChecked = selectedMcpToolNames.some((name) => toolMatchesReference(tool, name));
                  const canSelectTool = isToolEnabled(tool) && isToolOnline(tool);
                  return (
                    <label key={canonicalName} className="checkbox-option">
                      <input
                        type="checkbox"
                        checked={isChecked}
                        disabled={!isChecked && !canSelectTool}
                        onChange={(event) => {
                          const nextTools = event.target.checked
                            ? [...selectedMcpToolNames.filter((name) => !toolMatchesReference(tool, name)), canonicalName]
                            : selectedMcpToolNames.filter((name) => !toolMatchesReference(tool, name));
                          onGraphChange(
                            updateNode(graph, selectedNode.id, (node) => ({
                              ...node,
                              config: {
                                ...node.config,
                                tool_names: nextTools,
                              },
                            })),
                          );
                        }}
                      />
                      <span>
                        {toolLabel(tool)}
                        {toolLabel(tool) !== canonicalName ? <small><code>{canonicalName}</code></small> : null}
                        <small>{toolStatusLabel(tool)}</small>
                      </span>
                    </label>
                  );
                })}
              </div>
              <label className="checkbox-option">
                <input
                  type="checkbox"
                  checked={mcpToolExposureEnabled}
                  onChange={(event) =>
                    onGraphChange(
                      updateNode(graph, selectedNode.id, (node) => ({
                        ...node,
                        config: {
                          ...node.config,
                          expose_mcp_tools: event.target.checked,
                        },
                      })),
                    )
                  }
                />
                <span>
                  Expose MCP Tools To Connected API Nodes
                  <small>Makes the selected MCP tools callable by connected or targeted API/model nodes when the tools are enabled and online.</small>
                </span>
              </label>
              <label className="checkbox-option">
                <input
                  type="checkbox"
                  checked={Boolean(selectedNode.config.include_mcp_tool_context)}
                  onChange={(event) =>
                    onGraphChange(
                      updateNode(graph, selectedNode.id, (node) => ({
                        ...node,
                        config: {
                          ...node.config,
                          include_mcp_tool_context: event.target.checked,
                        },
                      })),
                    )
                  }
                />
                <span>
                  Inject MCP Prompt Context Into Connected API Nodes
                  <small>Adds descriptive MCP tool metadata to the connected model system prompt. This does not control tool callability.</small>
                </span>
              </label>
              <label>
                Usage Guidance
                <textarea
                  rows={4}
                  value={String(selectedNode.config.usage_hint ?? "")}
                  onChange={(event) =>
                    onGraphChange(
                      updateNode(graph, selectedNode.id, (node) => ({
                        ...node,
                        config: {
                          ...node.config,
                          usage_hint: event.target.value,
                        },
                      })),
                    )
                  }
                />
                <small>
                  Natural-language hint folded into the connected model&apos;s required MCP guidance block when MCP prompt
                  context is enabled.
                </small>
              </label>
            </>
          ) : null}
          {selectedNode.kind === "mcp_tool_executor" ? (
            <>
              <label className="checkbox-option">
                <input
                  type="checkbox"
                  checked={executorFollowUpEnabled}
                  onChange={(event) =>
                    onGraphChange(
                      updateNode(graph, selectedNode.id, (node) => ({
                        ...node,
                        config: {
                          ...node.config,
                          enable_follow_up_decision: event.target.checked,
                        },
                      })),
                    )
                  }
                />
                <span>
                  Enable Follow-Up Decision
                  <small>Let the executor inspect each MCP result with a model, repair invalid MCP tool-call schemas, and decide whether to stop or call another exposed MCP tool.</small>
                </span>
              </label>
              {executorFollowUpEnabled ? (
                <label className="checkbox-option">
                  <input
                    type="checkbox"
                    checked={Boolean(selectedNode.config.validate_last_tool_success ?? true)}
                    onChange={(event) =>
                      onGraphChange(
                        updateNode(graph, selectedNode.id, (node) => ({
                          ...node,
                          config: {
                            ...node.config,
                            validate_last_tool_success: event.target.checked,
                          },
                        })),
                      )
                    }
                  />
                  <span>
                    Stop On Failed Tool Result
                    <small>When enabled, the executor stops after actual MCP execution failures. Schema validation errors can still be repaired when retries are on.</small>
                  </span>
                </label>
              ) : null}
              {executorFollowUpEnabled ? (
                <label className="checkbox-option">
                  <input
                    type="checkbox"
                    checked={executorRetriesEnabled}
                    onChange={(event) =>
                      onGraphChange(
                        updateNode(graph, selectedNode.id, (node) => ({
                          ...node,
                          config: {
                            ...node.config,
                            allow_retries: event.target.checked,
                          },
                        })),
                      )
                    }
                  />
                  <span>
                    Enable Retries
                    <small>Retries require follow-up decisions. When on, the executor can repair malformed MCP tool-call schemas and continue model-guided follow-up checks; when off, it only executes the incoming tool call once.</small>
                  </span>
                </label>
              ) : null}
              <div className="inspector-meta">
                <span>Dispatch mode: one MCP tool call at a time from upstream API output</span>
                <span>Input binding: {executorBindingSummary}</span>
                <span>
                  Follow-up decision: {executorFollowUpEnabled ? "enabled via internal model loop" : "disabled"}
                </span>
                <span>
                  Retries: {executorFollowUpEnabled ? (executorRetriesEnabled ? "enabled for schema repair and follow-up" : "disabled") : "n/a"}
                </span>
                <span>Routes: on finish / on failure / terminal output</span>
              </div>
            </>
          ) : null}
          {selectedNode.kind === "control_flow_unit" ? (
            <>
              {isSpreadsheetRowNode ? (
                <>
                  <div className="contract-card">
                    <strong>Spreadsheet Rows</strong>
                    <span>Reads a CSV or XLSX file, maps each row to a header-keyed dictionary, and runs downstream nodes once per row in strict sequence.</span>
                    <span>Each row is emitted through the `loop-body` handle as `payload.row_data` plus row and sheet metadata.</span>
                  </div>
                  <label>
                    File Format
                    <select
                      value={String(selectedNode.config.file_format ?? "auto")}
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: { ...node.config, file_format: event.target.value, mode: "spreadsheet_rows" },
                          })),
                        )
                      }
                    >
                      <option value="auto">Auto Detect</option>
                      <option value="csv">CSV</option>
                      <option value="xlsx">Excel (.xlsx)</option>
                    </select>
                  </label>
                  <label>
                    Project File
                    <select
                      value={spreadsheetProjectFileSelectValue}
                      onChange={(event) => {
                        const nextFileId = event.target.value;
                        if (!nextFileId || nextFileId === "__manual__") {
                          return;
                        }
                        applySpreadsheetProjectFile(nextFileId);
                      }}
                    >
                      <option value="">Select a saved spreadsheet</option>
                      {hasManualSpreadsheetPath ? <option value="__manual__">Manual path</option> : null}
                      {spreadsheetProjectFiles.map((file) => (
                        <option key={file.file_id} value={file.file_id}>
                          {file.name}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label>
                    File Path
                    <input
                      value={String(selectedNode.config.file_path ?? "")}
                      placeholder="Project file path or graph env reference"
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              project_file_id: "",
                              project_file_name: "",
                              file_path: event.target.value,
                              mode: "spreadsheet_rows",
                            },
                          })),
                        )
                      }
                    />
                  </label>
                  <label>
                    Sheet Name
                    <input
                      value={String(selectedNode.config.sheet_name ?? "")}
                      placeholder="Leave blank to use the first sheet"
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: { ...node.config, sheet_name: event.target.value, mode: "spreadsheet_rows" },
                          })),
                        )
                      }
                    />
                  </label>
                  <div className="checkbox-grid">
                    <label>
                      Empty Row Policy
                      <select
                        value={String(selectedNode.config.empty_row_policy ?? "skip")}
                        onChange={(event) =>
                          onGraphChange(
                            updateNode(graph, selectedNode.id, (node) => ({
                              ...node,
                              config: { ...node.config, empty_row_policy: event.target.value, mode: "spreadsheet_rows" },
                            })),
                          )
                        }
                      >
                        <option value="skip">Skip empty rows</option>
                        <option value="include">Include empty rows</option>
                      </select>
                    </label>
                  </div>
                  <p className="node-help-text">
                    Row 1 is always treated as the header row. Each later row is emitted as one iteration using those
                    header titles as the parsed row keys.
                  </p>
                  <div className="context-builder-binding-actions">
                    <button
                      type="button"
                      className="secondary-button context-builder-inline-button"
                      onClick={() => void handleSpreadsheetPreview()}
                      disabled={isSpreadsheetPreviewLoading || spreadsheetResolvedFilePath.trim().length === 0}
                    >
                      {isSpreadsheetPreviewLoading ? "Loading Preview..." : "Preview Rows"}
                    </button>
                  </div>
                  <div className="inspector-meta">
                    <span>Resolved file path: {spreadsheetResolvedFilePath || "Enter a file path or graph env reference."}</span>
                    <span>Execution mode: sequential per-row loop through downstream nodes</span>
                    <span>Output handle: `loop-body`</span>
                    <span>Recommended shape for downstream prompts/tools: `payload.row_data` key-value pairs</span>
                  </div>
                  {spreadsheetIteratorState ? (
                    <div className="contract-card">
                      <strong>Iterator Progress</strong>
                      <span>Status: {String(spreadsheetIteratorState.status ?? "unknown")}</span>
                      <span>
                        Row progress: {String(spreadsheetIteratorState.current_row_index ?? 0)} / {String(spreadsheetIteratorState.total_rows ?? 0)}
                      </span>
                      <span>Sheet: {String(spreadsheetIteratorState.sheet_name ?? "first sheet")}</span>
                      {spreadsheetLoopRegion ? (
                        <span>
                          Loop region members: {String(spreadsheetLoopRegion.member_node_ids?.length ?? 0)}
                          {spreadsheetLoopMemberLabels.length > 0 ? ` (${spreadsheetLoopMemberLabels.join(", ")})` : ""}
                        </span>
                      ) : null}
                    </div>
                  ) : null}
                  {spreadsheetPreviewError ? <div className="tool-details-modal-help">{spreadsheetPreviewError}</div> : null}
                  <div className="contract-card">
                    <strong>Parsed Preview</strong>
                    <span>
                      {spreadsheetPreview
                        ? `${spreadsheetPreview.row_count} row(s) parsed from ${spreadsheetPreview.file_format.toUpperCase()}`
                        : "Run a preview to inspect headers and sample rows before execution."}
                    </span>
                    <pre className="context-builder-preview">
                      {spreadsheetPreview
                        ? JSON.stringify(
                            {
                              headers: spreadsheetPreview.headers,
                              sheet_name: spreadsheetPreview.sheet_name,
                              row_count: spreadsheetPreview.row_count,
                              sample_rows: spreadsheetPreview.sample_rows,
                            },
                            null,
                            2,
                          )
                        : "Preview output will appear here."}
                    </pre>
                  </div>
                </>
              ) : isSupabaseTableRowsNode ? (
                <>
                  <div className="contract-card">
                    <strong>Supabase Table Rows</strong>
                    <span>Reads rows from a Supabase table in ascending cursor order and can either skip previously processed rows or replay the full result set.</span>
                    <span>Each row is emitted through the `loop-body` handle as `payload.row_data`, containing only the selected columns.</span>
                  </div>
                  <div className="inspector-meta">
                    <span>Schema: {String(selectedNode.config.schema ?? "public") || "public"}</span>
                    <span>Table: {String(selectedNode.config.table_name ?? "not set") || "not set"}</span>
                    <span>Cursor: {String(selectedNode.config.cursor_column ?? "not set") || "not set"}</span>
                    <span>Row id: {String(selectedNode.config.row_id_column ?? "id") || "id"}</span>
                    <span>Cached rows: {selectedNode.config.include_previously_processed_rows === true ? "included" : "skipped"}</span>
                    <span>Output handle: `loop-body`</span>
                  </div>
                  {spreadsheetIteratorState ? (
                    <div className="contract-card">
                      <strong>Iterator Progress</strong>
                      <span>Status: {String(spreadsheetIteratorState.status ?? "unknown")}</span>
                      <span>
                        Row progress: {String(spreadsheetIteratorState.current_row_index ?? 0)} / {String(spreadsheetIteratorState.total_rows ?? 0)}
                      </span>
                      <span>Cached cursor: {String(spreadsheetIteratorState.last_cached_cursor_value ?? "none")}</span>
                      {spreadsheetLoopRegion ? (
                        <span>
                          Loop region members: {String(spreadsheetLoopRegion.member_node_ids?.length ?? 0)}
                          {spreadsheetLoopMemberLabels.length > 0 ? ` (${spreadsheetLoopMemberLabels.join(", ")})` : ""}
                        </span>
                      ) : null}
                    </div>
                  ) : null}
                  {onOpenProviderDetails ? (
                    <button type="button" className="secondary-button" onClick={() => onOpenProviderDetails(selectedNode.id)}>
                      Open Provider Details
                    </button>
                  ) : null}
                </>
              ) : isLogicConditionsNode ? (
                <>
                  <div className="contract-card">
                    <strong>Logic Conditions</strong>
                    <span>Evaluates the incoming envelope and routes execution through the first matching named branch or `Else`.</span>
                    <span>Each branch owns a boolean rule group that can combine rules with `ALL` and `ANY` logic.</span>
                  </div>
                  <div className="contract-card">
                    <strong>Incoming Contract</strong>
                    <span>Resolved incoming contract: {logicIncomingContractLabel}</span>
                    <span>Accepted node contracts: {contract?.accepted_inputs.join(", ") ?? "message_envelope, tool_result_envelope, data_envelope"}</span>
                  </div>
                  <div className="contract-card">
                    <strong>Configured Branches</strong>
                    <span>{logicConditionConfig?.branches.length ?? 0} branch{logicConditionConfig?.branches.length === 1 ? "" : "es"} configured before `Else`.</span>
                    <span>Else handle: {String(logicConditionConfig?.else_output_handle_id ?? CONTROL_FLOW_ELSE_HANDLE_ID)}</span>
                    {(logicConditionConfig?.branches ?? []).map((branch, index) => (
                      <span key={branch.id}>
                        {branch.label.trim() || `Branch ${index + 1}`}: {summarizeLogicGroup(branch.root_group)}
                      </span>
                    ))}
                    {onOpenProviderDetails ? (
                      <button type="button" className="secondary-button" onClick={() => onOpenProviderDetails(selectedNode.id)}>
                        Open Condition Builder
                      </button>
                    ) : null}
                  </div>
                </>
              ) : isParallelSplitterNode ? (
                <>
                  <div className="contract-card">
                    <strong>Parallel Splitter</strong>
                    <span>Copies the incoming envelope to every connected downstream standard branch.</span>
                    <span>Use this node when you want one explicit fan-out point instead of giving ordinary nodes multiple outputs.</span>
                  </div>
                  <label>
                    Configured Handle Count
                    <input value={String(parallelSplitterConfiguredHandleCount)} readOnly />
                  </label>
                  <label>
                    Active Connections
                    <input value={String(parallelSplitterConnectionCount)} readOnly />
                  </label>
                  <div className="contract-card">
                    <strong>Handle State</strong>
                    <span>{parallelSplitterHandles.length} total visible handle{parallelSplitterHandles.length === 1 ? "" : "s"}.</span>
                    {parallelSplitterHandles.map((handle) => (
                      <span key={handle.id}>
                        {handle.label} {"->"} {handle.id}
                      </span>
                    ))}
                  </div>
                  <div className="contract-card">
                    <strong>Outgoing Edges</strong>
                    <span>{parallelSplitterOutgoingEdges.length} raw outgoing edge{parallelSplitterOutgoingEdges.length === 1 ? "" : "s"} from this splitter.</span>
                    {parallelSplitterOutgoingEdges.map((edge) => (
                      <span key={edge.id}>
                        {edge.id}: {edge.kind} {"->"} {edge.target_id} ({String(edge.source_handle_id ?? "null")})
                      </span>
                    ))}
                  </div>
                  <div className="contract-card">
                    <strong>Branching</strong>
                    <span>Every connected standard outgoing edge runs in parallel from the same input envelope.</span>
                    <span>Connect one edge per branch you want to launch.</span>
                  </div>
                </>
              ) : null}
            </>
          ) : selectedNode.kind === "data" ? (
            <>
              {isSpreadsheetRowNode ? (
                <>
                  <div className="contract-card">
                    <strong>Spreadsheet Rows</strong>
                    <span>Reads a CSV or XLSX file, maps each row to a header-keyed dictionary, and runs downstream nodes once per row in strict sequence.</span>
                    <span>Each row is emitted as `payload.row_data` with `row_index`, `row_number`, `sheet_name`, and `source_file` metadata.</span>
                  </div>
                  <label>
                    File Format
                    <select
                      value={String(selectedNode.config.file_format ?? "auto")}
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: { ...node.config, file_format: event.target.value, mode: "spreadsheet_rows" },
                          })),
                        )
                      }
                    >
                      <option value="auto">Auto Detect</option>
                      <option value="csv">CSV</option>
                      <option value="xlsx">Excel (.xlsx)</option>
                    </select>
                  </label>
                  <label>
                    Project File
                    <select
                      value={spreadsheetProjectFileSelectValue}
                      onChange={(event) => {
                        const nextFileId = event.target.value;
                        if (!nextFileId || nextFileId === "__manual__") {
                          return;
                        }
                        applySpreadsheetProjectFile(nextFileId);
                      }}
                    >
                      <option value="">Select a saved spreadsheet</option>
                      {hasManualSpreadsheetPath ? <option value="__manual__">Manual path</option> : null}
                      {spreadsheetProjectFiles.map((file) => (
                        <option key={file.file_id} value={file.file_id}>
                          {file.name}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label>
                    File Path
                    <input
                      value={String(selectedNode.config.file_path ?? "")}
                      placeholder="Project file path or graph env reference"
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              project_file_id: "",
                              project_file_name: "",
                              file_path: event.target.value,
                              mode: "spreadsheet_rows",
                            },
                          })),
                        )
                      }
                    />
                  </label>
                  <label>
                    Sheet Name
                    <input
                      value={String(selectedNode.config.sheet_name ?? "")}
                      placeholder="Leave blank to use the first sheet"
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: { ...node.config, sheet_name: event.target.value, mode: "spreadsheet_rows" },
                          })),
                        )
                      }
                    />
                  </label>
                  <div className="checkbox-grid">
                    <label>
                      Empty Row Policy
                      <select
                        value={String(selectedNode.config.empty_row_policy ?? "skip")}
                        onChange={(event) =>
                          onGraphChange(
                            updateNode(graph, selectedNode.id, (node) => ({
                              ...node,
                              config: { ...node.config, empty_row_policy: event.target.value, mode: "spreadsheet_rows" },
                            })),
                          )
                        }
                      >
                        <option value="skip">Skip empty rows</option>
                        <option value="include">Include empty rows</option>
                      </select>
                    </label>
                  </div>
                  <p className="node-help-text">
                    Row 1 is always treated as the header row. Each later row is emitted as one iteration using those
                    header titles as the parsed row keys.
                  </p>
                  <div className="context-builder-binding-actions">
                    <button
                      type="button"
                      className="secondary-button context-builder-inline-button"
                      onClick={() => void handleSpreadsheetPreview()}
                      disabled={isSpreadsheetPreviewLoading || spreadsheetResolvedFilePath.trim().length === 0}
                    >
                      {isSpreadsheetPreviewLoading ? "Loading Preview..." : "Preview Rows"}
                    </button>
                  </div>
                  <div className="inspector-meta">
                    <span>Resolved file path: {spreadsheetResolvedFilePath || "Enter a file path or graph env reference."}</span>
                    <span>Execution mode: sequential per-row loop through downstream nodes</span>
                    <span>Recommended shape for downstream prompts/tools: `payload.row_data` key-value pairs</span>
                  </div>
                  {spreadsheetIteratorState ? (
                    <div className="contract-card">
                      <strong>Iterator Progress</strong>
                      <span>Status: {String(spreadsheetIteratorState.status ?? "unknown")}</span>
                      <span>
                        Row progress: {String(spreadsheetIteratorState.current_row_index ?? 0)} / {String(spreadsheetIteratorState.total_rows ?? 0)}
                      </span>
                      <span>Sheet: {String(spreadsheetIteratorState.sheet_name ?? "first sheet")}</span>
                      {spreadsheetLoopRegion ? (
                        <span>
                          Loop region members: {String(spreadsheetLoopRegion.member_node_ids?.length ?? 0)}
                          {spreadsheetLoopMemberLabels.length > 0 ? ` (${spreadsheetLoopMemberLabels.join(", ")})` : ""}
                        </span>
                      ) : null}
                    </div>
                  ) : null}
                  {spreadsheetPreviewError ? <div className="tool-details-modal-help">{spreadsheetPreviewError}</div> : null}
                  <div className="contract-card">
                    <strong>Parsed Preview</strong>
                    <span>
                      {spreadsheetPreview
                        ? `${spreadsheetPreview.row_count} row(s) parsed from ${spreadsheetPreview.file_format.toUpperCase()}`
                        : "Run a preview to inspect headers and sample rows before execution."}
                    </span>
                    <pre className="context-builder-preview">
                      {spreadsheetPreview
                        ? JSON.stringify(
                            {
                              headers: spreadsheetPreview.headers,
                              sheet_name: spreadsheetPreview.sheet_name,
                              row_count: spreadsheetPreview.row_count,
                              sample_rows: spreadsheetPreview.sample_rows,
                            },
                            null,
                            2,
                          )
                        : "Preview output will appear here."}
                    </pre>
                  </div>
                </>
              ) : isWriteTextFileNode ? (
                <>
                  <div className="contract-card">
                    <strong>Write Text File</strong>
                    <span>Writes the incoming payload into a sandboxed file for this run and selected agent.</span>
                    <span>Only relative paths inside the workspace are allowed.</span>
                  </div>
                  <label>
                    Relative File Path
                    <input
                      value={String(selectedNode.config.relative_path ?? "response.txt")}
                      placeholder="outputs/response.txt"
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              mode: "write_text_file",
                              relative_path: event.target.value,
                            },
                          })),
                        )
                      }
                    />
                  </label>
                  <label>
                    When File Exists
                    <select
                      value={String(selectedNode.config.exists_behavior ?? "")}
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => {
                            const nextConfig: Record<string, unknown> = { ...node.config, mode: "write_text_file" };
                            if (event.target.value) {
                              nextConfig.exists_behavior = event.target.value;
                            } else {
                              delete nextConfig.exists_behavior;
                            }
                            return {
                              ...node,
                              config: nextConfig,
                            };
                          }),
                        )
                      }
                    >
                      <option value="">Auto (overwrite normally, append in loops)</option>
                      <option value="overwrite">Overwrite</option>
                      <option value="append">Append</option>
                      <option value="error">Error</option>
                    </select>
                  </label>
                  <label className="checkbox-option">
                    <input
                      type="checkbox"
                      checked={selectedNode.config.append_newline !== false}
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              mode: "write_text_file",
                              append_newline: event.target.checked,
                            },
                          })),
                        )
                      }
                    />
                    <span>
                      Insert Newline Before Appended Content
                      <small>When appending to a non-empty file, adds a newline separator unless the file already ends with one.</small>
                    </span>
                  </label>
                  <div className="inspector-meta">
                    <span>Sandbox: per-run, per-agent workspace under `.graph-agent/`</span>
                    <span>Default file: {String(selectedNode.config.relative_path ?? "response.txt") || "response.txt"}</span>
                    <span>Input source: latest incoming payload unless a binding overrides it</span>
                    <span>Auto mode: overwrites outside loops and appends inside iterator executions</span>
                  </div>
                </>
              ) : isStructuredPayloadBuilderNode ? (
                <>
                  <div className="contract-card">
                    <strong>Structured Payload Builder</strong>
                    <span>Starts from a JSON object template and auto-fills only the missing fields from the incoming payload.</span>
                    <span>Use it when you want to hand-write a few values and let the node recursively discover the rest by field name.</span>
                  </div>
                  <div className="context-builder-binding-actions">
                    <button
                      type="button"
                      className="secondary-button context-builder-inline-button"
                      onClick={() => setIsStructuredPayloadBuilderLearnMoreOpen(true)}
                    >
                      Learn More
                    </button>
                  </div>
                  <div className="contract-card">
                    <strong>Dictionary Entries</strong>
                    <span>Add one key per row. Leave the value blank to auto-fill it from the incoming payload.</span>
                    <span>Special values: use <code>null</code>, <code>{`{}`}</code>, or <code>[]</code> if you want those missing shapes to be auto-filled.</span>
                  </div>
                  <div className="context-builder-binding-actions">
                    <button
                      type="button"
                      className="secondary-button context-builder-inline-button"
                      onClick={() =>
                        updateStructuredPayloadTemplateEntries([
                          ...structuredPayloadTemplateEntries,
                          {
                            id: `template-entry-new-${structuredPayloadTemplateEntries.length + 1}`,
                            key: "",
                            value: "",
                          },
                        ])
                      }
                    >
                      Add Entry
                    </button>
                  </div>
                  {structuredPayloadTemplateEntries.length > 0 ? (
                    <div className="checkbox-grid">
                      {structuredPayloadTemplateEntries.map((entry, index) => (
                        <div key={entry.id} className="context-builder-binding-card">
                          <div className="context-builder-binding-header">
                            <div>
                              <strong>Entry {index + 1}</strong>
                              <small>Blank value means auto-fill</small>
                            </div>
                            <div className="context-builder-binding-actions">
                              <button
                                type="button"
                                className="secondary-button context-builder-inline-button"
                                onClick={() =>
                                  updateStructuredPayloadTemplateEntries(
                                    structuredPayloadTemplateEntries.filter((candidate) => candidate.id !== entry.id),
                                  )
                                }
                              >
                                Remove
                              </button>
                            </div>
                          </div>
                          <label>
                            Key
                            <input
                              value={entry.key}
                              placeholder="email"
                              onChange={(event) =>
                                updateStructuredPayloadTemplateEntries(
                                  structuredPayloadTemplateEntries.map((candidate) =>
                                    candidate.id === entry.id
                                      ? { ...candidate, key: event.target.value }
                                      : candidate,
                                  ),
                                )
                              }
                            />
                          </label>
                          <label>
                            Value
                            <input
                              value={entry.value}
                              placeholder="Leave blank to auto-fill"
                              onChange={(event) =>
                                updateStructuredPayloadTemplateEntries(
                                  structuredPayloadTemplateEntries.map((candidate) =>
                                    candidate.id === entry.id
                                      ? { ...candidate, value: event.target.value }
                                      : candidate,
                                  ),
                                )
                              }
                            />
                          </label>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div className="tool-details-modal-help">
                      No entries yet. Add a dictionary entry to start defining the payload shape.
                    </div>
                  )}
                  <label className="checkbox-option">
                    <input
                      type="checkbox"
                      checked={Boolean(selectedNode.config.case_sensitive ?? false)}
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              mode: "structured_payload_builder",
                              case_sensitive: event.target.checked,
                            },
                          })),
                        )
                      }
                    />
                    <span>
                      Case Sensitive
                      <small>Match field names using exact case instead of lowercase-insensitive matching.</small>
                    </span>
                  </label>
                  <label>
                    Max Matches Per Field
                    <input
                      type="number"
                      value={String(selectedNode.config.max_matches_per_field ?? 25)}
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              mode: "structured_payload_builder",
                              max_matches_per_field: event.target.value ? Number(event.target.value) : 25,
                            },
                          })),
                        )
                      }
                    />
                  </label>
                  <div className="inspector-meta">
                    <span>Write the output shape you want as JSON, then leave any field blank, null, an empty object, or an empty array to let this node auto-fill it from upstream data.</span>
                    <span>Filled fields preserve any explicit values you wrote in the template.</span>
                    <span>Nested objects are searched recursively, with parent keys used as context when possible.</span>
                  </div>
                </>
              ) : isApolloEmailLookupNode ? (
                <>
                  <div className="contract-card">
                    <strong>Apollo Email Lookup</strong>
                    <span>Performs one Apollo `people/match` lookup, returns the full Apollo response payload, and reuses a shared cache across runs.</span>
                    <span>Use direct identifiers when possible to minimize credits and improve cache hit quality.</span>
                  </div>
                  <div className="tool-details-modal-help">
                    Apollo credentials come from the Environment section's <code>APOLLO_API_KEY</code> field automatically.
                  </div>
                  <label>
                    Name
                    <input
                      value={String(selectedNode.config.name ?? "")}
                      placeholder="Taylor Doe"
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              mode: "apollo_email_lookup",
                              name: event.target.value,
                            },
                          })),
                        )
                      }
                    />
                  </label>
                  <label>
                    Domain
                    <input
                      value={String(selectedNode.config.domain ?? "")}
                      placeholder="example.com"
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              mode: "apollo_email_lookup",
                              domain: event.target.value,
                            },
                          })),
                        )
                      }
                    />
                  </label>
                  <label>
                    Organization Name
                    <input
                      value={String(selectedNode.config.organization_name ?? "")}
                      placeholder="Example Co"
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              mode: "apollo_email_lookup",
                              organization_name: event.target.value,
                            },
                          })),
                        )
                      }
                    />
                  </label>
                  <label>
                    First Name
                    <input
                      value={String(selectedNode.config.first_name ?? "")}
                      placeholder="Taylor"
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              mode: "apollo_email_lookup",
                              first_name: event.target.value,
                            },
                          })),
                        )
                      }
                    />
                  </label>
                  <label>
                    Last Name
                    <input
                      value={String(selectedNode.config.last_name ?? "")}
                      placeholder="Doe"
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              mode: "apollo_email_lookup",
                              last_name: event.target.value,
                            },
                          })),
                        )
                      }
                    />
                  </label>
                  <label>
                    LinkedIn URL
                    <input
                      value={String(selectedNode.config.linkedin_url ?? "")}
                      placeholder="https://www.linkedin.com/in/example/"
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              mode: "apollo_email_lookup",
                              linkedin_url: event.target.value,
                            },
                          })),
                        )
                      }
                    />
                  </label>
                  <label>
                    Email
                    <input
                      value={String(selectedNode.config.email ?? "")}
                      placeholder="person@example.com"
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              mode: "apollo_email_lookup",
                              email: event.target.value,
                            },
                          })),
                        )
                      }
                    />
                  </label>
                  <label>
                    Twitter URL
                    <input
                      value={String(selectedNode.config.twitter_url ?? "")}
                      placeholder="https://x.com/example"
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              mode: "apollo_email_lookup",
                              twitter_url: event.target.value,
                            },
                          })),
                        )
                      }
                    />
                  </label>
                  <label>
                    Optional Conversation
                    <textarea
                      rows={4}
                      value={String(selectedNode.config.conversation ?? "")}
                      placeholder="Optional context or operator notes for this lookup"
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              mode: "apollo_email_lookup",
                              conversation: event.target.value,
                            },
                          })),
                        )
                      }
                    />
                  </label>
                  <div className="checkbox-grid">
                    <label className="checkbox-option">
                      <input
                        type="checkbox"
                        checked={Boolean(selectedNode.config.reveal_personal_emails ?? false)}
                        onChange={(event) =>
                          onGraphChange(
                            updateNode(graph, selectedNode.id, (node) => ({
                              ...node,
                              config: {
                                ...node.config,
                                mode: "apollo_email_lookup",
                                reveal_personal_emails: event.target.checked,
                              },
                            })),
                          )
                        }
                      />
                      <span>
                        Reveal Personal Emails
                        <small>Leave off by default to minimize credit usage and keep lookups work-email focused.</small>
                      </span>
                    </label>
                    <label className="checkbox-option">
                      <input
                        type="checkbox"
                        checked={Boolean(selectedNode.config.use_cache ?? true)}
                        onChange={(event) =>
                          onGraphChange(
                            updateNode(graph, selectedNode.id, (node) => ({
                              ...node,
                              config: {
                                ...node.config,
                                mode: "apollo_email_lookup",
                                use_cache: event.target.checked,
                              },
                            })),
                          )
                        }
                      />
                      <span>
                        Use Shared Cache
                        <small>Read and write the reusable `.graph-agent/cache/apollo-email/` entry for this normalized lookup.</small>
                      </span>
                    </label>
                    <label className="checkbox-option">
                      <input
                        type="checkbox"
                        checked={Boolean(selectedNode.config.force_refresh ?? false)}
                        onChange={(event) =>
                          onGraphChange(
                            updateNode(graph, selectedNode.id, (node) => ({
                              ...node,
                              config: {
                                ...node.config,
                                mode: "apollo_email_lookup",
                                force_refresh: event.target.checked,
                              },
                            })),
                          )
                        }
                      />
                      <span>
                        Force Refresh
                        <small>Bypass any existing shared cache entry and run one fresh Apollo lookup.</small>
                      </span>
                    </label>
                  </div>
                  <label>
                    Workspace Cache Path Template
                    <input
                      value={String(selectedNode.config.workspace_cache_path_template ?? "cache/apollo-email/{cache_key}.json")}
                      placeholder="cache/apollo-email/{cache_key}.json"
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              mode: "apollo_email_lookup",
                              workspace_cache_path_template: event.target.value,
                            },
                          })),
                        )
                      }
                    />
                  </label>
                  <div className="inspector-meta">
                    <span>Parameters: configure direct identifiers or person-plus-organization fields on the node, or supply them from upstream payloads.</span>
                    <span>Optional conversation: stored on the node for operator context and visible in the details UI.</span>
                    <span>Workspace mirror: {String(selectedNode.config.workspace_cache_path_template ?? "cache/apollo-email/{cache_key}.json")}</span>
                    <span>Shared cache: sibling of the configured `.graph-agent/runs` root under `.graph-agent/cache/apollo-email/`</span>
                  </div>
                </>
              ) : isLinkedInProfileFetchNode ? (
                <>
                  <div className="contract-card">
                    <strong>LinkedIn Profile Fetch</strong>
                    <span>Fetches a LinkedIn profile URL, parses it into structured JSON, and mirrors the parsed result into the agent workspace.</span>
                    <span>Shared cache entries live outside the per-run workspace so repeat profile URLs only scrape once unless you force a refresh.</span>
                  </div>
                  <label>
                    URL Field
                    <input
                      value={String(selectedNode.config.url_field ?? "url")}
                      placeholder="url"
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              mode: "linkedin_profile_fetch",
                              url_field: event.target.value,
                            },
                          })),
                        )
                      }
                    />
                  </label>
                  <label>
                    LinkedIn Data Directory
                    <input
                      value={String(selectedNode.config.linkedin_data_dir ?? "")}
                      placeholder="/Users/.../Desktop/Linkedin Data"
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              mode: "linkedin_profile_fetch",
                              linkedin_data_dir: event.target.value,
                            },
                          })),
                        )
                      }
                    />
                  </label>
                  <label>
                    Session State Path
                    <input
                      value={String(selectedNode.config.session_state_path ?? "")}
                      placeholder="Optional override for the LinkedIn storage-state JSON"
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              mode: "linkedin_profile_fetch",
                              session_state_path: event.target.value,
                            },
                          })),
                        )
                      }
                    />
                  </label>
                  <div className="checkbox-grid">
                    <label className="checkbox-option">
                      <input
                        type="checkbox"
                        checked={Boolean(selectedNode.config.use_cache ?? true)}
                        onChange={(event) =>
                          onGraphChange(
                            updateNode(graph, selectedNode.id, (node) => ({
                              ...node,
                              config: {
                                ...node.config,
                                mode: "linkedin_profile_fetch",
                                use_cache: event.target.checked,
                              },
                            })),
                          )
                        }
                      />
                      <span>
                        Use Shared Cache
                        <small>Read and write the reusable `.graph-agent/cache/linkedin/` entry for this normalized URL.</small>
                      </span>
                    </label>
                    <label className="checkbox-option">
                      <input
                        type="checkbox"
                        checked={Boolean(selectedNode.config.force_refresh ?? false)}
                        onChange={(event) =>
                          onGraphChange(
                            updateNode(graph, selectedNode.id, (node) => ({
                              ...node,
                              config: {
                                ...node.config,
                                mode: "linkedin_profile_fetch",
                                force_refresh: event.target.checked,
                              },
                            })),
                          )
                        }
                      />
                      <span>
                        Force Refresh
                        <small>Bypass any existing shared cache entry and scrape the profile again.</small>
                      </span>
                    </label>
                    <label className="checkbox-option">
                      <input
                        type="checkbox"
                        checked={Boolean(selectedNode.config.headless ?? false)}
                        onChange={(event) =>
                          onGraphChange(
                            updateNode(graph, selectedNode.id, (node) => ({
                              ...node,
                              config: {
                                ...node.config,
                                mode: "linkedin_profile_fetch",
                                headless: event.target.checked,
                              },
                            })),
                          )
                        }
                      />
                      <span>
                        Headless Browser
                        <small>Turn this off if the run may need a visible browser window for LinkedIn login.</small>
                      </span>
                    </label>
                  </div>
                  <label>
                    Navigation Timeout (ms)
                    <input
                      type="number"
                      value={String(selectedNode.config.navigation_timeout_ms ?? 45000)}
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => {
                            const nextConfig: Record<string, unknown> = { ...node.config, mode: "linkedin_profile_fetch" };
                            if (event.target.value) {
                              nextConfig.navigation_timeout_ms = Number(event.target.value);
                            } else {
                              delete nextConfig.navigation_timeout_ms;
                            }
                            return {
                              ...node,
                              config: nextConfig,
                            };
                          }),
                        )
                      }
                    />
                  </label>
                  <label>
                    Page Settle Delay (ms)
                    <input
                      type="number"
                      value={String(selectedNode.config.page_settle_ms ?? 3000)}
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => {
                            const nextConfig: Record<string, unknown> = { ...node.config, mode: "linkedin_profile_fetch" };
                            if (event.target.value) {
                              nextConfig.page_settle_ms = Number(event.target.value);
                            } else {
                              delete nextConfig.page_settle_ms;
                            }
                            return {
                              ...node,
                              config: nextConfig,
                            };
                          }),
                        )
                      }
                    />
                  </label>
                  <label>
                    Workspace Cache Path Template
                    <input
                      value={String(selectedNode.config.workspace_cache_path_template ?? "cache/linkedin/{cache_key}.json")}
                      placeholder="cache/linkedin/{cache_key}.json"
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              mode: "linkedin_profile_fetch",
                              workspace_cache_path_template: event.target.value,
                            },
                          })),
                        )
                      }
                    />
                  </label>
                  <div className="inspector-meta">
                    <span>
                      Input source: raw string payload or the <code>{String(selectedNode.config.url_field ?? "url")}</code> field from an object payload
                    </span>
                    <span>
                      Workspace mirror: {String(selectedNode.config.workspace_cache_path_template ?? "cache/linkedin/{cache_key}.json")}
                    </span>
                    <span>Shared cache: sibling of the configured `.graph-agent/runs` root under `.graph-agent/cache/linkedin/`</span>
                    <span>Only successful profile parses are written to the shared cache</span>
                  </div>
                </>
              ) : isRuntimeNormalizerNode ? (
                <>
                  <div className="contract-card">
                    <strong>Payload Field Extractor</strong>
                    <span>Searches the incoming payload for one or more named fields and forwards the matched value or values.</span>
                    <span>Use it when the payload structure is unknown but the user knows the variable names they need.</span>
                  </div>
                  <label>
                    Field Names
                    <div className="runtime-field-list">
                      {runtimeNormalizerFieldNames.map((fieldName, index) => (
                        <div key={`runtime-field-${index}`} className="runtime-field-list-row">
                          <input
                            value={fieldName}
                            placeholder={index === 0 ? "url" : "headline"}
                            onChange={(event) => {
                              const nextFieldNames = runtimeNormalizerFieldNames.map((candidate, candidateIndex) =>
                                candidateIndex === index ? event.target.value : candidate,
                              );
                              onGraphChange(
                                updateNode(graph, selectedNode.id, (node) => ({
                                  ...node,
                                  config: {
                                    ...node.config,
                                    mode: "runtime_normalizer",
                                    field_names: nextFieldNames,
                                    field_name: serializeLegacyConfigStringList(nextFieldNames),
                                  },
                                })),
                              );
                            }}
                          />
                          <button
                            type="button"
                            className="secondary-button runtime-field-list-button"
                            onClick={() => {
                              const nextFieldNames = runtimeNormalizerFieldNames.filter((_, candidateIndex) => candidateIndex !== index);
                              onGraphChange(
                                updateNode(graph, selectedNode.id, (node) => ({
                                  ...node,
                                  config: {
                                    ...node.config,
                                    mode: "runtime_normalizer",
                                    field_names: nextFieldNames,
                                    field_name: serializeLegacyConfigStringList(nextFieldNames),
                                  },
                                })),
                              );
                            }}
                            disabled={runtimeNormalizerFieldNames.length <= 1}
                          >
                            Remove
                          </button>
                        </div>
                      ))}
                      <button
                        type="button"
                        className="secondary-button runtime-field-list-add"
                        onClick={() => {
                          const nextFieldNames = [...runtimeNormalizerFieldNames, ""];
                          onGraphChange(
                            updateNode(graph, selectedNode.id, (node) => ({
                              ...node,
                              config: {
                                ...node.config,
                                mode: "runtime_normalizer",
                                field_names: nextFieldNames,
                                field_name: serializeLegacyConfigStringList(nextFieldNames),
                              },
                            })),
                          );
                        }}
                      >
                        Add Field
                      </button>
                    </div>
                    <small>Add each search field as its own row so order and edits stay explicit.</small>
                  </label>
                  <label>
                    Fallback Field Names
                    <textarea
                      rows={4}
                      value={
                        Array.isArray(selectedNode.config.fallback_field_names)
                          ? selectedNode.config.fallback_field_names.map((value) => String(value)).join("\n")
                          : String(selectedNode.config.fallback_field_names ?? "")
                      }
                      placeholder={"profile_url\nlinkedin_url"}
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              mode: "runtime_normalizer",
                              fallback_field_names: event.target.value,
                            },
                          })),
                        )
                      }
                    />
                  </label>
                  <label>
                    Preferred Path
                    <input
                      value={String(selectedNode.config.preferred_path ?? "")}
                      placeholder="data.user.url"
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              mode: "runtime_normalizer",
                              preferred_path: event.target.value,
                            },
                          })),
                        )
                      }
                    />
                  </label>
                  <label className="checkbox-option">
                    <input
                      type="checkbox"
                      checked={Boolean(selectedNode.config.case_sensitive ?? false)}
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              mode: "runtime_normalizer",
                              case_sensitive: event.target.checked,
                            },
                          })),
                        )
                      }
                    />
                    <span>
                      Case Sensitive
                      <small>Match field names with exact case instead of lowercase-insensitive matching.</small>
                    </span>
                  </label>
                  <label>
                    Max Matches
                    <input
                      type="number"
                      value={String(selectedNode.config.max_matches ?? 25)}
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              mode: "runtime_normalizer",
                              max_matches: event.target.value ? Number(event.target.value) : 25,
                            },
                          })),
                        )
                      }
                    />
                  </label>
                  <div className="inspector-meta">
                    <span>Output payload: a single matched value for one field, or an object keyed by field name when multiple fields are requested</span>
                    <span>Metadata includes matched paths and missing fields; artifacts include all discovered matches</span>
                    <span>Preferred path is tried first for single-field extraction, then the node falls back to recursive key search</span>
                  </div>
                </>
              ) : selectedNode.provider_id === "core.data_display" ? (
                <div className="inspector-meta">
                  <span>Display mode: visualizer envelope inspection</span>
                  <span>Behavior: passes the original payload through unchanged</span>
                  <span>Visualizer: shows the full incoming envelope under node output details</span>
                  <span>Outgoing edge: optional when you only want to inspect data here</span>
                </div>
              ) : isPromptBlockDataNode ? (
                <>
                  <div className="contract-card">
                    <strong>Prompt Block</strong>
                    <span>Creates one binding-only prompt message that can feed a Context Builder or bind directly into a model.</span>
                    <span>Role determines whether the block is treated as a system, user, or assistant message downstream.</span>
                  </div>
                  <div className="context-builder-placeholder-bar">
                    <button
                      type="button"
                      className="secondary-button context-builder-inline-button"
                      onClick={() =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: {
                              ...node.config,
                              content: String(
                                node.config.content ??
                                  PROMPT_BLOCK_STARTERS[String(node.config.role ?? "user")] ??
                                  PROMPT_BLOCK_STARTERS.user,
                              ).trim().length > 0
                                ? node.config.content
                                : PROMPT_BLOCK_STARTERS[String(node.config.role ?? "user")] ?? PROMPT_BLOCK_STARTERS.user,
                              mode: "prompt_block",
                            },
                          })),
                        )
                      }
                    >
                      Insert Starter
                    </button>
                    {promptBlockAvailableVariables.map((token) => (
                      <button
                        key={token}
                        type="button"
                        className="secondary-button context-builder-token-button"
                        onClick={() =>
                          onGraphChange(
                            updateNode(graph, selectedNode.id, (node) => ({
                              ...node,
                              config: {
                                ...node.config,
                                content: insertTokenAtEnd(String(node.config.content ?? ""), `{${token}}`),
                                mode: "prompt_block",
                              },
                            })),
                          )
                        }
                      >
                        {`{${token}}`}
                      </button>
                    ))}
                  </div>
                  <label>
                    Message Role
                    <select
                      value={String(selectedNode.config.role ?? "user")}
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: { ...node.config, role: event.target.value, mode: "prompt_block" },
                          })),
                        )
                      }
                    >
                      <option value="system">system</option>
                      <option value="user">user</option>
                      <option value="assistant">assistant</option>
                    </select>
                  </label>
                  <label>
                    Message Name
                    <input
                      value={String(selectedNode.config.name ?? "")}
                      placeholder="Optional label for the message block"
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: { ...node.config, name: event.target.value, mode: "prompt_block" },
                          })),
                        )
                      }
                    />
                  </label>
                  <label>
                    Message Content
                    <textarea
                      rows={6}
                      value={String(selectedNode.config.content ?? "")}
                      placeholder="Enter the message content to inject into downstream prompt assembly."
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: { ...node.config, content: event.target.value, mode: "prompt_block" },
                          })),
                        )
                      }
                    />
                  </label>
                  <div className="inspector-meta">
                    <span>Draft: {getPromptBlockPreview(selectedNode) || "Add content to preview this prompt block."}</span>
                    <span>Binding mode: source-only prompt block</span>
                    <span>Available variables: {promptBlockAvailableVariables.length > 0 ? promptBlockAvailableVariables.join(", ") : "None"}</span>
                  </div>
                  <div className="contract-card">
                    <strong>{runState ? "Rendered Preview" : "Template Preview"}</strong>
                    <pre className="context-builder-preview">{promptBlockRenderedPreview || "Prompt block output will appear here."}</pre>
                    <span>
                      {runState
                        ? "Preview uses the latest run input and graph variables."
                        : "Preview shows the prompt template structure until the graph has run."}
                    </span>
                  </div>
                </>
              ) : selectedNode.provider_id === CONTEXT_BUILDER_PROVIDER_ID ? (
                <>
                  <div className="contract-card">
                    <strong>Context Builder</strong>
                    <span>Connect any number of upstream nodes, name each section header, and compose one structured context payload.</span>
                    <span>
                      Connected inputs: {contextBuilderBindings.length > 0 ? String(contextBuilderBindings.length) : "None yet"}
                    </span>
                  </div>
                  <div className="checkbox-grid">
                    <strong>Connected Inputs</strong>
                    {contextBuilderBindings.length > 0 ? (
                      contextBuilderBindings.map((binding, index) => {
                        const sourcePreview = getContextBuilderSourcePreviewFromGraph(graph, runState, binding.sourceNodeId);
                        return (
                          <div key={binding.sourceNodeId} className="context-builder-binding-card">
                            <div className="context-builder-binding-header">
                              <div>
                                <strong>{binding.sourceLabel}</strong>
                                <small>{binding.sourceNodeId}</small>
                              </div>
                              <div className="context-builder-binding-actions">
                                <button
                                  type="button"
                                  className="secondary-button context-builder-inline-button"
                                  disabled={index === 0}
                                  onClick={() => {
                                    if (index === 0) {
                                      return;
                                    }
                                    const nextBindings = [...contextBuilderBindings];
                                    [nextBindings[index - 1], nextBindings[index]] = [nextBindings[index], nextBindings[index - 1]];
                                    updateContextBuilderBindings(nextBindings);
                                  }}
                                >
                                  Up
                                </button>
                                <button
                                  type="button"
                                  className="secondary-button context-builder-inline-button"
                                  disabled={index === contextBuilderBindings.length - 1}
                                  onClick={() => {
                                    if (index === contextBuilderBindings.length - 1) {
                                      return;
                                    }
                                    const nextBindings = [...contextBuilderBindings];
                                    [nextBindings[index], nextBindings[index + 1]] = [nextBindings[index + 1], nextBindings[index]];
                                    updateContextBuilderBindings(nextBindings);
                                  }}
                                >
                                  Down
                                </button>
                              </div>
                            </div>
                            <label>
                              Header
                              <input
                                value={binding.rawHeader}
                                placeholder={binding.sourceLabel}
                                onChange={(event) => {
                                  const nextBindings = contextBuilderBindings.map((candidate) =>
                                    candidate.sourceNodeId === binding.sourceNodeId
                                      ? {
                                          ...candidate,
                                          rawHeader: event.target.value,
                                          header: normalizeContextBuilderHeader(event.target.value, candidate.sourceLabel),
                                        }
                                      : candidate,
                                  );
                                  updateContextBuilderBindings(nextBindings);
                                }}
                              />
                            </label>
                            <label>
                              Placeholder
                              <input
                                value={binding.placeholder}
                                onChange={(event) => {
                                  const nextBindings = contextBuilderBindings.map((candidate) =>
                                    candidate.sourceNodeId === binding.sourceNodeId
                                      ? {
                                          ...candidate,
                                          placeholder: slugifyContextBuilderPlaceholder(
                                            event.target.value,
                                            candidate.sourceLabel,
                                          ),
                                          autoGenerated: false,
                                        }
                                      : candidate,
                                  );
                                  updateContextBuilderBindings(nextBindings);
                                }}
                              />
                            </label>
                            <div className="inspector-meta">
                              <span>Output header: {binding.header}</span>
                              <span>Token: {`{${binding.placeholder}}`}</span>
                              <span>{binding.autoGenerated ? "Auto-generated from the source label" : "Custom placeholder"}</span>
                              <span>
                                Preview: {sourcePreview !== null && sourcePreview.length > 0 ? sourcePreview : "Run the graph to preview this source."}
                              </span>
                            </div>
                          </div>
                        );
                      })
                    ) : (
                      <p className="inspector-hint">Connect upstream nodes to start building named placeholders.</p>
                    )}
                  </div>
                  <div className="context-builder-placeholder-bar">
                    {contextBuilderBindings.map((binding) => (
                      <button
                        key={binding.sourceNodeId}
                        type="button"
                        className="secondary-button context-builder-token-button"
                        onClick={() =>
                          updateContextBuilderTemplate(
                            contextBuilderTemplate.trim().length > 0
                              ? `${contextBuilderTemplate}${contextBuilderTemplate.endsWith("\n") ? "" : "\n"}{${binding.placeholder}}`
                              : `{${binding.placeholder}}`,
                          )
                        }
                      >
                        {`{${binding.placeholder}}`}
                      </button>
                    ))}
                    <button
                      type="button"
                      className="secondary-button context-builder-token-button"
                      onClick={() =>
                        updateContextBuilderTemplate(
                          contextBuilderTemplate.trim().length > 0
                            ? `${contextBuilderTemplate}${contextBuilderTemplate.endsWith("\n") ? "" : "\n"}{documents}`
                            : "{documents}",
                        )
                      }
                    >
                      {"{documents}"}
                    </button>
                    {contextBuilderBindings.length > 0 ? (
                      <button
                        type="button"
                        className="secondary-button context-builder-inline-button"
                        onClick={() => updateContextBuilderTemplate(generatedContextBuilderTemplate)}
                      >
                        Regenerate Template
                      </button>
                    ) : null}
                  </div>
                  <label>
                    Prompt Template
                    <textarea
                      rows={8}
                      value={contextBuilderTemplate}
                      placeholder="Connect upstream nodes to insert placeholders here."
                      onChange={(event) => updateContextBuilderTemplate(event.target.value)}
                    />
                  </label>
                  <div className="inspector-meta">
                    <span>
                      Available variables:{" "}
                      {contextBuilderAvailableVariables.size > 0
                        ? [...contextBuilderAvailableVariables].join(", ")
                        : "Connect sources to create placeholders."}
                    </span>
                    {contextBuilderDuplicatePlaceholders.length > 0 ? (
                      <span>Duplicate placeholders: {uniqueStrings(contextBuilderDuplicatePlaceholders).join(", ")}</span>
                    ) : null}
                    {contextBuilderInvalidPlaceholders.length > 0 ? (
                      <span>Invalid placeholders: {uniqueStrings(contextBuilderInvalidPlaceholders).join(", ")}</span>
                    ) : null}
                    {contextBuilderUnmatchedTokens.length > 0 ? (
                      <span>Template tokens without a source: {uniqueStrings(contextBuilderUnmatchedTokens).join(", ")}</span>
                    ) : null}
                  </div>
                  <div className="contract-card">
                    <div className="context-builder-preview-header">
                      <strong>{contextBuilderHasPreviewData ? "Rendered Preview" : "Template Preview"}</strong>
                      <button
                        className="context-builder-preview-toggle"
                        type="button"
                        onClick={() => setContextBuilderPreviewFormatted((v) => !v)}
                        title={contextBuilderPreviewFormatted ? "Switch to raw text" : "Switch to formatted view"}
                      >
                        {contextBuilderPreviewFormatted ? "Raw" : "Formatted"}
                      </button>
                    </div>
                    {contextBuilderRenderedPreview ? (
                      contextBuilderPreviewFormatted && !contextBuilderUsesCustomTemplate ? (
                        <div className="context-builder-preview-block-list">
                          {contextBuilderPreviewBlocks(contextBuilderBindings, contextBuilderPreviewVariables).map((block) => (
                            <section key={block.sourceNodeId} className="context-builder-preview-block">
                              <div className="context-builder-preview-block-header">{block.header}</div>
                              <pre className="context-builder-preview-block-body">
                                {block.body || "This block is waiting for runtime data."}
                              </pre>
                            </section>
                          ))}
                        </div>
                      ) : (
                        <pre className="context-builder-preview">
                          {contextBuilderPreviewFormatted
                            ? contextBuilderRenderedPreview.replace(/\\n/g, "\n").replace(/\\t/g, "\t")
                            : contextBuilderRenderedPreview}
                        </pre>
                      )
                    ) : (
                      <pre className="context-builder-preview">Template output will appear here.</pre>
                    )}
                    <span>
                      {contextBuilderHasPreviewData
                        ? "Preview uses the latest run outputs from connected nodes."
                        : "Preview shows template structure until the graph has run."}
                    </span>
                  </div>
                </>
              ) : (
                <>
                  <label>
                    Data Mode
                    <select
                      value={String(selectedNode.config.mode ?? "passthrough")}
                      onChange={(event) =>
                        onGraphChange(
                          updateNode(graph, selectedNode.id, (node) => ({
                            ...node,
                            config: { ...node.config, mode: event.target.value },
                          })),
                        )
                      }
                    >
                      <option value="passthrough">passthrough</option>
                      <option value="template">template</option>
                    </select>
                  </label>
                  {String(selectedNode.config.mode ?? "passthrough") === "template" ? (
                    <label>
                      Template
                      <textarea
                        rows={4}
                        value={String(selectedNode.config.template ?? "{input_payload}")}
                        onChange={(event) =>
                          onGraphChange(
                            updateNode(graph, selectedNode.id, (node) => ({
                              ...node,
                              config: { ...node.config, template: event.target.value },
                            })),
                          )
                        }
                      />
                    </label>
                  ) : null}
                </>
              )}
            </>
          ) : null}
          {onSaveNode ? (
            <div className="inspector-save-section">
              <button
                type="button"
                className="secondary-button inspector-save-button"
                onClick={() => onSaveNode(selectedNode)}
              >
                <svg viewBox="0 0 24 24" aria-hidden="true">
                  <path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2Z" />
                  <path d="M17 21v-8H7v8M7 3v5h8" />
                </svg>
                Save to Library
              </button>
              <span className="inspector-save-hint">Save this node's configuration for reuse in the Add menu.</span>
            </div>
          ) : null}
          </div>
        </section>
        {isStructuredPayloadBuilderLearnMoreOpen && isStructuredPayloadBuilderNode ? (
          <StructuredPayloadBuilderLearnMoreModal
            graph={graph}
            node={selectedNode}
            onClose={() => setIsStructuredPayloadBuilderLearnMoreOpen(false)}
          />
        ) : null}
      </>
    );
  }

  if (selectedEdge) {
    return (
      <section className="panel inspector-panel">
        <div className="panel-header">
          <h2>Edge Inspector</h2>
          <p>Configure routing and condition behavior for the selected edge.</p>
        </div>
        <div className="inspector-body">
          <label>
            Edge ID
            <input value={selectedEdge.id} readOnly />
          </label>
          <label>
            Label
            <input
              value={selectedEdge.label}
              onChange={(event) =>
                onGraphChange(updateEdge(graph, selectedEdge.id, (edge) => ({ ...edge, label: event.target.value })))
              }
            />
          </label>
          <label>
            Kind
            <select
              value={selectedEdge.kind}
              onChange={(event) =>
                onGraphChange(
                  updateEdge(graph, selectedEdge.id, (edge) => ({
                    ...edge,
                    kind: event.target.value,
                    condition:
                      event.target.value === "conditional"
                        ? edge.condition ?? {
                            id: `${edge.id}-condition`,
                            label: "Validation error",
                            type: "result_status_equals",
                            value: "validation_error",
                            path: null,
                          }
                        : null,
                  })),
                )
              }
            >
              <option value="standard">standard</option>
              <option value="conditional">conditional</option>
              <option value="binding">binding</option>
            </select>
          </label>
          <label>
            Priority
            <input
              type="number"
              value={selectedEdge.priority}
              onChange={(event) =>
                onGraphChange(
                  updateEdge(graph, selectedEdge.id, (edge) => ({ ...edge, priority: Number(event.target.value) })),
                )
              }
            />
          </label>
          {selectedEdge.kind === "conditional" && selectedEdge.condition ? (
            <>
              <label>
                Condition Label
                <input
                  value={selectedEdge.condition.label}
                  onChange={(event) =>
                    onGraphChange(
                      updateEdge(graph, selectedEdge.id, (edge) => ({
                        ...edge,
                        condition: { ...edge.condition!, label: event.target.value },
                      })),
                    )
                  }
                />
              </label>
              <label>
                Condition Type
                <select
                  value={selectedEdge.condition.type}
                  onChange={(event) =>
                    onGraphChange(
                      updateEdge(graph, selectedEdge.id, (edge) => ({
                        ...edge,
                        condition: { ...edge.condition!, type: event.target.value },
                      })),
                    )
                  }
                >
                  <option value="result_status_equals">result_status_equals</option>
                  <option value="result_has_error">result_has_error</option>
                  <option value="result_payload_path_equals">result_payload_path_equals</option>
                </select>
              </label>
              <label>
                Condition Value
                <input
                  value={String(selectedEdge.condition.value ?? "")}
                  onChange={(event) =>
                    onGraphChange(
                      updateEdge(graph, selectedEdge.id, (edge) => ({
                        ...edge,
                        condition: { ...edge.condition!, value: event.target.value },
                      })),
                    )
                  }
                />
              </label>
              <label>
                Condition Path
                <input
                  value={String(selectedEdge.condition.path ?? "")}
                  onChange={(event) =>
                    onGraphChange(
                      updateEdge(graph, selectedEdge.id, (edge) => ({
                        ...edge,
                        condition: { ...edge.condition!, path: event.target.value },
                      })),
                    )
                  }
                />
              </label>
            </>
          ) : null}
        </div>
      </section>
    );
  }

  return (
    <section className="panel inspector-panel">
      <div className="panel-header">
        <h2>Graph Inspector</h2>
        <p>Edit the top-level agent metadata and start node.</p>
      </div>
      <div className="inspector-body">
        <label>
          Graph ID
          <input value={graph.graph_id} onChange={(event) => onGraphChange({ ...graph, graph_id: event.target.value })} />
        </label>
        <label>
          Name
          <input value={graph.name} onChange={(event) => onGraphChange({ ...graph, name: event.target.value })} />
        </label>
        <label>
          Description
          <textarea
            rows={4}
            value={graph.description}
            onChange={(event) => onGraphChange({ ...graph, description: event.target.value })}
          />
        </label>
        <label>
          Start Node
          <select
            value={graph.start_node_id}
            onChange={(event) => onGraphChange({ ...graph, start_node_id: event.target.value })}
          >
            <option value="">Select a start node</option>
            {graph.nodes
              .filter((node) => node.category === "start")
              .map((node) => (
                <option key={node.id} value={node.id}>
                  {formatNodeLabel(node)}
                </option>
              ))}
          </select>
        </label>
        <div className="inspector-meta">
          <span>Nodes: {graph.nodes.length}</span>
          <span>Edges: {graph.edges.length}</span>
        </div>
      </div>
    </section>
  );
}
