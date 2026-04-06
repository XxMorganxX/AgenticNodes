import { useEffect, useMemo, useState } from "react";
import type { MouseEvent } from "react";

import { previewSpreadsheetRows } from "../lib/api";
import { CONTROL_FLOW_ELSE_HANDLE_ID } from "../lib/editor";
import { resolveGraphEnvReferences } from "../lib/graphEnv";
import {
  buildLogicBranchHandleId,
  createLogicConditionBranch,
  createLogicConditionGroup,
  createLogicConditionRule,
  normalizeLogicConditionConfig,
  serializeLogicConditionConfig,
  summarizeLogicGroup,
  type LogicConditionBranch,
  type LogicConditionGroup,
  type LogicConditionGroupChild,
  type LogicConditionRule,
} from "../lib/logicConditions";
import { resolveResponseSchemaDetails } from "../lib/responseSchema";
import { getNodeInstanceLabel } from "../lib/nodeInstanceLabels";
import type { EditorCatalog, GraphDefinition, GraphNode, SpreadsheetPreviewResult } from "../lib/types";

type ConditionDetailsModalProps = {
  graph: GraphDefinition;
  node: GraphNode;
  catalog: EditorCatalog | null;
  runtimeOutput?: unknown;
  onGraphChange: (graph: GraphDefinition) => void;
  onClose: () => void;
};

type ConditionModalTab = "overview" | "builder";

type PathSuggestion = {
  path: string;
  displayLabel: string;
  typeLabel: string;
  operatorSuggestions: string[];
  suggestedValues: string[];
  summary: string;
  detail: string;
};

const LOGIC_CONDITIONS_PROVIDER_ID = "core.logic_conditions";
const SPREADSHEET_ROW_PROVIDER_ID = "core.spreadsheet_rows";
const CONTRACT_OPTIONS = ["message_envelope", "tool_result_envelope", "data_envelope"];
const DEFAULT_OPERATOR_OPTIONS = ["exists", "equals", "not_equals", "contains", "gt", "gte", "lt", "lte"];

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function updateNode(graph: GraphDefinition, nodeId: string, updater: (node: GraphNode) => GraphNode): GraphDefinition {
  return {
    ...graph,
    nodes: graph.nodes.map((node) => (node.id === nodeId ? updater(node) : node)),
  };
}

function uniqueStrings(values: string[]): string[] {
  return [...new Set(values.filter((value) => value.trim().length > 0))];
}

function humanizePathLabel(path: string): string {
  if (!path.trim()) {
    return "Whole payload";
  }
  if (path === "status") {
    return "Upstream run status";
  }
  if (path === "message") {
    return "Upstream message text";
  }
  if (path === "data") {
    return "Nested result data";
  }
  if (path === "row_data") {
    return "Current spreadsheet row values";
  }
  if (path === "row_index") {
    return "Spreadsheet loop position";
  }
  if (path === "row_number") {
    return "Original spreadsheet row number";
  }
  if (path === "sheet_name") {
    return "Spreadsheet sheet name";
  }
  if (path === "source_file") {
    return "Spreadsheet source file";
  }
  if (path.startsWith("row_data.")) {
    return `Column: ${path.slice("row_data.".length)}`;
  }
  const segments = path.split(".");
  const lastSegment = segments[segments.length - 1] ?? path;
  const formattedLastSegment = lastSegment
    .replace(/[_-]+/g, " ")
    .replace(/\b\w/g, (character) => character.toUpperCase());
  if (segments.length === 1) {
    return formattedLastSegment;
  }
  const parentPath = segments
    .slice(0, -1)
    .map((segment) =>
      segment
        .replace(/[_-]+/g, " ")
        .replace(/\b\w/g, (character) => character.toUpperCase()),
    )
    .join(" / ");
  return `${formattedLastSegment} (${parentPath})`;
}

function describeSuggestionDetail(path: string, typeLabel: string, suggestedValues: string[]): string {
  const pathDetail = path.trim().length > 0 ? `Path: ${path}` : "Path: whole payload";
  const typeDetail = `Type: ${typeLabel}`;
  const sampleDetail = suggestedValues.length > 0 ? `Examples: ${suggestedValues.slice(0, 3).join(", ")}` : null;
  return [pathDetail, typeDetail, sampleDetail].filter(Boolean).join(" • ");
}

function formatActualValue(value: unknown): string {
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  if (value == null) {
    return "none";
  }
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function cloneGroupWithNewIds(group: LogicConditionGroup, suffix: string): LogicConditionGroup {
  return {
    ...group,
    id: `${group.id}-${suffix}`,
    children: group.children.map((child, index) => {
      if (child.type === "group") {
        return cloneGroupWithNewIds(child, `${suffix}-${index + 1}`);
      }
      return { ...child, id: `${child.id}-${suffix}-${index + 1}` };
    }),
  };
}

function updateLogicGroup(
  group: LogicConditionGroup,
  targetGroupId: string,
  updater: (group: LogicConditionGroup) => LogicConditionGroup,
): LogicConditionGroup {
  if (group.id === targetGroupId) {
    return updater(group);
  }
  return {
    ...group,
    children: group.children.map((child) => {
      if (child.type !== "group") {
        return child;
      }
      return updateLogicGroup(child, targetGroupId, updater);
    }),
  };
}

function updateLogicRule(
  group: LogicConditionGroup,
  targetRuleId: string,
  updater: (rule: LogicConditionRule) => LogicConditionRule,
): LogicConditionGroup {
  return {
    ...group,
    children: group.children.map((child) => {
      if (child.type === "group") {
        return updateLogicRule(child, targetRuleId, updater);
      }
      if (child.id === targetRuleId) {
        return updater(child);
      }
      return child;
    }),
  };
}

function removeGroupChild(group: LogicConditionGroup, targetGroupId: string, childId: string): LogicConditionGroup {
  if (group.id === targetGroupId) {
    const children = group.children.filter((child) => child.id !== childId);
    return {
      ...group,
      children: children.length > 0 ? children : [createLogicConditionRule(0)],
    };
  }
  return {
    ...group,
    children: group.children.map((child) => {
      if (child.type !== "group") {
        return child;
      }
      return removeGroupChild(child, targetGroupId, childId);
    }),
  };
}

function collectRules(group: LogicConditionGroup): LogicConditionRule[] {
  return group.children.flatMap((child) => (child.type === "group" ? collectRules(child) : [child]));
}

function buildIncomingContext(graph: GraphDefinition, node: GraphNode): {
  sourceNode: GraphNode | null;
  contractLabel: string;
  schema: Record<string, unknown> | null;
  sourceSummary: string;
} {
  const incomingEdges = graph.edges.filter((edge) => edge.target_id === node.id && edge.kind !== "binding");
  if (incomingEdges.length !== 1) {
    return {
      sourceNode: null,
      contractLabel: incomingEdges.length === 0 ? "No incoming execution edge" : "Multiple possible incoming contracts",
      schema: null,
      sourceSummary: incomingEdges.length === 0 ? "Connect an upstream node to unlock schema-aware suggestions." : "Schema suggestions need a single upstream source.",
    };
  }
  const incomingEdge = incomingEdges[0];
  const sourceNode = graph.nodes.find((candidate) => candidate.id === incomingEdge.source_id) ?? null;
  if (!sourceNode) {
    return {
      sourceNode: null,
      contractLabel: "Unknown source",
      schema: null,
      sourceSummary: "Upstream source node could not be resolved.",
    };
  }
  let contractLabel = "Envelope inferred from source node";
  if (
    incomingEdge.condition?.type === "result_payload_path_equals" &&
    incomingEdge.condition.path === "metadata.contract" &&
    typeof incomingEdge.condition.value === "string"
  ) {
    contractLabel = incomingEdge.condition.value;
  } else if (sourceNode.kind === "model" && incomingEdge.source_handle_id === "api-message") {
    contractLabel = "message_envelope";
  } else if (sourceNode.kind === "model" && incomingEdge.source_handle_id === "api-tool-call") {
    contractLabel = "tool_call_envelope";
  } else if (sourceNode.kind === "tool" || sourceNode.kind === "mcp_tool_executor") {
    contractLabel = "tool_result_envelope";
  } else if (
    graph.node_providers?.find((provider) => provider.provider_id === sourceNode.provider_id)?.category === "start"
  ) {
    contractLabel = "message_envelope";
  } else if (sourceNode.category === "data" || sourceNode.category === "control_flow_unit") {
    contractLabel = "data_envelope";
  }

  const schema =
    sourceNode.kind === "model" ? resolveResponseSchemaDetails(sourceNode.config as Record<string, unknown>).activeSchema : null;
  return {
    sourceNode,
    contractLabel,
    schema,
    sourceSummary: sourceNode.kind === "model"
      ? schema
        ? "Using the upstream model response schema to suggest payload paths."
        : "The upstream model does not have a structured response schema yet."
      : "Schema suggestions are strongest when the upstream node is a model with a response schema.",
  };
}

function operatorSuggestionsForSchema(schema: Record<string, unknown>): string[] {
  const typeValue = schema.type;
  const normalizedTypes = Array.isArray(typeValue)
    ? typeValue.filter((entry): entry is string => typeof entry === "string")
    : typeof typeValue === "string"
      ? [typeValue]
      : [];
  if (Array.isArray(schema.enum) && schema.enum.length > 0) {
    return ["equals", "not_equals"];
  }
  if (normalizedTypes.includes("boolean")) {
    return ["equals", "not_equals", "exists"];
  }
  if (normalizedTypes.some((entry) => entry === "number" || entry === "integer")) {
    return ["equals", "not_equals", "gt", "gte", "lt", "lte", "exists"];
  }
  if (normalizedTypes.includes("array")) {
    return ["contains", "exists"];
  }
  if (normalizedTypes.includes("object")) {
    return ["exists"];
  }
  return ["equals", "not_equals", "contains", "exists"];
}

function summarizeSchema(schema: Record<string, unknown>): string {
  if (Array.isArray(schema.enum) && schema.enum.length > 0) {
    return `Enum: ${schema.enum.map((entry) => String(entry)).join(", ")}`;
  }
  const typeValue = schema.type;
  if (Array.isArray(typeValue)) {
    return typeValue.join(" | ");
  }
  if (typeof typeValue === "string") {
    return typeValue;
  }
  if (isRecord(schema.properties)) {
    return "object";
  }
  if (isRecord(schema.items)) {
    return "array";
  }
  return "value";
}

function collectSchemaPathSuggestions(
  schema: Record<string, unknown>,
  prefix = "",
  depth = 0,
): PathSuggestion[] {
  if (depth > 4) {
    return [];
  }
  const suggestions: PathSuggestion[] = [];
  if (prefix) {
    suggestions.push({
      path: prefix,
      displayLabel: humanizePathLabel(prefix),
      typeLabel: summarizeSchema(schema),
      operatorSuggestions: operatorSuggestionsForSchema(schema),
      suggestedValues: Array.isArray(schema.enum) ? schema.enum.map((entry) => String(entry)) : [],
      summary: summarizeSchema(schema),
      detail: describeSuggestionDetail(prefix, summarizeSchema(schema), Array.isArray(schema.enum) ? schema.enum.map((entry) => String(entry)) : []),
    });
  }
  if (isRecord(schema.properties)) {
    for (const [key, propertySchema] of Object.entries(schema.properties)) {
      if (!isRecord(propertySchema)) {
        continue;
      }
      const path = prefix ? `${prefix}.${key}` : key;
      suggestions.push(...collectSchemaPathSuggestions(propertySchema, path, depth + 1));
    }
  }
  if (isRecord(schema.items)) {
    const arrayPath = prefix ? `${prefix}[]` : "[]";
    suggestions.push(...collectSchemaPathSuggestions(schema.items, arrayPath, depth + 1));
  }
  return suggestions;
}

function fallbackSuggestions(contractLabel: string): PathSuggestion[] {
  const base = [
    { path: "", displayLabel: "Whole payload", typeLabel: "whole payload", operatorSuggestions: ["exists", "equals", "not_equals"], suggestedValues: [], summary: "Match against the whole payload.", detail: describeSuggestionDetail("", "whole payload", []) },
    { path: "status", displayLabel: "Status", typeLabel: "string", operatorSuggestions: ["equals", "not_equals", "contains", "exists"], suggestedValues: ["ok", "needs_input", "error"], summary: "Common status field.", detail: describeSuggestionDetail("status", "string", ["ok", "needs_input", "error"]) },
    { path: "message", displayLabel: "Message", typeLabel: "string", operatorSuggestions: ["contains", "equals", "exists"], suggestedValues: [], summary: "Common message field.", detail: describeSuggestionDetail("message", "string", []) },
    { path: "data", displayLabel: "Data", typeLabel: "object | array", operatorSuggestions: ["exists"], suggestedValues: [], summary: "Nested data payload.", detail: describeSuggestionDetail("data", "object | array", []) },
  ];
  if (contractLabel === "data_envelope") {
    return [
      ...base,
      { path: "row_data", displayLabel: "Current spreadsheet row values", typeLabel: "object", operatorSuggestions: ["exists"], suggestedValues: [], summary: "Spreadsheet row payload.", detail: describeSuggestionDetail("row_data", "object", []) },
      { path: "row_index", displayLabel: "Spreadsheet loop position", typeLabel: "number", operatorSuggestions: ["equals", "gt", "gte", "lt", "lte"], suggestedValues: [], summary: "Current row index.", detail: describeSuggestionDetail("row_index", "number", []) },
      { path: "sheet_name", displayLabel: "Sheet name", typeLabel: "string", operatorSuggestions: ["equals", "contains", "exists"], suggestedValues: [], summary: "Spreadsheet sheet name.", detail: describeSuggestionDetail("sheet_name", "string", []) },
    ];
  }
  return base;
}

function buildSpreadsheetSuggestions(preview: SpreadsheetPreviewResult): PathSuggestion[] {
  const rowDataSuggestions = preview.headers.map((header) => {
    const sampleValues = uniqueStrings(
      preview.sample_rows
        .map((row) => row.row_data?.[header])
        .filter((value): value is string | number | boolean => typeof value === "string" || typeof value === "number" || typeof value === "boolean")
        .map((value) => String(value)),
    ).slice(0, 6);
    const numericSamples = sampleValues.filter((value) => value.trim().length > 0 && !Number.isNaN(Number(value)));
    return {
      path: `row_data.${header}`,
      displayLabel: `Column: ${header}`,
      typeLabel: numericSamples.length === sampleValues.length && sampleValues.length > 0 ? "number" : "string",
      operatorSuggestions:
        numericSamples.length === sampleValues.length && sampleValues.length > 0
          ? ["equals", "not_equals", "gt", "gte", "lt", "lte", "exists"]
          : ["equals", "not_equals", "contains", "exists"],
      suggestedValues: sampleValues,
      summary: `Spreadsheet column "${header}"`,
      detail: describeSuggestionDetail(`row_data.${header}`, numericSamples.length === sampleValues.length && sampleValues.length > 0 ? "number" : "string", sampleValues),
    } satisfies PathSuggestion;
  });
  return [
    { path: "row_data", displayLabel: "Current spreadsheet row values", typeLabel: "object", operatorSuggestions: ["exists"], suggestedValues: [], summary: "Spreadsheet row keyed by header.", detail: describeSuggestionDetail("row_data", "object", []) },
    ...rowDataSuggestions,
    { path: "row_index", displayLabel: "Spreadsheet loop position", typeLabel: "number", operatorSuggestions: ["equals", "gt", "gte", "lt", "lte"], suggestedValues: [], summary: "Current row index.", detail: describeSuggestionDetail("row_index", "number", []) },
    { path: "row_number", displayLabel: "Original spreadsheet row number", typeLabel: "number", operatorSuggestions: ["equals", "gt", "gte", "lt", "lte"], suggestedValues: [], summary: "Original spreadsheet row number.", detail: describeSuggestionDetail("row_number", "number", []) },
    { path: "sheet_name", displayLabel: "Spreadsheet sheet name", typeLabel: "string", operatorSuggestions: ["equals", "contains", "exists"], suggestedValues: preview.sheet_name ? [preview.sheet_name] : [], summary: "Spreadsheet sheet name.", detail: describeSuggestionDetail("sheet_name", "string", preview.sheet_name ? [preview.sheet_name] : []) },
    { path: "source_file", displayLabel: "Spreadsheet source file", typeLabel: "string", operatorSuggestions: ["equals", "contains", "exists"], suggestedValues: [], summary: "Resolved spreadsheet file path.", detail: describeSuggestionDetail("source_file", "string", []) },
  ];
}

export function ConditionDetailsModal({
  graph,
  node,
  catalog,
  runtimeOutput,
  onGraphChange,
  onClose,
}: ConditionDetailsModalProps) {
  const [activeTab, setActiveTab] = useState<ConditionModalTab>("overview");
  const [spreadsheetPreview, setSpreadsheetPreview] = useState<SpreadsheetPreviewResult | null>(null);
  const [spreadsheetPreviewError, setSpreadsheetPreviewError] = useState<string | null>(null);
  const [validationError, setValidationError] = useState<string | null>(null);
  const [selectedBranchId, setSelectedBranchId] = useState<string | null>(null);
  const nodeLabel = getNodeInstanceLabel(graph, node);
  const incomingContext = useMemo(() => buildIncomingContext(graph, node), [graph, node]);
  const schemaSuggestions = useMemo(() => {
    const suggestions =
      spreadsheetPreview != null
        ? buildSpreadsheetSuggestions(spreadsheetPreview)
        : incomingContext.schema
          ? collectSchemaPathSuggestions(incomingContext.schema)
          : fallbackSuggestions(incomingContext.contractLabel);
    return uniqueStrings(suggestions.map((suggestion) => suggestion.path)).map(
      (path) => suggestions.find((suggestion) => suggestion.path === path)!,
    );
  }, [incomingContext.contractLabel, incomingContext.schema, spreadsheetPreview]);
  const suggestedClauses = useMemo(
    () =>
      schemaSuggestions.flatMap((suggestion) =>
        suggestion.suggestedValues.slice(0, 4).map((value) => ({
          label: `${suggestion.displayLabel} = ${value}`,
          path: suggestion.path,
          operator: "equals",
          value,
        })),
      ),
    [schemaSuggestions],
  );
  const logicConfig = useMemo(() => normalizeLogicConditionConfig(node.config).normalized, [node]);
  const branches = logicConfig.branches;
  const selectedBranch = branches.find((branch) => branch.id === selectedBranchId) ?? branches[0] ?? null;
  const selectedBranchRules = selectedBranch ? collectRules(selectedBranch.root_group) : [];
  const selectedRule = selectedBranchRules[0] ?? null;
  const selectedSuggestion = selectedRule ? schemaSuggestions.find((suggestion) => suggestion.path === selectedRule.path) ?? null : null;
  const runtimeMetadata =
    runtimeOutput && typeof runtimeOutput === "object" && runtimeOutput !== null && "metadata" in runtimeOutput
      ? (runtimeOutput.metadata as Record<string, unknown> | null)
      : null;
  const branchEvaluations = Array.isArray(runtimeMetadata?.branch_evaluations) ? runtimeMetadata.branch_evaluations : [];
  const selectedBranchEvaluation = selectedBranch
    ? branchEvaluations.find(
        (entry) =>
          typeof entry === "object" &&
          entry !== null &&
          !Array.isArray(entry) &&
          ((entry as Record<string, unknown>).id === selectedBranch.id ||
            (entry as Record<string, unknown>).output_handle_id === selectedBranch.output_handle_id),
      ) ?? null
    : null;

  useEffect(() => {
    setActiveTab("overview");
    setValidationError(null);
    setSelectedBranchId(null);
  }, [node.id]);

  useEffect(() => {
    if (branches.length === 0) {
      setSelectedBranchId(null);
      return;
    }
    if (selectedBranchId == null || !branches.some((branch) => branch.id === selectedBranchId)) {
      setSelectedBranchId(branches[0].id);
    }
  }, [branches, selectedBranchId]);

  useEffect(() => {
    let cancelled = false;
    setSpreadsheetPreview(null);
    setSpreadsheetPreviewError(null);
    if (incomingContext.sourceNode?.provider_id !== SPREADSHEET_ROW_PROVIDER_ID) {
      return;
    }
    const config = incomingContext.sourceNode.config;
    const resolvedFilePath = String(resolveGraphEnvReferences(String(config.file_path ?? ""), graph) ?? "");
    if (!resolvedFilePath) {
      setSpreadsheetPreviewError("Spreadsheet suggestions need a resolvable file path on the connected spreadsheet rows node.");
      return;
    }
    void previewSpreadsheetRows({
      file_path: resolvedFilePath,
      file_format: String(config.file_format ?? "auto"),
      sheet_name: config.sheet_name == null ? null : String(config.sheet_name),
      header_row_index: typeof config.header_row_index === "number" ? config.header_row_index : 1,
      start_row_index: typeof config.start_row_index === "number" ? config.start_row_index : 2,
      empty_row_policy: String(config.empty_row_policy ?? "skip"),
    })
      .then((preview) => {
        if (!cancelled) {
          setSpreadsheetPreview(preview);
        }
      })
      .catch((error: unknown) => {
        if (!cancelled) {
          setSpreadsheetPreviewError(error instanceof Error ? error.message : "Failed to preview spreadsheet columns.");
        }
      });
    return () => {
      cancelled = true;
    };
  }, [graph, incomingContext.sourceNode]);

  useEffect(() => {
    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        event.preventDefault();
        requestClose();
      }
    }
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [branches, onClose]);

  function handleOverlayClick(event: MouseEvent<HTMLDivElement>) {
    if (event.target === event.currentTarget) {
      requestClose();
    }
  }

  function applyBranches(nextBranches: LogicConditionBranch[]) {
    setValidationError(null);
    const { normalized, handleRemap } = normalizeLogicConditionConfig({
      branches: nextBranches as unknown as Record<string, unknown>[],
      else_output_handle_id: logicConfig.else_output_handle_id,
    });
    const mutableHandleRemap = new Map<string, string[]>(
      [...handleRemap.entries()].map(([key, values]) => [key, [...values]]),
    );
    onGraphChange(
      {
        ...updateNode(graph, node.id, (currentNode) => ({
          ...currentNode,
          config: (() => {
            const nextConfig = { ...currentNode.config };
            delete nextConfig.clauses;
            return {
              ...nextConfig,
              mode: "logic_conditions",
              ...serializeLogicConditionConfig(normalized),
            };
          })(),
        })),
        edges: graph.edges.map((edge) => {
          if (edge.source_id !== node.id) {
            return edge;
          }
          const remappedHandleId = mutableHandleRemap.get(edge.source_handle_id ?? "")?.shift();
          return remappedHandleId ? { ...edge, source_handle_id: remappedHandleId } : edge;
        }),
      },
    );
  }

  function updateBranch(targetBranchId: string, updater: (branch: LogicConditionBranch) => LogicConditionBranch) {
    applyBranches(branches.map((branch) => (branch.id === targetBranchId ? updater(branch) : branch)));
  }

  function requestClose() {
    const missingLabelIndex = branches.findIndex((branch) => branch.label.trim().length === 0);
    if (missingLabelIndex >= 0) {
      setActiveTab("builder");
      setSelectedBranchId(branches[missingLabelIndex]?.id ?? null);
      setValidationError(`Branch ${missingLabelIndex + 1} needs a title before closing.`);
      return;
    }
    setValidationError(null);
    onClose();
  }

  function addBranch(suggestion?: { path: string; operator: string; value: string; label?: string }) {
    const nextBranch = createLogicConditionBranch(branches.length, suggestion?.label);
    nextBranch.root_group.children = [
      {
        ...createLogicConditionRule(branches.length),
        path: suggestion?.path ?? "",
        operator: suggestion?.operator ?? "equals",
        value: suggestion?.value ?? "",
      },
    ];
    const nextBranches = [...branches, nextBranch];
    applyBranches(nextBranches);
    setSelectedBranchId(nextBranch.id);
    setActiveTab("builder");
  }

  function duplicateBranch(targetBranchId: string) {
    const branch = branches.find((candidate) => candidate.id === targetBranchId);
    if (!branch) {
      return;
    }
    const nextBranch: LogicConditionBranch = {
      ...branch,
      id: `${branch.id}-copy-${Date.now()}`,
      label: branch.label.trim() ? `${branch.label} Copy` : "Branch Copy",
      output_handle_id: `${branch.output_handle_id}-copy`,
      root_group: cloneGroupWithNewIds(branch.root_group, `copy-${Date.now()}`),
    };
    const nextBranches = [...branches, nextBranch];
    applyBranches(nextBranches);
    setSelectedBranchId(nextBranch.id);
  }

  function removeBranch(targetBranchId: string) {
    const nextBranches = branches.filter((branch) => branch.id !== targetBranchId);
    const ensuredBranches = nextBranches.length > 0 ? nextBranches : [createLogicConditionBranch(0, "If")];
    applyBranches(ensuredBranches);
    setSelectedBranchId(ensuredBranches[0]?.id ?? null);
  }

  function updateBranchGroup(targetBranchId: string, targetGroupId: string, updater: (group: LogicConditionGroup) => LogicConditionGroup) {
    updateBranch(targetBranchId, (branch) => ({
      ...branch,
      root_group: updateLogicGroup(branch.root_group, targetGroupId, updater),
    }));
  }

  function updateBranchRule(targetBranchId: string, targetRuleId: string, updater: (rule: LogicConditionRule) => LogicConditionRule) {
    updateBranch(targetBranchId, (branch) => ({
      ...branch,
      root_group: updateLogicRule(branch.root_group, targetRuleId, updater),
    }));
  }

  function removeBranchChild(targetBranchId: string, targetGroupId: string, childId: string) {
    updateBranch(targetBranchId, (branch) => ({
      ...branch,
      root_group: removeGroupChild(branch.root_group, targetGroupId, childId),
    }));
  }

  function renderGroupEditor(branch: LogicConditionBranch, group: LogicConditionGroup, depth = 0) {
    return (
      <div key={group.id} className={`condition-group-card ${depth > 0 ? "is-nested" : ""}`}>
        <div className="condition-group-card-header">
          <div className="condition-group-card-controls">
            <div className="condition-match-panel">
              <div className="condition-match-copy">
                <strong>{depth === 0 ? "Branch matches when" : "This group matches when"}</strong>
                <span>{group.combinator === "all" ? "Every rule in this section must pass." : "At least one rule in this section must pass."}</span>
              </div>
              <div className="condition-match-toggle-row">
                <button
                  type="button"
                  className={`condition-match-toggle ${group.combinator === "all" ? "is-active" : ""}`}
                  onClick={() =>
                    updateBranchGroup(branch.id, group.id, (candidate) => ({
                      ...candidate,
                      combinator: "all",
                    }))
                  }
                >
                  Match all rules
                </button>
                <button
                  type="button"
                  className={`condition-match-toggle ${group.combinator === "any" ? "is-active" : ""}`}
                  onClick={() =>
                    updateBranchGroup(branch.id, group.id, (candidate) => ({
                      ...candidate,
                      combinator: "any",
                    }))
                  }
                >
                  Match any rule
                </button>
              </div>
            </div>
            <label className="condition-group-negated condition-group-negated--inline">
              <input
                type="checkbox"
                checked={group.negated}
                onChange={(event) =>
                  updateBranchGroup(branch.id, group.id, (candidate) => ({
                    ...candidate,
                    negated: event.target.checked,
                  }))
                }
              />
              Invert this result
            </label>
          </div>
          <div className="condition-group-actions">
            <button
              type="button"
              className="secondary-button"
              onClick={() =>
                updateBranchGroup(branch.id, group.id, (candidate) => ({
                  ...candidate,
                  children: [...candidate.children, createLogicConditionRule(candidate.children.length)],
                }))
              }
            >
              Add Rule
            </button>
            <button
              type="button"
              className="secondary-button"
              onClick={() =>
                updateBranchGroup(branch.id, group.id, (candidate) => ({
                  ...candidate,
                  children: [...candidate.children, createLogicConditionGroup(candidate.children.length)],
                }))
              }
            >
              Add Group
            </button>
          </div>
        </div>
        <div className="condition-group-card-summary">{summarizeLogicGroup(group)}</div>
        <div className="condition-group-card-body">
          {group.children.map((child, index) => {
            if (child.type === "group") {
              return renderGroupEditor(branch, child, depth + 1);
            }
            const matchingSuggestion = schemaSuggestions.find((suggestion) => suggestion.path === child.path) ?? null;
            return (
              <div key={child.id} className="condition-rule-row">
                <div className="condition-rule-row-header">
                  <strong>Rule {index + 1}</strong>
                  <button
                    type="button"
                    className="secondary-button"
                    onClick={() => removeBranchChild(branch.id, group.id, child.id)}
                  >
                    Remove
                  </button>
                </div>
                <div className="condition-rule-grid">
                  <label>
                    Field
                    <input
                      list={`condition-path-suggestions-${node.id}`}
                      value={child.path}
                      placeholder="Choose a field"
                      onChange={(event) =>
                        updateBranchRule(branch.id, child.id, (candidate) => ({ ...candidate, path: event.target.value }))
                      }
                    />
                  </label>
                  <label>
                    Operator
                    <select
                      value={child.operator}
                      onChange={(event) =>
                        updateBranchRule(branch.id, child.id, (candidate) => ({ ...candidate, operator: event.target.value }))
                      }
                    >
                      {(matchingSuggestion?.operatorSuggestions ?? DEFAULT_OPERATOR_OPTIONS).map((operator) => (
                        <option key={operator} value={operator}>
                          {operator}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label>
                    Value
                    <input
                      list={`condition-value-suggestions-${node.id}-${child.id}`}
                      value={child.value}
                      placeholder={child.operator === "exists" ? "Leave blank" : "Match value"}
                      onChange={(event) =>
                        updateBranchRule(branch.id, child.id, (candidate) => ({ ...candidate, value: event.target.value }))
                      }
                    />
                  </label>
                  <label>
                    Incoming Contract
                    <select
                      value={child.source_contracts[0] ?? ""}
                      onChange={(event) =>
                        updateBranchRule(branch.id, child.id, (candidate) => ({
                          ...candidate,
                          source_contracts: event.target.value ? [event.target.value] : [],
                        }))
                      }
                    >
                      <option value="">Any accepted contract</option>
                      {CONTRACT_OPTIONS.map((contract) => (
                        <option key={contract} value={contract}>
                          {contract}
                        </option>
                      ))}
                    </select>
                  </label>
                </div>
                <div className="inspector-meta">
                  <span>Field label: {matchingSuggestion?.displayLabel ?? humanizePathLabel(child.path)}</span>
                  <span>{matchingSuggestion ? `Schema type: ${matchingSuggestion.typeLabel}` : "Schema type unavailable"}</span>
                </div>
                {matchingSuggestion?.suggestedValues.length ? (
                  <datalist id={`condition-value-suggestions-${node.id}-${child.id}`}>
                    {matchingSuggestion.suggestedValues.map((value) => (
                      <option key={value} value={value} />
                    ))}
                  </datalist>
                ) : null}
              </div>
            );
          })}
        </div>
      </div>
    );
  }

  return (
    <div className="tool-details-modal-backdrop" onClick={handleOverlayClick} role="presentation">
      <section className="tool-details-modal tool-details-modal--wide" role="dialog" aria-modal="true" aria-labelledby="condition-details-modal-title">
        <div className="tool-details-modal-header">
          <div>
            <div className="tool-details-modal-eyebrow">Condition Builder</div>
            <h3 id="condition-details-modal-title">{nodeLabel}</h3>
            <p>
              Build named branches that each evaluate a boolean rule tree. The first matching branch wins; if none match,
              execution falls through to `Else`.
            </p>
          </div>
          <button type="button" className="secondary-button" onClick={requestClose}>
            Close
          </button>
        </div>

        <div className="tool-details-modal-body">
          <div className="modal-folder-tabs" role="tablist" aria-label="Condition modal sections">
            {[
              ["overview", "Overview"],
              ["builder", "Builder"],
            ].map(([tabId, label]) => (
              <button
                key={tabId}
                type="button"
                role="tab"
                aria-selected={activeTab === tabId}
                className={`modal-folder-tab ${activeTab === tabId ? "modal-folder-tab--active" : ""}`}
                onClick={() => setActiveTab(tabId as ConditionModalTab)}
              >
                {label}
              </button>
            ))}
          </div>

          <div className="modal-folder-panel">
            {activeTab === "overview" ? (
              <div className="modal-folder-section">
                <div className="contract-card">
                  <strong>How this node works</strong>
                  <span>Branches are checked top to bottom. The first branch whose boolean group evaluates true will run.</span>
                  <span>Paths are payload-relative. Use `status`, `answer`, or `row_data.city`, not `payload.status`.</span>
                </div>
                <div className="contract-card">
                  <strong>Incoming source</strong>
                  <span>Source node: {incomingContext.sourceNode ? getNodeInstanceLabel(graph, incomingContext.sourceNode) : "Unavailable"}</span>
                  <span>Resolved incoming contract: {incomingContext.contractLabel}</span>
                  <span>{incomingContext.sourceSummary}</span>
                  {spreadsheetPreview ? <span>Spreadsheet headers detected: {spreadsheetPreview.headers.join(", ")}</span> : null}
                  {spreadsheetPreviewError ? <span>{spreadsheetPreviewError}</span> : null}
                </div>
                <div className="contract-card">
                  <strong>Schema-aware path suggestions</strong>
                  <span>
                    {schemaSuggestions.length > 0
                      ? `${schemaSuggestions.length} payload path suggestion${schemaSuggestions.length === 1 ? "" : "s"} available from the upstream shape.`
                      : "No structured path suggestions found yet."}
                  </span>
                  <div className="condition-builder-chip-row">
                    {schemaSuggestions.slice(0, 10).map((suggestion) => (
                      <button
                        key={suggestion.path || "whole-payload"}
                        type="button"
                        className="condition-builder-chip"
                        onClick={() =>
                          addBranch({
                            label: suggestion.displayLabel,
                            path: suggestion.path,
                            operator: suggestion.operatorSuggestions[0] ?? "equals",
                            value: suggestion.suggestedValues[0] ?? "",
                          })
                        }
                      >
                        <span>{suggestion.displayLabel}</span>
                        <small>{suggestion.detail}</small>
                      </button>
                    ))}
                  </div>
                </div>
                {suggestedClauses.length > 0 ? (
                  <div className="contract-card">
                    <strong>Suggested starter branches</strong>
                    <div className="condition-builder-chip-row">
                      {suggestedClauses.slice(0, 8).map((suggestion) => (
                        <button
                          key={`${suggestion.path}-${suggestion.value}`}
                          type="button"
                          className="condition-builder-chip"
                          onClick={() => addBranch(suggestion)}
                        >
                          <span>{suggestion.label}</span>
                        </button>
                      ))}
                    </div>
                  </div>
                ) : null}
              </div>
            ) : null}

            {activeTab === "builder" ? (
              <div className="modal-folder-section">
                <div className="condition-builder-toolbar">
                  <div className="tool-details-modal-help">
                    First matching branch wins. Else handle: <code>{String(logicConfig.else_output_handle_id ?? CONTROL_FLOW_ELSE_HANDLE_ID)}</code>
                  </div>
                  <div className="condition-builder-row-actions">
                    <button type="button" className="secondary-button" onClick={() => addBranch()}>
                      Add Branch
                    </button>
                    {selectedBranch ? (
                      <button type="button" className="secondary-button" onClick={() => duplicateBranch(selectedBranch.id)}>
                        Duplicate
                      </button>
                    ) : null}
                    {selectedBranch ? (
                      <button type="button" className="secondary-button" onClick={() => removeBranch(selectedBranch.id)}>
                        Remove Branch
                      </button>
                    ) : null}
                  </div>
                </div>
                {validationError ? <div className="tool-details-modal-help">{validationError}</div> : null}
                <div className="condition-builder-layout">
                  <div className="condition-branch-sidebar">
                    {branches.map((branch, index) => {
                      const branchSummary = summarizeLogicGroup(branch.root_group);
                      const branchEvaluation =
                        branchEvaluations.find(
                          (entry) =>
                            typeof entry === "object" &&
                            entry !== null &&
                            !Array.isArray(entry) &&
                            (entry as Record<string, unknown>).id === branch.id,
                        ) ?? null;
                      const matched = !!(branchEvaluation && typeof branchEvaluation === "object" && (branchEvaluation as Record<string, unknown>).matched === true);
                      return (
                        <button
                          key={branch.id}
                          type="button"
                          className={`condition-branch-sidebar-item ${selectedBranch?.id === branch.id ? "is-active" : ""}`}
                          onClick={() => setSelectedBranchId(branch.id)}
                        >
                          <div className="condition-branch-sidebar-header">
                            <strong>{branch.label.trim() || `Branch ${index + 1}`}</strong>
                            <span>{matched ? "Matched" : "Pending"}</span>
                          </div>
                          <div className="condition-branch-sidebar-summary">{branchSummary}</div>
                        </button>
                      );
                    })}
                    <div className="contract-card">
                      <strong>Else</strong>
                      <span>Runs when no branch matches.</span>
                      <span>Handle: {logicConfig.else_output_handle_id}</span>
                    </div>
                  </div>
                  <div className="condition-builder-center">
                    {selectedBranch ? (
                      <>
                        <div className="condition-branch-header">
                          <label>
                            Branch Name
                            <input
                              value={selectedBranch.label}
                              placeholder={`Branch ${branches.findIndex((branch) => branch.id === selectedBranch.id) + 1}`}
                              onChange={(event) =>
                                updateBranch(selectedBranch.id, (branch) => ({
                                  ...branch,
                                  label: event.target.value,
                                  output_handle_id: buildLogicBranchHandleId(
                                    event.target.value,
                                    branch.id,
                                    branches.findIndex((candidate) => candidate.id === branch.id),
                                  ),
                                }))
                              }
                            />
                          </label>
                        </div>
                        {renderGroupEditor(selectedBranch, selectedBranch.root_group)}
                      </>
                    ) : (
                      <div className="contract-card">
                        <strong>No branches yet</strong>
                        <span>Add a branch to start building routing logic.</span>
                      </div>
                    )}
                  </div>
                  <div className="condition-builder-sidepanel">
                    <div className="contract-card">
                      <strong>Live Explanation</strong>
                      <span>{selectedBranch ? `Run "${selectedBranch.label.trim() || "Untitled Branch"}" when ${summarizeLogicGroup(selectedBranch.root_group)}.` : "Select a branch to see its explanation."}</span>
                      {selectedBranchEvaluation && typeof selectedBranchEvaluation === "object" && !Array.isArray(selectedBranchEvaluation) ? (
                        <span>Latest run: {(selectedBranchEvaluation as Record<string, unknown>).matched === true ? "matched" : "did not match"}</span>
                      ) : (
                        <span>Run the graph to see why this branch matched or failed.</span>
                      )}
                    </div>
                    <div className="contract-card">
                      <strong>Field Suggestions</strong>
                      <span>
                        {selectedSuggestion
                          ? `${selectedSuggestion.displayLabel} supports ${selectedSuggestion.operatorSuggestions.join(", ")}`
                          : "Pick a field to see tailored operators and sample values."}
                      </span>
                      <div className="condition-builder-chip-row">
                        {schemaSuggestions.slice(0, 10).map((suggestion) => (
                          <button
                            key={`${suggestion.path || "whole-payload"}-side`}
                            type="button"
                            className="condition-builder-chip"
                            onClick={() => {
                              if (!selectedBranch || !selectedRule) {
                                return;
                              }
                              updateBranchRule(selectedBranch.id, selectedRule.id, (rule) => ({
                                ...rule,
                                path: suggestion.path,
                                operator: suggestion.operatorSuggestions[0] ?? rule.operator,
                                value: suggestion.suggestedValues[0] ?? rule.value,
                              }));
                            }}
                          >
                            <span>{suggestion.displayLabel}</span>
                            <small>{suggestion.detail}</small>
                          </button>
                        ))}
                      </div>
                    </div>
                    <div className="contract-card">
                      <strong>Latest Observed Values</strong>
                      {selectedBranchEvaluation && typeof selectedBranchEvaluation === "object" && !Array.isArray(selectedBranchEvaluation) ? (
                        (() => {
                          const trace =
                            typeof (selectedBranchEvaluation as Record<string, unknown>).trace === "object" &&
                            (selectedBranchEvaluation as Record<string, unknown>).trace !== null &&
                            !Array.isArray((selectedBranchEvaluation as Record<string, unknown>).trace)
                              ? ((selectedBranchEvaluation as Record<string, unknown>).trace as Record<string, unknown>)
                              : null;
                          return Array.isArray(trace?.children) ? trace.children : [];
                        })()
                      ).filter((entry): entry is Record<string, unknown> => typeof entry === "object" && entry !== null && !Array.isArray(entry))
                        .map((entry) => (
                          <span key={String(entry.id ?? Math.random())}>
                            {(selectedSuggestion && selectedSuggestion.path === entry.path ? selectedSuggestion.displayLabel : humanizePathLabel(String(entry.path ?? ""))) || "Rule"}: {formatActualValue(entry.actual_value)}
                          </span>
                        )) : (
                        <span>Run the graph to inspect actual values seen by each rule.</span>
                      )}
                    </div>
                  </div>
                </div>
                <datalist id={`condition-path-suggestions-${node.id}`}>
                  {schemaSuggestions.map((suggestion) => (
                    <option key={suggestion.path || "whole-payload"} value={suggestion.path}>
                      {suggestion.displayLabel}
                    </option>
                  ))}
                </datalist>
              </div>
            ) : null}
          </div>
        </div>
      </section>
    </div>
  );
}
