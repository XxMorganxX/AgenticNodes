import { useEffect, useMemo, useState } from "react";
import type { ChangeEvent, MouseEvent } from "react";

import { fetchProviderDiagnostics, inspectSupabaseRuntimeStatus, preflightProvider, previewSupabaseSchema } from "../lib/api";
import { findProviderDefinition, inferModelResponseMode, modelProviderDefinitions, providerDefaultConfig, providerModelName } from "../lib/editor";
import { getGraphEnvVars, resolveGraphEnvReferences } from "../lib/graphEnv";
import { getNodeInstanceLabel } from "../lib/nodeInstanceLabels";
import { insertTokenAtEnd } from "../lib/promptBlockEditor";
import {
  parseResponseSchemaText,
  resolveResponseSchemaDetails,
  RESPONSE_SCHEMA_PRESETS,
  RESPONSE_SCHEMA_TEXT_CONFIG_KEY,
} from "../lib/responseSchema";
import { loadSessionSupabaseSchema, saveSessionSupabaseSchema } from "../lib/sessionSupabaseSchema";
import { SPREADSHEET_MATRIX_RECOMMENDED_USER_MESSAGE_TEMPLATE } from "../lib/spreadsheetMatrixPrompt";
import { resolveToolNodeDetails } from "../lib/toolNodeDetails";
import { NodeDetailsForm } from "./NodeDetailsForm";
import type {
  EditorCatalog,
  GraphDefinition,
  GraphNode,
  ProviderDiagnosticsResult,
  ProviderPreflightResult,
  SupabaseRuntimeStatusResult,
  SupabaseSchemaPreviewResult,
  SupabaseSchemaSource,
  ToolDefinition,
} from "../lib/types";

const LIVE_PROVIDER_VERIFICATION_STORAGE_KEY = "agentic-nodes-live-provider-verifications";

type ProviderDetailsModalProps = {
  graph: GraphDefinition;
  node: GraphNode;
  catalog: EditorCatalog | null;
  onGraphChange: (graph: GraphDefinition) => void;
  onClose: () => void;
};

type ProviderDetailsModalTab = "node" | "overview" | "prompt" | "routing" | "config" | "preview";

type PersistedProviderVerification = {
  preflightResult: ProviderPreflightResult;
  diagnostics: ProviderDiagnosticsResult;
};

function updateModelNode(
  graph: GraphDefinition,
  nodeId: string,
  updater: (node: GraphNode) => GraphNode,
): GraphDefinition {
  return {
    ...graph,
    nodes: graph.nodes.map((node) => (node.id === nodeId ? updater(node) : node)),
  };
}

function resolveProviderDefinition(node: GraphNode, catalog: EditorCatalog | null) {
  const directProvider = (catalog?.node_providers ?? []).find((provider) => provider.provider_id === node.provider_id) ?? null;
  if (node.kind !== "model") {
    return directProvider;
  }
  const providerName = String(node.config.provider_name ?? node.model_provider_name ?? "").trim();
  if (!providerName) {
    return directProvider;
  }
  return findProviderDefinition(catalog, providerName) ?? directProvider;
}

function toolCanonicalName(tool: ToolDefinition): string {
  return tool.canonical_name ?? tool.name;
}

function toolLabel(tool: ToolDefinition): string {
  return tool.display_name ?? tool.name;
}

function toolStatusLabel(tool: ToolDefinition): string {
  if (tool.enabled === false) {
    return "disabled";
  }
  if (tool.available === false) {
    return "offline";
  }
  return "ready";
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

function parseSupabaseSelect(selectValue: string, availableColumns: string[]): string[] {
  const trimmed = selectValue.trim();
  if (!trimmed || trimmed === "*") {
    return [...availableColumns];
  }
  return uniqueStrings(
    trimmed
      .split(",")
      .map((value) => value.trim())
      .filter((value) => value.length > 0),
  );
}

function normalizedGraphEnvValue(graph: GraphDefinition, key: string): string {
  const value = String(getGraphEnvVars(graph)[key] ?? "").trim();
  if (!value || value === key) {
    return "";
  }
  return value;
}

function extractTemplateTokens(template: string): string[] {
  return uniqueStrings(Array.from(template.matchAll(/\{([A-Za-z_][A-Za-z0-9_]*)\}/g)).map((match) => match[1] ?? ""));
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
    .filter((candidate): candidate is GraphNode => candidate !== null && candidate.kind === "mcp_context_provider");
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
    .map((nodeId) => graph.nodes.find((candidate) => candidate.id === nodeId) ?? null)
    .filter((candidate): candidate is GraphNode => candidate !== null && candidate.provider_id === "core.prompt_block");
}

function readPersistedProviderVerifications(): Record<string, PersistedProviderVerification> {
  try {
    const raw = localStorage.getItem(LIVE_PROVIDER_VERIFICATION_STORAGE_KEY);
    if (!raw) {
      return {};
    }
    const parsed = JSON.parse(raw) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return {};
    }
    return parsed as Record<string, PersistedProviderVerification>;
  } catch {
    return {};
  }
}

function getPersistedProviderVerification(storageKey: string): PersistedProviderVerification | null {
  const verifications = readPersistedProviderVerifications();
  const verification = verifications[storageKey];
  return verification ?? null;
}

function persistProviderVerification(storageKey: string, verification: PersistedProviderVerification): void {
  const verifications = readPersistedProviderVerifications();
  verifications[storageKey] = verification;
  localStorage.setItem(LIVE_PROVIDER_VERIFICATION_STORAGE_KEY, JSON.stringify(verifications));
}

function buildProviderVerificationStorageKey(providerName: string, providerConfig: Record<string, unknown>): string {
  return JSON.stringify({
    provider_name: providerName,
    provider_config: providerConfig,
  });
}

export function ProviderDetailsModal({
  graph,
  node,
  catalog,
  onGraphChange,
  onClose,
}: ProviderDetailsModalProps) {
  const nodeLabel = getNodeInstanceLabel(graph, node);
  const provider = resolveProviderDefinition(node, catalog);
  const isModelNode = node.kind === "model";
  const availableProviders = modelProviderDefinitions(catalog);
  const envVarEntries = Object.entries(getGraphEnvVars(graph));
  const providerName = isModelNode
    ? String(node.config.provider_name ?? node.model_provider_name ?? "not-set")
    : String(provider?.provider_id ?? node.provider_id ?? "not-set");
  const providerConfigFields = provider?.config_fields ?? [];
  const isSupabaseDataNode = node.provider_id === "core.supabase_data";
  const isSupabaseRowWriteNode = node.provider_id === "core.supabase_row_write";
  const isSupabaseCatalogNode = isSupabaseDataNode || isSupabaseRowWriteNode;
  const displayedUserMessageTemplate =
    node.provider_id === "core.spreadsheet_matrix_decision" &&
    (!String(node.config.user_message_template ?? "").trim() ||
      String(node.config.user_message_template ?? "").trim() === "{input_payload}")
      ? SPREADSHEET_MATRIX_RECOMMENDED_USER_MESSAGE_TEMPLATE
      : String(node.config.user_message_template ?? "{input_payload}");
  const displayedProviderConfigFields = isSupabaseCatalogNode
    ? providerConfigFields.filter((field) => !["supabase_url_env_var", "supabase_key_env_var"].includes(field.key))
    : providerConfigFields;
  const supportsLiveVerification = isModelNode && providerName !== "mock";
  const catalogTools = catalog?.tools ?? [];
  const mcpCatalogTools = catalogTools.filter((tool) => tool.source_type === "mcp");
  const standardCatalogTools = catalogTools.filter((tool) => tool.source_type !== "mcp");
  const allowedTools = Array.isArray(node.config.allowed_tool_names) ? (node.config.allowed_tool_names as string[]) : [];
  const selectedModelResponseMode = inferModelResponseMode(graph, node);
  const responseSchemaDetails = resolveResponseSchemaDetails(node.config as Record<string, unknown>);
  const mcpToolByName = new Map<string, ToolDefinition>();
  for (const tool of mcpCatalogTools) {
    for (const identifier of [toolCanonicalName(tool), tool.name, ...(tool.aliases ?? [])]) {
      const normalizedIdentifier = String(identifier).trim();
      if (normalizedIdentifier && !mcpToolByName.has(normalizedIdentifier)) {
        mcpToolByName.set(normalizedIdentifier, tool);
      }
    }
  }
  const connectedMcpContextNodes = node.kind === "model" ? getModelMcpContextNodes(graph, node) : [];
  const modelPromptBlockNodes = node.kind === "model" ? getModelPromptBlockNodes(graph, node) : [];
  const modelTargetedMcpNodeIds =
    node.kind === "model" && Array.isArray(node.config.tool_target_node_ids)
      ? uniqueStrings(node.config.tool_target_node_ids.map((nodeId) => String(nodeId)))
      : [];
  const modelPromptContextProviders = uniqueStrings(
    connectedMcpContextNodes
      .filter((contextNode) => Boolean(contextNode.config.include_mcp_tool_context))
      .map((contextNode) => getNodeInstanceLabel(graph, contextNode)),
  );
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
      const matchingCatalogTool = catalogTools.find((tool) => toolMatchesReference(tool, toolName));
      if (
        candidateToolNames.some(
          (configuredName) =>
            configuredName.trim() === toolName.trim() || (matchingCatalogTool ? toolMatchesReference(matchingCatalogTool, configuredName) : false),
        )
      ) {
        return candidate;
      }
    }
    return null;
  };
  const promptContextToolSummaries = node.kind === "model"
    ? connectedMcpContextNodes
        .flatMap((contextNode) => {
          if (!contextNode.config.include_mcp_tool_context) {
            return [];
          }
          const nodeToolNames = Array.isArray(contextNode.config.tool_names)
            ? contextNode.config.tool_names.map((toolName) => String(toolName)).filter((toolName) => toolName.trim().length > 0)
            : [];
          return nodeToolNames.map((toolName) => {
            const tool = mcpToolByName.get(toolName) ?? null;
            const canonicalName = tool ? toolCanonicalName(tool) : toolName;
            const overrideNode = findPromptOverrideNodeForTool(canonicalName) ?? contextNode;
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
            return {
              sourceNodeLabel: getNodeInstanceLabel(graph, contextNode),
              displayName: tool ? toolLabel(tool) : canonicalName,
              toolName: canonicalName,
              status: tool ? toolStatusLabel(tool) : "unknown",
              usageHint: String(contextNode.config.usage_hint ?? "").trim(),
              renderedPromptText: resolvedDetails.renderedPromptText,
              placeholderToken: "",
            };
          });
        })
        .map((tool, index) => ({ ...tool, placeholderToken: mcpToolPlaceholderToken(index) }))
    : [];
  const mcpToolContextPrompt = promptContextToolSummaries.map((tool) => tool.renderedPromptText.trim()).filter(Boolean).join("\n\n");
  const callableMcpToolNames = node.kind === "model"
    ? uniqueStrings(
        connectedMcpContextNodes.flatMap((contextNode) => {
          if (contextNode.config.expose_mcp_tools === false) {
            return [];
          }
          const nodeToolNames = Array.isArray(contextNode.config.tool_names)
            ? contextNode.config.tool_names.map((toolName) => String(toolName)).filter((toolName) => toolName.trim().length > 0)
            : [];
          return nodeToolNames
            .map((toolName) => mcpToolByName.get(toolName))
            .filter((tool): tool is ToolDefinition => tool !== undefined)
            .filter((tool) => toolStatusLabel(tool) === "ready")
            .map((tool) => toolCanonicalName(tool));
        }),
      ).sort()
    : [];
  const mcpToolGuidance = node.kind === "model"
    ? connectedMcpContextNodes
        .map((contextNode) => {
          const usageHint = String(contextNode.config.usage_hint ?? "").trim();
          if (!usageHint || !contextNode.config.include_mcp_tool_context) {
            return "";
          }
          const toolNames = (Array.isArray(contextNode.config.tool_names) ? contextNode.config.tool_names : [])
            .map((toolName) => String(toolName))
            .filter((toolName) => toolName.trim().length > 0)
            .map((toolName) => mcpToolByName.get(toolName))
            .filter((tool): tool is ToolDefinition => tool !== undefined)
            .map((tool) => toolLabel(tool));
          if (toolNames.length === 0) {
            return "";
          }
          return [`Tools: ${uniqueStrings(toolNames).join(", ")}`, "Guidance:", usageHint].join("\n");
        })
        .filter(Boolean)
        .join("\n\n")
    : "";
  const systemPromptTemplate = String(node.config.system_prompt ?? "");
  const systemPromptTokens = extractTemplateTokens(systemPromptTemplate);
  const mcpToolGuidanceBlock = buildMcpToolGuidanceBlock(callableMcpToolNames, mcpToolGuidance);
  const mcpToolContextBlock = mcpToolContextPrompt.trim().length > 0 ? `MCP Tool Context\n${mcpToolContextPrompt}` : "";
  const requiredMcpPlaceholders = uniqueStrings([
    ...(mcpToolGuidanceBlock ? ["mcp_tool_guidance_block"] : []),
    ...(mcpToolContextBlock ? ["mcp_tool_context_block"] : []),
    ...promptContextToolSummaries.map((tool) => tool.placeholderToken),
  ]);
  const availableSystemPromptPlaceholders = uniqueStrings([
    ...systemPromptTokens,
    ...Object.keys(getGraphEnvVars(graph)),
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
    ...promptContextToolSummaries.map((tool) => tool.placeholderToken),
  ]);
  const optionalSystemPromptPlaceholders = availableSystemPromptPlaceholders.filter(
    (token) => !requiredMcpPlaceholders.includes(token),
  );
  const modelGeneratedMcpPlaceholderTemplate = buildMcpToolPlaceholderTemplate(promptContextToolSummaries);
  const systemPromptPreviewVariables: Record<string, string> = {
    documents: "[]",
    input_payload: "",
    run_id: "",
    graph_id: graph.graph_id,
    current_node_id: node.id,
    available_tools: JSON.stringify(
      promptContextToolSummaries.map((tool) => ({
        name: tool.toolName,
        description: tool.displayName,
        status: tool.status,
      })),
      null,
      2,
    ),
    mcp_available_tool_names: JSON.stringify(callableMcpToolNames, null, 2),
    mcp_tool_context: JSON.stringify(
      {
        tool_names: promptContextToolSummaries.map((tool) => tool.toolName),
        prompt_blocks: promptContextToolSummaries.map((tool) => tool.renderedPromptText),
        usage_hints_text: mcpToolGuidance,
      },
      null,
      2,
    ),
    mcp_tool_context_prompt: mcpToolContextPrompt,
    mcp_tool_context_block: mcpToolContextBlock,
    mcp_tool_guidance: mcpToolGuidance,
    mcp_tool_guidance_block: mcpToolGuidanceBlock,
    mode: String(node.config.mode ?? node.prompt_name ?? ""),
    preferred_tool_name: String(node.config.preferred_tool_name ?? ""),
    response_mode: String(node.config.response_mode ?? "auto"),
    prompt_blocks: "[]",
    ...Object.fromEntries(promptContextToolSummaries.map((tool) => [tool.placeholderToken, tool.renderedPromptText])),
  };
  const systemPromptTemplatePreview = resolveGraphEnvReferences(systemPromptTemplate, graph, systemPromptPreviewVariables);
  const promptAssemblySections = node.kind === "model"
    ? (() => {
        const sections: string[] = [];
        const hasInlineMcpGuidanceBlock = systemPromptTemplate.includes("{mcp_tool_guidance_block}");
        const hasInlineMcpContextCoverage =
          systemPromptTemplate.includes("{mcp_tool_context_block}")
          || systemPromptTemplate.includes("{mcp_tool_context_prompt}")
          || (
            promptContextToolSummaries.length > 0
            && promptContextToolSummaries.every((tool) => systemPromptTemplate.includes(`{${tool.placeholderToken}}`))
          );
        if (mcpToolGuidanceBlock && !hasInlineMcpGuidanceBlock) {
          sections.push(mcpToolGuidanceBlock);
        }
        if (mcpToolContextBlock && !hasInlineMcpContextCoverage) {
          sections.push(mcpToolContextBlock);
        }
        const contract = buildPromptOnlyMcpToolDecisionContract(promptContextToolSummaries.length > 0, callableMcpToolNames);
        if (contract) {
          sections.push(contract);
        }
        return sections;
      })()
    : [];
  const finalSystemPromptPreview =
    node.kind === "model"
      ? [systemPromptTemplatePreview.trim(), ...promptAssemblySections.map((section) => section.trim()).filter(Boolean)]
          .filter(Boolean)
          .join("\n\n")
      : "";
  const [activeTab, setActiveTab] = useState<ProviderDetailsModalTab>("node");
  const [preflightResult, setPreflightResult] = useState<ProviderPreflightResult | null>(null);
  const [diagnostics, setDiagnostics] = useState<ProviderDiagnosticsResult | null>(null);
  const [preflightError, setPreflightError] = useState<string | null>(null);
  const [isPreflighting, setIsPreflighting] = useState(false);
  const [supabaseRuntimeStatus, setSupabaseRuntimeStatus] = useState<SupabaseRuntimeStatusResult | null>(null);
  const [isLoadingSupabaseRuntimeStatus, setIsLoadingSupabaseRuntimeStatus] = useState(false);
  const [supabaseSchemaPreview, setSupabaseSchemaPreview] = useState<SupabaseSchemaPreviewResult | null>(null);
  const [supabaseSchemaError, setSupabaseSchemaError] = useState<string | null>(null);
  const [isLoadingSupabaseSchema, setIsLoadingSupabaseSchema] = useState(false);
  const [supabaseSourceSearch, setSupabaseSourceSearch] = useState("");
  const [selectedSupabaseSourceName, setSelectedSupabaseSourceName] = useState<string>("");

  useEffect(() => {
    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        onClose();
      }
    }

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [onClose]);

  const preflightConfig = useMemo<Record<string, unknown>>(() => {
    if (!isModelNode) {
      return Object.fromEntries(providerConfigFields.map((field) => [field.key, node.config[field.key]]));
    }
    const entries: Array<[string, unknown]> = [["provider_name", providerName]];
    providerConfigFields.forEach((field) => {
      entries.push([field.key, node.config[field.key]]);
    });
    return Object.fromEntries(entries);
  }, [isModelNode, node.config, providerConfigFields, providerName]);
  const verificationStorageKey = useMemo(
    () => buildProviderVerificationStorageKey(providerName, preflightConfig),
    [preflightConfig, providerName],
  );
  const [persistedVerification, setPersistedVerification] = useState<PersistedProviderVerification | null>(null);

  useEffect(() => {
    setPersistedVerification(getPersistedProviderVerification(verificationStorageKey));
  }, [verificationStorageKey]);

  useEffect(() => {
    setActiveTab("node");
  }, [node.id, providerName]);

  useEffect(() => {
    let cancelled = false;
    if (!isModelNode || !providerName || providerName === "not-set") {
      setPreflightResult(null);
      setDiagnostics(null);
      setPreflightError(null);
      return () => {
        cancelled = true;
      };
    }

    setIsPreflighting(true);
    setPreflightError(null);
    Promise.all([
      preflightProvider(providerName, preflightConfig, false),
      fetchProviderDiagnostics(providerName, preflightConfig, false),
    ])
      .then(([result, diagnosticsResult]) => {
        if (!cancelled) {
          setPreflightResult(result);
          setDiagnostics(diagnosticsResult);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setPreflightError("Unable to load provider health.");
          setPreflightResult(null);
          setDiagnostics(null);
        }
      })
      .finally(() => {
        if (!cancelled) {
          setIsPreflighting(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [isModelNode, preflightConfig, providerName]);

  const displayedPreflightResult = useMemo(() => {
    if (
      preflightResult?.status === "installed" &&
      diagnostics?.authentication_status === "not_checked" &&
      persistedVerification?.diagnostics.active_backend === diagnostics.active_backend
    ) {
      return persistedVerification.preflightResult;
    }
    return preflightResult;
  }, [diagnostics, persistedVerification, preflightResult]);

  const displayedDiagnostics = useMemo(() => {
    if (
      diagnostics &&
      diagnostics.authentication_status === "not_checked" &&
      persistedVerification?.diagnostics.active_backend === diagnostics.active_backend
    ) {
      return {
        ...diagnostics,
        authentication_status: persistedVerification.diagnostics.authentication_status,
        preflight: persistedVerification.preflightResult,
      };
    }
    return diagnostics;
  }, [diagnostics, persistedVerification]);

  function updateProviderConfig(key: string, value: string | number | boolean) {
    onGraphChange(
      updateModelNode(graph, node.id, (currentNode) => ({
        ...currentNode,
        config: {
          ...currentNode.config,
          [key]: value,
        },
      })),
    );
  }

  function updateResponseSchemaText(nextText: string) {
    const { parsedSchema } = parseResponseSchemaText(nextText);
    onGraphChange(
      updateModelNode(graph, node.id, (currentNode) => {
        const nextConfig = { ...currentNode.config } as Record<string, unknown>;
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
        return { ...currentNode, config: nextConfig };
      }),
    );
  }

  function handleOverlayClick(event: MouseEvent<HTMLDivElement>) {
    if (event.target === event.currentTarget) {
      onClose();
    }
  }

  function handleTextInputChange(key: string) {
    return (event: ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>) => {
      updateProviderConfig(key, event.target.value);
    };
  }

  function handleNumberInputChange(key: string) {
    return (event: ChangeEvent<HTMLInputElement>) => {
      updateProviderConfig(key, event.target.value === "" ? "" : Number(event.target.value));
    };
  }

  function handleProviderChange(nextProviderName: string) {
    const nextProvider = findProviderDefinition(catalog, nextProviderName);
    if (!nextProvider) {
      return;
    }
    const nextProviderConfig = providerDefaultConfig(nextProvider);
    const providerConfigKeys = Array.from(
      new Set(
        availableProviders.flatMap((candidate) => [
          "provider_name",
          ...((candidate.config_fields ?? []).map((field) => field.key)),
        ]),
      ),
    );
    onGraphChange(
      updateModelNode(graph, node.id, (currentNode) => {
        const nextConfig = { ...currentNode.config };
        providerConfigKeys.forEach((key) => delete nextConfig[key]);
        return {
          ...currentNode,
          model_provider_name: nextProviderName,
          config: {
            ...nextConfig,
            ...nextProviderConfig,
            provider_name: nextProviderName,
          },
        };
      }),
    );
  }

  async function handleLiveVerification() {
    setIsPreflighting(true);
    setPreflightError(null);
    try {
      const [result, diagnosticsResult] = await Promise.all([
        preflightProvider(providerName, preflightConfig, true),
        fetchProviderDiagnostics(providerName, preflightConfig, true),
      ]);
      setPreflightResult(result);
      setDiagnostics(diagnosticsResult);
      const verification = { preflightResult: result, diagnostics: diagnosticsResult };
      persistProviderVerification(verificationStorageKey, verification);
      setPersistedVerification(verification);
    } catch {
      setPreflightError("Live provider verification failed.");
      setPreflightResult(null);
      setDiagnostics(null);
    } finally {
      setIsPreflighting(false);
    }
  }

  const resolvedPreviewConfig = Object.fromEntries(
    [["provider_name", providerName], ...providerConfigFields.map((field) => [field.key, node.config[field.key]])].map(
      ([key, value]) => [
        key,
        typeof value === "string" ? resolveGraphEnvReferences(value, graph) || null : (value ?? null),
      ],
    ),
  );
  const supabaseSourceKind = String(node.config.source_kind ?? "table") || "table";
  const supabaseOutputMode = String(node.config.output_mode ?? "records") || "records";
  const supabaseFilterLines = String(node.config.filters_text ?? "")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
  const supabaseUrlEnvVarName = String(node.config.supabase_url_env_var ?? "GRAPH_AGENT_SUPABASE_URL") || "GRAPH_AGENT_SUPABASE_URL";
  const supabaseKeyEnvVarName =
    String(node.config.supabase_key_env_var ?? "GRAPH_AGENT_SUPABASE_SECRET_KEY") || "GRAPH_AGENT_SUPABASE_SECRET_KEY";
  const localSupabaseUrlValue = normalizedGraphEnvValue(graph, supabaseUrlEnvVarName);
  const localSupabaseKeyValue = normalizedGraphEnvValue(graph, supabaseKeyEnvVarName);
  const hasLocalSupabaseRuntimeValues = Boolean(localSupabaseUrlValue && localSupabaseKeyValue);
  const hasSupabaseSchemaMemory = Boolean((supabaseSchemaPreview?.sources.length ?? 0) > 0);
  const isSupabaseRuntimeReady = hasLocalSupabaseRuntimeValues || supabaseRuntimeStatus?.ready === true;
  const isSupabaseRuntimePending = isSupabaseCatalogNode && supabaseRuntimeStatus === null && isLoadingSupabaseRuntimeStatus;
  const isSupabaseBrowserLocked = isSupabaseCatalogNode && !isSupabaseRuntimeReady && !hasSupabaseSchemaMemory;

  useEffect(() => {
    if (
      !isSupabaseCatalogNode ||
      activeTab !== "config" ||
      supabaseSchemaPreview !== null ||
      isLoadingSupabaseSchema ||
      !isSupabaseRuntimeReady
    ) {
      return;
    }
    void handleLoadSupabaseSchema();
  }, [activeTab, isLoadingSupabaseSchema, isSupabaseCatalogNode, isSupabaseRuntimeReady, supabaseSchemaPreview]);

  const filteredSupabaseSources = useMemo(() => {
    const sources = (supabaseSchemaPreview?.sources ?? []).filter((source) => !isSupabaseRowWriteNode || source.source_kind === "table");
    const query = supabaseSourceSearch.trim().toLowerCase();
    if (!query) {
      return sources;
    }
    return sources.filter((source) => {
      const haystack = [
        source.name,
        source.description,
        ...source.columns.map((column) => `${column.name} ${column.data_type} ${column.description}`),
      ]
        .join(" ")
        .toLowerCase();
      return haystack.includes(query);
    });
  }, [supabaseSchemaPreview, supabaseSourceSearch]);
  const selectedSupabaseSource = useMemo<SupabaseSchemaSource | null>(() => {
    const availableSources = supabaseSchemaPreview?.sources ?? [];
    if (availableSources.length === 0) {
      return null;
    }
    if (selectedSupabaseSourceName) {
      return availableSources.find((source) => source.name === selectedSupabaseSourceName) ?? null;
    }
    const configuredSourceName = String(isSupabaseRowWriteNode ? node.config.table_name ?? "" : node.config.source_name ?? "").trim();
    if (configuredSourceName) {
      return availableSources.find((source) => source.name === configuredSourceName) ?? null;
    }
    return availableSources[0] ?? null;
  }, [isSupabaseRowWriteNode, node.config.source_name, node.config.table_name, selectedSupabaseSourceName, supabaseSchemaPreview]);
  const selectedSupabaseColumnNames = useMemo(() => {
    if (isSupabaseRowWriteNode) {
      return [];
    }
    const availableColumns = selectedSupabaseSource?.columns.map((column) => column.name) ?? [];
    return parseSupabaseSelect(String(node.config.select ?? "*"), availableColumns);
  }, [isSupabaseRowWriteNode, node.config.select, selectedSupabaseSource]);

  useEffect(() => {
    if (!isSupabaseCatalogNode) {
      setSupabaseRuntimeStatus(null);
      setIsLoadingSupabaseRuntimeStatus(false);
      setSupabaseSchemaPreview(null);
      setSupabaseSchemaError(null);
      setIsLoadingSupabaseSchema(false);
      setSupabaseSourceSearch("");
      setSelectedSupabaseSourceName("");
      return;
    }
    const cachedSchema = loadSessionSupabaseSchema(graph);
    if (cachedSchema) {
      setSupabaseSchemaPreview(cachedSchema);
    }
    setSelectedSupabaseSourceName(String(isSupabaseRowWriteNode ? node.config.table_name ?? "" : node.config.source_name ?? "").trim());
  }, [graph, isSupabaseCatalogNode, isSupabaseRowWriteNode, node.config.source_name, node.config.table_name]);

  useEffect(() => {
    if (!isSupabaseCatalogNode) {
      return;
    }
    setSupabaseRuntimeStatus(null);
    setSupabaseSchemaError(null);
    const cachedSchema = loadSessionSupabaseSchema(graph);
    setSupabaseSchemaPreview(cachedSchema);
  }, [graph, isSupabaseCatalogNode, graph.env_vars, supabaseKeyEnvVarName, supabaseUrlEnvVarName]);

  useEffect(() => {
    if (!isSupabaseCatalogNode || activeTab !== "config" || supabaseRuntimeStatus !== null || isLoadingSupabaseRuntimeStatus) {
      return;
    }
    void handleLoadSupabaseRuntimeStatus();
  }, [activeTab, isLoadingSupabaseRuntimeStatus, isSupabaseCatalogNode, supabaseRuntimeStatus, supabaseKeyEnvVarName, supabaseUrlEnvVarName]);

  async function handleLoadSupabaseSchema() {
    if (!isSupabaseCatalogNode || !isSupabaseRuntimeReady) {
      return;
    }
    setIsLoadingSupabaseSchema(true);
    setSupabaseSchemaError(null);
    try {
      const result = await previewSupabaseSchema({
        supabase_url_env_var: String(resolvedPreviewConfig.supabase_url_env_var ?? "GRAPH_AGENT_SUPABASE_URL"),
        supabase_key_env_var: String(resolvedPreviewConfig.supabase_key_env_var ?? "GRAPH_AGENT_SUPABASE_SECRET_KEY"),
        schema: String(resolvedPreviewConfig.schema ?? "public") || "public",
        graph_env_vars: getGraphEnvVars(graph),
      });
      saveSessionSupabaseSchema(graph, result);
      setSupabaseSchemaPreview(result);
      const configuredSourceName = String(node.config.source_name ?? "").trim();
      const firstSource = result.sources[0]?.name ?? "";
      setSelectedSupabaseSourceName(configuredSourceName || firstSource);
    } catch (error) {
      setSupabaseSchemaPreview(null);
      setSupabaseSchemaError(error instanceof Error ? error.message : "Failed to load Supabase schema.");
    } finally {
      setIsLoadingSupabaseSchema(false);
    }
  }

  async function handleLoadSupabaseRuntimeStatus() {
    if (!isSupabaseCatalogNode) {
      return;
    }
    if (hasLocalSupabaseRuntimeValues) {
      setSupabaseRuntimeStatus({
        supabase_url_env_var: supabaseUrlEnvVarName,
        supabase_key_env_var: supabaseKeyEnvVarName,
        supabase_url_env_present: true,
        supabase_key_env_present: true,
        missing_env_vars: [],
        ready: true,
      });
      return;
    }
    setIsLoadingSupabaseRuntimeStatus(true);
    try {
      const result = await inspectSupabaseRuntimeStatus({
        supabase_url_env_var: supabaseUrlEnvVarName,
        supabase_key_env_var: supabaseKeyEnvVarName,
        graph_env_vars: getGraphEnvVars(graph),
      });
      setSupabaseRuntimeStatus(result);
    } catch (error) {
      setSupabaseRuntimeStatus({
        supabase_url_env_var: supabaseUrlEnvVarName,
        supabase_key_env_var: supabaseKeyEnvVarName,
        supabase_url_env_present: false,
        supabase_key_env_present: false,
        missing_env_vars: [supabaseUrlEnvVarName, supabaseKeyEnvVarName].filter(Boolean),
        ready: false,
      });
      setSupabaseSchemaError(error instanceof Error ? error.message : "Failed to inspect Supabase runtime environment.");
    } finally {
      setIsLoadingSupabaseRuntimeStatus(false);
    }
  }

  function applySupabaseSourceSelection(source: SupabaseSchemaSource, nextColumns?: string[]) {
    onGraphChange(
      updateModelNode(graph, node.id, (currentNode) => ({
        ...currentNode,
        config: {
          ...currentNode.config,
          ...(isSupabaseRowWriteNode
            ? {
                table_name: source.name,
              }
            : (() => {
                const availableColumns = source.columns.map((column) => column.name);
                const resolvedColumns = nextColumns ?? parseSupabaseSelect(String(node.config.select ?? "*"), availableColumns);
                return {
                  source_kind: source.source_kind,
                  source_name: source.name,
                  select:
                    resolvedColumns.length === 0 || resolvedColumns.length === availableColumns.length
                      ? "*"
                      : resolvedColumns.join(","),
                };
              })()),
        },
      })),
    );
    setSelectedSupabaseSourceName(source.name);
  }

  function toggleSupabaseColumn(columnName: string) {
    if (!selectedSupabaseSource || isSupabaseRowWriteNode) {
      return;
    }
    const availableColumns = selectedSupabaseSource.columns.map((column) => column.name);
    const nextSet = new Set(parseSupabaseSelect(String(node.config.select ?? "*"), availableColumns));
    if (nextSet.has(columnName)) {
      nextSet.delete(columnName);
    } else {
      nextSet.add(columnName);
    }
    applySupabaseSourceSelection(selectedSupabaseSource, availableColumns.filter((column) => nextSet.has(column)));
  }

  return (
    <div className="tool-details-modal-backdrop" onClick={handleOverlayClick} role="presentation">
      <section
        className="tool-details-modal provider-details-modal-shell"
        role="dialog"
        aria-modal="true"
        aria-labelledby="provider-details-modal-title"
      >
        <div className="tool-details-modal-header provider-details-modal-header">
          <div className="provider-details-modal-title-block">
            <div className="tool-details-modal-eyebrow">{isModelNode ? "API Provider Details" : "Node Provider Details"}</div>
            <h3 id="provider-details-modal-title">
              {nodeLabel}
              {provider ? ` · ${provider.display_name}` : ""}
            </h3>
            <p>
              {isModelNode
                ? "Required provider selection stays on the API node. Use this modal to review provider capabilities and tune optional provider parameters and prompt instructions for the selected API step."
                : "Use this modal to review the node provider capabilities and edit provider-specific configuration for this step."}
            </p>
            <div className="provider-details-modal-meta">
              <span className="provider-details-modal-meta-pill">provider {provider?.display_name ?? providerName}</span>
              <span className="provider-details-modal-meta-pill">node {node.kind}</span>
              {isModelNode ? <span className="provider-details-modal-meta-pill">response {selectedModelResponseMode ?? "message"}</span> : null}
              {isModelNode ? <span className="provider-details-modal-meta-pill">{responseSchemaDetails.statusLabel}</span> : null}
            </div>
          </div>
          <button type="button" className="secondary-button" onClick={onClose}>
            Close
          </button>
        </div>

        <div className="tool-details-modal-body">
          <div className="modal-folder-tabs" role="tablist" aria-label="Provider details sections">
            {[
              ["node", "Node"],
              ["overview", "Overview"],
              ...(isModelNode ? [["prompt", "Prompt"], ["routing", "Routing"]] as Array<[string, string]> : []),
              ["config", "Config"],
              ["preview", "Preview"],
            ].map(([tabId, label]) => (
              <button
                key={tabId}
                type="button"
                role="tab"
                aria-selected={activeTab === tabId}
                className={`modal-folder-tab ${activeTab === tabId ? "modal-folder-tab--active" : ""}`}
                onClick={() => setActiveTab(tabId as ProviderDetailsModalTab)}
              >
                {label}
              </button>
            ))}
          </div>

          <div className={`modal-folder-panel provider-details-panel provider-details-panel--${activeTab}`}>
            {activeTab === "node" ? (
              <div className="modal-folder-section provider-details-node-layout">
                <NodeDetailsForm
                  graph={graph}
                  node={node}
                  catalog={catalog}
                  onGraphChange={onGraphChange}
                />
                {isModelNode ? (
                  <>
                    <label>
                      Prompt Name
                      <input
                        value={String(node.config.prompt_name ?? node.prompt_name ?? "")}
                        onChange={(event) =>
                          onGraphChange(
                            updateModelNode(graph, node.id, (currentNode) => ({
                              ...currentNode,
                              prompt_name: event.target.value,
                              config: {
                                ...currentNode.config,
                                prompt_name: event.target.value,
                                mode: event.target.value,
                              },
                            })),
                          )
                        }
                      />
                    </label>
                    <label>
                      Model Provider Name
                      <input
                        value={String(node.config.provider_name ?? node.model_provider_name ?? "")}
                        onChange={(event) =>
                          onGraphChange(
                            updateModelNode(graph, node.id, (currentNode) => ({
                              ...currentNode,
                              model_provider_name: event.target.value,
                              config: {
                                ...currentNode.config,
                                provider_name: event.target.value,
                              },
                            })),
                          )
                        }
                      />
                    </label>
                  </>
                ) : (
                  <label>
                    Provider ID
                    <input value={String(provider?.provider_id ?? node.provider_id ?? "")} readOnly />
                  </label>
                )}
              </div>
            ) : null}

            {activeTab === "overview" ? (
              <div className="modal-folder-section provider-details-overview-layout">
                <section className="provider-details-summary provider-details-summary--hero">
                  <div className="provider-details-summary-header">
                    <strong>Selected Provider</strong>
                    <span>{provider?.display_name ?? providerName}</span>
                  </div>
                  <p>{provider?.description ?? "No provider description is available for the current selection."}</p>
                  {provider?.capabilities.length ? (
                    <div className="provider-details-capabilities">
                      {provider.capabilities.map((capability) => (
                        <span key={capability} className="provider-capability-chip">
                          {capability}
                        </span>
                      ))}
                    </div>
                  ) : null}
                </section>
                {isSupabaseDataNode ? (
                  <section className="provider-details-summary">
                    <div className="provider-details-summary-header">
                      <strong>Supabase Query Shape</strong>
                      <span>{supabaseSourceKind === "rpc" ? "RPC call" : "Table or view read"}</span>
                    </div>
                    <p>
                      This provider is for deterministic data loading from a fixed Supabase source. It uses the graph&apos;s saved
                      Supabase credentials from the Environment section, issues one read against the configured source, and emits
                      a normal data envelope that downstream Context Builder or model nodes can reuse.
                    </p>
                    <div className="provider-details-capabilities">
                      <span className="provider-capability-chip">source {String(node.config.source_name ?? "not set") || "not set"}</span>
                      <span className="provider-capability-chip">output {supabaseOutputMode}</span>
                      <span className="provider-capability-chip">limit {String(node.config.limit ?? 25)}</span>
                      <span className="provider-capability-chip">
                        {Boolean(node.config.single_row) ? "single row" : "multi row"}
                      </span>
                    </div>
                  </section>
                ) : null}
                <aside className="provider-details-overview-side">
                  <section className="provider-details-status-card">
                    <div className="provider-details-status-card-header">
                      <strong>{isModelNode ? "Health Check" : "Provider Status"}</strong>
                      <span>{isModelNode ? (supportsLiveVerification ? "Live capable" : "Mock / local only") : "Node-configured"}</span>
                    </div>
                    {isModelNode && displayedPreflightResult ? (
                      <div className="tool-details-modal-help">
                        <strong>Provider Health</strong>
                        <div>{displayedPreflightResult.message}</div>
                        {displayedPreflightResult.warnings?.map((warning) => (
                          <div key={warning}>{warning}</div>
                        ))}
                      </div>
                    ) : null}
                    {isModelNode && preflightError ? <div className="tool-details-modal-help">{preflightError}</div> : null}
                    {isModelNode && !supportsLiveVerification ? (
                      <div className="tool-details-modal-help">Live verification is not required for the mock provider.</div>
                    ) : null}
                    {isModelNode ? (
                      <button
                        type="button"
                        className="secondary-button provider-details-verify-button"
                        onClick={handleLiveVerification}
                        disabled={isPreflighting || !supportsLiveVerification}
                      >
                        {isPreflighting
                          ? "Checking Provider..."
                          : supportsLiveVerification
                            ? "Run Live Verification"
                            : "Live Verification Not Required"}
                      </button>
                  ) : (
                    <div className="tool-details-modal-help">
                      This node provider is configured directly in the graph and does not need live API verification.
                    </div>
                  )}
                  </section>
                  {isSupabaseDataNode ? (
                    <section className="provider-details-status-card">
                      <div className="provider-details-status-card-header">
                        <strong>Runtime Inputs</strong>
                        <span>{isSupabaseRuntimeReady ? "Unlocked" : "Locked"}</span>
                      </div>
                      <div className="tool-details-modal-help">
                        <div>
                          {isSupabaseRuntimeReady
                            ? "This node is using the verified Supabase credentials saved from the hero Environment section."
                            : "Set the Supabase runtime values from the hero Environment section's Supabase button before using the schema browser."}
                        </div>
                        <div>
                          URL env var: <code>{supabaseUrlEnvVarName}</code>
                          {supabaseRuntimeStatus ? ` (${supabaseRuntimeStatus.supabase_url_env_present ? "present" : "missing"})` : ""}
                        </div>
                        <div>
                          Key env var: <code>{supabaseKeyEnvVarName}</code>
                          {supabaseRuntimeStatus ? ` (${supabaseRuntimeStatus.supabase_key_env_present ? "present" : "missing"})` : ""}
                        </div>
                        <div>Schema: <code>{String(node.config.schema ?? "public")}</code></div>
                        <div>Select: <code>{String(node.config.select ?? "*") || "*"}</code></div>
                        {supabaseRuntimeStatus?.missing_env_vars.length ? (
                          <div>Missing: {supabaseRuntimeStatus.missing_env_vars.join(", ")}</div>
                        ) : null}
                      </div>
                    </section>
                  ) : null}
                  {isModelNode && displayedDiagnostics ? (
                    <section className="provider-details-status-card provider-details-status-card--diagnostics">
                      <div className="provider-details-status-card-header">
                        <strong>Diagnostics</strong>
                        <span>{displayedDiagnostics.active_backend}</span>
                      </div>
                      <div className="provider-diagnostics-card">
                        <div className="provider-diagnostics-section">
                          <div className="provider-diagnostics-section-title">Backend</div>
                          <div className="provider-diagnostics-row">
                            <span>Active backend</span>
                            <strong>{displayedDiagnostics.active_backend}</strong>
                          </div>
                          <div className="provider-diagnostics-row">
                            <span>Authentication status</span>
                            <strong>{displayedDiagnostics.authentication_status}</strong>
                          </div>
                        </div>
                        {displayedDiagnostics.active_backend === "claude_code" ? (
                          <div className="provider-diagnostics-section">
                            <div className="provider-diagnostics-section-title">Claude Code</div>
                            <div className="provider-diagnostics-row">
                              <span>Claude binary</span>
                              <strong>{displayedDiagnostics.claude_binary_exists ? "found" : "not found"}</strong>
                            </div>
                          </div>
                        ) : null}
                        {displayedDiagnostics.active_backend === "claude_code" || displayedDiagnostics.active_backend === "anthropic_api" ? (
                          <div className="provider-diagnostics-section">
                            <div className="provider-diagnostics-section-title">Environment</div>
                            <div className="provider-diagnostics-row">
                              <span>`ANTHROPIC_API_KEY` present</span>
                              <strong>{displayedDiagnostics.anthropic_api_key_present ? "yes" : "no"}</strong>
                            </div>
                          </div>
                        ) : null}
                        {displayedDiagnostics.child_env_sanitized ? (
                          <div className="provider-diagnostics-section">
                            <div className="provider-diagnostics-section-title">Child Process</div>
                            <div className="provider-diagnostics-list">
                              <div>Sanitized environment enabled.</div>
                              <div>Strips: {displayedDiagnostics.sanitized_env_removed_vars.join(", ")}</div>
                            </div>
                          </div>
                        ) : null}
                        {displayedDiagnostics.warning ? (
                          <div className="provider-diagnostics-section">
                            <div className="provider-diagnostics-section-title">Warning</div>
                            <div className="provider-diagnostics-list">
                              <div>{displayedDiagnostics.warning}</div>
                            </div>
                          </div>
                        ) : null}
                      </div>
                    </section>
                  ) : null}
                </aside>
              </div>
            ) : null}

            {activeTab === "prompt" && isModelNode ? (
              <div className="modal-folder-section provider-details-prompt-layout">
                <div className="provider-details-prompt-main">
                <label>
                  System Prompt
                  <textarea
                    rows={7}
                    value={String(node.config.system_prompt ?? "")}
                    placeholder="You are a helpful model node."
                    onChange={handleTextInputChange("system_prompt")}
                  />
                  <small>
                    Connected MCP edges already define which tools are in scope. MCP coverage is the only required part
                    here; all other placeholders are optional runtime values.
                  </small>
                </label>

                <label>
                  User Message Template
                  <textarea
                    rows={5}
                    value={displayedUserMessageTemplate}
                    onChange={handleTextInputChange("user_message_template")}
                  />
                </label>
                </div>

                <aside className="provider-details-prompt-sidebar">
                  {promptContextToolSummaries.length > 0 ? (
                    <div className="context-builder-binding-actions">
                      <button
                        type="button"
                        className="secondary-button context-builder-inline-button"
                        onClick={() => updateProviderConfig("system_prompt", modelGeneratedMcpPlaceholderTemplate)}
                      >
                        Build From Connected MCP Tools
                      </button>
                    </div>
                  ) : null}

                  {node.kind === "model" ? (
                    <div className="tool-details-modal-help provider-details-placeholder-panel provider-details-placeholder-panel--prompt">
                    <div className="provider-details-placeholder-header">
                      <strong>Required MCP placeholders</strong>
                      <span>Only needed if you want full inline MCP prompt control.</span>
                    </div>
                    <div className="graph-env-reference-list">
                      {requiredMcpPlaceholders.map((token) => (
                        <code key={token} className="placeholder-chip placeholder-chip--required">{`{${token}}`}</code>
                      ))}
                      {requiredMcpPlaceholders.length === 0 ? <span>None required.</span> : null}
                    </div>
                    <div className="provider-details-placeholder-rule">
                      <span className="provider-details-placeholder-rule-label">Rule</span>
                      <p>
                        Include <code>{"{mcp_tool_guidance_block}"}</code> plus either <code>{"{mcp_tool_context_block}"}</code> or
                        every ordered MCP tool placeholder for full inline MCP control. Missing MCP sections are appended
                        automatically.
                      </p>
                    </div>
                    {promptContextToolSummaries.length > 0 ? (
                      <div className="provider-details-placeholder-subsection">
                        <div className="provider-details-placeholder-subtitle">Ordered MCP tool placeholders</div>
                        <div className="provider-details-placeholder-list">
                          {promptContextToolSummaries.map((tool) => (
                            <span key={tool.placeholderToken}>
                              <code className="placeholder-chip placeholder-chip--optional">{`{${tool.placeholderToken}}`}</code> {tool.displayName} ({tool.status})
                            </span>
                          ))}
                        </div>
                      </div>
                    ) : null}
                    {optionalSystemPromptPlaceholders.length > 0 ? (
                      <div className="provider-details-placeholder-subsection">
                        <div className="provider-details-placeholder-subtitle">Optional placeholders</div>
                        <div className="graph-env-reference-list">
                          {optionalSystemPromptPlaceholders.map((token) => (
                            <code key={token} className="placeholder-chip placeholder-chip--optional">{`{${token}}`}</code>
                          ))}
                        </div>
                      </div>
                    ) : null}
                    </div>
                  ) : null}

                  <div className="tool-details-modal-help provider-details-placeholder-panel provider-details-placeholder-panel--env">
                    <div className="provider-details-placeholder-header">
                      <strong>Graph Variables</strong>
                      <span>Available in prompt fields and inserted at runtime.</span>
                    </div>
                    <div className="graph-env-reference-list">
                      {envVarEntries.map(([key, value]) => (
                        <code key={key} title={value}>
                          {`{${key}}`}
                        </code>
                      ))}
                    </div>
                  </div>
                </aside>
              </div>
            ) : null}

            {activeTab === "routing" && isModelNode ? (
              <div className="modal-folder-section provider-details-routing-layout">
                <div className="provider-details-routing-main">
                <label>
                  Response Mode
                  <select
                    value={String(node.config.response_mode ?? "auto") || "auto"}
                    onChange={handleTextInputChange("response_mode")}
                  >
                    <option value="auto">auto</option>
                    <option value="tool_call">tool_call</option>
                    <option value="message">message</option>
                  </select>
                  <small>
                    Choose a fixed mode or leave it on <code>auto</code> to follow graph wiring. Current effective mode:{" "}
                    <code>{selectedModelResponseMode ?? "message"}</code>.
                  </small>
                </label>

                <label>
                  Intended Output Schema
                  <div className="tool-details-modal-help provider-details-placeholder-panel">
                    <div className="provider-details-placeholder-header">
                      <strong>Schema Boilerplate</strong>
                      <span>Start from a common output shape, then customize the JSON schema below.</span>
                    </div>
                    <div className="provider-schema-boilerplate-list">
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
                          className="provider-schema-boilerplate-card"
                          onClick={() => updateResponseSchemaText(preset.schemaText)}
                        >
                          <strong>{preset.label}</strong>
                          <span>{preset.description}</span>
                        </button>
                      ))}
                    </div>
                  </div>
                  <textarea
                    rows={10}
                    className="tool-details-modal-code"
                    value={responseSchemaDetails.schemaText}
                    placeholder='Leave blank to allow any JSON value, or define a JSON Schema object like {"type":"object","properties":{...}}'
                    onChange={(event) => updateResponseSchemaText(event.target.value)}
                    spellCheck={false}
                  />
                  <small>
                    Optional JSON Schema for the final <code>message</code> payload this API block emits.
                  </small>
                </label>
                {responseSchemaDetails.schemaError ? (
                  <p className="error-text">Schema JSON error: {responseSchemaDetails.schemaError}</p>
                ) : null}

                <div className="contract-card">
                  <strong>Output Schema</strong>
                  <span>Status: {responseSchemaDetails.statusLabel}</span>
                  <span>Applies whenever this API block emits a final message.</span>
                </div>

                <div className="checkbox-grid provider-details-tool-grid">
                  <strong>Direct Registry Tools</strong>
                  {standardCatalogTools.map((tool) => {
                    const canonicalName = toolCanonicalName(tool);
                    const isChecked = allowedTools.some((name) => toolMatchesReference(tool, name));
                    const canSelectTool = tool.enabled !== false && tool.available !== false;
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
                              updateModelNode(graph, node.id, (currentNode) => ({
                                ...currentNode,
                                config: {
                                  ...currentNode.config,
                                  allowed_tool_names: nextTools,
                                  preferred_tool_name:
                                    nextTools.length > 0 ? String(currentNode.config.preferred_tool_name ?? nextTools[0]) : "",
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

                <label>
                  Preferred Tool Name
                  <input
                    value={String(node.config.preferred_tool_name ?? "")}
                    onChange={handleTextInputChange("preferred_tool_name")}
                  />
                </label>
                </div>

                <aside className="provider-details-routing-sidebar">
                <div className="contract-card provider-details-accent-card provider-details-accent-card--violet">
                  <strong>MCP Tools From Context Providers</strong>
                  <span>Callable MCP tools: {callableMcpToolNames.length > 0 ? callableMcpToolNames.join(", ") : "None"}</span>
                  <span>Prompt context sources: {modelPromptContextProviders.length > 0 ? modelPromptContextProviders.join(", ") : "None"}</span>
                  {modelTargetedMcpNodeIds.length > 0 ? (
                    <span>Targeted MCP provider IDs: {modelTargetedMcpNodeIds.join(", ")}</span>
                  ) : (
                    <span>MCP tools are supplied through connected or targeted MCP Context Provider nodes.</span>
                  )}
                </div>

                <div className="contract-card provider-details-accent-card provider-details-accent-card--blue">
                  <strong>Bound Prompt Blocks</strong>
                  <span>
                    Direct prompt messages:{" "}
                    {modelPromptBlockNodes.length > 0
                      ? modelPromptBlockNodes.map((promptNode) => `${getNodeInstanceLabel(graph, promptNode)} (${String(promptNode.config.role ?? "user")})`).join(", ")
                      : "None"}
                  </span>
                  <span>Bind Prompt Block nodes into the model to inject additional system, user, or assistant messages before the standard user template.</span>
                </div>

                <div className="contract-card provider-details-accent-card provider-details-accent-card--amber">
                  <strong>System Prompt Assembly</strong>
                  <span>Template placeholders: {systemPromptTokens.length > 0 ? systemPromptTokens.join(", ") : "None"}</span>
                  <span>
                    Required MCP guidance:{" "}
                    {mcpToolGuidanceBlock.length > 0
                      ? systemPromptTemplate.includes("{mcp_tool_guidance_block}")
                        ? "inline"
                        : "auto-appended"
                      : "not needed"}
                  </span>
                  <span>
                    Required MCP context:{" "}
                    {promptContextToolSummaries.length > 0
                      ? systemPromptTemplate.includes("{mcp_tool_context_block}")
                        || systemPromptTemplate.includes("{mcp_tool_context_prompt}")
                        || promptContextToolSummaries.every((tool) => systemPromptTemplate.includes(`{${tool.placeholderToken}}`))
                        ? "inline"
                        : "auto-appended"
                      : "not needed"}
                  </span>
                  <span>
                    Prompt block messages stay separate from the system prompt:{" "}
                    {modelPromptBlockNodes.length > 0 ? `${modelPromptBlockNodes.length} bound block${modelPromptBlockNodes.length === 1 ? "" : "s"}` : "None"}
                  </span>
                </div>
                </aside>

                {modelGeneratedMcpPlaceholderTemplate ? (
                  <section className="tool-details-modal-preview provider-details-routing-preview">
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

                <div className="checkbox-grid provider-details-placeholder-strip">
                  <strong>Insert Required MCP Placeholders</strong>
                  <span className="inspector-meta">
                    Necessary for full inline MCP control. If these are omitted, the runtime appends the missing MCP
                    sections automatically.
                  </span>
                  <div className="context-builder-placeholder-bar">
                    {mcpToolGuidanceBlock.length > 0 ? (
                      <button
                        type="button"
                        className="secondary-button context-builder-token-button context-builder-token-button--required"
                        onClick={() => updateProviderConfig("system_prompt", insertTokenAtEnd(String(node.config.system_prompt ?? ""), "{mcp_tool_guidance_block}"))}
                      >
                        mcp_tool_guidance_block
                      </button>
                    ) : null}
                    {promptContextToolSummaries.length > 0 ? (
                      <button
                        type="button"
                        className="secondary-button context-builder-token-button context-builder-token-button--required"
                        onClick={() => updateProviderConfig("system_prompt", insertTokenAtEnd(String(node.config.system_prompt ?? ""), "{mcp_tool_context_block}"))}
                      >
                        mcp_tool_context_block
                      </button>
                    ) : null}
                    {promptContextToolSummaries.map((tool) => (
                      <button
                        key={tool.placeholderToken}
                        type="button"
                        className="secondary-button context-builder-token-button context-builder-token-button--required"
                        onClick={() => updateProviderConfig("system_prompt", insertTokenAtEnd(String(node.config.system_prompt ?? ""), `{${tool.placeholderToken}}`))}
                      >
                        {tool.placeholderToken}
                      </button>
                    ))}
                  </div>
                </div>
              </div>
            ) : null}

            {activeTab === "config" ? (
              <div className="modal-folder-section provider-details-config-layout">
                {isSupabaseCatalogNode ? (
                  <section className="provider-details-schema-browser">
                    {isSupabaseBrowserLocked ? (
                      <div className="tool-details-modal-help provider-details-schema-lock-banner">
                        <strong>Supabase access is locked until the graph has verified Supabase auth.</strong>
                        <div>
                          Open the Environment section's Supabase button, save and verify the connection, then come back here to browse tables and columns.
                          {isSupabaseRuntimePending ? " Checking the runtime status now." : ""}
                        </div>
                        <div>
                          Required: <code>{supabaseUrlEnvVarName}</code> and <code>{supabaseKeyEnvVarName}</code>
                        </div>
                      </div>
                    ) : null}
                    <div className="provider-details-schema-browser-header">
                      <div>
                        <strong>Supabase Schema Browser</strong>
                        <span>
                          {isSupabaseBrowserLocked
                            ? "Verify Supabase auth to browse tables and columns."
                            : isSupabaseRowWriteNode
                              ? "Browse live tables and inspect columns, then apply the selected table to this node."
                              : "Browse live tables and columns, then apply the source and selected columns to this node."}
                        </span>
                      </div>
                      <div className="provider-details-schema-browser-actions">
                        <button
                          type="button"
                          className="secondary-button"
                          onClick={() => void handleLoadSupabaseSchema()}
                          disabled={isLoadingSupabaseSchema || !isSupabaseRuntimeReady}
                        >
                          {isLoadingSupabaseSchema ? "Loading..." : "Refresh Schema"}
                        </button>
                      </div>
                    </div>
                    <div className="provider-details-schema-browser-toolbar">
                      <label>
                        Search tables or columns
                        <input
                          value={supabaseSourceSearch}
                          onChange={(event) => setSupabaseSourceSearch(event.target.value)}
                          placeholder="projects, profiles, created_at..."
                          disabled={isSupabaseBrowserLocked}
                        />
                      </label>
                      <div className="provider-details-schema-browser-meta">
                        <span>{supabaseSchemaPreview ? `${filteredSupabaseSources.length}/${supabaseSchemaPreview.source_count} sources` : "No schema loaded yet"}</span>
                        <span>Schema: {String(resolvedPreviewConfig.schema ?? "public") || "public"}</span>
                      </div>
                    </div>
                    {supabaseSchemaError ? <p className="error-text">{supabaseSchemaError}</p> : null}
                    <div className="provider-details-schema-browser-layout">
                      <div className="provider-details-schema-source-list">
                        {filteredSupabaseSources.length > 0 ? (
                          filteredSupabaseSources.map((source) => {
                            const isActive = selectedSupabaseSource?.name === source.name;
                            const isConfigured = String(isSupabaseRowWriteNode ? node.config.table_name ?? "" : node.config.source_name ?? "").trim() === source.name;
                            return (
                              <button
                                key={source.name}
                                type="button"
                                className={`provider-details-schema-source-card${isActive ? " is-active" : ""}`}
                                onClick={() => setSelectedSupabaseSourceName(source.name)}
                                disabled={isSupabaseBrowserLocked}
                              >
                                <strong>{source.name}</strong>
                                <span>{source.columns.length} columns</span>
                                <span>{isConfigured ? "Selected in node" : source.source_kind}</span>
                              </button>
                            );
                          })
                        ) : (
                          <div className="tool-details-modal-help">Load the schema to inspect available sources.</div>
                        )}
                      </div>
                      <div className="provider-details-schema-column-panel">
                        {selectedSupabaseSource ? (
                          <>
                            <div className="provider-details-schema-column-header">
                              <div>
                                <strong>{selectedSupabaseSource.name}</strong>
                                <span>{selectedSupabaseSource.columns.length} columns</span>
                              </div>
                              <div className="provider-details-schema-column-actions">
                                <button
                                  type="button"
                                  className="secondary-button"
                                  onClick={() =>
                                    applySupabaseSourceSelection(
                                      selectedSupabaseSource,
                                      isSupabaseRowWriteNode ? undefined : selectedSupabaseColumnNames,
                                    )
                                  }
                                  disabled={isSupabaseBrowserLocked}
                                >
                                  {isSupabaseRowWriteNode ? "Use Table" : "Apply Selection"}
                                </button>
                                {!isSupabaseRowWriteNode ? (
                                  <button
                                    type="button"
                                    className="secondary-button"
                                    onClick={() => applySupabaseSourceSelection(selectedSupabaseSource, selectedSupabaseSource.columns.map((column) => column.name))}
                                    disabled={isSupabaseBrowserLocked}
                                  >
                                    Use All Columns
                                  </button>
                                ) : null}
                              </div>
                            </div>
                            {selectedSupabaseSource.description ? (
                              <p className="provider-details-schema-description">{selectedSupabaseSource.description}</p>
                            ) : null}
                            <div className="provider-details-schema-column-list">
                              {selectedSupabaseSource.columns.map((column) => {
                                const checked = selectedSupabaseColumnNames.includes(column.name);
                                return (
                                  <label key={column.name} className="provider-details-schema-column-row">
                                    {!isSupabaseRowWriteNode ? (
                                      <input
                                        type="checkbox"
                                        checked={checked}
                                        onChange={() => toggleSupabaseColumn(column.name)}
                                        disabled={isSupabaseBrowserLocked}
                                      />
                                    ) : <span />}
                                    <span className="provider-details-schema-column-main">
                                      <strong>{column.name}</strong>
                                      <small>{column.data_type}{column.nullable ? " · nullable" : ""}</small>
                                    </span>
                                    {column.description ? (
                                      <span className="provider-details-schema-column-description">{column.description}</span>
                                    ) : null}
                                  </label>
                                );
                              })}
                            </div>
                          </>
                        ) : (
                          <div className="tool-details-modal-help">Choose a source to inspect its columns.</div>
                        )}
                      </div>
                    </div>
                  </section>
                ) : null}
                <div className="provider-details-grid">
                  {isModelNode ? (
                    <label>
                      Provider
                      <select
                        value={providerName}
                        onChange={(event) => handleProviderChange(event.target.value)}
                      >
                        {availableProviders.map((candidate) => {
                          const candidateName = providerModelName(candidate);
                          return (
                            <option key={candidate.provider_id} value={candidateName}>
                              {candidate.display_name}
                            </option>
                          );
                        })}
                      </select>
                    </label>
                  ) : (
                    <label>
                      Provider
                      <input value={provider?.display_name ?? providerName} readOnly />
                    </label>
                  )}
                  {displayedProviderConfigFields.map((field) => (
                    <label key={field.key}>
                      {field.label}
                      {(() => {
                        const currentValue = String(node.config[field.key] ?? "");
                        const isSelectField = field.input_type === "select" && (field.options?.length ?? 0) > 0;
                        const isModelSelectField = isSelectField && field.key === "model";
                        const isCheckboxField = field.input_type === "checkbox";
                        const isTextareaField = field.input_type === "textarea";
                        const selectOptions =
                          isSelectField && currentValue && !field.options?.some((option) => option.value === currentValue)
                            ? [...(field.options ?? []), { value: currentValue, label: `Custom: ${currentValue}` }]
                            : (field.options ?? []);
                        const datalistId = `${node.id}-${field.key}-modal-options`;
                        return (
                          <>
                            {isModelSelectField ? (
                              <>
                                <input
                                  list={datalistId}
                                  value={currentValue}
                                  placeholder={field.placeholder || "Select or type a model id"}
                                  onChange={handleTextInputChange(field.key)}
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
                                onChange={handleTextInputChange(field.key)}
                              >
                                {selectOptions.map((option) => (
                                  <option key={option.value} value={option.value}>
                                    {option.label}
                                  </option>
                                ))}
                              </select>
                            ) : isCheckboxField ? (
                              <input
                                type="checkbox"
                                checked={Boolean(node.config[field.key] ?? false)}
                                onChange={(event) => updateProviderConfig(field.key, event.target.checked)}
                              />
                            ) : isTextareaField ? (
                              <textarea
                                rows={4}
                                value={currentValue}
                                placeholder={field.placeholder || undefined}
                                onChange={handleTextInputChange(field.key)}
                              />
                            ) : (
                              <input
                                type={field.input_type === "number" ? "number" : "text"}
                                value={currentValue}
                                placeholder={field.placeholder || undefined}
                                onChange={
                                  field.input_type === "number"
                                    ? handleNumberInputChange(field.key)
                                    : handleTextInputChange(field.key)
                                }
                              />
                            )}
                          </>
                        );
                      })()}
                    </label>
                  ))}
                </div>

                <div className="tool-details-modal-help">
                  {isModelNode
                    ? "Required provider choice is controlled from the API node itself. These fields are optional overrides for the selected provider."
                    : "These fields define how this node provider behaves for the selected graph node."}
                </div>

                {isSupabaseDataNode ? (
                  <div className="tool-details-modal-help">
                    <strong>Supabase data node notes</strong>
                    <div>Choose your table or view from the schema browser above, then pick the columns you want returned.</div>
                    <div>Use <code>filters_text</code> as one PostgREST query parameter per line, like <code>status=eq.active</code>.</div>
                    <div>Choose <code>records</code> when downstream nodes need structured JSON, or <code>markdown</code> when you want prompt-ready text.</div>
                    <div>Use <code>single_row</code> when the result should collapse to one record instead of an array.</div>
                  </div>
                ) : isSupabaseRowWriteNode ? (
                  <div className="tool-details-modal-help">
                    <strong>Supabase row write node notes</strong>
                    <div>Use the schema browser above to choose a destination table, then build the row with <code>base_row_json_path</code> and <code>column_values_json</code>.</div>
                    <div>Set a column spec to <code>{"{\"mode\":\"default\"}"}</code> to omit that column so the database default can apply.</div>
                    <div>Use <code>{"{\"mode\":\"path\",\"path\":\"customer.email\"}"}</code> for runtime values and <code>{"{\"mode\":\"literal\",\"value\":\"pending\"}"}</code> for fixed values.</div>
                  </div>
                ) : null}

                <div className="tool-details-modal-help">
                  Graph env refs can be used in any text field here:
                  <div className="graph-env-reference-list">
                    {envVarEntries.map(([key, value]) => (
                      <code key={key} title={value}>
                        {`{${key}}`}
                      </code>
                    ))}
                  </div>
                </div>
              </div>
            ) : null}

            {activeTab === "preview" ? (
              <div className="modal-folder-section provider-details-preview-layout">
                <section className="tool-details-modal-preview provider-details-preview-card">
                  <div className="tool-details-modal-preview-header">
                    <strong>Resolved Provider Config</strong>
                    <span>This preview shows provider settings after graph env references are substituted.</span>
                  </div>
                  <pre>{JSON.stringify(resolvedPreviewConfig, null, 2)}</pre>
                </section>
                {isSupabaseDataNode ? (
                  <section className="tool-details-modal-preview provider-details-preview-card">
                    <div className="tool-details-modal-preview-header">
                      <strong>Supabase Request Preview</strong>
                      <span>This is the intended query shape before runtime credentials are applied.</span>
                    </div>
                    <pre>{JSON.stringify({
                      source_kind: supabaseSourceKind,
                      source_name: String(node.config.source_name ?? ""),
                      schema: String(node.config.schema ?? "public"),
                      select: String(node.config.select ?? "*") || "*",
                      filters: supabaseFilterLines,
                      order_by: String(node.config.order_by ?? ""),
                      order_desc: Boolean(node.config.order_desc),
                      limit: Number(node.config.limit ?? 25),
                      single_row: Boolean(node.config.single_row),
                      output_mode: supabaseOutputMode,
                      rpc_params_json: String(node.config.rpc_params_json ?? "{}") || "{}",
                    }, null, 2)}</pre>
                  </section>
                ) : null}
                {node.kind === "model" ? (
                  <section className="tool-details-modal-preview provider-details-preview-card provider-details-preview-card--prompt">
                    <div className="tool-details-modal-preview-header">
                      <strong>Final System Prompt Preview</strong>
                      <span>This is the complete prompt after placeholders, MCP guidance, and connected tool context are assembled.</span>
                    </div>
                    <pre>{finalSystemPromptPreview || systemPromptTemplatePreview || "The final assembled system prompt will appear here."}</pre>
                  </section>
                ) : null}
              </div>
            ) : null}
          </div>
        </div>
      </section>
    </div>
  );
}
