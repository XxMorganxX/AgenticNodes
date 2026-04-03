import { useEffect, useMemo, useState } from "react";
import type { ChangeEvent, MouseEvent } from "react";

import { fetchProviderDiagnostics, preflightProvider } from "../lib/api";
import { findProviderDefinition, modelProviderDefinitions, providerDefaultConfig, providerModelName } from "../lib/editor";
import { getGraphEnvVars, resolveGraphEnvReferences } from "../lib/graphEnv";
import { getNodeInstanceLabel } from "../lib/nodeInstanceLabels";
import { resolveToolNodeDetails } from "../lib/toolNodeDetails";
import type {
  EditorCatalog,
  GraphDefinition,
  GraphNode,
  ProviderDiagnosticsResult,
  ProviderPreflightResult,
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
  const providerName = String(node.config.provider_name ?? node.model_provider_name ?? "").trim();
  if (!providerName) {
    return null;
  }
  return findProviderDefinition(catalog, providerName);
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
  const availableProviders = modelProviderDefinitions(catalog);
  const envVarEntries = Object.entries(getGraphEnvVars(graph));
  const providerName = String(node.config.provider_name ?? node.model_provider_name ?? "not-set");
  const providerConfigFields = provider?.config_fields ?? [];
  const supportsLiveVerification = providerName !== "mock";
  const catalogTools = catalog?.tools ?? [];
  const mcpCatalogTools = catalogTools.filter((tool) => tool.source_type === "mcp");
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
  const [preflightResult, setPreflightResult] = useState<ProviderPreflightResult | null>(null);
  const [diagnostics, setDiagnostics] = useState<ProviderDiagnosticsResult | null>(null);
  const [preflightError, setPreflightError] = useState<string | null>(null);
  const [isPreflighting, setIsPreflighting] = useState(false);

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
    const entries: Array<[string, unknown]> = [["provider_name", providerName]];
    providerConfigFields.forEach((field) => {
      entries.push([field.key, node.config[field.key]]);
    });
    return Object.fromEntries(entries);
  }, [node.config, providerConfigFields, providerName]);
  const verificationStorageKey = useMemo(
    () => buildProviderVerificationStorageKey(providerName, preflightConfig),
    [preflightConfig, providerName],
  );
  const [persistedVerification, setPersistedVerification] = useState<PersistedProviderVerification | null>(null);

  useEffect(() => {
    setPersistedVerification(getPersistedProviderVerification(verificationStorageKey));
  }, [verificationStorageKey]);

  useEffect(() => {
    let cancelled = false;
    if (!providerName || providerName === "not-set") {
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
  }, [preflightConfig, providerName]);

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

  function updateProviderConfig(key: string, value: string | number) {
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

  return (
    <div className="tool-details-modal-backdrop" onClick={handleOverlayClick} role="presentation">
      <section
        className="tool-details-modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="provider-details-modal-title"
      >
        <div className="tool-details-modal-header">
          <div>
            <div className="tool-details-modal-eyebrow">API Provider Details</div>
            <h3 id="provider-details-modal-title">
              {nodeLabel}
              {provider ? ` · ${provider.display_name}` : ""}
            </h3>
            <p>
              Required provider selection stays on the API node. Use this modal to review provider capabilities and tune
              optional provider parameters and prompt instructions for the selected API step.
            </p>
          </div>
          <button type="button" className="secondary-button" onClick={onClose}>
            Close
          </button>
        </div>

        <div className="tool-details-modal-body">
          <section className="provider-details-summary">
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
            {displayedPreflightResult ? (
              <div className="tool-details-modal-help">
                <strong>Provider Health</strong>
                <div>{displayedPreflightResult.message}</div>
                {displayedPreflightResult.warnings?.map((warning) => (
                  <div key={warning}>{warning}</div>
                ))}
              </div>
            ) : null}
            {displayedDiagnostics ? (
              <div className="tool-details-modal-help">
                <strong>Provider Diagnostics</strong>
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
              </div>
            ) : null}
            {preflightError ? <div className="tool-details-modal-help">{preflightError}</div> : null}
            {!supportsLiveVerification ? (
              <div className="tool-details-modal-help">Live verification is not required for the mock provider.</div>
            ) : null}
            <button
              type="button"
              className="secondary-button"
              onClick={handleLiveVerification}
              disabled={isPreflighting || !supportsLiveVerification}
            >
              {isPreflighting ? "Checking Provider..." : supportsLiveVerification ? "Run Live Verification" : "Live Verification Not Required"}
            </button>
          </section>

          <div className="provider-details-grid">
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
            <label className="provider-details-grid-full">
              System Prompt
              <textarea
                rows={7}
                value={String(node.config.system_prompt ?? "")}
                placeholder="You are a helpful model node."
                onChange={handleTextInputChange("system_prompt")}
              />
              <small>
                Connected MCP edges already define which tools are in scope. MCP coverage is the only required part here;
                all other placeholders are optional runtime values.
              </small>
              {node.kind === "model" ? (
                <div className="tool-details-modal-help provider-details-placeholder-panel">
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
            </label>
            {providerConfigFields.map((field) => (
              <label key={field.key}>
                {field.label}
                {(() => {
                  const currentValue = String(node.config[field.key] ?? "");
                  const isSelectField = field.input_type === "select" && (field.options?.length ?? 0) > 0;
                  const isModelSelectField = isSelectField && field.key === "model";
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
            Required provider choice is controlled from the API node itself. These fields are optional overrides for the
            selected provider.
          </div>

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

          <section className="tool-details-modal-preview">
            <div className="tool-details-modal-preview-header">
              <strong>Resolved Provider Config</strong>
              <span>This preview shows provider settings after graph env references are substituted.</span>
            </div>
            <pre>{JSON.stringify(resolvedPreviewConfig, null, 2)}</pre>
          </section>
          {node.kind === "model" ? (
            <section className="tool-details-modal-preview">
              <div className="tool-details-modal-preview-header">
                <strong>Final System Prompt Preview</strong>
                <span>This is the complete prompt after placeholders, MCP guidance, and connected tool context are assembled.</span>
              </div>
              <pre>{finalSystemPromptPreview || systemPromptTemplatePreview || "The final assembled system prompt will appear here."}</pre>
            </section>
          ) : null}
        </div>
      </section>
    </div>
  );
}
