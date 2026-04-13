import type { GraphDefinition, GraphNode, RunState, RuntimeEvent } from "./types";

const PROMPT_BLOCK_TOKEN_PATTERN = /\{([A-Za-z_][A-Za-z0-9_]*)\}/g;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function renderTemplate(template: string, variables: Record<string, string>): string {
  return template.replace(PROMPT_BLOCK_TOKEN_PATTERN, (_, token: string) => variables[token] ?? `{${token}}`);
}

function stringifyTemplateValue(value: unknown): string {
  if (typeof value === "string") {
    return value;
  }
  if (value == null) {
    return "";
  }
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function promptBlockVariables(graph: GraphDefinition, node: GraphNode, runState: RunState): Record<string, string> {
  return {
    ...Object.fromEntries(Object.entries(graph.env_vars ?? {}).map(([key, value]) => [key, String(value)])),
    current_node_id: node.id,
    documents: stringifyTemplateValue(runState.documents ?? []),
    graph_id: graph.graph_id,
    input_payload: stringifyTemplateValue(runState.input_payload),
    run_id: runState.run_id ?? "",
  };
}

function buildPromptBlockEnvelope(graph: GraphDefinition, node: GraphNode, runState: RunState): Record<string, unknown> {
  const role = String(node.config.role ?? "user").trim() || "user";
  const variables = promptBlockVariables(graph, node, runState);
  const renderedName = renderTemplate(String(node.config.name ?? ""), variables).trim();
  const renderedContent = renderTemplate(String(node.config.content ?? ""), variables).trim();
  const payload: Record<string, unknown> = {
    kind: "prompt_block",
    role,
    content: renderedContent,
  };
  if (renderedName) {
    payload.name = renderedName;
  }
  return {
    schema_version: "1.0",
    from_node_id: node.id,
    from_category: node.category,
    payload,
    artifacts: {},
    errors: [],
    tool_calls: [],
    metadata: {
      contract: "data_envelope",
      node_kind: node.kind,
      data_mode: "prompt_block",
      provider_id: node.provider_id,
      binding_only: true,
      prompt_block_role: role,
    },
  };
}

function payloadFromOutput(output: unknown): unknown {
  return isRecord(output) && Object.prototype.hasOwnProperty.call(output, "payload") ? output.payload : output;
}

function resolveRuntimeNodeBinding(
  graph: GraphDefinition,
  node: GraphNode,
  runState: RunState,
  resolveOutput: (nodeId: string) => unknown,
  binding?: Record<string, unknown> | null,
): unknown {
  const incomingEdges = graph.edges.filter((edge) => edge.target_id === node.id);
  if (!binding) {
    if (incomingEdges.length === 0) {
      return runState.input_payload;
    }
    for (let index = incomingEdges.length - 1; index >= 0; index -= 1) {
      const output = resolveOutput(incomingEdges[index].source_id);
      if (output !== undefined) {
        return output;
      }
    }
    return undefined;
  }

  const bindingType = String(binding.type ?? "latest_output");
  if (bindingType === "input_payload") {
    return runState.input_payload;
  }
  if (bindingType === "documents") {
    return runState.documents;
  }
  if (bindingType === "latest_output" || bindingType === "latest_envelope") {
    return resolveOutput(String(binding.source ?? ""));
  }
  if (bindingType === "latest_payload") {
    return payloadFromOutput(resolveOutput(String(binding.source ?? "")));
  }
  if (bindingType === "latest_error") {
    return runState.node_errors?.[String(binding.source ?? "")];
  }
  if (bindingType === "first_available_payload" && Array.isArray(binding.sources)) {
    for (const source of binding.sources) {
      const payload = payloadFromOutput(resolveOutput(String(source ?? "")));
      if (payload !== undefined) {
        return payload;
      }
    }
  }
  if (bindingType === "first_available_envelope" && Array.isArray(binding.sources)) {
    for (const source of binding.sources) {
      const envelope = resolveOutput(String(source ?? ""));
      if (envelope !== undefined) {
        return envelope;
      }
    }
  }
  return undefined;
}

function buildDisplayNodeOutput(
  graph: GraphDefinition,
  node: GraphNode,
  runState: RunState,
  resolveOutput: (nodeId: string) => unknown,
): Record<string, unknown> | undefined {
  if (node.kind !== "data" || node.provider_id !== "core.data_display") {
    return undefined;
  }
  const incomingEdges = graph.edges.filter((edge) => edge.target_id === node.id);
  if (incomingEdges.length === 0 && !node.config.input_binding) {
    return undefined;
  }

  const sourceValue = resolveRuntimeNodeBinding(
    graph,
    node,
    runState,
    resolveOutput,
    isRecord(node.config.input_binding) ? node.config.input_binding : null,
  );
  if (sourceValue === undefined) {
    return undefined;
  }

  const displayValue = sourceValue;
  const sourceEnvelope = isRecord(displayValue) && isRecord(displayValue.metadata) ? displayValue : null;
  const payload = isRecord(sourceValue) && Object.prototype.hasOwnProperty.call(sourceValue, "payload")
    ? sourceValue.payload
    : sourceValue;
  const displayOnly = Boolean(node.config.show_input_envelope);

  const artifacts = isRecord(sourceEnvelope?.artifacts) ? { ...sourceEnvelope.artifacts } : {};
  if (displayOnly) {
    artifacts.display_envelope = displayValue;
  }

  if (payload === undefined && !Object.prototype.hasOwnProperty.call(artifacts, "display_envelope")) {
    return undefined;
  }

  return {
    schema_version: typeof sourceEnvelope?.schema_version === "string" ? sourceEnvelope.schema_version : "1.0",
    from_node_id: node.id,
    from_category: node.category,
    payload,
    artifacts,
    errors: Array.isArray(sourceEnvelope?.errors) ? sourceEnvelope.errors : [],
    tool_calls: Array.isArray(sourceEnvelope?.tool_calls) ? sourceEnvelope.tool_calls : [],
    metadata: {
      ...(isRecord(sourceEnvelope?.metadata) ? sourceEnvelope.metadata : {}),
      contract: "data_envelope",
      node_kind: node.kind,
      display_only: displayOnly,
    },
  };
}

export function latestRuntimeResolvedNodeOutputs(
  graph: GraphDefinition | null,
  runState: RunState | null,
  normalizedEvents: RuntimeEvent[],
): Record<string, unknown> {
  const outputs: Record<string, unknown> = {};
  for (const event of normalizedEvents) {
    if (event.event_type !== "node.completed") {
      continue;
    }
    const nodeId = typeof event.payload.node_id === "string" ? event.payload.node_id : "";
    if (!nodeId || !Object.prototype.hasOwnProperty.call(event.payload, "output")) {
      continue;
    }
    outputs[nodeId] = event.payload.output;
  }

  Object.assign(outputs, runState?.node_outputs ?? {});
  if (!graph || !runState) {
    return outputs;
  }

  const nodesById = new Map(graph.nodes.map((node) => [node.id, node] as const));
  const memo = new Map<string, unknown>();
  const resolving = new Set<string>();

  const resolveOutput = (nodeId: string): unknown => {
    if (!nodeId) {
      return undefined;
    }
    if (Object.prototype.hasOwnProperty.call(outputs, nodeId)) {
      return outputs[nodeId];
    }
    if (memo.has(nodeId) || resolving.has(nodeId)) {
      return memo.get(nodeId);
    }
    const node = nodesById.get(nodeId);
    if (!node) {
      return undefined;
    }
    resolving.add(nodeId);
    let derived: unknown;
    if (node.provider_id === "core.prompt_block") {
      derived = buildPromptBlockEnvelope(graph, node, runState);
    } else if (node.provider_id === "core.data_display") {
      derived = buildDisplayNodeOutput(graph, node, runState, resolveOutput);
    }
    resolving.delete(nodeId);
    if (derived !== undefined) {
      memo.set(nodeId, derived);
    }
    return derived;
  };

  for (const node of graph.nodes) {
    const derived = resolveOutput(node.id);
    if (derived !== undefined && !Object.prototype.hasOwnProperty.call(outputs, node.id)) {
      outputs[node.id] = derived;
    }
  }

  return outputs;
}
