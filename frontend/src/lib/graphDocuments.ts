import type {
  AgentDefinition,
  EditorCatalog,
  GraphDefinition,
  GraphDocument,
  GraphNode,
  NodeProviderDefinition,
  RunState,
  RuntimeEvent,
  TestEnvironmentDefinition,
} from "./types";

function isDefaultEnvPlaceholder(key: string, value: string): boolean {
  const normalizedKey = String(key ?? "").trim();
  const normalizedValue = String(value ?? "").trim();
  return normalizedKey.length > 0 && normalizedValue === normalizedKey;
}

function mergeEnvVars(...envGroups: Array<Record<string, string> | undefined>): Record<string, string> {
  const merged: Record<string, string> = {};
  for (const envGroup of envGroups) {
    if (!envGroup) {
      continue;
    }
    for (const [key, rawValue] of Object.entries(envGroup)) {
      const value = String(rawValue ?? "");
      if (isDefaultEnvPlaceholder(key, value)) {
        const parentValue = String(merged[key] ?? "").trim();
        if (parentValue && !isDefaultEnvPlaceholder(key, parentValue)) {
          continue;
        }
      }
      merged[key] = value;
    }
  }
  return merged;
}

export function isTestEnvironment(graph: GraphDocument | null | undefined): graph is TestEnvironmentDefinition {
  return Boolean(graph && "agents" in graph && Array.isArray(graph.agents));
}

// Mirrors backend `TestEnvironmentDefinition.is_multi_agent`. Use this for UI/run-mode
// decisions; `isTestEnvironment` is now a shape predicate that matches every document
// the API returns and no longer discriminates single-graph vs multi-agent intent.
export function isMultiAgent(graph: GraphDocument | null | undefined): graph is TestEnvironmentDefinition {
  if (!isTestEnvironment(graph)) {
    return false;
  }
  return graph.graph_type === "test_environment" || graph.agents.length > 1;
}

export function getDefaultAgentId(graph: GraphDocument | null | undefined): string | null {
  if (!isTestEnvironment(graph)) {
    return null;
  }
  return graph.agents[0]?.agent_id ?? null;
}

export function getSelectedAgent(
  graph: GraphDocument | null | undefined,
  selectedAgentId: string | null | undefined,
): AgentDefinition | null {
  if (!isTestEnvironment(graph)) {
    return null;
  }
  return graph.agents.find((agent) => agent.agent_id === selectedAgentId) ?? graph.agents[0] ?? null;
}

export function getCanvasGraph(
  graph: GraphDocument | null | undefined,
  selectedAgentId: string | null | undefined,
): GraphDefinition | null {
  if (!graph) {
    return null;
  }
  if (!isTestEnvironment(graph)) {
    return graph;
  }
  const agent = getSelectedAgent(graph, selectedAgentId);
  if (!agent) {
    return null;
  }
  return {
    graph_id: graph.graph_id,
    name: agent.name,
    description: agent.description,
    version: agent.version,
    graph_type: "graph",
    email_routing_mode: graph.email_routing_mode,
    start_node_id: agent.start_node_id,
    env_vars: mergeEnvVars(graph.env_vars, agent.env_vars),
    supabase_connections: graph.supabase_connections,
    default_supabase_connection_id: graph.default_supabase_connection_id,
    run_store_supabase_connection_id: graph.run_store_supabase_connection_id,
    nodes: agent.nodes,
    edges: agent.edges,
    node_providers: graph.node_providers,
  };
}

export function updateSelectedAgentGraph(
  graph: GraphDocument,
  selectedAgentId: string | null | undefined,
  nextGraph: GraphDefinition,
): GraphDocument {
  if (!isTestEnvironment(graph)) {
    return nextGraph;
  }
  const targetAgentId = selectedAgentId ?? graph.agents[0]?.agent_id;
  if (!targetAgentId) {
    return graph;
  }
  return {
    ...graph,
    agents: graph.agents.map((agent) =>
      agent.agent_id === targetAgentId
        ? {
            ...agent,
            name: nextGraph.name,
            description: nextGraph.description,
            version: nextGraph.version,
            start_node_id: nextGraph.start_node_id,
            nodes: nextGraph.nodes,
            edges: nextGraph.edges,
          }
        : agent,
    ),
  };
}

export function updateDocumentEnvVars(
  graph: GraphDocument,
  envVars: Record<string, string>,
): GraphDocument {
  if (isTestEnvironment(graph)) {
    return { ...graph, env_vars: envVars };
  }
  return { ...graph, env_vars: envVars };
}

export function getSelectedRunState(runState: RunState | null, selectedAgentId: string | null | undefined): RunState | null {
  if (!runState) {
    return null;
  }
  if (!selectedAgentId || !runState.agent_runs) {
    return runState;
  }
  return runState.agent_runs[selectedAgentId] ?? null;
}

export function getSelectedRunId(runState: RunState | null, fallbackRunId: string | null, selectedAgentId: string | null | undefined): string | null {
  const selectedRun = getSelectedRunState(runState, selectedAgentId);
  return selectedRun?.run_id ?? fallbackRunId;
}

export function getSelectedRunFilesRequest(
  runState: RunState | null,
  fallbackRunId: string | null,
  selectedAgentId: string | null | undefined,
): { runId: string | null; agentId: string | null } {
  const selectedRun = getSelectedRunState(runState, selectedAgentId);
  if (selectedRun?.parent_run_id && selectedRun.agent_id) {
    return {
      runId: selectedRun.parent_run_id,
      agentId: selectedRun.agent_id,
    };
  }
  return {
    runId: selectedRun?.run_id ?? fallbackRunId,
    agentId: null,
  };
}

export function getStartNode(graph: GraphDefinition | null | undefined): GraphNode | null {
  if (!graph) {
    return null;
  }
  return graph.nodes.find((node) => node.id === graph.start_node_id) ?? null;
}

export function findNodeProvider(
  catalog: EditorCatalog | null | undefined,
  graph: GraphDefinition | null | undefined,
  providerId: string | null | undefined,
): NodeProviderDefinition | null {
  if (!providerId) {
    return null;
  }
  const fromGraph = (graph?.node_providers ?? []).find((provider) => provider.provider_id === providerId);
  if (fromGraph) {
    return fromGraph;
  }
  const fromCatalog = (catalog?.node_providers ?? []).find((provider) => provider.provider_id === providerId);
  return fromCatalog ?? null;
}

export function getListenerStartProvider(
  graph: GraphDefinition | null | undefined,
  catalog: EditorCatalog | null | undefined,
): NodeProviderDefinition | null {
  const startNode = getStartNode(graph);
  if (!startNode) {
    return null;
  }
  const provider = findNodeProvider(catalog, graph, startNode.provider_id);
  if (!provider || provider.trigger_mode !== "listener") {
    return null;
  }
  return provider;
}

export function filterEventsForAgent(
  events: RuntimeEvent[],
  selectedAgentId: string | null | undefined,
): RuntimeEvent[] {
  if (!selectedAgentId || events.every((event) => !event.agent_id)) {
    return events;
  }
  return events.filter((event) => event.agent_id === selectedAgentId);
}
