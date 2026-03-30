import type {
  GraphDefinition,
  GraphDocument,
  GraphNode,
  RunState,
  RuntimeEvent,
  TestEnvironmentDefinition,
} from "./types";

export type EnvironmentRunSummary = {
  runId: string | null;
  status: string;
  totalAgents: number;
  completedAgents: number;
  runningAgents: number;
  failedAgents: number;
  queuedAgents: number;
  activeAgentNames: string[];
  focusedAgentId: string | null;
  focusedAgentName: string | null;
  elapsedLabel: string;
};

export type AgentRunMilestone = {
  id: string;
  label: string;
  eventType: string;
  timestamp: string;
  tone: "idle" | "info" | "running" | "success" | "danger";
  nodeId: string | null;
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
  finalOutput: unknown;
  nodeErrors: Record<string, unknown>;
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

function nodeLabelMap(graph: GraphDefinition | null): Map<string, string> {
  return new Map((graph?.nodes ?? []).map((node) => [node.id, node.label]));
}

function graphByAgent(environment: TestEnvironmentDefinition): Map<string, GraphDefinition> {
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
        env_vars: { ...environment.env_vars, ...agent.env_vars },
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

function nodeIdFromEvent(event: RuntimeEvent): string | null {
  const payloadNodeId = event.payload.node_id;
  return typeof payloadNodeId === "string" && payloadNodeId.length > 0 ? payloadNodeId : null;
}

function eventTone(eventType: string): AgentRunMilestone["tone"] {
  if (eventType === "run.completed" || eventType === "node.completed") {
    return "success";
  }
  if (eventType === "run.failed") {
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
  const runningAgents = agentStates.filter((state) => state.status === "running").length;
  const completedAgents = agentStates.filter((state) => state.status === "completed").length;
  const failedAgents = agentStates.filter((state) => state.status === "failed").length;
  const queuedAgents = Math.max(0, graph.agents.length - runningAgents - completedAgents - failedAgents);
  return {
    runId: runState?.run_id ?? null,
    status: runState?.status ?? "idle",
    totalAgents: graph.agents.length,
    completedAgents,
    runningAgents,
    failedAgents,
    queuedAgents,
    activeAgentNames: graph.agents
      .filter((agent) => runState?.agent_runs?.[agent.agent_id]?.status === "running")
      .map((agent) => agent.name),
    focusedAgentId: selectedAgentId,
    focusedAgentName: focusedGraphName(graph, selectedAgentId),
    elapsedLabel: formatElapsed(runState?.started_at, runState?.ended_at),
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
    return {
      agentId: agent.agent_id,
      agentName: agent.name,
      status: agentState?.status ?? "idle",
      runId: agentState?.run_id ?? null,
      currentNodeId: agentState?.current_node_id ?? null,
      currentNodeLabel:
        (agentState?.current_node_id ? labels.get(agentState.current_node_id) : null) ??
        agentState?.current_node_id ??
        "n/a",
      completedNodes: Object.keys(agentState?.visit_counts ?? {}).length,
      totalNodes: currentGraph?.nodes.length ?? 0,
      transitionCount: agentState?.transition_history.length ?? 0,
      errorCount: Object.keys(agentState?.node_errors ?? {}).length,
      retryCount: agentEvents.filter((event) => event.event_type === "retry.triggered").length,
      elapsedLabel: formatElapsed(agentState?.started_at, agentState?.ended_at),
      milestones: agentEvents.map((event, index) => ({
        id: `${agent.agent_id}-${event.timestamp}-${index}`,
        label: milestoneLabel(event, currentGraph),
        eventType: event.event_type,
        timestamp: event.timestamp,
        tone: eventTone(event.event_type),
        nodeId: nodeIdFromEvent(event),
      })),
    };
  });
}

function normalizeFocusedEvents(events: RuntimeEvent[]): RuntimeEvent[] {
  return events.map((event) => ({ ...event, event_type: normalizeEventType(event.event_type) }));
}

export function buildFocusedRunSummary(
  graph: GraphDefinition | null,
  runState: RunState | null,
  events: RuntimeEvent[],
): FocusedRunSummary {
  const labels = nodeLabelMap(graph);
  const normalizedEvents = normalizeFocusedEvents(events);
  return {
    runId: runState?.run_id ?? null,
    status: runState?.status ?? "idle",
    currentNodeId: runState?.current_node_id ?? null,
    currentNodeLabel:
      (runState?.current_node_id ? labels.get(runState.current_node_id) : null) ?? runState?.current_node_id ?? "n/a",
    completedNodes: Object.keys(runState?.visit_counts ?? {}).length,
    totalNodes: graph?.nodes.length ?? 0,
    transitionCount: runState?.transition_history.length ?? 0,
    errorCount: Object.keys(runState?.node_errors ?? {}).length,
    retryCount: normalizedEvents.filter((event) => event.event_type === "retry.triggered").length,
    elapsedLabel: formatElapsed(runState?.started_at, runState?.ended_at),
    finalOutput: runState?.final_output ?? null,
    nodeErrors: runState?.node_errors ?? {},
  };
}

function buildExecutionGroup(
  id: string,
  graph: GraphDefinition | null,
  startedEvent: RuntimeEvent,
  completedEvent: RuntimeEvent | null,
): FocusedEventGroup {
  const labels = nodeLabelMap(graph);
  const nodeId = nodeIdFromEvent(startedEvent);
  const nodeLabel = nodeId ? (labels.get(nodeId) ?? nodeId) : "Node";
  const completePayloadError = completedEvent?.payload.error;
  const tone =
    completePayloadError != null ? "danger" : completedEvent ? "success" : "running";
  return {
    id,
    title: nodeLabel,
    subtitle: completePayloadError != null ? "Execution failed" : completedEvent ? "Execution completed" : "Execution in progress",
    tone,
    eventCount: completedEvent ? 2 : 1,
    startedAt: startedEvent.timestamp,
    endedAt: completedEvent?.timestamp ?? null,
    nodeId,
    lines: [
      startedEvent.summary,
      ...(completedEvent ? [completedEvent.summary] : []),
    ],
  };
}

function buildSingleEventGroup(id: string, event: RuntimeEvent): FocusedEventGroup {
  const eventType = normalizeEventType(event.event_type);
  const tone = eventTone(eventType);
  return {
    id,
    title: milestoneLabel(event, null),
    subtitle: eventType,
    tone: tone === "idle" ? "info" : tone,
    eventCount: 1,
    startedAt: event.timestamp,
    endedAt: null,
    nodeId: nodeIdFromEvent(event),
    lines: [event.summary],
  };
}

export function buildFocusedEventGroups(
  graph: GraphDefinition | null,
  events: RuntimeEvent[],
): FocusedEventGroup[] {
  const normalizedEvents = normalizeFocusedEvents(events);
  const groups: FocusedEventGroup[] = [];
  for (let index = 0; index < normalizedEvents.length; index += 1) {
    const event = normalizedEvents[index];
    if (event.event_type === "node.started") {
      const nextEvent = normalizedEvents[index + 1];
      const sameNodeCompleted =
        nextEvent &&
        nextEvent.event_type === "node.completed" &&
        nodeIdFromEvent(nextEvent) === nodeIdFromEvent(event);
      groups.push(
        buildExecutionGroup(
          `node-${event.timestamp}-${index}`,
          graph,
          event,
          sameNodeCompleted ? nextEvent : null,
        ),
      );
      if (sameNodeCompleted) {
        index += 1;
      }
      continue;
    }
    groups.push(buildSingleEventGroup(`event-${event.timestamp}-${index}`, event));
  }
  return groups.reverse();
}

export function nodeById(graph: GraphDefinition | null, nodeId: string | null): GraphNode | null {
  if (!graph || !nodeId) {
    return null;
  }
  return graph.nodes.find((node) => node.id === nodeId) ?? null;
}
