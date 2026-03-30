import { formatRunStatusLabel } from "../lib/runVisualization";
import type { RunState, TestEnvironmentDefinition } from "../lib/types";

type AgentOverviewProps = {
  graph: TestEnvironmentDefinition;
  selectedAgentId: string | null;
  runState: RunState | null;
  onSelectAgent: (agentId: string) => void;
};

export function AgentOverview({ graph, selectedAgentId, runState, onSelectAgent }: AgentOverviewProps) {
  return (
    <section className="panel agent-overview-panel">
      <div className="panel-header">
        <h2>Test Environment Agents</h2>
        <p>Each agent runs in isolation. Click a card to focus its graph and run details.</p>
      </div>
      <div className="agent-overview-grid">
        {graph.agents.map((agent) => {
          const agentRun = runState?.agent_runs?.[agent.agent_id] ?? null;
          const isSelected = selectedAgentId === agent.agent_id;
          return (
            <button
              key={agent.agent_id}
              type="button"
              className={`agent-overview-card ${isSelected ? "is-selected" : ""}`}
              onClick={() => onSelectAgent(agent.agent_id)}
            >
              <div className="agent-overview-card-header">
                <div>
                  <strong>{agent.name}</strong>
                  <div className="agent-overview-card-id">{agent.agent_id}</div>
                </div>
                <span className={`agent-overview-status agent-overview-status--${agentRun?.status ?? "idle"}`}>
                  {formatRunStatusLabel(agentRun?.status)}
                </span>
              </div>
              <p>{agent.description || "No description yet."}</p>
              <dl className="agent-overview-stats">
                <div>
                  <dt>Nodes</dt>
                  <dd>{agent.nodes.length}</dd>
                </div>
                <div>
                  <dt>Edges</dt>
                  <dd>{agent.edges.length}</dd>
                </div>
                <div>
                  <dt>Start</dt>
                  <dd>{agent.start_node_id || "unset"}</dd>
                </div>
                <div>
                  <dt>Run</dt>
                  <dd>{agentRun?.run_id ?? "not started"}</dd>
                </div>
              </dl>
            </button>
          );
        })}
      </div>
    </section>
  );
}
