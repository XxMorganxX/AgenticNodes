import { formatRunStatusLabel, type AgentRunLane } from "../lib/runVisualization";

type AgentRunSwimlanesProps = {
  lanes: AgentRunLane[];
  selectedAgentId: string | null;
  onSelectAgent: (agentId: string) => void;
  onSelectNode?: (agentId: string, nodeId: string | null) => void;
};

export function AgentRunSwimlanes({
  lanes,
  selectedAgentId,
  onSelectAgent,
  onSelectNode,
}: AgentRunSwimlanesProps) {
  return (
    <section className="panel agent-swimlanes-panel">
      <div className="panel-header">
        <h2>Agent Run Swimlanes</h2>
        <p>Track each agent over time and click a milestone to focus its graph.</p>
      </div>
      <div className="agent-swimlanes">
        {lanes.map((lane) => (
          <section
            key={lane.agentId}
            className={`agent-swimlane ${selectedAgentId === lane.agentId ? "is-selected" : ""}`}
            onClick={() => onSelectAgent(lane.agentId)}
          >
            <button type="button" className="agent-swimlane-meta" onClick={() => onSelectAgent(lane.agentId)}>
              <div className="agent-swimlane-heading">
                <strong>{lane.agentName}</strong>
                <span className={`agent-swimlane-status agent-swimlane-status--${lane.status}`}>
                  {formatRunStatusLabel(lane.status)}
                </span>
              </div>
              <div className="agent-swimlane-stats">
                <span>{lane.completedNodes}/{lane.totalNodes} nodes</span>
                <span>{lane.retryCount} retries</span>
                <span>{lane.errorCount} errors</span>
                <span>{lane.elapsedLabel}</span>
              </div>
              <div className="agent-swimlane-current">Current: {lane.currentNodeLabel}</div>
            </button>
            <div className="agent-swimlane-track" role="list" aria-label={`${lane.agentName} milestones`}>
              {lane.milestones.length === 0 ? (
                <div className="agent-swimlane-empty">No runtime milestones yet.</div>
              ) : (
                lane.milestones.map((milestone) => (
                  <button
                    key={milestone.id}
                    type="button"
                    role="listitem"
                    className={`agent-swimlane-milestone agent-swimlane-milestone--${milestone.tone}`}
                    title={`${milestone.label} at ${new Date(milestone.timestamp).toLocaleTimeString()}`}
                    onClick={() => {
                      onSelectAgent(lane.agentId);
                      onSelectNode?.(lane.agentId, milestone.nodeId);
                    }}
                  >
                    <span className="agent-swimlane-dot" />
                    <span className="agent-swimlane-milestone-copy">
                      <strong>{milestone.label}</strong>
                      <span>{new Date(milestone.timestamp).toLocaleTimeString()}</span>
                    </span>
                  </button>
                ))
              )}
            </div>
          </section>
        ))}
      </div>
    </section>
  );
}
