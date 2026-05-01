import { formatRunStatusLabel, type AgentRunLane } from "../lib/runVisualization";
import { RunErrorHover } from "./RunErrorHover";

type EnvironmentAgentMenuProps = {
  agents: AgentRunLane[];
  selectedAgentId: string | null;
  open: boolean;
  onToggle: () => void;
  onSelectAgent: (agentId: string) => void;
  onCreateAgent?: (agentName: string) => void;
  onRequestRemoveAgent?: (agentId: string) => void;
};

export function EnvironmentAgentMenu({
  agents,
  selectedAgentId,
  open,
  onToggle,
  onSelectAgent,
  onCreateAgent,
  onRequestRemoveAgent,
}: EnvironmentAgentMenuProps) {
  function handleCreateAgent() {
    onCreateAgent?.(`Agent #${agents.length + 1}`);
  }

  return (
    <div className={`environment-agent-menu${open ? " is-open" : ""}`}>
      <button
        type="button"
        className="environment-agent-menu-button"
        aria-expanded={open}
        aria-haspopup="listbox"
        onClick={onToggle}
      >
        <span className="environment-agent-menu-indicator" aria-hidden="true" />
        <span>Agents</span>
      </button>
      <div className="environment-agent-menu-panel" role="listbox" aria-label="Environment agents">
        <div className="environment-agent-menu-scroll">
          {agents.length > 0 ? agents.map((agent) => (
            <button
              key={agent.agentId}
              type="button"
              role="option"
              aria-selected={selectedAgentId === agent.agentId}
              className={`environment-agent-chip ${selectedAgentId === agent.agentId ? "is-selected" : ""}`}
              onClick={() => onSelectAgent(agent.agentId)}
            >
              {onRequestRemoveAgent ? (
                <span
                  role="button"
                  tabIndex={0}
                  className="environment-agent-remove-button"
                  aria-label={`Remove ${agent.agentName}`}
                  onClick={(event) => {
                    event.stopPropagation();
                    onRequestRemoveAgent(agent.agentId);
                  }}
                  onKeyDown={(event) => {
                    if (event.key === "Enter" || event.key === " ") {
                      event.preventDefault();
                      event.stopPropagation();
                      onRequestRemoveAgent(agent.agentId);
                    }
                  }}
                >
                  -
                </span>
              ) : null}
              <div className="environment-agent-chip-header">
                <strong>{agent.agentName}</strong>
                <span className={`environment-agent-chip-status environment-agent-chip-status--${agent.status}`}>
                  {formatRunStatusLabel(agent.status)}
                </span>
              </div>
              <div className="environment-agent-chip-meta">
                <span>{agent.completedNodes}/{agent.totalNodes} nodes</span>
                <RunErrorHover count={agent.errorCount} summaries={agent.errorSummaries} emptyLabel="No errors" />
                <span>{agent.elapsedLabel}</span>
              </div>
            </button>
          )) : (
            <p className="environment-agent-menu-empty">Create another workflow to turn this grouping into a multi-workflow grouping.</p>
          )}
          {onCreateAgent ? (
            <button type="button" className="environment-agent-add-button" onClick={handleCreateAgent} aria-label="Add workflow">
              +
            </button>
          ) : null}
        </div>
      </div>
    </div>
  );
}
