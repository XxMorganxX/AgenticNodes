import { useEffect, useMemo, useState } from "react";
import type { KeyboardEvent } from "react";

import { formatRunStatusLabel, type AgentRunLane, type EnvironmentRunSummary as EnvironmentRunSummaryData } from "../lib/runVisualization";
import { RunErrorHover } from "./RunErrorHover";

type AgentRunSwimlanesProps = {
  lanes: AgentRunLane[];
  selectedAgentId: string | null;
  environmentRunSummary?: EnvironmentRunSummaryData | null;
  onSelectAgent: (agentId: string) => void;
  onSelectNode?: (agentId: string, nodeId: string | null) => void;
};

function hasActiveTextSelection(): boolean {
  if (typeof window === "undefined") {
    return false;
  }
  return (window.getSelection?.()?.toString() ?? "").trim().length > 0;
}

function handleKeyboardActivate(
  event: KeyboardEvent<HTMLElement>,
  activate: () => void,
): void {
  if (event.key !== "Enter" && event.key !== " ") {
    return;
  }
  event.preventDefault();
  activate();
}

function formatEventTypeLabel(eventType: string): string {
  return eventType
    .split(".")
    .flatMap((segment) => segment.split(/[\s_-]+/))
    .filter(Boolean)
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ");
}

const EVENT_TYPE_DISPLAY_ORDER = [
  "run.started",
  "node.started",
  "node.completed",
  "edge.selected",
  "condition.evaluated",
  "retry.triggered",
  "run.completed",
  "run.failed",
  "run.cancelled",
  "run.interrupted",
] as const;

function compareEventTypes(left: string, right: string): number {
  const leftIndex = EVENT_TYPE_DISPLAY_ORDER.indexOf(left as (typeof EVENT_TYPE_DISPLAY_ORDER)[number]);
  const rightIndex = EVENT_TYPE_DISPLAY_ORDER.indexOf(right as (typeof EVENT_TYPE_DISPLAY_ORDER)[number]);
  if (leftIndex !== -1 || rightIndex !== -1) {
    if (leftIndex === -1) {
      return 1;
    }
    if (rightIndex === -1) {
      return -1;
    }
    return leftIndex - rightIndex;
  }
  return left.localeCompare(right);
}

function formatStructuredValue(value: unknown): string {
  if (typeof value === "string") {
    return value;
  }
  try {
    return JSON.stringify(value, null, 2) ?? String(value);
  } catch {
    return String(value);
  }
}

const RUN_LEVEL_NODE_TYPE_KEY = "__run_level__";

function nodeTypeFilterKey(nodeTypeLabel: string | null): string {
  return nodeTypeLabel?.trim().length ? nodeTypeLabel.trim() : RUN_LEVEL_NODE_TYPE_KEY;
}

function nodeTypeFilterLabel(nodeTypeLabel: string | null): string {
  return nodeTypeLabel?.trim().length ? nodeTypeLabel.trim() : "Run-level";
}

export function AgentRunSwimlanes({
  lanes,
  selectedAgentId,
  environmentRunSummary,
  onSelectAgent,
  onSelectNode,
}: AgentRunSwimlanesProps) {
  const eventTypeSummaries = useMemo(() => {
    const counts = new Map<string, number>();
    lanes.forEach((lane) => {
      lane.milestones.forEach((milestone) => {
        counts.set(milestone.eventType, (counts.get(milestone.eventType) ?? 0) + 1);
      });
    });
    return [...counts.entries()]
      .sort(([left], [right]) => compareEventTypes(left, right))
      .map(([eventType, count]) => ({ eventType, count }));
  }, [lanes]);
  const [visibleEventTypes, setVisibleEventTypes] = useState<Record<string, boolean>>({});
  const [visibleNodeTypesByLane, setVisibleNodeTypesByLane] = useState<Record<string, Record<string, boolean>>>({});

  useEffect(() => {
    setVisibleEventTypes((previous) => {
      const next: Record<string, boolean> = {};
      eventTypeSummaries.forEach(({ eventType }) => {
        next[eventType] = previous[eventType] ?? true;
      });
      return next;
    });
  }, [eventTypeSummaries]);

  useEffect(() => {
    setVisibleNodeTypesByLane((previous) =>
      Object.fromEntries(
        lanes.map((lane) => {
          const lanePrevious = previous[lane.agentId] ?? {};
          const laneNext: Record<string, boolean> = {};
          lane.milestones.forEach((milestone) => {
            const key = nodeTypeFilterKey(milestone.nodeTypeLabel);
            laneNext[key] = lanePrevious[key] ?? true;
          });
          return [lane.agentId, laneNext];
        }),
      ),
    );
  }, [lanes]);

  const allEventTypesVisible =
    eventTypeSummaries.length === 0 || eventTypeSummaries.every(({ eventType }) => visibleEventTypes[eventType] !== false);
  const isMilestoneVisible = (laneAgentId: string, eventType: string, nodeTypeLabel: string | null): boolean =>
    visibleEventTypes[eventType] !== false &&
    visibleNodeTypesByLane[laneAgentId]?.[nodeTypeFilterKey(nodeTypeLabel)] !== false;
  const visibleMilestoneCount = useMemo(
    () =>
      lanes.reduce(
        (total, lane) =>
          total +
          lane.milestones.filter((milestone) => isMilestoneVisible(lane.agentId, milestone.eventType, milestone.nodeTypeLabel)).length,
        0,
      ),
    [lanes, visibleEventTypes, visibleNodeTypesByLane],
  );

  return (
    <section className="panel agent-swimlanes-panel">
      <div className="panel-header">
        <h2>Agent Run Swimlanes</h2>
        <p>Track each agent over time and click a milestone to focus its graph.</p>
      </div>
      {environmentRunSummary ? (
        <div className="swimlane-run-summary-bar">
          <span className={`swimlane-run-status swimlane-run-status--${environmentRunSummary.status}`}>
            {formatRunStatusLabel(environmentRunSummary.status)}
          </span>
          <span>{environmentRunSummary.totalAgents} agents</span>
          {environmentRunSummary.runningAgents > 0 ? <span>{environmentRunSummary.runningAgents} running</span> : null}
          {environmentRunSummary.completedAgents > 0 ? <span>{environmentRunSummary.completedAgents} completed</span> : null}
          {environmentRunSummary.failedAgents > 0 ? <span className="error-text">{environmentRunSummary.failedAgents} failed</span> : null}
          {environmentRunSummary.queuedAgents > 0 ? <span>{environmentRunSummary.queuedAgents} queued</span> : null}
          <span>{environmentRunSummary.elapsedLabel}</span>
          {environmentRunSummary.activeAgentNames.length > 0 ? (
            <span>Active: {environmentRunSummary.activeAgentNames.join(", ")}</span>
          ) : null}
        </div>
      ) : null}
      {eventTypeSummaries.length > 0 ? (
        <div className="agent-swimlane-filters" aria-label="Toggle milestone card types">
          <div className="agent-swimlane-filter-header">
            <strong>Card types</strong>
            <span>
              Showing {visibleMilestoneCount} of {eventTypeSummaries.reduce((total, summary) => total + summary.count, 0)} cards
            </span>
          </div>
          <div className="agent-swimlane-filter-actions">
            <button
              type="button"
              className={`agent-swimlane-filter-chip agent-swimlane-filter-chip--all ${allEventTypesVisible ? "is-active" : ""}`}
              onClick={() =>
                setVisibleEventTypes(
                  Object.fromEntries(eventTypeSummaries.map(({ eventType }) => [eventType, true])),
                )
              }
            >
              Show all
            </button>
            {eventTypeSummaries.map(({ eventType, count }) => {
              const isVisible = visibleEventTypes[eventType] !== false;
              return (
                <button
                  key={eventType}
                  type="button"
                  aria-pressed={isVisible}
                  className={`agent-swimlane-filter-chip ${isVisible ? "is-active" : ""}`}
                  onClick={() =>
                    setVisibleEventTypes((previous) => ({
                      ...previous,
                      [eventType]: !isVisible,
                    }))
                  }
                >
                  <span>{formatEventTypeLabel(eventType)}</span>
                  <strong>{count}</strong>
                </button>
              );
            })}
          </div>
        </div>
      ) : null}
      <div className="agent-swimlanes">
        {lanes.map((lane) => {
          const nodeTypeSummaries = (() => {
            const counts = new Map<string, { label: string; count: number }>();
            lane.milestones.forEach((milestone) => {
              const key = nodeTypeFilterKey(milestone.nodeTypeLabel);
              const existing = counts.get(key);
              if (existing) {
                existing.count += 1;
                return;
              }
              counts.set(key, {
                label: nodeTypeFilterLabel(milestone.nodeTypeLabel),
                count: 1,
              });
            });
            return [...counts.entries()]
              .sort(([leftKey, left], [rightKey, right]) => {
                if (leftKey === RUN_LEVEL_NODE_TYPE_KEY) {
                  return -1;
                }
                if (rightKey === RUN_LEVEL_NODE_TYPE_KEY) {
                  return 1;
                }
                return left.label.localeCompare(right.label);
              })
              .map(([nodeTypeKey, summary]) => ({ nodeTypeKey, ...summary }));
          })();
          const allNodeTypesVisible =
            nodeTypeSummaries.length === 0 ||
            nodeTypeSummaries.every(({ nodeTypeKey }) => visibleNodeTypesByLane[lane.agentId]?.[nodeTypeKey] !== false);
          const visibleNodeTypeCount = nodeTypeSummaries.filter(
            ({ nodeTypeKey }) => visibleNodeTypesByLane[lane.agentId]?.[nodeTypeKey] !== false,
          ).length;
          const visibleMilestones = lane.milestones.filter((milestone) =>
            isMilestoneVisible(lane.agentId, milestone.eventType, milestone.nodeTypeLabel),
          );
          return (
            <section
              key={lane.agentId}
              className={`agent-swimlane ${selectedAgentId === lane.agentId ? "is-selected" : ""}`}
              onClick={() => onSelectAgent(lane.agentId)}
            >
              <div
                role="button"
                tabIndex={0}
                className="agent-swimlane-meta"
                onClick={(event) => {
                  event.stopPropagation();
                  if (hasActiveTextSelection()) {
                    return;
                  }
                  onSelectAgent(lane.agentId);
                }}
                onKeyDown={(event) =>
                  handleKeyboardActivate(event, () => {
                    onSelectAgent(lane.agentId);
                  })
                }
              >
                <div className="agent-swimlane-heading">
                  <strong>{lane.agentName}</strong>
                  <span className={`agent-swimlane-status agent-swimlane-status--${lane.status}`}>
                    {formatRunStatusLabel(lane.status)}
                  </span>
                </div>
                <div className="agent-swimlane-stats">
                  <span>{lane.completedNodes}/{lane.totalNodes} nodes</span>
                  <span>{lane.transitionCount} transitions</span>
                  <span>{lane.retryCount} retries</span>
                  <RunErrorHover count={lane.errorCount} summaries={lane.errorSummaries} />
                  <span>{lane.elapsedLabel}</span>
                </div>
                <div className="agent-swimlane-current">Current: {lane.currentNodeLabel}</div>
              </div>
              {nodeTypeSummaries.length > 0 ? (
                <div className="agent-swimlane-filters" aria-label={`${lane.agentName} node type filters`}>
                  <div className="agent-swimlane-filter-header">
                    <strong>Node types</strong>
                    <span>Show only specific node types in this lane.</span>
                  </div>
                  <div className="agent-swimlane-filter-actions">
                    <button
                      type="button"
                      className={`agent-swimlane-filter-chip agent-swimlane-filter-chip--all ${allNodeTypesVisible ? "is-active" : ""}`}
                      onClick={(event) => {
                        event.stopPropagation();
                        setVisibleNodeTypesByLane((previous) => ({
                          ...previous,
                          [lane.agentId]: Object.fromEntries(nodeTypeSummaries.map(({ nodeTypeKey }) => [nodeTypeKey, true])),
                        }));
                      }}
                    >
                      Show all
                    </button>
                    {nodeTypeSummaries.map(({ nodeTypeKey, label, count }) => {
                      const isVisible = visibleNodeTypesByLane[lane.agentId]?.[nodeTypeKey] !== false;
                      return (
                        <button
                          key={nodeTypeKey}
                          type="button"
                          aria-pressed={isVisible}
                          className={`agent-swimlane-filter-chip ${isVisible ? "is-active" : ""}`}
                          onClick={(event) => {
                            event.stopPropagation();
                            const nextLaneState = allNodeTypesVisible
                              ? Object.fromEntries(nodeTypeSummaries.map(({ nodeTypeKey: candidateKey }) => [candidateKey, candidateKey === nodeTypeKey]))
                              : isVisible && visibleNodeTypeCount === 1
                                ? Object.fromEntries(nodeTypeSummaries.map(({ nodeTypeKey: candidateKey }) => [candidateKey, true]))
                                : {
                                    ...(visibleNodeTypesByLane[lane.agentId] ?? {}),
                                    [nodeTypeKey]: !isVisible,
                                  };
                            setVisibleNodeTypesByLane((previous) => ({
                              ...previous,
                              [lane.agentId]: nextLaneState,
                            }));
                          }}
                        >
                          <span>{label}</span>
                          <strong>{count}</strong>
                        </button>
                      );
                    })}
                  </div>
                </div>
              ) : null}
              <div className="agent-swimlane-track" role="list" aria-label={`${lane.agentName} milestones`}>
                {lane.milestones.length === 0 ? (
                  <div className="agent-swimlane-empty">No runtime milestones yet.</div>
                ) : visibleMilestones.length === 0 ? (
                  <div className="agent-swimlane-empty">All milestone cards are hidden by the current filters.</div>
                ) : (
                  visibleMilestones.map((milestone) => (
                    <div
                      key={milestone.id}
                      role="listitem"
                      tabIndex={0}
                      className={`agent-swimlane-milestone agent-swimlane-milestone--${milestone.tone}`}
                      title={[
                        milestone.label,
                        milestone.timestampDetail,
                        milestone.relativeTimestampLabel,
                        milestone.deltaLabel,
                      ]
                        .filter(Boolean)
                        .join(" · ")}
                      onClick={(event) => {
                        event.stopPropagation();
                        if (hasActiveTextSelection()) {
                          return;
                        }
                        onSelectAgent(lane.agentId);
                        onSelectNode?.(lane.agentId, milestone.nodeId);
                      }}
                      onKeyDown={(event) =>
                        handleKeyboardActivate(event, () => {
                          onSelectAgent(lane.agentId);
                          onSelectNode?.(lane.agentId, milestone.nodeId);
                        })
                      }
                    >
                      <span className="agent-swimlane-dot" />
                      <div className="agent-swimlane-milestone-content">
                        <div className="agent-swimlane-milestone-header">
                          <span className="agent-swimlane-milestone-copy">
                            <strong>{milestone.label}</strong>
                            <span>
                              {[formatEventTypeLabel(milestone.eventType), milestone.nodeTypeLabel].filter(Boolean).join(" · ")}
                            </span>
                          </span>
                          <span className="agent-swimlane-milestone-time">
                            <strong>{milestone.timestampLabel}</strong>
                            {milestone.relativeTimestampLabel ? <span>{milestone.relativeTimestampLabel}</span> : null}
                            {milestone.deltaLabel ? <span>{milestone.deltaLabel}</span> : null}
                          </span>
                        </div>
                        {milestone.details.length > 0 ? (
                          <div className="agent-swimlane-milestone-details">
                            {milestone.details.map((detail) => (
                              <span key={`${milestone.id}-${detail.label}`} className="agent-swimlane-milestone-detail">
                                <strong>{detail.label}</strong>
                                <span>{detail.value}</span>
                              </span>
                            ))}
                          </div>
                        ) : null}
                        {milestone.dataSections.length > 0 ? (
                          <div className="agent-swimlane-milestone-data">
                            {milestone.dataSections.map((section) => (
                              <div key={`${milestone.id}-${section.label}`} className="agent-swimlane-milestone-json">
                                <strong>{section.label}</strong>
                                <pre>{formatStructuredValue(section.value)}</pre>
                              </div>
                            ))}
                          </div>
                        ) : null}
                      </div>
                    </div>
                  ))
                )}
              </div>
            </section>
          );
        })}
      </div>
    </section>
  );
}
