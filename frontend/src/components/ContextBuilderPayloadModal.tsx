import { useEffect, useMemo, useState } from "react";
import type { MouseEvent } from "react";

import {
  getContextBuilderBindings,
  normalizeContextBuilderHeader,
  type ContextBuilderBindingRow,
} from "../lib/contextBuilderBindings";
import type { ContextBuilderRuntimeView } from "../lib/contextBuilderRuntime";
import { getNodeInstanceLabel } from "../lib/nodeInstanceLabels";
import type { GraphDefinition, GraphNode, RunState } from "../lib/types";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function formatContextBuilderSourceValue(value: unknown): string {
  if (typeof value === "string") {
    return value;
  }
  if (value === undefined) {
    return "No context captured yet.";
  }
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

type ContextValueSection = {
  label: string;
  value: unknown;
};

type ContextBuilderStructuredEntry = {
  header: string;
  body: unknown;
};

function formatContextBuilderValueSections(value: unknown, fallbackLabel = "Value"): ContextValueSection[] {
  if (!isRecord(value)) {
    return [{ label: fallbackLabel, value }];
  }

  const sections: ContextValueSection[] = [];
  const hasPayload = Object.prototype.hasOwnProperty.call(value, "payload");
  const hasMetadata = Object.prototype.hasOwnProperty.call(value, "metadata");
  const hasArtifacts = Object.prototype.hasOwnProperty.call(value, "artifacts");
  const hasErrors = Object.prototype.hasOwnProperty.call(value, "errors");
  const hasToolCalls = Object.prototype.hasOwnProperty.call(value, "tool_calls");

  if (hasPayload || hasMetadata || hasArtifacts || hasErrors || hasToolCalls) {
    if (hasPayload) {
      sections.push({ label: "Payload", value: value.payload });
    }
    if (hasMetadata && isRecord(value.metadata) && Object.keys(value.metadata).length > 0) {
      sections.push({ label: "Metadata", value: value.metadata });
    }
    if (hasArtifacts && isRecord(value.artifacts) && Object.keys(value.artifacts).length > 0) {
      sections.push({ label: "Artifacts", value: value.artifacts });
    }
    if (hasErrors && Array.isArray(value.errors) && value.errors.length > 0) {
      sections.push({ label: "Errors", value: value.errors });
    }
    if (hasToolCalls && Array.isArray(value.tool_calls) && value.tool_calls.length > 0) {
      sections.push({ label: "Tool Calls", value: value.tool_calls });
    }
  }

  if (sections.length > 0) {
    return sections;
  }
  return [{ label: fallbackLabel, value }];
}

function extractStructuredContextEntries(value: unknown): ContextBuilderStructuredEntry[] | null {
  if (!Array.isArray(value) || value.length === 0) {
    return null;
  }
  const entries: ContextBuilderStructuredEntry[] = [];
  for (const item of value) {
    if (!isRecord(item)) {
      return null;
    }
    const pairs = Object.entries(item);
    if (pairs.length !== 1) {
      return null;
    }
    const [header, body] = pairs[0];
    if (!header.trim()) {
      return null;
    }
    entries.push({ header, body });
  }
  return entries;
}

function resolveContextBuilderSourceValue(runState: RunState | null, sourceNodeId: string): unknown {
  const sourceError = runState?.node_errors?.[sourceNodeId];
  if (sourceError !== undefined) {
    return sourceError;
  }
  const sourceOutput = runState?.node_outputs?.[sourceNodeId];
  return sourceOutput;
}

type ContextBuilderPayloadModalProps = {
  graph: GraphDefinition;
  node: GraphNode;
  runState: RunState | null;
  runtimeView: ContextBuilderRuntimeView | null;
  onGraphChange: (graph: GraphDefinition) => void;
  onClose: () => void;
};

function updateNode(graph: GraphDefinition, nodeId: string, updater: (node: GraphNode) => GraphNode): GraphDefinition {
  return {
    ...graph,
    nodes: graph.nodes.map((candidate) => (candidate.id === nodeId ? updater(candidate) : candidate)),
  };
}

export function ContextBuilderPayloadModal({ graph, node, runState, runtimeView, onGraphChange, onClose }: ContextBuilderPayloadModalProps) {
  const nodeLabel = getNodeInstanceLabel(graph, node);
  const [expandedSourceNodeId, setExpandedSourceNodeId] = useState<string | null>(null);

  useEffect(() => {
    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        onClose();
      }
    }

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [onClose]);

  const mergedOutput = useMemo(() => runState?.node_outputs?.[node.id] ?? null, [node, runState]);
  const mergedOutputSections = useMemo(
    () => formatContextBuilderValueSections(mergedOutput, "Merged payload"),
    [mergedOutput],
  );
  const contextBuilderBindings = useMemo(() => getContextBuilderBindings(node, graph), [graph, node]);

  useEffect(() => {
    if (!runtimeView || runtimeView.sources.length === 0) {
      setExpandedSourceNodeId(null);
      return;
    }
    if (expandedSourceNodeId && runtimeView.sources.some((slot) => slot.sourceNodeId === expandedSourceNodeId)) {
      return;
    }
    const firstSettledSource = runtimeView.sources.find((slot) => slot.status !== "pending");
    setExpandedSourceNodeId(firstSettledSource?.sourceNodeId ?? runtimeView.sources[0]?.sourceNodeId ?? null);
  }, [expandedSourceNodeId, runtimeView]);

  function handleOverlayClick(event: MouseEvent<HTMLDivElement>) {
    if (event.target === event.currentTarget) {
      onClose();
    }
  }

  const forwardingLabel = runtimeView?.isWaitingToForward
    ? "Holding downstream until every input is settled and the merge is complete."
    : runtimeView?.contextBuilderComplete === true
      ? "Merge complete — the run will forward this payload when execution reaches the next node."
      : runtimeView?.totalCount
        ? "Waiting for upstream nodes to produce output or errors for each bound input."
        : "Connect inputs to this context builder to track them here.";

  function updateContextBuilderBindings(bindings: ContextBuilderBindingRow[]) {
    onGraphChange(
      updateNode(graph, node.id, (candidate) => ({
        ...candidate,
        config: {
          ...candidate.config,
          input_bindings: bindings.map((binding) => ({
            source_node_id: binding.sourceNodeId,
            header: binding.rawHeader,
            placeholder: binding.placeholder,
            binding: binding.binding,
          })),
        },
      })),
    );
  }

  return (
    <div className="tool-details-modal-backdrop" onClick={handleOverlayClick} role="presentation">
      <section
        className="tool-details-modal display-response-modal context-builder-payload-modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="context-builder-payload-modal-title"
      >
        <div className="tool-details-modal-header">
          <div>
            <div className="tool-details-modal-eyebrow">Context Builder Payload</div>
            <h3 id="context-builder-payload-modal-title">{nodeLabel}</h3>
            <p>
              Each upstream input behaves like a promise: when it completes with a message or an error, it is reflected here. The builder only forwards once the
              runtime merge is complete.
            </p>
          </div>
          <button type="button" className="secondary-button" onClick={onClose}>
            Close
          </button>
        </div>

        <div className="tool-details-modal-body">
          <section className="context-builder-modal-section">
            <div className="tool-details-modal-preview-header">
              <strong>Forwarding</strong>
              <span className={runtimeView?.isWaitingToForward ? "context-builder-modal-flag context-builder-modal-flag--hold" : ""}>
                {forwardingLabel}
              </span>
            </div>
          </section>

          {runtimeView && runtimeView.sources.length > 0 ? (
            <section className="context-builder-modal-section">
              <div className="tool-details-modal-preview-header">
                <strong>Inputs</strong>
                <span>
                  {runtimeView.fulfilledCount + runtimeView.errorCount}/{runtimeView.totalCount} settled
                  {runtimeView.errorCount > 0 ? ` (${runtimeView.errorCount} with errors)` : ""}
                </span>
              </div>
              <ul className="context-builder-input-list">
                {runtimeView.sources.map((slot) => {
                  const isExpanded = expandedSourceNodeId === slot.sourceNodeId;
                  const contextValue = resolveContextBuilderSourceValue(runState, slot.sourceNodeId);
                  const detailId = `context-builder-input-detail-${slot.sourceNodeId}`;
                  const matchingBinding = contextBuilderBindings.find((binding) => binding.sourceNodeId === slot.sourceNodeId) ?? null;
                  return (
                    <li
                      key={slot.sourceNodeId}
                      className={`context-builder-input-row context-builder-input-row--${slot.status}${isExpanded ? " is-expanded" : ""}`}
                    >
                      <div className="context-builder-input-row-button">
                        <div className="context-builder-input-row-main">
                          <label className="context-builder-input-header-editor">
                            <span className="context-builder-input-header-label">Section header</span>
                            <input
                              className="context-builder-input-header-input"
                              value={matchingBinding?.rawHeader ?? ""}
                              placeholder={slot.sourceLabel}
                              onChange={(event) => {
                                const nextBindings = contextBuilderBindings.map((candidate) =>
                                  candidate.sourceNodeId === slot.sourceNodeId
                                    ? {
                                        ...candidate,
                                        rawHeader: event.target.value,
                                        header: normalizeContextBuilderHeader(event.target.value, candidate.sourceLabel),
                                      }
                                    : candidate,
                                );
                                updateContextBuilderBindings(nextBindings);
                              }}
                            />
                          </label>
                          <span className="context-builder-input-source">{slot.sourceLabel}</span>
                          <span className="context-builder-input-placeholder">{`{${slot.placeholder}}`}</span>
                        </div>
                        <div className="context-builder-input-status">
                          {slot.status === "pending" ? "Waiting…" : null}
                          {slot.status === "fulfilled" ? "Ready" : null}
                          {slot.status === "error" ? <span className="context-builder-input-error">{slot.errorSummary ?? "Error"}</span> : null}
                          <button
                            type="button"
                            className="context-builder-input-toggle"
                            aria-expanded={isExpanded}
                            aria-controls={detailId}
                            onClick={() => setExpandedSourceNodeId(isExpanded ? null : slot.sourceNodeId)}
                          >
                            {isExpanded ? "Hide context" : "Show context"}
                          </button>
                        </div>
                      </div>
                      {isExpanded ? (
                        <div id={detailId} className="context-builder-input-detail">
                          <div className="context-builder-input-detail-label">
                            {slot.status === "error" ? "Captured error" : "Captured context"}
                          </div>
                          <div className="context-builder-value-sections">
                            {formatContextBuilderValueSections(
                              contextValue,
                              slot.status === "error" ? "Error" : "Value",
                            ).map((section) => (
                              <div key={`${slot.sourceNodeId}-${section.label}`} className="context-builder-value-section">
                                <div className="context-builder-value-section-label">{section.label}</div>
                                <pre>{formatContextBuilderSourceValue(section.value)}</pre>
                              </div>
                            ))}
                          </div>
                        </div>
                      ) : null}
                    </li>
                  );
                })}
              </ul>
            </section>
          ) : null}

          <section className="tool-details-modal-preview display-response-modal-preview">
            <div className="tool-details-modal-preview-header">
              <strong>Current merged result</strong>
              <span>
                {runState?.current_node_id === node.id
                  ? "This node is executing."
                  : mergedOutput != null
                    ? "Latest snapshot from the active or most recent run."
                    : "No merged output yet for this run."}
              </span>
            </div>
            <div className="context-builder-value-sections">
              {mergedOutputSections.map((section) => {
                const structuredEntries = extractStructuredContextEntries(section.value);
                return (
                  <div key={`merged-${section.label}`} className="context-builder-value-section">
                    {structuredEntries ? (
                      <div className="context-builder-readable-preview">
                        {structuredEntries.map((entry, index) => (
                          <div key={`${section.label}-${entry.header}-${index}`} className="context-builder-readable-entry">
                            <div className="context-builder-readable-header">{entry.header}</div>
                            <pre className="context-builder-readable-body">{formatContextBuilderSourceValue(entry.body)}</pre>
                          </div>
                        ))}
                      </div>
                    ) : (
                      <>
                        <div className="context-builder-value-section-label">{section.label}</div>
                        <pre>{formatContextBuilderSourceValue(section.value)}</pre>
                      </>
                    )}
                  </div>
                );
              })}
            </div>
          </section>
        </div>
      </section>
    </div>
  );
}
