import { useEffect, useMemo } from "react";
import type { MouseEvent } from "react";

import { getNodeInstanceLabel } from "../lib/nodeInstanceLabels";
import type { GraphDefinition, GraphNode } from "../lib/types";

type ConditionResultsModalProps = {
  graph: GraphDefinition;
  node: GraphNode;
  conditionText: string;
  onClose: () => void;
};

export function ConditionResultsModal({ graph, node, conditionText, onClose }: ConditionResultsModalProps) {
  const nodeLabel = getNodeInstanceLabel(graph, node);

  useEffect(() => {
    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        onClose();
      }
    }
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [onClose]);

  const formattedConditionText = useMemo(() => conditionText, [conditionText]);

  function handleOverlayClick(event: MouseEvent<HTMLDivElement>) {
    if (event.target === event.currentTarget) {
      onClose();
    }
  }

  return (
    <div className="tool-details-modal-backdrop" onClick={handleOverlayClick} role="presentation">
      <section
        className="tool-details-modal display-response-modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="condition-results-modal-title"
      >
        <div className="tool-details-modal-header">
          <div>
            <div className="tool-details-modal-eyebrow">Condition Evaluation</div>
            <h3 id="condition-results-modal-title">{nodeLabel}</h3>
            <p>Inspect the last recorded clause-by-clause evaluation for this logic condition node.</p>
          </div>
          <button type="button" className="secondary-button" onClick={onClose}>
            Close
          </button>
        </div>

        <div className="tool-details-modal-body">
          <section className="tool-details-modal-preview display-response-modal-preview">
            <div className="tool-details-modal-preview-header">
              <strong>Condition Results</strong>
              <span>Snapshot from the latest available run output.</span>
            </div>
            <pre>{formattedConditionText}</pre>
          </section>
        </div>
      </section>
    </div>
  );
}
