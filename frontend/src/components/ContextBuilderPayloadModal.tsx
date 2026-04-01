import { useEffect, useMemo } from "react";
import type { MouseEvent } from "react";

import type { GraphNode, RunState } from "../lib/types";

function resolveContextBuilderPayload(node: GraphNode, runState: RunState | null): unknown {
  const nodeOutput = runState?.node_outputs?.[node.id];
  if (
    nodeOutput &&
    typeof nodeOutput === "object" &&
    !Array.isArray(nodeOutput) &&
    "payload" in nodeOutput
  ) {
    return nodeOutput.payload;
  }
  return nodeOutput ?? null;
}

type ContextBuilderPayloadModalProps = {
  node: GraphNode;
  runState: RunState | null;
  onClose: () => void;
};

export function ContextBuilderPayloadModal({ node, runState, onClose }: ContextBuilderPayloadModalProps) {
  useEffect(() => {
    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        onClose();
      }
    }

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [onClose]);

  const payload = useMemo(() => resolveContextBuilderPayload(node, runState), [node, runState]);
  const formattedPayload = useMemo(() => JSON.stringify(payload, null, 2), [payload]);

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
        aria-labelledby="context-builder-payload-modal-title"
      >
        <div className="tool-details-modal-header">
          <div>
            <div className="tool-details-modal-eyebrow">Context Builder Payload</div>
            <h3 id="context-builder-payload-modal-title">{node.label}</h3>
            <p>Inspect the full payload produced by this context builder during the current or latest run.</p>
          </div>
          <button type="button" className="secondary-button" onClick={onClose}>
            Close
          </button>
        </div>

        <div className="tool-details-modal-body">
          <section className="tool-details-modal-preview display-response-modal-preview">
            <div className="tool-details-modal-preview-header">
              <strong>Full Payload</strong>
              <span>{runState?.current_node_id === node.id ? "This node is still running." : "Snapshot from the latest available output."}</span>
            </div>
            <pre>{formattedPayload}</pre>
          </section>
        </div>
      </section>
    </div>
  );
}
