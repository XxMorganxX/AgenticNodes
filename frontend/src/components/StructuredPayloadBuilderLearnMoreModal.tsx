import { useEffect } from "react";
import type { MouseEvent } from "react";

import { getNodeInstanceLabel } from "../lib/nodeInstanceLabels";
import type { GraphDefinition, GraphNode } from "../lib/types";

type StructuredPayloadBuilderLearnMoreModalProps = {
  graph: GraphDefinition;
  node: GraphNode;
  onClose: () => void;
};

export function StructuredPayloadBuilderLearnMoreModal({
  graph,
  node,
  onClose,
}: StructuredPayloadBuilderLearnMoreModalProps) {
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

  function handleOverlayClick(event: MouseEvent<HTMLDivElement>) {
    if (event.target === event.currentTarget) {
      onClose();
    }
  }

  return (
    <div className="tool-details-modal-backdrop" onClick={handleOverlayClick} role="presentation">
      <section
        className="tool-details-modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="structured-payload-builder-learn-more-title"
      >
        <div className="tool-details-modal-header">
          <div>
            <div className="tool-details-modal-eyebrow">Structured Payload Builder</div>
            <h3 id="structured-payload-builder-learn-more-title">{nodeLabel}</h3>
            <p>Shape a clean JSON object by writing only the values you care about and letting the node fill the rest from upstream payload data.</p>
          </div>
          <button type="button" className="secondary-button" onClick={onClose}>
            Close
          </button>
        </div>

        <div className="tool-details-modal-body">
          <div className="modal-folder-panel">
            <div className="modal-folder-section">
              <div className="contract-card">
                <strong>How It Works</strong>
                <span>Write a JSON object template in `Template JSON`.</span>
                <span>Any explicit value you provide is preserved exactly as written.</span>
                <span>Missing values are auto-filled by recursively searching the incoming payload for matching field names.</span>
              </div>

              <div className="contract-card">
                <strong>Fields Eligible For Auto-Fill</strong>
                <span>Empty string values like <code>""</code></span>
                <span><code>null</code></span>
                <span>Empty objects like <code>{`{}`}</code></span>
                <span>Empty arrays like <code>[]</code></span>
              </div>

              <div className="contract-card">
                <strong>Example</strong>
                <pre>{`Template JSON
{
  "name": "",
  "domain": "openai.com",
  "linkedin_url": "",
  "email": ""
}

Incoming payload
{
  "person": {
    "name": "Taylor Doe",
    "linkedin_url": "https://www.linkedin.com/in/taylor-doe/"
  },
  "contact": {
    "email": "taylor@openai.com"
  },
  "company": {
    "domain": "example.com"
  }
}

Output
{
  "name": "Taylor Doe",
  "domain": "openai.com",
  "linkedin_url": "https://www.linkedin.com/in/taylor-doe/",
  "email": "taylor@openai.com"
}`}</pre>
              </div>

              <div className="contract-card">
                <strong>Notes</strong>
                <span>Nested objects use the parent key as context when possible, so a `person` block prefers values found under `person` first.</span>
                <span>`Case Sensitive` controls whether key matching ignores letter case.</span>
                <span>`Max Matches Per Field` limits recursive search work for each missing field.</span>
              </div>
            </div>
          </div>
        </div>
      </section>
    </div>
  );
}
