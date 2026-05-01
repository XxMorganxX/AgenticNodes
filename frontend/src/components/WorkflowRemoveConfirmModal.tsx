import { useEffect } from "react";
import type { MouseEvent } from "react";

type WorkflowRemoveConfirmModalProps = {
  workflowName: string;
  onClose: () => void;
  onConfirm: () => void;
};

export function WorkflowRemoveConfirmModal({ workflowName, onClose, onConfirm }: WorkflowRemoveConfirmModalProps) {
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
        className="tool-details-modal workflow-remove-confirm-modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="workflow-remove-confirm-modal-title"
      >
        <div className="tool-details-modal-header">
          <div>
            <div className="tool-details-modal-eyebrow">Remove Workflow</div>
            <h3 id="workflow-remove-confirm-modal-title">Remove {workflowName}?</h3>
            <p>This removes the workflow from this grouping. The rest of the grouping stays intact.</p>
          </div>
          <button type="button" className="secondary-button" onClick={onClose}>
            Cancel
          </button>
        </div>

        <div className="tool-details-modal-body">
          <div className="preferences-modal-actions">
            <button type="button" className="secondary-button" onClick={onClose}>
              Go Back
            </button>
            <button type="button" className="danger-button" onClick={onConfirm}>
              Remove Workflow
            </button>
          </div>
        </div>
      </section>
    </div>
  );
}
