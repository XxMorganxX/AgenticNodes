import { useEffect } from "react";
import type { MouseEvent } from "react";

type ProductionRunConfirmModalProps = {
  onClose: () => void;
  onConfirm: () => void;
};

export function ProductionRunConfirmModal({ onClose, onConfirm }: ProductionRunConfirmModalProps) {
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
        className="tool-details-modal production-run-confirm-modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="production-run-confirm-modal-title"
      >
        <div className="tool-details-modal-header">
          <div>
            <div className="tool-details-modal-eyebrow">Production Run</div>
            <h3 id="production-run-confirm-modal-title">Confirm Production Run</h3>
            <p>This run is configured to write to production email tables. Continue only if you intend to affect live email records.</p>
          </div>
          <button type="button" className="secondary-button" onClick={onClose}>
            Cancel
          </button>
        </div>

        <div className="tool-details-modal-body production-run-confirm-modal-body">
          <div className="tool-details-modal-help">
            Production mode targets <code>outbound_email_messages</code> and <code>inbound_email_messages</code> instead of the <code>_dev</code> tables.
          </div>
          <div className="preferences-modal-actions">
            <button type="button" className="secondary-button" onClick={onClose}>
              Go Back
            </button>
            <button type="button" className="danger-button" onClick={onConfirm}>
              Run In Production
            </button>
          </div>
        </div>
      </section>
    </div>
  );
}
