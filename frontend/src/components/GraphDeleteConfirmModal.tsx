import { useEffect, useMemo, useRef, useState } from "react";
import type { FormEvent, MouseEvent } from "react";

type GraphDeleteConfirmModalProps = {
  graphName: string;
  isDeleting: boolean;
  onClose: () => void;
  onConfirm: () => void;
};

export function GraphDeleteConfirmModal({ graphName, isDeleting, onClose, onConfirm }: GraphDeleteConfirmModalProps) {
  const [confirmationName, setConfirmationName] = useState("");
  const inputRef = useRef<HTMLInputElement | null>(null);
  const normalizedGraphName = graphName.trim();
  const canDelete = useMemo(
    () => confirmationName.trim() === normalizedGraphName && normalizedGraphName.length > 0 && !isDeleting,
    [confirmationName, isDeleting, normalizedGraphName],
  );

  useEffect(() => {
    inputRef.current?.focus();
  }, []);

  useEffect(() => {
    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape" && !isDeleting) {
        onClose();
      }
    }

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [isDeleting, onClose]);

  function handleOverlayClick(event: MouseEvent<HTMLDivElement>) {
    if (event.target === event.currentTarget && !isDeleting) {
      onClose();
    }
  }

  function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (canDelete) {
      onConfirm();
    }
  }

  return (
    <div className="tool-details-modal-backdrop" onClick={handleOverlayClick} role="presentation">
      <section
        className="tool-details-modal graph-delete-confirm-modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="graph-delete-confirm-modal-title"
      >
        <div className="tool-details-modal-header">
          <div>
            <div className="tool-details-modal-eyebrow">Delete Grouping</div>
            <h3 id="graph-delete-confirm-modal-title">Delete {normalizedGraphName}</h3>
            <p>This removes the grouping from the website and clears local grouping-specific state.</p>
          </div>
          <button type="button" className="secondary-button" onClick={onClose} disabled={isDeleting}>
            Cancel
          </button>
        </div>

        <form className="tool-details-modal-body graph-delete-confirm-modal-body" onSubmit={handleSubmit}>
          <div className="tool-details-modal-help graph-delete-confirm-warning">
            Type <strong>{normalizedGraphName}</strong> to confirm deletion.
          </div>
          <label className="graph-delete-confirm-field">
            Grouping name
            <input
              ref={inputRef}
              type="text"
              value={confirmationName}
              onChange={(event) => setConfirmationName(event.target.value)}
              placeholder={normalizedGraphName}
              autoComplete="off"
              disabled={isDeleting}
            />
          </label>
          <div className="preferences-modal-actions">
            <button type="button" className="secondary-button" onClick={onClose} disabled={isDeleting}>
              Go Back
            </button>
            <button type="submit" className="danger-button" disabled={!canDelete}>
              {isDeleting ? "Deleting..." : "Delete Grouping"}
            </button>
          </div>
        </form>
      </section>
    </div>
  );
}
