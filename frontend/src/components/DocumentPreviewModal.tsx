import { useEffect } from "react";
import type { MouseEvent } from "react";

type DocumentPreviewModalProps = {
  title: string;
  subtitle?: string;
  content: string;
  isLoading?: boolean;
  error?: string | null;
  truncated?: boolean;
  onClose: () => void;
};

export function DocumentPreviewModal({
  title,
  subtitle,
  content,
  isLoading,
  error,
  truncated,
  onClose,
}: DocumentPreviewModalProps) {
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
        className="tool-details-modal tool-details-modal--wide document-preview-modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="document-preview-modal-title"
      >
        <div className="tool-details-modal-header">
          <div>
            <div className="tool-details-modal-eyebrow">Uploaded Document</div>
            <h3 id="document-preview-modal-title">{title}</h3>
            {subtitle ? <p>{subtitle}</p> : null}
          </div>
          <button type="button" className="secondary-button" onClick={onClose}>
            Close
          </button>
        </div>

        <div className="tool-details-modal-body">
          <section className="tool-details-modal-preview document-preview-modal-preview">
            {isLoading ? (
              <p className="execution-file-preview-empty">Loading document...</p>
            ) : content ? (
              <pre className="execution-file-preview-content">{content}</pre>
            ) : (
              <p className="execution-file-preview-empty">No extractable text for this document.</p>
            )}
            {truncated ? (
              <p className="execution-file-preview-note">Preview truncated for large files.</p>
            ) : null}
            {error ? <p className="error-text">{error}</p> : null}
          </section>
        </div>
      </section>
    </div>
  );
}
