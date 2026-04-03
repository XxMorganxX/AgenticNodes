import { useEffect } from "react";
import type { ComponentProps, MouseEvent } from "react";

import { McpServerPanel } from "./McpServerPanel";

type McpServerModalProps = ComponentProps<typeof McpServerPanel> & {
  onClose: () => void;
};

export function McpServerModal({ onClose, ...panelProps }: McpServerModalProps) {
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
      <section className="tool-details-modal mcp-server-modal" role="dialog" aria-modal="true" aria-labelledby="mcp-server-modal-title">
        <div className="tool-details-modal-header">
          <div>
            <div className="tool-details-modal-eyebrow">Project MCP</div>
            <h3 id="mcp-server-modal-title">Manage MCP Servers</h3>
            <p>Project-level MCP servers live outside individual nodes. Use this modal to manage server lifecycle and global tool availability.</p>
          </div>
          <button type="button" className="secondary-button" onClick={onClose}>
            Close
          </button>
        </div>
        <div className="tool-details-modal-body mcp-server-modal-body">
          <McpServerPanel {...panelProps} />
        </div>
      </section>
    </div>
  );
}
