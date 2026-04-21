import { useCallback, useEffect, useMemo, useState } from "react";
import type { ChangeEvent, MouseEvent } from "react";

import { NodeDetailsForm } from "./NodeDetailsForm";
import { getNodeInstanceLabel } from "../lib/nodeInstanceLabels";
import { insertTokenAtEnd, listPromptBlockAvailableVariables, PROMPT_BLOCK_STARTERS, renderPromptBlockPreview } from "../lib/promptBlockEditor";
import type { EditorCatalog, GraphDefinition, GraphNode, RunState } from "../lib/types";
import { useDebouncedValue } from "../lib/useDebouncedValue";
import { useModalNodeDraft } from "../lib/useModalNodeDraft";

type PromptBlockDetailsModalProps = {
  graph: GraphDefinition;
  node: GraphNode;
  catalog: EditorCatalog | null;
  runState: RunState | null;
  onGraphChange: (graph: GraphDefinition) => void;
  onBackgroundPersistGraph?: (graph: GraphDefinition) => void;
  onClose: () => void;
};

type PromptBlockDetailsModalTab = "node" | "content" | "preview";

export function PromptBlockDetailsModal({
  graph,
  node,
  catalog,
  runState,
  onGraphChange,
  onBackgroundPersistGraph,
  onClose,
}: PromptBlockDetailsModalProps) {
  const {
    draftNode,
    updateDraftNode,
    flushCommit,
  } = useModalNodeDraft({
    graph,
    node,
    onGraphChange,
    onBackgroundPersist: onBackgroundPersistGraph,
    debounceMs: 750,
  });
  const [activeTab, setActiveTab] = useState<PromptBlockDetailsModalTab>("node");
  const debouncedPreviewNode = useDebouncedValue(draftNode, 150);
  const nodeLabel = useMemo(() => {
    const trimmedLabel = String(draftNode.label ?? "").trim();
    if (trimmedLabel) {
      return trimmedLabel;
    }
    return getNodeInstanceLabel(graph, draftNode);
  }, [draftNode, graph]);
  const availableVariables = listPromptBlockAvailableVariables(graph);
  const renderedPreview = activeTab === "preview" ? renderPromptBlockPreview(debouncedPreviewNode, graph, runState) : "";

  const handleRequestClose = useCallback(() => {
    flushCommit();
    onClose();
  }, [flushCommit, onClose]);

  useEffect(() => {
    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        handleRequestClose();
      }
    }

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [handleRequestClose]);

  useEffect(() => {
    setActiveTab("node");
  }, [node.id]);

  function updatePromptBlockConfig(updater: (config: GraphNode["config"]) => GraphNode["config"]) {
    updateDraftNode((currentNode) => ({
      ...currentNode,
      config: updater(currentNode.config),
    }));
  }

  function handleOverlayClick(event: MouseEvent<HTMLDivElement>) {
    if (event.target === event.currentTarget) {
      handleRequestClose();
    }
  }

  function handleRoleChange(event: ChangeEvent<HTMLSelectElement>) {
    updatePromptBlockConfig((config) => ({
      ...config,
      role: event.target.value,
      mode: "prompt_block",
    }));
  }

  function handleNameChange(event: ChangeEvent<HTMLInputElement>) {
    updatePromptBlockConfig((config) => ({
      ...config,
      name: event.target.value,
      mode: "prompt_block",
    }));
  }

  function handleContentChange(event: ChangeEvent<HTMLTextAreaElement>) {
    updatePromptBlockConfig((config) => ({
      ...config,
      content: event.target.value,
      mode: "prompt_block",
    }));
  }

  function handleInsertStarter() {
    updatePromptBlockConfig((config) => {
      const role = String(config.role ?? "user");
      const fallback = PROMPT_BLOCK_STARTERS[role] ?? PROMPT_BLOCK_STARTERS.user;
      const existingContent = String(config.content ?? "");
      return {
        ...config,
        content: existingContent.trim().length > 0 ? existingContent : fallback,
        mode: "prompt_block",
      };
    });
  }

  function handleInsertVariable(token: string) {
    updatePromptBlockConfig((config) => ({
      ...config,
      content: insertTokenAtEnd(String(config.content ?? ""), `{${token}}`),
      mode: "prompt_block",
    }));
  }

  function handleNodeChange(nextNode: GraphNode) {
    updateDraftNode(() => ({
      ...nextNode,
      config: {
        ...nextNode.config,
        mode: "prompt_block",
      },
    }));
  }

  return (
    <div className="tool-details-modal-backdrop" onClick={handleOverlayClick} role="presentation">
      <section
        className="tool-details-modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="prompt-block-details-modal-title"
      >
        <div className="tool-details-modal-header">
          <div>
            <div className="tool-details-modal-eyebrow">Prompt Block Details</div>
            <h3 id="prompt-block-details-modal-title">{nodeLabel}</h3>
            <p>Edit this message block in a dedicated modal and preview the rendered prompt with current graph variables.</p>
          </div>
          <button type="button" className="secondary-button" onClick={handleRequestClose}>
            Close
          </button>
        </div>

        <div className="tool-details-modal-body">
          <div className="modal-folder-tabs" role="tablist" aria-label="Prompt block sections">
            {[
              ["node", "Node"],
              ["content", "Content"],
              ["preview", "Preview"],
            ].map(([tabId, label]) => (
              <button
                key={tabId}
                type="button"
                role="tab"
                aria-selected={activeTab === tabId}
                className={`modal-folder-tab ${activeTab === tabId ? "modal-folder-tab--active" : ""}`}
                onClick={() => setActiveTab(tabId as PromptBlockDetailsModalTab)}
              >
                {label}
              </button>
            ))}
          </div>

          <div className="modal-folder-panel">
            {activeTab === "node" ? (
              <NodeDetailsForm
                graph={graph}
                node={draftNode}
                catalog={catalog}
                onNodeChange={handleNodeChange}
              />
            ) : null}

            {activeTab === "content" ? (
              <div className="modal-folder-section">
                <div className="context-builder-placeholder-bar">
                  <button type="button" className="secondary-button context-builder-inline-button" onClick={handleInsertStarter}>
                    Insert Starter
                  </button>
                  {availableVariables.map((token) => (
                    <button
                      key={token}
                      type="button"
                      className="secondary-button context-builder-token-button"
                      onClick={() => handleInsertVariable(token)}
                    >
                      {`{${token}}`}
                    </button>
                  ))}
                </div>

                <label>
                  Message Role
                  <select value={String(draftNode.config.role ?? "user")} onChange={handleRoleChange}>
                    <option value="system">system</option>
                    <option value="user">user</option>
                    <option value="assistant">assistant</option>
                  </select>
                </label>

                <label>
                  Message Name
                  <input
                    value={String(draftNode.config.name ?? "")}
                    placeholder="Optional label for the message block"
                    onChange={handleNameChange}
                  />
                </label>

                <label>
                  Message Content
                  <textarea
                    rows={8}
                    value={String(draftNode.config.content ?? "")}
                    placeholder="Enter the message content to inject into downstream prompt assembly."
                    onChange={handleContentChange}
                  />
                </label>

                <div className="tool-details-modal-help">
                  Available variables: {availableVariables.length > 0 ? availableVariables.join(", ") : "None"}
                </div>
              </div>
            ) : null}

            {activeTab === "preview" ? (
              <section className="tool-details-modal-preview">
                <div className="tool-details-modal-preview-header">
                  <strong>Rendered Preview</strong>
                  <span>This shows how the prompt block resolves after variable substitution.</span>
                </div>
                <pre>{renderedPreview}</pre>
              </section>
            ) : null}
          </div>
        </div>
      </section>
    </div>
  );
}
