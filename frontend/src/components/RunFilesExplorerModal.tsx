import { useEffect, useMemo, useState } from "react";
import type { MouseEvent } from "react";

import type { RunFilesystemFile, RunFilesystemFileContent, RunFilesystemListing } from "../lib/types";

type RunFilesExplorerModalProps = {
  listing: RunFilesystemListing | null;
  files: RunFilesystemFile[];
  selectedFilePath: string | null;
  selectedFile: RunFilesystemFile | null;
  selectedFileContent: RunFilesystemFileContent | null;
  isRunFilesLoading: boolean;
  isRunFileContentLoading: boolean;
  runFilesError: string | null;
  runFileContentError: string | null;
  onClose: () => void;
  onRefresh: () => void;
  onSelectFile: (path: string) => void;
};

function formatDocumentSize(sizeBytes: number): string {
  if (!Number.isFinite(sizeBytes) || sizeBytes <= 0) {
    return "0 B";
  }
  if (sizeBytes < 1024) {
    return `${sizeBytes} B`;
  }
  if (sizeBytes < 1024 * 1024) {
    return `${(sizeBytes / 1024).toFixed(1)} KB`;
  }
  return `${(sizeBytes / (1024 * 1024)).toFixed(1)} MB`;
}

function formatTimestamp(value: string): string {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return parsed.toLocaleString();
}

export function RunFilesExplorerModal({
  listing,
  files,
  selectedFilePath,
  selectedFile,
  selectedFileContent,
  isRunFilesLoading,
  isRunFileContentLoading,
  runFilesError,
  runFileContentError,
  onClose,
  onRefresh,
  onSelectFile,
}: RunFilesExplorerModalProps) {
  const [searchValue, setSearchValue] = useState("");
  const [agentFilter, setAgentFilter] = useState<string>("all");
  const [isPreviewExpanded, setIsPreviewExpanded] = useState(false);

  useEffect(() => {
    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        onClose();
      }
    }
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [onClose]);

  const agentIds = useMemo(() => {
    const values = Array.from(
      new Set(files.map((file) => String(file.agent_id ?? "").trim()).filter((value) => value.length > 0)),
    );
    return values.sort((left, right) => left.localeCompare(right));
  }, [files]);

  useEffect(() => {
    if (agentFilter !== "all" && !agentIds.includes(agentFilter)) {
      setAgentFilter("all");
    }
  }, [agentFilter, agentIds]);

  const filteredFiles = useMemo(() => {
    const query = searchValue.trim().toLowerCase();
    return files.filter((file) => {
      if (agentFilter !== "all" && file.agent_id !== agentFilter) {
        return false;
      }
      if (!query) {
        return true;
      }
      return [file.name, file.path, file.agent_id ?? ""].some((value) => value.toLowerCase().includes(query));
    });
  }, [agentFilter, files, searchValue]);

  function handleOverlayClick(event: MouseEvent<HTMLDivElement>) {
    if (event.target === event.currentTarget) {
      onClose();
    }
  }

  return (
    <div className="tool-details-modal-backdrop" onClick={handleOverlayClick} role="presentation">
      <section
        className="tool-details-modal tool-details-modal--wide run-files-explorer-modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="run-files-explorer-modal-title"
      >
        <div className="tool-details-modal-header run-files-explorer-modal-header">
          <div>
            <div className="tool-details-modal-eyebrow">Workspace Explorer</div>
            <h3 id="run-files-explorer-modal-title">Agent Files</h3>
            <p>Browse run artifacts in a dedicated file explorer instead of the cramped inline panel.</p>
          </div>
          <div className="run-files-explorer-actions">
            <button type="button" className="secondary-button" onClick={onRefresh}>
              {isRunFilesLoading ? "Refreshing..." : "Refresh"}
            </button>
            <button type="button" className="secondary-button" onClick={onClose}>
              Close
            </button>
          </div>
        </div>

        <div className="tool-details-modal-body run-files-explorer-modal-body">
          <section className="run-files-explorer-summary">
            <div className="run-files-explorer-stat">
              <span>Files</span>
              <strong>{files.length}</strong>
            </div>
            <div className="run-files-explorer-stat">
              <span>Agents</span>
              <strong>{agentIds.length > 0 ? agentIds.length : "1"}</strong>
            </div>
            <div className="run-files-explorer-workspace">
              <span>Workspace root</span>
              <code>{listing?.workspace_root ?? "Workspace root unavailable"}</code>
            </div>
          </section>

          <section className="run-files-explorer-toolbar">
            <label className="run-files-explorer-search">
              <span>Search</span>
              <input
                type="search"
                value={searchValue}
                onChange={(event) => setSearchValue(event.target.value)}
                placeholder="Filter by filename or path"
              />
            </label>
            {agentIds.length > 1 ? (
              <div className="run-files-explorer-filters" role="tablist" aria-label="Agent file filters">
                <button
                  type="button"
                  className={`run-files-explorer-filter ${agentFilter === "all" ? "is-active" : ""}`}
                  onClick={() => setAgentFilter("all")}
                >
                  All Agents
                </button>
                {agentIds.map((agentId) => (
                  <button
                    key={agentId}
                    type="button"
                    className={`run-files-explorer-filter ${agentFilter === agentId ? "is-active" : ""}`}
                    onClick={() => setAgentFilter(agentId)}
                  >
                    {agentId}
                  </button>
                ))}
              </div>
            ) : null}
          </section>

          <div className={`execution-files-browser run-files-explorer-browser ${isPreviewExpanded ? "is-preview-expanded" : ""}`}>
            <div className="execution-files-list-panel run-files-explorer-list-panel">
              <div className="execution-files-browser-header">
                <strong>Workspace Files</strong>
                <span>{filteredFiles.length} shown</span>
              </div>
              {filteredFiles.length > 0 ? (
                <div className="execution-files-list" role="list" aria-label="Agent workspace files">
                  {filteredFiles.map((file) => (
                    <button
                      key={file.path}
                      type="button"
                      className={`execution-file-row ${selectedFilePath === file.path ? "is-selected" : ""}`}
                      onClick={() => onSelectFile(file.path)}
                    >
                      <strong>{file.name}</strong>
                      <span className="execution-file-row-path">{file.path}</span>
                      <span>
                        {formatDocumentSize(file.size_bytes)} · {formatTimestamp(file.modified_at)}
                      </span>
                    </button>
                  ))}
                </div>
              ) : (
                <p className="execution-file-preview-empty">No files match the current search and filter.</p>
              )}
            </div>

            <div className="execution-file-preview run-files-explorer-preview">
              {selectedFile ? (
                <>
                  <div className="execution-files-browser-header execution-files-browser-header--preview">
                    <div>
                      <strong>{selectedFile.name}</strong>
                      <span className="execution-file-preview-path">{selectedFile.path}</span>
                    </div>
                    <div className="run-files-explorer-preview-header-actions">
                      <span>
                        {selectedFile.mime_type} · {formatDocumentSize(selectedFile.size_bytes)}
                      </span>
                      <button
                        type="button"
                        className="secondary-button run-files-explorer-preview-toggle"
                        onClick={() => setIsPreviewExpanded((current) => !current)}
                      >
                        {isPreviewExpanded ? "Collapse" : "Expand"}
                      </button>
                    </div>
                  </div>
                  {isRunFileContentLoading ? <p className="execution-file-preview-empty">Loading file preview...</p> : null}
                  {selectedFileContent ? <pre className="execution-file-preview-content">{selectedFileContent.content}</pre> : null}
                  {selectedFileContent?.truncated ? (
                    <p className="execution-file-preview-note">Preview truncated for large files.</p>
                  ) : null}
                  {selectedFileContent?.workspace_path ? (
                    <p className="execution-file-preview-note">Stored in workspace as {selectedFileContent.workspace_path}.</p>
                  ) : null}
                </>
              ) : (
                <p className="execution-file-preview-empty">Select a file to preview it.</p>
              )}
              {runFileContentError ? <p className="error-text">{runFileContentError}</p> : null}
            </div>
          </div>

          {runFilesError ? <p className="error-text">{runFilesError}</p> : null}
        </div>
      </section>
    </div>
  );
}
