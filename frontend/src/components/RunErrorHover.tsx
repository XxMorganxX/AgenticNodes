import { useEffect, useId, useRef, useState } from "react";

import type { AgentRunErrorSummary } from "../lib/runVisualization";

type RunErrorHoverProps = {
  count: number;
  summaries: AgentRunErrorSummary[];
  className?: string;
  emptyLabel?: string;
};

const MAX_VISIBLE_ERRORS = 6;
const CLOSE_DELAY_MS = 140;

function formatTooltipTitle(summaries: AgentRunErrorSummary[]): string {
  return summaries
    .map((summary) =>
      [`${summary.nodeLabel}: ${summary.message}`, ...summary.metadata].filter(Boolean).join(" · "),
    )
    .join("\n");
}

export function RunErrorHover({
  count,
  summaries,
  className = "",
  emptyLabel = "0 errors",
}: RunErrorHoverProps) {
  const [isOpen, setIsOpen] = useState(false);
  const closeTimeoutRef = useRef<number | null>(null);
  const tooltipId = useId();

  const clearCloseTimeout = () => {
    if (closeTimeoutRef.current !== null) {
      window.clearTimeout(closeTimeoutRef.current);
      closeTimeoutRef.current = null;
    }
  };

  const openTooltip = () => {
    clearCloseTimeout();
    setIsOpen(true);
  };

  const scheduleTooltipClose = () => {
    clearCloseTimeout();
    closeTimeoutRef.current = window.setTimeout(() => {
      setIsOpen(false);
      closeTimeoutRef.current = null;
    }, CLOSE_DELAY_MS);
  };

  useEffect(() => () => clearCloseTimeout(), []);

  if (count <= 0 || summaries.length === 0) {
    return <span className={`run-error-hover run-error-hover--empty ${className}`.trim()}>{emptyLabel}</span>;
  }

  const visibleSummaries = summaries.slice(0, MAX_VISIBLE_ERRORS);
  const remainingCount = Math.max(0, summaries.length - visibleSummaries.length);

  return (
    <span className={`run-error-hover ${isOpen ? "is-open" : ""} ${className}`.trim()} title={formatTooltipTitle(summaries)}>
      <span
        className="run-error-hover-trigger"
        aria-describedby={isOpen ? tooltipId : undefined}
        tabIndex={0}
        onMouseEnter={openTooltip}
        onMouseLeave={scheduleTooltipClose}
        onFocus={openTooltip}
        onBlur={scheduleTooltipClose}
      >
        {count} error{count === 1 ? "" : "s"}
      </span>
      <span
        id={tooltipId}
        className="run-error-hover-popup"
        role="tooltip"
        onMouseEnter={openTooltip}
        onMouseLeave={scheduleTooltipClose}
      >
        <strong>Run Errors</strong>
        {visibleSummaries.map((summary) => (
          <span key={summary.id} className="run-error-hover-item">
            <span className="run-error-hover-item-header">
              <span>{summary.nodeLabel}</span>
              {summary.errorTypeLabel ? <span>{summary.errorTypeLabel}</span> : null}
            </span>
            <span className="run-error-hover-item-message">{summary.message}</span>
            {summary.metadata.length > 0 ? <span className="run-error-hover-item-meta">{summary.metadata.join(" · ")}</span> : null}
          </span>
        ))}
        {remainingCount > 0 ? (
          <span className="run-error-hover-more">
            +{remainingCount} more error{remainingCount === 1 ? "" : "s"}
          </span>
        ) : null}
      </span>
    </span>
  );
}
