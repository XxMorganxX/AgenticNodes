import { useEffect, useMemo, useRef } from "react";
import type { ChangeEvent, CSSProperties, MouseEvent, ReactNode, UIEvent } from "react";

type PromptPane = {
  key: "system" | "user";
  title: string;
  description?: string;
  value: string;
  placeholder?: string;
  onChange: (nextValue: string) => void;
};

type PromptFullscreenModalProps = {
  eyebrow: string;
  title: string;
  description?: string;
  initialFocus?: "system" | "user";
  panes: [PromptPane, PromptPane];
  validPlaceholders: string[];
  onClose: () => void;
};

const PLACEHOLDER_PATTERN = /\{([A-Za-z_][A-Za-z0-9_]*)\}/g;

type PlaceholderPalette = {
  outline: string;
  background: string;
  text: string;
  dot: string;
};

function placeholderPalette(token: string): PlaceholderPalette {
  let hash = 0;
  for (let index = 0; index < token.length; index += 1) {
    hash = (hash * 31 + token.charCodeAt(index)) & 0x7fffffff;
  }
  const hue = hash % 360;
  return {
    outline: `hsl(${hue}, 78%, 68%)`,
    background: `hsla(${hue}, 80%, 60%, 0.18)`,
    text: `hsl(${hue}, 92%, 88%)`,
    dot: `hsl(${hue}, 80%, 62%)`,
  };
}

function highlightPlaceholders(text: string, validSet: Set<string>): ReactNode[] {
  const nodes: ReactNode[] = [];
  if (!text) {
    return nodes;
  }
  let cursor = 0;
  let match: RegExpExecArray | null;
  let counter = 0;
  PLACEHOLDER_PATTERN.lastIndex = 0;
  while ((match = PLACEHOLDER_PATTERN.exec(text)) !== null) {
    const token = match[1] ?? "";
    if (!validSet.has(token)) {
      continue;
    }
    if (match.index > cursor) {
      nodes.push(text.slice(cursor, match.index));
    }
    const palette = placeholderPalette(token);
    const style: CSSProperties = {
      color: palette.text,
      background: palette.background,
      boxShadow: `0 0 0 1px ${palette.outline} inset`,
    };
    nodes.push(
      <span
        key={`ph-${counter}-${match.index}`}
        className="prompt-fullscreen-placeholder"
        style={style}
      >
        {`{${token}}`}
      </span>,
    );
    counter += 1;
    cursor = PLACEHOLDER_PATTERN.lastIndex;
  }
  if (cursor < text.length) {
    nodes.push(text.slice(cursor));
  }
  return nodes;
}

function collectUniqueTokens(validSet: Set<string>, ...sources: string[]): string[] {
  const seen = new Set<string>();
  const tokens: string[] = [];
  for (const source of sources) {
    PLACEHOLDER_PATTERN.lastIndex = 0;
    let match: RegExpExecArray | null;
    while ((match = PLACEHOLDER_PATTERN.exec(source)) !== null) {
      const token = match[1] ?? "";
      if (token && validSet.has(token) && !seen.has(token)) {
        seen.add(token);
        tokens.push(token);
      }
    }
  }
  return tokens;
}

type HighlightedPromptEditorProps = {
  pane: PromptPane;
  autoFocus: boolean;
  validSet: Set<string>;
};

function HighlightedPromptEditor({ pane, autoFocus, validSet }: HighlightedPromptEditorProps) {
  const textareaRef = useRef<HTMLTextAreaElement | null>(null);
  const overlayRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (autoFocus) {
      textareaRef.current?.focus({ preventScroll: true });
    }
  }, [autoFocus]);

  function handleScroll(event: UIEvent<HTMLTextAreaElement>) {
    const overlay = overlayRef.current;
    if (!overlay) {
      return;
    }
    overlay.scrollTop = event.currentTarget.scrollTop;
    overlay.scrollLeft = event.currentTarget.scrollLeft;
  }

  function handleChange(event: ChangeEvent<HTMLTextAreaElement>) {
    pane.onChange(event.target.value);
  }

  const overlayNodes = useMemo(() => highlightPlaceholders(pane.value, validSet), [pane.value, validSet]);

  return (
    <div className={`prompt-fullscreen-editor prompt-fullscreen-editor--${pane.key}`}>
      <div
        ref={overlayRef}
        className="prompt-fullscreen-editor-overlay"
        aria-hidden="true"
      >
        {overlayNodes}
        {"\n"}
      </div>
      <textarea
        ref={textareaRef}
        className="prompt-fullscreen-editor-textarea"
        value={pane.value}
        placeholder={pane.placeholder}
        onChange={handleChange}
        onScroll={handleScroll}
        spellCheck={false}
      />
    </div>
  );
}

export function PromptFullscreenModal({
  eyebrow,
  title,
  description,
  initialFocus = "system",
  panes,
  validPlaceholders,
  onClose,
}: PromptFullscreenModalProps) {
  useEffect(() => {
    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        onClose();
      }
    }
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [onClose]);

  const [paneA, paneB] = panes;

  const validSet = useMemo(() => new Set(validPlaceholders), [validPlaceholders]);

  const uniquePlaceholders = useMemo(
    () => collectUniqueTokens(validSet, paneA.value, paneB.value),
    [validSet, paneA.value, paneB.value],
  );

  const totalPlaceholderMatches = useMemo(() => {
    const tally = (source: string) => {
      PLACEHOLDER_PATTERN.lastIndex = 0;
      let count = 0;
      let match: RegExpExecArray | null;
      while ((match = PLACEHOLDER_PATTERN.exec(source)) !== null) {
        const token = match[1] ?? "";
        if (validSet.has(token)) {
          count += 1;
        }
      }
      return count;
    };
    return tally(paneA.value) + tally(paneB.value);
  }, [validSet, paneA.value, paneB.value]);

  function handleBackdropMouseDown(event: MouseEvent<HTMLDivElement>) {
    if (event.button !== 0) {
      return;
    }
    if (event.target === event.currentTarget) {
      onClose();
    }
  }

  return (
    <div
      className="prompt-fullscreen-backdrop"
      onMouseDown={handleBackdropMouseDown}
      role="presentation"
    >
      <section
        className="prompt-fullscreen-shell prompt-fullscreen-shell--dual"
        role="dialog"
        aria-modal="true"
        aria-labelledby="prompt-fullscreen-title"
      >
        <header className="prompt-fullscreen-header">
          <div className="prompt-fullscreen-heading">
            <span className="prompt-fullscreen-eyebrow">{eyebrow}</span>
            <h2 id="prompt-fullscreen-title">{title}</h2>
            {description ? <p>{description}</p> : null}
          </div>
          <div className="prompt-fullscreen-header-actions">
            <span className="prompt-fullscreen-stat">
              <strong>{uniquePlaceholders.length}</strong>
              <span>unique placeholder{uniquePlaceholders.length === 1 ? "" : "s"}</span>
            </span>
            <span className="prompt-fullscreen-stat">
              <strong>{totalPlaceholderMatches}</strong>
              <span>total reference{totalPlaceholderMatches === 1 ? "" : "s"}</span>
            </span>
            <button type="button" className="secondary-button" onClick={onClose}>
              Done
            </button>
          </div>
        </header>

        {uniquePlaceholders.length > 0 ? (
          <div className="prompt-fullscreen-legend">
            {uniquePlaceholders.map((token) => {
              const palette = placeholderPalette(token);
              return (
                <span
                  key={token}
                  className="prompt-fullscreen-legend-chip"
                  style={{
                    borderColor: palette.outline,
                    color: palette.text,
                    background: palette.background,
                  }}
                >
                  <span className="prompt-fullscreen-legend-dot" style={{ background: palette.dot }} />
                  <code>{`{${token}}`}</code>
                </span>
              );
            })}
          </div>
        ) : (
          <div className="prompt-fullscreen-legend prompt-fullscreen-legend--empty">
            <span>No placeholders detected — add tokens like <code>{"{input_payload}"}</code> to interpolate runtime values.</span>
          </div>
        )}

        <div className="prompt-fullscreen-body prompt-fullscreen-body--dual">
          {panes.map((pane) => (
            <div
              key={pane.key}
              className={`prompt-fullscreen-pane prompt-fullscreen-pane--${pane.key}`}
            >
              <div className="prompt-fullscreen-pane-header">
                <strong>{pane.title}</strong>
                {pane.description ? <span>{pane.description}</span> : null}
              </div>
              <HighlightedPromptEditor
                pane={pane}
                autoFocus={pane.key === initialFocus}
                validSet={validSet}
              />
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}
