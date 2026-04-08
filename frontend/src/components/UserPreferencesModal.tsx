import { useEffect } from "react";
import type { ChangeEvent, KeyboardEvent as ReactKeyboardEvent, MouseEvent } from "react";

import { DEFAULT_USER_PREFERENCES } from "../lib/userPreferences";
import type { UserPreferences } from "../lib/userPreferences";

type UserPreferencesModalProps = {
  preferences: UserPreferences;
  onUpdatePreferences: (preferences: UserPreferences) => void;
  onResetPreferences: () => void;
  onClose: () => void;
};

export function UserPreferencesModal({
  preferences,
  onUpdatePreferences,
  onResetPreferences,
  onClose,
}: UserPreferencesModalProps) {
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

  function handleBackgroundDragSensitivityChange(event: ChangeEvent<HTMLInputElement>) {
    onUpdatePreferences({
      ...preferences,
      backgroundDragSensitivityPercent: Number(event.target.value),
    });
  }

  function formatShortcutKey(event: KeyboardEvent | ReactKeyboardEvent<HTMLInputElement>): string {
    const parts: string[] = [];
    if (event.metaKey || event.ctrlKey) {
      parts.push("Mod");
    }
    if (event.altKey) {
      parts.push("Alt");
    }
    if (event.shiftKey) {
      parts.push("Shift");
    }
    const rawKey = event.key.length === 1 ? event.key.toUpperCase() : event.key;
    const normalizedKey =
      rawKey === " " ? "Space" : rawKey === "Escape" || rawKey === "Tab" || rawKey.startsWith("Arrow") ? rawKey : rawKey;
    if (["Meta", "Control", "Alt", "Shift"].includes(normalizedKey)) {
      return parts.join("+");
    }
    parts.push(normalizedKey);
    return parts.join("+");
  }

  function handleSaveGraphShortcutKeyDown(event: ReactKeyboardEvent<HTMLInputElement>) {
    event.preventDefault();
    event.stopPropagation();
    const nextAccelerator = formatShortcutKey(event.nativeEvent);
    onUpdatePreferences({
      ...preferences,
      keyboardShortcuts: {
        ...preferences.keyboardShortcuts,
        saveGraph: {
          accelerator: nextAccelerator,
        },
      },
    });
  }

  function handleSaveGraphShortcutChange(event: ChangeEvent<HTMLInputElement>) {
    onUpdatePreferences({
      ...preferences,
      keyboardShortcuts: {
        ...preferences.keyboardShortcuts,
        saveGraph: {
          accelerator: event.target.value,
        },
      },
    });
  }

  function handleRunGraphShortcutKeyDown(event: ReactKeyboardEvent<HTMLInputElement>) {
    event.preventDefault();
    event.stopPropagation();
    const nextAccelerator = formatShortcutKey(event.nativeEvent);
    onUpdatePreferences({
      ...preferences,
      keyboardShortcuts: {
        ...preferences.keyboardShortcuts,
        runGraph: {
          accelerator: nextAccelerator,
        },
      },
    });
  }

  function handleRunGraphShortcutChange(event: ChangeEvent<HTMLInputElement>) {
    onUpdatePreferences({
      ...preferences,
      keyboardShortcuts: {
        ...preferences.keyboardShortcuts,
        runGraph: {
          accelerator: event.target.value,
        },
      },
    });
  }

  return (
    <div className="tool-details-modal-backdrop" onClick={handleOverlayClick} role="presentation">
      <section
        className="tool-details-modal user-preferences-modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="user-preferences-modal-title"
      >
        <div className="tool-details-modal-header">
          <div>
            <div className="tool-details-modal-eyebrow">Preferences</div>
            <h3 id="user-preferences-modal-title">User Preferences</h3>
            <p>Store personal editor behavior locally for this browser. More preferences can be added here over time.</p>
          </div>
          <button type="button" className="secondary-button" onClick={onClose}>
            Close
          </button>
        </div>

        <div className="tool-details-modal-body user-preferences-modal-body">
          <label>
            Background Drag Sensitivity
            <div className="preferences-slider-row">
              <input
                type="range"
                min="10"
                max="70"
                step="1"
                value={preferences.backgroundDragSensitivityPercent}
                onChange={handleBackgroundDragSensitivityChange}
              />
              <span className="preferences-slider-value">{preferences.backgroundDragSensitivityPercent}%</span>
            </div>
          </label>

          <div className="tool-details-modal-help">
            Higher values make right-drag background panning follow your mouse more aggressively. The default is{" "}
            <code>{DEFAULT_USER_PREFERENCES.backgroundDragSensitivityPercent}%</code>.
          </div>

          <div className="preferences-shortcut-card">
            <div className="preferences-shortcut-copy">
              <strong>Save Graph</strong>
              <span>
                Press a shortcut to save the current graph and override the browser save action. Leave the field empty to disable it.
              </span>
            </div>
            <input
              type="text"
              className="preferences-shortcut-input"
              value={preferences.keyboardShortcuts.saveGraph.accelerator}
              placeholder={DEFAULT_USER_PREFERENCES.keyboardShortcuts.saveGraph.accelerator}
              onChange={handleSaveGraphShortcutChange}
              onKeyDown={handleSaveGraphShortcutKeyDown}
              spellCheck={false}
              aria-label="Save graph shortcut"
            />
          </div>

          <div className="preferences-shortcut-card">
            <div className="preferences-shortcut-copy">
              <strong>Run Agent</strong>
              <span>
                Press a shortcut to run the current graph or environment and override the browser refresh action. Leave the field empty to disable it.
              </span>
            </div>
            <input
              type="text"
              className="preferences-shortcut-input"
              value={preferences.keyboardShortcuts.runGraph.accelerator}
              placeholder={DEFAULT_USER_PREFERENCES.keyboardShortcuts.runGraph.accelerator}
              onChange={handleRunGraphShortcutChange}
              onKeyDown={handleRunGraphShortcutKeyDown}
              spellCheck={false}
              aria-label="Run graph shortcut"
            />
          </div>

          <div className="preferences-modal-actions">
            <button type="button" className="secondary-button" onClick={onResetPreferences}>
              Reset to Default
            </button>
          </div>
        </div>
      </section>
    </div>
  );
}
