import { useEffect } from "react";
import type { ChangeEvent, MouseEvent } from "react";

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
