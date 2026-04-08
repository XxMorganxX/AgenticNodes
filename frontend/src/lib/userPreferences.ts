export type KeyboardShortcutPreference = {
  accelerator: string;
};

export type KeyboardShortcutsPreferences = {
  saveGraph: KeyboardShortcutPreference;
  runGraph: KeyboardShortcutPreference;
};

export type UserPreferences = {
  backgroundDragSensitivityPercent: number;
  keyboardShortcuts: KeyboardShortcutsPreferences;
};

export const DEFAULT_USER_PREFERENCES: UserPreferences = {
  backgroundDragSensitivityPercent: 28,
  keyboardShortcuts: {
    saveGraph: {
      accelerator: "Mod+S",
    },
    runGraph: {
      accelerator: "Mod+R",
    },
  },
};

const STORAGE_KEY = "agentic-nodes-user-preferences";
const MIN_BACKGROUND_DRAG_SENSITIVITY_PERCENT = 10;
const MAX_BACKGROUND_DRAG_SENSITIVITY_PERCENT = 70;

function clampBackgroundDragSensitivityPercent(value: unknown): number {
  const numericValue = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(numericValue)) {
    return DEFAULT_USER_PREFERENCES.backgroundDragSensitivityPercent;
  }
  return Math.min(
    MAX_BACKGROUND_DRAG_SENSITIVITY_PERCENT,
    Math.max(MIN_BACKGROUND_DRAG_SENSITIVITY_PERCENT, Math.round(numericValue)),
  );
}

function normalizeKeyboardShortcutPreference(
  value: unknown,
  fallback: KeyboardShortcutPreference,
): KeyboardShortcutPreference {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return fallback;
  }
  const record = value as Record<string, unknown>;
  const accelerator =
    typeof record.accelerator === "string" && record.accelerator.trim().length > 0
      ? record.accelerator.trim()
      : fallback.accelerator;
  if (record.enabled === false) {
    return { accelerator: "" };
  }
  return { accelerator };
}

function normalizeUserPreferences(value: unknown): UserPreferences {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return DEFAULT_USER_PREFERENCES;
  }
  const record = value as Record<string, unknown>;
  const keyboardShortcutsRecord =
    record.keyboardShortcuts && typeof record.keyboardShortcuts === "object" && !Array.isArray(record.keyboardShortcuts)
      ? (record.keyboardShortcuts as Record<string, unknown>)
      : {};
  return {
    backgroundDragSensitivityPercent: clampBackgroundDragSensitivityPercent(record.backgroundDragSensitivityPercent),
    keyboardShortcuts: {
      saveGraph: normalizeKeyboardShortcutPreference(
        keyboardShortcutsRecord.saveGraph,
        DEFAULT_USER_PREFERENCES.keyboardShortcuts.saveGraph,
      ),
      runGraph: normalizeKeyboardShortcutPreference(
        keyboardShortcutsRecord.runGraph,
        DEFAULT_USER_PREFERENCES.keyboardShortcuts.runGraph,
      ),
    },
  };
}

export function getUserPreferences(): UserPreferences {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) {
      return DEFAULT_USER_PREFERENCES;
    }
    return normalizeUserPreferences(JSON.parse(raw) as unknown);
  } catch {
    return DEFAULT_USER_PREFERENCES;
  }
}

export function saveUserPreferences(preferences: UserPreferences): UserPreferences {
  const normalized = normalizeUserPreferences(preferences);
  localStorage.setItem(STORAGE_KEY, JSON.stringify(normalized));
  return normalized;
}

export function resetUserPreferences(): UserPreferences {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(DEFAULT_USER_PREFERENCES));
  return DEFAULT_USER_PREFERENCES;
}
