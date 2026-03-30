import { useEffect, useMemo, useRef } from "react";

const STORAGE_KEY = "graph-drag-diagnostics";
const SEARCH_PARAM_KEYS = ["graphDebug", "graphDiagnostics"] as const;
const ENABLED_VALUES = new Set(["1", "true", "on", "yes", "drag", "graph"]);

function isEnabledValue(value: string | null | undefined) {
  return Boolean(value && ENABLED_VALUES.has(value.trim().toLowerCase()));
}

function formatDiagnosticError(error: unknown) {
  if (error instanceof Error) {
    return {
      name: error.name,
      message: error.message,
      stack: error.stack,
    };
  }
  return {
    message: typeof error === "string" ? error : String(error),
  };
}

export function isGraphDiagnosticsEnabled() {
  if (!import.meta.env.DEV || typeof window === "undefined") {
    return false;
  }

  const params = new URLSearchParams(window.location.search);
  for (const key of SEARCH_PARAM_KEYS) {
    const value = params.get(key);
    if (isEnabledValue(value)) {
      return true;
    }
  }

  return isEnabledValue(window.localStorage.getItem(STORAGE_KEY));
}

export function useGraphDiagnosticsEnabled() {
  return useMemo(() => isGraphDiagnosticsEnabled(), []);
}

export function logGraphDiagnostic(scope: string, message: string, details?: Record<string, unknown>) {
  if (!isGraphDiagnosticsEnabled()) {
    return;
  }

  const prefix = `[graph-diagnostics:${scope}] ${message}`;
  if (details) {
    console.debug(prefix, details);
    return;
  }
  console.debug(prefix);
}

export function warnGraphDiagnostic(
  scope: string,
  message: string,
  error: unknown,
  details: Record<string, unknown> = {},
) {
  if (!isGraphDiagnosticsEnabled()) {
    return;
  }

  console.warn(`[graph-diagnostics:${scope}] ${message}`, {
    ...details,
    error: formatDiagnosticError(error),
  });
}

export function useRenderDiagnostics(
  scope: string,
  active: boolean,
  details: Record<string, unknown>,
  sampleEvery = 20,
) {
  const enabled = useGraphDiagnosticsEnabled();
  const renderCountRef = useRef(0);
  const lastLoggedRenderRef = useRef(0);

  renderCountRef.current += 1;

  useEffect(() => {
    if (!enabled || !active) {
      return;
    }

    const renderCount = renderCountRef.current;
    if (renderCount === 1 || renderCount - lastLoggedRenderRef.current >= sampleEvery) {
      lastLoggedRenderRef.current = renderCount;
      logGraphDiagnostic(scope, "render", {
        renderCount,
        ...details,
      });
    }
  }, [active, details, enabled, sampleEvery, scope]);
}
