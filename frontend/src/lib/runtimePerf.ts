/**
 * Opt-in runtime performance diagnostics for active-run streaming.
 * Enable in dev: localStorage.setItem("graphAgentPerf", "1") then reload.
 */

declare global {
  interface Window {
    /** When true (with import.meta.env.DEV), logs perf timings for run flush / projection. */
    __GRAPH_AGENT_PERF__?: boolean;
  }
}

export function isRuntimePerfEnabled(): boolean {
  if (!import.meta.env.DEV || typeof performance === "undefined") {
    return false;
  }
  try {
    if (typeof window !== "undefined") {
      if (window.__GRAPH_AGENT_PERF__ === true) {
        return true;
      }
      if (window.localStorage?.getItem("graphAgentPerf") === "1") {
        return true;
      }
    }
  } catch {
    return false;
  }
  return false;
}

export function perfMeasure<T>(label: string, fn: () => T): T {
  if (!isRuntimePerfEnabled()) {
    return fn();
  }
  const t0 = performance.now();
  try {
    return fn();
  } finally {
    const ms = performance.now() - t0;
    // eslint-disable-next-line no-console -- dev-only diagnostics
    console.debug(`[graphAgentPerf] ${label}`, { ms: Number(ms.toFixed(2)) });
  }
}
