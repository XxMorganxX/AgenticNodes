import { useEffect } from "react";

/**
 * Hook that fires `onChange` whenever the document's visibility flips.
 * Used to pause expensive render work while a tab is backgrounded and
 * to drain buffered work in a single batch on refocus.
 */
export function usePageVisibility(onChange: (hidden: boolean) => void): void {
  useEffect(() => {
    if (typeof document === "undefined") {
      return;
    }
    const handler = () => onChange(document.hidden);
    document.addEventListener("visibilitychange", handler);
    return () => {
      document.removeEventListener("visibilitychange", handler);
    };
  }, [onChange]);
}
