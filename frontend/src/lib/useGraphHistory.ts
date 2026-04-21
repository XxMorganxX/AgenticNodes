import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import type { GraphDocument } from "./types";

const MAX_HISTORY = 80;
const NO_PENDING_QUIET_UPDATE = Symbol("NO_PENDING_QUIET_UPDATE");

type GraphHistoryEntry = {
  graph: GraphDocument | null;
  stateId: number;
};

export type GraphHistory = {
  graph: GraphDocument | null;
  stateId: number;
  /** Update graph and push previous state onto the undo stack. */
  set: (next: GraphDocument | null) => number;
  /** Update graph without recording history (use for intermediate drags). */
  setQuiet: (next: GraphDocument | null) => number;
  undo: () => void;
  redo: () => void;
  canUndo: boolean;
  canRedo: boolean;
  reset: (next: GraphDocument | null) => number;
};

export function useGraphHistory(initial: GraphDocument | null = null): GraphHistory {
  const nextStateIdRef = useRef(1);
  const allocateStateId = useCallback(() => {
    const nextId = nextStateIdRef.current;
    nextStateIdRef.current += 1;
    return nextId;
  }, []);
  const initialEntryRef = useRef<GraphHistoryEntry>({
    graph: initial,
    stateId: initial == null ? 0 : nextStateIdRef.current++,
  });
  const [entry, setEntry] = useState<GraphHistoryEntry>(initialEntryRef.current);
  const currentEntryRef = useRef<GraphHistoryEntry>(initialEntryRef.current);
  const undoStack = useRef<GraphHistoryEntry[]>([]);
  const redoStack = useRef<GraphHistoryEntry[]>([]);
  const [canUndo, setCanUndo] = useState(false);
  const [canRedo, setCanRedo] = useState(false);
  const quietBaselineRef = useRef<GraphHistoryEntry | typeof NO_PENDING_QUIET_UPDATE>(NO_PENDING_QUIET_UPDATE);

  const syncFlags = useCallback(() => {
    setCanUndo(undoStack.current.length > 0);
    setCanRedo(redoStack.current.length > 0);
  }, []);

  const set = useCallback(
    (next: GraphDocument | null) => {
      const previousEntry = currentEntryRef.current;
      const baseline = quietBaselineRef.current === NO_PENDING_QUIET_UPDATE ? previousEntry : quietBaselineRef.current;
      quietBaselineRef.current = NO_PENDING_QUIET_UPDATE;
      if (previousEntry.graph === next) {
        syncFlags();
        return previousEntry.stateId;
      }
      const nextEntry = {
        graph: next,
        stateId: allocateStateId(),
      } satisfies GraphHistoryEntry;
      if (baseline.graph !== next) {
        undoStack.current.push(baseline);
        if (undoStack.current.length > MAX_HISTORY) {
          undoStack.current.splice(0, undoStack.current.length - MAX_HISTORY);
        }
        redoStack.current = [];
      }
      currentEntryRef.current = nextEntry;
      setEntry(nextEntry);
      syncFlags();
      return nextEntry.stateId;
    },
    [allocateStateId, syncFlags],
  );

  const undo = useCallback(() => {
    const prev = undoStack.current.pop();
    if (prev === undefined) return;
    quietBaselineRef.current = NO_PENDING_QUIET_UPDATE;
    redoStack.current.push(currentEntryRef.current);
    currentEntryRef.current = prev;
    setEntry(prev);
    syncFlags();
  }, [syncFlags]);

  const redo = useCallback(() => {
    const next = redoStack.current.pop();
    if (next === undefined) return;
    quietBaselineRef.current = NO_PENDING_QUIET_UPDATE;
    undoStack.current.push(currentEntryRef.current);
    currentEntryRef.current = next;
    setEntry(next);
    syncFlags();
  }, [syncFlags]);

  const setQuiet = useCallback(
    (next: GraphDocument | null) => {
      const previousEntry = currentEntryRef.current;
      if (previousEntry.graph === next) {
        return previousEntry.stateId;
      }
      if (quietBaselineRef.current === NO_PENDING_QUIET_UPDATE) {
        quietBaselineRef.current = previousEntry;
      }
      const nextEntry = {
        graph: next,
        stateId: allocateStateId(),
      } satisfies GraphHistoryEntry;
      currentEntryRef.current = nextEntry;
      setEntry(nextEntry);
      return nextEntry.stateId;
    },
    [allocateStateId],
  );

  const reset = useCallback(
    (next: GraphDocument | null) => {
      const nextEntry = {
        graph: next,
        stateId: next == null ? 0 : allocateStateId(),
      } satisfies GraphHistoryEntry;
      undoStack.current = [];
      redoStack.current = [];
      quietBaselineRef.current = NO_PENDING_QUIET_UPDATE;
      currentEntryRef.current = nextEntry;
      setEntry(nextEntry);
      syncFlags();
      return nextEntry.stateId;
    },
    [allocateStateId, syncFlags],
  );

  useEffect(() => {
    function handleKeyDown(e: KeyboardEvent) {
      const isMod = e.metaKey || e.ctrlKey;
      if (!isMod || e.key.toLowerCase() !== "z") return;

      const target = e.target as HTMLElement | null;
      if (target && (target.tagName === "INPUT" || target.tagName === "TEXTAREA" || target.isContentEditable)) {
        return;
      }

      e.preventDefault();
      if (e.shiftKey) {
        redo();
      } else {
        undo();
      }
    }

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [undo, redo]);

  return useMemo(
    () => ({ graph: entry.graph, stateId: entry.stateId, set, setQuiet, undo, redo, canUndo, canRedo, reset }),
    [entry.graph, entry.stateId, set, setQuiet, undo, redo, canUndo, canRedo, reset],
  );
}
