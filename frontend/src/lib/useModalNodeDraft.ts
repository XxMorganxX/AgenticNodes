import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import type { GraphDefinition, GraphNode } from "./types";

function replaceNodeInGraph(graph: GraphDefinition, nextNode: GraphNode): GraphDefinition {
  return {
    ...graph,
    nodes: graph.nodes.map((node) => (node.id === nextNode.id ? nextNode : node)),
  };
}

function nodeDraftSignature(node: GraphNode): string {
  return JSON.stringify(node);
}

type UseModalNodeDraftArgs = {
  graph: GraphDefinition;
  node: GraphNode;
  onGraphChange: (graph: GraphDefinition) => void;
  onBackgroundPersist?: ((graph: GraphDefinition) => void) | null;
  debounceMs?: number;
};

type UseModalNodeDraftResult = {
  draftNode: GraphNode;
  updateDraftNode: (updater: (node: GraphNode) => GraphNode) => void;
  flushCommit: () => void;
  hasDraftChanges: boolean;
  resetDraft: () => void;
};

/**
 * Keeps modal-local edits off the live graph/canvas render path.
 *
 * Contract:
 * - Typing only updates local `draftNode` state — it never touches draftGraph
 *   or the canvas.
 * - Background autosave is debounced off `draftSignature`, so the hook fires
 *   autosave at most once per distinct draft value. It reads the current
 *   persist callback from a ref, which means that if the parent component
 *   re-renders (e.g. because an earlier autosave resolved and called
 *   `setGraphs`), we do NOT reschedule the timer or re-run any effect.
 * - The final commit into live graph state is done exactly once:
 *     - via `flushCommit()` when the modal asks to close, or
 *     - via an unmount cleanup that runs only on true unmount.
 *   The cleanup explicitly does not depend on the `onGraphChange` callback
 *   identity — if it did, an unstable parent callback (e.g. a fresh function
 *   on every App render) would fire the cleanup on every commit, which would
 *   call `onGraphChange` on every render and blow up with
 *   "Maximum update depth exceeded", unmounting the whole app.
 */
export function useModalNodeDraft({
  graph,
  node,
  onGraphChange,
  onBackgroundPersist = null,
  debounceMs = 300,
}: UseModalNodeDraftArgs): UseModalNodeDraftResult {
  const [draftNode, setDraftNode] = useState(node);
  const activeNodeIdRef = useRef(node.id);
  const sessionBaselineSignatureRef = useRef(nodeDraftSignature(node));
  const latestPropNodeRef = useRef(node);
  const latestDraftNodeRef = useRef(node);
  const didFinalizeSessionRef = useRef(false);
  const latestGraphRef = useRef(graph);
  const onGraphChangeRef = useRef(onGraphChange);
  const onBackgroundPersistRef = useRef(onBackgroundPersist);

  latestPropNodeRef.current = node;
  latestDraftNodeRef.current = draftNode;
  latestGraphRef.current = graph;
  onGraphChangeRef.current = onGraphChange;
  onBackgroundPersistRef.current = onBackgroundPersist;

  const draftSignature = useMemo(() => nodeDraftSignature(draftNode), [draftNode]);
  const propNodeSignature = useMemo(() => nodeDraftSignature(node), [node]);
  const hasSessionChanges = draftSignature !== sessionBaselineSignatureRef.current;
  const needsQuietFlush = draftSignature !== propNodeSignature;

  const flushCommit = useCallback(() => {
    didFinalizeSessionRef.current = true;
    if (sessionBaselineSignatureRef.current === nodeDraftSignature(latestDraftNodeRef.current)) {
      return;
    }
    onGraphChangeRef.current(replaceNodeInGraph(latestGraphRef.current, latestDraftNodeRef.current));
  }, []);

  const resetDraft = useCallback(() => {
    const nextNode = latestPropNodeRef.current;
    setDraftNode(nextNode);
    sessionBaselineSignatureRef.current = nodeDraftSignature(nextNode);
    didFinalizeSessionRef.current = false;
  }, []);

  const updateDraftNode = useCallback((updater: (node: GraphNode) => GraphNode) => {
    setDraftNode((current) => updater(current));
    didFinalizeSessionRef.current = false;
  }, []);

  // Switching which node the modal is editing (different node.id). If we had
  // un-committed edits for the previous node we commit them into live graph
  // state here; this is the only place that touches the live graph during an
  // open session aside from an explicit close.
  useEffect(() => {
    if (activeNodeIdRef.current === node.id) {
      if (!hasSessionChanges) {
        setDraftNode(node);
      }
      return;
    }
    if (
      !didFinalizeSessionRef.current &&
      sessionBaselineSignatureRef.current !== nodeDraftSignature(latestDraftNodeRef.current)
    ) {
      onGraphChangeRef.current(replaceNodeInGraph(latestGraphRef.current, latestDraftNodeRef.current));
    }
    activeNodeIdRef.current = node.id;
    sessionBaselineSignatureRef.current = nodeDraftSignature(node);
    didFinalizeSessionRef.current = false;
    setDraftNode(node);
  }, [hasSessionChanges, node]);

  // Background autosave. Dependency is the draft signature (a string), so this
  // effect re-runs exactly when the user's local draft content changes, not
  // when the parent re-renders for unrelated reasons. The persist callback is
  // read from a ref so we never need the callback identity as a dependency.
  useEffect(() => {
    if (!needsQuietFlush) {
      return;
    }
    const timeoutId = window.setTimeout(() => {
      const persistFn = onBackgroundPersistRef.current;
      if (!persistFn) {
        return;
      }
      persistFn(replaceNodeInGraph(latestGraphRef.current, latestDraftNodeRef.current));
    }, debounceMs);
    return () => {
      window.clearTimeout(timeoutId);
    };
  }, [debounceMs, draftSignature, needsQuietFlush]);

  // True-unmount commit. Runs only when the modal is actually torn down.
  // Intentionally depends on nothing: we do not want this cleanup to fire on
  // every App render just because the parent hands us a fresh function
  // reference for `onGraphChange`.
  useEffect(() => {
    return () => {
      if (didFinalizeSessionRef.current) {
        return;
      }
      if (sessionBaselineSignatureRef.current === nodeDraftSignature(latestDraftNodeRef.current)) {
        return;
      }
      onGraphChangeRef.current(replaceNodeInGraph(latestGraphRef.current, latestDraftNodeRef.current));
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return {
    draftNode,
    updateDraftNode,
    flushCommit,
    hasDraftChanges: hasSessionChanges,
    resetDraft,
  };
}
