import type { GraphNode, GraphNodeConfig } from "./types";

export type SavedNode = {
  id: string;
  name: string;
  description: string;
  kind: string;
  category: string;
  provider_id: string;
  provider_label: string;
  config: GraphNodeConfig;
  model_provider_name?: string;
  prompt_name?: string;
  tool_name?: string;
  saved_at: string;
};

const STORAGE_KEY = "agentic-nodes-saved-nodes";

export function getSavedNodes(): SavedNode[] {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    return raw ? (JSON.parse(raw) as SavedNode[]) : [];
  } catch {
    return [];
  }
}

export function saveNodeToLibrary(node: GraphNode, name?: string): SavedNode {
  const saved: SavedNode = {
    id: `saved-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    name: name?.trim() || node.label,
    description: node.description ?? "",
    kind: node.kind,
    category: node.category,
    provider_id: node.provider_id,
    provider_label: node.provider_label,
    config: { ...node.config },
    model_provider_name: node.model_provider_name,
    prompt_name: node.prompt_name,
    tool_name: node.tool_name,
    saved_at: new Date().toISOString(),
  };

  const existing = getSavedNodes();
  existing.push(saved);
  localStorage.setItem(STORAGE_KEY, JSON.stringify(existing));
  return saved;
}

export function deleteSavedNode(id: string): void {
  const existing = getSavedNodes();
  localStorage.setItem(STORAGE_KEY, JSON.stringify(existing.filter((node) => node.id !== id)));
}

export function renameSavedNode(id: string, name: string): void {
  const existing = getSavedNodes();
  const index = existing.findIndex((node) => node.id === id);
  if (index >= 0) {
    existing[index] = { ...existing[index], name: name.trim() || existing[index].name };
    localStorage.setItem(STORAGE_KEY, JSON.stringify(existing));
  }
}
