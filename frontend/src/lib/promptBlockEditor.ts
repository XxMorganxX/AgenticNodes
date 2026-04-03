import type { GraphDefinition, GraphNode, RunState } from "./types";

const PROMPT_BLOCK_BASE_VARIABLES = ["current_node_id", "documents", "graph_id", "input_payload", "run_id"];
const PROMPT_BLOCK_TOKEN_PATTERN = /\{([A-Za-z_][A-Za-z0-9_]*)\}/g;

export const PROMPT_BLOCK_STARTERS: Record<string, string> = {
  system: "You are a helpful assistant. Follow the workspace rules and answer clearly.",
  user: "{input_payload}",
  assistant: "Previous draft: {input_payload}",
};

function uniqueStrings(values: string[]): string[] {
  return [...new Set(values.filter((value) => value.trim().length > 0))];
}

function stringifyPreviewValue(value: unknown): string {
  if (typeof value === "string") {
    return value;
  }
  if (value === null || value === undefined) {
    return "";
  }
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

function renderTemplate(template: string, variables: Record<string, string>): string {
  return template.replace(PROMPT_BLOCK_TOKEN_PATTERN, (_, token: string) => variables[token] ?? `{${token}}`);
}

export function listPromptBlockAvailableVariables(graph: GraphDefinition): string[] {
  return uniqueStrings([...Object.keys(graph.env_vars ?? {}), ...PROMPT_BLOCK_BASE_VARIABLES]);
}

export function promptBlockTemplateVariables(
  graph: GraphDefinition,
  node: GraphNode,
  runState: RunState | null,
): Record<string, string> {
  return {
    ...Object.fromEntries(Object.entries(graph.env_vars ?? {}).map(([key, value]) => [key, String(value)])),
    current_node_id: node.id,
    documents: runState?.documents != null ? stringifyPreviewValue(runState.documents) : "",
    graph_id: graph.graph_id,
    input_payload: runState?.input_payload != null ? stringifyPreviewValue(runState.input_payload) : "",
    run_id: runState?.run_id ?? "",
  };
}

export function renderPromptBlockPreview(node: GraphNode, graph: GraphDefinition, runState: RunState | null): string {
  const role = String(node.config.role ?? "user").trim() || "user";
  const name = String(node.config.name ?? "").trim();
  const content = String(node.config.content ?? "");
  const variables = promptBlockTemplateVariables(graph, node, runState);
  const renderedName = renderTemplate(name, variables).trim();
  const renderedContent = renderTemplate(content, variables).trim();
  const header = renderedName ? `${role} (${renderedName})` : role;
  return `${header}: ${renderedContent}`.trim();
}

export function insertTokenAtEnd(value: string, token: string): string {
  if (!value.trim()) {
    return token;
  }
  return `${value}${value.endsWith("\n") ? "" : "\n"}${token}`;
}
