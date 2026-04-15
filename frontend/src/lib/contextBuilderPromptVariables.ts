import { getContextBuilderBindings } from "./contextBuilderBindings";
import { getNodeInstanceLabel } from "./nodeInstanceLabels";
import type { GraphDefinition, GraphNode } from "./types";

const CONTEXT_BUILDER_PROVIDER_ID = "core.context_builder";

export type ContextBuilderPromptVariable = {
  token: string;
  header: string;
  sourceNodeId: string;
  sourceLabel: string;
  contextBuilderNodeId: string;
  contextBuilderLabel: string;
};

export function getModelContextBuilderPromptVariables(
  graph: GraphDefinition,
  modelNode: GraphNode,
): ContextBuilderPromptVariable[] {
  if (modelNode.kind !== "model") {
    return [];
  }

  const variables: ContextBuilderPromptVariable[] = [];
  for (const edge of graph.edges) {
    if (edge.target_id !== modelNode.id) {
      continue;
    }
    const contextBuilderNode = graph.nodes.find((node) => node.id === edge.source_id);
    if (!contextBuilderNode || contextBuilderNode.provider_id !== CONTEXT_BUILDER_PROVIDER_ID) {
      continue;
    }

    const contextBuilderLabel = getNodeInstanceLabel(graph, contextBuilderNode);
    for (const binding of getContextBuilderBindings(contextBuilderNode, graph)) {
      variables.push({
        token: binding.placeholder,
        header: binding.header,
        sourceNodeId: binding.sourceNodeId,
        sourceLabel: binding.sourceLabel,
        contextBuilderNodeId: contextBuilderNode.id,
        contextBuilderLabel,
      });
    }
  }

  const seen = new Set<string>();
  return variables.filter((variable) => {
    if (seen.has(variable.token)) {
      return false;
    }
    seen.add(variable.token);
    return true;
  });
}
