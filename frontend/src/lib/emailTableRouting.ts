import type { AgentDefinition, GraphDocument, GraphNode } from "./types";

export type EmailRoutingMode = "development" | "production";

export const DEFAULT_NEW_GRAPH_EMAIL_ROUTING_MODE: EmailRoutingMode = "development";
export const DEFAULT_LEGACY_GRAPH_EMAIL_ROUTING_MODE: EmailRoutingMode = "production";

const EMAIL_TABLE_MIRRORS = [
  {
    production: "outbound_email_messages",
    development: "outbound_email_messages_dev",
  },
  {
    production: "inbound_email_messages",
    development: "inbound_email_messages_dev",
  },
] as const;

function isTestEnvironmentDocument(graph: GraphDocument): graph is GraphDocument & { agents: AgentDefinition[] } {
  return "agents" in graph && Array.isArray(graph.agents);
}

function splitSchemaPrefix(value: string): { prefix: string; tableName: string } {
  const trimmed = value.trim();
  const lastDot = trimmed.lastIndexOf(".");
  if (lastDot < 0) {
    return { prefix: "", tableName: trimmed };
  }
  return {
    prefix: trimmed.slice(0, lastDot + 1),
    tableName: trimmed.slice(lastDot + 1),
  };
}

function retargetEmailTableName(value: string, mode: EmailRoutingMode): string {
  const trimmed = value.trim();
  if (!trimmed) {
    return value;
  }
  const { prefix, tableName } = splitSchemaPrefix(trimmed);
  for (const mirror of EMAIL_TABLE_MIRRORS) {
    if (tableName === mirror.production || tableName === mirror.development) {
      return `${prefix}${mode === "production" ? mirror.production : mirror.development}`;
    }
  }
  return value;
}

function retargetNode(node: GraphNode, mode: EmailRoutingMode): GraphNode {
  const nextConfig = { ...node.config };

  if (
    (node.provider_id === "core.outbound_email_logger" || node.provider_id === "core.supabase_row_write")
    && typeof nextConfig.table_name === "string"
  ) {
    nextConfig.table_name = retargetEmailTableName(nextConfig.table_name, mode);
  }

  if (node.provider_id === "core.supabase_data" && typeof nextConfig.source_name === "string") {
    const sourceKind = String(nextConfig.source_kind ?? "table").trim().toLowerCase() || "table";
    if (sourceKind !== "rpc") {
      nextConfig.source_name = retargetEmailTableName(nextConfig.source_name, mode);
    }
  }

  return {
    ...node,
    config: nextConfig,
  };
}

function collectNodes(graph: GraphDocument): GraphNode[] {
  if (isTestEnvironmentDocument(graph)) {
    return graph.agents.flatMap((agent) => agent.nodes);
  }
  return graph.nodes;
}

function countMatchedTables(graph: GraphDocument): { development: number; production: number } {
  const counts = { development: 0, production: 0 };
  for (const node of collectNodes(graph)) {
    const candidateValues: string[] = [];
    if (
      (node.provider_id === "core.outbound_email_logger" || node.provider_id === "core.supabase_row_write")
      && typeof node.config.table_name === "string"
    ) {
      candidateValues.push(node.config.table_name);
    }
    if (
      node.provider_id === "core.supabase_data"
      && typeof node.config.source_name === "string"
      && (String(node.config.source_kind ?? "table").trim().toLowerCase() || "table") !== "rpc"
    ) {
      candidateValues.push(node.config.source_name);
    }
    for (const candidateValue of candidateValues) {
      const { tableName } = splitSchemaPrefix(candidateValue);
      for (const mirror of EMAIL_TABLE_MIRRORS) {
        if (tableName === mirror.development) {
          counts.development += 1;
        } else if (tableName === mirror.production) {
          counts.production += 1;
        }
      }
    }
  }
  return counts;
}

export function normalizeEmailRoutingMode(value: unknown): EmailRoutingMode | null {
  const normalized = String(value ?? "").trim().toLowerCase();
  if (normalized === "development" || normalized === "production") {
    return normalized;
  }
  return null;
}

export function resolveEmailRoutingMode(graph: GraphDocument | null | undefined): EmailRoutingMode {
  const explicit = normalizeEmailRoutingMode(graph?.email_routing_mode);
  if (explicit) {
    return explicit;
  }
  if (!graph) {
    return DEFAULT_LEGACY_GRAPH_EMAIL_ROUTING_MODE;
  }
  const counts = countMatchedTables(graph);
  if (counts.development > 0 && counts.production === 0) {
    return "development";
  }
  return DEFAULT_LEGACY_GRAPH_EMAIL_ROUTING_MODE;
}

export function emailRoutingModeLabel(mode: EmailRoutingMode): string {
  return mode === "production" ? "Production tables" : "Development tables";
}

export const EMAIL_TABLE_SUFFIX_ENV_VAR_KEY = "EMAIL_TABLE_SUFFIX";

export function emailTableSuffixForMode(mode: EmailRoutingMode): string {
  return mode === "production" ? "" : "_dev";
}

function withEmailTableSuffixEnvVar(
  envVars: Record<string, string> | undefined,
  mode: EmailRoutingMode,
): Record<string, string> {
  return {
    ...(envVars ?? {}),
    [EMAIL_TABLE_SUFFIX_ENV_VAR_KEY]: emailTableSuffixForMode(mode),
  };
}

export function syncEmailTableSuffixEnvVar(graph: GraphDocument): GraphDocument {
  const mode = resolveEmailRoutingMode(graph);
  const desired = emailTableSuffixForMode(mode);
  const current = graph.env_vars?.[EMAIL_TABLE_SUFFIX_ENV_VAR_KEY];
  if (current === desired) {
    return graph;
  }
  return {
    ...graph,
    env_vars: withEmailTableSuffixEnvVar(graph.env_vars, mode),
  };
}

export function applyEmailRoutingMode(graph: GraphDocument, mode: EmailRoutingMode): GraphDocument {
  if (isTestEnvironmentDocument(graph)) {
    return {
      ...graph,
      email_routing_mode: mode,
      env_vars: withEmailTableSuffixEnvVar(graph.env_vars, mode),
      agents: graph.agents.map((agent) => ({
        ...agent,
        nodes: agent.nodes.map((node) => retargetNode(node, mode)),
      })),
    };
  }
  return {
    ...graph,
    email_routing_mode: mode,
    env_vars: withEmailTableSuffixEnvVar(graph.env_vars, mode),
    nodes: graph.nodes.map((node) => retargetNode(node, mode)),
  };
}
