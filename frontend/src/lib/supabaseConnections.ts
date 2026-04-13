import type { GraphDefinition, GraphDocument, GraphNode, SupabaseConnectionDefinition, TestEnvironmentDefinition } from "./types";

export const LEGACY_SUPABASE_URL_ENV_VAR = "GRAPH_AGENT_SUPABASE_URL";
export const LEGACY_SUPABASE_KEY_ENV_VAR = "GRAPH_AGENT_SUPABASE_SECRET_KEY";
export const LEGACY_SUPABASE_PROJECT_REF_ENV_VAR = "SUPABASE_PROJECT_REF";
export const LEGACY_SUPABASE_ACCESS_TOKEN_ENV_VAR = "SUPABASE_ACCESS_TOKEN";
export const IMPLICIT_LEGACY_SUPABASE_CONNECTION_ID = "__legacy_default_supabase__";

export type DerivedSupabaseConnection = SupabaseConnectionDefinition & {
  isImplicit?: boolean;
};

export type SupabaseConnectionSelectOption = {
  value: string;
  label: string;
  missing?: boolean;
};

export type ResolvedSupabaseBinding = {
  connectionId: string;
  connectionName: string;
  isNamedConnection: boolean;
  isImplicitConnection: boolean;
  missingConnection: boolean;
  supabaseUrlEnvVar: string;
  supabaseKeyEnvVar: string;
  projectRefEnvVar: string;
  accessTokenEnvVar: string;
};

function isTestEnvironmentDocument(graph: GraphDocument | null | undefined): graph is TestEnvironmentDefinition {
  return Boolean(graph && "agents" in graph && Array.isArray(graph.agents));
}

function allDocumentNodes(graph: GraphDocument | null | undefined): GraphNode[] {
  if (!graph) {
    return [];
  }
  if (isTestEnvironmentDocument(graph)) {
    return graph.agents.flatMap((agent) => agent.nodes);
  }
  return graph.nodes;
}

function slugify(value: string, fallback: string): string {
  const normalized = value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
  return normalized || fallback;
}

function sanitizeConnection(connection: SupabaseConnectionDefinition): SupabaseConnectionDefinition | null {
  const connectionId = String(connection.connection_id ?? "").trim();
  const name = String(connection.name ?? "").trim();
  if (!connectionId || !name) {
    return null;
  }
  return {
    connection_id: connectionId,
    name,
    supabase_url_env_var: String(connection.supabase_url_env_var ?? LEGACY_SUPABASE_URL_ENV_VAR).trim() || LEGACY_SUPABASE_URL_ENV_VAR,
    supabase_key_env_var: String(connection.supabase_key_env_var ?? LEGACY_SUPABASE_KEY_ENV_VAR).trim() || LEGACY_SUPABASE_KEY_ENV_VAR,
    project_ref_env_var: String(connection.project_ref_env_var ?? LEGACY_SUPABASE_PROJECT_REF_ENV_VAR).trim() || LEGACY_SUPABASE_PROJECT_REF_ENV_VAR,
    access_token_env_var: String(connection.access_token_env_var ?? LEGACY_SUPABASE_ACCESS_TOKEN_ENV_VAR).trim() || LEGACY_SUPABASE_ACCESS_TOKEN_ENV_VAR,
  };
}

export function getExplicitSupabaseConnections(graph: GraphDocument | GraphDefinition | null | undefined): SupabaseConnectionDefinition[] {
  const rawConnections = Array.isArray(graph?.supabase_connections) ? graph.supabase_connections : [];
  const seenIds = new Set<string>();
  return rawConnections.flatMap((connection) => {
    const sanitized = sanitizeConnection(connection);
    if (!sanitized || seenIds.has(sanitized.connection_id)) {
      return [];
    }
    seenIds.add(sanitized.connection_id);
    return [sanitized];
  });
}

function nodeUsesLegacyDefaultSupabaseEnv(node: GraphNode): boolean {
  const connectionId = String(node.config.supabase_connection_id ?? "").trim();
  if (connectionId) {
    return false;
  }
  const urlEnvVar = String(node.config.supabase_url_env_var ?? LEGACY_SUPABASE_URL_ENV_VAR).trim() || LEGACY_SUPABASE_URL_ENV_VAR;
  const keyEnvVar = String(node.config.supabase_key_env_var ?? LEGACY_SUPABASE_KEY_ENV_VAR).trim() || LEGACY_SUPABASE_KEY_ENV_VAR;
  return urlEnvVar === LEGACY_SUPABASE_URL_ENV_VAR && keyEnvVar === LEGACY_SUPABASE_KEY_ENV_VAR;
}

function shouldIncludeImplicitLegacyConnection(graph: GraphDocument | GraphDefinition | null | undefined, explicitConnections: SupabaseConnectionDefinition[]): boolean {
  const hasExplicitLegacyConnection = explicitConnections.some(
    (connection) =>
      connection.supabase_url_env_var === LEGACY_SUPABASE_URL_ENV_VAR && connection.supabase_key_env_var === LEGACY_SUPABASE_KEY_ENV_VAR,
  );
  if (hasExplicitLegacyConnection) {
    return false;
  }
  const envVars = graph?.env_vars ?? {};
  const hasLegacyValues = Boolean(
    String(envVars[LEGACY_SUPABASE_URL_ENV_VAR] ?? "").trim() || String(envVars[LEGACY_SUPABASE_KEY_ENV_VAR] ?? "").trim(),
  );
  if (hasLegacyValues) {
    return true;
  }
  return allDocumentNodes(graph).some(nodeUsesLegacyDefaultSupabaseEnv);
}

export function getSupabaseConnections(graph: GraphDocument | GraphDefinition | null | undefined): DerivedSupabaseConnection[] {
  const explicitConnections = getExplicitSupabaseConnections(graph);
  if (!shouldIncludeImplicitLegacyConnection(graph, explicitConnections)) {
    return explicitConnections;
  }
  return [
    ...explicitConnections,
    {
      connection_id: IMPLICIT_LEGACY_SUPABASE_CONNECTION_ID,
      name: "Default Supabase",
      supabase_url_env_var: LEGACY_SUPABASE_URL_ENV_VAR,
      supabase_key_env_var: LEGACY_SUPABASE_KEY_ENV_VAR,
      project_ref_env_var: LEGACY_SUPABASE_PROJECT_REF_ENV_VAR,
      access_token_env_var: LEGACY_SUPABASE_ACCESS_TOKEN_ENV_VAR,
      isImplicit: true,
    },
  ];
}

export function getSupabaseConnectionById(
  graph: GraphDocument | GraphDefinition | null | undefined,
  connectionId: string,
): DerivedSupabaseConnection | null {
  const normalizedConnectionId = String(connectionId ?? "").trim();
  if (!normalizedConnectionId) {
    return null;
  }
  return getSupabaseConnections(graph).find((connection) => connection.connection_id === normalizedConnectionId) ?? null;
}

export function getSupabaseConnectionSelectOptions(
  graph: GraphDocument | GraphDefinition | null | undefined,
  config: Record<string, unknown>,
): SupabaseConnectionSelectOption[] {
  const options: SupabaseConnectionSelectOption[] = getSupabaseConnections(graph).map((connection) => ({
    value: connection.connection_id,
    label: `${connection.name}${connection.isImplicit ? " (legacy)" : ""}`,
  }));
  const resolvedBinding = resolveSupabaseBinding(graph, config);
  if (
    resolvedBinding.missingConnection &&
    resolvedBinding.connectionId &&
    !options.some((option) => option.value === resolvedBinding.connectionId)
  ) {
    options.push({
      value: resolvedBinding.connectionId,
      label: `Missing: ${resolvedBinding.connectionName}`,
      missing: true,
    });
  }
  return options;
}

export function resolveSupabaseBinding(
  graph: GraphDocument | GraphDefinition | null | undefined,
  config: Record<string, unknown>,
): ResolvedSupabaseBinding {
  const connectionId = String(config.supabase_connection_id ?? "").trim();
  if (connectionId) {
    const connection = getSupabaseConnectionById(graph, connectionId);
    if (!connection) {
      return {
        connectionId,
        connectionName: connectionId,
        isNamedConnection: true,
        isImplicitConnection: false,
        missingConnection: true,
        supabaseUrlEnvVar: "",
        supabaseKeyEnvVar: "",
        projectRefEnvVar: "",
        accessTokenEnvVar: "",
      };
    }
    return {
      connectionId: connection.connection_id,
      connectionName: connection.name,
      isNamedConnection: true,
      isImplicitConnection: Boolean(connection.isImplicit),
      missingConnection: false,
      supabaseUrlEnvVar: connection.supabase_url_env_var,
      supabaseKeyEnvVar: connection.supabase_key_env_var,
      projectRefEnvVar: connection.project_ref_env_var,
      accessTokenEnvVar: connection.access_token_env_var,
    };
  }
  return {
    connectionId: "",
    connectionName: "Compatibility mode",
    isNamedConnection: false,
    isImplicitConnection: false,
    missingConnection: false,
    supabaseUrlEnvVar: String(config.supabase_url_env_var ?? LEGACY_SUPABASE_URL_ENV_VAR).trim() || LEGACY_SUPABASE_URL_ENV_VAR,
    supabaseKeyEnvVar: String(config.supabase_key_env_var ?? LEGACY_SUPABASE_KEY_ENV_VAR).trim() || LEGACY_SUPABASE_KEY_ENV_VAR,
    projectRefEnvVar: LEGACY_SUPABASE_PROJECT_REF_ENV_VAR,
    accessTokenEnvVar: LEGACY_SUPABASE_ACCESS_TOKEN_ENV_VAR,
  };
}

export function collectReferencedSupabaseConnectionIds(graph: GraphDocument | GraphDefinition | null | undefined): Set<string> {
  const referencedIds = new Set<string>();
  for (const node of allDocumentNodes(graph)) {
    const connectionId = String(node.config.supabase_connection_id ?? "").trim();
    if (connectionId) {
      referencedIds.add(connectionId);
    }
  }
  return referencedIds;
}

export function managedSupabaseEnvKeys(graph: GraphDocument | GraphDefinition | null | undefined): Set<string> {
  const keys = new Set<string>([
    LEGACY_SUPABASE_URL_ENV_VAR,
    LEGACY_SUPABASE_KEY_ENV_VAR,
    LEGACY_SUPABASE_PROJECT_REF_ENV_VAR,
    LEGACY_SUPABASE_ACCESS_TOKEN_ENV_VAR,
  ]);
  for (const connection of getExplicitSupabaseConnections(graph)) {
    keys.add(connection.supabase_url_env_var);
    keys.add(connection.supabase_key_env_var);
    keys.add(connection.project_ref_env_var);
    keys.add(connection.access_token_env_var);
  }
  return keys;
}

export function createSupabaseConnectionIdentity(
  existingConnections: readonly SupabaseConnectionDefinition[],
  name: string,
): SupabaseConnectionDefinition {
  const existingIds = new Set(existingConnections.map((connection) => connection.connection_id));
  const existingEnvKeys = new Set(
    existingConnections.flatMap((connection) => [
      connection.supabase_url_env_var,
      connection.supabase_key_env_var,
      connection.project_ref_env_var,
      connection.access_token_env_var,
    ]),
  );
  const baseSlug = slugify(name, "connection");
  let suffix = 0;
  while (true) {
    const suffixText = suffix > 0 ? `_${suffix + 1}` : "";
    const connectionId = `supabase-${baseSlug.replace(/_/g, "-")}${suffix > 0 ? `-${suffix + 1}` : ""}`;
    const upperSlug = `${baseSlug}${suffixText}`.toUpperCase();
    const candidate: SupabaseConnectionDefinition = {
      connection_id: connectionId,
      name: name.trim() || "Supabase Connection",
      supabase_url_env_var: `GRAPH_AGENT_SUPABASE_${upperSlug}_URL`,
      supabase_key_env_var: `GRAPH_AGENT_SUPABASE_${upperSlug}_SECRET_KEY`,
      project_ref_env_var: `SUPABASE_${upperSlug}_PROJECT_REF`,
      access_token_env_var: `SUPABASE_${upperSlug}_ACCESS_TOKEN`,
    };
    if (existingIds.has(candidate.connection_id)) {
      suffix += 1;
      continue;
    }
    const hasEnvCollision = [
      candidate.supabase_url_env_var,
      candidate.supabase_key_env_var,
      candidate.project_ref_env_var,
      candidate.access_token_env_var,
    ].some((envKey) => existingEnvKeys.has(envKey));
    if (!hasEnvCollision) {
      return candidate;
    }
    suffix += 1;
  }
}
