import type { GraphDocument } from "./types";

export type StandardGraphEnvField = {
  key: string;
  label: string;
  placeholder: string;
  tooltipText?: string;
};

export const NON_PERSISTED_GRAPH_ENV_KEYS = new Set(["MICROSOFT_GRAPH_ACCESS_TOKEN"]);

export const DEFAULT_GRAPH_ENV_VARS: Record<string, string> = {
  OPENAI_API_KEY: "OPENAI_API_KEY",
  ANTHROPIC_API_KEY: "ANTHROPIC_API_KEY",
  DISCORD_BOT_TOKEN: "DISCORD_BOT_TOKEN",
  APOLLO_API_KEY: "APOLLO_API_KEY",
  LINKEDIN_DATA_DIR: "LINKEDIN_DATA_DIR",
  GRAPH_AGENT_SUPABASE_URL: "",
  GRAPH_AGENT_SUPABASE_SECRET_KEY: "",
  SUPABASE_PROJECT_REF: "",
  SUPABASE_ACCESS_TOKEN: "",
  EMAIL_TABLE_SUFFIX: "_dev",
};

export const STANDARD_GRAPH_ENV_FIELDS: readonly StandardGraphEnvField[] = [
  {
    key: "OPENAI_API_KEY",
    label: "OpenAI API Key Reference",
    placeholder: "OPENAI_API_KEY",
  },
  {
    key: "ANTHROPIC_API_KEY",
    label: "Anthropic API Key Reference",
    placeholder: "ANTHROPIC_API_KEY",
  },
  {
    key: "DISCORD_BOT_TOKEN",
    label: "Discord Bot Token Reference",
    placeholder: "DISCORD_BOT_TOKEN",
  },
  {
    key: "APOLLO_API_KEY",
    label: "Apollo API Key Reference",
    placeholder: "APOLLO_API_KEY",
    tooltipText: "Used automatically by Apollo Email Lookup nodes. Set this once in Environment instead of per node.",
  },
  {
    key: "LINKEDIN_DATA_DIR",
    label: "LinkedIn Data Directory Reference",
    placeholder: "LINKEDIN_DATA_DIR",
    tooltipText: "Used by LinkedIn Profile Fetch nodes. Set this as a machine-local environment variable so graphs stay portable.",
  },
  {
    key: "GRAPH_AGENT_SUPABASE_URL",
    label: "Supabase URL Reference",
    placeholder: "GRAPH_AGENT_SUPABASE_URL",
  },
  {
    key: "GRAPH_AGENT_SUPABASE_SECRET_KEY",
    label: "Supabase Secret Key Reference",
    placeholder: "GRAPH_AGENT_SUPABASE_SECRET_KEY",
  },
  {
    key: "SUPABASE_PROJECT_REF",
    label: "Supabase Project Ref Reference",
    placeholder: "SUPABASE_PROJECT_REF",
  },
  {
    key: "SUPABASE_ACCESS_TOKEN",
    label: "Supabase Access Token Reference",
    placeholder: "SUPABASE_ACCESS_TOKEN",
  },
];

const GRAPH_ENV_REFERENCE_PATTERN = /\{([A-Za-z_][A-Za-z0-9_]*)\}/g;

export function sanitizeGraphEnvVars(envVars: Record<string, string> | null | undefined): Record<string, string> {
  if (!envVars) {
    return {};
  }
  return Object.fromEntries(
    Object.entries(envVars).flatMap(([key, value]) => {
      const trimmedKey = key.trim();
      if (!trimmedKey || NON_PERSISTED_GRAPH_ENV_KEYS.has(trimmedKey)) {
        return [];
      }
      return [[trimmedKey, typeof value === "string" ? value : String(value ?? "")]];
    }),
  );
}

export function getGraphEnvVars(graph: GraphDocument | null | undefined): Record<string, string> {
  const nextEnvVars: Record<string, string> = { ...DEFAULT_GRAPH_ENV_VARS };
  const rawEnvVars = sanitizeGraphEnvVars(graph?.env_vars);
  if (!rawEnvVars) {
    return nextEnvVars;
  }

  for (const [key, value] of Object.entries(rawEnvVars)) {
    const trimmedKey = key.trim();
    if (!trimmedKey) {
      continue;
    }
    nextEnvVars[trimmedKey] = typeof value === "string" ? value : String(value ?? "");
  }

  return nextEnvVars;
}

export function resolveGraphEnvReferences(
  value: string,
  graph: GraphDocument | null | undefined,
  extraVariables: Record<string, string> = {},
): string {
  const variables = {
    ...getGraphEnvVars(graph),
    ...extraVariables,
  };

  return value.replace(GRAPH_ENV_REFERENCE_PATTERN, (match, key: string) => variables[key] ?? match);
}
