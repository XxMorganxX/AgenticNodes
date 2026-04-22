import type { GraphNode, ProviderConfigOptionDefinition } from "./types";

export type ClaudeCodePresetDefinition = {
  id: string;
  label: string;
  description: string;
  providerConfig: Record<string, string | number | boolean>;
  appliesTo: string[];
};

export const CLAUDE_CODE_PRESETS: ClaudeCodePresetDefinition[] = [
  {
    id: "custom",
    label: "Custom",
    description: "Keep the Claude Code provider fully manual and leave existing prompts untouched.",
    providerConfig: {
      max_turns: 4,
    },
    appliesTo: ["No automatic prompt changes"],
  },
  {
    id: "general_assistant",
    label: "General Assistant",
    description: "Balanced Claude Code provider settings for everyday writing and task execution without editing prompts.",
    providerConfig: {
      model: "sonnet",
      timeout_seconds: 60,
      max_turns: 4,
    },
    appliesTo: ["Model", "Timeout", "Max turns"],
  },
  {
    id: "deep_reasoning",
    label: "Deep Reasoning",
    description: "Use heavier Claude Code provider settings for planning, analysis, and trickier decisions without editing prompts.",
    providerConfig: {
      model: "opus",
      timeout_seconds: 90,
      max_turns: 4,
    },
    appliesTo: ["Model", "Timeout", "Max turns"],
  },
  {
    id: "email_repeated_style",
    label: "Repeated Email Drafting",
    description: "Use Claude Code provider defaults that fit repeated email drafting while preserving your existing prompt templates.",
    providerConfig: {
      model: "sonnet",
      timeout_seconds: 60,
      max_turns: 4,
    },
    appliesTo: ["Model", "Timeout", "Max turns"],
  },
];

export const CLAUDE_CODE_PRESET_OPTIONS: ProviderConfigOptionDefinition[] = CLAUDE_CODE_PRESETS.map((preset) => ({
  value: preset.id,
  label: preset.label,
}));

export function getClaudeCodePreset(presetId: string): ClaudeCodePresetDefinition | null {
  return CLAUDE_CODE_PRESETS.find((preset) => preset.id === presetId) ?? null;
}

export function applyClaudeCodePresetToNode(node: GraphNode, presetId: string): GraphNode {
  const preset = getClaudeCodePreset(presetId);
  const nextConfig: GraphNode["config"] = {
    ...node.config,
    preset: presetId,
  };
  if (!preset || preset.id === "custom") {
    return {
      ...node,
      config: nextConfig,
    };
  }
  Object.assign(nextConfig, preset.providerConfig);
  return {
    ...node,
    config: nextConfig,
  };
}
