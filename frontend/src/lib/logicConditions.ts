import type { GraphNodeConfig } from "./types";

export const DEFAULT_LOGIC_BRANCH_HANDLE_ID = "control-flow-if";
export const DEFAULT_LOGIC_ELSE_HANDLE_ID = "control-flow-else";

export type LogicCombinator = "all" | "any";

export type LogicConditionRule = {
  id: string;
  type: "rule";
  path: string;
  operator: string;
  value: string;
  source_contracts: string[];
};

export type LogicConditionGroupChild = LogicConditionRule | LogicConditionGroup;

export type LogicConditionGroup = {
  id: string;
  type: "group";
  combinator: LogicCombinator;
  negated: boolean;
  children: LogicConditionGroupChild[];
};

export type LogicConditionBranch = {
  id: string;
  label: string;
  output_handle_id: string;
  root_group: LogicConditionGroup;
};

export type LogicConditionNodeConfig = {
  branches: LogicConditionBranch[];
  else_output_handle_id: string;
};

type LogicClauseCandidate = {
  id?: unknown;
  label?: unknown;
  path?: unknown;
  operator?: unknown;
  value?: unknown;
  source_contracts?: unknown;
  output_handle_id?: unknown;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function asStringArray(value: unknown): string[] {
  return Array.isArray(value) ? value.map((entry) => String(entry).trim()).filter(Boolean) : [];
}

function slugify(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function compactString(value: unknown): string {
  return typeof value === "string" ? value : value == null ? "" : String(value);
}

function ensureUniqueHandleId(handleId: string, usedHandleIds: Set<string>): string {
  if (!usedHandleIds.has(handleId)) {
    usedHandleIds.add(handleId);
    return handleId;
  }
  let suffix = 2;
  let nextHandleId = `${handleId}-${suffix}`;
  while (usedHandleIds.has(nextHandleId)) {
    suffix += 1;
    nextHandleId = `${handleId}-${suffix}`;
  }
  usedHandleIds.add(nextHandleId);
  return nextHandleId;
}

export function buildLogicBranchHandleId(label: string, fallback: string, index: number): string {
  const slug = slugify(label) || slugify(fallback) || `condition-${index + 1}`;
  return `control-flow-${slug}`;
}

export function buildLogicRuleId(index: number): string {
  return `rule-${Date.now()}-${index}`;
}

export function buildLogicGroupId(index: number): string {
  return `group-${Date.now()}-${index}`;
}

export function createLogicConditionRule(index: number): LogicConditionRule {
  return {
    id: buildLogicRuleId(index),
    type: "rule",
    path: "",
    operator: "equals",
    value: "",
    source_contracts: [],
  };
}

export function createLogicConditionGroup(index: number, combinator: LogicCombinator = "all"): LogicConditionGroup {
  return {
    id: buildLogicGroupId(index),
    type: "group",
    combinator,
    negated: false,
    children: [createLogicConditionRule(index)],
  };
}

export function createLogicConditionBranch(index: number, label?: string): LogicConditionBranch {
  const branchLabel = label ?? `Branch ${index + 1}`;
  return {
    id: `branch-${Date.now()}-${index}`,
    label: branchLabel,
    output_handle_id: buildLogicBranchHandleId(branchLabel, `branch-${index + 1}`, index),
    root_group: createLogicConditionGroup(index, "all"),
  };
}

function normalizeLogicRule(candidate: unknown, index: number): LogicConditionRule {
  const record = isRecord(candidate) ? candidate : {};
  return {
    id: compactString(record.id) || `rule-${index + 1}`,
    type: "rule",
    path: compactString(record.path),
    operator: compactString(record.operator) || "equals",
    value: compactString(record.value),
    source_contracts: asStringArray(record.source_contracts),
  };
}

function normalizeLogicGroup(candidate: unknown, index: number): LogicConditionGroup {
  const record = isRecord(candidate) ? candidate : {};
  const rawChildren = Array.isArray(record.children) ? record.children : [];
  const children = rawChildren
    .map((child, childIndex) => normalizeLogicGroupChild(child, childIndex))
    .filter((child): child is LogicConditionGroupChild => child !== null);
  return {
    id: compactString(record.id) || `group-${index + 1}`,
    type: "group",
    combinator: record.combinator === "any" ? "any" : "all",
    negated: record.negated === true,
    children: children.length > 0 ? children : [createLogicConditionRule(index)],
  };
}

function normalizeLogicGroupChild(candidate: unknown, index: number): LogicConditionGroupChild | null {
  if (!isRecord(candidate)) {
    return null;
  }
  if (candidate.type === "group") {
    return normalizeLogicGroup(candidate, index);
  }
  return normalizeLogicRule(candidate, index);
}

function branchFromLegacyClause(rawClause: LogicClauseCandidate, index: number): LogicConditionBranch {
  const clauseId = compactString(rawClause.id) || `clause-${index + 1}`;
  const label = compactString(rawClause.label);
  return {
    id: clauseId,
    label,
    output_handle_id: compactString(rawClause.output_handle_id) || DEFAULT_LOGIC_BRANCH_HANDLE_ID,
    root_group: {
      id: `group-${clauseId}`,
      type: "group",
      combinator: "all",
      negated: false,
      children: [
        {
          id: `rule-${clauseId}`,
          type: "rule",
          path: compactString(rawClause.path),
          operator: compactString(rawClause.operator) || "equals",
          value: compactString(rawClause.value),
          source_contracts: asStringArray(rawClause.source_contracts),
        },
      ],
    },
  };
}

export function normalizeLogicConditionConfig(config: GraphNodeConfig): {
  normalized: LogicConditionNodeConfig;
  handleRemap: Map<string, string[]>;
} {
  const rawBranches = Array.isArray(config.branches) ? config.branches : null;
  const rawClauses = Array.isArray(config.clauses) ? config.clauses : null;
  const elseHandleId = compactString(config.else_output_handle_id) || DEFAULT_LOGIC_ELSE_HANDLE_ID;
  const handleRemap = new Map<string, string[]>();
  const usedHandleIds = new Set<string>([elseHandleId]);
  const candidateBranches = rawBranches != null
    ? rawBranches
    : rawClauses != null
      ? rawClauses.map((rawClause, index) => branchFromLegacyClause((isRecord(rawClause) ? rawClause : {}) as LogicClauseCandidate, index))
      : [createLogicConditionBranch(0, "If")];
  const branches = candidateBranches.map((candidate, index) => {
    const record = isRecord(candidate) ? candidate : {};
    const label = compactString(record.label);
    const fallbackId = compactString(record.id) || `branch-${index + 1}`;
    const previousHandleId = compactString(record.output_handle_id) || DEFAULT_LOGIC_BRANCH_HANDLE_ID;
    const shouldReplaceLegacyHandle = previousHandleId === DEFAULT_LOGIC_BRANCH_HANDLE_ID && candidateBranches.length > 1;
    const baseHandleId = shouldReplaceLegacyHandle
      ? buildLogicBranchHandleId(label, fallbackId, index)
      : previousHandleId;
    const nextHandleId = ensureUniqueHandleId(baseHandleId, usedHandleIds);
    if (previousHandleId !== nextHandleId) {
      handleRemap.set(previousHandleId, [...(handleRemap.get(previousHandleId) ?? []), nextHandleId]);
    }
    return {
      id: fallbackId,
      label,
      output_handle_id: nextHandleId,
      root_group: normalizeLogicGroup(record.root_group, index),
    } satisfies LogicConditionBranch;
  });
  return {
    normalized: {
      branches,
      else_output_handle_id: elseHandleId,
    },
    handleRemap,
  };
}

export function serializeLogicConditionConfig(config: LogicConditionNodeConfig): Record<string, unknown> {
  function serializeGroupChild(child: LogicConditionGroupChild): Record<string, unknown> {
    if (child.type === "group") {
      return {
        id: child.id,
        type: "group",
        combinator: child.combinator,
        negated: child.negated,
        children: child.children.map(serializeGroupChild),
      };
    }
    return {
      id: child.id,
      type: "rule",
      path: child.path,
      operator: child.operator,
      value: child.value,
      source_contracts: child.source_contracts,
    };
  }

  return {
    branches: config.branches.map((branch) => ({
      id: branch.id,
      label: branch.label,
      output_handle_id: branch.output_handle_id,
      root_group: serializeGroupChild(branch.root_group),
    })),
    else_output_handle_id: config.else_output_handle_id,
  };
}

export function summarizeLogicRule(rule: LogicConditionRule): string {
  const path = rule.path.trim() || "payload";
  const operator = rule.operator.replace(/_/g, " ");
  const value = rule.operator === "exists" ? "" : ` ${rule.value || "?"}`;
  return `${path} ${operator}${value}`.trim();
}

export function summarizeLogicGroup(group: LogicConditionGroup): string {
  const childSummaries = group.children.map((child) => {
    if (child.type === "group") {
      return `(${summarizeLogicGroup(child)})`;
    }
    return summarizeLogicRule(child);
  });
  const joiner = group.combinator === "any" ? " OR " : " AND ";
  const summary = childSummaries.join(joiner) || "No rules";
  return group.negated ? `NOT ${summary}` : summary;
}
