import { useMemo, useState } from "react";

import type {
  EditorCatalog,
  McpCapabilityDefinition,
  McpServerDraft,
  McpServerStatus,
  McpServerTemplate,
  McpServerTestResult,
  ToolDefinition,
} from "../lib/types";

type McpServerPanelProps = {
  catalog: EditorCatalog | null;
  onBootMcpServer: (serverId: string) => void;
  onStopMcpServer: (serverId: string) => void;
  onRefreshMcpServer: (serverId: string) => void;
  onToggleMcpTool: (toolName: string, enabled: boolean) => void;
  onCreateMcpServer: (server: McpServerDraft) => Promise<unknown>;
  onUpdateMcpServer: (serverId: string, server: McpServerDraft) => Promise<unknown>;
  onDeleteMcpServer: (serverId: string) => Promise<unknown>;
  onTestMcpServer: (server: McpServerDraft) => Promise<McpServerTestResult | null>;
  mcpPendingKey: string | null;
  title?: string;
  description?: string;
  className?: string;
};

type CapabilityFilter = "all" | "tool" | "resource" | "resource_template" | "prompt";
type DiscoveryFilter = "all" | "running" | "offline" | "issues";
type McpPanelTab = "overview" | "templates" | "discovery" | "editor" | "servers";

type McpServerFormState = {
  server_id: string;
  display_name: string;
  description: string;
  transport: "stdio" | "http";
  command_text: string;
  cwd: string;
  env_text: string;
  base_url: string;
  timeout_seconds: string;
  auto_boot: boolean;
  persistent: boolean;
};

type FormInsight = {
  errors: string[];
  warnings: string[];
};

function createBlankForm(transport: "stdio" | "http" = "stdio"): McpServerFormState {
  return {
    server_id: "",
    display_name: "",
    description: "",
    transport,
    command_text: "",
    cwd: "",
    env_text: "",
    base_url: "",
    timeout_seconds: "15",
    auto_boot: false,
    persistent: true,
  };
}

function formFromServer(server: McpServerStatus): McpServerFormState {
  return {
    server_id: server.server_id,
    display_name: server.display_name,
    description: server.description,
    transport: server.transport ?? "stdio",
    command_text: (server.config?.command ?? []).join("\n"),
    cwd: server.config?.cwd ?? "",
    env_text: Object.entries(server.config?.env ?? {})
      .map(([key, value]) => `${key}=${value}`)
      .join("\n"),
    base_url: server.config?.base_url ?? "",
    timeout_seconds: String(server.config?.timeout_seconds ?? 15),
    auto_boot: server.auto_boot,
    persistent: server.persistent,
  };
}

function formFromDraft(draft: McpServerDraft): McpServerFormState {
  return {
    server_id: draft.server_id,
    display_name: draft.display_name,
    description: draft.description,
    transport: draft.transport,
    command_text: draft.command.join("\n"),
    cwd: draft.cwd ?? "",
    env_text: Object.entries(draft.env ?? {})
      .map(([key, value]) => `${key}=${value}`)
      .join("\n"),
    base_url: draft.base_url ?? "",
    timeout_seconds: String(draft.timeout_seconds ?? 15),
    auto_boot: draft.auto_boot,
    persistent: draft.persistent,
  };
}

function parseCommand(commandText: string): string[] {
  return commandText
    .split("\n")
    .map((part) => part.trim())
    .filter(Boolean);
}

function parseEnv(envText: string): Record<string, string> {
  const env: Record<string, string> = {};
  for (const rawLine of envText.split("\n")) {
    const line = rawLine.trim();
    if (!line) {
      continue;
    }
    const equalsIndex = line.indexOf("=");
    if (equalsIndex === -1) {
      env[line] = "";
      continue;
    }
    const key = line.slice(0, equalsIndex).trim();
    if (!key) {
      continue;
    }
    env[key] = line.slice(equalsIndex + 1).trim();
  }
  return env;
}

function toDraft(form: McpServerFormState): McpServerDraft {
  return {
    server_id: form.server_id.trim(),
    display_name: form.display_name.trim(),
    description: form.description.trim(),
    transport: form.transport,
    command: parseCommand(form.command_text),
    cwd: form.cwd.trim() || null,
    env: parseEnv(form.env_text),
    base_url: form.base_url.trim() || null,
    timeout_seconds: Number.parseInt(form.timeout_seconds.trim() || "15", 10) || 15,
    auto_boot: form.auto_boot,
    persistent: form.persistent,
  };
}

function isToolOnline(tool: ToolDefinition): boolean {
  return tool.available !== false;
}

function isToolEnabled(tool: ToolDefinition): boolean {
  return tool.enabled !== false;
}

function toolStatusLabel(tool: ToolDefinition): string {
  if (!isToolEnabled(tool)) {
    return "disabled";
  }
  if (!isToolOnline(tool)) {
    return "offline";
  }
  return "ready";
}

function toolCanonicalName(tool: ToolDefinition): string {
  return tool.canonical_name ?? tool.name;
}

function toolLabel(tool: ToolDefinition): string {
  return tool.display_name ?? tool.name;
}

function capabilityLabel(capability: McpCapabilityDefinition): string {
  return capability.title || capability.display_name || capability.name;
}

function capabilityReference(capability: McpCapabilityDefinition): string {
  if (typeof capability.metadata?.uri === "string" && capability.metadata.uri.trim()) {
    return capability.metadata.uri;
  }
  if (typeof capability.metadata?.uri_template === "string" && capability.metadata.uri_template.trim()) {
    return capability.metadata.uri_template;
  }
  return capability.canonical_name;
}

function capabilityStatusLabel(capability: McpCapabilityDefinition): string {
  if (capability.available === false) {
    return "offline";
  }
  if (capability.capability_type === "tool" && capability.enabled === false) {
    return "disabled";
  }
  return "discovered";
}

function declaredCapabilityLabels(server: McpServerStatus): string[] {
  return Object.keys(server.declared_capabilities ?? {}).sort();
}

function templateProvenanceLabel(template: McpServerTemplate): string {
  const registry = typeof template.provenance?.registry === "string" ? template.provenance.registry : null;
  const publisher = typeof template.provenance?.publisher === "string" ? template.provenance.publisher : null;
  return [template.source, registry, publisher].filter(Boolean).join(" • ");
}

function normalizeSearch(value: string): string {
  return value.trim().toLowerCase();
}

function includesQuery(haystack: Array<string | null | undefined>, query: string): boolean {
  if (!query) {
    return true;
  }
  return haystack.some((item) => item?.toLowerCase().includes(query));
}

function summarizeConfig(server: McpServerStatus): string {
  if (server.transport === "http") {
    return server.config?.base_url?.trim() || "HTTP endpoint not set";
  }
  const command = server.config?.command ?? [];
  if (command.length === 0) {
    return "Command not configured";
  }
  return command.join(" ");
}

function countServerTools(serverId: string, tools: ToolDefinition[]): number {
  return tools.filter((tool) => tool.server_id === serverId).length;
}

function countServerReadyTools(serverId: string, tools: ToolDefinition[]): number {
  return tools.filter((tool) => tool.server_id === serverId && isToolEnabled(tool) && isToolOnline(tool)).length;
}

function buildFormInsights(form: McpServerFormState): FormInsight {
  const errors: string[] = [];
  const warnings: string[] = [];
  const timeout = Number.parseInt(form.timeout_seconds.trim() || "0", 10);
  const command = parseCommand(form.command_text);
  const envEntries = parseEnv(form.env_text);
  const envKeys = Object.keys(envEntries);

  if (!form.server_id.trim()) {
    errors.push("Server ID is required.");
  } else if (!/^[a-zA-Z0-9._-]+$/.test(form.server_id.trim())) {
    errors.push("Server ID should use letters, numbers, dots, underscores, or dashes.");
  }
  if (!form.display_name.trim()) {
    errors.push("Display name is required.");
  }
  if (!Number.isFinite(timeout) || timeout < 1) {
    errors.push("Timeout must be at least 1 second.");
  }

  if (form.transport === "stdio") {
    if (command.length === 0) {
      errors.push("A stdio server needs at least one command argument.");
    }
    if (!form.cwd.trim()) {
      warnings.push("Working directory is empty. Relative paths will resolve from the API process.");
    }
  }

  if (form.transport === "http") {
    if (!form.base_url.trim()) {
      errors.push("Base URL is required for HTTP servers.");
    } else {
      try {
        const parsed = new URL(form.base_url.trim());
        if (!["http:", "https:"].includes(parsed.protocol)) {
          errors.push("Base URL must start with http:// or https://.");
        }
      } catch {
        errors.push("Base URL must be a valid URL.");
      }
    }
  }

  if (!form.description.trim()) {
    warnings.push("Add a short description so this server is easier to recognize later.");
  }
  if (envKeys.length > 0 && envKeys.some((key) => key.includes(" "))) {
    warnings.push("Environment variable keys should not contain spaces.");
  }
  if (form.transport === "stdio" && command.length > 0 && command[0]?.startsWith("python")) {
    warnings.push("Python-based servers require the same runtime to be available where the API process runs.");
  }
  if (form.auto_boot && !form.persistent) {
    warnings.push("Auto boot is on, but the definition is not persisted locally.");
  }

  return { errors, warnings };
}

function StatCard({ label, value, tone = "default" }: { label: string; value: string; tone?: "default" | "good" | "warn" }) {
  return (
    <div className={`mcp-stat-card mcp-stat-card--${tone}`}>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

export function McpServerPanel({
  catalog,
  onBootMcpServer,
  onStopMcpServer,
  onRefreshMcpServer,
  onToggleMcpTool,
  onCreateMcpServer,
  onUpdateMcpServer,
  onDeleteMcpServer,
  onTestMcpServer,
  mcpPendingKey,
  title = "MCP Servers",
  description = "Manage persistent MCP servers and globally enable the tools they publish.",
  className = "",
}: McpServerPanelProps) {
  const mcpServers = catalog?.mcp_servers ?? [];
  const mcpCapabilities = catalog?.mcp_capabilities ?? [];
  const mcpServerTemplates = catalog?.mcp_server_templates ?? [];
  const tools = catalog?.tools ?? [];
  const [editingServerId, setEditingServerId] = useState<string | null>(null);
  const [formState, setFormState] = useState<McpServerFormState>(createBlankForm);
  const [testSnapshot, setTestSnapshot] = useState<McpServerTestResult | null>(null);
  const [localError, setLocalError] = useState<string | null>(null);
  const [capabilityFilter, setCapabilityFilter] = useState<CapabilityFilter>("all");
  const [discoveryFilter, setDiscoveryFilter] = useState<DiscoveryFilter>("all");
  const [discoveryQuery, setDiscoveryQuery] = useState("");
  const [templateQuery, setTemplateQuery] = useState("");
  const [activeTab, setActiveTab] = useState<McpPanelTab>("overview");

  const editingServer = useMemo(
    () => mcpServers.find((server) => server.server_id === editingServerId) ?? null,
    [editingServerId, mcpServers],
  );
  const isCreating = editingServerId === "__new__";
  const isEditing = Boolean(editingServerId);
  const formInsights = useMemo(() => buildFormInsights(formState), [formState]);
  const parsedCommand = useMemo(() => parseCommand(formState.command_text), [formState.command_text]);
  const parsedEnv = useMemo(() => parseEnv(formState.env_text), [formState.env_text]);
  const discoveryQueryValue = useMemo(() => normalizeSearch(discoveryQuery), [discoveryQuery]);
  const templateQueryValue = useMemo(() => normalizeSearch(templateQuery), [templateQuery]);

  const summary = useMemo(() => {
    const runningServers = mcpServers.filter((server) => server.running).length;
    const issueServers = mcpServers.filter((server) => Boolean(server.error)).length;
    const readyTools = tools.filter((tool) => tool.server_id && isToolEnabled(tool) && isToolOnline(tool)).length;
    const offlineCapabilities = mcpCapabilities.filter((capability) => capability.available === false).length;
    return {
      runningServers,
      issueServers,
      readyTools,
      offlineCapabilities,
    };
  }, [mcpCapabilities, mcpServers, tools]);

  const filteredTemplates = useMemo(
    () =>
      mcpServerTemplates.filter((template) =>
        includesQuery(
          [
            template.template_id,
            template.display_name,
            template.description,
            template.draft.transport,
            ...(template.capability_hints ?? []),
            templateProvenanceLabel(template),
          ],
          templateQueryValue,
        ),
      ),
    [mcpServerTemplates, templateQueryValue],
  );

  const filteredServers = useMemo(
    () =>
      mcpServers.filter((server) => {
        const matchesQuery =
          includesQuery(
            [
              server.server_id,
              server.display_name,
              server.description,
              server.transport,
              server.config_summary,
              summarizeConfig(server),
              server.source,
              ...(server.capability_types ?? []),
              ...declaredCapabilityLabels(server),
              ...server.tool_names,
            ],
            discoveryQueryValue,
          ) ||
          mcpCapabilities.some(
            (capability) =>
              capability.server_id === server.server_id &&
              includesQuery(
                [capability.canonical_name, capability.name, capability.display_name, capability.title, capability.description],
                discoveryQueryValue,
              ),
          );

        if (!matchesQuery) {
          return false;
        }
        if (discoveryFilter === "running") {
          return server.running;
        }
        if (discoveryFilter === "offline") {
          return !server.running;
        }
        if (discoveryFilter === "issues") {
          return Boolean(server.error) || countServerReadyTools(server.server_id, tools) === 0;
        }
        return true;
      }),
    [discoveryFilter, discoveryQueryValue, mcpCapabilities, mcpServers, tools],
  );

  function beginCreate(templateDraft?: McpServerDraft, transport?: "stdio" | "http") {
    setEditingServerId("__new__");
    setFormState(templateDraft ? formFromDraft(templateDraft) : createBlankForm(transport));
    setTestSnapshot(null);
    setLocalError(null);
    setActiveTab("editor");
  }

  function beginEdit(server: McpServerStatus) {
    setEditingServerId(server.server_id);
    setFormState(formFromServer(server));
    setTestSnapshot(null);
    setLocalError(null);
    setActiveTab("editor");
  }

  function cancelEdit() {
    setEditingServerId(null);
    setFormState(createBlankForm());
    setTestSnapshot(null);
    setLocalError(null);
  }

  async function handleSubmit() {
    if (formInsights.errors.length > 0) {
      setLocalError(formInsights.errors[0] ?? "Complete the required MCP server fields.");
      return;
    }
    try {
      const draft = toDraft(formState);
      if (isCreating) {
        await onCreateMcpServer(draft);
      } else if (editingServer) {
        await onUpdateMcpServer(editingServer.server_id, draft);
      }
      cancelEdit();
    } catch (error) {
      setLocalError(error instanceof Error ? error.message : "Unable to save MCP server.");
    }
  }

  async function handleTest() {
    if (formInsights.errors.length > 0) {
      setLocalError(formInsights.errors[0] ?? "Complete the required MCP server fields before testing.");
      setTestSnapshot(null);
      return;
    }
    try {
      const result = await onTestMcpServer(toDraft(formState));
      setTestSnapshot(result);
      setLocalError(null);
    } catch (error) {
      setLocalError(error instanceof Error ? error.message : "Unable to test MCP server.");
      setTestSnapshot(null);
    }
  }

  return (
    <section className={`mcp-server-panel ${className}`.trim()}>
      <div className="modal-folder-tabs" role="tablist" aria-label="MCP sections">
        {([
          ["overview", "Overview"],
          ["templates", `Templates (${filteredTemplates.length})`],
          ["discovery", "Discovery"],
          ["editor", isEditing ? "Define Server" : "Definition"],
          ["servers", `Project Servers (${filteredServers.length})`],
        ] as Array<[McpPanelTab, string]>).map(([tabId, label]) => (
          <button
            key={tabId}
            type="button"
            role="tab"
            aria-selected={activeTab === tabId}
            className={`modal-folder-tab ${activeTab === tabId ? "modal-folder-tab--active" : ""}`}
            onClick={() => setActiveTab(tabId)}
          >
            {label}
          </button>
        ))}
      </div>

      <div className="modal-folder-panel">
        {activeTab === "overview" ? (
          <div className="contract-card mcp-server-hero">
            <div className="mcp-server-hero-copy">
              <strong>{title}</strong>
              <span>{description}</span>
              <span>Project MCP is shared infrastructure: define servers here, then let nodes opt into the tools they should expose or describe.</span>
            </div>
            <div className="mcp-server-actions">
              <button type="button" className="secondary-button" onClick={() => beginCreate(undefined, "stdio")} disabled={isCreating}>
                New stdio Server
              </button>
              <button type="button" className="secondary-button" onClick={() => beginCreate(undefined, "http")} disabled={isCreating}>
                New HTTP Server
              </button>
              {isEditing ? (
                <button type="button" className="secondary-button" onClick={cancelEdit}>
                  Cancel
                </button>
              ) : null}
            </div>
            <div className="mcp-stats-grid">
              <StatCard label="Servers" value={`${mcpServers.length}`} />
              <StatCard label="Running" value={`${summary.runningServers}`} tone="good" />
              <StatCard label="Ready Tools" value={`${summary.readyTools}`} tone="good" />
              <StatCard label="Need Attention" value={`${summary.issueServers + summary.offlineCapabilities}`} tone="warn" />
            </div>
          </div>
        ) : null}

        {activeTab === "templates" ? (
          <div className="mcp-template-browser">
            <div className="mcp-template-browser-header">
              <div>
                <strong>Quick Start</strong>
                <span>Start from a curated draft when you want discovery to work before you memorize every flag and env var.</span>
              </div>
              <label className="provider-search mcp-inline-search">
                Search templates
                <input
                  value={templateQuery}
                  onChange={(event) => setTemplateQuery(event.target.value)}
                  placeholder="filesystem, github, resource, prompt..."
                />
              </label>
            </div>
            {filteredTemplates.length > 0 ? (
              <div className="mcp-template-list">
                {filteredTemplates.map((template) => (
                  <div key={template.template_id} className="mcp-template-card">
                    <div className="mcp-template-card-header">
                      <div>
                        <strong>{template.display_name}</strong>
                        <p>{template.description}</p>
                      </div>
                      <button type="button" className="secondary-button" onClick={() => beginCreate(template.draft)}>
                        Use Template
                      </button>
                    </div>
                    <p className="mcp-server-meta">
                      <code>{template.template_id}</code>
                      <span>{template.draft.transport}</span>
                      {template.capability_hints?.map((hint) => (
                        <span key={hint}>{hint}</span>
                      ))}
                      {templateProvenanceLabel(template) ? <span>{templateProvenanceLabel(template)}</span> : null}
                    </p>
                    <div className="mcp-template-preview">
                      <span>{template.draft.transport === "http" ? template.draft.base_url || "Remote MCP endpoint" : template.draft.command.join(" ")}</span>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <p className="inspector-hint">No templates match that search yet.</p>
            )}
          </div>
        ) : null}

        {activeTab === "discovery" ? (
          <div className="mcp-capability-browser-toolbar mcp-capability-browser-toolbar--discovery">
            <div className="mcp-capability-browser-toolbar-top">
              <div>
                <strong>Discovery Workspace</strong>
                <span>Search across servers, discovered capabilities, and published tool names before you edit anything.</span>
              </div>
              <label className="provider-search mcp-inline-search">
                Search discovery
                <input
                  value={discoveryQuery}
                  onChange={(event) => setDiscoveryQuery(event.target.value)}
                  placeholder="server id, tool name, capability, command..."
                />
              </label>
            </div>
            <div className="mcp-discovery-filter-row">
              {(["all", "running", "offline", "issues"] as DiscoveryFilter[]).map((filter) => (
                <button
                  key={filter}
                  type="button"
                  className={`secondary-button ${discoveryFilter === filter ? "is-active" : ""}`.trim()}
                  onClick={() => setDiscoveryFilter(filter)}
                >
                  {filter === "all" ? `All (${mcpServers.length})` : filter === "running" ? `Running (${summary.runningServers})` : filter === "offline" ? `Offline (${mcpServers.length - summary.runningServers})` : `Issues (${summary.issueServers})`}
                </button>
              ))}
            </div>
            <div className="mcp-capability-filter-row">
              {(["all", "tool", "resource", "resource_template", "prompt"] as CapabilityFilter[]).map((filter) => (
                <button
                  key={filter}
                  type="button"
                  className={`secondary-button ${capabilityFilter === filter ? "is-active" : ""}`.trim()}
                  onClick={() => setCapabilityFilter(filter)}
                >
                  {filter === "all" ? "All capabilities" : filter}
                </button>
              ))}
            </div>
          </div>
        ) : null}

        {activeTab === "editor" ? (isEditing ? (
          <div className="mcp-server-card mcp-server-card--editor">
            <div className="mcp-server-card-header">
              <div>
                <strong>{isCreating ? "Define MCP Server" : `Edit ${editingServer?.display_name ?? "MCP Server"}`}</strong>
                <p>
                  {isCreating
                    ? "Fill in the transport details, validate the connection, then save it into the project-level MCP catalog."
                    : "Update the saved server definition, then re-test or refresh discovery when the upstream server changes."}
                </p>
              </div>
              <span className="status-pill is-muted">{formState.transport}</span>
            </div>

            <div className="mcp-editor-layout">
              <div className="mcp-server-form-grid">
                <label>
                  Server ID
                  <input
                    value={formState.server_id}
                    onChange={(event) => setFormState((current) => ({ ...current, server_id: event.target.value }))}
                    disabled={!isCreating}
                    placeholder="weather_remote"
                  />
                </label>
                <label>
                  Display Name
                  <input
                    value={formState.display_name}
                    onChange={(event) => setFormState((current) => ({ ...current, display_name: event.target.value }))}
                    placeholder="Remote Weather"
                  />
                </label>
                <label>
                  Transport
                  <select
                    value={formState.transport}
                    onChange={(event) =>
                      setFormState((current) => ({
                        ...current,
                        transport: event.target.value === "http" ? "http" : "stdio",
                      }))
                    }
                  >
                    <option value="stdio">stdio subprocess</option>
                    <option value="http">remote HTTP</option>
                  </select>
                </label>
                <label>
                  Timeout Seconds
                  <input
                    type="number"
                    min={1}
                    value={formState.timeout_seconds}
                    onChange={(event) => setFormState((current) => ({ ...current, timeout_seconds: event.target.value }))}
                  />
                </label>
                <label className="mcp-server-form-grid--full">
                  Description
                  <textarea
                    value={formState.description}
                    onChange={(event) => setFormState((current) => ({ ...current, description: event.target.value }))}
                    rows={2}
                    placeholder="What this server exposes and why the team might use it."
                  />
                </label>
                {formState.transport === "stdio" ? (
                  <>
                    <label className="mcp-server-form-grid--full">
                      Command
                      <textarea
                        value={formState.command_text}
                        onChange={(event) => setFormState((current) => ({ ...current, command_text: event.target.value }))}
                        rows={4}
                        placeholder={"python\n-m\nmy_mcp_server"}
                      />
                      <small>Enter one subprocess argument per line so discovery uses the exact invocation you expect.</small>
                    </label>
                    <label>
                      Working Directory
                      <input
                        value={formState.cwd}
                        onChange={(event) => setFormState((current) => ({ ...current, cwd: event.target.value }))}
                        placeholder="/path/to/project"
                      />
                    </label>
                    <label className="mcp-server-form-grid--full">
                      Environment Variables
                      <textarea
                        value={formState.env_text}
                        onChange={(event) => setFormState((current) => ({ ...current, env_text: event.target.value }))}
                        rows={4}
                        placeholder={"PYTHONPATH=/path/to/src\nFOO=bar"}
                      />
                      <small>Use one `KEY=value` pair per line. Referencing host secrets like `${"{GITHUB_TOKEN}"}` is fine.</small>
                    </label>
                  </>
                ) : (
                  <label className="mcp-server-form-grid--full">
                    Base URL
                    <input
                      value={formState.base_url}
                      onChange={(event) => setFormState((current) => ({ ...current, base_url: event.target.value }))}
                      placeholder="https://example.com/mcp"
                    />
                  </label>
                )}
                <label className="checkbox-option">
                  <input
                    type="checkbox"
                    checked={formState.auto_boot}
                    onChange={(event) => setFormState((current) => ({ ...current, auto_boot: event.target.checked }))}
                  />
                  <span>
                    Auto boot
                    <small>Reconnect this server when the app starts.</small>
                  </span>
                </label>
                <label className="checkbox-option">
                  <input
                    type="checkbox"
                    checked={formState.persistent}
                    onChange={(event) => setFormState((current) => ({ ...current, persistent: event.target.checked }))}
                  />
                  <span>
                    Persist locally
                    <small>Keep this server definition in the user-local `.graph-agent` store.</small>
                  </span>
                </label>
              </div>

              <aside className="mcp-editor-sidebar">
                <div className="mcp-editor-sidebar-card">
                  <strong>Definition Health</strong>
                  <div className="mcp-editor-checklist">
                    <div className={`mcp-editor-check ${formInsights.errors.length === 0 ? "is-good" : "is-bad"}`}>
                      <span>{formInsights.errors.length === 0 ? "Ready to test" : `${formInsights.errors.length} required fix${formInsights.errors.length === 1 ? "" : "es"}`}</span>
                    </div>
                    {formInsights.errors.map((error) => (
                      <div key={error} className="mcp-editor-check is-bad">
                        <span>{error}</span>
                      </div>
                    ))}
                    {formInsights.warnings.map((warning) => (
                      <div key={warning} className="mcp-editor-check is-warn">
                        <span>{warning}</span>
                      </div>
                    ))}
                    {formInsights.errors.length === 0 && formInsights.warnings.length === 0 ? (
                      <div className="mcp-editor-check is-good">
                        <span>This definition has the core fields needed for discovery and boot.</span>
                      </div>
                    ) : null}
                  </div>
                </div>

                <div className="mcp-editor-sidebar-card">
                  <strong>Connection Preview</strong>
                  {formState.transport === "stdio" ? (
                    <>
                      <p className="mcp-editor-preview-label">Command</p>
                      <div className="mcp-preview-chip-list">
                        {parsedCommand.length > 0 ? parsedCommand.map((part, index) => <code key={`${part}-${index}`}>{part}</code>) : <span className="inspector-hint">No command args yet.</span>}
                      </div>
                      <p className="mcp-editor-preview-label">Environment</p>
                      <div className="mcp-preview-chip-list">
                        {Object.keys(parsedEnv).length > 0 ? Object.entries(parsedEnv).map(([key, value]) => <code key={key}>{`${key}=${value}`}</code>) : <span className="inspector-hint">No env vars set.</span>}
                      </div>
                    </>
                  ) : (
                    <>
                      <p className="mcp-editor-preview-label">Endpoint</p>
                      <div className="mcp-preview-chip-list">
                        {formState.base_url.trim() ? <code>{formState.base_url.trim()}</code> : <span className="inspector-hint">No base URL yet.</span>}
                      </div>
                    </>
                  )}
                </div>

                <div className="mcp-editor-sidebar-card">
                  <strong>Definition Notes</strong>
                  <p className="inspector-hint">Discovery happens when you test, boot, or refresh the server. Published tools can then be enabled globally below.</p>
                </div>
              </aside>
            </div>

            <div className="mcp-server-actions">
              <button type="button" className="secondary-button" onClick={() => void handleTest()} disabled={mcpPendingKey === `test:${formState.server_id || "draft"}`}>
                {mcpPendingKey === `test:${formState.server_id || "draft"}` ? "Testing..." : "Test Discovery"}
              </button>
              <button
                type="button"
                className="secondary-button"
                onClick={() => void handleSubmit()}
                disabled={
                  formInsights.errors.length > 0 ||
                  mcpPendingKey === `create:${formState.server_id}` ||
                  mcpPendingKey === `update:${editingServer?.server_id ?? ""}`
                }
              >
                {isCreating
                  ? mcpPendingKey === `create:${formState.server_id}`
                    ? "Creating..."
                    : "Create Server"
                  : mcpPendingKey === `update:${editingServer?.server_id ?? ""}`
                    ? "Saving..."
                    : "Save Changes"}
              </button>
            </div>

            {testSnapshot ? (
              <div className="mcp-test-result">
                <p className="mcp-server-message">{testSnapshot.message}</p>
                <p className="mcp-server-meta">
                  <span>{testSnapshot.capability_count ?? testSnapshot.capabilities.length} capabilities</span>
                  {(testSnapshot.capability_types ?? []).map((item) => (
                    <span key={item}>{item}</span>
                  ))}
                  {Object.keys(testSnapshot.declared_capabilities ?? {}).map((item) => (
                    <span key={item}>declares {item}</span>
                  ))}
                </p>
                {testSnapshot.capabilities.length > 0 ? (
                  <div className="mcp-capability-list">
                    {testSnapshot.capabilities.map((capability) => (
                      <div key={capability.canonical_name} className="mcp-capability-card">
                        <div className="mcp-capability-card-header">
                          <strong>{capabilityLabel(capability)}</strong>
                          <span className="status-pill is-muted">{capability.capability_type}</span>
                        </div>
                        <p className="mcp-server-meta">
                          <code>{capabilityReference(capability)}</code>
                          <span>{capabilityStatusLabel(capability)}</span>
                        </p>
                      </div>
                    ))}
                  </div>
                ) : null}
              </div>
            ) : null}
            {localError ? <p className="error-text">{localError}</p> : null}
          </div>
        ) : (
            <div className="mcp-server-card">
              <strong>No server definition is open.</strong>
              <p className="inspector-hint">Start a new stdio or HTTP server from the Overview tab, or open a template from the Templates tab.</p>
            </div>
          )) : null}

        {activeTab === "servers" ? (
          filteredServers.length > 0 ? (
            filteredServers.map((server) => {
            const serverTools = tools.filter((tool) => tool.server_id === server.server_id);
            const serverCapabilities = mcpCapabilities
              .filter((capability) => capability.server_id === server.server_id)
              .filter((capability) => capabilityFilter === "all" || capability.capability_type === capabilityFilter)
              .filter((capability) =>
                includesQuery(
                  [capability.canonical_name, capability.name, capability.display_name, capability.title, capability.description],
                  discoveryQueryValue,
                ),
              );
            const bootPending = mcpPendingKey === `boot:${server.server_id}`;
            const stopPending = mcpPendingKey === `stop:${server.server_id}`;
            const refreshPending = mcpPendingKey === `refresh:${server.server_id}`;
            const deletePending = mcpPendingKey === `delete:${server.server_id}`;
            const readyTools = countServerReadyTools(server.server_id, tools);

            return (
              <div key={server.server_id} className="mcp-server-card">
                <div className="mcp-server-card-header">
                  <div>
                    <strong>{server.display_name}</strong>
                    <p>{server.description}</p>
                    <p className="mcp-server-meta">
                      <code>{server.server_id}</code>
                      <span>{server.transport}</span>
                      <span>{summarizeConfig(server)}</span>
                      {server.source ? <span>{server.source}</span> : null}
                      <span>{readyTools}/{countServerTools(server.server_id, tools)} ready tools</span>
                      {typeof server.capability_count === "number" ? <span>{server.capability_count} capabilities</span> : null}
                      {(server.capability_types ?? []).map((item) => (
                        <span key={item}>{item}</span>
                      ))}
                      {declaredCapabilityLabels(server).map((item) => (
                        <span key={item}>declares {item}</span>
                      ))}
                    </p>
                  </div>
                  <div className="mcp-server-status-stack">
                    <span className={`status-pill ${server.running ? "is-ready" : "is-muted"}`}>{server.running ? "running" : "offline"}</span>
                    {server.error ? <span className="status-pill is-danger">needs attention</span> : null}
                  </div>
                </div>
                <div className="mcp-server-actions">
                  <button type="button" className="secondary-button" onClick={() => onBootMcpServer(server.server_id)} disabled={server.running || bootPending}>
                    {bootPending ? "Booting..." : "Boot"}
                  </button>
                  <button type="button" className="secondary-button" onClick={() => onRefreshMcpServer(server.server_id)} disabled={!server.running || refreshPending}>
                    {refreshPending ? "Refreshing..." : "Refresh"}
                  </button>
                  <button type="button" className="secondary-button" onClick={() => onStopMcpServer(server.server_id)} disabled={!server.running || stopPending}>
                    {stopPending ? "Stopping..." : "Stop"}
                  </button>
                  {server.editable ? (
                    <button type="button" className="secondary-button" onClick={() => beginEdit(server)} disabled={deletePending}>
                      Edit
                    </button>
                  ) : null}
                  {server.editable ? (
                    <button type="button" className="secondary-button" onClick={() => void onDeleteMcpServer(server.server_id)} disabled={deletePending}>
                      {deletePending ? "Deleting..." : "Delete"}
                    </button>
                  ) : null}
                </div>
                {server.error ? <p className="error-text">{server.error}</p> : null}
                {serverCapabilities.length > 0 ? (
                  <div className="mcp-capability-list">
                    {serverCapabilities.map((capability) => (
                      <div key={capability.canonical_name} className="mcp-capability-card">
                        <div className="mcp-capability-card-header">
                          <div>
                            <strong>{capabilityLabel(capability)}</strong>
                            {capability.description ? <p>{capability.description}</p> : null}
                          </div>
                          <span className="status-pill is-muted">{capability.capability_type}</span>
                        </div>
                        <p className="mcp-server-meta">
                          <code>{capabilityReference(capability)}</code>
                          <span>{capabilityStatusLabel(capability)}</span>
                          {capability.schema_warning ? <span>{capability.schema_warning}</span> : null}
                        </p>
                        {capability.metadata && Object.keys(capability.metadata).length > 0 ? (
                          <details className="mcp-capability-details">
                            <summary>Metadata</summary>
                            <pre>{JSON.stringify(capability.metadata, null, 2)}</pre>
                          </details>
                        ) : null}
                      </div>
                    ))}
                  </div>
                ) : (
                  <p className="inspector-hint">No {capabilityFilter === "all" ? "" : `${capabilityFilter} `}capabilities match this server right now.</p>
                )}
                {serverTools.length > 0 ? (
                  <div className="mcp-tool-list">
                    {serverTools.map((tool) => {
                      const toolName = toolCanonicalName(tool);
                      const pending = mcpPendingKey === `tool:${toolName}`;
                      return (
                        <label key={toolName} className="checkbox-option mcp-tool-option">
                          <input
                            type="checkbox"
                            checked={isToolEnabled(tool)}
                            disabled={pending}
                            onChange={(event) => onToggleMcpTool(toolName, event.target.checked)}
                          />
                          <span>
                            {toolLabel(tool)}
                            {toolLabel(tool) !== toolName ? (
                              <small>
                                <code>{toolName}</code>
                              </small>
                            ) : null}
                            <small>{toolStatusLabel(tool)}</small>
                            {tool.schema_warning ? <small>{tool.schema_warning}</small> : null}
                          </span>
                        </label>
                      );
                    })}
                  </div>
                ) : (
                  <p className="inspector-hint">No tools registered for this server yet.</p>
                )}
              </div>
            );
            })
          ) : (
            <div className="mcp-server-card">
              <strong>No MCP servers match this view.</strong>
              <p className="inspector-hint">Try clearing the discovery search, switching filters, or start from a template above.</p>
            </div>
          )
        ) : null}
      </div>
    </section>
  );
}
