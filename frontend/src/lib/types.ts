export type GraphPosition = {
  x: number;
  y: number;
};

export type GraphNodeConfig = Record<string, unknown>;

export type GraphNode = {
  id: string;
  kind: string;
  category: string;
  label: string;
  description?: string;
  provider_id: string;
  provider_label: string;
  position: GraphPosition;
  config: GraphNodeConfig;
  model_provider_name?: string;
  prompt_name?: string;
  tool_name?: string;
};

export type GraphEdgeCondition = {
  id: string;
  label: string;
  type: string;
  value?: unknown;
  path?: string | null;
};

export type GraphEdge = {
  id: string;
  source_id: string;
  target_id: string;
  source_handle_id?: string | null;
  target_handle_id?: string | null;
  label: string;
  kind: string;
  priority: number;
  waypoints?: GraphPosition[];
  condition?: GraphEdgeCondition | null;
};

export type GraphDefinition = {
  graph_id: string;
  name: string;
  description: string;
  version: string;
  graph_type?: "graph" | "test_environment";
  default_input?: string;
  start_node_id: string;
  env_vars?: Record<string, string>;
  supabase_connections?: SupabaseConnectionDefinition[];
  default_supabase_connection_id?: string;
  nodes: GraphNode[];
  edges: GraphEdge[];
  node_providers?: NodeProviderDefinition[];
};

export type AgentDefinition = {
  agent_id: string;
  name: string;
  description: string;
  version: string;
  start_node_id: string;
  env_vars?: Record<string, string>;
  nodes: GraphNode[];
  edges: GraphEdge[];
};

export type TestEnvironmentDefinition = {
  graph_id: string;
  name: string;
  description: string;
  version: string;
  graph_type: "test_environment" | "graph";
  default_input?: string;
  env_vars?: Record<string, string>;
  supabase_connections?: SupabaseConnectionDefinition[];
  default_supabase_connection_id?: string;
  agents: AgentDefinition[];
  node_providers?: NodeProviderDefinition[];
};

export type GraphDocument = GraphDefinition | TestEnvironmentDefinition;

export type ProviderConfigFieldDefinition = {
  key: string;
  label: string;
  input_type: string;
  help_text: string;
  placeholder: string;
  options?: ProviderConfigOptionDefinition[];
};

export type ProviderConfigOptionDefinition = {
  value: string;
  label: string;
};

export type NodeProviderDefinition = {
  provider_id: string;
  display_name: string;
  category: string;
  node_kind: string;
  description: string;
  capabilities: string[];
  produces_side_effects?: boolean;
  preserves_input_payload?: boolean;
  model_provider_name?: string | null;
  default_config?: Record<string, unknown>;
  config_fields?: ProviderConfigFieldDefinition[];
};

export type ToolDefinition = {
  name: string;
  canonical_name?: string;
  display_name?: string;
  aliases?: string[];
  description: string;
  input_schema: Record<string, unknown>;
  source_type?: string;
  capability_type?: string;
  server_id?: string | null;
  enabled?: boolean;
  available?: boolean;
  availability_error?: string;
  schema_origin?: string;
  schema_warning?: string;
  managed?: boolean;
};

export type McpTransport = "stdio" | "http";

export type McpServerConfig = {
  command?: string[];
  cwd?: string | null;
  env?: Record<string, string>;
  headers?: Record<string, string>;
  base_url?: string | null;
  timeout_seconds?: number | null;
};

export type McpServerDraft = {
  server_id: string;
  display_name: string;
  description: string;
  transport: McpTransport;
  command: string[];
  cwd?: string | null;
  env: Record<string, string>;
  headers: Record<string, string>;
  base_url?: string | null;
  timeout_seconds: number;
  auto_boot: boolean;
  persistent: boolean;
};

export type McpCapabilityDefinition = {
  canonical_name: string;
  name: string;
  display_name?: string;
  title?: string;
  aliases?: string[];
  capability_type: string;
  description: string;
  input_schema: Record<string, unknown>;
  metadata?: Record<string, unknown>;
  server_id: string;
  enabled?: boolean;
  available?: boolean;
  availability_error?: string;
  schema_origin?: string;
  schema_warning?: string;
  managed?: boolean;
};

export type McpServerTemplate = {
  template_id: string;
  display_name: string;
  description: string;
  draft: McpServerDraft;
  capability_hints?: string[];
  provenance?: Record<string, unknown>;
  source?: string;
};

export type McpServerStatus = {
  server_id: string;
  display_name: string;
  description: string;
  transport: McpTransport;
  auto_boot: boolean;
  persistent: boolean;
  source?: string;
  editable?: boolean;
  config?: McpServerConfig;
  config_summary?: string;
  running: boolean;
  tool_names: string[];
  error: string;
  pid?: number | null;
  booted_at?: string | null;
  declared_capabilities?: Record<string, unknown>;
  server_info?: Record<string, unknown>;
  capability_types?: string[];
  capability_count?: number;
};

export type McpServerTestResult = {
  ok: boolean;
  server: McpServerStatus;
  declared_capabilities?: Record<string, unknown>;
  server_info?: Record<string, unknown>;
  capability_types?: string[];
  capability_count?: number;
  capabilities: McpCapabilityDefinition[];
  tool_names: string[];
  tools: Array<Record<string, unknown>>;
  message: string;
};

export type StartRunOptions = {
  agent_ids?: string[];
  documents?: RunDocument[];
};

export type ConnectionRule = {
  source_category: string;
  target_category: string;
  rationale: string;
};

export type CategoryContract = {
  category: string;
  accepted_inputs: string[];
  produced_outputs: string[];
  description: string;
};

export type EditorCatalog = {
  node_providers: NodeProviderDefinition[];
  tools: ToolDefinition[];
  connection_rules: ConnectionRule[];
  contracts: Record<string, CategoryContract>;
  provider_statuses?: Record<string, ProviderPreflightResult>;
  microsoft_auth?: MicrosoftAuthStatus | null;
  mcp_servers?: McpServerStatus[];
  mcp_capabilities?: McpCapabilityDefinition[];
  mcp_server_templates?: McpServerTemplate[];
};

export type ProviderPreflightResult = {
  provider_name?: string;
  status: string;
  ok: boolean;
  message: string;
  warnings?: string[];
  details: Record<string, unknown>;
};

export type ProviderDiagnosticsResult = {
  provider_name: string;
  active_backend: string;
  claude_binary_exists: boolean;
  claude_binary_path?: string | null;
  anthropic_api_key_present: boolean;
  warning?: string | null;
  child_env_sanitized: boolean;
  sanitized_env_removed_vars: string[];
  authentication_status: string;
  preflight: ProviderPreflightResult;
};

export type SpreadsheetPreviewRow = {
  row_number: number;
  row_data: Record<string, unknown>;
};

export type SpreadsheetPreviewResult = {
  source_file: string;
  file_format: string;
  sheet_name?: string | null;
  sheet_names: string[];
  headers: string[];
  row_count: number;
  sample_rows: SpreadsheetPreviewRow[];
};

export type SupabaseSchemaColumn = {
  name: string;
  data_type: string;
  nullable: boolean;
  description: string;
};

export type SupabaseConnectionDefinition = {
  connection_id: string;
  name: string;
  supabase_url_env_var: string;
  supabase_key_env_var: string;
  project_ref_env_var: string;
  access_token_env_var: string;
};

export type SupabaseSchemaSource = {
  name: string;
  source_kind: string;
  columns: SupabaseSchemaColumn[];
  description: string;
};

export type SupabaseSchemaPreviewResult = {
  schema: string;
  source_count: number;
  sources: SupabaseSchemaSource[];
};

export type SupabaseRuntimeStatusResult = {
  supabase_url_env_var: string;
  supabase_key_env_var: string;
  supabase_url_env_present: boolean;
  supabase_key_env_present: boolean;
  missing_env_vars: string[];
  ready: boolean;
};

export type SupabaseSchemaTypeMismatch = {
  column_name: string;
  expected_types: string[];
  actual_type: string;
  required: boolean;
};

export type OutboundEmailLogTableValidationResult = {
  schema: string;
  table_name: string;
  configured: boolean;
  table_found: boolean;
  valid: boolean;
  available_columns: string[];
  missing_required_columns: string[];
  missing_optional_columns: string[];
  type_mismatches: SupabaseSchemaTypeMismatch[];
  warnings: string[];
};

export type SupabaseAuthVerificationResult = {
  schema: string;
  static_auth_valid: boolean;
  source_count: number;
  sources: SupabaseSchemaSource[];
  mcp_auth_checked: boolean;
  mcp_auth_valid: boolean;
  warnings: string[];
  mcp_server?: {
    server_name: string;
    server_version: string;
  };
};

export type MicrosoftAuthStatus = {
  status: string;
  connected: boolean;
  pending: boolean;
  client_id: string;
  tenant_id: string;
  account_username: string;
  request_id: string;
  user_code: string;
  verification_uri: string;
  verification_uri_complete: string;
  message: string;
  expires_at: string;
  connected_at: string;
  last_error: string;
  scopes: string[];
};

export type RunDocument = {
  document_id: string;
  name: string;
  mime_type: string;
  size_bytes: number;
  storage_path: string;
  text_content: string;
  text_excerpt: string;
  status: string;
  error?: string | null;
};

export type ProjectFile = {
  file_id: string;
  graph_id: string;
  name: string;
  mime_type: string;
  size_bytes: number;
  storage_path: string;
  status: string;
  created_at: string;
  error?: string | null;
};

export type RunFilesystemFile = {
  path: string;
  name: string;
  size_bytes: number;
  modified_at: string;
  mime_type: string;
  agent_id?: string | null;
  run_id?: string | null;
};

export type RunFilesystemListing = {
  requested_run_id: string;
  run_id: string;
  agent_id: string | null;
  workspace_root: string;
  files: RunFilesystemFile[];
};

export type RunFilesystemFileContent = RunFilesystemFile & {
  requested_run_id: string;
  content: string;
  truncated: boolean;
  encoding: string;
  workspace_path?: string;
};

export type RuntimeEvent = {
  schema_version: string;
  event_type: string;
  summary: string;
  payload: Record<string, unknown>;
  run_id: string;
  agent_id?: string | null;
  parent_run_id?: string | null;
  timestamp: string;
};

export type LoopRegionState = {
  iterator_node_id: string;
  iterator_type?: string | null;
  status?: string | null;
  current_row_index?: number | null;
  total_rows?: number | null;
  active_iteration_id?: string | null;
  member_node_ids: string[];
  iteration_ids?: string[];
  sheet_name?: string | null;
  source_file?: string | null;
  file_format?: string | null;
};

export type RunState = {
  run_id: string;
  graph_id: string;
  agent_id?: string | null;
  agent_name?: string | null;
  parent_run_id?: string | null;
  current_node_id: string | null;
  current_edge_id?: string | null;
  status: string;
  status_reason?: string | null;
  started_at: string | null;
  ended_at: string | null;
  runtime_instance_id?: string | null;
  last_heartbeat_at?: string | null;
  input_payload: unknown;
  documents?: RunDocument[];
  node_inputs?: Record<string, unknown>;
  node_outputs: Record<string, unknown>;
  edge_outputs?: Record<string, unknown>;
  node_errors: Record<string, unknown>;
  node_statuses?: Record<string, string>;
  iterator_states?: Record<string, Record<string, unknown>>;
  loop_regions?: Record<string, LoopRegionState>;
  visit_counts: Record<string, number>;
  transition_history: Array<Record<string, unknown>>;
  event_history: RuntimeEvent[];
  final_output: unknown;
  terminal_error: Record<string, unknown> | null;
  agent_runs?: Record<string, RunState>;
};

export function cloneGraphDefinition<T extends GraphDocument>(graph: T): T {
  return JSON.parse(JSON.stringify(graph)) as T;
}
