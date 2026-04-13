from __future__ import annotations

import os
from pathlib import Path
import sys

from graph_agent import config
from graph_agent.providers.claude_code import ClaudeCodeCLIModelProvider
from graph_agent.providers.discord import DiscordMessageSender
from graph_agent.providers.outlook import OutlookDraftClient
from graph_agent.providers.mock import MockModelProvider
from graph_agent.providers.vendor_api import ClaudeMessagesModelProvider, OpenAIChatModelProvider
from graph_agent.runtime.core import GraphDefinition, RuntimeServices
from graph_agent.runtime.microsoft_auth import MicrosoftAuthService
from graph_agent.runtime.node_providers import (
    NodeCategory,
    NodeProviderDefinition,
    NodeProviderRegistry,
    ProviderConfigOptionDefinition,
    ProviderConfigFieldDefinition,
)
from graph_agent.tools.base import ToolDefinition, ToolRegistry
from graph_agent.tools.example_tools import build_search_catalog_tool
from graph_agent.tools.mcp import McpServerDefinition, McpServerManager, canonical_mcp_tool_name


def _mcp_python_path_env() -> dict[str, str]:
    src_root = Path(__file__).resolve().parents[2]
    current_python_path = str(src_root)
    inherited_python_path = str(sys.path[0]).strip()
    env_python_path = os.environ.get("PYTHONPATH", "").strip()
    python_path_parts = [current_python_path]
    if inherited_python_path and inherited_python_path not in python_path_parts:
        python_path_parts.append(inherited_python_path)
    if env_python_path and env_python_path not in python_path_parts:
        python_path_parts.append(env_python_path)
    return {"PYTHONPATH": os.pathsep.join(python_path_parts)}


def _weather_mcp_server_definition() -> McpServerDefinition:
    return McpServerDefinition(
        server_id="weather_mcp",
        display_name="Weather MCP Server",
        description="Built-in weather MCP server backed by a live weather lookup tool.",
        command=[sys.executable, "-m", "graph_agent.tools.weather_mcp_server"],
        env=_mcp_python_path_env(),
        auto_boot=False,
        persistent=True,
        source="builtin",
    )


def _time_mcp_server_definition() -> McpServerDefinition:
    return McpServerDefinition(
        server_id="time_mcp",
        display_name="Time MCP Server",
        description="Built-in MCP server that returns the current local minute time.",
        command=[sys.executable, "-m", "graph_agent.tools.time_mcp_server"],
        env=_mcp_python_path_env(),
        auto_boot=False,
        persistent=True,
        source="builtin",
    )


def build_example_services(*, include_user_mcp_servers: bool = False) -> RuntimeServices:
    registry = ToolRegistry()
    registry.register(build_search_catalog_tool())
    registry.register(
        ToolDefinition(
            name=canonical_mcp_tool_name("weather_mcp", "weather_current"),
            display_name="weather_current",
            aliases=["weather_current"],
            description="Fetch the current weather conditions for a city or location string.",
            input_schema={
                "type": "object",
                "properties": {"location": {"type": "string"}},
                "required": ["location"],
            },
            source_type="mcp",
            capability_type="tool",
            server_id="weather_mcp",
            enabled=True,
            available=False,
            availability_error="MCP server is offline.",
            managed=True,
        )
    )
    registry.register(
        ToolDefinition(
            name=canonical_mcp_tool_name("time_mcp", "time_current_minute"),
            display_name="time_current_minute",
            aliases=["time_current_minute"],
            description="Return the current local time rounded down to the minute.",
            input_schema={
                "type": "object",
                "properties": {},
            },
            source_type="mcp",
            capability_type="tool",
            server_id="time_mcp",
            enabled=True,
            available=False,
            availability_error="MCP server is offline.",
            managed=True,
        )
    )
    mcp_server_manager = McpServerManager(registry)
    mcp_server_manager.register_server(_weather_mcp_server_definition())
    mcp_server_manager.register_server(_time_mcp_server_definition())
    if include_user_mcp_servers:
        mcp_server_manager.load_user_servers()
    node_providers = NodeProviderRegistry()
    node_providers.register(
        NodeProviderDefinition(
            provider_id="start.manual_run",
            display_name="Run Button Start",
            category=NodeCategory.START,
            node_kind="input",
            description="Starts a graph when the editor Run button is clicked and captures that payload.",
            capabilities=["run button trigger", "accepts external input"],
        )
    )
    node_providers.register(
        NodeProviderDefinition(
            provider_id="start.discord_message",
            display_name="Discord Message Start",
            category=NodeCategory.START,
            node_kind="input",
            description="Starts a graph from an incoming Discord channel message handled by the configured bot.",
            capabilities=["discord channel trigger", "bot event listener", "message payload capture"],
        )
    )
    node_providers.register(
        NodeProviderDefinition(
            provider_id="core.input",
            display_name="Core Input Node (Legacy)",
            category=NodeCategory.START,
            node_kind="input",
            description="Legacy alias for the manual run start node kept for backward compatibility.",
            capabilities=["legacy alias", "run button trigger"],
        )
    )
    node_providers.register(
        NodeProviderDefinition(
            provider_id="core.api",
            display_name="API Call Node",
            category=NodeCategory.API,
            node_kind="model",
            description="Runs a model-agnostic API call step and selects its concrete provider from node configuration.",
            capabilities=["schema proposal", "schema repair", "response composition"],
        )
    )
    node_providers.register(
        NodeProviderDefinition(
            provider_id="core.spreadsheet_matrix_decision",
            display_name="Spreadsheet Matrix Decision",
            category=NodeCategory.API,
            node_kind="model",
            description="Uses an LLM to choose the best matching first-column row and first-row column from a spreadsheet decision matrix, then emits the selected cell value.",
            capabilities=["spreadsheet matrix lookup", "llm-guided row selection", "llm-guided column selection"],
            model_provider_name="mock",
            default_config={
                "provider_name": "mock",
                "model": "mock-default",
                "mode": "spreadsheet_matrix_decision",
                "system_prompt": (
                    "Use the spreadsheet decision matrix to select the single best matching row and column for the user's request. "
                    "Infer the strongest fit from the full context, reconcile ambiguous titles using actual responsibilities and scope, "
                    "and avoid shallow keyword matching."
                ),
                "user_message_template": (
                    "Analyze the context below and choose the matrix cell that best matches the strongest underlying fit.\n\n"
                    "Pay close attention to what the subject is most likely to care about or respond to.\n"
                    "If a role title is broad, ambiguous, or prestige-coded, use the detailed responsibilities, tools, "
                    "and outcomes to interpret it more precisely.\n\n"
                    "Context:\n"
                    "{input_payload}"
                ),
                "response_mode": "message",
                "file_format": "auto",
                "file_path": "",
                "sheet_name": "",
            },
            config_fields=[
                ProviderConfigFieldDefinition(
                    key="file_format",
                    label="File Format",
                    input_type="select",
                    options=[
                        ProviderConfigOptionDefinition(value="auto", label="Auto Detect"),
                        ProviderConfigOptionDefinition(value="csv", label="CSV"),
                        ProviderConfigOptionDefinition(value="xlsx", label="Excel (.xlsx)"),
                    ],
                ),
                ProviderConfigFieldDefinition(
                    key="file_path",
                    label="File Path",
                    placeholder="/absolute/path/to/matrix.xlsx or {GRAPH_ENV_VAR}",
                ),
                ProviderConfigFieldDefinition(
                    key="sheet_name",
                    label="Sheet Name",
                    placeholder="Leave blank to use the first sheet",
                ),
            ],
        )
    )
    node_providers.register(
        NodeProviderDefinition(
            provider_id="provider.mock",
            display_name="Mock Provider",
            category=NodeCategory.PROVIDER,
            node_kind="provider",
            description="Provides mock-model settings to a generic API call node.",
            capabilities=["local test provider", "schema proposal", "response composition"],
            model_provider_name="mock",
            default_config={"provider_name": "mock", "model": "mock-default"},
            config_fields=[
                ProviderConfigFieldDefinition(
                    key="model",
                    label="Model",
                    input_type="select",
                    options=[
                        ProviderConfigOptionDefinition(value="mock-default", label="mock-default"),
                    ],
                ),
            ],
        )
    )
    node_providers.register(
        NodeProviderDefinition(
            provider_id="provider.openai",
            display_name="OpenAI Provider",
            category=NodeCategory.PROVIDER,
            node_kind="provider",
            description="Provides OpenAI chat-completions settings to a generic API call node.",
            capabilities=["structured output", "tool-schema generation", "response composition"],
            model_provider_name="openai",
            default_config={
                "provider_name": "openai",
                "model": "gpt-4.1-mini",
                "api_key_env_var": "OPENAI_API_KEY",
            },
            config_fields=[
                ProviderConfigFieldDefinition(
                    key="model",
                    label="Model",
                    input_type="select",
                    options=[
                        ProviderConfigOptionDefinition(value="gpt-4.1-mini", label="gpt-4.1-mini"),
                        ProviderConfigOptionDefinition(value="gpt-4.1", label="gpt-4.1"),
                        ProviderConfigOptionDefinition(value="gpt-4o-mini", label="gpt-4o-mini"),
                    ],
                ),
                ProviderConfigFieldDefinition(key="temperature", label="Temperature", input_type="number"),
                ProviderConfigFieldDefinition(key="max_tokens", label="Max Tokens", input_type="number"),
                ProviderConfigFieldDefinition(key="api_base", label="API Base"),
                ProviderConfigFieldDefinition(
                    key="api_key_env_var",
                    label="API Key Env Var",
                    placeholder="OPENAI_API_KEY",
                ),
            ],
        )
    )
    node_providers.register(
        NodeProviderDefinition(
            provider_id="provider.claude",
            display_name="Anthropic API Provider",
            category=NodeCategory.PROVIDER,
            node_kind="provider",
            description="Uses the Anthropic Messages API with an ANTHROPIC_API_KEY and pay-per-usage API billing.",
            capabilities=["Anthropic API key auth", "structured output", "tool-schema generation", "response composition"],
            model_provider_name="claude",
            default_config={
                "provider_name": "claude",
                "model": "claude-3-5-haiku-latest",
                "max_tokens": 1024,
                "api_key_env_var": "ANTHROPIC_API_KEY",
            },
            config_fields=[
                ProviderConfigFieldDefinition(
                    key="model",
                    label="Model",
                    input_type="select",
                    options=[
                        ProviderConfigOptionDefinition(value="claude-3-5-haiku-latest", label="claude-3-5-haiku-latest"),
                        ProviderConfigOptionDefinition(value="claude-3-7-sonnet-latest", label="claude-3-7-sonnet-latest"),
                    ],
                ),
                ProviderConfigFieldDefinition(key="temperature", label="Temperature", input_type="number"),
                ProviderConfigFieldDefinition(key="max_tokens", label="Max Tokens", input_type="number"),
                ProviderConfigFieldDefinition(key="api_base", label="API Base"),
                ProviderConfigFieldDefinition(
                    key="api_key_env_var",
                    label="API Key Env Var",
                    placeholder="ANTHROPIC_API_KEY",
                ),
            ],
        )
    )
    node_providers.register(
        NodeProviderDefinition(
            provider_id="provider.claude_code",
            display_name="Claude Code Provider",
            category=NodeCategory.PROVIDER,
            node_kind="provider",
            description="Delegates to the local Claude Code CLI authenticated on this machine and strips ANTHROPIC_API_KEY from child processes to preserve subscription-backed auth.",
            capabilities=["local Claude subscription", "sanitized child env", "structured output", "tool-schema generation"],
            model_provider_name="claude_code",
            default_config={
                "provider_name": "claude_code",
                "model": "sonnet",
                "cli_path": "claude",
                "timeout_seconds": 60,
                "max_turns": 2,
            },
            config_fields=[
                ProviderConfigFieldDefinition(
                    key="model",
                    label="Model",
                    input_type="select",
                    options=[
                        ProviderConfigOptionDefinition(value="haiku", label="haiku"),
                        ProviderConfigOptionDefinition(value="sonnet", label="sonnet"),
                        ProviderConfigOptionDefinition(value="opus", label="opus"),
                    ],
                ),
                ProviderConfigFieldDefinition(key="cli_path", label="Claude CLI Path", placeholder="claude"),
                ProviderConfigFieldDefinition(key="working_directory", label="Working Directory"),
                ProviderConfigFieldDefinition(key="timeout_seconds", label="Timeout Seconds", input_type="number"),
                ProviderConfigFieldDefinition(key="max_turns", label="Max Turns", input_type="number"),
            ],
        )
    )
    node_providers.register(
        NodeProviderDefinition(
            provider_id="tool.registry",
            display_name="Registry Tool Node",
            category=NodeCategory.TOOL,
            node_kind="tool",
            description="Dispatches a registered tool and surfaces validation failures as routable results.",
            capabilities=["tool dispatch", "schema validation"],
        )
    )
    node_providers.register(
        NodeProviderDefinition(
            provider_id="tool.mcp_context_provider",
            display_name="MCP Context Provider",
            category=NodeCategory.TOOL,
            node_kind="mcp_context_provider",
            description="Registers MCP tools for a connected API node, controls whether those tools are callable, and optionally injects MCP metadata into the runtime system prompt.",
            capabilities=["mcp tool registration", "callable tool exposure", "system prompt context"],
            default_config={"tool_names": [], "expose_mcp_tools": True, "include_mcp_tool_context": False},
        )
    )
    node_providers.register(
        NodeProviderDefinition(
            provider_id="tool.mcp_tool_executor",
            display_name="MCP Tool Executor",
            category=NodeCategory.TOOL,
            node_kind="mcp_tool_executor",
            description="Dispatches MCP tool calls and can optionally run a model-guided follow-up loop to decide whether another exposed MCP tool is needed or the work is complete.",
            capabilities=["mcp tool dispatch", "success routing", "failure routing", "follow-up tool decision"],
            default_config={
                "enable_follow_up_decision": False,
                "allow_retries": True,
                "provider_name": "claude_code",
                "prompt_name": "mcp_executor_follow_up",
                "mode": "mcp_executor_follow_up",
                "system_prompt": (
                    "Review the original request and tool history in the input payload. "
                    "If the last MCP tool call failed schema validation, repair it using the validation details in the input payload. "
                    "If the last MCP tool execution failed for another reason, do not request another tool call. "
                    "If more live MCP data is still required, call exactly one exposed MCP tool. "
                    "Otherwise return the final answer."
                ),
                "user_message_template": "{input_payload}",
                "response_mode": "auto",
                "validate_last_tool_success": True,
                "model": "sonnet",
                "cli_path": "claude",
                "timeout_seconds": 90,
                "max_turns": 3,
            },
        )
    )
    node_providers.register(
        NodeProviderDefinition(
            provider_id="core.data",
            display_name="Core Data Node",
            category=NodeCategory.DATA,
            node_kind="data",
            description="Applies deterministic passthrough or template transforms between execution steps.",
            capabilities=["passthrough transforms", "template transforms", "wire routing junctions"],
        )
    )
    node_providers.register(
        NodeProviderDefinition(
            provider_id="core.context_builder",
            display_name="Context Builder",
            category=NodeCategory.DATA,
            node_kind="data",
            description="Collects text from multiple upstream nodes and renders it into one reusable prompt or context block.",
            capabilities=["multi-input prompt composition", "named placeholders", "templated context assembly"],
            default_config={
                "mode": "context_builder",
                "template": "",
                "input_bindings": [],
                "joiner": "\n\n",
            },
        )
    )
    node_providers.register(
        NodeProviderDefinition(
            provider_id="core.prompt_block",
            display_name="Prompt Block",
            category=NodeCategory.DATA,
            node_kind="data",
            description="Defines a single system, user, or assistant prompt message as a reusable binding-only block.",
            capabilities=["single prompt authoring", "message role selection", "binding-only model context"],
            default_config={
                "mode": "prompt_block",
                "role": "user",
                "content": "",
                "name": "",
            },
            config_fields=[
                ProviderConfigFieldDefinition(
                    key="role",
                    label="Message Role",
                    input_type="select",
                    options=[
                        ProviderConfigOptionDefinition(value="system", label="system"),
                        ProviderConfigOptionDefinition(value="user", label="user"),
                        ProviderConfigOptionDefinition(value="assistant", label="assistant"),
                    ],
                ),
                ProviderConfigFieldDefinition(key="name", label="Message Name"),
                ProviderConfigFieldDefinition(key="content", label="Message Content", input_type="textarea"),
            ],
        )
    )
    node_providers.register(
        NodeProviderDefinition(
            provider_id="core.data_display",
            display_name="Envelope Display Node",
            category=NodeCategory.DATA,
            node_kind="data",
            description="Shows the exact incoming envelope in the visualizer while passing the original payload through unchanged.",
            capabilities=["envelope inspection", "visualizer display", "payload passthrough"],
            produces_side_effects=True,
            preserves_input_payload=True,
            default_config={"mode": "passthrough", "show_input_envelope": True, "lock_passthrough": True},
        )
    )
    node_providers.register(
        NodeProviderDefinition(
            provider_id="core.write_text_file",
            display_name="Write Text File",
            category=NodeCategory.DATA,
            node_kind="data",
            description="Writes the incoming payload into a sandboxed text file inside the active agent workspace for this run.",
            capabilities=["sandboxed file output", "text serialization", "agent workspace artifacts"],
            produces_side_effects=True,
            preserves_input_payload=True,
            default_config={
                "mode": "write_text_file",
                "relative_path": "response.txt",
            },
            config_fields=[
                ProviderConfigFieldDefinition(
                    key="relative_path",
                    label="Relative File Path",
                    help_text="Saved inside the agent workspace for this run. Nested folders like outputs/response.txt are allowed.",
                    placeholder="response.txt",
                ),
                ProviderConfigFieldDefinition(
                    key="exists_behavior",
                    label="When File Exists",
                    input_type="select",
                    help_text="Leave unset to overwrite outside loops and append inside spreadsheet/iterator loops.",
                    options=[
                        ProviderConfigOptionDefinition(value="overwrite", label="Overwrite"),
                        ProviderConfigOptionDefinition(value="append", label="Append"),
                        ProviderConfigOptionDefinition(value="error", label="Error"),
                    ],
                ),
            ],
        )
    )
    node_providers.register(
        NodeProviderDefinition(
            provider_id="core.apollo_email_lookup",
            display_name="Apollo Email Lookup",
            category=NodeCategory.DATA,
            node_kind="data",
            description="Looks up a person in Apollo, returns the full people/match response, and reuses a shared cache across runs.",
            capabilities=["apollo people match", "email lookup", "shared cache reuse"],
            default_config={
                "mode": "apollo_email_lookup",
                "input_binding": {"type": "input_payload"},
                "api_key_env_var": "APOLLO_API_KEY",
                "name": "",
                "domain": "",
                "organization_name": "",
                "first_name": "",
                "last_name": "",
                "linkedin_url": "",
                "email": "",
                "twitter_url": "",
                "conversation": "",
                "reveal_personal_emails": False,
                "use_cache": True,
                "force_refresh": False,
                "workspace_cache_path_template": "cache/apollo-email/{cache_key}.json",
            },
            config_fields=[
                ProviderConfigFieldDefinition(key="api_key_env_var", label="Apollo API Key Env Var"),
                ProviderConfigFieldDefinition(key="name", label="Name"),
                ProviderConfigFieldDefinition(key="domain", label="Domain"),
                ProviderConfigFieldDefinition(key="organization_name", label="Organization Name"),
                ProviderConfigFieldDefinition(key="first_name", label="First Name"),
                ProviderConfigFieldDefinition(key="last_name", label="Last Name"),
                ProviderConfigFieldDefinition(key="linkedin_url", label="LinkedIn URL"),
                ProviderConfigFieldDefinition(key="email", label="Email"),
                ProviderConfigFieldDefinition(key="twitter_url", label="Twitter URL"),
                ProviderConfigFieldDefinition(
                    key="conversation",
                    label="Optional Conversation",
                    input_type="textarea",
                    help_text="Optional operator context saved on the node for reference in the details UI.",
                ),
                ProviderConfigFieldDefinition(
                    key="reveal_personal_emails",
                    label="Reveal Personal Emails",
                    input_type="checkbox",
                ),
                ProviderConfigFieldDefinition(key="use_cache", label="Use Cache", input_type="checkbox"),
                ProviderConfigFieldDefinition(key="force_refresh", label="Force Refresh", input_type="checkbox"),
                ProviderConfigFieldDefinition(
                    key="workspace_cache_path_template",
                    label="Workspace Cache Path Template",
                ),
            ],
        )
    )
    node_providers.register(
        NodeProviderDefinition(
            provider_id="core.linkedin_profile_fetch",
            display_name="LinkedIn Profile Fetch",
            category=NodeCategory.DATA,
            node_kind="data",
            description="Fetches a LinkedIn profile page, parses the profile into structured JSON, and reuses a shared cache across runs.",
            capabilities=["linkedin profile fetch", "shared cache reuse", "agent workspace mirror"],
            default_config={
                "mode": "linkedin_profile_fetch",
                "input_binding": {"type": "input_payload"},
                "url_field": "url",
                "linkedin_data_dir": "/Users/morgannstuart/Desktop/Linkedin Data",
                "session_state_path": "",
                "headless": False,
                "navigation_timeout_ms": 45000,
                "page_settle_ms": 3000,
                "use_cache": True,
                "force_refresh": False,
                "workspace_cache_path_template": "cache/linkedin/{cache_key}.json",
            },
            config_fields=[
                ProviderConfigFieldDefinition(key="url_field", label="URL Field"),
                ProviderConfigFieldDefinition(key="linkedin_data_dir", label="LinkedIn Data Directory"),
                ProviderConfigFieldDefinition(key="session_state_path", label="Session State Path"),
                ProviderConfigFieldDefinition(key="headless", label="Headless", input_type="checkbox"),
                ProviderConfigFieldDefinition(
                    key="navigation_timeout_ms",
                    label="Navigation Timeout (ms)",
                    input_type="number",
                ),
                ProviderConfigFieldDefinition(
                    key="page_settle_ms",
                    label="Page Settle Delay (ms)",
                    input_type="number",
                ),
                ProviderConfigFieldDefinition(key="use_cache", label="Use Cache", input_type="checkbox"),
                ProviderConfigFieldDefinition(key="force_refresh", label="Force Refresh", input_type="checkbox"),
                ProviderConfigFieldDefinition(
                    key="workspace_cache_path_template",
                    label="Workspace Cache Path Template",
                ),
            ],
        )
    )
    node_providers.register(
        NodeProviderDefinition(
            provider_id="core.structured_payload_builder",
            display_name="Structured Payload Builder",
            category=NodeCategory.DATA,
            node_kind="data",
            description="Lets the user sketch a JSON object, then auto-fills missing fields from the incoming payload by matching field names.",
            capabilities=["json template merge", "auto-filled missing fields", "payload shaping"],
            default_config={
                "mode": "structured_payload_builder",
                "input_binding": {"type": "input_payload"},
                "template_json": "{\n  \"name\": \"\",\n  \"domain\": \"\",\n  \"linkedin_url\": \"\",\n  \"email\": \"\"\n}",
                "case_sensitive": False,
                "max_matches_per_field": 25,
            },
            config_fields=[
                ProviderConfigFieldDefinition(
                    key="template_json",
                    label="Template JSON",
                    input_type="textarea",
                    help_text="Provide the object shape you want. Any empty string, null, empty object, or empty array field is eligible for auto-fill from the incoming payload.",
                    placeholder='{\n  "name": "",\n  "domain": "",\n  "linkedin_url": "",\n  "email": ""\n}',
                ),
                ProviderConfigFieldDefinition(
                    key="case_sensitive",
                    label="Case Sensitive",
                    input_type="checkbox",
                ),
                ProviderConfigFieldDefinition(
                    key="max_matches_per_field",
                    label="Max Matches Per Field",
                    input_type="number",
                ),
            ],
        )
    )
    node_providers.register(
        NodeProviderDefinition(
            provider_id="core.supabase_data",
            display_name="Supabase Data Source",
            category=NodeCategory.DATA,
            node_kind="data",
            description="Loads deterministic data from a fixed Supabase table, view, or RPC and forwards it as a reusable data envelope.",
            capabilities=["supabase table reads", "supabase rpc calls", "deterministic context loading"],
            default_config={
                "mode": "supabase_data",
                "supabase_url_env_var": "GRAPH_AGENT_SUPABASE_URL",
                "supabase_key_env_var": "GRAPH_AGENT_SUPABASE_SECRET_KEY",
                "schema": "public",
                "source_kind": "table",
                "source_name": "",
                "select": "*",
                "filters_text": "",
                "order_by": "",
                "order_desc": False,
                "limit": 25,
                "single_row": False,
                "output_mode": "records",
                "rpc_params_json": "{}",
            },
            config_fields=[
                ProviderConfigFieldDefinition(key="schema", label="Schema"),
                ProviderConfigFieldDefinition(
                    key="source_kind",
                    label="Source Kind",
                    input_type="select",
                    options=[
                        ProviderConfigOptionDefinition(value="table", label="Table or View"),
                        ProviderConfigOptionDefinition(value="rpc", label="RPC"),
                    ],
                ),
                ProviderConfigFieldDefinition(key="source_name", label="Source Name"),
                ProviderConfigFieldDefinition(key="select", label="Select"),
                ProviderConfigFieldDefinition(
                    key="filters_text",
                    label="Filters",
                    input_type="textarea",
                    help_text="One PostgREST query parameter per line, for example status=eq.active.",
                    placeholder="status=eq.active\nteam_id=eq.123",
                ),
                ProviderConfigFieldDefinition(key="order_by", label="Order By"),
                ProviderConfigFieldDefinition(key="order_desc", label="Descending Order", input_type="checkbox"),
                ProviderConfigFieldDefinition(key="limit", label="Limit", input_type="number"),
                ProviderConfigFieldDefinition(key="single_row", label="Single Row", input_type="checkbox"),
                ProviderConfigFieldDefinition(
                    key="output_mode",
                    label="Output Mode",
                    input_type="select",
                    options=[
                        ProviderConfigOptionDefinition(value="records", label="Records"),
                        ProviderConfigOptionDefinition(value="markdown", label="Markdown"),
                    ],
                ),
                ProviderConfigFieldDefinition(
                    key="rpc_params_json",
                    label="RPC Params JSON",
                    input_type="textarea",
                    placeholder="{\n  \"project_id\": \"123\"\n}",
                ),
            ],
        )
    )
    node_providers.register(
        NodeProviderDefinition(
            provider_id="core.supabase_row_write",
            display_name="Supabase Row Write",
            category=NodeCategory.DATA,
            node_kind="data",
            description="Writes a single row to an existing Supabase table using a mix of runtime-bound values, literals, and omitted columns so database defaults can apply.",
            capabilities=["supabase inserts", "supabase upserts", "runtime column mapping", "database default preservation"],
            produces_side_effects=True,
            default_config={
                "mode": "supabase_row_write",
                "supabase_url_env_var": "GRAPH_AGENT_SUPABASE_URL",
                "supabase_key_env_var": "GRAPH_AGENT_SUPABASE_SECRET_KEY",
                "schema": "public",
                "table_name": "",
                "write_mode": "insert",
                "on_conflict": "",
                "ignore_duplicates": False,
                "returning": "representation",
                "base_row_json_path": "",
                "column_values_json": "{}",
            },
            config_fields=[
                ProviderConfigFieldDefinition(key="schema", label="Schema"),
                ProviderConfigFieldDefinition(key="table_name", label="Table Name"),
                ProviderConfigFieldDefinition(
                    key="write_mode",
                    label="Write Mode",
                    input_type="select",
                    options=[
                        ProviderConfigOptionDefinition(value="insert", label="Insert"),
                        ProviderConfigOptionDefinition(value="upsert", label="Upsert"),
                    ],
                ),
                ProviderConfigFieldDefinition(
                    key="on_conflict",
                    label="On Conflict Columns",
                    help_text="Optional comma-separated unique key columns for upserts, like id or workspace_id,slug.",
                    placeholder="id",
                ),
                ProviderConfigFieldDefinition(key="ignore_duplicates", label="Ignore Duplicate Upserts", input_type="checkbox"),
                ProviderConfigFieldDefinition(
                    key="returning",
                    label="Returning",
                    input_type="select",
                    options=[
                        ProviderConfigOptionDefinition(value="representation", label="Return Inserted Row"),
                        ProviderConfigOptionDefinition(value="minimal", label="Return Write Summary"),
                    ],
                ),
                ProviderConfigFieldDefinition(
                    key="base_row_json_path",
                    label="Base Row JSON Path",
                    help_text="Optional path inside the incoming payload to use as the starting row object. Use $ for the full payload.",
                    placeholder="$ or row_data",
                ),
                ProviderConfigFieldDefinition(
                    key="column_values_json",
                    label="Column Values JSON",
                    input_type="textarea",
                    help_text=(
                        "Map column names to literal values or specs like "
                        '{"email":{"mode":"path","path":"contact.email"},"status":{"mode":"literal","value":"pending"},"created_at":{"mode":"default"}}.'
                    ),
                    placeholder='{\n  "email": {"mode": "path", "path": "contact.email"},\n  "status": {"mode": "literal", "value": "pending"},\n  "created_at": {"mode": "default"}\n}',
                ),
            ],
        )
    )
    node_providers.register(
        NodeProviderDefinition(
            provider_id="core.outbound_email_logger",
            display_name="Outbound Email Logger",
            category=NodeCategory.DATA,
            node_kind="data",
            description="Attaches to an Outlook draft end node, validates the target Supabase table against the outbound email schema, and logs each created draft as a new row during runtime.",
            capabilities=["binding-only Outlook draft logging", "Supabase schema validation", "outbound email persistence"],
            produces_side_effects=True,
            default_config={
                "mode": "outbound_email_logger",
                "supabase_url_env_var": "GRAPH_AGENT_SUPABASE_URL",
                "supabase_key_env_var": "GRAPH_AGENT_SUPABASE_SECRET_KEY",
                "schema": "public",
                "table_name": "outbound_email_messages",
                "message_type": "initial",
                "outreach_step": 0,
                "sales_approach": "",
                "sales_approach_version": "",
                "parent_outbound_email_id": "",
                "root_outbound_email_id": "",
                "metadata_json": "{}",
            },
            config_fields=[
                ProviderConfigFieldDefinition(key="schema", label="Schema"),
                ProviderConfigFieldDefinition(key="table_name", label="Table Name"),
                ProviderConfigFieldDefinition(
                    key="message_type",
                    label="Message Type",
                    input_type="select",
                    options=[
                        ProviderConfigOptionDefinition(value="initial", label="Initial Outreach"),
                        ProviderConfigOptionDefinition(value="follow_up", label="Follow Up"),
                    ],
                ),
                ProviderConfigFieldDefinition(key="outreach_step", label="Outreach Step", input_type="number"),
                ProviderConfigFieldDefinition(
                    key="sales_approach",
                    label="Sales Approach",
                    help_text="Supports graph env refs and runtime template variables like {run_id} and {graph_id}.",
                    placeholder="value-led personalization",
                ),
                ProviderConfigFieldDefinition(
                    key="sales_approach_version",
                    label="Sales Approach Version",
                    placeholder="v1",
                ),
                ProviderConfigFieldDefinition(
                    key="parent_outbound_email_id",
                    label="Parent Outbound Email Id",
                    help_text="Optional template for follow-up chains.",
                    placeholder="{input_payload[parent_outbound_email_id]}",
                ),
                ProviderConfigFieldDefinition(
                    key="root_outbound_email_id",
                    label="Root Outbound Email Id",
                    help_text="Optional template for grouping a full outreach chain.",
                    placeholder="{input_payload[root_outbound_email_id]}",
                ),
                ProviderConfigFieldDefinition(
                    key="metadata_json",
                    label="Metadata JSON",
                    input_type="textarea",
                    help_text="Optional JSON object merged into the stored metadata after template rendering.",
                    placeholder='{\n  "campaign_id": "spring-launch",\n  "run_id": "{run_id}"\n}',
                ),
            ],
        )
    )
    node_providers.register(
        NodeProviderDefinition(
            provider_id="core.runtime_normalizer",
            display_name="Payload Field Extractor",
            category=NodeCategory.DATA,
            node_kind="data",
            description="Finds one or more named fields inside an incoming payload with unknown structure and forwards the matched value or values.",
            capabilities=["recursive field lookup", "unknown payload extraction", "value isolation", "multi-field extraction"],
            default_config={
                "mode": "runtime_normalizer",
                "input_binding": {"type": "input_payload"},
                "field_name": "url",
                "fallback_field_names": [],
                "preferred_path": "",
                "case_sensitive": False,
                "max_matches": 25,
            },
            config_fields=[
                ProviderConfigFieldDefinition(
                    key="field_name",
                    label="Field Names",
                    input_type="textarea",
                    help_text="One or more keys to look for anywhere in the incoming payload. Separate multiple field names with commas or new lines.",
                    placeholder="url\nheadline",
                ),
                ProviderConfigFieldDefinition(
                    key="fallback_field_names",
                    label="Fallback Field Names",
                    input_type="textarea",
                    help_text="Optional alternate field names to try if a single requested field is not found.",
                    placeholder="profile_url\nlinkedin_url",
                ),
                ProviderConfigFieldDefinition(
                    key="preferred_path",
                    label="Preferred Path",
                    help_text="Optional exact dot path to try first before recursive key search, like data.user.url.",
                ),
                ProviderConfigFieldDefinition(
                    key="case_sensitive",
                    label="Case Sensitive",
                    input_type="checkbox",
                ),
                ProviderConfigFieldDefinition(
                    key="max_matches",
                    label="Max Matches",
                    input_type="number",
                ),
            ],
        )
    )
    node_providers.register(
        NodeProviderDefinition(
            provider_id="core.spreadsheet_rows",
            display_name="Spreadsheet Rows",
            category=NodeCategory.CONTROL_FLOW_UNIT,
            node_kind="control_flow_unit",
            description="Reads a CSV or XLSX file, normalizes each row into a header-keyed dictionary, and iterates rows sequentially through downstream execution steps.",
            capabilities=["csv parsing", "xlsx parsing", "header normalization", "sequential row iteration"],
            default_config={
                "mode": "spreadsheet_rows",
                "file_format": "auto",
                "file_path": "",
                "sheet_name": "",
                "header_row_index": 1,
                "start_row_index": 2,
                "empty_row_policy": "skip",
            },
            config_fields=[
                ProviderConfigFieldDefinition(
                    key="file_format",
                    label="File Format",
                    input_type="select",
                    options=[
                        ProviderConfigOptionDefinition(value="auto", label="Auto Detect"),
                        ProviderConfigOptionDefinition(value="csv", label="CSV"),
                        ProviderConfigOptionDefinition(value="xlsx", label="Excel (.xlsx)"),
                    ],
                ),
                ProviderConfigFieldDefinition(
                    key="file_path",
                    label="File Path",
                    placeholder="/absolute/path/to/file.xlsx or {GRAPH_ENV_VAR}",
                ),
                ProviderConfigFieldDefinition(
                    key="sheet_name",
                    label="Sheet Name",
                    placeholder="Leave blank to use the first sheet",
                ),
                ProviderConfigFieldDefinition(
                    key="header_row_index",
                    label="Header Row",
                    input_type="number",
                    placeholder="1",
                ),
                ProviderConfigFieldDefinition(
                    key="start_row_index",
                    label="First Data Row",
                    input_type="number",
                    placeholder="2",
                ),
                ProviderConfigFieldDefinition(
                    key="empty_row_policy",
                    label="Empty Row Policy",
                    input_type="select",
                    options=[
                        ProviderConfigOptionDefinition(value="skip", label="Skip Empty Rows"),
                        ProviderConfigOptionDefinition(value="include", label="Include Empty Rows"),
                    ],
                ),
            ],
        )
    )
    node_providers.register(
        NodeProviderDefinition(
            provider_id="core.logic_conditions",
            display_name="Logic Conditions",
            category=NodeCategory.CONTROL_FLOW_UNIT,
            node_kind="control_flow_unit",
            description="Evaluates the incoming envelope against ordered clause rules and routes execution into an explicit if or else branch.",
            capabilities=["envelope inspection", "contract-aware branching", "conditional routing"],
            default_config={
                "mode": "logic_conditions",
                "clauses": [
                    {
                        "id": "if",
                        "label": "If",
                        "path": "",
                        "operator": "equals",
                        "value": "",
                        "source_contracts": [],
                        "output_handle_id": "control-flow-if",
                    }
                ],
                "else_output_handle_id": "control-flow-else",
            },
        )
    )
    node_providers.register(
        NodeProviderDefinition(
            provider_id="core.parallel_splitter",
            display_name="Parallel Splitter",
            category=NodeCategory.CONTROL_FLOW_UNIT,
            node_kind="control_flow_unit",
            description="Duplicates the incoming envelope across every connected downstream branch so they can run in parallel from one explicit fan-out step.",
            capabilities=["parallel branch fan-out", "shared envelope forwarding", "explicit splitter node"],
            default_config={
                "mode": "parallel_splitter",
            "output_handle_count": 4,
            },
        )
    )
    node_providers.register(
        NodeProviderDefinition(
            provider_id="end.agent_run",
            display_name="End Agent Run",
            category=NodeCategory.END,
            node_kind="output",
            description="Stops the current run immediately when reached, even from inside iterator executions.",
            capabilities=["immediate termination", "iterator halt", "terminal output"],
        )
    )
    node_providers.register(
        NodeProviderDefinition(
            provider_id="core.output",
            display_name="Core Output Node",
            category=NodeCategory.END,
            node_kind="output",
            description="Returns the terminal response for the run.",
            capabilities=["final output"],
        )
    )
    node_providers.register(
        NodeProviderDefinition(
            provider_id="end.discord_message",
            display_name="Discord Message End",
            category=NodeCategory.END,
            node_kind="output",
            description="Sends the resolved output to a designated Discord channel without replacing the canonical run final output.",
            capabilities=["discord delivery", "side-effect output"],
            produces_side_effects=True,
            default_config={
                "discord_bot_token_env_var": "{DISCORD_BOT_TOKEN}",
                "discord_channel_id": "",
                "message_template": "{message_payload}",
            },
            config_fields=[
                ProviderConfigFieldDefinition(
                    key="discord_bot_token_env_var",
                    label="Discord Bot Token Env Var",
                    placeholder="{DISCORD_BOT_TOKEN}",
                ),
                ProviderConfigFieldDefinition(
                    key="discord_channel_id",
                    label="Discord Channel ID",
                    placeholder="123456789012345678",
                ),
                ProviderConfigFieldDefinition(
                    key="message_template",
                    label="Message Template",
                    help_text="Optional template. Use {message_payload} or {message_json} to format the Discord message.",
                    input_type="textarea",
                    placeholder="{message_payload}",
                ),
            ],
        )
    )
    node_providers.register(
        NodeProviderDefinition(
            provider_id="end.outlook_draft",
            display_name="Outlook Draft End",
            category=NodeCategory.END,
            node_kind="output",
            description="Creates a draft email in Outlook using Microsoft Graph and never sends it automatically.",
            capabilities=["outlook draft creation", "email drafting", "side-effect output"],
            produces_side_effects=True,
            default_config={
                "to": "",
                "subject": "",
                "require_to": True,
                "require_subject": True,
                "require_body": True,
            },
            config_fields=[
                ProviderConfigFieldDefinition(
                    key="to",
                    label="To",
                    placeholder="person@example.com, teammate@example.com",
                ),
                ProviderConfigFieldDefinition(
                    key="subject",
                    label="Subject",
                    placeholder="Draft subject",
                ),
            ],
        )
    )
    return RuntimeServices(
        model_providers={
            "claude": ClaudeMessagesModelProvider(),
            "claude_code": ClaudeCodeCLIModelProvider(),
            "mock": MockModelProvider(),
            "openai": OpenAIChatModelProvider(),
        },
        node_provider_registry=node_providers,
        tool_registry=registry,
        mcp_server_manager=mcp_server_manager,
        discord_message_sender=DiscordMessageSender(),
        outlook_draft_client=OutlookDraftClient(),
        microsoft_auth_service=MicrosoftAuthService(),
        config={
            "max_steps": config.DEFAULT_RUN_MAX_STEPS,
            "max_visits_per_node": config.DEFAULT_MAX_VISITS_PER_NODE,
            "max_repair_attempts": config.DEFAULT_MAX_REPAIR_ATTEMPTS,
        },
    )


def build_example_graph_payload() -> dict[str, object]:
    return {
        "graph_id": config.DEFAULT_GRAPH_ID,
        "name": "Tool Schema Repair Example",
        "description": "Demonstrates a model -> tool -> repair -> model loop using the shared graph envelope contract.",
        "version": "1.0",
        "start_node_id": "start",
        "nodes": [
            {
                "id": "start",
                "kind": "input",
                "category": "start",
                "label": "Start Input",
                "provider_id": "start.manual_run",
                "provider_label": "Run Button Start",
                "description": "Captures the starting payload for a graph run.",
                "position": {"x": 120, "y": 120},
                "config": {
                    "input_binding": {"type": "input_payload"},
                },
            },
            {
                "id": "propose_tool",
                "kind": "model",
                "category": "api",
                "label": "Propose Tool Payload",
                "provider_id": "core.api",
                "provider_label": "API Call Node",
                "description": "Creates the first tool payload attempt and routes it to the tool node.",
                "model_provider_name": "mock",
                "prompt_name": "schema_proposal",
                "position": {"x": 420, "y": 120},
                "config": {
                    "provider_name": "mock",
                    "model": "mock-default",
                    "prompt_name": "schema_proposal",
                    "mode": "schema_proposal",
                    "system_prompt": config.SCHEMA_PROPOSAL_PROMPT,
                    "user_message_template": "Request: {user_request}\\nAvailable tools: {available_tools}",
                    "response_mode": "tool_call",
                    "preferred_tool_name": "search_catalog",
                    "allowed_tool_names": ["search_catalog"],
                    "metadata_bindings": {
                        "user_request": {"type": "latest_payload", "source": "start"},
                    },
                },
            },
            {
                "id": "run_tool",
                "kind": "tool",
                "category": "tool",
                "label": "Run Tool",
                "provider_id": "tool.registry",
                "provider_label": "Registry Tool Node",
                "description": "Validates and runs the selected tool.",
                "tool_name": "search_catalog",
                "position": {"x": 760, "y": 300},
                "config": {
                    "tool_name": "search_catalog",
                    "input_binding": {
                        "type": "first_available_envelope",
                        "sources": ["repair_tool", "propose_tool"],
                    },
                },
            },
            {
                "id": "repair_tool",
                "kind": "model",
                "category": "api",
                "label": "Repair Tool Payload",
                "provider_id": "core.api",
                "provider_label": "API Call Node",
                "description": "Repairs the tool payload after validation failure.",
                "model_provider_name": "mock",
                "prompt_name": "schema_repair",
                "position": {"x": 420, "y": 420},
                "config": {
                    "provider_name": "mock",
                    "model": "mock-default",
                    "prompt_name": "schema_repair",
                    "mode": "schema_repair",
                    "system_prompt": config.SCHEMA_REPAIR_PROMPT,
                    "user_message_template": "Request: {user_request}\\nValidation error: {tool_error}\\nAvailable tools: {available_tools}",
                    "response_mode": "tool_call",
                    "preferred_tool_name": "search_catalog",
                    "allowed_tool_names": ["search_catalog"],
                    "metadata_bindings": {
                        "user_request": {"type": "latest_payload", "source": "start"},
                        "tool_error": {"type": "latest_error", "source": "run_tool"},
                    },
                },
            },
            {
                "id": "compose_response",
                "kind": "model",
                "category": "api",
                "label": "Compose Final Response",
                "provider_id": "core.api",
                "provider_label": "API Call Node",
                "description": "Builds the response shown to the user.",
                "model_provider_name": "mock",
                "prompt_name": "final_response",
                "position": {"x": 1080, "y": 120},
                "config": {
                    "provider_name": "mock",
                    "model": "mock-default",
                    "prompt_name": "final_response",
                    "mode": "final_response",
                    "system_prompt": config.FINAL_RESPONSE_PROMPT,
                    "user_message_template": "Request: {user_request}\\nTool result: {tool_result}",
                    "response_mode": "message",
                    "metadata_bindings": {
                        "user_request": {"type": "latest_payload", "source": "start"},
                        "tool_result": {"type": "latest_payload", "source": "run_tool"},
                    },
                },
            },
            {
                "id": "finish",
                "kind": "output",
                "category": "end",
                "label": "Output Response",
                "provider_id": "core.output",
                "provider_label": "Core Output Node",
                "description": "Returns the final response payload.",
                "position": {"x": 1380, "y": 120},
                "config": {
                    "source_binding": {"type": "latest_envelope", "source": "compose_response"},
                },
            },
        ],
        "edges": [
            {
                "id": "edge-start-propose",
                "source_id": "start",
                "target_id": "propose_tool",
                "label": "begin",
                "kind": "standard",
                "priority": 100,
                "condition": None,
            },
            {
                "id": "edge-propose-run",
                "source_id": "propose_tool",
                "target_id": "run_tool",
                "label": "try tool",
                "kind": "standard",
                "priority": 100,
                "condition": None,
            },
            {
                "id": "edge-propose-repair",
                "source_id": "propose_tool",
                "target_id": "repair_tool",
                "label": "repair invalid proposal",
                "kind": "conditional",
                "priority": 10,
                "condition": {
                    "id": "proposal_validation_failed",
                    "label": "Proposal validation failed",
                    "type": "result_status_equals",
                    "value": "validation_error",
                },
            },
            {
                "id": "edge-run-repair",
                "source_id": "run_tool",
                "target_id": "repair_tool",
                "label": "repair invalid schema",
                "kind": "conditional",
                "priority": 10,
                "condition": {
                    "id": "tool_validation_failed",
                    "label": "Tool validation failed",
                    "type": "result_status_equals",
                    "value": "validation_error",
                },
            },
            {
                "id": "edge-run-compose",
                "source_id": "run_tool",
                "target_id": "compose_response",
                "label": "tool succeeded",
                "kind": "standard",
                "priority": 100,
                "condition": None,
            },
            {
                "id": "edge-repair-run",
                "source_id": "repair_tool",
                "target_id": "run_tool",
                "label": "retry tool",
                "kind": "standard",
                "priority": 100,
                "condition": None,
            },
            {
                "id": "edge-compose-finish",
                "source_id": "compose_response",
                "target_id": "finish",
                "label": "finalize",
                "kind": "standard",
                "priority": 100,
                "condition": None,
            },
        ],
    }


def build_auto_branching_graph_payload() -> dict[str, object]:
    return {
        "graph_id": "auto-branching-example",
        "name": "Auto Branching Example",
        "description": "Shows an auto-mode API node routing tool calls to an MCP executor and message responses to a downstream output node.",
        "version": "1.0",
        "start_node_id": "start",
        "nodes": [
            {
                "id": "start",
                "kind": "input",
                "category": "start",
                "label": "Start Input",
                "provider_id": "start.manual_run",
                "provider_label": "Run Button Start",
                "description": "Captures the starting payload for a graph run.",
                "position": {"x": 120, "y": 120},
                "config": {"input_binding": {"type": "input_payload"}},
            },
            {
                "id": "weather_context",
                "kind": "mcp_context_provider",
                "category": "tool",
                "label": "Weather Context",
                "provider_id": "tool.mcp_context_provider",
                "provider_label": "MCP Context Provider",
                "description": "Exposes the MCP weather tool to the auto-routing model node.",
                "position": {"x": 340, "y": 280},
                "config": {
                    "tool_names": ["weather_current"],
                    "expose_mcp_tools": True,
                    "include_mcp_tool_context": False,
                },
            },
            {
                "id": "model",
                "kind": "model",
                "category": "api",
                "label": "Auto Route Response",
                "provider_id": "core.api",
                "provider_label": "API Call Node",
                "description": "Emits either a message envelope or a tool-call envelope depending on normalized provider output.",
                "model_provider_name": "mock",
                "prompt_name": "auto_route",
                "position": {"x": 420, "y": 120},
                "config": {
                    "provider_name": "mock",
                    "model": "mock-default",
                    "prompt_name": "auto_route",
                    "mode": "auto_route",
                    "system_prompt": "Answer directly when possible or call the MCP weather tool when live data is needed.",
                    "user_message_template": "{input_payload}",
                    "response_mode": "auto",
                    "allowed_tool_names": ["weather_current"],
                },
            },
            {
                "id": "executor",
                "kind": "mcp_tool_executor",
                "category": "tool",
                "label": "Run MCP Tool",
                "provider_id": "tool.mcp_tool_executor",
                "provider_label": "MCP Tool Executor",
                "description": "Dispatches tool-call envelopes coming from the auto-routing model node.",
                "position": {"x": 780, "y": 120},
                "config": {},
            },
            {
                "id": "finish",
                "kind": "output",
                "category": "end",
                "label": "Output Response",
                "provider_id": "core.output",
                "provider_label": "Core Output Node",
                "description": "Returns either the model message payload or the MCP tool result payload.",
                "position": {"x": 1080, "y": 120},
                "config": {
                    "source_binding": {
                        "type": "first_available_payload",
                        "sources": ["executor", "model"],
                    }
                },
            },
        ],
        "edges": [
            {
                "id": "edge-start-model",
                "source_id": "start",
                "target_id": "model",
                "label": "begin",
                "kind": "standard",
                "priority": 100,
                "condition": None,
            },
            {
                "id": "edge-weather-binding",
                "source_id": "weather_context",
                "target_id": "model",
                "source_handle_id": "tool-context",
                "target_handle_id": "api-tool-context",
                "label": "tool context",
                "kind": "binding",
                "priority": 0,
                "condition": None,
            },
            {
                "id": "edge-model-executor",
                "source_id": "model",
                "target_id": "executor",
                "label": "dispatch tool call",
                "source_handle_id": "api-tool-call",
                "kind": "conditional",
                "priority": 10,
                "condition": {
                    "id": "model_emitted_tool_call",
                    "label": "Model emitted tool call envelope",
                    "type": "result_payload_path_equals",
                    "path": "metadata.contract",
                    "value": "tool_call_envelope",
                },
            },
            {
                "id": "edge-model-finish",
                "source_id": "model",
                "target_id": "finish",
                "source_handle_id": "api-message",
                "label": "deliver message",
                "kind": "conditional",
                "priority": 20,
                "condition": {
                    "id": "model_emitted_message",
                    "label": "Model emitted message envelope",
                    "type": "result_payload_path_equals",
                    "path": "metadata.contract",
                    "value": "message_envelope",
                },
            },
            {
                "id": "edge-executor-finish",
                "source_id": "executor",
                "target_id": "finish",
                "label": "deliver tool result",
                "kind": "standard",
                "priority": 100,
                "condition": None,
            },
        ],
    }


def build_example_graph() -> GraphDefinition:
    return GraphDefinition.from_dict(build_example_graph_payload())
