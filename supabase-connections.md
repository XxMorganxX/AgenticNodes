# Supabase Connections

This document explains exactly how Graph Agent Studio's Supabase connection system works after the multi-project update.

## What Problem This Solves

Before this system, Supabase-backed nodes effectively assumed one shared project:

- `GRAPH_AGENT_SUPABASE_URL`
- `GRAPH_AGENT_SUPABASE_SECRET_KEY`

That made it awkward to run one graph against multiple Supabase projects in the same execution.

The new system separates:

- connection metadata, stored on the graph document
- connection values, stored in graph env vars
- node bindings, stored per node as a stable connection id

That lets one graph read from one Supabase project, write to another, and still preserve backward compatibility with older graphs.

## The Data Model

Two new optional root-level fields exist on both `graph` and `test_environment` documents:

```json
{
  "supabase_connections": [
    {
      "connection_id": "analytics-db",
      "name": "Analytics DB",
      "supabase_url_env_var": "GRAPH_AGENT_SUPABASE_ANALYTICS_URL",
      "supabase_key_env_var": "GRAPH_AGENT_SUPABASE_ANALYTICS_SECRET_KEY",
      "project_ref_env_var": "SUPABASE_ANALYTICS_PROJECT_REF",
      "access_token_env_var": "SUPABASE_ANALYTICS_ACCESS_TOKEN"
    }
  ],
  "default_supabase_connection_id": "analytics-db"
}
```

Each `SupabaseConnectionDefinition` has these fields:

- `connection_id`: the stable identifier that nodes bind to
- `name`: the user-facing label shown in the editor
- `supabase_url_env_var`: the env-var key that stores the Supabase base URL
- `supabase_key_env_var`: the env-var key that stores the Supabase secret key or legacy `service_role` key
- `project_ref_env_var`: the env-var key that stores the Supabase project ref
- `access_token_env_var`: the env-var key that stores the optional hosted MCP access token

The graph also still has `env_vars`, which store the actual values for those keys.

## What Lives Where

There are three separate layers:

### 1. Graph document metadata

Stored in:

- `supabase_connections`
- `default_supabase_connection_id`

This is the stable registry of named connections.

### 2. Graph env vars

Stored in:

- `graph.env_vars`

This is where the actual URL, key, project ref, and optional access token live.

Example:

```json
{
  "env_vars": {
    "GRAPH_AGENT_SUPABASE_ANALYTICS_URL": "https://abc123.supabase.co",
    "GRAPH_AGENT_SUPABASE_ANALYTICS_SECRET_KEY": "sb_secret_...",
    "SUPABASE_ANALYTICS_PROJECT_REF": "abc123",
    "SUPABASE_ANALYTICS_ACCESS_TOKEN": ""
  }
}
```

### 3. Node config

Supabase-capable nodes can now store:

```json
{
  "supabase_connection_id": "analytics-db"
}
```

That means the node is bound to a named connection, not hard-coded URL/key env-var names.

## Which Nodes Use This

The named connection system applies to:

- `core.supabase_data`
- `core.supabase_row_write`
- `core.outbound_email_logger`

The runtime validates that any `supabase_connection_id` used by those nodes exists on the graph.

## Exact Runtime Resolution Rules

At runtime, Supabase nodes resolve credentials with this precedence:

1. If `node.config.supabase_connection_id` is present and non-empty:
   - the runtime looks up that id in `graph.supabase_connections`
   - if found, it uses that connection's `supabase_url_env_var` and `supabase_key_env_var`
   - if not found, execution fails with `missing_supabase_connection`
2. If `node.config.supabase_connection_id` is absent:
   - the runtime falls back to the legacy per-node fields:
     - `supabase_url_env_var`
     - `supabase_key_env_var`
   - if those are also absent, it falls back to:
     - `GRAPH_AGENT_SUPABASE_URL`
     - `GRAPH_AGENT_SUPABASE_SECRET_KEY`

That behavior is implemented by `resolve_supabase_runtime_env_var_names(...)`.

This is intentional:

- named connections are the new primary path
- raw env-var names remain the compatibility path
- there is no silent fallback from a missing named connection to another project

If a named connection is missing, the run should fail rather than risk writing to the wrong database.

## How Actual Credential Values Are Resolved

After the runtime knows which env-var names to use, it resolves their values with this process:

1. Treat the selected string as an env-var name
2. Check the process environment for that env var
3. If the process env does not provide a value, check `graph.env_vars`
4. Return the first non-empty value

This means a connection can be backed by:

- a process-level environment variable
- a graph-stored value in `graph.env_vars`

Process env wins if both exist.

This resolution is handled by `resolve_graph_process_env(...)`.

## How New Connections Are Created

When you create a connection in the editor, the system generates:

- a stable `connection_id`
- stable env-var names derived from the initial name

Example for `Analytics DB`:

- `connection_id`: `supabase-analytics-db`
- `supabase_url_env_var`: `GRAPH_AGENT_SUPABASE_ANALYTICS_DB_URL`
- `supabase_key_env_var`: `GRAPH_AGENT_SUPABASE_ANALYTICS_DB_SECRET_KEY`
- `project_ref_env_var`: `SUPABASE_ANALYTICS_DB_PROJECT_REF`
- `access_token_env_var`: `SUPABASE_ANALYTICS_DB_ACCESS_TOKEN`

Important detail:

- these generated env-var keys do not change when you rename the connection later

That is what keeps existing node bindings stable over time.

## Default Connection for New Nodes

The graph can optionally set:

- `default_supabase_connection_id`

When a new Supabase-capable node is created, the frontend checks that field. If it is non-empty, the new node starts with:

```json
{
  "supabase_connection_id": "<default id>"
}
```

This only affects new nodes. Existing nodes keep whatever binding they already had.

## Legacy Compatibility Mode

Older graphs may not have `supabase_connections` at all. They may only rely on:

- `GRAPH_AGENT_SUPABASE_URL`
- `GRAPH_AGENT_SUPABASE_SECRET_KEY`

The frontend preserves this in two ways.

### Implicit legacy connection

If the graph does not have an explicit named connection for the legacy default pair, but either:

- the legacy env vars already contain values, or
- a node still uses the legacy default env vars

the UI creates an implicit derived connection:

- `connection_id`: `__legacy_default_supabase__`
- `name`: `Default Supabase`

This implicit connection exists only as a compatibility layer in the editor until the user saves it explicitly.

### Compatibility mode on nodes

If a node has no `supabase_connection_id`, the UI shows it as:

- `Compatibility mode (raw env vars)`

That means:

- the node still runs
- the node still uses its stored raw env-var names
- the editor does not silently rewrite it to a named connection

This avoids forced migrations.

## What Happens When You Save a Legacy Connection

If the user opens the connection manager and saves the implicit legacy connection, the frontend materializes it into a real graph connection.

It becomes a normal explicit registry entry using the legacy env-var names:

- `GRAPH_AGENT_SUPABASE_URL`
- `GRAPH_AGENT_SUPABASE_SECRET_KEY`
- `SUPABASE_PROJECT_REF`
- `SUPABASE_ACCESS_TOKEN`

From that point on, it is persisted in `supabase_connections`.

## Delete Rules

Connections cannot be deleted if a node still references their `connection_id`.

The editor checks the graph and blocks deletion when the connection id is still in use.

If the deleted connection was also the graph default:

- the first remaining connection becomes the new default, or
- the default is cleared if none remain

## Project Ref and Access Token

Each connection carries two additional env-var keys:

- `project_ref_env_var`
- `access_token_env_var`

Their role is different from the URL/key pair:

- `project_ref_env_var` supports hosted Supabase MCP tooling
- `access_token_env_var` stores the optional hosted MCP access token

In the connection modal:

- `project_ref` is derived from the URL on save
- `access_token` is optional

The core read/write runtime currently needs the URL and secret key. The project ref and access token mainly support verification and hosted MCP-related workflows.

## Verification and Schema Browsing

The editor uses the selected connection to verify Supabase access and to preview schema metadata.

Important behavior:

- the schema browser resolves the node's named connection before fetching schema data
- if the connection id is missing, the browser is locked and shows an error instead of guessing
- cached schema results are scoped by:
  - graph id
  - connection identity
  - schema name

This prevents one Supabase project's cached schema from overwriting another project's cached schema in the same browser session.

## Test Environments

For `test_environment` documents:

- `supabase_connections` lives at the document root
- `default_supabase_connection_id` also lives at the document root
- every agent graph receives the shared registry during validation and runtime graph construction

That means all agents in the same test environment can bind to the same shared connection registry.

## REST Auth Behavior

Supabase REST requests now build auth headers this way:

- always send `apikey: <key>`
- if the key does not start with `sb_secret_`, also send:
  - `Authorization: Bearer <key>`
- if the key starts with `sb_secret_`, omit the `Authorization` header

So the behavior is:

- legacy JWT-style `service_role` keys: `apikey` + `Authorization`
- modern `sb_secret_*` keys: `apikey` only

This matches the compatibility requirements of newer Supabase secret keys.

## Supabase Run Store

The run store is separate from graph-scoped node connections.

It is still process-scoped, and `SupabaseRunStore.from_env()` resolves credentials in this order:

### URL selection

1. Read `GRAPH_AGENT_RUN_STORE_SUPABASE_URL_ENV_VAR`
2. Treat its value as the name of another env var
3. Read that env var
4. If unset, fall back to `GRAPH_AGENT_SUPABASE_URL`
5. If still unset, fall back to `SUPABASE_URL`

### Secret key selection

1. Read `GRAPH_AGENT_RUN_STORE_SUPABASE_SECRET_KEY_ENV_VAR`
2. Treat its value as the name of another env var
3. Read that env var
4. If unset, fall back to `GRAPH_AGENT_SUPABASE_SECRET_KEY`
5. Then fall back to:
   - `GRAPH_AGENT_SUPABASE_SERVICE_ROLE_KEY`
   - `SUPABASE_SERVICE_ROLE_KEY`
   - `SUPABASE_SECRET_KEY`

This lets the runtime event store point at a different Supabase project than the graph's application nodes.

## Recommended Mental Model

The simplest way to think about the system is:

- graphs own a registry of named Supabase connections
- connection ids are stable references
- actual secrets still live in env vars
- nodes bind to connection ids, not directly to projects
- legacy raw env-var binding still works when needed

## Recommended Naming Pattern

For multiple projects, prefer a predictable naming convention:

- `GRAPH_AGENT_SUPABASE_APP_URL`
- `GRAPH_AGENT_SUPABASE_APP_SECRET_KEY`
- `SUPABASE_APP_PROJECT_REF`
- `SUPABASE_APP_ACCESS_TOKEN`

- `GRAPH_AGENT_SUPABASE_ANALYTICS_URL`
- `GRAPH_AGENT_SUPABASE_ANALYTICS_SECRET_KEY`
- `SUPABASE_ANALYTICS_PROJECT_REF`
- `SUPABASE_ANALYTICS_ACCESS_TOKEN`

The exact keys do not matter as long as each connection points to the correct env-var names.

## Summary

The new system gives you:

- multiple Supabase projects in one graph
- stable per-node project targeting
- backward compatibility for older graphs
- safe failure when a named connection goes missing
- compatibility with both legacy `service_role` keys and modern `sb_secret_*` keys
- a separate process-level selector for the Supabase run store
