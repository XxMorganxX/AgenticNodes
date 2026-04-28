---
name: supabase-connections
description: "Multi-project Supabase connection contract for graph documents. Use when adding/editing supabase_connections or default_supabase_connection_id on a graph or test_environment document, binding nodes via supabase_connection_id, debugging missing_supabase_connection errors, working with core.supabase_data / core.supabase_row_write / core.outbound_email_logger, touching resolve_supabase_runtime_env_var_names() or resolve_graph_process_env(), or modifying SupabaseRunStore env-var resolution."
---

# Supabase connections contract

Full reference: `docs/supabase-connections.md`. Read it for the historical
"why" and full edge cases. This skill is the load-bearing rule set.

## Three-layer model

A graph stores Supabase access in three independent layers:

1. **Connection metadata** — on the graph document under `supabase_connections`
   (a list of `SupabaseConnectionDefinition` entries) and
   `default_supabase_connection_id`.
2. **Connection values** — in `graph.env_vars`, keyed by the env-var names the
   connection points at.
3. **Node binding** — each Supabase-capable node stores a stable
   `supabase_connection_id` (not raw URL/key env-var names).

A connection rename does **not** change its generated env-var keys — that's
how node bindings stay stable over time.

## Connection definition shape

```json
{
  "connection_id": "analytics-db",
  "name": "Analytics DB",
  "supabase_url_env_var": "GRAPH_AGENT_SUPABASE_ANALYTICS_URL",
  "supabase_key_env_var": "GRAPH_AGENT_SUPABASE_ANALYTICS_SECRET_KEY",
  "project_ref_env_var": "SUPABASE_ANALYTICS_PROJECT_REF",
  "access_token_env_var": "SUPABASE_ANALYTICS_ACCESS_TOKEN"
}
```

## Nodes covered

The named-connection system applies to:

- `core.supabase_data`
- `core.supabase_row_write`
- `core.outbound_email_logger`

The runtime validates that any `supabase_connection_id` used by these nodes
exists on the graph.

## Runtime resolution precedence (do not bypass)

Implemented in `resolve_supabase_runtime_env_var_names(...)`:

1. If `node.config.supabase_connection_id` is set:
   - look it up in `graph.supabase_connections`
   - found → use its `supabase_url_env_var` + `supabase_key_env_var`
   - missing → fail with `missing_supabase_connection`
2. If absent, fall back to legacy per-node `supabase_url_env_var` /
   `supabase_key_env_var` fields.
3. If those are absent, fall back to `GRAPH_AGENT_SUPABASE_URL` /
   `GRAPH_AGENT_SUPABASE_SECRET_KEY`.

**Critical invariant:** there is no silent fallback from a missing named
connection to another project. Failing loud is the contract — quiet fallback
risks writes to the wrong database.

After name resolution, values are resolved via `resolve_graph_process_env(...)`:
process env wins over `graph.env_vars`; first non-empty value wins.

## REST auth header rule

- always send `apikey: <key>`
- if the key starts with `sb_secret_`, **omit** the `Authorization` header
- otherwise (legacy `service_role` JWT), also send `Authorization: Bearer <key>`

## Run store is separate

`SupabaseRunStore.from_env()` resolves credentials independently from
graph-scoped node connections — it walks
`GRAPH_AGENT_RUN_STORE_SUPABASE_URL_ENV_VAR` →
`GRAPH_AGENT_SUPABASE_URL` → `SUPABASE_URL`, and similarly for the secret
key. This lets event storage point at a different project than the app's
data nodes. Don't conflate the two.

## Editor-only behaviors

- A graph with no explicit named connections but legacy env-var values gets
  an implicit `__legacy_default_supabase__` connection (UI only) until the
  user saves it explicitly. Don't persist this implicitly.
- A node without `supabase_connection_id` displays as
  `Compatibility mode (raw env vars)` — UI must not silently rewrite it.
- Connection deletion is blocked while any node still binds to its
  `connection_id`.

## Test-environment documents

For `test_environment` documents, `supabase_connections` and
`default_supabase_connection_id` live at the document root and are shared
across every agent graph in that environment.

## Common edits and what to update

| Change | Touch |
|---|---|
| New Supabase-capable node kind | Add to validator; honor `supabase_connection_id` in the runtime resolver. |
| New connection-level env-var | Update `SupabaseConnectionDefinition`, generation logic in editor, and `resolve_*` helpers. |
| Modify auth header behavior | One place: REST auth builder. Keep the `sb_secret_*` branch intact. |
| Run-store-only change | Edit `supabase_run_store.py` only — do not touch the per-node resolver. |
