# Trigger Modes for Start Nodes

## Why this exists

A graph runs only when its start node tells the runtime to begin. Two operational
patterns exist for "telling the runtime to begin":

1. **Immediate** — something explicit calls `manager.start_run(graph_id, payload)`.
   The UI Run button, an API caller, or a script does this. The graph is dormant
   until that call.
2. **Listener** — a long-lived background service watches for an external event
   and calls `start_run(...)` when one arrives. The graph is dormant from the
   user's perspective but a service is keeping watch on its behalf.

Every start-node provider declares which pattern it follows via `trigger_mode`.

## The contract

`NodeProviderDefinition` carries two fields relevant to triggering:

| Field | Type | Meaning |
|---|---|---|
| `trigger_mode` | `"immediate"` \| `"listener"` | What activates the graph. Defaults to `"immediate"` for backwards compatibility. |
| `listener_transport` | `"outbound_socket"` \| `"inbound_webhook"` \| `None` | Only meaningful when `trigger_mode == "listener"`. Tells the runtime and UI whether the listener connects out from this server (no public URL needed) or accepts inbound HTTP (public URL needed). |

Today's start-node inventory:

| Provider | trigger_mode | listener_transport |
|---|---|---|
| `start.manual_run` | `immediate` | — |
| `core.input` (legacy alias) | `immediate` | — |
| `start.discord_message` | `listener` | `outbound_socket` |
| `start.cron_schedule` | `listener` | — |
| `start.webhook` | `listener` | `inbound_webhook` |

## How the runtime uses these fields

`GraphRunManager` owns a list of `TriggerService` objects (`src/graph_agent/providers/triggers.py`).
Each service exposes `activate(graph_id)`, `deactivate(graph_id)`, and `stop()`:

- `activate(graph_id)` — start or register the transport/timer for an active
  listener session.
- `deactivate(graph_id)` — unregister the graph when its listener session ends.
- `stop()` — release any long-lived resources (sockets, threads, subprocesses).

The services are the Discord listener (wrapped by `_DiscordTriggerAdapter`
in `src/graph_agent/api/manager.py`), the internal cron scheduler
(`CronTriggerService` in `src/graph_agent/providers/cron.py`), and the webhook
slug registry (`WebhookTriggerService` in `src/graph_agent/providers/webhook.py`).
Inbound HTTP is handled by FastAPI `POST|GET|… /api/webhooks/{slug}` which
dispatches to `_start_child_run` when a listener session is active.

`start.cron_schedule` is listener-mode even though it does not use a network
transport. A listener session keeps the schedule active; each due cron fire
starts a child run with input payload fields such as `source="cron_schedule"`,
`prompt`, `cron_expression`, `timezone`, `scheduled_for`, and `fired_at`.

## Inbound listeners require a public URL — Cloudflare

A graph whose start node is `listener` + `inbound_webhook` cannot fire unless an
external service can POST to this server. The supported exposure mechanism is a
**Cloudflare tunnel**.

Cloudflare configuration is a first-class connection alongside Microsoft auth and
Supabase connections:

- Backend store: `src/graph_agent/api/cloudflare_store.py` (`.graph-agent/cloudflare_config.json`).
- Manager methods: `get_cloudflare_config()` / `set_cloudflare_config(payload)` / `clear_cloudflare_config()`.
- API endpoints: `GET / PUT / DELETE /api/editor/integrations/cloudflare`.

The persisted record holds:

```json
{
  "tunnel_token_env_var": "CLOUDFLARE_TUNNEL_TOKEN",
  "public_hostname": "example.trycloudflare.com"
}
```

The actual tunnel token is **never** stored on disk — only the env-var name that
references it. This matches how `DISCORD_BOT_TOKEN`, `OPENAI_API_KEY`, and the
Supabase connection secrets work today. Operators set the secret in `.env` (or
the deployment env); the modal records which env-var name to read.

`GET /api/editor/integrations/cloudflare` returns the saved record, a
`token_configured` flag, and **managed tunnel runtime** fields (no secret values):

```json
{
  "tunnel_token_env_var": "CLOUDFLARE_TUNNEL_TOKEN",
  "public_hostname": "webhooks.example.com",
  "token_configured": true,
  "tunnel_state": "running",
  "tunnel_pid": 12345,
  "tunnel_ref_count": 1,
  "tunnel_active_graph_ids": ["<graph_id>"],
  "tunnel_last_error": null,
  "tunnel_last_exit_code": null,
  "tunnel_log_tail": ["[stdout] line..."]
}
```

`token_configured` reflects whether the named env-var is currently populated in
the running process — a light health-check signal for the UI without leaking the
secret value. `tunnel_state` and related fields describe the API-managed
`cloudflared` process (see below).

### Managed `cloudflared` (named tunnel token)

When a listener session starts for a start node with
`listener_transport: "inbound_webhook"`, the manager starts a single shared
`cloudflared` subprocess:

`cloudflared tunnel --no-autoupdate run --token <value from env>`

- The token value is read from the process environment using
  `tunnel_token_env_var` (default `CLOUDFLARE_TUNNEL_TOKEN`); it is never written
  to `cloudflare_config.json`.
- The process stops when the last such listener session ends, or when the
  runtime is reset / background services stop.
- Optional: set `GRAPH_AGENT_CLOUDFLARED_PATH` to a full path if `cloudflared`
  is not on `PATH`.

Implementation: `src/graph_agent/api/cloudflare_tunnel.py`, wired from
`GraphRunManager.start_listener_session` / `stop_listener_session`.

## What's deliberately not here yet

This document records the paradigm. Several pieces follow in later changes:

- A `trigger_source` field on `RunState` so the run list can label runs by cause.

### Multi-agent (`test_environment`) + listeners

Several agents may each use a listener start (`start.webhook`, `start.discord_message`, or `start.cron_schedule`). Each fires independently into its swimlane.

- **Webhook:** Each webhook agent must use a **distinct** `webhook_path_slug` within the environment (`GraphStore` validates). **Listen** registers **all** slugs on the environment `graph_id`. Dispatch resolves the agent by matching the HTTP `{slug}`.
- **Cron:** Each cron agent gets its own activation key: environment id plus an internal separator plus `agent_id` (`CronTriggerService` treats this like `graph_id`). Child runs use `agent_ids=[that agent]`.
- **Discord:** All Discord agents in the same environment must share one resolved bot token (same env var / value). The listener session starts the shared Discord client once; messages route to the agent whose start config matches the channel and filters, and only that swimlane runs.

### Implemented: `start.webhook`

- Provider registration and config fields: `src/graph_agent/examples/tool_schema_repair.py`.
- Trigger service + verification helpers: `src/graph_agent/providers/webhook.py`.
- HTTP ingress: `POST` / `GET` / … on `/api/webhooks/{slug}` in `src/graph_agent/api/app.py`.
- Slug uniqueness across graphs: `GraphStore._validate_webhook_slug_uniqueness`.

**Ingress policy:** Without **`GRAPH_AGENT_WEBHOOK_INGRESS_ENABLED=1`**, the API still accepts `/api/webhooks/{slug}` from **loopback** (e.g. `127.0.0.1`) and **private / link-local** client addresses (typical same-machine and LAN testing). Set the env var to **1** (and restart) so **public internet** clients can trigger webhooks. The editor catalog’s `webhook_ingress_enabled` flag reflects the public-ingress opt-in, not local testing.

When those land, this document picks them up — and so does
`.claude/skills/triggers/SKILL.md`, which the repo convention requires to ship in
the same commit as updates here.
