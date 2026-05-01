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

## How the runtime uses these fields

`GraphRunManager` owns a list of `TriggerService` objects (`src/graph_agent/providers/triggers.py`).
Each service exposes `activate(graph_id)`, `deactivate(graph_id)`, and `stop()`:

- `activate(graph_id)` — start or register the transport/timer for an active
  listener session.
- `deactivate(graph_id)` — unregister the graph when its listener session ends.
- `stop()` — release any long-lived resources (sockets, threads, subprocesses).

Today the services are the Discord listener (wrapped by `_DiscordTriggerAdapter`
in `src/graph_agent/api/manager.py`) and the internal cron scheduler
(`CronTriggerService` in `src/graph_agent/providers/cron.py`). When
`start.webhook` lands, it slots into this list with no further changes to the
lifecycle plumbing.

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

`GET /api/editor/integrations/cloudflare` returns:

```json
{
  "tunnel_token_env_var": "CLOUDFLARE_TUNNEL_TOKEN",
  "public_hostname": "example.trycloudflare.com",
  "token_configured": true
}
```

`token_configured` reflects whether the named env-var is currently populated in
the running process — a light health-check signal for the UI without leaking the
secret value.

## What's deliberately not here yet

This document records the paradigm. Several pieces follow in later changes:

- A `start.webhook` node provider with config schema (slug, signature scheme,
  event filter) and the `/api/webhooks/{slug}` FastAPI router.
- Booting `cloudflared` as a managed subprocess from the manager (modeled on
  `mcp_server_manager`). Today the user runs `cloudflared` themselves and pastes
  the public hostname into the modal.
- A `trigger_source` field on `RunState` so the run list can label runs by cause.
- Multi-agent (`test_environment`) routing rules for inbound triggers.

When those land, this document picks them up — and so does
`.claude/skills/triggers/SKILL.md`, which the repo convention requires to ship in
the same commit as updates here.
