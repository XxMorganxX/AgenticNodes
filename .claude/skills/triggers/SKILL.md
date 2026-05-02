---
name: triggers
description: Use when work touches start-node activation — adding/modifying start-node providers, listener services, the Cloudflare tunnel config surface, or anything to do with how a graph gets kicked off (manual run vs. event listener). Triggers automatically on edits to runtime/node_providers.py start kinds, providers/discord.py, providers/triggers.py, providers/webhook.py, api/cloudflare_store.py, api/manager.py listener-sync code, or docs/triggers.md.
---

# Triggers — start-node activation paradigm

The canonical reference is **`docs/triggers.md`**. Read it before touching any
start-node provider, listener service, or the Cloudflare config surface.

## Quick contract

Every start-node `NodeProviderDefinition` declares:

- `trigger_mode: "immediate" | "listener"` (default `"immediate"`).
- `listener_transport: "outbound_socket" | "inbound_webhook" | None`
  (set only when `trigger_mode == "listener"`).

Mapping today:

| Provider | trigger_mode | listener_transport |
|---|---|---|
| `start.manual_run` / `core.input` | immediate | — |
| `start.discord_message` | listener | outbound_socket |
| `start.cron_schedule` | listener | — |
| `start.webhook` | listener | inbound_webhook |

For **`graph_type: test_environment`**, multiple agents may use listener starts **at once** (webhook, Discord, and/or cron). **Listen** uses the environment document id, arms every listener agent, and child runs are scoped with **`agent_ids=[…]`** so only the matching swimlane executes. Webhook agents need **pairwise distinct** `webhook_path_slug` values; cron uses composite internal keys per agent; Discord agents in one environment should use the same bot token.

## Where the runtime branches on this

- `src/graph_agent/providers/triggers.py` — `TriggerService` Protocol
  (`name`, `activate(graph_id)`, `deactivate(graph_id)`, `stop()`).
- `src/graph_agent/providers/cron.py` — internal cron scheduler service for
  `start.cron_schedule`; fires child runs while a listener session is active.
- `src/graph_agent/providers/webhook.py` — `WebhookTriggerService` registers the
  active slug while `start.webhook` listens; HTTP hits `/api/webhooks/{slug}`.
- `src/graph_agent/api/manager.py`:
  - `_DiscordTriggerAdapter` — adapts `DiscordTriggerService` to the protocol.
  - `self._trigger_services` — list of adapters the manager iterates.
  - `start_listener_session()` / `stop_listener_session()` — activate and
    deactivate the right trigger service for the graph's start provider.

When you add a **new** listener kind beyond the built-ins:

1. Register the `NodeProviderDefinition` with `trigger_mode="listener"` and the
   correct `listener_transport`.
2. Implement a service that satisfies the `TriggerService` Protocol. It can
   adopt the protocol directly or be wrapped by a thin adapter inside the
   manager (the Discord pattern).
3. Append it to `self._trigger_services` in `GraphRunManager.__init__`.
4. Update `docs/triggers.md` and this skill in the same commit.

## Cloudflare configuration

Inbound webhook listeners require a public URL. The Cloudflare tunnel
configuration lives at:

- Store: `src/graph_agent/api/cloudflare_store.py` (`.graph-agent/cloudflare_config.json`).
- Managed subprocess: `src/graph_agent/api/cloudflare_tunnel.py` — starts
  `cloudflared tunnel run --token …` when an active listener session uses
  `listener_transport: "inbound_webhook"`; stops when the last such session ends
  or on runtime reset / background shutdown.
- Manager API: `get_cloudflare_config()`, `set_cloudflare_config(payload)`,
  `clear_cloudflare_config()` — GET merges tunnel runtime status (`tunnel_state`,
  `tunnel_pid`, `tunnel_ref_count`, etc.).
- HTTP API: `GET / PUT / DELETE /api/editor/integrations/cloudflare`.
- Editor catalog `GET /api/editor/catalog` includes `cloudflare` (merged config +
  tunnel runtime) for webhook URL hints in the inspector.

The tunnel token is referenced by env-var name only — never stored on disk.
`token_configured` in the GET response reports whether the named env-var is
populated in the current process. Optional `GRAPH_AGENT_CLOUDFLARED_PATH`
points at the `cloudflared` binary when it is not on `PATH`.

## Webhook ingress policy

Inbound HTTP to `/api/webhooks/{slug}` from **the public internet** requires
`GRAPH_AGENT_WEBHOOK_INGRESS_ENABLED` in the API process; **localhost and private
LAN** callers still work without it. Opt in only when
external callers should trigger graphs.

## Don't do

- Don't add new listener wiring to `start_background_services()` or
  `stop_background_services()` — extend `self._trigger_services` instead.
- Don't store the tunnel token (or any secret) directly in
  `cloudflare_config.json`. Only the env-var name belongs there.
- Don't drop `trigger_mode` / `listener_transport` from the API payload that
  reaches the frontend — the studio uses them to badge start nodes and warn
  when a graph requires Cloudflare.
