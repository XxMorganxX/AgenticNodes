---
name: triggers
description: Use when work touches start-node activation — adding/modifying start-node providers, listener services, the Cloudflare tunnel config surface, or anything to do with how a graph gets kicked off (manual run vs. event listener). Triggers automatically on edits to runtime/node_providers.py start kinds, providers/discord.py, providers/triggers.py, providers/webhook.py (future), api/cloudflare_store.py, api/manager.py listener-sync code, or docs/triggers.md.
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

## Where the runtime branches on this

- `src/graph_agent/providers/triggers.py` — `TriggerService` Protocol
  (`name`, `sync()`, `stop()`).
- `src/graph_agent/api/manager.py`:
  - `_DiscordTriggerAdapter` — adapts `DiscordTriggerService` to the protocol.
  - `self._trigger_services` — list of adapters the manager iterates.
  - `_sync_trigger_services()` / `_stop_trigger_services()` — the central
    lifecycle hooks called from `start_background_services()`,
    `stop_background_services()`, `reset_runtime()`, and the three graph
    mutation sites (`create_graph`, `update_graph`, `delete_graph`).

When you add a new listener kind (e.g. `start.webhook`):

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
- Manager API: `get_cloudflare_config()`, `set_cloudflare_config(payload)`,
  `clear_cloudflare_config()`.
- HTTP API: `GET / PUT / DELETE /api/editor/integrations/cloudflare`.

The tunnel token is referenced by env-var name only — never stored on disk.
`token_configured` in the GET response reports whether the named env-var is
populated in the current process.

## Don't do

- Don't add new listener wiring to `start_background_services()` or
  `stop_background_services()` — extend `self._trigger_services` instead.
- Don't store the tunnel token (or any secret) directly in
  `cloudflare_config.json`. Only the env-var name belongs there.
- Don't drop `trigger_mode` / `listener_transport` from the API payload that
  reaches the frontend — the studio uses them to badge start nodes and warn
  when a graph requires Cloudflare.
