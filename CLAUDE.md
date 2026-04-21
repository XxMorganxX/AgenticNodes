# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Commands

Run the full app (backend + frontend, auto-picks ports, reloads on change):

```bash
python3 run.py
```

`run.py` requires `.venv` at the repo root and will refuse to start without it. First-time setup:

```bash
python3 -m venv .venv
.venv/bin/pip install -e .
(cd frontend && npm install)
```

Run backend and frontend separately (useful when debugging one side):

```bash
PYTHONPATH=src .venv/bin/python -m uvicorn graph_agent.api.app:app --reload --host 127.0.0.1 --port 8000
cd frontend && GRAPH_AGENT_API_PROXY=http://127.0.0.1:8000 npm run dev -- --host 127.0.0.1 --port 5173
```

Backend tests (unittest, not pytest):

```bash
.venv/bin/python -m unittest discover -s tests
.venv/bin/python -m unittest tests.test_data_driven_graphs            # single file
.venv/bin/python -m unittest tests.test_data_driven_graphs.ClassName.test_method  # single test
```

Frontend typecheck + production build:

```bash
cd frontend && npm run build
```

## Architecture

### Graph document shapes

Two authoring models live side-by-side in the same store:

- `graph`: a single execution graph (nodes, edges, start node, env vars, project files, Supabase connections).
- `test_environment`: a multi-agent document that groups several isolated agent graphs under one run, with per-agent swimlanes in the studio.

Both shapes flow through the same API and runtime; the UI switches presentation based on `graph_type`.

### Node provider system (the key abstraction)

Graph semantics are separated from concrete vendors via `NodeProviderDefinition` (see `src/graph_agent/runtime/node_providers.py`). The runtime validates connections by **category**, not by vendor class:

`start`, `api`, `provider`, `tool`, `control_flow_unit`, `data`, `end`

When adding functionality, the preferred extension order is:

1. Register a new `NodeProviderDefinition` — works for most additions.
2. Add a new model provider implementation under `src/graph_agent/providers/`.
3. Add a new node kind **only** when execution semantics actually differ.

Provider bindings (e.g. `mock.model`, `openai.chat`, `claude.messages`, `tool.registry`) let the canvas stay stable while vendors change underneath.

### Runtime core

`src/graph_agent/runtime/core.py` owns execution. `engine.py` implements the control loop:

1. Start at the declared start node.
2. Execute current node with current `RunContext`.
3. Persist node output/errors/timing into run state.
4. Evaluate outgoing edges in priority; take first matching conditional, otherwise first standard edge.
5. Stop on output-node completion, guard limit, or unrecoverable error.

Safety guards (`DEFAULT_RUN_MAX_STEPS`, `DEFAULT_MAX_VISITS_PER_NODE`, `DEFAULT_MAX_REPAIR_ATTEMPTS` in `config.py`) produce structured terminal events, not exceptions.

Nodes do **not** mutate shared state directly — they return results that the runtime reduces into `RunState` via `api/run_state_reducer.py`.

### Event contract (`runtime.v1`)

Events are the single source of truth: they drive SSE live streaming, replay, reducer-based state projection, and UI rendering. Defined in `src/graph_agent/runtime/event_contract.py`.

Base event types: `run.started`, `node.started`, `node.completed`, `edge.selected`, `condition.evaluated`, `retry.triggered`, `run.completed`, `run.failed`, `run.cancelled`, `run.interrupted`. Multi-agent parent streams wrap these with an `agent.` prefix.

Compatibility rules inside `runtime.v1`:
- adding optional payload fields is allowed
- existing `event_type` names and reducer-critical payload keys (documented in `logging.md`) must stay stable
- renaming an event or changing its meaning requires a new schema version or adapter

### API layer

`src/graph_agent/api/app.py` is the FastAPI surface. `manager.py` (`GraphRunManager`) owns background services, run lifecycle, graph CRUD, MCP servers, Microsoft auth, and Supabase diagnostics. Persistence is split:

- `graph_store.py` — graph/environment documents
- `run_store.py` + `supabase_run_store.py` — run manifests/events
- `run_log_store.py` — per-run event logs
- `project_files.py` — graph-scoped uploads

Run store backend is selected by `GRAPH_AGENT_RUN_STORE` (`filesystem` or `supabase`).

### Frontend

React 19 + Vite + reactflow studio in `frontend/src/`. `App.tsx` is the top-level shell; significant state lives in the many `lib/*.ts` modules (graph history, env vars, runtime event projections, saved nodes, hotbar favorites). Vite proxies `/api` to `GRAPH_AGENT_API_PROXY` (set by `run.py`).

### Supabase connections (multi-project)

Graphs can declare multiple named Supabase connections. Connection metadata lives on the graph document, values live in graph env vars, and nodes bind to a `connection_id`. A connection rename must not break node bindings — use the stable generated env-var names. Full spec: `supabase-connections.md`.

### Local persistence layout

All runtime state lives under ignored directories at the repo root:

- `.graph-agent/` — graph store, MCP server state, project files, uploads, Outlook dedupe sqlite, Apollo email cache
- `.logs/runs/` — filesystem run store

These are seeded from bundled defaults on first boot (see `examples/`).

## Repository Conventions

These rules come from `.cursor/rules/` and are enforced for every change:

- **Publish-ready repo**: treat the repo as push-to-GitHub at any time. No user-specific, machine-specific, generated, secret, or one-person working files in tracked source. Prefer ignored paths (`.graph-agent/`, `.logs/`) or a user-data dir.
- **Gitignore in the same change**: when a feature writes files, add the `.gitignore` entry in the same commit. Commit templates (`.env.example`), never filled-in `.env`.
- **Bootstrap from tracked templates**: if mutable local state is needed, copy from a bundled default into an ignored path on first run — never write mutable state into `src/`, `tests/`, or other tracked files.
- **Feature READMEs**: new features get a purpose-first README near the feature (human purpose, then implementation overview, then edge cases). Update it when behavior changes.

## Design Notes

Design intent for the major components lives in top-level docs — read these before making structural changes:

- `model.md` — provider-agnostic `ModelProvider` contract, prompts live in `config.py`
- `control-loop.md` — traversal rules and termination semantics
- `state.md` — `RunState` fields and access patterns
- `logging.md` — full `runtime.v1` event catalog and compatibility rules
- `tool-registry.md` — tool result envelope (success / validation failure / execution failure)
- `supabase-connections.md` — connection document shape and runtime precedence
- `outreach-email-schema.md` — email outbound log table contract
- `memory.md` — placeholder; durable memory is not implemented

## Environment Variables

`.env` is **not** auto-loaded — variables must be exported in the shell that runs `run.py` (or the standalone uvicorn/vite commands). `.env.example` is reference only.

Commonly needed: `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `DISCORD_BOT_TOKEN`, `GRAPH_AGENT_SUPABASE_URL`, `GRAPH_AGENT_SUPABASE_SECRET_KEY`, `APOLLO_API_KEY`. Supabase supports both legacy `service_role` JWT keys and newer `sb_secret_*` keys. Secret keys are server-side only.

Optional path overrides: `GRAPH_AGENT_UPLOAD_DIR`, `GRAPH_AGENT_PROJECT_FILE_DIR`, `GRAPH_AGENT_WORKSPACE_DIR`, `GRAPH_AGENT_MCP_TEMPLATE_DIR`.
