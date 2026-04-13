# Graph Agent Studio

Graph Agent Studio is a local graph editor and runtime for building agent workflows, tool pipelines, and multi-agent test environments.

This repository contains:

- a Python runtime and FastAPI API for graph execution
- a React studio for authoring, inspecting, and running graphs
- a node-provider system that keeps graph semantics separate from concrete model or integration vendors
- built-in support for files, documents, MCP servers, spreadsheet-driven flows, and selected external integrations

## What It Supports

### Authoring models

You can work with two document shapes:

- `graph`: a single execution graph
- `test_environment`: a multi-agent environment that groups several isolated agent graphs under one document

The bundled `test-environment` example demonstrates a three-agent setup with separate run lanes and focused visualization.

### Node categories

The runtime validates connections by category rather than by vendor-specific node class:

- `start`
- `api`
- `provider`
- `tool`
- `control_flow_unit`
- `data`
- `end`

That lets the canvas stay stable while providers change underneath it.

### Built-in provider coverage

The current catalog includes support for:

- manual and Discord-triggered start nodes
- generic API/model nodes backed by provider bindings
- provider nodes for `mock`, `OpenAI`, `Anthropic API`, and local `Claude Code`
- registry tool nodes and MCP-backed tool execution
- control-flow nodes for spreadsheet row iteration, logic conditions, and parallel splitting
- data nodes for prompt blocks, context building, runtime normalization, LinkedIn profile fetch, Supabase reads, Supabase writes, and text-file output
- end nodes for standard output, Discord message delivery, and Outlook draft creation

### Runtime and editor features

- graph and environment CRUD through the FastAPI API
- live run streaming over Server-Sent Events
- persisted run manifests, event logs, and state snapshots
- per-run file browsing for generated workspace output
- project file uploads tied to a graph
- run document uploads with extracted text content
- provider preflight checks and diagnostics
- spreadsheet preview tooling in the editor
- Microsoft device-code auth status and connect/disconnect flows
- MCP server creation, testing, boot/stop/refresh, capability discovery, and tool toggling

## Project Layout

- `src/graph_agent/runtime/`: graph model, node execution, run state, event contract, filesystem helpers, spreadsheets, Supabase, LinkedIn, and auth helpers
- `src/graph_agent/api/`: FastAPI app, graph store, run manager, run persistence, project files, and bundled graph payloads
- `src/graph_agent/providers/`: model providers plus Discord and Outlook integrations
- `src/graph_agent/tools/`: local tools, MCP support, and built-in MCP servers
- `src/graph_agent/examples/`: bundled services and example graphs
- `frontend/`: React + Vite studio and run visualizer
- `tests/`: backend and runtime coverage for graph execution, MCP, integrations, spreadsheets, Supabase, files, and environments

Supporting design notes live in:

- [model.md](/Users/morgannstuart/Desktop/agentic-nodes/model.md)
- [tool-registry.md](/Users/morgannstuart/Desktop/agentic-nodes/tool-registry.md)
- [control-loop.md](/Users/morgannstuart/Desktop/agentic-nodes/control-loop.md)
- [supabase-connections.md](/Users/morgannstuart/Desktop/agentic-nodes/supabase-connections.md)
- [outreach-email-schema.md](/Users/morgannstuart/Desktop/agentic-nodes/outreach-email-schema.md)
- [state.md](/Users/morgannstuart/Desktop/agentic-nodes/state.md)
- [memory.md](/Users/morgannstuart/Desktop/agentic-nodes/memory.md)
- [logging.md](/Users/morgannstuart/Desktop/agentic-nodes/logging.md)

## Quick Start

### Prerequisites

- Python `3.9+`
- `npm`

### Backend setup

Create a local virtual environment and install the package:

```bash
python3 -m venv .venv
.venv/bin/pip install -e .
```

### Frontend setup

Install the studio dependencies:

```bash
cd frontend
npm install
cd ..
```

### Start the app

Run the helper launcher from the repo root:

```bash
python3 run.py
```

`run.py` will:

- require the local `.venv`
- start the FastAPI backend with reload enabled
- start the Vite frontend
- pick open ports starting from `8000` for the backend and `5173` for the frontend
- wire the frontend proxy to the chosen backend port automatically

Open the frontend URL printed in the terminal and use Graph Agent Studio there.

## Running Pieces Separately

If you want to run the services yourself instead of using `run.py`:

```bash
PYTHONPATH=src .venv/bin/python -m uvicorn graph_agent.api.app:app --reload --host 127.0.0.1 --port 8000
```

```bash
cd frontend
GRAPH_AGENT_API_PROXY=http://127.0.0.1:8000 npm run dev -- --host 127.0.0.1 --port 5173
```

## Environment Variables

The app reads environment variables from the current shell process. `.env.example` is a reference file, not an auto-loaded config file.

Common variables:

- `OPENAI_API_KEY`
- `ANTHROPIC_API_KEY`
- `DISCORD_BOT_TOKEN`
- `GRAPH_AGENT_SUPABASE_URL`
- `GRAPH_AGENT_SUPABASE_SECRET_KEY`
- `GRAPH_AGENT_SUPABASE_SCHEMA`
- `SUPABASE_PROJECT_REF`
- `SUPABASE_ACCESS_TOKEN`
- `GRAPH_AGENT_RUN_STORE`
- `GRAPH_AGENT_RUN_STORE_SUPABASE_URL_ENV_VAR`
- `GRAPH_AGENT_RUN_STORE_SUPABASE_SECRET_KEY_ENV_VAR`

Useful optional paths:

- `GRAPH_AGENT_UPLOAD_DIR` for uploaded run documents
- `GRAPH_AGENT_PROJECT_FILE_DIR` for graph-scoped project files
- `GRAPH_AGENT_WORKSPACE_DIR` for generated workspace files
- `GRAPH_AGENT_MCP_TEMPLATE_DIR` for additional MCP server templates

## Persistence

By default, run data is stored on the local filesystem under `.logs/runs/`.

Set:

```bash
GRAPH_AGENT_RUN_STORE=filesystem
```

to keep the default behavior, or:

```bash
GRAPH_AGENT_RUN_STORE=supabase
```

to use the Supabase-backed run store. The Supabase run store expects `GRAPH_AGENT_SUPABASE_URL` and `GRAPH_AGENT_SUPABASE_SECRET_KEY`.

The studio can now manage multiple named Supabase connections per graph. Each connection stores its actual values in graph env vars, while Supabase nodes bind to a connection id instead of hard-coding a single project.

For additional connections, use graph-scoped env vars such as:

```bash
GRAPH_AGENT_SUPABASE_ANALYTICS_URL=
GRAPH_AGENT_SUPABASE_ANALYTICS_SECRET_KEY=
SUPABASE_ANALYTICS_PROJECT_REF=
SUPABASE_ANALYTICS_ACCESS_TOKEN=
```

New Supabase connections use stable generated env var names so renaming a connection does not break node bindings.

If you want the Supabase run store to use a non-default env-var pair, set:

```bash
GRAPH_AGENT_RUN_STORE_SUPABASE_URL_ENV_VAR=GRAPH_AGENT_SUPABASE_ANALYTICS_URL
GRAPH_AGENT_RUN_STORE_SUPABASE_SECRET_KEY_ENV_VAR=GRAPH_AGENT_SUPABASE_ANALYTICS_SECRET_KEY
```

Supabase REST calls now support both legacy JWT-style `service_role` keys and newer `sb_secret_*` keys. Secret keys should remain server-side only.

For the exact graph document shape, runtime precedence rules, compatibility mode behavior, and run-store selection logic, see [supabase-connections.md](/Users/morgannstuart/Desktop/agentic-nodes/supabase-connections.md).

## MCP Support

The studio supports both built-in and user-managed MCP servers.

Current capabilities include:

- stdio and HTTP transport definitions
- capability discovery for tools, resources, resource templates, and prompts
- per-tool enable/disable controls
- built-in weather and time MCP servers
- bundled official Supabase MCP templates

Environment references such as `${SUPABASE_PROJECT_REF}` and `${SUPABASE_ACCESS_TOKEN}` are expanded when a server is tested or booted.

## Files and Documents

Two file flows are built in:

- project files: graph-scoped assets, including spreadsheets used by graph nodes
- run documents: uploaded files whose extracted text can be injected into prompts and preserved across environment runs

Spreadsheet uploads can be previewed from the editor, and uploaded `.xlsx` documents are normalized into derived CSV content for runtime use.

## Typical Workflow

1. Start the app with `python3 run.py`.
2. Open a bundled graph or create a new graph/environment in the studio.
3. Drag provider cards onto the canvas and connect nodes by valid category.
4. Configure provider bindings, tools, documents, project files, or graph env vars.
5. Run the graph or selected environment agents.
6. Inspect the timeline, active nodes, errors, generated files, and final output.

## Apollo Email Lookup Node

The `core.apollo_email_lookup` data node performs one Apollo `people/match` lookup, returns the full Apollo response payload, and stores successful or deterministic negative results in a shared cache under `.graph-agent/cache/apollo-email/`.

Set `APOLLO_API_KEY` in the environment, then provide either a direct identifier like `linkedin_url` or enough person-plus-organization fields for Apollo to match without extra retries.

## Tests

Run the backend test suite from the repo root:

```bash
.venv/bin/python -m unittest discover -s tests
```

## Extending The System

The main extension points are:

- register a new `NodeProviderDefinition`
- add a new model provider implementation
- add a registry tool or MCP server
- add a new node type only when execution semantics differ
- seed new bundled graphs or environments through the API/store layer

The core design goal is to keep graph structure stable while concrete providers, tools, and integrations remain pluggable.
