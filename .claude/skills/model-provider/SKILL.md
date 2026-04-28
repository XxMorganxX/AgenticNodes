---
name: model-provider
description: "ModelProvider contract for vendor-swappable model nodes. Use when adding a new model provider implementation under src/graph_agent/providers/, wiring a vendor like OpenAI/Anthropic/Claude/mock behind the existing api category (mock.model, openai.chat, claude.messages), modifying prompts in config.py, or designing how a new model node returns trace metadata, usage, and recoverable errors."
---

# Model provider contract

Source: `docs/model.md`. Model nodes are `api`-category nodes identified by
pluggable node-provider IDs (`mock.model`, `openai.chat`, `claude.messages`,
…). The graph never sees vendor specifics — the `ModelProvider` interface
abstracts them.

## Where things live

- **Provider implementations** — `src/graph_agent/providers/`
- **Prompts** — `config.py` (NOT inline in runtime code)
- **Node category** — `api` (registered as standard `NodeProviderDefinition`s)

## Execution contract — what providers receive

- provider configuration
- prompt messages or structured prompt fields
- optional response schema hints
- the current `RunContext` (for observability)

## Execution contract — what providers must return

- structured result payload (the thing downstream nodes consume)
- raw content for inspection when helpful
- usage / latency metadata when available
- a **recoverable error shape** when generation fails (not a thrown
  exception — recoverable errors must be routable through conditional
  edges, same pattern as the tool registry)

## Adding a new vendor (the smallest path)

1. Implement the `ModelProvider` interface under
   `src/graph_agent/providers/<vendor>.py`.
2. Register a `NodeProviderDefinition` with `category: api` and a
   namespaced `provider_id` (e.g. `<vendor>.<endpoint>`).
3. Add prompts to `config.py` if the vendor needs distinct prompt strategies.
4. Wire `_node_from_dict()` in `runtime/core.py` only if you need
   provider-specific constructor state — usually you don't.
5. Cover the provider with tests in the same style as existing
   provider tests.

If steps 4 and 5 of the node-development guide aren't surfacing for you,
this is probably **just a model-provider swap, not a node addition** — the
graph behavior stays the same.

## Anti-patterns

- Embedding vendor-specific transport logic in `runtime/core.py` instead
  of behind the provider interface.
- Hardcoding prompts in the provider implementation instead of `config.py`.
- Throwing on recoverable model errors — kills repair-loop routing.
- Introducing a new node kind when only the vendor changed.

## Future expansion

Swapping providers should be a wiring change, not a runtime change. Keep
the interface lean enough that adding a new vendor doesn't require
modifying the engine.
