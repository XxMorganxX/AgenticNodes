---
name: tool-registry
description: "Tool registry contract — how tools are registered, validated, executed, and routed on. Use when adding a new tool definition, modifying tool dispatch behavior, debugging validation/execution failures, working with the tool.registry node provider, or touching the tool result envelope (success / validation failure / execution failure)."
---

# Tool registry contract

Source: `docs/tool-registry.md`. Tool nodes live in the `tool` category and
the default dispatch node provider is `tool.registry` (future remote-tool
providers will plug into the same category).

## What a tool registration must include

- **name** — stable string identifier.
- **description** — concise human-readable purpose.
- **input schema** — validated before `execute` is called.
- **execute function** — pure-ish Python that returns a normalized envelope.

## Result envelope (the contract that matters)

Every tool execution returns one of three shapes — they are **first-class
runtime results**, not exceptions:

1. **Success** — structured output payload.
2. **Validation failure** — actionable error details, before `execute` runs.
3. **Execution failure** — error details + retry guidance, after `execute`
   raised or returned a structured failure.

The runtime routes on result type via conditional edges. That's how repair
loops and fallback branches are expressed in graphs.

## Anti-patterns

- Throwing exceptions out of `execute` for expected failure modes — they
  bypass conditional routing and become opaque terminal errors.
- Encoding "forbidden tool sequences" inside the registry. The first
  version intentionally pushes those constraints into graph structure and
  edge conditions, not the registry.
- Hard-coding tool dispatch behavior outside the `tool.registry` provider —
  breaks future remote-tool provider plug-in.

## When adding a new tool

1. Register the definition (name + description + input schema + execute).
2. If the input schema is non-trivial, add a unit test that asserts the
   validation-failure envelope shape for at least one bad input.
3. If `execute` can fail at runtime, add a test that asserts the
   execution-failure envelope is emitted (not raised).
4. If the tool needs new external services or credentials, surface them
   through the standard env-var resolution — don't introduce a side channel.

## Future surface (don't pre-build, but keep the door open)

Documented as v1 simplifications; design choices should not block these:

- dynamic tool discovery
- external tool registries
- remote tool execution
- policy-aware dispatch restrictions
