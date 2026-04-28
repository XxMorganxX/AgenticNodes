---
name: authoring-skills
description: "Process for authoring or updating .claude/skills/ entries that auto-inject project guides into Claude Code. Use when the user asks to create a new skill, convert a docs/ guide into an auto-injected skill, update an existing SKILL.md description for better triggering, or audit which design docs are wired up vs. which are orphaned in CLAUDE.md."
---

# Authoring skills for this repo

Skills under `.claude/skills/<name>/SKILL.md` are how this project gets
design guides into Claude Code automatically. Every skill's `name` +
`description` is loaded into the assistant's context every turn; the body
is only loaded when the description matches the user's task.

This skill describes the process for adding new ones and is the canonical
reference when the user says "make this a skill" or "wire up this doc".

## Where things live in this repo

| Layer | Path | Auto-injection behavior |
|---|---|---|
| Always-on rules | `CLAUDE.md` | Full content, every turn. |
| On-demand guides | `.claude/skills/<name>/SKILL.md` | Name + description every turn; body on match. |
| Canonical design docs | `docs/*.md` | None — referenced by skills, read on demand. |
| Cursor rules | `.cursor/rules/*.mdc` | None for Claude Code (Cursor only). Duplicated into CLAUDE.md if always-relevant. |

When adding a new design doc, also add a skill that wraps it. Otherwise the
doc is invisible to the assistant unless the user names the path.

## When to create a skill vs. extend CLAUDE.md vs. just write a doc

- **CLAUDE.md** — short, always-relevant rule that applies to every
  conversation in this repo (e.g. repo conventions, common commands).
  Keep CLAUDE.md as the index — never paste full guides in.
- **Skill** — domain guide that should auto-load *when relevant*. Most
  design docs in `docs/` should have a paired skill.
- **Just a doc** — long-form reference that's not action-shaped (e.g.
  historical decisions, architecture explainers). Skills should still
  point at it.

## File layout for a new skill

```
.claude/skills/<kebab-case-name>/
  SKILL.md
```

Single SKILL.md per skill, frontmatter on top:

```markdown
---
name: <same as folder name>
description: "<one-line trigger description — see rules below>"
---

# <Title>

<concise body — see body rules below>
```

## Writing the description (the most important part)

The description is the only thing the assistant sees every turn before
deciding to load the body. It must be specific enough that the assistant
can match it against user requests with high precision.

Rules:

1. Lead with what the skill *is* — "Authoring contract for X", "Schema
   contract for Y", "Process for Z".
2. Include a "**Use when**" clause that lists concrete triggers:
   - file paths (`src/graph_agent/runtime/core.py`)
   - function names (`resolve_supabase_runtime_env_var_names`)
   - provider IDs (`core.python_script_runner`, `core.supabase_data`)
   - vendor names (`OpenAI`, `Anthropic`, `Outlook`)
   - user activities (`adding`, `debugging`, `wiring`, `migrating`)
3. Avoid vague triggers ("when working on the project", "for general
   tasks"). Vague triggers cause spurious loads and dilute precision.
4. Keep it to ~2 sentences max. Long descriptions get noisy in the
   always-loaded list.

Bad: `"Use this for Supabase stuff."`
Good: `"Multi-project Supabase connection contract for graph documents. Use when adding/editing supabase_connections on a graph, binding nodes via supabase_connection_id, debugging missing_supabase_connection errors, working with core.supabase_data / core.supabase_row_write / core.outbound_email_logger, or modifying SupabaseRunStore env-var resolution."`

## Writing the body

The body is only loaded when triggered, so it should be the **operational
contract**, not the prose explainer.

Rules:

1. **Point at the canonical doc up top.** Format: ``Source: `docs/<file>.md`. ...``
   The skill is a load-bearing summary; the doc is the long-form reference.
2. **Lead with invariants and rules.** What must not be violated. Tables
   and bullet lists beat prose for fast scanning.
3. **Include load-bearing identifiers.** File paths, function names,
   column names, env-var names, exit codes. These are what the assistant
   needs to write correct code.
4. **End with anti-patterns or "when adding a new X".** Concrete
   guardrails are higher value than philosophy.
5. **Don't duplicate the entire doc.** If the skill is 1:1 with the doc,
   you are wasting context — extract the contract, leave the explainer
   in `docs/`.

Target length: 80–200 lines. The `python-script-runner` skill (~170
lines) is the upper end of useful; the `tool-registry` skill (~50 lines)
is the lower end.

## Adding a new skill — checklist

1. Identify the trigger surface — what user requests / file paths /
   activities should auto-load this?
2. Pick a kebab-case name. Match the source doc filename when reasonable
   (e.g. `docs/supabase-connections.md` → `supabase-connections`).
3. Write the description first; iterate on it before the body. The
   description is the load gate.
4. Write the body as a contract summary, not a tutorial. Point at the
   canonical doc.
5. Verify the description is specific by listing 5 user requests it
   should match and 5 it should not. If any of the "should not" examples
   plausibly match, tighten the description.
6. Add the skill — no other registration step needed; Claude Code
   discovers files under `.claude/skills/` automatically.

## Updating an existing skill

- **Description tweak** — safe; just update the frontmatter and reload.
- **Body change** — safe if the rules in the doc haven't changed.
- **Doc + skill drift** — when the canonical `docs/<file>.md` changes
  meaningfully, the skill body needs the same update. Consider whether
  the skill description still accurately reflects the trigger surface.

## Auditing skill coverage

To check which design docs are wired vs. orphaned:

```bash
ls .claude/skills/   # what's wired
ls docs/             # what exists
```

Anything in `docs/` without a matching skill is only reachable when the
user names the file. Decide per-doc:

- **Wire it** if it's action-shaped (contracts, schemas, authoring rules).
- **Leave it** if it's a historical/architectural explainer the assistant
  shouldn't auto-load.

## Anti-patterns

- Vague descriptions — pollute the always-loaded list, dilute matching.
- Skills that duplicate the entire `docs/` file — wasted context on load.
- Skills with no canonical doc reference — implementation drift becomes
  invisible.
- Catch-all "general project" skills — better to have many narrow skills
  than one broad one.
- Auto-load triggers based on the user's mood ("when frustrated", "when
  asking general questions") — descriptions should match concrete tasks.

## Existing skill inventory (as of this writing)

- `python-script-runner` — `core.python_script_runner` script contract
- `node-development` — adding nodes end-to-end
- `supabase-connections` — multi-project Supabase contract
- `runtime-events` — `runtime.v1` event/state/control-loop contract
- `tool-registry` — tool result envelope contract
- `model-provider` — vendor-swappable model interface
- `outreach-email-schema` — outbound/inbound email tables
- `authoring-skills` — this file (meta)

Docs without a paired skill (intentional — these are explainers, not
contracts):

- `docs/memory.md` — placeholder; durable memory not implemented.
