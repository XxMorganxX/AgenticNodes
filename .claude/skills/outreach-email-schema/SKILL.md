---
name: outreach-email-schema
description: "Schema contract for public.outbound_email_messages and public.inbound_email_messages tables (and their _dev mirrors). Use when working on the core.outbound_email_logger node, drafting email persistence, syncing inbound replies, distinguishing initial outreach from follow-ups via message_type/outreach_step, threading via parent_outbound_email_id/root_outbound_email_id, or modifying the outreach email DB schema."
---

# Outreach email schema contract

Source: `docs/outreach-email-schema.md`. The agent **drafts** emails — it
does not send them — so the schema centers on content + threading, not
delivery state.

## Two-table model

- `public.outbound_email_messages` — outreach drafts (initial + follow-ups).
- `public.inbound_email_messages` — replies, linked back to outbound rows.

Inbound rows reference outbound via `outbound_email_id`. Follow-up
outbound rows link to prior outbound via `parent_outbound_email_id`,
with `root_outbound_email_id` grouping the full chain.

## Dev mirror tables (use during development)

- `public.outbound_email_messages_dev`
- `public.inbound_email_messages_dev`

Same schema, internal foreign keys repointed at the dev tables. Use them
for draft logging during development; production-intended runs use the
non-`_dev` tables. Configure the `outbound_email_logger` node accordingly.

## Outbound row — load-bearing fields

| Column | Purpose |
|---|---|
| `recipient_email` | Required minimum. |
| `subject`, `body_text` | Message content (default to empty string). |
| `message_type` | `'initial'` or `'follow_up'`. Drives downstream branching. |
| `outreach_step` | Integer step in the outreach sequence. |
| `sales_approach`, `sales_approach_version` | Distinguish strategy/version for analysis. |
| `parent_outbound_email_id` | The specific prior outreach this follow-up extends. |
| `root_outbound_email_id` | Group the whole chain. |
| `provider_draft_id`, `provider_message_id`, `internet_message_id`, `conversation_id` | Provider-side identifiers. |
| `drafted_at` | Required, defaults to `now()`. |
| `observed_sent_at` | **Only** populated when an external signal confirms send. |
| `metadata` | jsonb — `cc`, `bcc`, attachments, template context, `metadata.generation_prompt` for prompt traces. |
| `raw_provider_payload` | jsonb — full provider response for replay. |

## Inbound row — load-bearing fields

| Column | Purpose |
|---|---|
| `outbound_email_id` | Link to triggering outbound row. |
| `mailbox_account` | Required — which account received it. |
| `sender_email`, `subject`, `body_text`, `clean_body_text` | Reply content. |
| `provider_message_id`, `internet_message_id`, `in_reply_to_internet_message_id`, `conversation_id` | Threading. |
| `sent_at`, `received_at` | `received_at` is required. |
| `metadata`, `raw_provider_payload` | jsonb. |

## Invariants (do not violate)

- **Drafting is not sending.** Do not infer "sent" from row creation. Only
  `observed_sent_at` (or another verified send signal) means sent.
- **Persist prompt traces in `metadata.generation_prompt`** — full
  `system_prompt`, `user_prompt`, and request message list. Evaluations
  depend on having the exact prompt that produced the draft.
- **Treat `metadata` as the home for sparse fields** — cc/bcc, attachments,
  template context. Don't add new top-level columns for sparsely populated
  data.
- **Treat `raw_provider_payload` as the source of truth for replay** —
  don't drop or filter it.

## Minimum-compatible logger

The bare-minimum compatible logger only requires `recipient_email`. Every
other column has defaults; populate them as data becomes available. Don't
fail logging just because optional fields are missing.

## When extending the schema

- New sparse field → add to `metadata` jsonb first; promote to a column
  only when query patterns demand it.
- New required threading concept → add as a real column with appropriate
  foreign keys and update both production and `_dev` tables identically.
- New status concept (e.g., real send tracking) → add a new column rather
  than overloading `observed_sent_at`.
