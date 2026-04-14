# Outreach Email Schema

## Purpose

This note captures the current working schema for outreach email tracking.

The outbound table is used for both initial outreach and follow-up drafts. The agent creates drafts but does not send emails directly, so the schema is centered on the message content and threading data rather than a detailed delivery-status model.

## Design Decisions

- Treat outbound rows as outreach messages, not only follow-ups.
- Keep query-heavy fields as first-class columns.
- Store provider-specific or less frequently queried details in `jsonb`.
- Do not infer "sent" from draft creation alone.
- Allow later syncing of actual send activity with `observed_sent_at` if that becomes available.

## Table 1: `public.outbound_email_messages`

```sql
create table if not exists public.outbound_email_messages (
  id uuid primary key default gen_random_uuid(),
  source_run_id text references public.runs(run_id) on delete set null,

  provider text not null default 'outlook',
  mailbox_account text,

  recipient_email text not null,
  subject text not null default '',
  body_text text not null default '',

  message_type text not null default 'initial' check (message_type in ('initial', 'follow_up')),
  outreach_step integer not null default 0,
  sales_approach text,
  sales_approach_version text,

  parent_outbound_email_id uuid references public.outbound_email_messages(id) on delete set null,
  root_outbound_email_id uuid references public.outbound_email_messages(id) on delete set null,

  provider_draft_id text,
  provider_message_id text,
  internet_message_id text,
  conversation_id text,

  drafted_at timestamptz not null default now(),
  observed_sent_at timestamptz,

  metadata jsonb not null default '{}'::jsonb,
  raw_provider_payload jsonb not null default '{}'::jsonb,

  created_at timestamptz not null default now()
);
```

## Table 2: `public.inbound_email_messages`

```sql
create table if not exists public.inbound_email_messages (
  id uuid primary key default gen_random_uuid(),
  outbound_email_id uuid references public.outbound_email_messages(id) on delete set null,

  provider text not null default 'outlook',
  mailbox_account text not null,

  sender_email text not null,
  subject text not null default '',
  body_text text not null default '',
  clean_body_text text,

  provider_message_id text,
  internet_message_id text,
  in_reply_to_internet_message_id text,
  conversation_id text,

  sent_at timestamptz,
  received_at timestamptz not null,

  metadata jsonb not null default '{}'::jsonb,
  raw_provider_payload jsonb not null default '{}'::jsonb,

  created_at timestamptz not null default now()
);
```

## Agent Guidance

- The bare minimum compatible logger table only needs `recipient_email`; the remaining columns can be added as optional/default-backed fields and will be populated when available.
- Use `message_type` and `outreach_step` to distinguish initial outreach from later follow-ups.
- Use `parent_outbound_email_id` to link a follow-up to the specific prior outreach message it extends.
- Use `root_outbound_email_id` to group a full outreach chain when needed.
- Treat `metadata` as the home for fields like `cc`, `bcc`, attachments, template context, and other sparse email metadata.
- Treat `raw_provider_payload` as the full provider response for replay, debugging, and future extraction.
- Do not build workflows that assume a drafted message was sent unless `observed_sent_at` or another verified sent signal is present.
