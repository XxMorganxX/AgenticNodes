create table if not exists public.outbound_email_messages_dev (
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

  parent_outbound_email_id uuid references public.outbound_email_messages_dev(id) on delete set null,
  root_outbound_email_id uuid references public.outbound_email_messages_dev(id) on delete set null,

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

create table if not exists public.inbound_email_messages_dev (
  id uuid primary key default gen_random_uuid(),
  outbound_email_id uuid references public.outbound_email_messages_dev(id) on delete set null,

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
