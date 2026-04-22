-- Idempotent schema for the run store. Safe to re-run any time.
--
-- Model: `runs` is an append-only log. Every row is a snapshot of a run's state
-- at a particular boundary; rows share the same `run_id` but have distinct
-- autoincrement ids. `initialize_run` writes a row with phase='started';
-- `write_state` writes a row with phase='ended' on each terminal/flush event.
-- Multi-agent runs naturally produce more than two rows per parent run.

create table if not exists public.runs (
  id bigint generated always as identity primary key,
  run_id text not null,
  phase text,
  graph_id text not null,
  agent_id text,
  agent_name text,
  parent_run_id text,
  status text not null,
  status_reason text,
  started_at text,
  ended_at text,
  runtime_instance_id text,
  last_heartbeat_at text,
  input_payload jsonb,
  final_output jsonb,
  terminal_error jsonb,
  current_node_id text,
  current_edge_id text,
  state_snapshot jsonb,
  metadata jsonb not null default '{}'::jsonb,
  created_at text not null
);

-- Existing-install migration: older schemas used run_id as the primary key
-- and had a FK from run_events.run_id -> runs.run_id. Detach both so we can
-- append multiple rows per run_id.
do $$
declare current_pk_column text;
begin
  select a.attname into current_pk_column
  from pg_index i
  join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey)
  where i.indrelid = 'public.runs'::regclass and i.indisprimary
  limit 1;

  if current_pk_column = 'run_id' then
    alter table if exists public.run_events drop constraint if exists run_events_run_id_fkey;
    alter table public.runs drop constraint if exists runs_pkey;
    alter table public.runs add column if not exists id bigint generated always as identity;
    alter table public.runs add primary key (id);
  end if;
end $$;

-- Additive column reconciliation for projects whose tables predate newer columns.
alter table if exists public.runs add column if not exists phase text;
alter table if exists public.runs add column if not exists agent_id text;
alter table if exists public.runs add column if not exists agent_name text;
alter table if exists public.runs add column if not exists parent_run_id text;
alter table if exists public.runs add column if not exists status_reason text;
alter table if exists public.runs add column if not exists started_at text;
alter table if exists public.runs add column if not exists ended_at text;
alter table if exists public.runs add column if not exists runtime_instance_id text;
alter table if exists public.runs add column if not exists last_heartbeat_at text;
alter table if exists public.runs add column if not exists input_payload jsonb;
alter table if exists public.runs add column if not exists final_output jsonb;
alter table if exists public.runs add column if not exists terminal_error jsonb;
alter table if exists public.runs add column if not exists current_node_id text;
alter table if exists public.runs add column if not exists current_edge_id text;
alter table if exists public.runs add column if not exists state_snapshot jsonb;
alter table if exists public.runs add column if not exists metadata jsonb not null default '{}'::jsonb;

create index if not exists runs_run_id_id_idx on public.runs (run_id, id desc);
create index if not exists runs_graph_id_created_at_idx on public.runs (graph_id, created_at desc);
create index if not exists runs_parent_run_id_idx on public.runs (parent_run_id);

create table if not exists public.run_events (
  id bigint generated always as identity primary key,
  run_id text not null,
  sequence_number bigint not null,
  event_type text not null,
  timestamp text not null,
  agent_id text,
  parent_run_id text,
  summary text not null,
  payload jsonb not null default '{}'::jsonb,
  metadata jsonb not null default '{}'::jsonb,
  unique (run_id, sequence_number)
);

alter table if exists public.run_events drop constraint if exists run_events_run_id_fkey;
alter table if exists public.run_events add column if not exists agent_id text;
alter table if exists public.run_events add column if not exists parent_run_id text;
alter table if exists public.run_events add column if not exists payload jsonb not null default '{}'::jsonb;
alter table if exists public.run_events add column if not exists metadata jsonb not null default '{}'::jsonb;

create index if not exists run_events_run_id_sequence_idx on public.run_events (run_id, sequence_number asc);
create index if not exists run_events_parent_run_id_idx on public.run_events (parent_run_id);

notify pgrst, 'reload schema';
