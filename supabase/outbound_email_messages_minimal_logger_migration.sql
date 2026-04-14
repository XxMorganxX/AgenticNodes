do $$
begin
  if to_regclass('public.outbound_email_messages') is null then
    raise notice 'Skipping outbound_email_messages migration because public.outbound_email_messages does not exist.';
    return;
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'outbound_email_messages'
      and column_name = 'mailbox_account'
  ) then
    execute 'alter table public.outbound_email_messages alter column mailbox_account drop not null';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'outbound_email_messages'
      and column_name = 'sales_approach'
  ) then
    execute 'alter table public.outbound_email_messages alter column sales_approach drop not null';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'outbound_email_messages'
      and column_name = 'message_type'
  ) then
    execute 'alter table public.outbound_email_messages alter column message_type set default ''initial''';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'outbound_email_messages'
      and column_name = 'drafted_at'
  ) then
    execute 'alter table public.outbound_email_messages alter column drafted_at set default now()';
  end if;
end
$$;
