-- Create table equivalent to the old MySQL `a_dis`
create table if not exists public.a_dis (
  typename varchar(255),
  username varchar(255),
  createtime varchar(255),
  content text,
  url text,
  id varchar(50) primary key
);

-- Option B (recommended): anon key + RLS + insert-only policy
-- 用 anon key 写入时，一般以 role=anon 访问；这里只放行 INSERT，不放行 SELECT/UPDATE/DELETE。

alter table public.a_dis enable row level security;

-- Ensure API roles can access schema/table (RLS still applies)
grant usage on schema public to anon, authenticated;

-- Grant only INSERT (no SELECT/UPDATE/DELETE)
revoke all on table public.a_dis from anon, authenticated;
grant insert on table public.a_dis to anon, authenticated;

-- Insert-only policy (minimal validation to reduce garbage writes)
drop policy if exists "a_dis_insert" on public.a_dis;
create policy "a_dis_insert"
on public.a_dis
for insert
to anon, authenticated
with check (
  id is not null
  and length(id) > 0
  and length(id) <= 50
);
