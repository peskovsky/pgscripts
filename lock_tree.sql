-- Copyright 2023 Peter Peskovsky
-- 
--    Licensed under the Apache License, Version 2.0 (the "License");
--    you may not use this file except in compliance with the License.
--    You may obtain a copy of the License at
-- 
--        http://www.apache.org/licenses/LICENSE-2.0
with recursive tree (lev,pid,root_pid,pid_string) as (
    --The first level: the processes that block other processes and are not blocked themselves.
  select
    1 as lev,
    all_blocking_pids.pid,
    all_blocking_pids.pid as root_pid,
    all_blocking_pids.pid::text as pid_string
  from
    (
      -- The list of processes that block any other process
      select distinct unnest(pg_blocking_pids(pid)) as pid
      from pg_locks
      where not granted
    ) all_blocking_pids
  where not exists (
      -- Getting those blocking processes that are not blocked by anybody
      select 1
      from pg_locks blocking_pids_locks
      where blocking_pids_locks.pid = all_blocking_pids.pid
        and not granted
    )
    or all_blocking_pids.pid = 0 -- prepared transaction case
  union all
  -- Next level includes the processes locked by the previous level.
  select
    tree.lev + 1,
    locks_nl.pid,
    tree.root_pid,
    tree.pid_string || '>' || locks_nl.pid::text
  from
    pg_locks locks_nl,  
    unnest(pg_blocking_pids(locks_nl.pid)) blocking_pids_nl (pid),
    tree
  where not locks_nl.granted
    and tree.pid = blocking_pids_nl.pid
    and tree.lev <= 1000
)
select
  rn_tree.pid_string,
  (
	case when rn_tree.pid != 0
         then (
           select transactionid::text
           from pg_locks tid_locks
           where tid_locks.pid = rn_tree.pid
             and tid_locks.locktype = 'transactionid'
             and tid_locks.mode = 'ExclusiveLock'
             and tid_locks.granted = true
         )
         else 'prep.trans.'
    end
  ) as tid,
  date_trunc('second',clock_timestamp() - pgsa.query_start) as query_time,
  case when rn_tree.lev > 1 then 'blocked' else pgsa.state || ' / ' || pgsa.wait_event end as state,
  pgsa.datname,
  (
    select string_agg(
         locks.mode
      || ' / ' || locks.locktype
      || ' ('
      || case
           when locks.locktype = 'transactionid'
           then    'tid = ' || locks.transactionid
                || coalesce(
                     (
                       select ', global XID = ' || gid
                       from pg_prepared_xacts prep_trans
                       where prep_trans.transaction=locks.transactionid
                     ),
                     ''
                   )
           when locks.locktype = 'relation'
           then locks.full_relation_name
           when locks.locktype = 'tuple'
           then locks.full_relation_name || ', ctid=(' || locks.page || ',' || locks.tuple || ')'
           else ''
         end
      || ')'
      , ' / '
    )
    from (
      select
        *,
        (
	      select
	           '['
	        || (case rels.relkind
	              when 'r' then 'ORD.TABLE'
	              when 'i' then 'INDEX'
	              when 'S' then 'SEQUENCE'
	              when 't' then 'TOAST.TABLE'
	              when 'v' then 'VIEW'
	              when 'm' then 'MAT.VIEW'
	              when 'c' then 'COMP.TYPE'
	              when 'f' then 'FOREIGN.TABLE'
	              when 'p' then 'PART.TABLE'
	              when 'I' then 'PART.INDEX'
	            end
	           )
	        || '] '   
	        || (select nspname from pg_namespace where oid = rels.relnamespace)
	        || '.'
	        || rels.relname
	        || ', oid = ' || pgl.relation
	      from pg_class rels
	      where rels.oid = pgl.relation
        ) as full_relation_name
      from pg_locks pgl
    ) locks
    where locks.pid = rn_tree.pid
      and locks.granted = false
  ) as lock_mode_type_info,
  regexp_replace(pgsa.query, E'[\\n\\r]+', ' ', 'g') as query_text, -- Making the query single-line
  pgsa.usename,
  pgsa.client_addr,
  pgsa.application_name,
  rn_tree.root_pid -- adding it here to make the aggregates like root_pid,count(*) possible.
from (
  select
    lt.*,
    row_number() over(partition by pid order by lev,pid_string) as rn
  from tree lt
) rn_tree
left outer join pg_stat_activity pgsa on (pgsa.pid = rn_tree.pid)
where rn_tree.rn = 1
order by pid_string;
