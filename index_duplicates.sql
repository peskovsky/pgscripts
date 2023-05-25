-- Copyright 2023 Peter Peskovsky
-- 
--    Licensed under the Apache License, Version 2.0 (the "License");
--    you may not use this file except in compliance with the License.
--    You may obtain a copy of the License at
-- 
--        http://www.apache.org/licenses/LICENSE-2.0
with indlist as (
  select
    pgn.nspname as schema_name,
    pgc.relname as table_name,
    inds.indexrelid::regclass as index_name,
    (
      select string_agg(attrs.attname,', ')
      from (
        select a.attname
        from pg_attribute a
        where a.attrelid = inds.indexrelid 
        order by a.attnum
      ) attrs
    ) as column_list,
    (
      --substring(indexdef from 'USING (\S+)')
      select distinct pgam.amname
      from
        unnest(inds.indclass) indclasses(classid),
        pg_opclass pgopc,
        pg_am pgam
      where pgopc.oid =indclasses.classid
        and pgam.oid = pgopc.opcmethod
    )
    as algorithm,
    case when exists (select 1 from pg_constraint c where c.conindid = inds.indexrelid::regclass)
         then true
         else false
    end as used_by_constraints,
    pg_relation_size(inds.indexrelid::regclass) as index_size
  from
    pg_index inds,
    pg_class pgc,
    pg_namespace pgn
  where pgc.oid = inds.indrelid
    and pgn.oid = pgc.relnamespace
    and pgn.nspname not in ('pg_catalog', 'information_schema', 'pg_toast')
)
select
  il1.schema_name || '.' || il1.table_name as table_name,
  il1.index_name,il1.column_list,il1.algorithm,il1.used_by_constraints,il1.index_size,
  il2.index_name as covering_index_name,
  il2.column_list as covering_index_cols,
  il2.algorithm as covering_index_algo
from
  indlist il1,
  indlist il2
where il2.schema_name = il1.schema_name
  and il2.table_name = il1.table_name
  and il2.index_name != il1.index_name
  and position (il1.column_list in il2.column_list) = 1
order by index_size desc
limit 20;
