-- Copyright 2023 Peter Peskovsky
-- 
--    Licensed under the Apache License, Version 2.0 (the "License");
--    you may not use this file except in compliance with the License.
--    You may obtain a copy of the License at
-- 
--        http://www.apache.org/licenses/LICENSE-2.0
select
   pgsai.schemaname,
   pgsai.relname,
   pgsai.indexrelname as index_name,
   (
    select string_agg(attrs.attname,', ')
    from (
      select a.attname
      from pg_attribute a
      where a.attrelid = pgsai.indexrelid
      order by a.attnum
    ) attrs
  ) as column_list,
  pg_size_pretty(pg_relation_size(pgsai.indexrelid::regclass)) as index_size,
  stats_reset.stats_reset as last_db_stats_reset
from
  pg_stat_all_indexes pgsai,
  (select stats_reset from pg_stat_database where datname = current_database()) stats_reset
where pgsai.idx_scan = 0
  and pgsai.schemaname not in ('pg_catalog', 'information_schema')
  and pgsai.relname not like 'pg_toast%'
order by pg_relation_size(pgsai.indexrelid::regclass) desc
limit 30;
