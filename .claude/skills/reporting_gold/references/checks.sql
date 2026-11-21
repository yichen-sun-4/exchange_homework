-- =====================================================================
-- Verification probes for a reporting (gold) model in data-airflow's dbt
-- project. Each numbered block is a SEPARATE query — run one at a time.
-- Aggregates only: never SELECT identifiable patient rows from here.
--
-- Substitute by hand:
--   <SCHEMA>    DASHBOARDS / QUICKSIGHT / UC_PORTAL / UNIKA_INSIGHTS (or *_DEV)
--   <RELATION>  the relation name from SKILL.md §1 — NOT the file name
--   <SILVER>    the fully-qualified silver relation it passes through
-- =====================================================================


-- ---------------------------------------------------------------------
-- 1. DOES THE RELATION EXIST UNDER THE NAME YOU COMPUTED?
--    ds_/qs_ are removed with .replace(), so a stray "ds_" inside the
--    name disappears too (ds_billing_records_athena_ehr ->
--    BILLING_RECORATHENA_EHR). Zero rows = wrong name, not missing build.
-- ---------------------------------------------------------------------
select table_schema, table_name, table_type,
       to_varchar(created, 'YYYY-MM-DD HH24:MI')      as created,
       to_varchar(last_altered, 'YYYY-MM-DD HH24:MI') as last_altered
from analytics.information_schema.tables
where table_schema = '<SCHEMA>'
  and table_name ilike '%<fragment>%';


-- ---------------------------------------------------------------------
-- 2. COLUMN LIST — the deployed truth, not the model file. A merged
--    column is absent here until the relation is rebuilt; on a view,
--    dbt_processed_ts is query-time and proves nothing.
-- ---------------------------------------------------------------------
select ordinal_position, column_name, data_type
from analytics.information_schema.columns
where table_schema = '<SCHEMA>'
  and table_name   = '<RELATION>'
order by ordinal_position;


-- ---------------------------------------------------------------------
-- 3. RECONCILE WITH SILVER — gold is a passthrough, so counts match.
--    Replace <grain cols> with the grain the header comment claims.
-- ---------------------------------------------------------------------
select
    (select count(*) from analytics.<SCHEMA>.<RELATION>)                    as n_gold,
    (select count(*) from <SILVER>)                                         as n_silver,
    (select count(distinct <grain cols>) from analytics.<SCHEMA>.<RELATION>) as n_grain;


-- ---------------------------------------------------------------------
-- 4. RATE WITHOUT A DENOMINATOR — how many rows show 0 because nothing
--    was measured? Any n_zero_unmeasured > 0 means the reader needs the
--    denominator column, and the 0 should probably be NULL.
-- ---------------------------------------------------------------------
select
    count(*)                                              as n_rows,
    count_if(<rate_col> = 0)                              as n_zero,
    count_if(<rate_col> = 0 and coalesce(<denom_col>,0) = 0) as n_zero_unmeasured,
    count_if(<rate_col> is null)                          as n_null
from analytics.<SCHEMA>.<RELATION>;


-- ---------------------------------------------------------------------
-- 5. GRANTS SURVIVE A REBUILD — run the day after the first nightly.
--    `created` moving means the object was replaced; grants survive only
--    with COPY GRANTS. Use SHOW GRANTS, not account_usage (it omitted
--    the dropped grants entirely on 2026-11-10).
-- ---------------------------------------------------------------------
show grants on table analytics.<SCHEMA>.<RELATION>;   -- or: on view


-- ---------------------------------------------------------------------
-- 6. HAND-CREATED, NOT dbt — for a relation with no model file. A
--    declared column list in the header over SELECT * is the latch that
--    breaks when upstream widens. Port to dbt; don't re-run the DDL.
-- ---------------------------------------------------------------------
select get_ddl('view', 'analytics.<SCHEMA>.<RELATION>');


-- ---------------------------------------------------------------------
-- 7. STALE DASHBOARD: WRONG SCHEMA OR STALE CACHE? Compare the figure
--    the UI shows against both. Matches _DEV exactly -> the app reads the
--    wrong schema. Matches an older prod value -> cache.
-- ---------------------------------------------------------------------
select
    (select count(*) from analytics.<SCHEMA>.<RELATION>)     as n_prod,
    (select count(*) from analytics.<SCHEMA>_DEV.<RELATION>) as n_dev;
