-- =====================================================================
-- Reconcile probes for a silver (transformation) model, run AFTER the
-- model is built in DBT_TRANSFORMATION_DEV — or against its compiled SQL
-- pasted into a subquery (target/compiled/dp3/models/transformation/…).
--
-- Each numbered block is a SEPARATE query; `dbt show --inline` takes one.
--     bash shared_tmp/dbt_dev_local.sh show --quiet --limit 50 --inline "<one block>"
--
-- Aggregates only. Never select identifiable patient rows from here.
-- =====================================================================


-- ---------------------------------------------------------------------
-- 1. GRAIN — n_rows must equal n_grain. If not, the yml grain test fails
--    and every SUM downstream is inflated. Concatenate composite keys
--    with a separator that cannot appear in them.
-- ---------------------------------------------------------------------
select
    count(*)                                                as n_rows,
    count(distinct <key1> || '|' || <key2>)                 as n_grain,
    count_if(<key1> is null or <key2> is null)              as n_null_key
from analytics.dbt_transformation_dev.<model>_v1_0_0;


-- ---------------------------------------------------------------------
-- 2. FAN-OUT PER JOIN — paste the model's CTEs, then count each one.
--    A count that rises across a LEFT JOIN is a fan-out; one that falls
--    across an INNER JOIN is a silent population cut. Both need a reason
--    in the docstring.
-- ---------------------------------------------------------------------
with
/* … the model's CTEs, verbatim … */
counts as (
    select 'base'       as step, count(*) as n from base
    union all select 'after_join_1', count(*) from joined_1
    union all select 'final',        count(*) from final
)
select * from counts;


-- ---------------------------------------------------------------------
-- 3. POPULATION — entities in vs out. A patient model that loses
--    patients relative to its spine must say which and why.
-- ---------------------------------------------------------------------
select
    (select count(distinct patient_id) from analytics.dbt_transformation_dev.uc_patient_v1_0_0) as n_spine,
    (select count(distinct patient_id) from analytics.dbt_transformation_dev.<model>_v1_0_0)   as n_model;


-- ---------------------------------------------------------------------
-- 4. DETERMINISM — run twice (two separate builds or two executions of
--    the compiled SQL) and compare. Equal counts are NOT sufficient: a
--    DATE-only QUALIFY ordering yields identical counts and different
--    rows. The hash must match.
-- ---------------------------------------------------------------------
select
    count(*)                 as n_rows,
    hash_agg(*)              as content_hash
from analytics.dbt_transformation_dev.<model>_v1_0_0;


-- ---------------------------------------------------------------------
-- 5. DEPLOYED COLUMNS — the view in the warehouse, not the file. A
--    merged view is not a rebuilt one; Snowflake freezes the column list
--    at CREATE. Run against DBT_TRANSFORMATION (prod) after merge too.
-- ---------------------------------------------------------------------
select ordinal_position, substr(column_name, 1, 30) as col, data_type
from analytics.information_schema.columns
where table_schema = 'DBT_TRANSFORMATION_DEV'
  and table_name   = upper('<model>_v1_0_0')
order by ordinal_position;


-- ---------------------------------------------------------------------
-- 6. SCD SANITY — an SCD with zero closed versions is a snapshot, not
--    history (it was fed a current-state staging model). Measured on
--    organization_scd_v1_0_0, 2026-11-20: 38 rows, 38 ids, 0 closed.
-- ---------------------------------------------------------------------
select
    count(*)                         as n_rows,
    count(distinct <key>)            as n_keys,
    count_if(not is_current)         as n_closed_versions,
    count_if(is_current)             as n_current,
    min(valid_from)                  as first_valid_from
from analytics.dbt_transformation_dev.<model>_scd_v1_0_0;
