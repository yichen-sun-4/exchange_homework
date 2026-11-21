-- =====================================================================
-- Profiling probes for a Mongo collection in MONGO_UNIFIEDCARE.PROCESSED,
-- run BEFORE writing the PRODUCTION view DDL.
--
-- Each numbered block is a SEPARATE, self-contained query. `dbt show --inline`
-- takes one statement at a time — do not paste the whole file.
--
--     bash shared_tmp/dbt_dev_local.sh show --quiet --limit 50 --inline "<one block>"
--
-- Substitute the collection name by hand: lower-case in `processed.<x>`,
-- upper-case in INFORMATION_SCHEMA predicates. Column names inside the
-- collection are camelCase AND case-sensitive — keep every double quote.
--
-- Verified against MONGO_UNIFIEDCARE on 2026-10-06.
-- =====================================================================


-- ---------------------------------------------------------------------
-- 1. DISCOVERY — PROCESSED collections with no PRODUCTION view.
--    Measured 2026-10-06: 326 rows, 96,503,890 rows of data behind them.
--    Most are scratch / _TEMP_ / app-internal. Pick by consumer, not size.
-- ---------------------------------------------------------------------
with processed as (
    select table_name, row_count
    from mongo_unifiedcare.information_schema.tables
    where table_schema = 'PROCESSED'
      and table_type   = 'BASE TABLE'
      and table_name not like '%\_HISTORICAL'
      and table_name not like '%\_STAGE'
      and table_name not in ('MANIFEST', 'SYNC_LOG')
),
production as (
    select table_name
    from mongo_unifiedcare.information_schema.views
    where table_schema = 'PRODUCTION'
)
select
    substr(p.table_name, 1, 18) as collection,
    p.row_count
from processed p
left join production v
       on v.table_name = p.table_name
where v.table_name is null
  and p.row_count > 0
order by p.row_count desc;


-- ---------------------------------------------------------------------
-- 2. SHAPE — every column, its inferred type, and whether the
--    _historical twin exists. OBJECT / ARRAY / VARIANT columns are the
--    ones that need a decision in the view; everything else is a cast.
-- ---------------------------------------------------------------------
select
    ordinal_position              as pos,
    substr(column_name, 1, 22)    as col,
    data_type
from mongo_unifiedcare.information_schema.columns
where table_schema = 'PROCESSED'
  and table_name   = '<COLLECTION>'
order by ordinal_position;


-- ---------------------------------------------------------------------
-- 3. GRAIN — is it really one row per _id? The base PROCESSED table is a
--    current snapshot, so n_rows should equal n_ids. If it does not, the
--    view needs dedup and the staging model cannot claim `unique`.
-- ---------------------------------------------------------------------
select
    count(*)                                  as n_rows,
    count(distinct "_id":"oid"::varchar)      as n_ids,
    count_if("_id" is null)                   as n_id_null,
    min(ingest_date)                          as first_ingest,
    max(ingest_date)                          as last_ingest
from mongo_unifiedcare.processed.<collection>;


-- ---------------------------------------------------------------------
-- 3.5 ROW KINDS — is this collection one entity, or does the writer keep
--    its own bookkeeping here too? Substitute the discriminator (often
--    `kind` or `type`) and the field that identifies a real record.
--    Measured on uc_communication_coverage: 88 rows, 2 coverage segments,
--    86 sync state (QUARANTINE 66 / CHECKPOINT 19 / READ_BARRIER 1).
--    Expose the discriminator in staging; do NOT filter in the view, and
--    do NOT not_null-test a column the bookkeeping rows leave NULL.
-- ---------------------------------------------------------------------
select
    coalesce("<kindCol>", '<null>')                 as kind,
    count(*)                                        as n_rows,
    count_if("<identifyingCol>" is not null)        as n_identified,
    substr(min("_id":"oid"::varchar), 1, 30)        as sample_id
from mongo_unifiedcare.processed.<collection>
group by 1
order by n_rows desc;


-- ---------------------------------------------------------------------
-- 3.6 IS _current ACTUALLY CURRENT? — the SNAPSHOT_FROM_FULL_EXPORT test.
--    _historical inserts on (_id, updatedAt) only, and _current derives
--    from it, so a rewrite that does not move updatedAt is DISCARDED --
--    in FULL mode too. Every row landing on exactly 00:00:00 is the
--    date-granularity trap: the stamp advances day to day but loses every
--    intra-day rewrite, so a shape probe shows nothing wrong. A healthy
--    per-document stamp shows scattered wall-clock times.
--    If trapped, add the collection to SNAPSHOT_FROM_FULL_EXPORT and skip
--    the _historical twin. See SKILL.md §2.5.
-- ---------------------------------------------------------------------
select
    to_varchar("updatedAt":"date"::datetime, 'YYYY-MM-DD HH24:MI:SS') as upd,
    count(*)                                                          as n_rows
from mongo_unifiedcare.processed.<collection>
group by 1
order by 1 desc;


-- ---------------------------------------------------------------------
-- 4. DATE SHAPE — THE MANDATORY CHECK. Repeat per date column.
--    `"col":"date"::datetime` is correct ONLY when shape_object wins.
--      shape_flat    > 0  -> cast as "col"::datetime
--      shape_double  > 0  -> {"date":{"date":...}}  -> house cast = SILENT NULL
--      shape_numlong > 0  -> {"date":{"numberLong"}} -> see sql/measurements.sql:94
--    type-agnostic: works whether the column inferred as OBJECT or VARCHAR.
-- ---------------------------------------------------------------------
select
    count(*)                                                        as n_rows,
    count_if("<dateCol>" is not null)                               as n_set,
    count_if(left(to_varchar("<dateCol>"), 1) <> '{')               as shape_flat,
    count_if(left(to_varchar(
        try_parse_json(to_varchar("<dateCol>")):"date"), 1) = '{')  as shape_double,
    count_if(to_varchar(
        try_parse_json(to_varchar("<dateCol>")):"date")
        ilike '%numberLong%')                                       as shape_numlong,
    count_if(try_to_timestamp(replace(to_varchar(
        try_parse_json(to_varchar("<dateCol>")):"date"), 'T', ' '))
        is not null)                                                as castable_object_form
from mongo_unifiedcare.processed.<collection>;


-- ---------------------------------------------------------------------
-- 5. NESTED OBJECT — which keys exist and how often, before deciding
--    what to project flat alongside the ::variant passthrough.
--    A key present on <50% of rows is usually not worth a column.
-- ---------------------------------------------------------------------
select
    substr(f.key, 1, 24)                      as nested_key,
    count(*)                                  as n_present,
    round(100 * count(*) / max(t.total), 1)   as pct_of_rows,
    substr(max(to_varchar(f.value)), 1, 18)   as sample_value
from mongo_unifiedcare.processed.<collection> c,
     lateral flatten(input => c."<objectCol>") f,
     (select count(*) as total from mongo_unifiedcare.processed.<collection>) t
group by f.key
order by n_present desc;


-- ---------------------------------------------------------------------
-- 6. ARRAY — element count distribution. Decides whether the array is
--    worth flattening downstream at all, and whether a QUALIFY-pick-one
--    in the view would silently drop data.
-- ---------------------------------------------------------------------
select
    array_size("<arrayCol>")  as n_elements,
    count(*)                  as n_rows
from mongo_unifiedcare.processed.<collection>
group by 1
order by 1;


-- ---------------------------------------------------------------------
-- 7. SOFT DELETE — expose as is_deleted, never filter on it in the view.
-- ---------------------------------------------------------------------
select
    coalesce("deleted"::boolean, false) as is_deleted,
    count(*)                            as n_rows
from mongo_unifiedcare.processed.<collection>
group by 1;


-- ---------------------------------------------------------------------
-- 8. RECONCILE — run AFTER writing the view body, with the body pasted
--    verbatim into the subquery and the CREATE stripped off. n / n_ids
--    must match probe 3; each date's non-null count must match probe 4.
-- ---------------------------------------------------------------------
select
    count(*)                            as n,
    count(distinct <entity>_id)         as n_ids,
    count_if(created_at is not null)    as n_created,
    count_if(updated_at is not null)    as n_updated
from ( /* the SELECT body of the view, verbatim */ );
