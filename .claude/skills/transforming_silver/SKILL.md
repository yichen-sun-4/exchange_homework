---
name: transforming_silver
description: Build a silver (transformation-layer) dbt model in data-airflow on top of `stg_*` staging models — `dags/dbt/dp3/models/transformation/**`. Use when asked to "add a transformation model", "build a _daily / _monthly / _scd / _transactions model", "turn this staging table into something usable", "make an SCD for <entity>", "add an incremental model", "convert this SQL to dbt" (transformation half), or when a reporting model needs a reusable upstream. Covers naming and versioning, reading upstreams through `versioned_model`, SCD2 and incremental patterns, the docstring and schema-yml contract, and verifying against DEV before merge. Sequel to `staging_brownze`; hands off to `reporting_gold`.
---

# transforming_silver

**Silver is where the grain gets decided, and nothing downstream re-checks it.** Bronze hands
you one row per Mongo `_id`. Silver turns that into one row per *patient-day*, *patient-month*,
*entity-version* — and every gold model, dashboard and Mongo sink treats that grain as a fact.
A fan-out introduced here is invisible in a `SUM` three layers up.

**And your yml tests do not run in production.** The prod dbt DAG renders with
`render_exclude = ["resource_type:test", …]` when `cluster_name == "dbt-airflow"`
(`dags/dbt/dp3/snow_analytics_dbt_production.py:311-316`). Unit tests are excluded on *every*
cluster (`:307`, Cosmos has no converter). CI is `dbt parse` only (`.github/workflows/ci.yml:52-53`).
**Every test you write runs exactly as often as you run it by hand in DEV.** Prove the grain
before merge or nobody will.

All paths below are relative to `dags/dbt/dp3/`. Read `origin/main`, not your checkout.

---

## 0 · How to run SQL here

Same as bronze: `bash shared_tmp/dbt_dev_local.sh show --quiet --limit N --inline "…"`
(machine-local, gitignored; `--vars` is mandatory even to parse because
`models/transformation/patient_scd_v1_0_0.sql:107` calls `var('clinics_to_exclude')` unguarded).
It builds into `DBT_TRANSFORMATION_DEV` — **a shared schema** that local Airflow also rebuilds
from whatever branch the main checkout is on. Building from a worktree overwrites a table another
session may be reading. For a read-only proof, compile and stitch (§6).

---

## 1 · Name it, place it, pick the upstreams

| thing | house rule | count on origin/main (2026-11-20) |
|---|---|---|
| file | `<domain>_<entity>_<grain>_v1_0_0.sql`, flat in `models/transformation/` | 379 of 417 `.sql` are versioned; subfolders only `access/`, `act/`, `mnt/` |
| grain suffix | `_daily` `_weekly` `_monthly` `_scd` `_transactions` `_latest` | — |
| breaking change | new file `_v2_0_0`, keep v1 building | 1 model has a v2 (`uc_patient_cost_monthly_v2_0_0`) |
| stable name | unversioned file whose body is `{{ select_versioned_model('<name>', '1.0.0') }}` | 38 such wrappers (`uc_patient.sql`, `patient.sql`, …) |

**No `dim_` / `fct_` prefixes and no `_<schema>__models.yml`.** The generic `sql-to-dbt` command
suggests both; neither exists in this repo.

**Read every dbt upstream through `versioned_model` / `select_versioned_model`**
(`macros/versioned_model.sql:20`), never `ref('x_v1_0_0')` by hand — 327 of 417 models do.
The macro validates the semver and produces the `ref`, so the DAG edge exists.

**Read staging, not `source()`.** 50 models still call `source(...)` directly (43 of them
`MONGO_UNIFIEDCARE_PRODUCTION`) — legacy, do not copy it. If the collection has no `stg_` model,
go back to `staging_brownze`; reading `MONGO_UNIFIEDCARE_PROCESSED` puts case-sensitive camelCase
and the flattening into a model where nobody will look for it.

**Reuse the hubs before writing a join.** Most-referenced upstreams: `stg_chat_messages` (27),
`uc_patient_metrics_monthly` (25), `patient_medications_scd` (24), `uc_patient` (23),
`medical_org` (22), `employee` (19), `stg_measurements` (18), `enrolled_program_scd` (17).
`uc_patient_v1_0_0.sql:21` and `patient_v1_0_0.sql:13` already apply `filter_out_test_user`;
a model that reads patients from staging instead must apply it itself or test accounts leak in.

---

## 2 · Choose the materialization

`dbt_project.yml:80-85` defaults the folder to `view` with two post-hooks
(`add_dbt_processed_ts`, `apply_snowflake_tags`) into `DBT_TRANSFORMATION_SCHEMA`. Measured split:
**219 table, 173 view, 18 incremental, 2 ephemeral.**

- **View** when it is cheap and few models read it. **Table** when it is read by many.
  Production dbt is compile-bound, not execution-bound: Snowflake re-expands a view body (and
  every CTE) at each reference site. `uc_patient_metrics_daily` is a 419-line view read by 47 model
  files — each one re-expands its 16-way join (measured 2026-08-19: compile 3.5x execution). A hot upstream as a view costs every consumer.
- **Incremental: you must add `full_refresh=false`.** Both prod DAGs pass `"full_refresh": True`
  project-wide (`snow_analytics_dbt_production.py:338`, `snow_analytics_dbt_production_4pm_run.py:360`),
  so without the pin your table is dropped and rebuilt twice a day and the
  `{% if is_incremental() %}` branch is dead code. The flag cannot be removed — 
  `uc_patient_events_txn_v1_0_0.sql:709` depends on it. Precedent with the incident written in:
  `uc_patient_match_dim_v1_0_0.sql:67-73`. Only 3 of 18 incremental models carry the pin.
- **The pin's cost:** `on_schema_change` becomes the only drift path and a deliberate rebuild is
  a manual drop. Say so in the docstring.

---

## 3 · SCD2 — read history, not the snapshot

`convert_to_scd2(source, unique_keys, other_columns, backfill_ts_col='', updated_at_col='updated_at')`
(`macros/convert_to_scd2.sql:1`) emits CTEs ending in `scd_2`; the model is `WITH {{ … }} SELECT * FROM scd_2`.

**Feed it the `_historical` staging model.** It builds versions from rows it is given; a
current-snapshot staging model has one row per key, so you get one "version" per key whose
`valid_from` is the *last* update. 16 of 23 SCD models read a `_historical` upstream.
`organization_scd_v1_0_0` reads `stg_uc_organizations` (current) — measured 2026-11-20:
**38 rows, 38 ids, 0 closed versions.** An as-of join to a date before that `valid_from` drops
the entity. If no `_historical` view exists, that is a bronze change (§3 twin), not a silver one.

The macro documents its own hole: if the first record (created_at = updated_at) is missing from
history, that version is missing too. `backfill_ts_col` adds at most one synthetic row.

To read the current row: `{{ get_scd2_current(versioned_model('x_scd', '1.0.0')) }}`. **Always
pass `versioned_model(...)`, never a string** — the string branch at `macros/get_scd2_current.sql:10`
tests an undefined variable, so a bare string is emitted verbatim with no `ref` and no DAG edge.
All 16 existing calls pass `versioned_model`; keep it that way.

---

## 4 · Write the model

```sql
{{ config(materialized='table') }}

{# <name> — <one line: what question this answers>.

    GRAIN: one row per (<keys>). <Why it is unique — which upstream is already at
    that grain, which join is collapsed before it is read.>

    <Measured facts with the date measured. Which date column is in which timezone.
    Which count is a safe denominator. What a NULL means. What was deliberately
    not filtered, and why.> #}

WITH

source AS (
    {{ select_versioned_model('stg_x', '1.0.0') }}
),

...

SELECT * FROM final
```

Rules, each of which a model here has gotten wrong:

- **The docstring is the contract.** Benchmark: `apcm_inactive_monthly_v1_0_0.sql` (commit
  `938fdbbe`, PR #1401) — `GRAIN:`, the as-of date, the window, which timezone every date is in,
  and the measured effect of each judgement call. 147 models say `grain`; 158 of 379 have a
  docstring at all. Be in the first group.
- **Use plain `{# … #}` inside a SELECT list, never `{#- … -#}`.** The whitespace-strip eats the
  newline and joins the next line onto a `----- banner -----`, turning two columns into a SQL
  comment. `dbt parse` and `dbt run` both pass. Grep the *compiled* SQL for the column (§6).
- **One timezone per model, named.** `stg_measurements_v1_0_0:17` builds `local_measurement_date`
  in the *patient's* timezone; `clinic_events_transactions_v1_0_0:88` builds `checked_out_date_local`
  in the *clinic's*. Mixing either with a Pacific date moves events across a month boundary.
- **Every window / `QUALIFY ROW_NUMBER()` / `ASOF` ordering must be a total order.** Ordering by a
  DATE with several rows per day picks arbitrarily: a SIP report returned 511 then 490 on
  back-to-back runs of identical SQL. Add a timestamp and a stable id to the `ORDER BY`.
- **`DATEDIFF('month', a, b)` counts boundaries crossed**, not months elapsed — 41.8% of rows
  landed in the wrong tenure bucket on one query. Use days for tenure.
- **Booleans: `COALESCE(…, FALSE)`.** A NULL flag is dropped by both `WHERE NOT flag` and `COUNT_IF`.
- **Do not hardcode reference data in a `CASE`.** If it is a lookup, it is a seed.

---

## 5 · Document and test it

Put the entry in the domain yml beside the model (`<domain>_schemas.yml`); `schemas.yml` holds
11 models and is not the default. Use `data_tests:` (28 files) — `tests:` (12 files) is the old key.

What the folder actually uses: `not_null` 482, `accepted_values` 254,
`dbt_utils.expression_is_true` 93, `dbt_utils.unique_combination_of_columns` 85, `unique` 32.
**Composite grains are the norm here — the grain test is usually
`unique_combination_of_columns`, not `unique`.** Minimum: that test on the grain, `not_null` on
every key and FK. `severity: warn` only for a known, written-down exception.

Logic that can be stated as fixtures belongs in a `unit_tests_<model>.yml` (6 exist; see
`unit_tests_uc_patient_risk_gap_onsets.yml`, commit `a53c3533`). A cross-model invariant is a
singular test in `tests/assert_<claim>.sql` (246 exist). Remember: none of these run in prod.

**Fixtures must not encode the assumption under test.** A fixture built from what you believe
the data looks like proves the model agrees with you, not with the warehouse.

---

## 6 · Verify

Run the reconcile probes in `references/reconcile.sql` against the built DEV relation (or the
compiled SQL pasted into a subquery). In order:

1. **Grain** — `n_rows = n_distinct(grain)`. If not, the yml test will fail; find the fan-out.
2. **Fan-out per join** — row count before and after each join CTE.
3. **Population** — the entity count reconciles with the upstream's (patients in vs out).
4. **Determinism** — run twice, compare a hash of the whole output. Equal counts are not enough.
5. **Compiled columns** — `grep -c "AS <col>"` on `target/compiled/dp3/models/transformation/<model>.sql`
   for every column you added. Compiled output lands under `dags/dbt/dp3/target`, not repo root.

```bash
bash shared_tmp/dbt_dev_local.sh parse
bash shared_tmp/dbt_dev_local.sh run  --select <model>_v1_0_0
bash shared_tmp/dbt_dev_local.sh test --select <model>_v1_0_0
```

For a read-only proof without touching the shared DEV schema: `compile`, then substitute each
changed upstream's `ANALYTICS.<SCHEMA>_DEV.<model>` with `( <its compiled SQL> )`. Compiled refs are
**lowercase**, and every fragment opens `WITH source AS (…)`, so nested fragments collide —
prefix each fragment's CTE names (`source` → `f3_source`), except `scd_2`.

A `--select`-scoped run truncates three git-tracked CSVs via `on-run-end`
(`adhoc/testing/dws_report.csv`, `scripts/dp3_migration/inventory_rds_*.csv`).
`git restore` them before committing.

---

## 7 · After merge

- **A new model is scheduled the moment it merges.** Both prod DAGs build the whole project by
  exclusion; never add a `select=` to "make sure it runs" — it narrows the nightly to that list.
- **A merged view is not a rebuilt view.** Snowflake freezes a view's column list at creation; a
  downstream model reading your new column fails with `invalid identifier` until the view is
  rebuilt. Judge by `INFORMATION_SCHEMA.COLUMNS`, not by task state. `dbt_processed_ts` on a view
  is query time, not build time (`macros/add_dbt_processed_ts.sql:15-43`).
- **Grants do not survive the nightly by default.** A table survives only with
  `config(copy_grants=true)` (47 transformation models set it). A **view loses them anyway**: the
  folder post-hook re-issues `CREATE OR REPLACE VIEW` without `COPY GRANTS`
  (`add_dbt_processed_ts.sql:19`). A one-off `GRANT SELECT` on a silver view is gone by ~04:00 the
  next morning — grant at schema level (future grants) or make it a `copy_grants` table.

---

## Pitfalls

- **Trusting yml tests as a prod safety net.** Prod renders without them; CI only parses. §5.
- **Incremental without `full_refresh=false`.** Rebuilt twice daily; `is_incremental()` never true. §2.
- **SCD over a current snapshot.** One version per key, history starts at the last edit. §3.
- **`get_scd2_current('string')`.** No ref, no edge, builds against whatever exists. §3.
- **`{#- -#}` in a SELECT list.** Silently deletes columns. §4.
- **A DATE-only ordering in `QUALIFY` / `ASOF`.** Non-deterministic; equal counts hide it. §4.
- **Mixed timezones.** Patient-local, clinic-local and Pacific dates disagree at month edges. §4.
- **A hot upstream as a view.** Every consumer re-compiles its whole body. §2.
- **`unique` on a composite grain** — or no grain test at all. §5.
- **Reading `source()` or PROCESSED from silver** to skip a bronze change. §1.

---

## Verification before declaring done

- [ ] Grain stated in the docstring as `GRAIN: one row per (…)`, with why it is unique.
- [ ] `n_rows = n_distinct(grain)` measured in DEV, numbers quoted; fan-out checked per join.
- [ ] Every upstream read through `versioned_model` / `select_versioned_model`; no new `source()`.
- [ ] Materialization chosen deliberately; any incremental carries `full_refresh=false` and the
      docstring says how to rebuild it.
- [ ] Any SCD reads a `_historical` upstream, or the docstring says why it cannot.
- [ ] Every window / `QUALIFY` / `ASOF` ordering is total; two runs hash-identical.
- [ ] Every date column's timezone named; month logic uses one calendar.
- [ ] Added columns found by `grep` in the compiled SQL, not just the source.
- [ ] yml entry in the domain `*_schemas.yml` with `data_tests:` — grain test + `not_null` on keys;
      `dbt test` passes in DEV with counts quoted (it will not run anywhere else).
- [ ] Tracked CSVs restored; no `select=` added to a DAG.
- [ ] Stated which consumers need a rebuild or re-grant after merge.

For the commit and PR, hand off to `/commit`. For a model consumers or dashboards read directly,
hand off to `reporting_gold`.
