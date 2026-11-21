---
name: reporting_gold
description: Publish a silver transformation model to a consumer in data-airflow's dbt reporting layer — `models/reporting/{dashboards,quicksight,uc_portal,unika_insights}` — and wire the reader that binds to it (okr_dashboard in data-services, QuickSight, the Snowflake→Mongo sink, report DAGs under dags/reports). Use when asked to "add a dashboard table", "publish X to DASHBOARDS", "add a ds_ / qs_ / ip_ model", "expose this to the dashboard", "make a reporting model", "the dashboard can't see the new column", "sink this table to Mongo", "the prod dashboard is stale", "grant lost overnight", or when a reporting relation's name doesn't match any file. Covers picking the folder, the relation-name rule, the copy_grants and denominator contracts, schema yml and tests, consumer binding, and verifying against Snowflake before merge.
---

# reporting_gold

**Gold holds no logic. It is a contract with a reader you do not control.** The logic lives
in the silver model (`transforming_silver`); a reporting model selects it, names it, and
promises the reader a relation name, a column list, a grain, and a grant. Every failure
below is one of those four promises breaking silently — the reader gets a 503, a smaller
plausible number, or a 0.0% that means "not measured".

Measured on origin/main 2026-11-20:

| folder | schema env var (prod value) | models | materialized | relation name | tests |
|---|---|---|---|---|---|
| `reporting/dashboards` | `DBT_DASHBOARDS_SCHEMA` (`DASHBOARDS`) | 157 (156 `ds_`) | 140 table | `ds_` **replaced away** | 54 of 157 in yml |
| `reporting/quicksight` | `DBT_QUICKSIGHT_SCHEMA` (`QUICKSIGHT`) | 124 `qs_` | mostly view | `qs_` **replaced away** | none |
| `reporting/uc_portal` | `DBT_UC_PORTAL_SCHEMA` (`UC_PORTAL`) | 32 `ip_` | 32 table | file name as-is | 32 of 32 in yml |
| `reporting/unika_insights` | `DBT_UNIKA_INSIGHTS_SCHEMA` (`UNIKA_INSIGHTS`) | 21 | 4 incremental | `_v1_0_0` from the filename | 10 of 21 in yml |

Folder → schema is `dags/dbt/dp3/dbt_project.yml:93-111`; prod values are
`dags/dbt/dp3/dbt_env.prod.json`. **New dashboard work goes in `reporting/dashboards`.**
QuickSight is a frozen display contract: quoted human labels (`"Billable Hours"`), no
tests, and `dbt_project.yml:95-98` deliberately withholds `dbt_processed_ts` from it
because a new column "may break the QS datasets". Add a `qs_` twin only if a QuickSight
dataset will actually read it.

Worked examples: `f104c18c` / PR #1409 (`ds_on_call_rd_metrics_daily` — one-file
passthrough of a silver model, the `qs_` twin beside it) and `88aa13c0` / PR #1523 (a
DPP funnel table with its yml).

---

## 1 · Name it — the file name is not the relation name

`dags/dbt/dp3/macros/built_in_overrides.sql` overrides `generate_alias_name` with an
if/elif chain; the first match wins:

1. `alias=` in the model's `config()` → verbatim (5 quicksight models keep `QS_` this way).
2. in the QuickSight schema → `node.name.replace("qs_", "")`
3. in the DASHBOARDS schema → `node.name.replace("ds_", "")`
4. dbt `version:` → `_v<x_y_z>` appended. **No model uses dbt `versions:`**; every
   `_v1_0_0` you see is typed into the filename.

**`.replace()` removes every occurrence, not the prefix.** `ds_billing_records_athena_ehr`
is live in Snowflake as `ANALYTICS.DASHBOARDS.BILLING_RECORATHENA_EHR` (verified
2026-11-20). Before naming a `ds_`/`qs_` model, check the rest of the name does not contain
`ds_` or `qs_` (`records_`, `cards_`, `fields_`, `needs_`, `holds_`…). If it does, rename it
or set `alias=`.

Version the filename (`ds_<thing>_v1_0_0`) when a reader will pin to it — 52 of 157
dashboards models do. A breaking column change is a new `_v2_0_0` file beside the old one,
not an edit, because the reader's SQL names the old relation.

The full resolution rules (seeds, snapshots, blast radius) live in data-airflow's
`dashboard-lineage-audit` skill — use it before renaming or deleting anything here.

---

## 2 · Read silver through the house macro

```sql
-- <RELATION_NAME> -- <one line: what a row is>.
-- Lands as ANALYTICS.DASHBOARDS.<RELATION_NAME>.
--
-- Pass-through of <silver_model>_v1_0_0, which holds all the logic.
-- Grain: one row per (<cols>). <What NULL means here. Which column is the denominator.>

{{ config(
    materialized='table',
    copy_grants=true
) }}

WITH

source AS (
    {{ select_versioned_model('<silver_model>', '1.0.0') }}
),

renamed AS (
    SELECT
        <explicit column list, snake_case>
    FROM source
)

SELECT * FROM renamed
```

`select_versioned_model` (`macros/versioned_model.sql:20`) is `SELECT * FROM ref('<name>_v1_0_0')`
with a semver check. 133 of 334 reporting models use it.

Rules, each from something that went wrong:

- **`copy_grants=true` on every model, views included.** 150 of 157 dashboards models set
  it; 0 of 124 quicksight models do. Nothing in the project uses dbt `grants:`, so every
  reader grant is a manual `GRANT`, and the nightly `CREATE OR REPLACE` drops it unless the
  statement says `COPY GRANTS`. 2026-11-10: two of three grants made the day before were
  gone by morning with nobody revoking anything. PR #1409's message says `copy_grants` does
  "not apply" to views — **wrong**: dbt-snowflake's `relations/view/create.sql` emits
  `copy grants` for views too. **Except under `unika_insights/`**: that folder runs the
  `add_dbt_processed_ts()` post-hook (`dbt_project.yml:109-110`), which re-issues
  `CREATE OR REPLACE VIEW` without `COPY GRANTS` (`macros/add_dbt_processed_ts.sql:19`), so
  a *view* there loses its grants whatever its config says. Make it a table, or grant at
  schema level. Same trap as silver's views — see `transforming_silver`.
- **Logic stays in silver.** If you're writing a join, a window, or a CASE that defines a
  metric, it belongs in `transforming_silver`; gold renames and selects. Twins (`ds_` +
  `qs_`) then reconcile by construction.
- **Explicit column list, never a bare `SELECT *` to the reader.** Snowflake freezes a view's
  column list at creation, and a hand-written view with a declared column list over
  `SELECT a.*` breaks outright when upstream widens (`OKR_DASHBOARD_APCM_METRICS`, down until
  2026-09-17: "declared 28 column(s), but view query produces 29").
- **Every rate ships with its denominator column.** A reader seeing `A1C_CONTROL_RATE = 0.0`
  can't tell "nobody controlled" from "nobody measured" without `A1C_VALUE_CIRCLE_CNT` next
  to it. In `CLINICAL_OUTCOME_*` six separate wrong conclusions came from rates read without
  denominators — 1,621 patients with zero A1C measurements reading as 0% control, eye/foot
  exam at 0.00% in all 12 months because the field is unpopulated. Keep unmeasured as
  `NULL`, not `0`, and say so in the header.
- **Name the cohort the percentage is over.** `uc_patient_signals` is 0.30% of enrolled but
  18.5% of the rule's eligible cohort for the same signal. If a reader will compute a share,
  publish the eligible count, not just the roster.
- **Incremental needs `full_refresh=false`.** Both prod dbt DAGs pass `full_refresh: True`
  project-wide (`snow_analytics_dbt_production.py:338`, `..._4pm_run.py:360`), so without the
  config an incremental model is rebuilt from scratch every run and its history is lost.
  Precedent: the 3 `unika_insights/uc_patient_bp_*_scores*` models.

---

## 3 · Document and test it

Add the entry to the folder's domain yml (`ds_<domain>_schemas.yml`; dashboards has 11 yml files).
103 of 157 dashboards models have no yml entry — don't add to that.

- Model description: the grain, what NULL means, which column is the safe denominator, and
  who reads it (the dashboard page / sink collection / report).
- `not_null` on the grain columns; `unique` (or
  `dbt_utils.unique_combination_of_columns`) on the grain once you've measured it. Both
  `data_tests:` (196) and `tests:` (41) appear in dashboards yml; use `data_tests:`.
- `+persist_docs` is on project-wide (`dbt_project.yml:68-70`), so the description becomes
  the Snowflake table/column comment. A semantic view or databot agent will read it — write
  it for them.

---

## 4 · Bind the reader

A merged model is scheduled automatically — both prod DAGs build the whole project with no
`select=`. Don't add one "to make sure it runs"; it narrows the whole nightly. What does
*not* happen automatically is the reader:

| reader | how it binds | trap |
|---|---|---|
| **okr_dashboard** (data-services `apps/okr_dashboard/lib/dashboards/<area>/schema.ts`) | one env var **per area** (`DPP_SCHEMA_ENV`, `CLINICAL_OUTCOME_SCHEMA`, … 13+ names) | Prod read `DASHBOARDS_DEV` for 9 days (fixed 2026-10-05, PR #231): the env var was set in no deployed config and the default was `dev`. `DASHBOARDS_DEV` had stopped rebuilding, so the panel showed **smaller plausible numbers**, not an error. A new area must default to `prod`, and a test asserting `toContain('DASHBOARDS')` passes on `DASHBOARDS_DEV` — assert `not.toContain('DASHBOARDS_DEV')`. |
| **Snowflake→Mongo sink** (`dags/snowflake/ai_team/snowflake_to_mongo_sync/`) | Airflow Variable `snowflake_to_mongo_sync_tables` (glob list, `:79`) **and** a semantic view with `primary_key` | No semantic-view key → skipped into `sf_sync_skipped`. Keys resolve against `dbt_env.prod.json` (`:145`), so a `_DEV`/`_LOCAL` object never matches. Rows go `SELECT *` (`:1522`) with **UPPERCASE** field names; a single-column key is moved into `_id` (`:724`), a composite becomes `_id = {…}` sorted by name (`:726`) and Mongo matches it by field order. Collection is `sf_<db_schema_table>` lowercased (`:343`). |
| **Report DAGs** (`dags/reports/sql/*.sql`) | hardcoded fully-qualified prod names | They read `analytics.dbt_transformation.*` / `dbt_staging.*` directly — no `ref()`, so dbt lineage and the reporting layer can't see them. Renaming or re-versioning a silver model breaks a report with no dbt error. `git grep -i <relation> dags/reports` before any rename. |
| **QuickSight** | dataset in the QuickSight UI, outside git | Column renames break the dataset; that is why `qs_` models carry display labels and never gain columns casually. |

The semantic view that the sink and databot read is the next step — see the handoff at the end.

---

## 5 · Verify

Run `references/checks.sql` (one block at a time). Locally, build into the dev schema:

```bash
bash shared_tmp/dbt_dev_local.sh parse
bash shared_tmp/dbt_dev_local.sh run  --select <model>
bash shared_tmp/dbt_dev_local.sh test --select <model>
```

That writes to `DASHBOARDS_DEV` etc. `dags/dbt/dp3/dbt_env_guard.py` fails the DAG parse if a
non-prod cluster's dbt_env points at a schema without `_DEV`. **A new reporting folder
means a new `+schema` env var**, which must be added to `ENV_VAR_SCHEMA_KEYS`
(`dbt_env_guard.py:41`) and all three `dbt_env.*.json` files —
`tests/dags/test_dbt_env_guard.py` fails if `dbt_project.yml` drifts from the guard.

Reconcile against silver: row count and grain-distinct count equal the upstream's, and the
column list equals what you declared.

---

## Pitfalls

- **Searching for `<RELATION>.sql`.** The `ds_`/`qs_` prefix is not in the relation, and
  `.replace()` can eat the middle of the name. §1.
- **No `copy_grants`.** The reader's role works today and fails tomorrow at ~03:00; reads as
  "the credential broke". A one-off `GRANT` is not a fix. Diagnose with `created` (moved =
  replaced = grant lost), and use `show grants` — `account_usage.grants_to_roles` showed no
  row at all for the dropped grants.
- **A green upstream task proves nothing.** Cosmos runs one dbt invocation per model; a
  silver view built from pre-merge code goes green, then gold, built minutes later from
  post-merge code, fails on `invalid identifier` *in the gold SQL*. Judge by
  `information_schema.columns`, not task state. On a view, `dbt_processed_ts` is
  `CURRENT_TIMESTAMP` evaluated at query time — it always looks fresh.
- **"Not in dbt, so not ours."** `ANALYTICS.DASHBOARDS` holds 211 objects against 157 model
  files (2026-11-20). Some are hand-created views with frozen column lists that break when
  silver widens. `select get_ddl('view', '<fqn>')` before assuming; port to dbt rather than
  re-running the DDL.
- **A 0.0% rate.** Almost always "not measured". Publish the denominator, keep NULL as NULL,
  and don't let a reader rank on a rate built from fewer than ~30.
- **Stale dashboard ≠ cache.** If the UI matches the `DASHBOARDS_DEV` figure exactly, it is
  reading the wrong schema. Cache staleness shows an *older prod* value, never the dev one.
- **Editing a column a reader binds to.** Report SQL, QuickSight datasets, and Mongo field
  names are outside dbt's lineage. Version up instead.
- **Incremental without `full_refresh=false`.** The `is_incremental()` branch is dead code in
  prod.

---

## Verification before declaring done

- [ ] Folder chosen deliberately (dashboards by default); relation name computed with the
      §1 rule and stated — and confirmed it contains no stray `ds_` / `qs_`.
- [ ] Model is a passthrough of a silver model via `select_versioned_model`; no metric logic
      in gold.
- [ ] `copy_grants=true` in `config()`; incremental models also carry `full_refresh=false`.
- [ ] Header comment states the landing relation, grain, NULL meaning, and denominator.
- [ ] Every rate column has its denominator column next to it.
- [ ] yml entry with description and grain tests; `dbt parse` clean; built and tested in
      the `_DEV` schema, counts quoted.
- [ ] Row count and grain-distinct count reconcile with the silver model.
- [ ] Reader binding stated: which dashboard area / env var (defaulting to prod), which
      sink glob + semantic view, or which report SQL — and that reader verified against the
      prod relation after the nightly run, not just `_DEV`.
- [ ] `git grep -i` for the relation across `dags/reports` and data-services before any
      rename or column removal.

For a semantic view over this model (sink key, databot, Cortex), hand off to
`drafts/semantic_view.md`. For the silver model underneath, see `transforming_silver`.
