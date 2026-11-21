---
name: staging_bronze
description: Wire a Mongo collection that only exists in MONGO_UNIFIEDCARE.PROCESSED through to a dbt staging model — the PRODUCTION ODS view DDL, the sources.yml entry, the staging model, and its schema yml. Use when asked to "ingest a mongo collection", "add an ODS view", "expose <collection> in PRODUCTION", "add a staging model for <collection>", "that table is only in PROCESSED", or when a dbt model needs a source that has not been wired yet. Covers finding unwired collections, profiling date/oid/nested shapes before writing a single cast, the four-file change, and verifying against Snowflake and dbt before merge.
---

# mongo-collection-ingest

**PROCESSED is free. PRODUCTION is not.** Every Mongo collection lands in
`MONGO_UNIFIEDCARE.PROCESSED` nightly with no code change — `snow_mongo_uc_sync.py`
enumerates `list_collection_names()` and maintains its own manifest. So a table can sit
there, fresh and populated, with zero references anywhere in this repo. **Searching git for
a collection name finds nothing and proves nothing.**

What is missing is the ODS view, and the wiring for it is a filename. At
`dags/snowflake/mongo_unifiedcare/snow_mongo_uc_sync.py:2560`:

```python
for suffix in ("", "_historical"):
    view_ddl_file = f"{BASE_PATH}/sql/{collection_name}{suffix}.sql"
    if os.path.exists(view_ddl_file):
        with open(view_ddl_file, "r", encoding="utf-8") as file:
            view_query = file.read()
        snow_hook.run(view_query)
```

That is the entire PRODUCTION mechanism — an `os.path.exists` probe on two literal paths,
then the file content sent to Snowflake byte-for-byte. **Absence of the file, not absence of
data, is why an ODS view is missing.**

**The usual change is four files and zero code:**

| file | why |
|---|---|
| `dags/snowflake/mongo_unifiedcare/sql/{collection}.sql` | the ODS view DDL |
| `dags/dbt/dp3/models/sources.yml` | register under `MONGO_UNIFIEDCARE_PRODUCTION` |
| `dags/dbt/dp3/models/staging/mongodb/stg_{collection}_v1_0_0.sql` | the staging model |
| `dags/dbt/dp3/models/staging/mongodb/staging_core_schemas.yml` | its description + tests |

Worked examples: commit `b1421ccc` (one collection, minimal) and PR #1152 / `dcb58a56`
(four collections, with `_historical` twins).

**There is sometimes a fifth file, and it is code.** If the documents are rewritten in place
without `updatedAt` advancing *per rewrite*, `_current` is already wrong before your view
runs and no amount of view SQL fixes it — the collection needs one more entry in
`SNAPSHOT_FROM_FULL_EXPORT`. **Settle this in §2.5, before writing the view.** It is
invisible in the finished SQL, so no reviewer will catch it for you. Worked example:
`uc_communication_coverage`.

---

## 0 · How to run SQL here

Everything below needs a warehouse. In order of preference:

```bash
bash shared_tmp/dbt_dev_local.sh show --quiet --limit 50 --inline "select ..."
```

That host wrapper is gitignored and machine-local; if it is absent, use
`shared_tmp/dbt_dev.sh` from inside the Airflow worker container, or the Snowflake MCP if
this session has one. Three `dbt show` quirks that will waste a round trip each:

- A trailing `limit` **inside** the inline SQL is a syntax error. Use `--limit`.
- More than 5 rows requires `--limit`.
- Output columns truncate at ~20 chars — wrap long names in `substr(x,1,18)`.

---

## 1 · Find the collection

If you were handed a name, skip to §2. To sweep for candidates, run **query 1** of
`references/probe.sql`. That file is a set of separate statements, one per block — `dbt show
--inline` takes one at a time, so paste a single block, never the whole file.

Measured 2026-10-06:
**326 PROCESSED collections have no PRODUCTION view, holding 96.5M rows.** The list is not a
backlog — most of those are scratch tables, `_TEMP_*`, one-off imports, and app-internal
queues that nobody should model. **Pick by a consumer that exists, never by row count.**

Then confirm it is genuinely unwired, since the three layers can disagree:

```bash
ls dags/snowflake/mongo_unifiedcare/sql/{collection}*.sql     # ODS file?
grep -n "{COLLECTION}" dags/dbt/dp3/models/sources.yml        # registered?
ls dags/dbt/dp3/models/staging/mongodb/stg_{collection}*      # staged?
```

---

## 2 · Profile before you write a single cast

Run every query in `references/probe.sql` against the collection. This is the step that
decides whether the model is correct, and it is the one that gets skipped.

**The date-shape check is mandatory and is the highest-risk thing in this procedure.**
`bson.json_util` serializes dates four different ways across collections, and the house cast
`"col":"date"::datetime` is correct for exactly one of them:

| shape | example | house cast |
|---|---|---|
| object | `{"date": "2026-01-02T…"}` | ✅ `"col":"date"::datetime` |
| flat string | `"2026-01-02T…"` | ❌ → use `"col"::datetime` |
| double-nested | `{"date": {"date": …}}` | ❌ → **silent NULL** |
| numberLong | `{"date": {"numberLong": …}}` | ❌ → **silent NULL**, see `sql/measurements.sql:94-113` |

The wrong form yields NULL, not an error. The DAG itself guards all four in
`DATE_FIELD_NORMALIZE_CASE` (`snow_mongo_uc_sync.py:47`) — the view files do not, so you must
check each date column yourself and say so in the PR.

Also settle, from the probe output:

- **Grain.** One row per what? `PROCESSED.{collection}` is already one row per `_id`, so the
  base view needs no dedup and no `WHERE` — only two of 174 files have a top-level `WHERE`.
- **`_id` is always an OBJECT.** The exporter rewrites bare-string ids to `{"oid": …}`
  (`snow_mongo_uc_sync.py:948-960`), so `"_id":"oid"::varchar` always applies. Older files
  like `measurements.sql:35` carry a defensive `LEFT(…,1) = '{'` CASE; you do not need it.
- **Soft deletes.** If there is a `deleted` field, expose it as `is_deleted` — do **not**
  filter on it. No view in the directory filters; the decision belongs downstream.
- **Nested objects and arrays.** Note which ones actually carry data and how stable their
  keys are. §3 explains what to do with them.
- **How many *kinds* of row are in here?** A collection is not always one entity. The
  writer may keep its own bookkeeping in the same place — checkpoints, resume tokens,
  quarantine, barrier markers — on rows whose columns are disjoint from the real records and
  whose identifying field is NULL. `uc_communication_coverage` is 88 rows of which **2** are
  coverage; the other 86 are sync state. Group by the candidate discriminator before
  believing any row count:

  ```sql
  select coalesce("kind", '<null>') as kind, count(*) as n,
         count_if("<theIdentifyingField>" is not null) as n_identified
  from mongo_unifiedcare.processed.<collection> group by 1 order by n desc;
  ```

  Expose the discriminator and a derived `is_<entity>` boolean in the staging model; do not
  filter in the view. And do not put `not_null` on a field the bookkeeping rows leave NULL —
  check which columns are NULL *per kind*, or the test fails for a correct view.

---

## 2.5 · Does `_current` even hold the latest document?

**Everything in §3 assumes `PROCESSED.{collection}` is the collection as of the last sync.
For some collections it is not, and the view cannot tell.** Decide this here, not after the
PR.

`_historical` is loaded by a MERGE keyed on (`"_id":"oid"`, normalized `updatedAt`) with
`WHEN NOT MATCHED INSERT` only, and `_current` is then derived from `_historical`. So a
rewrite that does **not** move `updatedAt` to a new value collides with the row already
there and is **silently discarded** — in FULL mode too, because the FULL CTAS also reads
`_historical`. Two failure shapes:

- **No `updatedAt` at all**, or it never changes → the document is frozen at first-seen.
- **`updatedAt` at DATE granularity** (always `00:00:00`) → frozen *within each UTC day*.
  Every rewrite after the day's first is lost. This one looks healthy: the column is
  populated and does advance day to day, so a shape probe shows nothing wrong.

Measure it, one query:

```bash
bash shared_tmp/dbt_dev_local.sh show --quiet --limit 20 --inline "
select to_varchar(\"updatedAt\":\"date\"::datetime,'YYYY-MM-DD HH24:MI:SS') as upd,
       count(*) as n
from mongo_unifiedcare.processed.<collection>
group by 1 order by 1 desc"
```

**Every row landing on exactly `00:00:00` is the date-granularity trap.** A healthy
per-document stamp shows scattered wall-clock times.

If either shape holds, add the collection to `SNAPSHOT_FROM_FULL_EXPORT`
(`snow_mongo_uc_sync.py:63`) with a comment giving the measured evidence, matching the
entries already there. That makes `replace_current_with_full_export` set `_current` straight
from the export and bypass `_historical` entirely. Membership in the set **also forces
`process_interval = INTERVAL.FULL`** (`:2662`), so the full-export precondition is
self-satisfying — you do not need to change the manifest. The `SNAPSHOT_MIN_ROW_RATIO`
(0.95) guard refuses a short export, so a dropped cursor fails safe instead of deleting
`_current`.

Two consequences for the rest of this procedure:

- **Skip the `_historical` twin** (§3). Its "history" is a lossy first-seen-per-day sample,
  not a change log. Publishing a view over it invites someone to reconstruct a timeline that
  was never recorded. Say so in `sources.yml` rather than leaving the absence unexplained.
- **Weigh it by which column matters.** A stale descriptive field is a nuisance; a stale
  *watermark* or freshness stamp inverts a downstream decision, because consumers treat it
  as proof of recency. For `uc_communication_coverage` the frozen column was `dataAsOf`,
  which decides a silence band versus `engagement_silence_unknown`.

---

## 3 · Write the PRODUCTION view

```sql
CREATE OR REPLACE VIEW mongo_unifiedcare.production.{collection}
COPY GRANTS
COMMENT = ''
AS
SELECT
    "_id":"oid"::varchar                 AS {entity}_id,
    "_recordId"::number                  AS record_id,

    "orgId":"oid"::varchar               AS organization_id,
    "memberId":"oid"::varchar            AS patient_id,

    "status"::varchar                    AS status,
    "sequence"::number                   AS sequence,

    "address":"city"::varchar            AS address_city,
    "address"::variant                   AS address,

    "createdAt":"date"::datetime         AS created_at,
    "updatedAt":"date"::datetime         AS updated_at,
    ingest_date::datetime                AS ingest_date
FROM mongo_unifiedcare.processed.{collection};
```

Rules, each of which something in the directory has gotten wrong:

- **Filename must equal the Mongo collection name exactly, including case.** The probe is
  `os.path.exists`, and production runs on Linux. `sql/chatmessages.sql` is committed for
  collection `chatMessages`; on Linux that view is **never rebuilt by the DAG** and only
  survives because `rerun_mongo_prod_views.py` globs case-blind. macOS hides this.
- **`COPY GRANTS` is mandatory.** Without it, the nightly `CREATE OR REPLACE` drops every
  downstream grant.
- **The file must be complete, self-contained DDL** — fully qualified name, terminating `;`.
  It is `file.read()` + `snow_hook.run()`, not a template. `load_sql_template()` is
  deliberately *not* used here, so **Jinja in this file is sent to Snowflake literally and
  fails.**
- **No header comment.** Zero of 174 files have one. Prose goes in `COMMENT = '…'` and, at
  length, in `sources.yml`.
- **Pass VARIANT through; flatten in dbt, not here.** Only 6 of 174 files use
  `LATERAL FLATTEN`. Flattening an array in the view freezes one document's shape into the
  ODS contract. Project the handful of scalar keys you are sure of *and* keep the whole
  object as `::variant` next to them (`uc_working_locations.sql:16-20`).
- **`ingest_date::datetime AS ingest_date` goes last**, unquoted. It is the dbt freshness
  `loaded_at_field` and the SCD-2 version column.
- Column names: camelCase → snake_case with semantic expansion (`orgId` → `organization_id`).

### The `_historical` twin — optional

`PROCESSED.{collection}_historical` is created automatically (append-only SCD-2, one row per
`_id` × `updatedAt`). The PRODUCTION view over it is not. The two suffixes are independent —
10 collections have a base view only, 7 have `_historical` only.

**Add it when the collection's history is the point** — a status that moves, a value that
gets corrected, anything you would otherwise rebuild with a snapshot. Skip it for reference
data and append-only event logs.

If you add it, the file is **byte-identical to the base except two names**: the view name on
line 1 and the `FROM` target on the last line, both gaining `_historical`. Do not re-derive
the column list — copy it. The staging model gets the matching
`stg_{collection}_historical_v1_0_0.sql`.

---

## 4 · Prove the view body before you create it

There is **no CI for any of this.** `.github/workflows/ci.yml` runs ruff, black, pyright,
and `dbt parse`. No SQL step, no sqlfluff (`.sqlfluff` exists but nothing calls it), no test
anywhere under `tests/` touches this DAG or these views. A broken view body is caught by you
or by production.

So run the `SELECT` on its own — without the `CREATE` — and reconcile against the probe:

```bash
bash shared_tmp/dbt_dev_local.sh show --quiet --limit 5 --inline "
select count(*) as n,
       count(distinct {entity}_id) as n_ids,
       count_if(created_at is not null) as n_created,
       count_if(updated_at is not null) as n_updated
from ( <the SELECT body, verbatim> )"
```

Row count must equal the PROCESSED count, `n_ids` must equal it too (that is the grain
claim), and every date column's non-null count must match what the probe measured on the raw
column. **A date column that profiled as 100% populated and comes back 0% non-null is the
double-nested trap, not an empty field.**

---

## 5 · Register the source

`dags/dbt/dp3/models/sources.yml`, under `- name: MONGO_UNIFIEDCARE_PRODUCTION` (line ~407).
Upper-case table name, roughly alphabetical among its neighbours:

```yaml
      - name: UC_WORKING_LOCATIONS
        description: |
          Daily working-location records for UnifiedCare users. Each row captures
          the user's work mode and optional clinic address for a working date.
```

The description is where measured facts belong — grain, row count as of a date, which
denominator is safe, which column lies. See the `UC_DPP_PARTICIPANT_SESSIONS` entry for the
standard to match.

⚠ Do **not** register it under `MONGO_UNIFIEDCARE_PROCESSED` instead. That source exists but
is used by only a couple of models, and its column names are camelCase **and
case-sensitive** — every reference needs double quotes. 52 of 56 mongodb staging models read
PRODUCTION. Read PROCESSED only if you have a reason you can write down.

---

## 6 · Write the staging model

`dags/dbt/dp3/models/staging/mongodb/stg_{collection}_v1_0_0.sql`:

```sql
{{ config(materialized='view') }}

{# One row per <thing>.

   Grain: one row per {entity}_id.

   <Measured facts, with the date you measured them. What the status column actually
   means. Which counts are safe denominators and which are not. Why anything was left
   as VARIANT.> #}

WITH

source AS (

    SELECT * FROM {{ source('MONGO_UNIFIEDCARE_PRODUCTION', '{COLLECTION}') }}

),

renamed AS (

    SELECT
        ------------------------ ids -----------------------------
        {ENTITY}_ID,
        PATIENT_ID,

        ----------------- attributes & metrics -------------------
        STATUS,

        --------------------- dates & times ----------------------
        CREATED_AT,
        UPDATED_AT,
        INGEST_DATE

    FROM source

)

SELECT * FROM renamed
```

Conventions: `source` → `renamed` → `SELECT * FROM renamed`; upper-case column references
(the view already emitted snake_case); dashed comment dividers between field groups. The
`{# … #}` docstring carries the real knowledge — `stg_uc_dpp_participant_sessions_v1_0_0.sql`
is the benchmark, and it is mostly docstring for good reason.

Add derived boolean flags here if the raw columns disagree with each other, and **wrap them
in `COALESCE(…, FALSE)`** — `NULL <> 'LOCKED'` is NULL, and a NULL flag is silently dropped
by both `WHERE NOT flag` and `COUNT_IF`.

Then the yml entry in `staging_core_schemas.yml` (the default — 45 of 63 models live there;
use a domain file like `schemas_dpp.yml` only for a cluster of related collections):

```yaml
  - name: stg_{collection}_v1_0_0
    description: |
      <what it is, one row per what>
    columns:
      - name: {entity}_id
        description: Unique identifier for the record.
        data_type: string
        data_tests:
          - not_null
      - name: patient_id
        description: UnifiedCare patient the record belongs to.
        data_type: string
        data_tests:
          - not_null
```

`not_null` on the grain column and every FK, at minimum. Add `unique` on the grain only
after §4 proved `n_ids = n_rows` — on a `_historical` model it will not be unique.

---

## 7 · Verify

```bash
bash shared_tmp/dbt_dev_local.sh parse
bash shared_tmp/dbt_dev_local.sh run  --select stg_{collection}_v1_0_0
bash shared_tmp/dbt_dev_local.sh test --select stg_{collection}_v1_0_0
```

That builds into `DBT_STAGING_DEV`. `--vars` is mandatory even to parse (the wrapper supplies
it) because `patient_scd_v1_0_0` calls `var("clinics_to_exclude")` unguarded.

Failing tests are not a finding to report — they are the job. Fix and re-run.

---

## 8 · Apply the view in Snowflake

⚠ **PRODUCTION is not env-split.** `MONGO_UNIFIEDCARE.PROCESSED` / `.PRODUCTION` are
hardcoded in both the DAG and every `.sql` file; there is no dev/prod schema switch. Which
account you hit is decided entirely by the connection. **Creating this view from a
prod-pointed connection creates the real production view.** Say so before you run it.

The nightly DAG will apply the DDL on its own — but only when that collection next has ≥1
changed record. The view block sits *after* the `return` for an empty sync window
(`snow_mongo_uc_sync.py:2440`), so **a new `.sql` file for a quiet collection can sit unapplied
indefinitely.** To apply immediately, either execute the DDL directly or run
`scripts/rerun_mongo_prod_views.py`, which globs the whole directory and re-executes every
file (it needs its own `.env` + key pair, and stops at the first failure).

---

## Pitfalls

- **"It's not in git, so it isn't synced."** Backwards. PROCESSED is generic and automatic;
  git silence says nothing about whether data is landing.
- **`"col":"date"::datetime` on a double-nested or numberLong date.** Silent NULLs, no error,
  found months later. §2.
- **Filename case.** Works on your Mac, dead on Linux. §3.
- **Omitting `COPY GRANTS`.** Every downstream grant disappears on the next nightly rebuild.
- **Flattening an array in the ODS view.** Freezes one document's shape into the contract
  for everyone. Pass VARIANT through.
- **Filtering `deleted` in the view.** No view does. You are removing a decision downstream
  consumers need to make.
- **`unique` on a `_historical` grain column.** It is one row per `_id` × `updatedAt`.
- **Reading PROCESSED from dbt to "skip a layer."** Case-sensitive camelCase columns, no
  freshness contract, and you have moved the flattening into a model where the next person
  will not look for it.
- **Trusting the nightly run to apply a new view.** It only fires on a collection that
  changed that night. §8.
- **Assuming `PROCESSED.{collection}` is the current document.** If `updatedAt` does not
  move on every rewrite, it is not, and the view inherits the staleness silently. §2.5.
- **Reading a populated `updatedAt` as a healthy change clock.** A date-granularity stamp
  (every row `00:00:00`) advances daily and still loses every intra-day rewrite. §2.5.
- **Assuming one collection is one kind of thing.** A collection can carry the writer's own
  bookkeeping rows next to the records you want, with disjoint columns and no `metric` /
  discriminator populated. Project the discriminator, do not filter on it. §2.

---

## Verification before declaring done

- [ ] Collection confirmed present and populated in `MONGO_UNIFIEDCARE.PROCESSED`, with the
      row count written down.
- [ ] **Every** date column's shape profiled, and the PR states which form each one is.
- [ ] ODS filename matches the Mongo collection name character-for-character.
- [ ] View body executed as a bare `SELECT`: row count and distinct-id count match PROCESSED,
      and every date column's non-null count matches the raw profile.
- [ ] `updatedAt` distribution measured and the `SNAPSHOT_FROM_FULL_EXPORT` decision stated —
      added with evidence, or ruled out because the stamp moves per rewrite. §2.5.
- [ ] Row kinds counted. If the collection carries bookkeeping rows, the staging model
      exposes the discriminator and no `not_null` test sits on a column those rows leave NULL.
- [ ] `_historical` decision made deliberately and stated — added, or skipped with a reason.
- [ ] `sources.yml` entry carries measured facts, not a restatement of the table name.
- [ ] `dbt parse` clean; staging model built to `DBT_STAGING_DEV`; all its tests pass, with
      the counts quoted.
- [ ] Stated whether the view was created in Snowflake, against which account, and whether
      the nightly run will pick it up or needs `rerun_mongo_prod_views.py`.

For the commit and PR, hand off to `/commit`. For a transformation model on top of this
staging model, hand off to `/sql-to-dbt`.
