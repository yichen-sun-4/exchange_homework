# semantic_view — drafting doc

> **Status: DRAFT.** As of 2026-11-20. Not a skill yet — the open questions at the bottom
> must be settled first. Step 4 of 4: [staging_bronze](../.claude/skills/staging_bronze/) →
> transforming_silver → [reporting_gold](../.claude/skills/reporting_gold/) → **semantic view**.
> Facts below were read from `data-airflow` **origin/main** on 2026-11-20; counts drift.

## Thesis

**A semantic view is a contract, not documentation.** In this warehouse, the YAML is read
by machines that act on it: the Snowflake→Mongo sink takes its `primary_key` as the Mongo
`_id`, and Cortex Analyst turns its measures into SQL nobody reviews. A wrong description
is a nuisance. A wrong key stops a sync; a wrong `default_aggregation` or a missing
denominator produces a confident, wrong number in front of a non-analyst.

So the gold model's measured facts — grain, denominators, what zero means — must be
**written into** the view. Cortex cannot read a `{# … #}` docstring.

## What exists today (and what it does not)

| place | files | what it is | `base_table` schema |
|---|---|---|---|
| `dags/snowflake/ai_team/snowflake_to_mongo_sync/semantic_views/` | 86 | sink key contracts; **69 are key-only** ("Sync key contract for …") | `DBT_TRANSFORMATION` 79, `UNIKA_INSIGHTS` 6 |
| `dags/dbt/dp3/semantic_views/db_*.yml` | 144 | DataBot / Cortex Analyst views, `verified_by: databot` | `DATABOT` 144 |

- **Format is Cortex Analyst YAML** (`tables / time_dimensions / dimensions / measures /
  relationships / verified_queries`), not native `CREATE SEMANTIC VIEW` DDL. Native views:
  `SHOW SEMANTIC VIEWS IN ACCOUNT` returned nothing to the MCP role on 2026-11-20; the only
  one known is `ANALYTICS.DATABOT.SV_PATIENT_MEASUREMENTS`, and it is dead.
- **Zero semantic views point at a gold (`reporting/`) model.** Not one `base_table`
  resolves to `DASHBOARDS`. Gold is greenfield; this doc is for that.
- **The DataBot tier is being retired.** `ANALYTICS.DATABOT` is a dbt alias layer whose real
  job is stripping version suffixes (115 of ~145 views). Do not add new `db_*.yml`. Its
  generator DAG (`snow_analytics_databot_semantic_sync.py`) merges YAML and `PUT`s it to
  `@ANALYTICS.DATABOT.STAGE` — that is the only Cortex upload path in the repo.
- **Existing tooling:** `/semantic-view` command (`.claude/commands/semantic-view.md`,
  DataBot-shaped — it hardcodes `DBT_DATABOT_SCHEMA` and `tier: 3`; do not follow those two
  parts for gold), the sink's `semantic_views/CLAUDE.md` (hard rules, below), and
  `~/semantic_view_gen` (`semantic-gen`: deterministic descriptions + live-validated keys,
  every value tagged with its source).

## What a semantic view is for here — and what it is not

**For:** (1) declaring the true grain so the sink can key it; (2) letting Cortex Analyst /
an LLM answer questions over a gold model *without* rediscovering its traps; (3) naming
the denominator next to every rate.

**Not:** a place to compute anything. No joins the gold model should have done, no
filters, no business logic in `expr` beyond a cast or a `NULLIF` ratio. If a metric needs
logic, it belongs in the gold model and the view projects the column.

**Not** a replacement for the alias layer. If the reason you want a view is "consumers
shouldn't have to know `_V1_0_0`", that is the DATABOT problem, and it is an open question
(below), not a semantic view.

## Inputs required from the gold model before drafting

Do not start until the gold model (see [reporting_gold](../.claude/skills/reporting_gold/))
can hand you all of these, measured, with a date:

| input | why the view needs it |
|---|---|
| **Grain**, as a column list, with `count(*) = count(distinct grain)` proven | becomes `primary_key`; the sink GROUP-BY-verifies it and skips the object forever if wrong |
| **Every rate's denominator column** | a rate with no denominator in the view gets averaged and ranked |
| **What 0 vs NULL means** per metric | `CLINICAL_OUTCOME_*` rates are 0.0% when *unmeasured*; Cortex will sort on it |
| **Cohort each count is over** | enrolled vs all-time roster differ ~2x (23,451 vs 44,572 in Aug 2026); signal eligible cohorts differ up to 60x |
| **Non-additive columns** | rates, distinct counts, panel sizes across overlapping grains must not be `sum`med |
| **Compound-identity grains** | e.g. assignee grain is `(ASSIGNEE_ID, ASSIGNEE_ROLE)`; five people hold two roles |
| **Schema env var** | gold lives under a different `DBT_*_SCHEMA` than transformation — check `dbt_env.prod.json` |
| **Refresh time** | `CLINICAL_OUTCOME_*` rebuild ~03:25 nightly; a `verified_query` run earlier sees yesterday |

## Drafting procedure

**§1 · Decide the consumer.** Sink key only → key-only file in the sink dir, stop. Cortex /
LLM consumer → full view. Both → full view in the sink dir (one file per `base_table`; the
sink dir wins over `dp3/semantic_views` per object).

**§2 · Generate the skeleton, don't hand-type it.** Run `semantic-gen` against the gold
object (`--validate-keys`). Keep its `confidence` / `needs_review` tags until §5. Hand-typing
column lists is how a stale `expr` gets in — and a nonexistent `expr` column makes the
sink skip the object.

**§3 · Settle the key.** Use the gold model's proven grain, not the generator's guess.
Scalar columns only. If the gold object is already synced, the key set is frozen — changing
it yields `skipped_id_changed`, and recovery means dropping the Mongo collection and its
`sf_sync_state` doc.

**§4 · Classify columns, then fix the aggregations by hand.** The `/semantic-view` suffix
rules (`_cnt`→sum, `_rate`→avg) are a start and are wrong for gold:
- `_rate` / `_pct` → **never `avg`** across grains. Prefer exposing numerator + denominator
  as `sum` measures and the rate as a measure `expr: SUM(num) / NULLIF(SUM(den), 0)`.
- Panel sizes, distinct-patient counts at a rolled-up grain → no `default_aggregation`, or
  `max` with a description saying why.
- `is_*` / `has_*` → dimension **and** `::INT` `sum` measure (house convention).

**§5 · Write the traps into descriptions.** Each rate's description names its denominator
column and what 0.0 means. Each count names its cohort. Table description states grain,
row count as of a date, and refresh time. Clear every `needs_review` tag; one left in means
a generated guess ships as fact.

**§6 · Verified queries that encode the traps.** 3–5. At least one must exercise the
denominator (rate recomputed from num/den, filtered on den ≥ a floor), and one must be the
grain proof. Bare table names, `verified_by: claude`, `verified_at` = today. Run each one
for real and paste the result into the PR.

**§7 · Verify.** YAML parses; every `expr` column exists on the live object; key unique on
live data; then follow the sink's checklist. If it targets Cortex, ask three questions in
plain English and check the generated SQL picks the right denominator.

## Template skeleton (full view, gold object)

```yaml
name: <gold_object_lower>                       # == filename without .yml
description: >
  <Grain in one line.> <The 2-3 questions this answers.> Rates are over measured
  patients only; see each rate's denominator.

tables:
  - name: <GOLD_OBJECT_UPPER>
    description: >
      One row per <grain cols>. <N> rows as of <YYYY-MM-DD>. Rebuilt nightly ~<HH:MM> UTC.
      <What 0 vs NULL means.>
    base_table:
      database: "{{ env_var('DBT_DATABASE') }}"
      schema: "{{ env_var('<GOLD_SCHEMA_ENV_VAR>') }}"   # OPEN: confirm for reporting/dashboards
      table: <GOLD_OBJECT_UPPER>
    primary_key:
      columns: [<grain_col_1>, <grain_col_2>]

    time_dimensions:
      - name: period_start
        expr: period_start
        description: <period grain and timezone>
        data_type: date

    dimensions:
      - name: <segment>
        expr: <segment>
        description: <meaning; which values are bookkeeping, not real segments>
        data_type: varchar
        sample_values: ["<non-PHI value>"]

    measures:
      - name: <metric>_numerator_cnt
        expr: <metric>_numerator_cnt
        description: Patients meeting <condition>, among <denominator cohort>.
        data_type: number
        default_aggregation: sum
      - name: <metric>_denominator_cnt
        expr: <metric>_denominator_cnt
        description: Measured patients (<cohort>). 0 means unmeasured, not 0%.
        data_type: number
        default_aggregation: sum
      - name: <metric>_rate
        expr: SUM(<metric>_numerator_cnt) / NULLIF(SUM(<metric>_denominator_cnt), 0)
        description: >
          <Metric> rate. NULL when unmeasured. Do not rank rows with
          <metric>_denominator_cnt < <floor>.
        data_type: number

relationships: []

lineage:
  dbt_models: [<silver_model>, <gold_model>]
  tier: <OPEN>
  refresh_cadence: daily

verified_queries:
  - name: grain_is_unique
    question: "Is there exactly one row per <grain>?"
    verified_at: "2026-11-20T00:00:00"
    verified_by: claude
    sql: >
      SELECT COUNT(*) AS n, COUNT(DISTINCT <grain_col_1>, <grain_col_2>) AS n_grain
      FROM <GOLD_OBJECT_UPPER>
  - name: rate_with_denominator_floor
    question: "Which <segments> have the lowest <metric> rate, among those with enough measured patients?"
    verified_at: "2026-11-20T00:00:00"
    verified_by: claude
    sql: >
      SELECT <segment>, SUM(<metric>_numerator_cnt) / NULLIF(SUM(<metric>_denominator_cnt), 0) AS rate,
             SUM(<metric>_denominator_cnt) AS measured
      FROM <GOLD_OBJECT_UPPER>
      GROUP BY 1 HAVING SUM(<metric>_denominator_cnt) >= <floor>
      ORDER BY rate
```

## Review checklist

- [ ] Consumer decided (§1); file in the right dir; filename == lowercased `base_table.table`.
- [ ] `primary_key` == gold model's proven grain, scalar only, uniqueness re-checked live.
- [ ] `base_table` uses quoted env-var templates; no literal schema, no `_DEV`.
- [ ] Every `expr` names a column on the live object.
- [ ] No `avg` on a rate; every rate has its numerator and denominator exposed.
- [ ] Every rate/count description names its cohort and what 0 / NULL means.
- [ ] No `needs_review` tag left from `semantic-gen`.
- [ ] No PHI in `sample_values` (names, DOB, addresses, free text, MRNs).
- [ ] Verified queries run today; one proves grain, one exercises a denominator floor.
- [ ] If already synced: key set byte-for-byte unchanged.

## Open questions for other devs

1. **Native `CREATE SEMANTIC VIEW` or Cortex YAML on a stage?** Everything live is YAML;
   the sink parses YAML only. Native DDL would need a converter or
   `SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML` and a deploy step that does not exist yet.
2. **Where do gold semantic views live?** The sink dir is sync-scoped; `dp3/semantic_views`
   is the retiring DataBot tier. A third dir, or does the sink dir become the home?
3. **Who consumes Cortex over gold once DataBot is retired?** If nobody, the full-view half
   of this doc is speculative and gold should get key-only files until a consumer exists.
4. **What replaces version-suffix absorption** when `ANALYTICS.DATABOT` goes? Not a semantic
   view's job, but it is the reason most people reached for one.
5. **`lineage.tier` for gold** — DataBot uses `3` for everything. Is tier meaningful?
6. **Denominator floor** (≈30 was used on clinic A1C) — one house number, or per metric?
7. **Native semantic views visibility** — `SHOW SEMANTIC VIEWS IN ACCOUNT` returned empty to
   the MCP role; confirm with a role that can see `ANALYTICS.DASHBOARDS`.
