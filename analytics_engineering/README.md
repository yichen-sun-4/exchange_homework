# analytics_engineering

Skills for taking a data-airflow Mongo collection up through the dbt layers, plus a visualizer of
the layers and a draft for the step after gold.

**See the difference first:** open [`visualizer/layers.html`](visualizer/layers.html). It shows
PROCESSED → PRODUCTION → bronze → silver → gold as one diagram plus a side-by-side table.

## The ladder

The skills follow a dbt model as it moves up through the layers. Each one ends by naming the next.

| layer | skill | turns | into |
|---|---|---|---|
| bronze | [`staging_bronze`](skills/staging_bronze/SKILL.md) | a raw Mongo collection in `PROCESSED` | an ODS view + `stg_*` staging model |
| silver | [`transforming_silver`](skills/transforming_silver/SKILL.md) | staging models | transformation models (`models/transformation/`) |
| gold | [`reporting_gold`](skills/reporting_gold/SKILL.md) | transformation models | reporting models and their consumers (`models/reporting/`) |
| semantic | [`drafts/semantic_view.md`](drafts/semantic_view.md) — **draft** | gold models | Snowflake semantic views |

## Layout

```
.claude/skills/<name>/SKILL.md        a finished skill; the frontmatter `name` matches the directory
.claude/skills/<name>/references/     the SQL / scripts the skill tells you to run
drafts/<topic>.md                     not a skill yet; open questions are left in it on purpose
```

## Layout

```
skills/<name>/SKILL.md        a finished skill; the frontmatter `name` matches the directory
skills/<name>/references/     the SQL the skill tells you to run
drafts/<topic>.md             not a skill yet; open questions are left in it on purpose
visualizer/layers.html        self-contained page; open it in a browser
```

## Install

```bash
cp -R analytics_engineering/skills/staging_bronze ~/.claude/skills/      # one skill, user-wide
cp -R analytics_engineering/skills/*              <repo>/.claude/skills/  # all three, one project
```

## Provenance

| skill | source | as of |
|---|---|---|
| `staging_bronze` | `data-airflow` `.claude/skills/mongo-collection-ingest/` | `origin/main` @ `1032dfdb`, copied verbatim except for `name:` |
| `transforming_silver` | written from `data-airflow` `origin/main` | 2026-11-20 |
| `reporting_gold` | written from `data-airflow` `origin/main` | 2026-11-20 |

If a skill came from another repo, record the commit you copied it from. A copy with no commit on it
can't be diffed against its source, and you won't be able to tell when it went stale.
