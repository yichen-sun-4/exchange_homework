# exchange_homework

A swap shelf for skills and context between devs. Each skill here is a file you can drop into your own
`.claude/skills/` and use, and each draft is one that hasn't become a skill yet and is still being worked on.

**This repo is files, not a tool.** There's nothing to install or run. Copy a skill directory into a
project (or `~/.claude/skills/`) and Claude Code picks it up by name.

```bash
cp -R .claude/skills/staging_bronze ~/.claude/skills/        # one skill, user-wide
cp -R .claude/skills/*              <repo>/.claude/skills/   # all of them, one project
```

## The medallion ladder

The skills follow a dbt model as it moves up through the layers. Each one ends by naming the next.

| layer | skill | turns | into |
|---|---|---|---|
| bronze | [`staging_bronze`](.claude/skills/staging_bronze/SKILL.md) | a raw Mongo collection in `PROCESSED` | an ODS view + `stg_*` staging model |
| silver | [`transforming_silver`](.claude/skills/transforming_silver/SKILL.md) | staging models | transformation models (`models/transformation/`) |
| gold | [`reporting_gold`](.claude/skills/reporting_gold/SKILL.md) | transformation models | reporting models and their consumers (`models/reporting/`) |
| semantic | [`drafts/semantic_view.md`](drafts/semantic_view.md) — **draft** | gold models | Snowflake semantic views |

## Layout

```
.claude/skills/<name>/SKILL.md        a finished skill; the frontmatter `name` matches the directory
.claude/skills/<name>/references/     the SQL / scripts the skill tells you to run
drafts/<topic>.md                     not a skill yet; open questions are left in it on purpose
```

## Provenance

| skill | source | as of |
|---|---|---|
| `staging_bronze` | `data-airflow` `.claude/skills/mongo-collection-ingest/` | `origin/main` @ `1032dfdb`, copied verbatim except for `name:` |
| `transforming_silver` | written from `data-airflow` `origin/main` | 2026-11-20 |
| `reporting_gold` | written from `data-airflow` `origin/main` | 2026-11-20 |

If a skill came from another repo, record the commit you copied it from. A copy with no commit on it
can't be diffed against its source, and you won't be able to tell when it went stale.

## House rules

How to write a skill is in [`AGENTS.md`](AGENTS.md). In short:

1. **A skill carries only what the model cannot know.** That means our paths, our rules, and our
   gotchas. It doesn't need a tutorial.
2. **Anything repeatable becomes a script in `references/`.** The skill should say "run X" instead of
   explaining the steps.
3. **Every rule should trace back to an incident.** If you can't name what went wrong, delete the rule.
4. **Numbers carry a date.** "326 unwired collections" means nothing without "measured 2026-10-06".
5. **Drafts graduate by moving.** When a draft's open questions are closed, `git mv` it into
   `.claude/skills/<name>/SKILL.md` and add a frontmatter block.
