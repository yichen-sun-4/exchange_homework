# exchange_homework

A swap shelf for skills and context between devs. Each topic gets its own folder of skills, drafts,
and visuals. Copy a skill into your own `.claude/skills/` and Claude Code picks it up by name.

**This repo is files, not a tool.** There's nothing to install or run.

## Folders

| folder | what's in it |
|---|---|
| [`analytics_engineering/`](analytics_engineering/) | bronze → silver → gold dbt skills for data-airflow, a semantic-view draft, and a [layers visualizer](analytics_engineering/visualizer/layers.html) |

## Layout of a topic folder

```
<topic>/README.md                    what the folder covers, where each skill came from, how to install
<topic>/skills/<name>/SKILL.md       a finished skill; the frontmatter `name` matches the directory
<topic>/skills/<name>/references/    the SQL / scripts the skill tells you to run
<topic>/drafts/<topic>.md            not a skill yet; open questions are left in it on purpose
<topic>/visualizer/*.html            self-contained pages; open them in a browser
```

Skills aren't kept under `.claude/skills/` here, so this repo doesn't load them into its own
sessions. Copy them to wherever you want to use them.

## House rules

How to write a skill is in [`AGENTS.md`](AGENTS.md). In short:

1. **A skill carries only what the model cannot know.** That means our paths, our rules, and our
   gotchas. It doesn't need a tutorial.
2. **Anything repeatable becomes a script in `references/`.** The skill should say "run X" instead of
   explaining the steps.
3. **Every rule should trace back to an incident.** If you can't name what went wrong, delete the rule.
4. **Numbers carry a date.** "326 unwired collections" means nothing without "measured 2026-10-06".
5. **Drafts graduate by moving.** When a draft's open questions are closed, `git mv` it into
   `<topic>/skills/<name>/SKILL.md` and add a frontmatter block.

If a skill came from another repo, record the commit you copied it from in the topic README. A copy
with no commit on it can't be diffed against its source, and you won't be able to tell when it went stale.
