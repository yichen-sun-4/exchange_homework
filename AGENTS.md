# How we write skills here

Four rules. They are about *durability*: what still works after the model changes underneath us.

## 1. A skill carries only what the model cannot know

Our paths. Our rules. Our gotchas. That is the whole brief.

No step-by-step "how to code", no explanations of git, SQL, regex or Java. **Newer models do that
better than our old instructions do**, and every paragraph of it is a paragraph that will age into
being wrong while sounding authoritative.

Keep: `~/.local/bin/sfq` is how you reach Snowflake, not the MCP. `claude-phi` is a zsh function so
`herdr agent start` cannot launch it. `LabMappingMatcher` resolves a row *before* any metric
definition, so a metric with no rows is unreachable. A single leading slash in `Edit(/Users/...)`
resolves relative to the project root and binds to nothing.

Cut: anything a competent engineer would work out from the code in five minutes.

## 2. Anything repeatable becomes a script. The skill says "run X"

**Scripts do not get worse when the model changes.** Prose does.

If you find yourself writing the same four commands into a skill, that is a script. Put it in the
skill's `references/` and have the skill say "run block 3 of `references/probe.sql`" instead of
copying the query into the prose. The query will change, and the prose won't get updated with it.

| instead of explaining | the skill says |
|---|---|
| how to find unwired collections, profile date shapes, check grain | run `references/probe.sql`, one block at a time |
| how to reconcile a view body against its source | run probe block 8 with the body pasted in |

A script also **fails loudly and identically every time**, which prose cannot. A skill that says
"check the date shapes" gets skipped under pressure. A probe that returns `shape_double = 4,812`
doesn't.

## 3. Fixed from real mistakes, immediately

When an agent does something wrong, open a PR here and fix the skill or script **then**, not on a
review schedule. Every rule in these briefs should be traceable to something that actually went
wrong once. If you cannot name the incident, the rule is speculation and should go.

Write the incident into the fix, briefly, so nobody later "cleans up" a rule whose cost they cannot
see. That is why `staging_bronze` says *`chatmessages.sql` is never rebuilt on Linux* instead of just
"match the case".

## 4. A/B the skill. If no-skill wins, cut it

Now and then, run the same task twice — once with the skill, once without. If the no-skill run is as
good or better, the skill is **cargo** and should be cut down or deleted.

This is the only rule that reliably shrinks a brief. Without it they only grow, because every
incident adds a paragraph and nothing ever removes one.

