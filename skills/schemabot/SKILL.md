---
name: schemabot
description: Make, apply, and troubleshoot SchemaBot schema changes. Edit declarative schema files, read plans, run PR comment commands safely, and unblock checks on MySQL, Vitess, and PostgreSQL.
---

# SchemaBot

SchemaBot runs schema changes through reviewed GitHub pull requests. The repo holds the desired schema as declarative files. SchemaBot diffs those files against the live database, posts the DDL it would run, gates the PR with a required check, and applies the change before the PR merges.

Two things work the opposite way from most schema tooling:

- You edit the canonical `CREATE TABLE` files to the end state you want. You never write an `ALTER` statement or a numbered change script.
- You apply from the PR, then merge. Merging does not run the change; it records a change that has already been applied.

This skill is for people making schema changes with SchemaBot. Running and configuring a SchemaBot server is covered in the [project docs](https://github.com/block/schemabot/tree/main/docs).

Your team may load its own skill alongside this one, for the facts that differ per team: the name of its CLI wrapper, its servers and environments, and where to ask for help. Where the two disagree on those facts, the team's skill wins. It can add rules to the command policy below but never remove one.

## Command policy

Read [Agent command policy](references/command-policy.md) before you post or hand over any `schemabot` command. In short:

1. **Production: never post.** Hand the exact command to the human. The comment is the authorization, and SchemaBot records its author as the operator.
2. **Never post `--allow-unsafe`, `--skip-revert`, `--override`, or `unlock --force`**, in any environment.
3. **Ask first** before `apply-confirm`, `release`, `unlock`, `rollback`, `rollback-confirm`, `revert`, or `skip-revert`, and before closing a PR.
4. **Other environments: find out who else uses the database first.** Hand the command over if the database is shared, critical, or you can't tell. Otherwise say what you are about to do, then post it.
5. **The user can opt out.** If they say they post every command themselves, hand every command over.
6. **Stop when SchemaBot refuses**, and tell the user its reason. Rewrite the schema files only into a safe form it names, then plan again.
7. **Never act outside SchemaBot.** No DDL of your own against the database, and on Vitess nothing in the PlanetScale console.

## Start here

- [ ] Search the current worktree for `schemabot.yaml` and the schema files next to it. Don't rely on a path you remember.
- [ ] Read `database` and `type` from `schemabot.yaml`. Speak only to that engine: `mysql`, `vitess`, or `postgres`. PostgreSQL support is beta, so check [what is not GA yet](https://github.com/block/schemabot/blob/main/docs/postgresql.md#beta-status) before promising it can do something.
- [ ] If the plan rolls out to several targets, load [Sharded databases](references/sharded-databases.md) before advising.
- [ ] Edit the declarative files to the desired end state. See [Authoring](references/authoring.md).
- [ ] Run the repo's own local convergence, code generation, and tests, if it has them.
- [ ] Open the PR. SchemaBot plans on its own. Read the DDL and the environments the plan lists.
- [ ] Drive the PR through the environments under the command policy, then merge once the required checks pass.

No plan comment can mean the files already match the live database; read the SchemaBot check before treating silence as a failure. A check in `action_required` shows as a red mark on GitHub but is not reporting a failure: it is waiting for something, such as an apply, a confirmation, or a fresh plan after a rollback. Read what the check says it is waiting for before choosing a command; see [Reading the check](references/pr-workflow.md#reading-the-check).

## Load only what the task needs

| Load | When |
|---|---|
| [Agent command policy](references/command-policy.md) | Before posting or handing over any command |
| [Authoring schema changes](references/authoring.md) | Finding or editing schema files, adding tables and columns, choosing the layout for an engine |
| [PR workflow and commands](references/pr-workflow.md) | Opening or reading a SchemaBot PR, reading check states, controlling an apply in flight, review requirements |
| [Refusals, blocks, and failures](references/refusals-and-failures.md) | A plan is blocked, an apply is refused or failed, a freeze holds a change, or an apply reached a final state |
| [PR locks and blockers](references/pr-blockers.md) | A plan or check is missing or stale, a command is refused, the database is locked, the branch is behind, or a database has drifted from its files |
| [Change behavior](references/change-behavior.md) | Instant versus copy-based execution, busy tables, deferred cutover, drops, renames, deploy order |
| [Recovery](references/recovery.md) | Stopping or undoing a change, the Vitess revert window, trying a change in a shared staging database |
| [Sharded databases](references/sharded-databases.md) | A plan that rolls out to several targets, `--target`, a rollout where one target failed |
| [Sharded Vitess tables](references/vitess-sharded-tables.md) | Adding or dropping a sharded Vitess table with its VSchema and sequence |
| [The CLI](references/cli.md) | Where a change stands, whether it finished, how long it has left, its logs |
| [GitHub outages](references/github-outage.md) | Checks, comment replies, or merge queues stall across many PRs at once, whether GitHub or SchemaBot is the cause, or someone asks to turn off the required check |

## Vocabulary

Say **schema change**. Users and other tools have other names for it; treat any request to change table structure as a schema change and answer in SchemaBot's terms. Never suggest a hand-written `ALTER` or a numbered change script: the declarative files replace both.

SchemaBot changes table structure, not row contents. Moving or backfilling data is a job for the application or a backfill tool. Handle the schema part here and say which part is outside SchemaBot.

If the repo has no `schemabot.yaml`, keep to its existing workflow unless the user asks to set SchemaBot up. The [quick start](https://github.com/block/schemabot#quick-start) and `schemabot onboard` cover that.

## Reference

- [Pre-merge workflow](https://github.com/block/schemabot/blob/main/docs/pre-merge-workflow.md): the PR loop for humans
- [Working with AI agents](https://github.com/block/schemabot/blob/main/docs/ai-agents.md): what this skill's policy is based on
- [Engines](https://github.com/block/schemabot/blob/main/docs/engines.md): what each engine supports
- [Lint and safety levels](https://github.com/block/schemabot/blob/main/docs/lint-and-safety-levels.md): what blocks an apply
- [CLI guide](https://github.com/block/schemabot/blob/main/docs/cli.md)
