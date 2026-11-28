# PR locks and blockers

## Contents

- [One change per database at a time](#one-change-per-database-at-a-time)
- [Scoping flags](#scoping-flags)
- [Common blockers](#common-blockers)
- [Reconciling a database that is behind its files](#reconciling-a-database-that-is-behind-its-files)

## One change per database at a time

SchemaBot runs one schema change per database at a time. An apply takes the database's **lock**, and the lock stays with that PR until it merges or closes, or until `schemabot unlock` is posted on it. An apply from another PR is refused with a comment saying another PR holds the lock and naming it.

A plan is a diff against the live schema, so a second change running at the same time would make it wrong. Concurrent schema changes to one database are not supported, and breaking the lock doesn't buy concurrency. When you hit the lock, wait or coordinate with the PR that holds it.

When two PRs change the same database around the same time:

1. **Wait for the other PR to merge or close.** The lock is released then, not when its apply finishes, so a change that has applied but not merged still holds it.
2. **Then look at your plan for changes you didn't make.** Your branch's files predate the other change, so your plan now includes DDL that undoes it, such as dropping the column they just added. That is the sign your branch is stale.
3. **Rebase onto the base branch** or merge it in, and let SchemaBot plan again. The plan shrinks back to your change. Never apply a plan that undoes someone else's work. SchemaBot also refuses the apply while the base branch has newer schema changes than your branch.

**Stacked PRs plan the whole stack.** A PR stacked on an unmerged PR plans both PRs' changes, including warnings and unsafe flags that belong to the base PR. Don't read those as a problem with your change, and never reach for `--allow-unsafe` to get past them. Merge the base PR first, rebase, and plan again.

## Scoping flags

- **Several databases in one PR.** When a PR touches more than one database's schema directory, SchemaBot posts "Multiple Databases Detected" and each command needs `-d <database>`.
- **A rollout of several targets.** `--target <target>` narrows `plan` and `apply` to one target. See [Sharded databases](sharded-databases.md).

## Common blockers

- **No plan comment.** The PR may not touch any file under a schema directory (the directory holding `schemabot.yaml`), or the repo isn't set up for SchemaBot. Or the plan found nothing to change, in which case the passing check is the only signal. Check the file paths and the check before suspecting a failure.
- **Check missing, stuck as "Expected", or out of date.** Fix it in this order. First, push a new commit, ideally by rebasing onto the base branch: SchemaBot plans the new head, and the rebase picks up schema changes that merged meanwhile. Second, comment `schemabot plan`, which recomputes the plan and the checks without touching the branch. Third, close and reopen the PR, but ask the user first: closing releases the PR's locks and clears its stored check state, as `unlock` does (see the [command policy](command-policy.md#ask-before-posting-these-in-any-environment)).
- **Check exists but didn't update.** The **Re-run** button on the check's detail page reconciles it again. It works only on a check for the PR's current head commit; otherwise use `schemabot plan`.
- **Blocked by deployment drift.** The live schema doesn't match the files for reasons outside this PR. The block holds until the environment is reconciled. That takes an operator: say so, and don't try to force an apply through it.
- **Command refused, not authorized.** Applies, lifecycle commands, and plans that read a database's live schema are permission-gated. The refusal names who is authorized; loop one of them in. SchemaBot may also require an approved PR and passing required checks; the refusal says which is missing.
- **"Apply rejected" because the base schema is newer.** The base branch has schema changes this branch doesn't include, so applying could revert schema that already landed. Never look for a way around it. Merge or rebase the base branch into the PR, wait for the new plan, and check that it contains only this PR's change, with no DDL on tables the PR didn't edit. Then post or hand over the apply again under the [command policy](command-policy.md).
- **Refused because of new commits since discovery.** Commits landed on the branch between the command and SchemaBot reading the files. Nothing to fix: review the newest plan, then post the command again.
- **"Repository Too Large to Search".** The repo's file tree is too large for GitHub to list in full, so SchemaBot searches only the schema directories its server configuration names for the database, and here it had none it could trust. The fix the comment names is an operator one: an `allowed_dirs` entry for the database's schema directory. Meanwhile, a PR that changes a file under the right schema directory (the nonce edit below) avoids the search, and so does checking that `-d` names the database exactly.
- **Refused by a freeze.** The environment is frozen. The refusal names the freeze and says how to override it: run the command again with `--override` and a reason. Never post that yourself: the reason is the user's to give, and by default it lets the change through at once. Tell the user what is frozen and hand over the command if they want to override it. See [Freezes](refusals-and-failures.md#freezes).
- **Database locked.** See [One change per database at a time](#one-change-per-database-at-a-time). `unlock` on the PR holding the lock discards its locked plans; ask the user first (see the [command policy](command-policy.md)). No comment can release another PR's lock, and no lock is released while an apply for that database is still running.
- **Lint warnings on the plan.** Warnings don't block. Error-severity findings are the unsafe gate: see [Unsafe changes and drops](change-behavior.md#unsafe-changes-and-drops). The linter checks the whole table, so it can flag columns your change didn't touch. Say that these are non-blocking warnings on existing schema, outside this PR, worth a later change.
- **Failed apply.** Retryable errors are retried on their own first; the progress comment says so. Otherwise, fix the cause and apply again.

## Reconciling a database that is behind its files

A database can fall behind files that are already correct: a missed apply, or a freshly created database. There is nothing to edit, but SchemaBot plans only a PR that changes something under the schema directory.

Use a **nonce edit**: add or remove a comment line, such as `# nonce`, in the database's `schemabot.yaml`. The PR now has a change under the schema directory, SchemaBot plans it, and the plan shows everything the database is missing, including tables whose files the PR didn't touch. Apply as usual. Leave the comment in place and toggle it the other way next time.

The nonce plan is also a check before an apply: it surfaces drift you would otherwise apply blind. Read it closely, especially for `DROP` statements nobody expected. See [Reconciling drift with a nonce edit](https://github.com/block/schemabot/blob/main/docs/pre-merge-workflow.md#reconciling-drift-with-a-nonce-edit).
