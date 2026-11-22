# Recovery

## Contents

- [Undoing a change](#undoing-a-change)
- [Trying a change in a shared staging database](#trying-a-change-in-a-shared-staging-database)

## Undoing a change

How to undo depends on the PR's state and, on Vitess, the revert window. Every undo command here is one you ask about before posting: see the [command policy](command-policy.md).

- **The apply is still running.** Use a lifecycle command: `stop` (MySQL, resumable) or `cancel` (permanent). See [Lifecycle commands](pr-workflow.md#lifecycle-commands). **Never undo a running change by editing the schema files back.** Pushing a commit that removes the change does not stop the apply: it keeps going to completion, and once it lands the database no longer matches the files. The check then stays blocked until the change is rolled back or the environment is reconciled. Command first; edit files afterwards if still needed.
- **The PR is open and the change has applied:**
  - **Vitess, inside the revert window:** `schemabot revert <apply-id> -e <env>` puts the previous table back. `schemabot skip-revert <apply-id> -e <env>` ends the window early instead.
  - **MySQL, PostgreSQL, or Vitess after the window:** `schemabot rollback <apply-id> -e <env>`, then `schemabot rollback-confirm -e <env>`. A rollback plans and applies the previous schema as a new change. It is refused for a rollout of several targets; see [Sharded databases](sharded-databases.md#commands).
- **The PR was closed without merging.** `rollback` is no longer available. Open a new PR that brings the schema files to the state you want. Closing a PR does not cancel an apply that is still running: it keeps going and holds its lock and check state until it finishes.
- **The PR merged.** Undo it in git: revert the schema edit, or open a new PR with the inverse change, and apply it through the normal workflow.

`rollback` works only on the **most recent** apply, and only while its PR is open. An apply ID from an older apply or a different PR won't roll back.

Undoing an additive change drops what the change added, along with any data written to it since. Say so when you ask.

[Review a rollback before running it](https://github.com/block/schemabot/blob/main/docs/cli.md#review-a-rollback-before-running-it) shows what a rollback plan looks like.

## Trying a change in a shared staging database

A feature that depends on a schema change can be tested end to end before you commit to either: apply the change to staging, run the feature against it, then keep it or back it out. The whole loop runs from one PR:

1. Open a PR with the feature and the schema change.
2. Apply to staging, under the [command policy](command-policy.md).
3. Run the feature's staging tests against it.
4. **It works:** the human applies to the remaining environments and merges.
5. **It doesn't:** roll the change back (`schemabot rollback <apply-id> -e staging`, then `schemabot rollback-confirm -e staging`, after asking), and the PR iterates or closes.

Your branch is yours, but a shared staging database is not: other people's tests read the same tables, and an applied change stays after your PR closes. So:

- **Only additive changes while experimenting.** A new column, table, or index goes unnoticed by code that predates it. A drop breaks anything still reading what it removed and takes the data with it; keep drops out of the experiment and use the two-PR [deploy order](change-behavior.md#deploy-order-schema-first-except-drops). If the experiment is abandoned, roll the change back rather than leaving it in staging.
- **Trial an index removal by hiding it.** Mark the index `INVISIBLE` in its table file (MySQL). The optimizer stops using it while MySQL keeps it up to date, so staging traffic shows what the drop would cost, and removing the keyword brings it back with no rebuild.
- **Prove a new index with `EXPLAIN`.** Once the staging apply completes, run `EXPLAIN` on the queries the index is for and confirm the optimizer picks it. An index the planner never uses is pure write overhead, and staging is the cheapest place to find out.
- **Split the schema out early.** Once the columns are settled, move the schema files, and any generated data-access code, onto a fresh branch from the base branch. Open that as its own PR, apply it, and merge it, then rebase the feature branch onto it. Merging the schema PR also releases the database lock, which the feature PR would otherwise hold until it merges.

[Habits for a shared staging database](https://github.com/block/schemabot/blob/main/docs/pre-merge-workflow.md#habits-for-a-shared-staging-database) covers the same habits for humans.
