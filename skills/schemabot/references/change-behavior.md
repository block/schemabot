# Change behavior and advanced rollouts

## Contents

- [How a change runs](#how-a-change-runs)
- [Unsafe changes and drops](#unsafe-changes-and-drops)
- [Deploy order](#deploy-order-schema-first-except-drops)
- [Renaming a column or table](#renaming-a-column-or-table)
- [Emptying a table](#emptying-a-table)

## How a change runs

Speak only to the database's engine. The [engine capability matrix](https://github.com/block/schemabot/blob/main/docs/engines.md#capability-matrix) is the source for what each one supports.

**Instant changes.** Many everyday changes, adding a column above all, run as **instant DDL**: the database edits table metadata without touching rows, so the change finishes in milliseconds whatever the table's size. On MySQL this covers adding and dropping columns, changing a column default, and adding a value to the **end** of an `ENUM` or `SET`. Inserting an `ENUM` value mid-list or reordering values is a full table copy, which can surprise on a big table. MySQL and Vitess use instant DDL automatically wherever the change allows it, with two exceptions that copy rows instead: a change applied with `--defer-cutover`, since an instant change leaves no cutover to hold, and on Vitess a destructive change such as dropping a column, so the revert window can undo it. An instant change that seems stuck is not slowly copying; something else is wrong.

**Copy-based changes.** Changes that must rewrite rows, such as adding an index or changing a column's type, run online in the background on MySQL and Vitess: the engine builds a new copy of the table, keeps it in step with writes, and swaps it in at **cutover**. On a big table this can take a long time; the progress comment shows rows copied and an ETA. Let it run. There is no speed setting.

**Throttling.** A `(throttled)` note on the progress bar means the engine is holding the copy back to protect the database, and it resumes on its own. A brief throttle needs no action. For a long one, point the user at the [throttle reference](https://github.com/block/schemabot/blob/main/docs/throttle.md) for that reason, which says what to check. High CPU on the writer during a copy is expected and is not a reason to stop. `stop` (MySQL) is for when the application itself is suffering, such as latency or errors climbing; `start` it again once the application has recovered.

**PostgreSQL** runs each statement natively, under budgets that cancel a statement that would rewrite a large table. It has no online copy yet, so some changes are refused at plan time. The [PostgreSQL guide](https://github.com/block/schemabot/blob/main/docs/postgresql.md#supported-changes) lists what is supported.

When telling a user how their change will run, attach the meaning to the term: "instant (metadata only, milliseconds even on a huge table)" or "copy-based (rows copied online in the background; the PR comment shows progress and an ETA)".

**Several tables in one change.** On MySQL, each table's change runs as its own task, one after another, and each table's cutover runs as soon as its copy is done. Mid-apply, earlier tables have changed and later ones haven't. Applying with `--defer-cutover` holds the swap: the `ALTER`s that copy rows run first, nothing is swapped, and `schemabot cutover <apply-id> -e <env>` then swaps all of those tables together. Only those `ALTER`s swap together, and only on one target: a `CREATE TABLE` in the same plan runs before them, a `DROP TABLE` runs after the cutover, and an `ALTER` that runs as direct DDL lands as soon as it runs. Use it only when altered tables need their cutovers at the same moment, such as matching collations on columns they join on; otherwise let each table's cutover run as soon as it is ready. PostgreSQL has no deferred cutover.

**Busy tables: time the cutover.** A copy-based change ends with a brief lock at cutover. On a busy table, long-running queries holding the table can block it, and they may be killed to let the cutover through, so the application must treat killed queries as retryable. For a copy-based change on a busy table, apply with `--defer-cutover` and run `schemabot cutover <apply-id> -e <env>` at a quiet time. Instant changes need none of this.

**Vitess revert window.** On Vitess, a copy-based change ends in a **revert window**: the previous table is kept, so `schemabot revert <apply-id> -e <env>` can undo the change. `schemabot skip-revert <apply-id> -e <env>` closes the window early. The progress comment links the PlanetScale deploy request, which shows the window's state. Instant Vitess changes have no window. Once the window closes, the change is permanent and is undone with `rollback`. See [Recovery](recovery.md).

## Unsafe changes and drops

Drops are still declarative. Delete a table's `.sql` file to drop the table; remove a column's line from its `CREATE TABLE` to drop the column. SchemaBot plans the `DROP` itself.

A change is **unsafe** when it destroys data or structure: `DROP TABLE`, `DROP COLUMN`, or dropping an index that was not made invisible first. The plan lists it under ⚠️ Issues and the apply is refused until someone acknowledges it with `--allow-unsafe`. That command is the human's, in every environment (see the [command policy](command-policy.md)). Follow the deploy order below first.

The gate does not catch every change that can lose data. Narrowing a column's type, such as `varchar(255)` to `varchar(10)` or `bigint` to `tinyint`, and removing an `ENUM` or `SET` value plan and apply without `--allow-unsafe`, on every engine. Before applying one, check it against the data the column already holds, and tell the user SchemaBot did not flag it.

Some statements the engine cannot run at all are listed under ⛔ Cannot apply. No flag overrides those: rewrite the change. [Lint and safety levels](https://github.com/block/schemabot/blob/main/docs/lint-and-safety-levels.md) explains each level.

**Dropping an index:**

- **Make it invisible first,** in its own PR, then drop it in a later PR once nothing has suffered. The optimizer treats an invisible index as if it were gone, but making it visible again is instant; recovering from a wrong drop means rebuilding the index while the application struggles. On MySQL, mark the index in the table file: `` KEY `idx_status` (`status`) INVISIBLE ``. SchemaBot plans `ALTER TABLE … ALTER INDEX … INVISIBLE`, which is instant.
- **If the index is being replaced, add the new one first,** confirm queries use it, then hide and drop the old one.

### Deploy order: schema first, except drops

- **Additive changes (a column, table, or index): schema first, application second.** Ship the schema edit and any regenerated data-access code in the same PR. SchemaBot applies the change before merge and the application deploys after, so the database has the new column before any code expects it.
- **Drops: application first, schema second.** The old application version is still running when the drop applies, so it must already have stopped using the column or table:
  1. **PR 1, make the application ready.** Remove every read and write of the column or table, including from generated data-access code and ORM mappings. Confirm, from query metrics or logs, that nothing still uses it.
  2. **Deploy PR 1.**
  3. **PR 2, drop it** declaratively from the schema files.
  4. Apply PR 2 (with `--allow-unsafe`, posted by the human), then merge once the check passes.

Dropping something still in use causes errors or an outage. The two PRs are what prevent it.

**Generated data-access code.** If the repo generates models from the schema files, the code built from a commit must match that commit's schema files. When generated code is committed, regenerate it with the schema change and review its diff: exactly the expected columns and tables should appear or disappear. When it is generated at build time, the schema edit alone is enough. A model that is deliberately narrower than the schema is fine, such as one that excludes a column ahead of a drop. A stale model that was never regenerated breaks the deploy order silently.

## Renaming a column or table

There is no in-place rename. SchemaBot diffs the desired state against the live one and cannot know that a new name *means* a rename, so a renamed column plans as `DROP COLUMN` of the old name plus `ADD COLUMN` of the new one. That destroys the old column's data, and the `DROP` makes the plan unsafe.

- **If the data must survive,** do it as an add and a drop: PR 1 adds the new column; copy the data across with the application or a backfill tool; move the code to the new column and deploy; then PR 2 drops the old column in the drop order above.
- **If the data can be lost** (unused, empty, or disposable), the rename in one PR is fine. Confirm in the plan that the drop and add are intended; the human applies it with `--allow-unsafe`.

Renaming a table works the same way: the plan drops the old table and creates the new one, empty.

## Emptying a table

SchemaBot does not run `TRUNCATE TABLE`. To empty a table declaratively:

- **Drop and recreate, in two PRs.** PR 1 deletes the table's file; PR 2 adds it back. One PR that deletes and re-adds the same file describes the live schema exactly, so there is nothing to plan.
- **Or rename it, in one PR,** which plans a `DROP` of the old name and a `CREATE` of the new one.

Either way the rows are gone and anything still using the old table breaks the moment it applies. The unsafe gate, `--allow-unsafe`, and the application-first deploy order all apply.
