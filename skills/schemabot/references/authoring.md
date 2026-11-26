# Authoring schema changes

## Contents

- [Finding the schema](#finding-the-schema)
- [Tailor advice to the engine](#tailor-advice-to-the-engine)
- [Repo layout](#repo-layout)
- [Making the change](#making-the-change)
- [Local development and PR scope](#local-development-and-pr-scope)

## Finding the schema

A database managed by SchemaBot has a schema directory holding `schemabot.yaml` (which names the `database` and its `type`) and one `.sql` file per table. Vitess keyspaces also hold a `vschema.json`.

**Find paths fresh, every session.** Search the current worktree for `schemabot.yaml`. Don't use a path you remember from an earlier conversation or from docs: layouts move. Before you make a claim about a path you didn't just find, check it against the base branch (`git ls-tree origin/HEAD -- <path>`). Local files the base branch doesn't know aren't part of the schema.

## Tailor advice to the engine

Read `type` from `schemabot.yaml` and address only that engine. Don't mention another engine, its lifecycle, or its commands to a user on a different one.

When you state the setup back, use the names exactly as they appear in the repo: "SchemaBot manages the `orders` database (MySQL)". There is one SchemaBot; the engine is a property of the database. Avoid phrasing like "the MySQL SchemaBot", and don't turn identifiers into prose names: the exact names are what commands and log searches take.

- **`mysql`** runs through [Spirit](https://github.com/block/spirit): instant DDL where possible, otherwise an online copy and cutover. There is no branch, deploy request, or revert window. `stop` pauses a running change and `start` resumes it; `cancel` ends a running or stopped change for good. An applied change is undone with `rollback`. See the [MySQL guide](https://github.com/block/schemabot/blob/main/docs/mysql.md).
- **`vitess`** runs through PlanetScale: a branch, a deploy request, then cutover. Copy-based changes then get a **revert window**; instant ones finish immediately. Sharding lives in each keyspace's `vschema.json`. `cancel` ends a running change for good. There is no pause, so `stop` is refused with a pointer to `cancel`. Undo with `revert` inside the window, or `rollback` after it. `skip-revert` does not undo anything: it closes the window early and keeps the change. See the [Vitess guide](https://github.com/block/schemabot/blob/main/docs/vitess.md).
- **`postgres`** runs through [pg-sprite](https://github.com/block/pg-sprite) and is **beta**. Each statement runs natively under time and size budgets; there is no online copy yet, so a change that needs one, such as a general column type change, is blocked in the plan at any table size, and other `ALTER TABLE` changes run only on tables up to the size limit, unless they change only the catalog, such as adding a nullable column. `stop`, `start`, `cutover`, `revert`, and `skip-revert` are not supported, and `cancel` works only on a concurrent index build. Read [what is not GA yet](https://github.com/block/schemabot/blob/main/docs/postgresql.md#beta-status) before promising anything.

## Repo layout

A schema directory holds `schemabot.yaml` and the table files. Files sit next to `schemabot.yaml` for a single schema, or in one subdirectory per **namespace** (a MySQL database, a PostgreSQL schema, or a Vitess keyspace). See [Namespaces](https://github.com/block/schemabot/blob/main/docs/namespaces.md).

**MySQL**, one namespace:

```text
schema/
├── schemabot.yaml       database: shop, type: mysql
└── shop/
    ├── users.sql        CREATE TABLE `users` (...)
    └── orders.sql       CREATE TABLE `orders` (...)
```

**PostgreSQL**, one subdirectory per PostgreSQL schema. PostgreSQL defines indexes outside the table, so a table's file holds its `CREATE TABLE` followed by that table's `CREATE INDEX` statements:

```text
schema/
├── schemabot.yaml       database: shop, type: postgres
└── public/              PostgreSQL schema
    ├── users.sql        CREATE TABLE "users" (...); CREATE INDEX ...
    └── orders.sql       CREATE TABLE "orders" (...)
```

**Vitess**, one subdirectory per keyspace, each with its own `vschema.json`:

```text
schema/
├── schemabot.yaml       database: shop, type: vitess
├── shop_sharded/        sharded keyspace
│   ├── orders.sql
│   └── vschema.json     vindexes and sharded tables
└── shop/                unsharded keyspace, such as sequence tables
    ├── orders_seq.sql
    └── vschema.json
```

The repository's [examples](https://github.com/block/schemabot/tree/main/examples) directory has complete MySQL and Vitess layouts. For PostgreSQL, `schemabot pull` prints a live database's schema in the format the files use; see [Schema pull](https://github.com/block/schemabot/blob/main/docs/postgresql.md#schema-pull).

## Making the change

Edit the files to the **desired end state**. SchemaBot computes the DDL.

To add a `deleted_at` column to `payments`, change `payments.sql` from:

```sql
CREATE TABLE `payments` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `amount_cents` bigint NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
```

to:

```sql
CREATE TABLE `payments` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `amount_cents` bigint NOT NULL,
  `deleted_at` datetime NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
```

The plan comment on the PR then shows:

```sql
ALTER TABLE `payments` ADD COLUMN `deleted_at` datetime NULL;
```

That is the whole authoring step.

- **One table per file.** Edit the table's file to what the table should become.
- **Add a file to create a table; delete it to drop the table.** Drops are unsafe and follow a deploy order: see [Unsafe changes and drops](change-behavior.md#unsafe-changes-and-drops).
- **New columns on an existing table: prefer `NULL`.** The change applies while the old application is still running. A `NOT NULL` column without a `DEFAULT` fails every insert that doesn't set it, the moment it applies. If the column must end up `NOT NULL` with no default, stage it: add it nullable, backfill it, make the application always write it, then tighten it in a later PR. Tightening is copy-based, not instant.
- **Avoid foreign keys.** The online copy cannot preserve them. On MySQL, adding one can run only as direct DDL that blocks writes, which the server allows on small tables when [direct execution](https://github.com/block/schemabot/blob/main/docs/direct-execution.md) is on; otherwise the plan refuses it. A new table that takes part in a foreign key is flagged unsafe, because it could never be changed online afterwards. A table that already has foreign keys may not be changeable online at all, and that depends on the live table, so it can show up at apply time rather than in the plan. Some teams keep foreign keys knowingly and accept the cost; if the user's tables already have them, say what that means for this change rather than removing them. For new tables, keep referential integrity in the application. See [Lint and safety levels](https://github.com/block/schemabot/blob/main/docs/lint-and-safety-levels.md#what-unsafe-means).
- **Primary keys:** on MySQL, `bigint unsigned AUTO_INCREMENT` is a good default, with any public identifier in its own `UNIQUE` column. See [Choosing a primary key](https://github.com/block/schemabot/blob/main/docs/mysql.md#choosing-a-primary-key). On PostgreSQL the linter expects `bigint` or `uuid`.
- **Vitess:** edit the `.sql` for table structure and the keyspace's `vschema.json` for vindexes, sequences, and placement. Adding or dropping a sharded table touches both keyspaces; keep all the pieces in one PR. See [Sharded Vitess tables](vitess-sharded-tables.md).

## Local development and PR scope

**Converge locally from the same files.** The declarative files are as authoritative locally as anywhere else. Many repos converge a local or test database to them in place; for MySQL, [Spirit](https://github.com/block/spirit) can diff a running database against the files. Use the repo's own tooling, regenerate any data-access code the repo commits, and run the tests. The [dev loop](https://github.com/block/schemabot/blob/main/docs/pre-merge-workflow.md#the-dev-loop) describes it.

**Scope PRs so each apply does the most work it can:**

- **Put every change to one table in one PR.** Each apply is its own online change, and on a big table one copy can take days. Two PRs that each alter the same table pay for the copy twice. One PR with the table's full end state plans a single `ALTER`. If you know another change to the table is coming soon, include it.
- **Keep unrelated tables in separate PRs** for a database serving production traffic, so each plan, apply, and undo covers one change. Early-stage projects can batch freely.
- **Only when several tables need their cutovers at the same moment,** such as matching collations on columns they join on, put them in one PR and, on MySQL, apply it with `--defer-cutover` so their swaps run together. Most changes don't need this. [Change behavior](change-behavior.md#how-a-change-runs) says which statements it covers. PostgreSQL has no deferred cutover.
- **Ship the schema early.** A feature's schema usually settles before its code. Once you know the columns you need, put the schema change in its own PR, apply it, and merge it. That also releases the database lock, which a long-lived feature PR would otherwise hold.
