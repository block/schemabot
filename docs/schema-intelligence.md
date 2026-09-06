# Schema intelligence

SchemaBot can tell you what is in your databases, how it got there, and what is
changing now. Give your tools and agents that context with read-only access.

![SchemaBot brings live schemas, recorded changes, and active progress from a distributed database fleet into view.](../assets/schema-intelligence-fleet.svg)

[Examples](#start-with-a-question) · [Read-only access](#give-a-tool-read-only-access) ·
[API reference](#where-the-answers-come-from) · [Limits](#what-schemabot-does-not-remember)

## Start with a question

The examples below use illustrative values and excerpts of the CLI output.
Expand **API equivalent** for the HTTP requests and response fields. Authenticate requests as described under [read-only access](#give-a-tool-read-only-access).

**What databases are there?** Start with `schemabot databases`. It lists the
managed databases, their engines, and their configured environments and deployments.

```sh
schemabot databases
```

```text
DATABASE   TYPE      ENVIRONMENTS         DEPLOYMENTS
shop       mysql     staging, production  staging: us-east; production: eu-west, us-east
analytics  postgres  production           production: us-east
```

<details>
<summary>API equivalent</summary>

```http
GET /api/databases
```

```json
{
  "databases": [
    {
      "database": "shop",
      "type": "mysql",
      "environments": [
        {"environment": "staging", "deployments": ["us-east"]},
        {"environment": "production", "deployments": ["us-east", "eu-west"]}
      ]
    },
    {
      "database": "analytics",
      "type": "postgres",
      "environments": [
        {"environment": "production", "deployments": ["us-east"]}
      ]
    }
  ]
}
```

</details>

Use a database name and environment from this inventory in the reads below.

**What does this database look like?** Pull its live schema using a database
and environment from the inventory.

```sh
schemabot pull -d shop -e production
```

SQL output excerpt:

```sql
-- Namespace `shop` — 1 table

CREATE TABLE `orders` (
  `id` bigint unsigned NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
```

To focus on a table, add `--table orders`; it filters names by substring.

<details>
<summary>API equivalent</summary>

```http
POST /api/pull
Content-Type: application/json

{"database": "shop", "environment": "production"}
```

```json
{
  "database": "shop",
  "environment": "production",
  "namespaces": {
    "shop": {
      "tables": {
        "orders": "CREATE TABLE `orders` (\n  `id` bigint unsigned NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
      }
    }
  }
}
```

</details>

The response gives you the table definitions without logging into the database.
A pull reads the environment's **primary deployment**, not every deployment or
shard independently; it does not prove every copy in the fleet matches.

**Which tables are missing a primary key?** Add `--lint` to inspect a live
schema for issues. On a MySQL database, a finding looks like this:

```sh
schemabot pull -d shop -e production --lint
```

Lint output excerpt:

```sql
-- Lint: 1 violation
--   [warning] order_events: No primary key defined
```

<details>
<summary>API equivalent</summary>

```http
POST /api/pull
Content-Type: application/json

{"database": "shop", "environment": "production", "lint": true}
```

```json
{
  "database": "shop",
  "environment": "production",
  "namespaces": {
    "shop": {
      "lint": [{
        "table": "order_events",
        "linter": "primary_key",
        "severity": "warning",
        "message": "No primary key defined"
      }]
    }
  }
}
```

</details>

Repeat across the supported databases and environments in your inventory, then
group these findings with their database, environment, namespace, and table.
Your agent can list the affected tables; your dashboard can track fixes over
time. Schedule pulls and cache results rather than scanning the fleet on every
question. For structured columns and indexes alongside the DDL, add
`catalog_detail: detailed` where supported.

**Which change added this column?** Start with the database's history,
then inspect a candidate apply.
Check the recorded table DDL and task outcome, and follow the PR URL when one
is present. The apply's completion time tells you when that apply finished;
it is not a separate timestamp for each DDL statement. Stored plans add the
proposed DDL and source commit, but history exposes no plan ID to join a plan
to its exact execution. A plan alone does not prove a change ran.


Take a candidate `apply_id` from history, then read its execution record:

```sh
schemabot status -d shop -e production
schemabot status apply-example-42
```

The first command prints the database's history; the second shows one apply.

<details>
<summary>API equivalent</summary>

```http
GET /api/progress/apply/apply-example-42
```

```json
{
  "apply_id": "apply-example-42",
  "state": "completed",
  "database": "shop",
  "environment": "production",
  "pull_request": "https://github.com/example/schemas/pull/42",
  "completed_at": "2026-09-01T03:02:00Z",
  "tables": [{
    "table_name": "orders",
    "ddl": "ALTER TABLE `orders` ADD COLUMN `discount_code` varchar(32) DEFAULT NULL",
    "status": "completed"
  }]
}
```

</details>

The completed task supplies the DDL evidence; the PR URL supplies the review
context.

**What is running across the fleet, and what needs attention?** List running
applies, then watch one with the CLI's live progress view.
Compare successive snapshots to see whether copying is advancing, throttled,
or waiting for cutover. Status returns a bounded list, so narrow by environment
and deployment when it is truncated. The API exposes `has_more` and
`state_counts` to distinguish the returned rows from the totals.


```sh
schemabot status -e production --state running --limit 100
schemabot progress apply-example-73
```

The first command lists running applies; `progress` refreshes the selected
apply until it finishes. One frame from that live view:

```text
~ orders: 🟦🟦🟦🟦🟦🟦🟦🟦🟦🟦🟦🟦⬜⬜⬜⬜⬜⬜⬜⬜ 60.00% (throttled)
  ALTER TABLE `orders` ADD INDEX `idx_status`(`status`);
  • Rows: 6,000,000 / 10,000,000
```

Use `schemabot status apply-example-73` for a single snapshot.
The API example below uses `active=true`, which also includes other in-flight
states.

<details>
<summary>API equivalent</summary>

```http
GET /api/status?active=true&environment=production&limit=100
```

```json
{
  "limit": 100,
  "max_limit": 1000,
  "has_more": true,
  "state_counts": {"running": 120},
  "applies": [{"apply_id": "apply-example-73", "database": "shop", "state": "running"}]
}
```

This excerpt shows one row from a truncated list: 120 applies match, but only
100 are returned. Inspect a returned apply:

```http
GET /api/progress/apply/apply-example-73
```

```json
{
  "apply_id": "apply-example-73",
  "state": "running",
  "tables": [{
    "table_name": "orders",
    "status": "running",
    "rows_copied": 6000000,
    "rows_total": 10000000,
    "percent_complete": 60,
    "throttled": true
  }]
}
```

</details>

This copy is throttled at 60%; compare later snapshots before calling it stuck.

These are workflows you can build from SchemaBot's API; the caller supplies
the search, aggregation, and presentation.

## Give a tool read-only access

Every endpoint in this guide belongs to the read tier. `POST /api/pull` uses a
request body to select what to read; it does not change the schema.

Configure authentication before connecting a service or agent. With OIDC,
keep its identity outside the admin groups that grant write access. With
forward-auth, use the allowlisted read-service identity lane, which rejects
write requests. See [authentication](configuration.md#authentication) for setup.

A schema catalog or replication service can start with two calls: `databases`
to discover registered sources, then `pull` with `catalog_detail: detailed`
where supported to read their table structure without parsing DDL. Store each
snapshot with its database, environment, namespace, table, and fetch time;
refresh periodically and respect `Retry-After` when a pull is rate limited.

Add `history` and `progress` when you need execution context, such as the DDL
and recorded task timestamps for a SchemaBot-managed change. These records
complement the live snapshot; they are not a stream of every schema change
made by any means.

The CLI exposes the same reads: `schemabot status`, `databases`, `list-plans`,
and `logs` accept `--json`; `schemabot pull` uses `-o json`.

## Where the answers come from

| Question | Read | Source and freshness |
|---|---|---|
| What is live? | `POST /api/pull` | Reads the target database now; rate limited and not cached by SchemaBot |
| What was proposed or run? | `GET /api/plans`, `/api/history/{database}`, `/api/logs` | SchemaBot's stored plans, execution records, and lifecycle events |
| What is changing? | `GET /api/status`, then `/api/progress/apply/{apply_id}` | Stored apply state, plus engine progress while active; stored results after completion |

Storage reads do not scan the target database. Pulls do, so inventories should
keep their own timestamped snapshots and respect the pull rate limits.

## What is live right now: `pull`

`POST /api/pull` returns the current schema of a database in one environment,
as SchemaBot sees it: one canonical `CREATE TABLE` statement per table, grouped
by namespace (a schema name on MySQL, a keyspace on Vitess, a schema on
PostgreSQL). It is the same read `schemabot onboard` uses to generate a
declarative directory, and the same read a plan starts from.

For a multi-deployment environment, this reads only the primary deployment:
first in the configured rollout order, otherwise first alphabetically. The
request has no deployment selector.

```json
{ "database": "shop", "environment": "production" }
```

```json
{
  "database": "shop",
  "type": "mysql",
  "environment": "production",
  "table_count": 2,
  "namespaces": {
    "shop": {
      "tables": {
        "orders": "CREATE TABLE `orders` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT, ...",
        "users":  "CREATE TABLE `users` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT, ..."
      }
    }
  }
}
```

Three options turn a schema dump into an inventory:

- **`namespaces`** restricts the pull to named schemas. Omit it and SchemaBot
  discovers every namespace the database has.
- **`catalog_detail`** adds a structured catalog beside the DDL. `basic`, the
  default, returns the DDL and artifacts alone. `detailed` adds the namespace
  catalog (engine, table count) and a per-table catalog: the table's kind and
  comment, columns with types and nullability, indexes with their columns and
  uniqueness, foreign keys, and the engine's own estimated row count and data
  size, so a caller never has to parse `CREATE TABLE` text to answer "which
  tables have a `tenant_id` column" or "which tables are over a gigabyte".
  Catalog estimates come from the engine's statistics and can lag reality.
- **`lint`** runs SchemaBot's schema-shape linters over every pulled table and
  attaches the findings per namespace, each with its linter name, severity,
  table, and column. A clean audit returns an explicit empty list, so a caller
  can tell "no findings" from "not requested". This is how you audit a whole
  fleet for a discouraged primary key type or a disallowed charset without
  planning a change. The audit runs only the linters that look at the schema
  itself; the diff-based ones (unsafe drops, dropping an index that was never
  made invisible) need a proposed change and never fire on a pull. See
  [lint-and-safety-levels.md](lint-and-safety-levels.md#auditing-a-live-schema-pull---lint).

For example, inspect a MySQL table with its size estimates:

```sh
schemabot pull -d shop -e production --table orders --catalog-detail detailed
```

The CLI adds this line above the table's SQL:

```sql
-- Table `orders` — rows ~18,402,551, size ~4.0 GiB (engine estimates)
```

Add `-o json` to read columns, indexes, and estimates as structured fields.
The pretty view keeps columns and indexes in the SQL instead of repeating them.

<details>
<summary>API equivalent and structured catalog</summary>

```http
POST /api/pull
Content-Type: application/json

{"database": "shop", "environment": "production", "catalog_detail": "detailed"}
```

Response excerpt for `orders` (the CLI's `--table` filter runs client-side):

```json
{
  "database": "shop",
  "environment": "production",
  "namespaces": {
    "shop": {
      "table_catalog": {
        "orders": {
          "name": "orders",
          "kind": "table",
          "columns": [{"name": "id", "type": "bigint unsigned", "nullable": false}],
          "indexes": [{"name": "PRIMARY", "primary": true, "unique": true, "parts": ["id"]}],
          "estimated_row_count": 18402551,
          "data_size_bytes": 4294967296
        }
      }
    }
  }
}
```

</details>

The envelope differs by dialect:

- **Vitess.** The namespace's VSchema comes back as an artifact beside the
  tables. `detailed` returns the namespace catalog without the per-table
  catalog, since that metadata is read from the information schema on the
  MySQL path and has no Vitess equivalent wired up yet.
- **PostgreSQL.** A schema is the namespace, ordinary and partitioned tables
  are exported, and only `basic` catalog detail is available. The full
  envelope is in [postgresql.md](postgresql.md).
- **Lint.** The linters parse MySQL-family DDL only, so a lint request against
  another dialect is rejected rather than answered clean.

The CLI wraps it as `schemabot pull -d shop -e production`, with `--namespace`,
`--table` (a client-side filter that errors when it matches nothing),
`--catalog-detail`, `--lint`, and `-o json` for the raw response.

### What a pull costs

A pull reads the live database: one connection per namespace, one
`SHOW CREATE TABLE` per table, and at `detailed` several scans over the
information schema. That is cheap for a single database and expensive for a
tight loop over a fleet, so pull is rate limited in two lanes that must both
have budget: per caller and per target database. A refused pull is a `429`
with a `Retry-After` header and a machine-readable `rate_limited` error code,
so a well-behaved client backs off and retries rather than treating it as a
failure. The default budgets, and how to tune them, are in
[configuration.md](configuration.md#rate-limits). The expected shape of a
consumer is a periodic sync that pulls each database on a schedule and keeps
its own copy, not a per-request passthrough.

## What was proposed and what ran

SchemaBot stores computed plans separately from execution records. A plan
captures proposed DDL and its source commit. An apply records an execution,
with operations for deployments, tasks for tables, and lifecycle logs. These
records explain SchemaBot-managed changes; they are not an audit of changes
made outside SchemaBot.

### The fleet view: `GET /api/status`

`GET /api/status` is the one call that spans every database. It returns the
most recent applies across the SchemaBot instance. Alongside the list it returns
`state_counts`, a tally of every apply matching the filters regardless of the
list size. For example, `failed=true&last=24h` counts failed applies updated
within the last day.

| Filter | Meaning |
|---|---|
| `environment`, `deployment` | Scope to one environment or one data plane |
| `active=true` | Only applies still in flight |
| `state=<state>` or `failed=true` | Only applies in a state, or only failures |
| `last=<duration>` | Only applies updated within a window, such as `24h` |
| `limit` | Result limit: default 20, maximum 1,000 |

Check `has_more` before treating the list as complete. There is no pagination
cursor; increase the limit or narrow the filters. Even at the maximum, a
busy scope can exceed the returned list. `state_counts` still covers all
matching applies.

Each row carries the apply ID, database, environment, deployment, state,
engine, caller, error message, and started, completed, and updated
timestamps. `schemabot status` renders it; `schemabot status --json` returns
it raw.

### One database's history: `GET /api/history/{database}`

`GET /api/history/{database}` lists every apply ever run against a database,
newest first, optionally filtered to one environment. Each entry names the
apply, its state, engine, environment, caller, error and error code if it
failed, and when it started and completed. The caller is the stored identity,
with `cli` or `owner/repo#N` as a fallback when it was not recorded.
`schemabot status -d shop` renders it.

### The DDL itself: `GET /api/plans`

History records executions. Plans describe what was proposed.
`GET /api/plans` lists stored plans, filterable by `database`, `environment`,
`repository`, and `pull_request` (with `repository`), plus a `last` window.
Each summary carries the plan ID, database, environment, deployment, the
repository, PR, and head SHA it was planned from, a count of changes by
operation, and how many were unsafe or blocked.

`GET /api/plans/{plan_id}` returns the full plan: for every table, the exact
DDL that was computed, the change type, whether it was classified unsafe and
why, and whether it was classified for direct execution. Because a plan is
stamped with the commit it was computed from, a caller can join it back to the
repository to inspect the proposed change at that commit. To establish what
actually ran, inspect the apply's task DDL and outcome through progress.
`schemabot list-plans` and `schemabot list-plans <plan_id>` render
both, with `--json` for the raw response.

The list defaults to 20 plans and caps at 200. Check `has_more`; there is no
pagination cursor. Filters narrow the recent results but do not provide an
exhaustive search across all historical DDL.

### The lifecycle log: `GET /api/logs`

`GET /api/logs/{database}?apply_id=…` (or `GET /api/logs?apply_id=…`) returns
SchemaBot's own event stream for an apply: queued, claimed, dispatched, every
state transition, every error, each with its level and source. Passing
`deployment` switches to the data plane's log for the same apply, which is
where the engine's own row-copy and throttle lines live. `schemabot logs
<apply-id>` renders it and `-f` follows it.

### Locks and settings

`GET /api/locks` lists every database lock currently held and who holds it (a
PR or a CLI operator), which is how a dashboard shows what is claimed before it
shows what is running. `GET /api/settings` returns the deployment-wide settings
that operators can change at runtime. `schemabot locks` and `schemabot
settings` render them.

## What is changing now: `GET /api/progress/apply/{apply_id}`

While an apply runs, `GET /api/progress/apply/{apply_id}` is the live view.
For every table it returns the DDL being applied, rows copied against rows
total, percent complete, an ETA in seconds, the checksum position, whether
the copy is currently throttled and why, and whether the change ran as
instant DDL. Sharded engines add one entry per shard with its own rows, ETA,
and cutover attempts. Multi-deployment applies list each operation with its
deployment, target, state, and cutover policy.

```
  apply-7f3a…  shop / production          running        ETA 42m

    orders   ALTER TABLE `orders` ADD INDEX `idx_status` (`status`)
             rows 61,204,113 / 88,000,000   69%   throttled: false
             shard -80    31,010,442 / 44,000,000   70%
             shard 80-    30,193,671 / 44,000,000   68%
```

The numbers come from the engine while the apply is active, so they are as
fresh as the last poll. Once the apply is terminal, the same endpoint answers
from storage: rows, throttle state, and checksum counts are preserved on the
task record, while ETA and per-shard rows are not, since neither means
anything for a finished change. `schemabot progress <apply-id>` renders it
and redraws until the apply finishes; `schemabot status <apply-id>` prints one
snapshot.

## What SchemaBot does not remember

Keep these boundaries in mind when interpreting a result:

- **Lint findings on a plan.** The lint results and errors that appear on the
  PR comment are computed at plan time and not persisted with the plan. A
  stored plan returns them empty, and empty means "not stored", not "clean".
  To audit a live schema after the fact, use `pull` with `lint`.
- **Table sizes on a plan.** A plan carries DDL and classification, not row
  counts. Sizes live on the pull catalog (engine estimates) and on progress
  (exact rows copied).
- **Changes outside SchemaBot.** History starts when SchemaBot started
  managing the database. Changes made outside SchemaBot have no corresponding
  plan or apply in its history; a pull shows their current schema, not who made them.
- **Reversals as erased history.** A rollback is itself an apply
  with its own ID, so the record shows the change and then the reversal,
  never a gap.
