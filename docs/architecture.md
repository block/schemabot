# Architecture

```
┌───────────────────────────────────────────────────────────────────────┐
│                             SchemaBot                                 │
│                                                                       │
│  ┌────────┐                                                           │
│  │ GitHub │──────┐                                                    │
│  │   PR   │      │                                                    │
│  └────────┘      ▼                                                    │
│              ┌─────────┐    ┌──────┐    ┌─────────────┐   ┌────────┐  │
│  ┌─────────┐ │   API   │───▶│ Tern │───▶│   Spirit    │──▶│ MySQL  │  │
│  │   CLI   │▶│ pkg/api │    │Client│    ├─────────────┤   ├────────┤  │
│  │ pkg/cmd │ └────┬────┘    └──┬───┘    │ PlanetScale │──▶│ Vitess │  │
│  └─────────┘      │            │        └─────────────┘   └────────┘  │
│                   ▼            │                                      │
│              ┌─────────┐       │                                      │
│              │ Storage │◀──────┘                                      │
│              │  MySQL  │                                              │
│              └─────────┘                                              │
└───────────────────────────────────────────────────────────────────────┘
```

## Declarative Schema

SchemaBot uses declarative schema files — you describe the desired end state in SQL, and SchemaBot computes the DDL needed to get there. See the [README](../README.md#declarative-schema) for examples.

Schema files are organized by [namespace](namespaces.md) (MySQL schema name or Vitess keyspace) in a directory with a `schemabot.yaml` config.

## Layers

SchemaBot has three layers:

```
┌─────────────────────────────────────────────────┐
│  CLI / PR Comments / API                        │  User-facing
├─────────────────────────────────────────────────┤
│  Tern (orchestrator)                            │  Plans, applies, tasks, locks
├─────────────────────────────────────────────────┤
│  Engine (executor)                              │  Diffs schema, executes DDL
└─────────────────────────────────────────────────┘
```

- **CLI** ([`pkg/cmd`](../pkg/cmd/)): User-facing commands (`plan`, `apply`, `progress`, `stop`, `start`, `cutover`, etc.)
- **GitHub PR Comments**: Trigger schema changes via PR comments (`schemabot plan -e staging`). See [GitHub App Setup](./github-app-setup.md)
- **API** ([`pkg/api`](../pkg/api/)): HTTP service that manages Tern client pools and crash recovery
- **Tern** ([`pkg/tern`](../pkg/tern/)): Schema change orchestration — two implementations:
  - `LocalClient`: Embedded engine (single-process, for easy deployments (recommended to start))
  - `GRPCClient`: Delegates work to remote deployments (for distributed / multi-tenant architectures)
- **Engine** ([`pkg/engine`](../pkg/engine/)): Stateless executor interface for schema change backends
- **Storage** ([`pkg/storage`](../pkg/storage/)): Interface-based persistence (locks, plans, applies, tasks, logs, settings). MySQL implementation in [`pkg/storage/mysqlstore`](../pkg/storage/mysqlstore/)

Supporting packages: [`pkg/ddl`](../pkg/ddl/) (schema diffing), [`pkg/lint`](../pkg/lint/) (safety linting and auto-fix), [`pkg/secrets`](../pkg/secrets/) (secret resolution), [`pkg/schema`](../pkg/schema/) (shared schema types and embedded storage SQL)

## User Layer (CLI / PR Comments / API)

Users interact with SchemaBot through three interfaces:

**CLI** — `schemabot` commands for local development and CI:

```
schemabot plan -s ./schema -e staging       # Preview changes
schemabot apply -s ./schema -e staging -y   # Apply changes
schemabot progress -d mydb -e staging       # Watch progress
schemabot cutover -d mydb -e staging        # Trigger cutover
schemabot stop -d mydb -e staging           # Pause execution
schemabot start -d mydb -e staging          # Resume execution
schemabot volume -d mydb -e staging -v 8    # Adjust speed
schemabot revert -d mydb -e staging         # Roll back (Vitess)
schemabot skip-revert -d mydb -e staging    # Finalize (Vitess)
```

**PR Comments** — SchemaBot is a GitHub app installed on repos. The PR workflow:

1. Developer opens a PR that modifies files in a schema directory
2. SchemaBot auto-runs `plan` and posts a PR comment with the DDL diff
3. On new commits, SchemaBot re-plans and updates the comment
4. Developer triggers apply via PR comment:
   - `schemabot apply -e staging` — SchemaBot plans, locks, and posts a confirmation footer
   - `schemabot apply-confirm -e staging` — confirms and starts execution
   Options like `--defer-cutover` are passed in the apply step (see [Apply Options](#apply-options))
5. SchemaBot posts progress updates as the schema change executes
6. If `--defer-cutover` was used, developer triggers cutover via `schemabot cutover`
7. On PR merge/close, SchemaBot cleans up (releases locks, cancels pending changes)

Users can also run `schemabot plan` manually in a PR comment to re-plan without waiting for auto-plan.

**Check Runs** — SchemaBot creates GitHub check runs per (environment, database) that block merge until schema changes are applied. Production applies require staging to be clean first (staging-first enforcement). See [`pkg/checks/README.md`](../pkg/checks/README.md) for the full check run lifecycle, state transitions, and edge cases.

**API** — HTTP endpoints that both CLI and webhook use internally. The SchemaBot server exposes `/v1/plan`, `/v1/apply`, `/v1/progress`, `/v1/cutover`, etc.

### Apply Options

When applying, users can pass options that control execution:

| Option | Effect |
|---|---|
| `--defer-cutover` | Pause before the final table swap. User must manually trigger cutover. |
| `--enable-revert` | Keep a revert window open after cutover (Vitess only). User can roll back. |
| `--allow-unsafe` | Permit destructive changes (see [Unsafe Changes](#unsafe-changes) below). |

### Unsafe Changes

SchemaBot uses [Spirit's linter](https://github.com/block/spirit/tree/main/pkg/lint) to detect unsafe changes at plan time. The engine calls `lint.PlanChanges()`, which combines schema diffing with per-statement linting in a single pass. Each planned DDL statement comes back with lint violations at three severity levels:

- **Error** — blocks apply unless `--allow-unsafe` is passed (e.g., `DROP TABLE`, `DROP COLUMN`)
- **Warning** — informational, shown to user but does not block
- **Info** — suggestions and style preferences

Unsafe operations that produce error-severity violations:

- `DROP TABLE` — deletes the entire table and its data
- `DROP COLUMN` — deletes a column and its data from every row
- `MODIFY COLUMN` (type change) — may truncate or lose data if the new type is narrower
- `DROP INDEX` without first making it invisible — may cause query performance regression

`HasErrors()` on the plan result checks if any lint warning has error severity. The CLI, webhook check runs, and PR comments all use this to gate applies and surface warnings to reviewers.

### Control Operations

Users can control a running schema change via CLI, PR comments, or PlanetScale UI:

| Command | What happens |
|---|---|
| `schemabot stop` | Pause execution (Spirit: checkpoint saved; Vitess: cancel permanently) |
| `schemabot start` | Resume from checkpoint (Spirit only) |
| `schemabot cutover` | Trigger the final table swap |
| `schemabot volume 8` | Adjust execution speed (1=slowest, 11=fastest) |
| `schemabot revert` | Roll back a completed change during the revert window (Vitess only) |
| `schemabot skip-revert` | Close the revert window and finalize (Vitess only) |

## Tern Layer (Orchestrator)

Tern is the orchestration layer. It manages the schema change lifecycle: creating records, calling the engine, polling for progress, and tracking state. It defines a proto interface (`Plan`, `Apply`, `Progress`, `Cutover`, `Stop`, `Start`, `Volume`, `Revert`, `SkipRevert`).

### Plan

A **plan** is a diff between desired schema (files on disk) and current schema (live database).

```
schemabot plan -s ./schema -e staging

CLI → API → Tern.Plan()
               │
               ├─ reads schema files from disk
               ├─ calls engine.Plan(SchemaFiles, Credentials)
               │     engine diffs desired vs current → returns DDL
               ├─ stores Plan record in DB (DDL, namespaces, original schema)
               └─ returns PlanResponse{PlanID, Changes}
```

The engine's `Plan()` is a pure diff — no side effects, no storage. Tern wraps it with identity (PlanID) and persistence.

A plan record contains:
- The computed DDL per [namespace](namespaces.md)
- The original schema (for rollback)
- Metadata (database, type, environment, repo, PR)

Example plan for a MySQL database:

```
Plan: plan-a1b2c3d4

Namespace: testapp
  ALTER TABLE users ADD COLUMN email VARCHAR(255)
  ALTER TABLE orders ADD INDEX idx_status (status)
```

This plan has 2 DDL changes in one namespace. Applying it creates 2 tasks (one per DDL).

### Apply

An **apply** executes a previously created plan. One apply can have multiple **tasks**.

```
schemabot apply (after confirming plan)

CLI → API → Tern.Apply(PlanID)
               │
               ├─ looks up Plan from storage
               ├─ creates Apply record (links to Plan)
               ├─ creates Task records (one per DDL statement)
               └─ starts execution (mode depends on engine and flags)
```

### Apply vs Task

| | Apply | Task |
|---|---|---|
| **What** | The overall schema change operation | One DDL statement within an apply |
| **Granularity** | 1 per `schemabot apply` invocation | 1 per table being changed |
| **Example** | "Apply plan-123 to staging" | "ALTER TABLE users ADD COLUMN email" |
| **State** | pending → running → completed | pending → running → completed |
| **Storage** | `applies` table | `tasks` table |

Example: A plan with 3 DDL changes creates 1 apply and 3 tasks:

```
Apply: apply-456 (state: running)
  ├─ Task 1: ALTER TABLE users ADD COLUMN email     (state: completed)
  ├─ Task 2: ALTER TABLE orders ADD INDEX idx_status (state: running)
  └─ Task 3: CREATE TABLE audit_log                 (state: pending)
```

### Execution Modes

Both engines automatically detect and use **[instant DDL](https://dev.mysql.com/doc/refman/8.4/en/innodb-online-ddl-operations.html)** when possible. Instant DDL applies the change immediately via a metadata-only operation (no row copying). When instant DDL is used, the task completes in milliseconds with no copy phase.

Operations that support instant DDL include:
- Adding or dropping a column
- Setting or dropping a column default value
- Changing an index type
- Modifying ENUM/SET column definitions
- Adding or dropping a virtual generated column

Note: some instant operations (e.g., dropping a column) are also flagged as unsafe since they cause data loss. Instant DDL is skipped when `--defer-cutover` or `--enable-revert` is set, since those require the full online DDL flow for cutover and revert control.

**Spirit (MySQL) — Sequential** (default):
- Each task runs independently: instant DDL or copy rows → cutover → next task
- If task 2 fails, task 3 is cancelled but task 1's changes are already live

**Spirit (MySQL) — Atomic** (`--defer-cutover`):
- Tern still creates one task per DDL in storage (for per-table progress tracking)
- But submits all DDLs in one engine call
- All pause at "waiting for cutover"
- User triggers cutover → all tables swap together
- Note: "atomic" means atomic cutover, not parallel execution

```
Sequential:                        Atomic (--defer-cutover):

Task 1 → engine.Apply(DDL 1)      Task 1 ┐
  → cutover ✓                      Task 2 ┤→ engine.Apply(DDL 1 + 2 + 3)
Task 2 → engine.Apply(DDL 2)      Task 3 ┘
  → cutover ✓                        → engine runs DDLs
Task 3 → engine.Apply(DDL 3)        → waits for cutover
  → cutover ✓                        → cutover all together

(one engine call per task)         (one engine call, tasks track progress)
```

**PlanetScale (Vitess)** — always submits all DDLs as one [deploy request](https://planetscale.com/docs/vitess/schema-changes/deploy-requests):

1. Creates a branch from main
2. Applies all DDL and VSchema changes to the branch
3. Creates one deploy request (all DDLs share a `migration_context`)
4. Runs the deploy request — Vitess online DDL runs each DDL sequentially

Each DDL becomes a separate Vitess migration with its own `migration_uuid`, visible in `SHOW VITESS_MIGRATIONS`. DDL tasks in SchemaBot map 1:1 with migration UUIDs — one task per DDL. DDLs run sequentially, but within a single DDL, all shards run in parallel. Per-shard progress is surfaced in the Progress API but not stored — only aggregated per-task progress is persisted.

VSchema updates are tracked as separate tasks in the `vitess_tasks` table (one per keyspace). A deploy can be DDL-only, VSchema-only, or both.

| Flags | Behavior |
|---|---|
| (none) | DDLs run → auto-cutover → auto-skip revert → completed |
| `--defer-cutover` | DDLs run → pause at waiting for cutover → user triggers cutover → completed |
| `--defer-cutover --enable-revert` | DDLs run → pause → user triggers cutover → revert window → user reverts or skips → completed |

### Integration Modes

There are two ways to deploy the tern layer:

**Local Mode** (`LocalClient`) — Everything runs in one process. SchemaBot calls the engine directly and manages all state in its own storage (MySQL).

```
┌─────────────────────────────────────────────────────┐
│ schemabot process                                   │
│                                                     │
│ CLI / Webhook / API                                 │
│      │                                              │
│      ▼                                              │
│ SchemaBot Storage (plans, applies, tasks, locks)    │
│      │                                              │
│      ▼                                              │
│ LocalClient (tern orchestrator)                     │
│      │                                              │
│      ▼                                              │
│ Engine (Spirit or PlanetScale) ─────────────────────┼──▶ Target DB
└─────────────────────────────────────────────────────┘
```

Used for: local development, self-hosted deployments, single-binary setups.

**gRPC Mode** (`GRPCClient`) — SchemaBot delegates execution to an external Tern service over gRPC. SchemaBot still maintains its own storage for locks, plans, applies, and tasks — but the engine runs remotely.

```
┌──────────────────────────────┐        ┌──────────────────────────────┐
│ SchemaBot Server             │ gRPC   │ External Tern                │
│                              │        │                              │
│ CLI / Webhook / API          │        │ Engine (Spirit, etc.)        │
│      │                       │        │      │                       │
│      ▼                       │        │      ▼                       │
│ SchemaBot Storage            │        │ Internal state               │
│ (locks, plans, applies)      │        │ (opaque to SchemaBot)        │
│      │                       │        │      │                       │
│      ▼                       │        │      ▼                       │
│ GRPCClient ──────────────────┼────────▶ Tern Proto Interface ───────▶│ Target DB
└──────────────────────────────┘        └──────────────────────────────┘
```

Used for: distributed deployments where SchemaBot and the database engine run on different hosts.

**Identity resolution (`apply_identifier` vs `external_id`):**

In gRPC mode, SchemaBot and Tern maintain separate storage with separate IDs. When Apply succeeds, Tern returns its own ID (the remote engine's apply identifier). SchemaBot generates a separate `apply_identifier` for its HTTP callers and stores Tern's ID as `external_id`:

```
Apply flow:
  ExecuteApply → client.Apply() → Tern returns ApplyId:"tern-42"
    → SchemaBot generates apply_identifier="apply-abc123"
    → stores external_id="tern-42"
    → returns apply_id="apply-abc123" to HTTP caller

Subsequent RPCs (Progress, Stop, Start, Cutover, Volume):
  HTTP caller sends apply_id="apply-abc123"
    → resolveApplyID("apply-abc123")
    → storage lookup → external_id="tern-42"
    → GRPCClient sends ApplyId:"tern-42" to Tern
```

In local mode (`client.IsRemote() == false`), `LocalClient` runs in the same process and writes to the same database as the API layer:

```
Apply flow (local):
  ExecuteApply checks client.IsRemote() → false
  ExecuteApply → client.Apply()
    → LocalClient creates apply in DB:
        apply_identifier="apply-def456", external_id="" (not set — no remote Tern)
    → returns ApplyId:"apply-def456"
  ExecuteApply gets resp.ApplyId="apply-def456"
    → IsRemote()==false, so looks up existing record directly
    → reuses LocalClient's apply record
    → returns apply_id="apply-def456" to HTTP caller

Subsequent RPCs (local):
  HTTP caller sends apply_id="apply-def456"
    → resolveApplyID("apply-def456")
    → storage lookup → external_id="" (empty)
    → falls through to return apply_identifier="apply-def456"
    → LocalClient receives ApplyId:"apply-def456"
    → scopes task lookup to that apply
```

The `IsRemote()` method on the `tern.Client` interface makes this branching explicit.

Both modes implement the same `tern.Client` interface — callers don't know which mode is active.

## Engine Layer (Executor)

The engine is a stateless executor. It diffs schemas, executes DDL, and reports progress. It knows nothing about plans, applies, tasks, locks, or storage.

| Engine method | What it does |
|---|---|
| `Plan()` | Diff desired vs current schema → compute DDL |
| `Apply()` | Execute DDL in background |
| `Progress()` | Return current execution status |
| `Stop()` | Cancel execution |
| `Start()` | Resume stopped execution (Spirit only) |
| `Cutover()` | Trigger table swap |
| `Revert()` | Roll back completed change (Vitess only) |
| `SkipRevert()` | Close revert window (Vitess only) |
| `Volume()` | Adjust execution speed |

### Engine Differences

| | Spirit (MySQL) | PlanetScale (Vitess) |
|---|---|---|
| DDL execution | Inside SchemaBot process | Inside Vitess (remote) |
| Crash recovery | Resume from checkpoint table | Query PlanetScale API |
| Stop/Start | Pause + resume from checkpoint | Cancel permanently (no resume) |
| Cutover | Drop sentinel table | Complete deploy request |
| Revert | Not supported | Revert deploy request |
| Progress source | Spirit runner status | SHOW VITESS_MIGRATIONS |
| Multi-shard | N/A | Per-shard progress tracking |

## State Machine

See [pkg/state/README.md](../pkg/state/README.md) for the full state machine documentation, including apply states, task states, and how engine states map to SchemaBot states.
