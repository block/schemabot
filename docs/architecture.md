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
- **API** ([`pkg/api`](../pkg/api/)): HTTP service that manages routes, server lifecycle, and Tern client pools
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
schemabot progress <apply_id>               # Watch progress
schemabot cutover -e staging <apply_id>     # Trigger cutover
schemabot stop -e staging <apply_id>        # Pause execution
schemabot start -e staging <apply_id>       # Resume execution
schemabot volume -e staging <apply_id> -v 8 # Adjust speed
schemabot revert -e staging <apply_id>      # Revert during the revert window (Vitess)
schemabot skip-revert -e staging <apply_id> # Finalize (Vitess)
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
7. On PR merge/close, SchemaBot cleans up stored PR state and releases locks

Users can also run `schemabot plan` manually in a PR comment to re-plan without waiting for auto-plan.

**Check Runs** — SchemaBot publishes aggregate GitHub checks that block merge until managed schema changes are applied. Per-database state is stored internally and rolled up into the aggregate check. Production applies require staging to be clean first when environments are ordered. See [`check-runs.md`](check-runs.md) for the full lifecycle, race-safety model, and branch protection setup.

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

**Design principle: control operations must be stateless.** A control operation
should work by writing to shared external state (the target database or an API),
not by reaching into the in-memory state of a specific SchemaBot process.

SchemaBot is designed for HA — multiple instances can run behind a load balancer,
and any instance may restart at any time. A webhook or CLI request can be routed to
any instance, so control operations cannot depend on hitting the "right" process.
The engine's in-memory state (e.g., Spirit's `runningMigration`) is ephemeral and
process-local.

Each engine uses shared external signals for control:

| Engine | Cutover signal | Stop signal | State source |
|---|---|---|---|
| Spirit | Drop `_spirit_sentinel` table in target DB | Cancel via context / flag table | Checkpoint table + INFORMATION_SCHEMA |
| PlanetScale | Complete deploy request via API | Cancel deploy request via API | PlanetScale API + SHOW VITESS_MIGRATIONS |

These signals are durable and accessible from any instance — the engine running in
the background detects the signal regardless of which SchemaBot instance triggered it.

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

### Progress Flow And Observers

Tern's progress poller is where raw engine progress becomes SchemaBot state. On each tick, Tern asks the engine for progress, derives task state from the engine response, derives the apply state from those tasks, persists both, and notifies an optional `ProgressObserver`.

```
Engine progress
      |
      v
Poller reads current engine state
      |
      v
Derive task state
  - normalize raw engine state
  - preserve stored task state if it is already ahead
  - one task per DDL
  - rows copied, rows total, percent complete
      |
      v
Derive apply state
  - aggregate stored task states
  - active, terminal, and control states
      |
      v
Persist applies + tasks
      |
      +--> Observer.OnProgress / Observer.OnTerminal
              |
              v
          GitHub PR
          - progress comment
          - terminal summary comment
```

State authority is intentionally one-way: engine progress is an input, not the durable source of truth. Raw engine states are first normalized into canonical task states. The poller then reconciles that normalized engine task state with the stored task row: terminal stored tasks stay terminal, scheduler/control-owned states such as `stopped` and `failed_retryable` are not overwritten by stale active engine polls, and ordinary active states can move forward but not backward. Apply state is derived after task rows are coherent, so PR comments, CLI progress, and scheduler checks never need to reason about raw engine states directly.

Unknown raw engine states normalize to `running`. This keeps unfamiliar in-flight work visible and blocking without leaking engine-specific strings into SchemaBot state or UI. Once the engine state is understood, update `NormalizeTaskStatus()` and the state-policy tests so the new state has an explicit task-state mapping and ordering policy.

The `ProgressObserver` interface (`pkg/tern/observer.go`) enables external notifications from the apply progress poller. Observers see SchemaBot's derived apply/task state, not raw engine state.

```go
type ProgressObserver interface {
    OnProgress(apply *storage.Apply, tasks []*storage.Task)
    OnTerminal(apply *storage.Apply, tasks []*storage.Task)
}
```

**How it works:**

```
Webhook handler: "someone commented schemabot apply"
    |
    +-- Creates CommentObserver (posts PR comments)
    +-- Calls SetPendingObserver on the API service
    +-- Calls ExecuteApply
            |
            v
        store pending apply + tasks
            +-- Registers Observer on the apply before scheduler dispatch
            +-- Wakes scheduler
                    |
                    v
                Scheduler worker claims apply
                    |
                    v
                Client.ResumeApply()
                    |
                    +-- Progress tick: poll engine, derive task/apply state
                    |   +-- Observer.OnProgress()
                    |       +-- edits PR progress comment
                    |
                    +-- Terminal state: Observer.OnTerminal()
                        +-- edits progress comment to final state
                        +-- posts summary comment
                        +-- updates check runs
```

**Key design properties:**

- **Single progress path** — notifications are tied to the same progress path
  that updates task and apply state. There is no unrelated watcher with its own
  interpretation of state.
- **Per-apply** — each apply has its own observer (or none). CLI applies have no
  observer. Webhook applies get a `CommentObserver`.
- **Fire-and-forget** — observer errors are logged but never block the schema
  change. If GitHub is down, the apply continues.
- **Reconstructable** — on recovery after restart, the observer is reconstructed
  from the apply record's stored GitHub context (repo, PR, installation ID).
- **Composable** — different observers for different backends (GitHub comments,
  Slack, etc.). A `MultiObserver` can combine them.
- **Works for both client types** — `LocalClient` notifies from the engine poller.
  `GRPCClient` notifies from a storage-polling goroutine. The control plane posts
  comments regardless of where the engine runs.

**Missing summary recovery.** The `apply_comments` table tracks which comments
were posted (`progress`, `summary`). On startup, the webhook runtime starts a
best-effort reconciliation pass for completed applies with a progress comment
but no summary comment. That means `OnTerminal` was missed during a process
restart. The webhook runtime posts the missing summary from the apply record's
stored GitHub context, and errors never fail server startup.

### Scheduler

The scheduler is part of Tern orchestration. The API service starts it on server startup because the server owns process lifecycle, configuration, and the Tern client pool, but the scheduler's work is to coordinate applies: claim storage records, refresh their heartbeat lease, and call `ResumeApply()` on the right Tern client.

Each scheduler worker runs immediately on startup, then polls every 10 seconds. The worker claims at most one apply per tick and resumes it through the Tern client for that apply's deployment and environment. Fresh applies also send a best-effort wake signal after their apply and task rows are committed, so a worker can claim them immediately instead of waiting for the next poll tick.

`scheduler_workers` controls scheduler concurrency. The default is four workers, so an untuned server can still make progress across independent databases and environments concurrently. More workers help larger installations with many independent schema changes because each worker can claim and resume a different target during the same scheduler tick.

In a multi-pod deployment, every SchemaBot pod runs its own scheduler. Each scheduler has its own configurable worker pool, and every worker coordinates through shared storage before resuming an apply. When a recovered apply came from a PR comment, the worker attaches a reconstructed `ProgressObserver` before calling `ResumeApply()` so the resumed progress poller can keep updating PR comments.

```
+--------------------------+      +--------------------------+
| SchemaBot pod A          |      | SchemaBot pod B          |
| Scheduler                |      | Scheduler                |
| +----------------------+ |      | +----------------------+ |
| | worker 0             | |      | | worker 0             | |
| | claim apply work     | |      | | claim apply work     | |
| | attach Observer      | |      | | attach Observer      | |
| | ResumeApply          | |      | | ResumeApply          | |
| +----------------------+ |      | +----------------------+ |
| +----------------------+ |      | +----------------------+ |
| | worker 1             | |      | | worker 1             | |
| | claim apply work     | |      | | claim apply work     | |
| | attach Observer      | |      | | attach Observer      | |
| | ResumeApply          | |      | | ResumeApply          | |
| +----------------------+ |      | +----------------------+ |
+------------+-------------+      +-------------+------------+
             |                                  |
             | claims + leases                  | claims + leases
             v                                  v
        +-----------------------------------------------+
        | Shared SchemaBot storage                      |
        | applies, tasks, named apply-target locks      |
        | claims use FOR UPDATE SKIP LOCKED             |
        +-----------------------------------------------+

             |                                  |
             | Observer posts/edits comments    | Observer posts/edits comments
             v                                  v
        +-----------------------------------------------+
        | GitHub PR                                     |
        | progress comment + terminal summary comment   |
        +-----------------------------------------------+
```

The storage arrows represent scheduler coordination. The GitHub arrows represent optional observer notifications; CLI applies simply run without an observer.

A **claim** is an atomic storage operation: it selects one apply that needs work and refreshes its `updated_at` heartbeat in the same transaction. That heartbeat refresh is the scheduler's lease while it reloads state and calls `ResumeApply()`. Claims use `FOR UPDATE SKIP LOCKED` so multiple scheduler workers can run concurrently without taking the same apply.

A running apply becomes claimable after its heartbeat has been stale for more than one minute and it is in a state that can be resumed from persisted metadata. Terminal applies are already done, and stopped applies are not auto-resumed; the user must call `schemabot start`.

Freshly queued applies are also claimable once their task rows exist. `ExecuteApply` stores the pending apply and its initial tasks in one transaction, then sends a best-effort wake signal to the scheduler. The wake path is only a latency optimization: it nudges one worker to call the same claim path immediately instead of waiting for the next poll tick. It never bypasses storage claims or creates a second queue. If the scheduler is stopped or a wake is already pending, the signal is skipped and the normal poll loop still finds the apply.

```
HTTP apply request
      |
      v
store pending apply + tasks
      |
      v
best-effort scheduler wake
      |
      v
worker calls FindNextApply()
      |
      v
claim row + refresh heartbeat
      |
      v
TernClient.ResumeApply()
      |
      v
dispatch through LocalClient or GRPCClient
```

SchemaBot has two different kinds of locking:

- **Database locks** are coordination state for users and automation. They make ownership visible in CLI/webhook flows and let a human intentionally hold, release, or force-release work for a database.
- **Apply invariants** are correctness rules enforced by storage. They do not depend on a database lock being present, because direct API callers and `--no-lock` flows still must not create invalid concurrent work.

SchemaBot enforces one active apply per database, database type, and environment when an apply is created or moved back into an active state. Storage takes a per-target MySQL named lock, checks `applies`, writes the row, and releases the lock before returning.

The named lock is internal storage infrastructure, not user-facing lock state or apply lifecycle state. It gives first writers a target-specific serialization point without creating mutex rows during the apply request. Same-target writers wait and re-check `applies`; different targets use different lock names and can proceed concurrently.

If storage cannot acquire the named lock within the bounded wait, the write fails before changing `applies`. If storage cannot confirm lock release, it discards the connection so MySQL releases the session-scoped lock.

The scheduler relies on that invariant. Additional workers increase concurrency across independent targets; they do not make one database/environment run multiple applies at once.

If a worker finds no claimable apply, it waits briefly and retries once during the same tick. This lets another worker that lost a claim race pick up a different target without waiting for the next 10-second poll.

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

**gRPC Mode** (`GRPCClient`) — SchemaBot delegates execution to an external Tern service over gRPC. SchemaBot still maintains its own storage for locks, plans, applies, and tasks. HTTP apply requests queue durable control-plane rows first; scheduler workers dispatch the queued apply to the remote Tern service and then poll it until terminal.

```
┌──────────────────────────────┐        ┌──────────────────────────────┐
│ SchemaBot Server             │ gRPC   │ External Tern                │
│                              │        │                              │
│ CLI / Webhook / API          │        │ Engine (Spirit, etc.)        │
│      │                       │        │      │                       │
│      ▼                       │        │      ▼                       │
│ SchemaBot Storage            │        │ Internal state               │
│ (locks, plans, applies)      │        │ (remote apply rows)          │
│      │                       │        │      │                       │
│      ▼                       │        │      ▼                       │
│ Scheduler worker             │        │ Tern Proto Interface         │
│      │                       │        │      │                       │
│      ▼                       │        │      ▼                       │
│ GRPCClient ──────────────────┼────────▶ Engine ─────────────────────▶│ Target DB
└──────────────────────────────┘        └──────────────────────────────┘
```

Used for: distributed deployments where SchemaBot and the database engine run on different hosts.

**Identity resolution (`apply_identifier` vs `external_id`):**

In gRPC mode, SchemaBot and Tern maintain separate storage with separate IDs. SchemaBot generates an `apply_identifier` for HTTP callers when it queues the apply. The scheduler later calls remote Tern, receives Tern's own apply ID, and stores that remote ID as `external_id`:

An accepted apply must return an apply ID and must be represented in SchemaBot
storage before webhook progress tracking or Check Run ownership can proceed.
Without that stored ID, SchemaBot cannot safely tie terminal engine state back
to the PR check that started the work.

```
Apply flow:
  ExecuteApply
    → stores apply_identifier="apply-abc123", external_id=""
    → wakes scheduler
    → returns apply_id="apply-abc123" to HTTP caller

  Scheduler worker claims apply "apply-abc123"
    → GRPCClient.ResumeApply() calls remote Apply()
    → Tern returns ApplyId:"tern-42"
    → SchemaBot stores external_id="tern-42"
    → worker polls remote progress until terminal

Subsequent RPCs (Progress, Stop, Start, Cutover, Volume):
  HTTP caller sends apply_id="apply-abc123"
    → resolveApplyID("apply-abc123")
    → storage lookup → external_id="tern-42"
    → GRPCClient sends ApplyId:"tern-42" to Tern
```

In local mode (`client.IsRemote() == false`), `LocalClient` runs in the same process and writes to the same database as the API layer:

```
Apply flow (local):
  ExecuteApply
    → stores apply_identifier="apply-def456", external_id=""
    → wakes scheduler
    → returns apply_id="apply-def456" to HTTP caller

  Scheduler worker claims apply "apply-def456"
    → LocalClient.ResumeApply() runs the engine locally
    → external_id remains empty

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
