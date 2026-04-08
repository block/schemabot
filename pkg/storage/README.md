# storage

Package `storage` defines the persistence interfaces for SchemaBot. All state — locks, plans, applies, tasks, logs, settings — flows through these interfaces. The MySQL implementation lives in [`storage/mysqlstore`](./mysqlstore/).

## Interface Hierarchy

`Storage` is the top-level interface that provides access to seven specialized stores:

```go
type Storage interface {
    Locks()     LockStore
    Plans()     PlanStore
    Applies()   ApplyStore
    Tasks()     TaskStore
    ApplyLogs() ApplyLogStore
    Checks()    CheckStore
    Settings()  SettingsStore
    Ping(ctx)   error
    Close()     error
}
```

| Store | Purpose |
|-------|---------|
| `LockStore` | Database-level deployment locks |
| `PlanStore` | Schema change plans (DDL, original schema for rollback) |
| `ApplyStore` | Schema change execution state, heartbeat-based leasing |
| `TaskStore` | Individual DDL tasks within an apply, progress tracking |
| `ApplyLogStore` | Audit trail (state transitions, errors, progress events) |
| `CheckStore` | GitHub status check state |
| `SettingsStore` | Admin-level key-value settings |

## Lock Coordination

Locks prevent concurrent schema changes to the same database. The lock key is `database:type` — deliberately **not** per-environment. This means a staging lock also blocks production, which is an intentional safety measure to prevent conflicting schema changes across environments.

```go
lockStore.Acquire(ctx, &Lock{DatabaseName: "mydb", DatabaseType: "mysql", Owner: "pr-123"})
// Returns ErrLockHeld if another owner holds the lock
// Idempotent if the same owner already holds it
```

`ForceRelease` is an admin override for the `schemabot unlock` command.

## State Machine

See [`pkg/state`](../state/) for the full state hierarchy (apply states, task states, engine states, derivation rules, and normalization).

## Heartbeat-Based Recovery

The apply store supports crash recovery through heartbeat-based leasing:

- **Heartbeat**: Workers call `Heartbeat(applyID)` every 10 seconds to signal they're alive
- **ClaimForRecovery**: Finds applies with stale heartbeats (>1 minute since last update) using `FOR UPDATE SKIP LOCKED` to prevent races between recovery workers
- If a worker crashes, its apply becomes claimable after the heartbeat times out

## Key Types

- **Lock**: Database name, type, owner (PR identifier), timestamps
- **Plan**: DDL statements, table changes, original schema (for rollback), schema files
- **Apply**: Links to plan and lock, state, environment, engine, options (defer_cutover, volume)
- **Task**: Single DDL within an apply — table name, DDL, state, progress (rows copied/total, ETA)
- **ApplyLog**: Level, event type, source (schemabot/spirit), message, state transitions

## MySQL Implementation

The `storage/mysqlstore` package implements all interfaces using a `*sql.DB` connection. Each store is a thin wrapper with SQL queries. Plans store their DDL and schema data as JSON in a `plan_data` column. The schema tables themselves are defined in [`pkg/schema`](../schema/) and applied on startup via `api.EnsureSchema()`.
