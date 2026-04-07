# tern

Package `tern` provides the schema change orchestration layer. It sits between the HTTP API and the engine, managing the full lifecycle of a schema change: planning, execution, progress tracking, control operations, and crash recovery.

## Two Implementations

**LocalClient** — Embeds the engine (Spirit) directly in the SchemaBot process. Used when the config specifies `databases:` with DSNs. Best for development and single-server deployments.

**GRPCClient** — Delegates to a remote Tern service over gRPC. Used when the config specifies `tern_deployments:` with host:port addresses. For distributed deployments where schema changes run on dedicated hosts closer to the database.

Both implement the same `Client` interface, so the API layer doesn't need to know which is in use.

## Client Interface

```go
type Client interface {
    Plan(ctx, *PlanRequest) (*PlanResponse, error)
    Apply(ctx, *ApplyRequest) (*ApplyResponse, error)
    Progress(ctx, *ProgressRequest) (*ProgressResponse, error)
    Cutover(ctx, *CutoverRequest) (*CutoverResponse, error)
    Stop(ctx, *StopRequest) (*StopResponse, error)
    Start(ctx, *StartRequest) (*StartResponse, error)
    Volume(ctx, *VolumeRequest) (*VolumeResponse, error)
    Revert(ctx, *RevertRequest) (*RevertResponse, error)
    SkipRevert(ctx, *SkipRevertRequest) (*SkipRevertResponse, error)
    RollbackPlan(ctx, database string) (*PlanResponse, error)
    Health(ctx) error
    ResumeApply(ctx, *storage.Apply) error
    Close() error
}
```

Request/response types are protobuf-generated (`pkg/proto/ternv1`).

## Apply Flow

### 1. Plan

The CLI or API calls `Plan()` with the desired schema files. The Tern client passes them to the engine, which diffs against the live database and returns DDL statements with lint warnings. The plan is stored in SchemaBot's storage for later use by `Apply()`.

### 2. Apply

`Apply()` creates an Apply record (one Apply contains N Tasks, one per table) and launches background execution in one of two modes:

- **Sequential mode** (default): Each table's DDL is executed one at a time. Each task runs through Spirit independently and cuts over before the next starts.
- **Atomic mode** (`--defer-cutover`): All DDLs are passed to Spirit in a single call. All tables copy in parallel and cut over together when the user triggers cutover.

### 3. Poll

Background goroutines poll the engine for progress every 500ms and update task state in storage (rows copied, ETA, progress %). A heartbeat updates the apply's `updated_at` every 10 seconds to signal the worker is alive.

### 4. Cutover

In atomic mode with `--defer-cutover`, the schema change pauses at `WaitingForCutover` once all row copying is complete. The user calls `Cutover()` to trigger the final table swap. In sequential mode, each table cuts over automatically.

## Recovery

If the SchemaBot server crashes during an apply, the recovery mechanism resumes it:

1. The API service's recovery worker polls every 10 seconds for applies with stale heartbeats (no update for 1+ minute)
2. It claims the apply using `FOR UPDATE SKIP LOCKED` to prevent races between workers
3. It calls `ResumeApply()` on the appropriate Tern client
4. **LocalClient**: Calls `engine.Apply()` with the same DDL — Spirit auto-detects its checkpoint table and resumes from where it left off
5. **GRPCClient**: Calls `Start()` on the remote Tern service, then spawns a local progress poller

Stopped applies (user called `stop`) are not auto-resumed — the user must explicitly call `start`.

## Conflict Detection

Before starting an apply, `LocalClient` checks for active tasks that might conflict. It verifies with the engine (not just storage) to handle stale state — if storage says a task is active but Spirit has no running schema change, the stale task is marked as failed.

## File Layout

| File | Purpose |
|------|---------|
| `client.go` | `Client` interface definition |
| `local_client.go` | `LocalClient` — embedded engine, Plan/Apply/Progress |
| `local_apply.go` | Apply execution: sequential/atomic modes, polling, heartbeats |
| `local_control.go` | Control operations: Cutover, Stop, Start, Volume, Revert, RollbackPlan, ResumeApply |
| `grpc_client.go` | `GRPCClient` — delegates to remote Tern over gRPC |
| `server.go` | gRPC server wrapper that exposes a `Client` as a Tern gRPC service |
| `state_converters.go` | Helpers for converting between engine, storage, and proto state representations |
