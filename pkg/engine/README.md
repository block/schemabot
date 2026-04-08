# engine

Package `engine` defines the interface that all schema change backends must implement.

SchemaBot supports multiple engines for executing schema changes on different database platforms. Each engine implements the same `Engine` interface, allowing the rest of SchemaBot to be engine-agnostic.

## Available Engines

| Engine | Package | Status | Platform |
|--------|---------|--------|----------|
| Spirit | [`engine/spirit`](./spirit/) | Implemented | MySQL |
| PlanetScale | `engine/planetscale` | Stub | Vitess |
| PostgreSQL | `engine/postgres` | Stub | PostgreSQL |

## Engine Interface

The `Engine` interface follows a state machine pattern with four categories of operations:

```go
type Engine interface {
    Name() string

    // Planning
    Plan(ctx, *PlanRequest) (*PlanResult, error)

    // Execution
    Apply(ctx, *ApplyRequest) (*ApplyResult, error)
    Progress(ctx, *ProgressRequest) (*ProgressResult, error)

    // Control
    Stop(ctx, *ControlRequest) (*ControlResult, error)
    Start(ctx, *ControlRequest) (*ControlResult, error)
    Cutover(ctx, *ControlRequest) (*ControlResult, error)
    Revert(ctx, *ControlRequest) (*ControlResult, error)
    SkipRevert(ctx, *ControlRequest) (*ControlResult, error)
    Volume(ctx, *VolumeRequest) (*VolumeResult, error)
}
```

**Plan** computes the DDL needed to transform the current schema into the desired schema. It fetches the live schema from the database, diffs it against the target `.sql` files, and returns DDL statements with lint warnings.

**Apply** starts executing the DDL asynchronously. Call `Progress()` to poll for status.

**Progress** returns the current state, per-table row copy metrics, and ETA.

**Control operations** manage a running schema change: pause (`Stop`), resume (`Start`), trigger the final table swap (`Cutover`), adjust speed (`Volume`), or roll back (`Revert`/`SkipRevert`).

## State Machine

```
                    ┌──────────┐
                    │ Pending  │
                    └────┬─────┘
                         │ Apply()
                         ▼
               ┌─────────────────┐    Stop()    ┌─────────┐
               │    Running      │─────────────▶│ Stopped │
               └────────┬────────┘              └─────────┘
                        │
           ┌────────────┴────────────┐
           │ (defer_cutover=true)    │ (auto)
           ▼                         ▼
  ┌────────────────────┐    ┌──────────────┐
  │WaitingForCutover   │    │ CuttingOver  │
  └────────┬───────────┘    └──────┬───────┘
           │ Cutover()             │
           ▼                       ▼
  ┌──────────────┐         ┌─────────────┐
  │ CuttingOver  │         │  Completed  │
  └──────┬───────┘         └─────────────┘
         │
         ▼
  ┌─────────────┐
  │  Completed  │
  └─────────────┘

  Terminal states: Completed, Failed, Stopped, Reverted
```

## Resume Contract

Engines must support resume: if the server restarts mid-schema-change, the engine must be able to resume from where it left off. The `ResumeState` field on requests carries opaque state (e.g., Spirit's checkpoint table name) that enables this.

## Key Types

- **PlanRequest/PlanResult**: Schema files in, DDL + table changes + lint warnings out
- **ApplyRequest/ApplyResult**: DDL + options in, async acceptance out
- **ProgressResult**: State, per-table `TableProgress` (rows copied/total, ETA, progress %)
- **ControlResult**: Accepted flag + message
- **VolumeRequest**: Volume level 1 (minimal) to 11 (maximum throughput)
- **Credentials**: DSN for MySQL, or org/token for PlanetScale
