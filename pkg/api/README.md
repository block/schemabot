# api

Package `api` provides the SchemaBot HTTP API service. It routes requests to Tern clients, manages a lazy client pool, and runs a background recovery worker for crash detection.

## Service

The `Service` type is the core of the API layer. It holds the storage connection, server config, and a pool of Tern clients keyed by `database/environment`.

```go
type Service struct {
    storage     storage.Storage
    config      *ServerConfig
    ternClients map[string]tern.Client  // lazy, created on first request
    // ...
}
```

### Tern Client Pool

Clients are created lazily on first request via `TernClient(database, environment)`:

1. **Local mode**: If the database is in the `databases:` config section, creates a `LocalClient` with an embedded Spirit engine. DSN is resolved via `pkg/secrets`.
2. **gRPC mode**: If the database maps to a `tern_deployments:` entry, creates a `GRPCClient` pointing to remote services that implement the Tern proto.

Lazy creation means a connection failure to one database doesn't block startup or affect other databases.

## HTTP Routes

### Schema Change Operations

| Method | Path | Handler | Description |
|--------|------|---------|-------------|
| POST | `/api/plan` | `handlePlan` | Generate a schema change plan |
| POST | `/api/apply` | `handleApply` | Execute a plan |
| GET | `/api/progress/{database}` | `handleProgress` | Poll progress for latest schema change for a database |
| GET | `/api/progress/apply/{apply_id}` | `handleProgressByApplyID` | Progress by apply ID |

### Control Operations

| Method | Path | Handler | Description |
|--------|------|---------|-------------|
| POST | `/api/cutover` | `handleCutover` | Trigger cutover |
| POST | `/api/stop` | `handleStop` | Pause schema change |
| POST | `/api/start` | `handleStart` | Resume schema change |
| POST | `/api/volume` | `handleVolume` | Adjust speed (1-11) |
| POST | `/api/revert` | `handleRevert` | Revert completed change |
| POST | `/api/skip-revert` | `handleSkipRevert` | Finalize change that's in the revert window |
| POST | `/api/rollback/plan` | `handleRollbackPlan` | Plan a rollback |

### Status and History

| Method | Path | Handler | Description |
|--------|------|---------|-------------|
| GET | `/api/status` | `handleStatus` | All active schema changes |
| GET | `/api/history/{database}` | `handleDatabaseHistory` | Apply history for a database |
| GET | `/api/databases/{database}/environments` | `handleDatabaseEnvironments` | List environments |
| GET | `/api/logs/{database}` | `handleLogs` | Apply logs for a database |
| GET | `/api/logs` | `handleLogsWithoutDatabase` | Logs by apply ID |

### Locks and Settings

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/locks/acquire` | Acquire deployment lock |
| DELETE | `/api/locks` | Release lock |
| GET | `/api/locks` | List all locks |
| GET/POST | `/api/settings` | Read/write settings |

### Health

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Storage connectivity check |
| GET | `/tern-health/{deployment}/{environment}` | Tern client health |

## Recovery Worker

On startup, the service launches a background recovery worker (`StartRecoveryWorker`) that handles crash detection:

1. Polls every 10 seconds for applies with stale heartbeats (no update for 1+ minute)
2. Claims them atomically using `FOR UPDATE SKIP LOCKED`
3. Gets the appropriate Tern client for the database/environment
4. Calls `ResumeApply()` — the engine resumes from its checkpoint
5. Loops until no more applies need recovery

Stopped applies (user called `schemabot stop`) are **not** auto-resumed. The user must explicitly call `schemabot start`.

## Configuration

Config is loaded from `SCHEMABOT_CONFIG_FILE` (YAML). Key sections:

- `storage.dsn` — SchemaBot's internal database connection
- `databases` — Local mode: database name → type + per-environment DSNs
- `tern_deployments` — gRPC mode: deployment name → per-environment Tern addresses
- `repos` — Repository → deployment mapping

See the top-level [README](../../README.md) for configuration examples.

## File Layout

| File | Purpose |
|------|---------|
| `service.go` | `Service` type, Tern client pool, route registration, graceful shutdown |
| `config.go` | `ServerConfig` loading and validation |
| `scheduler.go` | Recovery worker (heartbeat-based crash detection) |
| `plan_handlers.go` | Plan and Apply HTTP handlers |
| `control_handlers.go` | Cutover, Stop, Start, Volume, Revert handlers |
| `progress_handlers.go` | Progress, Status, History handlers |
| `health_handlers.go` | Health checks and JSON helpers |
| `lock_handlers.go` | Lock acquire/release/list handlers |
| `log_handlers.go` | Apply log handlers |
| `settings_handlers.go` | Settings read/write handlers |
| `proto_helpers.go` | Proto ↔ HTTP type conversion helpers |
| `ensure_schema.go` | `EnsureSchema()` — applies embedded SQL schema to storage DB using Spirit |
