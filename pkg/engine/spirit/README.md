# spirit

Package `spirit` implements the `engine.Engine` interface for MySQL databases using [Spirit](https://github.com/block/spirit), a gh-ost-style online DDL tool.

Spirit copies table data row-by-row to a new shadow table with the desired schema, replays binlog changes to stay in sync, then atomically swaps the tables. This allows schema changes on large tables without blocking reads or writes.

## Key Types

**Engine** — The main type. Holds a `ddl.Differ` for schema diffing, a `lint.Linter` for safety checks, and a `runningMigration` for tracking the active schema change. Only one schema change can run at a time per Engine instance.

**runningMigration** — Tracks a running schema change: the Spirit runners, affected tables, DDL statements, state, and a cancel function for stopping. Created by `Apply()`, consumed by `Progress()` and control operations.

**Config** — Engine configuration:
- `TargetChunkTime` (default 500ms): How long each batch of row copies should take
- `Threads` (default 4): Number of concurrent copier threads
- `LockWaitTimeout` (default 30s): How long to wait for table locks during cutover
- `DebugLogs`: Enable verbose Spirit debug output

## How It Works

### Plan

1. Connects to the target database and fetches the current schema (`SHOW CREATE TABLE`)
2. Diffs current vs. desired schema using `ddl.Differ` (powered by Spirit's SQL parser)
3. Runs `lint.Linter` to detect unsafe changes (DROP TABLE, DROP COLUMN, etc.)
4. Classifies each DDL statement via AST analysis (CREATE, ALTER, DROP)
5. Returns DDL statements, table changes, and lint warnings

### Apply

1. Categorizes DDL statements by type: CREATE, ALTER, DROP
2. Executes CREATE statements first (direct SQL), then ALTER (via Spirit), then DROP (direct SQL)
3. For ALTER statements, creates a `spiritmigration.Migration` with:
   - `DeferCutOver` / `RespectSentinel` — When deferred cutover is requested
   - Non-strict mode — Spirit falls back to a fresh copy when checkpoint resume fails (idempotent behavior)
4. Runs the migration in a background goroutine with a cancellable context
5. If only CREATE/DROP statements exist (no ALTER), completes immediately without Spirit

### Progress

Polls Spirit's Progress API and maps Spirit's internal states to engine states:
- `status.WaitingOnSentinelTable` → `StateWaitingForCutover` (when cutover is deferred)
- `status.Close` → `StateCompleted`
- Returns per-table metrics: rows copied, rows total, ETA, progress percentage

### Stop and Resume

**Stop** forces a checkpoint dump before cancelling the context. Spirit only auto-checkpoints every ~50 seconds, so forcing a checkpoint preserves maximum progress. The checkpoint stores the copier watermark, binlog position, and DDL statement.

**Start** (resume) calls `Apply()` again with the same DDL. Spirit automatically detects its `_tablename_chkpnt` checkpoint table and resumes from where it left off.

### Cutover

When `defer_cutover` is set, Spirit pauses after row copy completes and waits for a sentinel table (`_spirit_sentinel`) to be dropped. Calling `Cutover()` drops this sentinel table, signaling Spirit to proceed with the atomic table swap.

### Volume

Adjusts schema change speed by mapping volume levels (1-11) to Spirit settings:

| Volume | Threads | Chunk Time | Lock Wait |
|--------|---------|------------|-----------|
| 1      | 1       | 500ms      | 5s        |
| 3      | 2       | 2s         | 30s       |
| 5      | 4       | 4s         | 30s       |
| 11     | 16      | 5s         | 30s       |

Implementation: Stop → reconfigure → Start. Spirit resumes from its checkpoint with the new settings.

## Checkpoint Error Handling

Two checkpoint-related errors require user intervention:

- **ErrMismatchedAlter**: A checkpoint table exists with a different DDL statement. This means a previous schema change was interrupted and left behind a checkpoint. The user must clean up the checkpoint tables before retrying.

- **ErrBinlogNotFound**: A checkpoint references a binlog file that no longer exists on the server (binlog retention expired). All copy progress is lost and the schema change must be restarted from scratch.

## File Layout

| File | Purpose |
|------|---------|
| `spirit.go` | Engine type, `Plan()`, `Apply()`, `Progress()` |
| `execution.go` | `executeMigration()`, Spirit runner setup, error handling |
| `control.go` | `Stop()`, `Start()`, `Cutover()`, `Volume()`, `Revert()`, `SkipRevert()` |
| `helpers.go` | Schema fetching, DSN parsing, statement classification, internal table detection |
| `logger.go` | Custom log handler that filters Spirit debug logs and routes to apply log storage |
