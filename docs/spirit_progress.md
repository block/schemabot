# Spirit Progress Architecture

How progress data flows from Spirit through SchemaBot to the CLI/TUI
(Terminal User Interface — the interactive Bubbletea-based progress view).

## Data flow overview

```
Spirit Runner       → Engine wrapper    → LocalClient.Progress → API                   → CLI/TUI
(in-memory)           (spirit.go)         (local_client.go)      (progress_handlers.go)   (watch_tui.go)
runner.Progress()     Progress()          Progress()             handleProgress()          fetchProgress()
```

Each layer adds, transforms, or merges data. Understanding which values are live
(read from Spirit's in-memory state) vs persisted (read from MySQL storage) is
key to debugging stale-progress issues.

## What Spirit exposes

`runner.Progress()` returns [`status.Progress`](https://github.com/block/spirit/blob/main/pkg/status/progress.go):

| Field           | Type              | Notes |
|-----------------|-------------------|-------|
| `CurrentState`  | `status.State`    | Atomic int32 enum: `Initial`, `CopyRows`, `WaitingOnSentinelTable`, `Checksum`, `CutOver`, `Close`, ... |
| `Summary`       | `string`          | `"71436/221193 32.30% copyRows ETA 5m 30s"` |
| `Tables[]`      | `[]TableProgress` | Per-table: `TableName`, `RowsCopied` (uint64), `RowsTotal` (uint64), `IsComplete` (bool) |

Key details:
- **ETA is embedded in `Summary`**, not a separate field. Downstream layers parse it out with a regex.
- **`IsComplete`** comes from the chunker's in-memory `finalChunkSent` flag, NOT from the checkpoint table. This means `IsComplete` is lost on crash — it only exists while the runner is alive.
- **`RowsCopied` can exceed `RowsTotal`** when rows are inserted during the copy. All downstream layers clamp this for display.

## Engine layer

`pkg/engine/spirit/spirit.go` `Progress()`:

1. Reads `runner.Progress()` from the single runner (all tables share one runner in atomic mode).
2. Maps `status.State` → `engine.State` enum via `spiritStateToString()`:
   - `CopyRows` → `"copying_rows"`, `WaitingOnSentinelTable` → `"waiting_for_cutover"`, `Close` → `"completed"`, etc.
3. Determines overall state: preserves terminal states (`stopped`, `failed`) from `runningMigration.state` — Spirit's state doesn't override these.
4. Builds per-table `engine.TableProgress`:
   - Calculates `Progress` percent (clamped 0–100).
   - Clamps `RowsCopied` to `RowsTotal` for display.
   - Sets `ProgressDetail` = formatted summary like `"12345/50000 24% copyRows"`.
   - If `IsComplete` is true, overrides `State` to `"complete"` and `Progress` to 100.

Key types: `engine.ProgressResult`, `engine.TableProgress` (`pkg/engine/engine.go`).

When no runner exists (engine stopped, no active schema change), returns `StatePending` with
message `"No active schema change"`.

## Tern layer — live vs stored

`pkg/tern/local_client.go` `Progress()`:

1. Loads ALL tasks for this database from storage.
2. Picks the most relevant task (priority: running > pending > terminal).
3. Calls `eng.Progress()` to get live engine data.
4. Merges live + stored:

**Rule: engine data wins when available; storage is the fallback.**

```
For each task in the current apply:
    if engine has progress for this table:
        use engine's State, RowsCopied, RowsTotal, Progress, ETA, ProgressDetail
    else:
        use stored RowsCopied, RowsTotal, ProgressPercent from the task row
```

The fallback matters for:
- **Stopped tasks**: progress was saved at stop time (see "Stop snapshot" below).
- **Completed tasks**: the engine may have been shut down.
- **Crash recovery**: the engine is gone but storage has the last polled snapshot.

Overall state is derived from ALL tasks in the apply via `deriveOverallState()` in `local_apply.go`:
running > pending > stopped > failed > completed.

## What gets persisted in storage

Task fields updated during polling (`storage.Task` in `pkg/storage/types.go`):

| Field             | Type   | Source |
|-------------------|--------|--------|
| `RowsCopied`      | int64  | From `engine.TableProgress.RowsCopied` |
| `RowsTotal`       | int64  | From `engine.TableProgress.RowsTotal` |
| `ProgressPercent`  | int    | From `engine.TableProgress.Progress` (0–100) |
| `ETASeconds`      | int    | From `engine.TableProgress.ETASeconds` |
| `IsInstant`       | bool   | From `engine.TableProgress.IsInstant` |
| `State`           | string | Mapped from `engine.State` |
| `StartedAt`       | time   | Set when task transitions to RUNNING |
| `CompletedAt`     | time   | Set when engine reports a terminal state |
| `UpdatedAt`       | time   | Bumped on every poll |

**When task rows are updated:**

| Trigger | What writes | Frequency | Fields updated |
|---------|-------------|-----------|----------------|
| `pollForCompletionAtomic` (atomic mode) | Poller goroutine | Every 500ms | `State`, `RowsCopied`, `RowsTotal`, `ProgressPercent`, `ETASeconds`, `UpdatedAt`, `CompletedAt` (on terminal) |
| `pollTaskToCompletion` (sequential mode) | Poller goroutine | Every 500ms | Same as above, plus `IsInstant` |
| `LocalClient.Stop()` | Stop handler | Once, after `eng.Stop()` blocks | `State` → STOPPED (or COMPLETED if table finished), `RowsCopied`, `RowsTotal`, `ProgressPercent`, `ETASeconds`, `CompletedAt` |
| `LocalClient.Progress()` | Progress handler | On each API call (if engine state changed and task is non-terminal) | `State`, `UpdatedAt`, `CompletedAt` |
| `executeApplyAtomic` / `executeApplySequential` | Apply launcher | Once at start | `State` → RUNNING, `StartedAt` |

The pollers are the primary write path during normal operation. The Stop handler is special:
it calls `eng.Stop()` first (which blocks until Spirit's goroutine exits), THEN reads
`eng.Progress()` and persists the final snapshot. This ordering guarantees the stopped progress
reflects the true final state — tables that finished their row copy before Spirit exited will
have `IsComplete=true` and are marked COMPLETED rather than STOPPED.

## Polling modes (atomic vs sequential)

### Atomic mode (`--defer-cutover`)

One Spirit runner handles all DDLs together. `pollForCompletionAtomic` polls every 500ms:
- Updates ALL tasks with the shared engine state and per-table row counts.
- Maintains a heartbeat (bumps `apply.updated_at` every 10s) so the recovery loop knows it's alive.
- Auto-triggers cutover if `defer_cutover` is NOT set (shouldn't happen in this mode, but defensive).

### Sequential mode (default)

One Spirit runner per table, processed in order. `pollTaskToCompletion` polls every 500ms:
- Updates the single active task with engine progress.
- Re-fetches task state from storage each tick to detect external Stop signals.
- When the task reaches a terminal state, returns. The outer loop (`executeApplySequential`) then starts the next table.

## API and CLI layers

### API (`pkg/api/progress_handlers.go`)

`progressResponseFromProto()` does a direct proto → JSON mapping. `handleProgress()` and
`handleProgressByApplyID()` add apply-level fields: `apply_id`, `database`, `environment`, `volume`.

Volume is read from the apply's stored options, not from the engine.

### CLI/TUI (`pkg/cmd/commands/watch_tui.go`)

The TUI polls the API every **2 seconds** via `tick()`.

`parseProgressResult()` converts the JSON response to internal types. For each table,
if `ProgressDetail` is non-empty, it runs `ParseSpiritProgress()` — a regex parser
in `pkg/cmd/templates/progress.go` that extracts structured data from Spirit's summary string:

```
"71436/221193 32.30% copyRows ETA 5m 30s"
 ↓       ↓      ↓       ↓          ↓
RowsCopied RowsTotal Percent State    ETA
```

The parsed values override the structured API fields (`RowsCopied`, `RowsTotal`, `Percent`)
because `ProgressDetail` comes directly from Spirit and is more current than the separately-polled
numeric fields.

## TUI rendering reference

The TUI and CLI use emoji progress bars to convey state at a glance. Each color maps to a
specific state. The bar is 20 squares wide; filled squares represent percent complete.

Defined in `pkg/cmd/templates/progress.go`.

### Progress bar colors

| Color | Emoji | Meaning | Used when |
|-------|-------|---------|-----------|
| Blue  | `🟦`  | In progress | Actively copying rows |
| Yellow | `🟨` | Waiting | Waiting for cutover / cutting over |
| Green | `🟩`  | Complete | Table finished successfully |
| Orange | `🟧` | Stopped | Stopped mid-progress (partially complete) |
| Red   | `🟥`  | Failed | Table failed |
| White | `⬜`  | Empty | Remaining (unfilled portion of any bar) |

### Per-table display by state

**Copying rows** — blue bar with percent, row counts, and ETA:
```
  orders: 🟦🟦🟦🟦🟦🟦⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜ 32% (71,436/221,193 rows) ETA 5m 30s
          ALTER TABLE `orders` ADD COLUMN `discount` int NOT NULL DEFAULT 0
```

**Queued** (pending, not yet started) — empty bar:
```
  products: ⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜ queued
            ALTER TABLE `products` ADD COLUMN `weight` decimal(10,2)
```

**Starting** (running but no row data yet):
```
  users: ⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜ starting...
         ALTER TABLE `users` ADD COLUMN `phone` varchar(20)
```

**Waiting for cutover** — yellow bar at 100%:
```
  orders: 🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨 ⏸️ Waiting for cutover
          ALTER TABLE `orders` ADD COLUMN `discount` int NOT NULL DEFAULT 0
```

**Cutting over** — yellow bar at 100% with spinner:
```
  orders: 🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨🟨 🔄 Cutting over...
          ALTER TABLE `orders` ADD COLUMN `discount` int NOT NULL DEFAULT 0
```

**Complete** — green bar at 100%:
```
  orders: 🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩 ✓ Complete
          ALTER TABLE `orders` ADD COLUMN `discount` int NOT NULL DEFAULT 0
```

**Stopped** — orange bar at the progress when stop occurred:
```
  orders: 🟧🟧🟧🟧🟧🟧⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜ ⏹️ Stopped at 32%
          ALTER TABLE `orders` ADD COLUMN `discount` int NOT NULL DEFAULT 0
```

**Failed** — red bar at the progress when failure occurred:
```
  orders: 🟥🟥🟥🟥🟥⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜ ❌ Failed
          ALTER TABLE `orders` ADD COLUMN `discount` int NOT NULL DEFAULT 0
```

**Cancelled** (sequential mode — earlier table failed, this one never ran):
```
  products: ⊘ Cancelled (not started)
            ALTER TABLE `products` ADD COLUMN `weight` decimal(10,2)
```

### Status line (above tables)

The TUI shows a single status line above the table list. It varies by overall state:

| State | Status line |
|-------|-------------|
| Starting | `⠋ Loading...` |
| Pending | `⠋ Starting...` |
| Running | `⠋ 🔄 Copying rows... ETA 5m 30s` |
| Stopping | `⠋ Stopping...` |
| Waiting for cutover | *(cutover prompt shown in footer instead)* |
| Cutting over | `⠋ Cutting over...` |
| Completed | *(no status line — completion message shown after tables)* |
| Stopped | *(no status line — stopped message shown after tables)* |

The `⠋` is a Braille spinner (animated in the TUI, static here).

### Footer

| State | Footer |
|-------|--------|
| Running | `ESC detach · s stop · v volume` |
| Running (volume mode) | `Volume: ████████░░░ 8/11` / `↑↓ adjust · 1-9 direct · ESC done` |
| Waiting for cutover (with `--cutover`) | `Press Enter to proceed with cutover (or ESC to detach)` |
| Waiting for cutover (no `--cutover`) | `To proceed: schemabot cutover --apply-id <id>` |
| Cutting over | `Cutover in progress - please wait...` |
| Stopped | `Use 'schemabot start --apply-id <id>' to resume.` |

## Key behaviors

1. **Live-first, storage-fallback.** The Tern layer always prefers live engine data. Storage is only
   used when the engine has no data for a table (stopped, completed, or different apply).

2. **Stop snapshot timing.** `LocalClient.Stop()` calls `eng.Stop()` which blocks until Spirit's
   goroutine exits. Only THEN does it read `eng.Progress()` and persist the snapshot. This means
   tables that finished their row copy before Spirit exited will have `IsComplete=true` and are
   marked as COMPLETED rather than STOPPED.

3. **`IsComplete` is in-memory only.** Spirit's `TableProgress.IsComplete` is set from the
   chunker's `finalChunkSent` flag — it's never written to the checkpoint table. On crash, this
   flag is lost. Recovery uses re-plan (diff against current DB state) rather than relying on
   `IsComplete`.

4. **Crash recovery loses up to 500ms of progress.** The pollers write to storage every 500ms.
   On crash, the last persisted snapshot may be up to 500ms stale. The recovery loop
   (`Service.RecoverInProgress`) finds applies with stale heartbeats and calls
   `Tern.ResumeApply()`, which re-plans against the actual DB state to determine what still needs
   to be done.

5. **ETA is only available via `ProgressDetail` parsing.** Spirit embeds ETA in its summary string.
   The engine layer doesn't extract it into a separate field — it flows through as `ProgressDetail`
   and is parsed by the CLI with a regex. If the regex fails, no ETA is shown.

6. **`RowsCopied` clamping.** Spirit can report `RowsCopied > RowsTotal` due to concurrent inserts.
   The engine layer clamps rows for display, and the CLI templates also clamp independently.
