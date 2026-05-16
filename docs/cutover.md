# Cutover

Cutover is the final step of an online schema change — the atomic swap from the old
table to the new table.

## Why defer cutover?

By default, cutover happens automatically as soon as the schema change is ready.
The `--defer-cutover` flag pauses before this step, letting the operator choose
when to commit.

This matters because cutover requires a brief metadata lock (MDL) on the table.
While the lock is held, new queries against the table are blocked. For most tables
this is milliseconds. But for very busy tables with long-running queries, acquiring
the MDL can take longer — the lock request waits behind all in-flight transactions.
If long-running queries don't finish in time, the engine may force-kill them to
proceed (see [Force-killing blocking queries](#force-killing-blocking-queries)).

The main reason to defer is to **schedule cutover during off-peak hours**,
minimizing the impact of metadata lock acquisition on application traffic.

## How it works

When `--defer-cutover` is set, the apply pauses after row copy (and checksum for
Spirit) is complete. The schema change is fully prepared but not yet live. The operator
triggers cutover explicitly:

```
schemabot cutover <apply-id> -e <environment>
```

This can be done via the CLI or as a PR comment.

Without `--defer-cutover`, cutover happens automatically — no pause, no manual step.

## State transitions

With `--defer-cutover`:
```
running → waiting_for_cutover → cutting_over → completed
```

Without `--defer-cutover`:
```
running → cutting_over → completed
```

| State | Meaning |
|-------|---------|
| `running` | Row copy and binlog streaming in progress |
| `waiting_for_cutover` | Preparation done, paused, waiting for operator to trigger cutover |
| `cutting_over` | Engine is executing the table swap |
| `completed` | Schema change is live |

**Note on `waiting_for_cutover` without defer:** Spirit always enters
`WaitingOnSentinelTable` status internally (controlled by `RespectSentinel`,
which defaults to `true`). But without `--defer-cutover`, no sentinel table
is created, so Spirit advances through this state in milliseconds. SchemaBot's
5-second progress poll typically doesn't observe it — the task transitions
directly from `running` to `completed` from SchemaBot's perspective.

## Entry points

Both the CLI and PR comments resolve to the same tern client `Cutover()` call:

```
CLI:        schemabot cutover <apply-id> -e staging
            → POST /api/cutover → client.Cutover()

PR comment: schemabot cutover <apply-id> -e staging
            → webhook → handleCutoverCommand() → client.Cutover()
```

The tern client delegates to the engine, which handles cutover differently per engine.

## Spirit (MySQL)

Spirit uses a **sentinel table** as the coordination mechanism for deferred cutover.

### Sentinel table

The sentinel table is a dummy table that acts as a gate. Its existence means
"don't cut over yet." Dropping it means "proceed."

| Phase | What happens |
|-------|-------------|
| **Creation** | Spirit creates `_spirit_sentinel` (`id int NOT NULL PRIMARY KEY`) in the target database before row copy starts |
| **Waiting** | After row copy + checksum, Spirit enters `WaitingOnSentinelTable` and polls `INFORMATION_SCHEMA.TABLES` every 1 second (max 48 hours) |
| **Trigger** | SchemaBot executes `DROP TABLE IF EXISTS db._spirit_sentinel` |
| **Detection** | Spirit's next poll (within 1 second) sees the table is gone and proceeds to cutover |

Spirit has no cutover RPC. The `DROP TABLE` is the signal. SchemaBot wraps this
in the engine interface.

### Cutover algorithm

Spirit uses "rename under lock" (`pkg/migration/cutover.go`):

1. **LOCK TABLES** — acquire metadata lock on both the original and shadow (`_new`) tables. New queries against these tables are blocked until the lock is released.
2. **Flush binlog** — replay remaining binlog events under the lock, ensuring the shadow table is fully caught up.
3. **RENAME TABLE** — single atomic statement:
   ```sql
   RENAME TABLE `original` TO `_original_old`, `_original_new` TO `original`
   ```
4. **UNLOCK TABLES** — locks released, queries resume against the new table.

The rename is atomic — both swaps succeed or both fail. Write downtime is
typically milliseconds.

### Force-killing blocking queries

Acquiring the metadata lock at step 1 can be blocked by long-running queries or
transactions that hold locks on the table. Spirit handles this with **targeted
force-killing**, which is **enabled by default**.

How it works:

1. Spirit attempts to acquire the metadata lock with a configurable timeout
   (`--lock-wait-timeout`, default 30 seconds)
2. At **90% of the timeout** (27 seconds by default), Spirit starts killing
   blocking transactions
3. Spirit reads `performance_schema` to identify only the specific connections
   holding metadata locks on the migrating table
4. Those connections are killed with `KILL <pid>`

Safety guardrails:

| Check | Behavior |
|-------|----------|
| **Explicit `LOCK TABLES`** | Spirit refuses to kill these and fails immediately — they indicate an operator error and are not safely retryable |
| **Heavy transactions** (> 1M rows modified) | Skipped to avoid rollback storms that could cause more damage than the lock wait |
| **Spirit's own connection** | Excluded from kill candidates |

The `--skip-force-kill` flag disables this behavior entirely. However, Spirit's
docs warn that repeatedly failing to acquire metadata locks without force-kill
is itself dangerous — the retried lock attempts can queue up and bring down
production. Targeted killing is actually safer for busy systems.

Force-kill also applies during the **checksum phase**, which requires a similar
metadata lock to create consistent read views.

Required MySQL privileges for force-kill: `CONNECTION_ADMIN` (or `SUPER`),
`PROCESS` on `*.*`, and `SELECT` on `performance_schema.*`.

### State persistence

Spirit's migration state (`WaitingOnSentinelTable`, `CopyRows`, etc.) is
**in-memory only** — it lives on the `Runner` struct as an atomic int32.
The checkpoint table persists copier watermark and binlog position for crash
recovery, but not the state machine position.

After a container restart, a new Runner reports `Initial` regardless of where
the previous run was. SchemaBot handles this by checking whether the
`_spirit_sentinel` table still exists in the target database. If it does,
the task enters a `recovering` state that blocks cutover until Spirit
re-reaches `WaitingOnSentinelTable`. See `reconcileTaskStateFromEngine`
in `pkg/tern/local_client.go`.

## PlanetScale (Vitess)

_TODO: Document PlanetScale/Vitess cutover flow (deploy request lifecycle, VReplication mechanism, force cutover, etc)._

## PR comment UX

When a task enters `waiting_for_cutover`, the `ReadyToComplete` flag is set
(via `transitionTaskState`). The PR comment renders:

```
1/1 table(s) ready for cutover

⏸️ Ready for cutover

To proceed with the cutover:
  schemabot cutover <apply-id> -e <environment>
```

### Error comments

All error cases are rendered via `pkg/webhook/templates/cutover.go`:

| Error | When |
|-------|------|
| Missing Apply ID | `schemabot cutover -e staging` (no apply ID) |
| Missing Environment | `schemabot cutover apply_123` (no `-e` flag) |
| Apply Not Found | Apply ID doesn't exist in storage |
| Cutover Not Available | Apply is not in `waiting_for_cutover` state |
| Cutover Failed | Tern client connection or RPC failure |
| Cutover Not Accepted | Engine rejected the cutover |
