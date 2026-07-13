# Storage Outage Behavior

How a SchemaBot server behaves when its own storage database becomes
unreachable — for example during a credential rotation window, a failover, or a
network partition. The design goal: **a storage outage degrades the control
plane; it never destroys in-flight schema changes, and it never converts
uncertainty into a false verdict.**

## The two databases

Every apply involves two databases with independent credentials and failure
domains:

- **The target database** — where the schema change actually runs. Credentials
  come from the apply request or the data-plane deployment.
- **The storage database** — SchemaBot's own bookkeeping: applies, operations,
  tasks, leases, control requests, check state.

A storage outage does not affect the target database. The engine work (row
copying, binlog application, checksumming, checkpointing) continues unaffected,
whether it runs in-process (local drives) or in a remote data plane (gRPC
drives). Spirit's checkpoint table lives on the *target* database, so
resume-safety keeps advancing through a storage outage.

## Probes: liveness vs readiness

The Kubernetes probes encode the first-order policy:

| Probe | Endpoint | Checks | On failure |
|---|---|---|---|
| Startup | `/livez` | process serves HTTP | boot budget before liveness arms |
| Liveness | `/livez` | process serves HTTP | restart the pod |
| Readiness | `/health` | storage ping | stop routing traffic |

Liveness is process-only because a restart only fixes process faults (a wedged
runtime, a deadlocked listener). An unreachable storage database is not fixable
by restart — killing the pod would abort in-flight local drives and add
crash-loop backoff on top of the outage. Readiness carries the dependency
check: an instance that cannot reach storage is pulled from the Service and
rejoins the moment storage recovers.

Boot is patient for the same reason: a pod that starts (or restarts) during
the outage retries storage boot inside the startup-probe budget, re-resolving
the DSN on every attempt so a rotated credential is picked up as soon as the
mounted secret refreshes, instead of crash-looping until the outage ends.

During a shared-cause outage (every replica loses storage at once), all pods go
unready together and the Service has no endpoints. Inbound webhooks and API
calls fail at the connection level for the duration. That is honest — those
requests need storage — and visible: GitHub marks each delivery as failed so an
operator can redeliver it after recovery (GitHub App webhooks are never
redelivered automatically), and the reconciling pollers converge poll-driven
state on their next pass.

## What happens to an in-flight apply

Three components with three different lifetimes:

```
storage outage begins
      │
      ▼
drive loop exits within seconds        ← fail-closed on safety reads
      │
      │    the schema change keeps running the entire time
      │    (engine goroutine / remote data plane — no storage
      │     dependency, its own throttling and limits intact)
      │
~1 min: heartbeat stale → row claimable  (no peer can claim; they
      │                                   share the same outage)
      ▼
storage recovers → operator poll re-claims the stale row
      → reattaches to the still-running work
      → mirroring, heartbeats, supervision resume
```

The drive loop is a supervisor: it relays control requests (stop, cancel,
cutover) from storage to the engine, mirrors progress into storage, and
heartbeats the lease. It is a bookkeeper, not a safety governor — everything
that protects the target database (throttling, lock timeouts, chunk sizing,
checksumming) lives inside the engine and keeps operating while unsupervised.

## Failure taxonomy in the drive loops

Storage failures during a drive fall into three deliberate categories:

1. **Progress and state mirror writes: log and keep driving.** A failed task
   or display-metadata write is logged and skipped; the next poll tick writes
   current progress. Mirroring is idempotent — a missed write costs staleness,
   nothing else.

2. **Safety-gating reads: exit the drive attempt.** If the driver cannot read
   pending control requests, it must not keep driving blind to a possible stop
   or cancel. Both the local and remote drive loops exit "for operator retry"
   on this failure, leaving the operation row claimable. The engine work is
   unaffected by the supervisor exiting.

3. **Terminal-state writes: error out, never fabricate.** A failure to persist
   a terminal state leaves the row non-terminal and claimable; the state is
   re-derived from tasks on the next claim. Storage uncertainty is never
   converted into a completed apply or a passing check.

## The unsupervised window

Between the drive loop exiting and the post-recovery re-claim, the schema
change runs unsupervised. During this window:

- **No command can be missed.** Operators cannot stop/cancel/retune the apply —
  but they also cannot submit those commands, because control requests are
  written to the same storage that is down and the API is unready. Command
  intake and command visibility fail together. Worst case, an intervention is
  delayed by the outage duration.
- **Progress reporting goes dark.** The PR comment freezes and status queries
  fail until supervision reattaches.
- **Decision points wait.** An apply reaching a state that needs the control
  plane (for example a deferred cutover awaiting approval) parks in its safe
  waiting state.

This is the same property that lets remote data-plane applies survive
control-plane deploys and restarts; local drives get it from the probe split
above. The window is bounded and self-healing: outage duration, plus up to a
minute of heartbeat staleness, plus one operator poll.

## Split-brain protection on recovery

After recovery there is a short race where a peer driver could claim a
stale-heartbeat row before the original driver's next heartbeat lands. Three
independent fences make this safe:

1. **Lease tokens.** Every storage write from a drive is guarded by its lease
   token. A claim rotates the token, so a displaced driver's writes match zero
   rows and it receives a lease-lost error, on which it cancels its own run.
   The lease-lost signal is only raised after a successful read proves the
   token changed — a connection error can never be misread as a lost lease, so
   an outage cannot trigger a false self-fence.
2. **Engine-level locking.** A local Spirit run holds a metadata lock on the
   target database for the duration of the run; a usurping resume cannot touch
   the table until the original releases it. Remote drives reattach by stored
   remote apply id, which is idempotent.
3. **Heartbeat retry tolerance.** The operator-level heartbeat distinguishes
   "heartbeat failed" (storage unreachable — logged, retried next tick, drive
   continues) from "lease lost" (token mismatch — self-fence). Transient
   storage errors do not stop a healthy drive.
