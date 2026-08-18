# Throttle reference

When a schema change's progress bar carries a `(throttled)` annotation, the
engine's throttler is deliberately pausing the copy or the checksum verify to
protect the database. Throttling is backpressure, not a hang: the work resumes
on its own as soon as the pressure signal clears, and no operator action is
required for the schema change to finish.

The annotation's tooltip shows the raw reason reported by the engine, a short
tip, and a link to this document. Reasons follow the grammar
`<signal> <observed> <op> <threshold>`. When several signals throttle at the
same time, the reasons are joined with `; `.

This page explains each signal: what it measures, why the engine pauses on it,
and what to look at when the throttle lasts longer than expected.

## replica-lag

```
replica-lag 12000ms >= 120000ms
```

The schema change is configured with a read replica to protect
(`--replica-dsn`), and that replica's replication lag has reached the
configured tolerance (`--replica-max-lag`, default 120s). The copy pauses so
the replica can catch up, then resumes.

**When to act.** A brief throttle during a bursty write period is normal. A
sustained throttle means the replica cannot keep up with the source's write
rate even without the copy running. Check the replica's health and its own
load; the schema change is a symptom here, not the cause.

### replica-lag unobservable (failing closed)

```
replica-lag unobservable for 45s (failing closed)
```

Lag polling against the replica has been failing for longer than the staleness
threshold. The engine cannot tell whether the replica is keeping up, so it
fails closed: the copy pauses until polling recovers rather than trusting a
stale reading.

**When to act.** Check connectivity and credentials from the engine to the
replica. If the replica is intentionally gone, remove `--replica-dsn` to
proceed without lag protection.

## redo-aware

```
redo-aware 4 > 3
```

Read this as: active threads greater than the instance's threshold. The
engine selects this algorithm when it detects an Aurora source. It counts
threads actively executing queries (from `performance_schema`) and subtracts
threads parked on Aurora's redo-log flush wait, which consume no CPU. When
the active count exceeds the instance's budget (vCPUs plus headroom), the
copy yields so application queries keep their CPU. In the example, 4 active
threads exceed a budget of 3 on a 2-vCPU instance.

**When to act.** Usually nothing: the throttle is doing its job, trading copy
speed for application latency. If the copy must finish sooner, either reduce
application load on the instance or move to a larger instance class. Raising
the copy's own concurrency does not help while this signal is active.

## threads-running

```
threads-running 130 > 128
```

The same thread-load protection as [redo-aware](#redo-aware), measured more
coarsely: the global `Threads_running` counter compared against the instance's
budget. The engine uses this fallback when it lacks the `performance_schema`
access the redo-aware signal needs. Unlike redo-aware, threads parked on
redo-log waits count as load, so this signal is more conservative.

**When to act.** Same as redo-aware. Granting the engine's user
`performance_schema` read access upgrades the signal to redo-aware.

## commit-latency

```
commit-latency 112.4ms >= 100ms
```

The average commit latency on the database has crossed the configured
threshold (`--max-commit-latency`, default 100ms, auto-enabled on Aurora).
Slow commits mean the storage layer is saturating, so the copy backs off
before write latency degrades for the application.

**When to act.** A sustained throttle points at storage pressure: check the
instance's write IOPS and commit latency metrics. On Aurora this signal often
pairs with redo-aware; together they indicate the instance is undersized for
the combined application and copy load.

## Throttled with no reason

A throttler that predates reason reporting, or one that implements no reason
extension, reports the throttled flag with an empty reason. The progress
surfaces show the bare `(throttled)` annotation with no tooltip. The
backpressure semantics are the same; only the explanation is missing.
