# Apply completion notifications

Opt in on each server that should publish completions from its storage:

```yaml
notifications:
  type: sqs
  queue_url: https://sqs.us-east-1.amazonaws.com/123456789012/apply-completions
  region: us-east-1
  lookback: 24h
  interval: 30s
  observation_interval: 5m
  batch_size: 100
```

An omitted/empty `type` disables publication. Unknown types fail startup; SQS is
currently the only implementation of `Publisher.Publish(ctx, Event) error`.
Region can be omitted to use the AWS SDK default configuration. Credentials use
the existing AWS SDK default chain; the server needs `sqs:SendMessage` on the
configured queue. This feature creates neither a queue nor IAM permissions.

The **24h default is provisional**, following the existing summary-reconciliation
lookback, not an established receiver requirement. Configure a positive duration
for the delivery window and polling interval. Batches default to 100 and are
limited to 1–1000. Each pass sends at most one batch, sequentially. An initial
deploy publishes existing completed applies within that window, not just future
completions; size the interval and batch for that catch-up load.

```text
completed applies (any completion writer; GitHub optional)
    |
    | recent window + bounded batch, on startup and each tick
    v
claim separate apply_notifications row ---- lost claim ---> skip
    |
    v
SQS send ---- failure ---> release + last_error ---> later tick retries
    |
    v
mark published (only while the same claim is still current)
```

The job belongs to the server lifecycle, not the webhook handler. Build validates
configuration and constructs the publisher; Start begins delivery and a separate
backlog observer immediately. Close cancels/drains publication before closing
storage/telemetry, with a bounded wait. This preserves the GitHub-independent
availability boundary (AV-1), bounded shutdown (AV-14), and apply/driver ownership
(OW-8). Only the new tracking table is written. Both storage bootstrappers create
it additively (AV-9); apply writes and summary reconciliation remain unchanged.

## Delivery and payload

Only `applies.state = completed` with a recorded completion time qualifies. The
scan uses `completed_at` and its existing index, not `updated_at` or `apply_logs`.
It covers aggregate projection, already-completed-engine control settlement,
recovery with no tasks, recovery after re-plan finds no work, taskless grouped
completion, and sequential completion. Non-GitHub applies qualify too.

The JSON body contains only:

```json
{
  "apply_identifier": "apply-example",
  "database": "orders",
  "database_type": "mysql",
  "environment": "staging",
  "repository": "example/schema",
  "pull_request": 42,
  "completed_at": "2026-01-01T12:00:00Z"
}
```

Repository and PR are omitted when absent. Completion time is UTC. Receivers
must deduplicate by stable `apply_identifier`, not the SQS-assigned message ID.
For a FIFO queue, the publisher also uses that identifier for SQS deduplication
and the fixed `apply-completions` message group. No consumer is included.

A conditional SQL claim excludes competing sends during normal operation. Claims
last one minute, use the database clock, and carry a random token independent of
driver leases. A send has a 15s deadline; individual SQL operations have 5s
deadlines. Release and mark require that same unexpired token, so an old sender
cannot erase a replacement claim. Send failures release immediately and record
the latest error; no attempt history, custom backoff or retry counters exist.
An unreleased claim becomes eligible after expiry, including after restart.

A crash after SQS accepts a message but before marking publication can duplicate
it. Slow or ambiguous network replies can also produce duplicates. This is not
an exactly-once protocol. Failures/restarts catch up **only while the completion
is inside the configured lookback**; an outage longer than the window can drop
notifications. No delivery outside the window is attempted, and occasional drops
are acceptable. Failed, cancelled and stopped applies do not produce events.

## Backlog metrics

Two observable gauges are exposed through the existing `/metrics` and optional
OTLP pipeline:

- `schemabot.apply_notifications.oldest_unpublished_age_seconds`: age of the
  oldest completed, unpublished apply observed over the last finished sweep,
  including completions outside the delivery window and active publication claims.
- `schemabot.apply_notifications.out_of_window`: count of observed unpublished
  completed applies older than the sweep's fixed `now - lookback` cutoff.

These are **observed backlog gauges, not a durable lost-event counter**. Do not
sum replicas sharing storage; use max. Existing historical completions with no
tracking rows are included, even when they predate this feature. That history
can remain out of window indefinitely; the feature does not infer an activation
baseline or call an empty tracking table a healthy zero.

The observer reads one primary-key page of 500 applies per observation interval (default 5m),
including non-completed and published rows before filtering in memory. Each sweep
pins its ID ceiling and database-clock cutoff once. This bounds SQL work per
page without adding a storage index or running an all-history filtered COUNT.
Concurrent completions/publications are eventually reflected on a subsequent
sweep; a sweep is not a transactionally consistent snapshot. Delivery runs on a
separate loop and never waits for this historical scan.

Both gauges update together only after a complete sweep. Refresh latency grows
with history (approximately `ceil(apply rows / 500) * observation_interval`); a restart loses
the in-memory cursor and starts again. On query failure the cursor and last-good
gauges remain, and a warning records the failed pass. Gauges are absent until the
first finished sweep and cleared when the runner stops; consult logs for stalled
sweeps rather than interpreting repeated samples as fresh storage reads.

To graph in an existing Prometheus-compatible dashboard, use:

```promql
max(schemabot_apply_notifications_oldest_unpublished_age_seconds)
max(schemabot_apply_notifications_out_of_window)
```

Filter by your deployment/storage identity before combining pods. A growing lag
suggests checking publisher errors, queue permissions, and catch-up throughput.
Out-of-window backlog cannot be recovered by this job. No dashboard or alerting
service is provisioned here; verify collection mapping and panels in the target
deployment before treating dashboard visibility as validated.
