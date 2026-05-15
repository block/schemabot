# SchemaBot Metrics

SchemaBot exposes metrics via OpenTelemetry. All metrics are available at `GET /metrics` (Prometheus format) and optionally pushed via OTLP when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.

## Custom Metrics

| Metric | Type | Attributes | Description |
|---|---|---|---|
| `schemabot.plans.total` | Counter | database, environment, status | Total plan operations |
| `schemabot.plan.duration_seconds` | Histogram | database, environment, status | Plan execution time |
| `schemabot.applies.total` | Counter | database, environment, status | Total apply operations |
| `schemabot.apply.duration_seconds` | Histogram | database, environment, status | Apply API call time |
| `schemabot.active_applies` | UpDownCounter | database, environment | In-progress applies |
| `schemabot.check_ownership_misses_total` | Counter | operation, repository, database, database_type, environment | Guarded check updates skipped because ownership changed |
| `schemabot.webhook.events_total` | Counter | event_type, action, repository, status | GitHub webhook events |
| `schemabot.control_operations_total` | Counter | operation, database, environment, status | Control operations (cutover, stop, start, etc.) |
| `schemabot.lock_operations_total` | Counter | operation, database, status | Lock acquire/release operations |
| `schemabot.recovery.cycles_total` | Counter | — | Recovery worker polling cycles |
| `schemabot.recovery.recovered_total` | Counter | — | Applies recovered by the recovery worker |
| `schemabot.recovery.failed_total` | Counter | — | Recovery attempts that failed |

### Attribute Values

**status** (plans/applies): `success`, `error`, `rejected`

**operation** (check ownership): `complete_apply`, `rollback_action_required`

**operation** (control): `cutover`, `stop`, `start`, `volume`, `revert`, `skip_revert`, `rollback_plan`

**status** (control): `success`, `error`, `rejected`

**operation** (locks): `acquire`, `release`

**status** (locks): `success`, `conflict`, `not_found`, `not_owned`, `error`

**event_type** (webhooks): `issue_comment`, `pull_request`, `check_run`, `ping`

**action** (webhooks): `created`, `opened`, `synchronize`, `reopened`, `closed`, `requested`, `completed` (omitted for events without actions like `ping`)

**status** (webhooks): `processed`, `invalid_signature`, `ignored`

### Check Ownership Misses

`schemabot.check_ownership_misses_total` should normally be near zero. A spike
means an apply or rollback worker reached a terminal path after the stored check
state had already moved to a different owner, usually because a new commit,
newer apply, rollback, pod restart, or recovery path raced with the older worker.
The guarded update prevented the stale worker from overwriting current merge-gate
state, so the metric is a near-miss signal rather than proof that check state was
corrupted.

A spike is still dangerous because the live database can keep changing after the
PR's desired schema has moved on. For example, an apply can start for commit A,
an agent can push commit B that removes the schema change, and commit A's apply
can still reach the database. The guard prevents the old apply worker from
marking the current check successful, but it does not undo live-schema drift.

Operator response:

1. Group by `repository`, `environment`, `database_type`, `database`, and
   `operation` to identify whether the spike is isolated or global.
2. For an isolated PR/database, inspect the PR timeline for new commits while an
   apply was running, then compare the current PR head, stored check state, and
   active apply state before allowing merge.
3. For a global spike, check recent deploys, pod restarts, recovery activity, and
   webhook redeliveries. A broad spike can indicate duplicate workers or a
   service-level race, not just user commit churn.
4. If the live schema may now differ from the PR's current declarative schema,
   re-plan the current head and decide whether to apply again, roll back, or
   hold the PR until drift is resolved.

## HTTP Server Metrics

The `otelhttp` middleware automatically produces standard HTTP metrics for every endpoint:

| Metric | Type | Description |
|---|---|---|
| `http.server.request.duration` | Histogram | Request latency by method and status code |
| `http.server.request.body.size` | Histogram | Request body sizes |
| `http.server.response.body.size` | Histogram | Response body sizes |

## Adding New Metrics

Define recording functions in `metrics.go` following the existing pattern:

```go
func RecordXxx(ctx context.Context, attrs ...string) {
    meter := otel.Meter(meterName)
    counter, err := meter.Int64Counter("schemabot.xxx.total",
        otelmetric.WithDescription("Description"),
        otelmetric.WithUnit("{unit}"),
    )
    if err != nil {
        slog.Warn("failed to create counter", "error", err)
        return
    }
    counter.Add(ctx, 1, otelmetric.WithAttributes(...))
}
```

The OTel SDK deduplicates instruments with the same name, so calling `Int64Counter` on every invocation is safe and cheap after the first registration.
