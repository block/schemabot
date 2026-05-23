# SchemaBot Metrics

SchemaBot exposes metrics via OpenTelemetry. All metrics are available at `GET /metrics` (Prometheus format) and optionally pushed via OTLP when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.

## Custom Metrics

| Metric | Type | Attributes | Description |
|---|---|---|---|
| `schemabot.plans.total` | Counter | repository, database, environment, status | Total plan operations |
| `schemabot.plan.duration_seconds` | Histogram | repository, database, environment, status | Plan execution time |
| `schemabot.applies.total` | Counter | repository, database, environment, status | Total apply operations |
| `schemabot.apply.duration_seconds` | Histogram | repository, database, environment, status | Apply API call time |
| `schemabot.schema_request.errors_total` | Counter | repository, command, database, environment, reason | Schema request errors by reason |
| `schemabot.active_applies` | UpDownCounter | database, environment | In-progress applies |
| `schemabot.check_ownership_misses_total` | Counter | operation, repository, database, database_type, environment | Guarded check updates skipped because ownership changed |
| `schemabot.status_check_operations_total` | Counter | operation, status, repository, database, database_type, environment | Status-check storage and GitHub operations |
| `schemabot.webhook.events_total` | Counter | event_type, action, repository, status | GitHub webhook events |
| `schemabot.control_operations_total` | Counter | operation, database, environment, status | Control operations (cutover, stop, start, etc.) |
| `schemabot.lock_operations_total` | Counter | operation, database, status | Lock acquire/release operations |
| `schemabot.scheduler.resumed_total` | Counter | database, environment, previous_state | Applies resumed by the scheduler |
| `schemabot.scheduler.resume_failures_total` | Counter | database, environment, reason | Scheduler resume attempts that failed |
| `schemabot.scheduler.claim_failures_total` | Counter | reason | Scheduler claim attempts that failed |
| `schemabot.scheduler.claim_duration_seconds` | Histogram | database, environment, previous_state | Scheduler claim and resume duration |

### Attribute Values

**status** (plans): `success`, `error`

**status** (applies): `success`, `error`, `rejected`, `conflict`

**operation** (check ownership): `apply_finished`, `rollback_finished`

**operation** (status checks): `plan_check_recorded`, `apply_started`, `apply_finished`, `rollback_finished`, `aggregate_check_sync`, `stale_check_cleanup`, `stale_check_reconciliation`, `schema_config_discovery`, `schema_config_environment_validation`

**status** (status checks): `success`, `error`, `skipped`, `stale`, `noop`, `blocked` (operation outcome, not GitHub Check Run conclusion)

**operation** (control): `cutover`, `stop`, `start`, `volume`, `revert`, `skip_revert`, `rollback_plan`

**status** (control): `success`, `error`, `rejected`

**operation** (locks): `acquire`, `release`

**status** (locks): `success`, `conflict`, `not_found`, `not_owned`, `error`

**event_type** (webhooks): `issue_comment`, `pull_request`, `check_run`, `ping`

**action** (webhooks): `created`, `opened`, `synchronize`, `reopened`, `closed`, `requested`, `completed` (omitted for events without actions like `ping`)

**status** (webhooks): `processed`, `invalid_signature`, `ignored`

**reason** (scheduler claim failures): `storage_error`, `expire_retryable_error`, `unknown`

**reason** (scheduler resume failures): `missing_deployment`, `no_client`, `resume_error`, `retry_budget_exhausted`

### Check Ownership Misses

`schemabot.check_ownership_misses_total` should normally be near zero. A spike
means an apply or rollback worker reached a terminal path after the stored check
state had already moved to a different owner, usually because a new commit,
newer apply, rollback, pod restart, or recovery path raced with the older worker.
The guarded update prevented the stale worker from overwriting current merge-gate
state, so the metric is a near-miss signal rather than proof that check state was
corrupted.

Operation values:

| Operation | Meaning |
|---|---|
| `apply_finished` | A worker tried to record a terminal apply result, but the stored check state no longer belonged to that apply. |
| `rollback_finished` | A rollback worker tried to mark the check `action_required`, but the stored check state no longer belonged to that rollback apply. |

A spike is still dangerous because the live database can keep changing after the
PR's desired schema has moved on. For example, an apply can start for commit A,
an agent can push commit B that removes the schema change, and commit A's apply
can still reach the database. The guard prevents the old apply worker from
marking the current check successful, but it does not undo live-schema drift.

Operator response:

1. Group by `repository`, `environment`, `database_type`, `database`, and
   `operation` to identify whether the spike is isolated or global.
2. For an isolated PR/database, inspect the PR timeline for new commits while an
   apply was running, then compare the latest commit on the PR branch, stored
   check state, and active apply state before allowing merge.
3. For a global spike, check recent deploys, pod restarts, recovery activity, and
   webhook redeliveries. A broad spike can indicate duplicate workers or a
   service-level race, not just user commit churn.
4. If the live schema may now differ from the PR's current declarative schema,
   re-plan the current head and decide whether to apply again, roll back, or
   hold the PR until drift is resolved.

### Status Check Operations

`schemabot.status_check_operations_total` tracks lower-level status-check work
that does not always involve ownership conflicts. Use it to see whether
SchemaBot is successfully storing check state, publishing aggregate Check Runs,
or intentionally blocking a passing aggregate because older stored state still
requires operator attention.

Operation values:

| Operation | Meaning |
|---|---|
| `plan_check_recorded` | SchemaBot stored per-database check state for a plan result. This is the internal state later rolled into the aggregate GitHub Check Run. |
| `apply_started` | SchemaBot marked stored check state as owned by an accepted apply and set it to `in_progress`. This is a check lifecycle event, not proof that the engine has started copying rows. |
| `apply_finished` | SchemaBot updated stored check state after an apply reached a terminal state, such as success or failure. |
| `rollback_finished` | SchemaBot marked stored check state `action_required` after a rollback succeeded because the PR's desired schema is no longer present in that environment. |
| `aggregate_check_sync` | SchemaBot tried to make the visible aggregate GitHub Check Run match stored per-database check state. The status label says whether it created/updated, skipped, blocked, or failed. |
| `stale_check_cleanup` | SchemaBot handled stored check state for a database that is no longer touched by the latest commit on the PR branch. Plan-only state can be cleared; apply-owned state stays blocked. |
| `stale_check_reconciliation` | SchemaBot repaired stale `in_progress` stored check state by comparing it with authoritative apply state after a worker restart, crash, or race. |
| `schema_config_discovery` | SchemaBot discovered managed schema configs for the PR before deciding what to plan or which aggregate checks to publish. |
| `schema_config_environment_validation` | SchemaBot found schema changes but none of the environments declared by `schemabot.yaml` are allowed for this deployment, so it failed the aggregate check closed. |

A spike in `status="blocked"` for `operation="aggregate_check_sync"` or
`operation="stale_check_cleanup"` means SchemaBot is fail-closing instead of
allowing the latest commit on the PR branch to pass while earlier check state
still matters. For example, commit A can add a schema change and start an apply,
then commit B can remove that schema change before the apply finishes. SchemaBot
blocks the aggregate Check Run for commit B until an operator decides whether
the target environment needs another apply, a rollback, or manual reconciliation.
A spike in `status="error"` usually points to storage or GitHub API failures and
should be investigated before relying on branch-protection state.

### Control Operations

`schemabot.control_operations_total` tracks operator commands that act on an
existing apply or generate a rollback plan.

Operation values:

| Operation | Meaning |
|---|---|
| `cutover` | Trigger cutover for an apply that is waiting for cutover confirmation. |
| `stop` | Request that the engine stop an apply. |
| `start` | Request that the engine start or resume an apply. |
| `volume` | Change the engine's throttling or volume setting for an apply. |
| `revert` | Request that the engine revert an apply. |
| `skip_revert` | Skip the post-deploy revert window for an apply. |
| `rollback_plan` | Generate a rollback plan from a previous completed apply. |

### Lock Operations

`schemabot.lock_operations_total` tracks database-level lock acquisition and
release attempts.

Operation values:

| Operation | Meaning |
|---|---|
| `acquire` | Try to acquire the database lock for a plan/apply workflow. |
| `release` | Try to release the database lock, either by owner or administrative override. |

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
