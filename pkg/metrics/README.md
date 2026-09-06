# SchemaBot Metrics

SchemaBot exposes metrics via OpenTelemetry. All metrics are available at `GET /metrics` (Prometheus format) and optionally pushed via OTLP when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.

## Custom Metrics

Every SchemaBot-owned metric emits a non-empty `environment` attribute. Metrics
that are not scoped to one schema change environment use `environment="unknown"`.
Some attributes listed below are optional and only appear when that context is
available, such as `repository`, `github_app`, and `installation_id`.

| Metric | Type | Attributes | Description |
|---|---|---|---|
| `schemabot.plans.total` | Counter | repository, database, environment, status | Total plan operations |
| `schemabot.plan.duration_seconds` | Histogram | repository, database, environment, status | Plan execution time |
| `schemabot.applies.total` | Counter | repository, database, environment, status | Total apply operations |
| `schemabot.apply.duration_seconds` | Histogram | repository, database, environment, status | Apply API call time |
| `schemabot.deployment_diff.total` | Counter | database, deployment, environment, status | Review-time per-deployment plan diffs by outcome (the fan-out that feeds the drift rollup) |
| `schemabot.schema_freshness.rejected.total` | Counter | action, environment | Plan/apply/apply-confirm rejected because PR HEAD advanced after discovery loaded schema files |
| `schemabot.command.rejected_stale_plan.total` | Counter | action, environment | Apply-confirm rejected because PR HEAD advanced after the confirmation plan was posted |
| `schemabot.source_policy.blocks_total` | Counter | operation, database, environment, reason | Trusted-source plan/apply requests blocked by source policy |
| `schemabot.pr_command_actor_authorization.total` | Counter | command, database, environment, repository, status, reason | GitHub PR command actor authorization decisions |
| `schemabot.schema_request.errors_total` | Counter | repository, command, database, environment, reason | Schema request errors by reason |
| `schemabot.active_applies` | UpDownCounter | database, environment | In-progress applies |
| `schemabot.check_ownership_misses_total` | Counter | operation, repository, database, database_type, environment | Guarded check updates skipped because ownership changed |
| `schemabot.promotion.config_error_blocks_total` | Counter | repository, database, environment | Applies blocked because the target environment is absent from the configured promotion order |
| `schemabot.status_check_operations_total` | Counter | operation, status, repository, database, database_type, environment | Status-check storage and GitHub operations |
| `schemabot.webhook.events_total` | Counter | environment, app_name, event_type, action, repository, status | GitHub webhook events (`action` is omitted for events that carry none; `repository` is omitted for the `repo_not_configured` status — see [Webhook Ownership Rejections](#webhook-ownership-rejections)) |
| `schemabot.webhook.unregistered_repository_ignored_total` | Counter | environment, app_name, event_type, action, repository | Webhook events ignored because the repository is not configured |
| `schemabot.webhook.inbox_depth` | Gauge | environment, state | Durable webhook inbox rows by state |
| `schemabot.webhook.inbox_oldest_claimable_age_seconds` | Gauge | environment | Age of the oldest ready-to-claim durable webhook inbox row |
| `schemabot.webhook.inbox_stuck_processing` | Gauge | environment | Durable webhook inbox rows stuck in processing past the attempt cap |
| `schemabot.webhook.inbox_stats_collection_failures` | Counter | environment | Failed durable webhook inbox metric snapshots (liveness signal for the inbox gauges) |
| `schemabot.webhook.check_suite_recovery_total` | Counter | environment, repository, outcome | Durable check_suite recovery outcomes (`covered`, `synthesized`, `resynthesized`, `already_queued` per candidate PR; `no_pr_at_ingress`, `no_open_pr`, `truncated` per delivery) |
| `schemabot.webhook.inbox_dispatch_lag_seconds` | Histogram | environment, event_type, repository | Time from webhook receipt to the delivery's first dispatch claim |
| `schemabot.webhook.dispatch_duration_seconds` | Histogram | environment, event_type, outcome | Duration of one durable webhook dispatch claim by outcome (deliberately no repository label — see the ledger note below) |
| `schemabot.github.requests_total` | Counter | environment, operation, category, resource, status, repository, github_app, installation_id | GitHub API request attempts observed by SchemaBot, including attempts that never produced a response |
| `schemabot.github.pr_file_cap_exceeded_total` | Counter | environment, repository, schema_visible | Auto-plan runs failed closed because the PR changes more files than GitHub will report for a single PR. Deterministic per PR, not an outage. Every over-cap PR gets the same cap-specific blocking check; `schema_visible` records whether the reported files include a schema or config path (`true`: the PR plausibly carries a schema change that needs a smaller PR; `false`: the visible files look like a pure refactor) |
| `schemabot.github.rate_limit.limit` | Gauge | environment, operation, resource, repository, github_app, installation_id | GitHub primary rate limit for the observed API resource |
| `schemabot.github.rate_limit.remaining` | Gauge | environment, operation, resource, repository, github_app, installation_id | GitHub primary rate limit requests remaining for the observed API resource |
| `schemabot.github.rate_limit.used` | Gauge | environment, operation, resource, repository, github_app, installation_id | GitHub primary rate limit requests used for the observed API resource |
| `schemabot.control_operations_total` | Counter | operation, database, environment, status | Control operations (cutover, stop, start, etc.) |
| `schemabot.remote_control_requests.rejected_total` | Counter | operation, engine, database, deployment, environment | Control requests a remote data plane accepted and its own driver then failed, mirrored back so the operator learns the command never took effect — see [Control Operations](#control-operations) |
| `schemabot.remote_control_requests.stale_resends_total` | Counter | operation, database, deployment, environment | Retransmissions of a stop/cancel request the data plane accepted but has not consumed past the stale threshold — see [Control Operations](#control-operations) |
| `schemabot.remote_apply_dedup_total` | Counter | database, environment, outcome | Idempotency-keyed dispatches replayed against their existing operation on the keyed apply — see [Remote Apply Attaches](#remote-apply-attaches) |
| `schemabot.remote_apply_attach_total` | Counter | database, environment, outcome | Sibling dispatches resolved into an existing deployment-keyed apply — see [Remote Apply Attaches](#remote-apply-attaches) |
| `schemabot.remote_apply_key_echo_mismatch_total` | Counter | database, environment | Remote dispatches refused fail-closed because the data plane's accepted response echoed a different operation key than the dispatch derives — see [Remote Apply Attaches](#remote-apply-attaches) |
| `schemabot.remote_apply_deployment_id_conflict_total` | Counter | database, environment, deployment | Remote dispatch results refused fail-closed because the deployment already correlates to a different remote apply id — see [Remote Apply Attaches](#remote-apply-attaches) |
| `schemabot.lock_operations_total` | Counter | operation, database, environment, status | Lock acquire/release operations |
| `schemabot.direct_write_authorization.total` | Counter | operation, database, environment, status, reason | Per-database direct-write (CLI/API) authorization decisions at the handler layer |
| `schemabot.rate_limit_decisions.total` | Counter | endpoint, scope, environment, decision | API request-budget decisions on rate-limited endpoints. `scope` is which budget was consulted (`caller` or `target`) and `decision` is `allow` or `limit`, so the limited rate has a denominator and a client approaching its budget is visible before it is turned away. The caller identity and target database are deliberately absent (hundreds of each); a limited request logs both — see [Rate Limits](#rate-limits) |
| `schemabot.operator.resumed_total` | Counter | database, environment, previous_state | Applies resumed by the operator |
| `schemabot.operator.resume_failures_total` | Counter | database, environment, reason | Operator resume attempts that failed |
| `schemabot.operator.claim_failures_total` | Counter | environment, reason | Operator claim attempts that failed |
| `schemabot.operator.claim_duration_seconds` | Histogram | database, environment, previous_state | Operator claim and resume duration |
| `schemabot.operator.stuck_pending_applies` | Gauge | environment | Pending applies past the stuck threshold that a driver should have claimed (sampled; capped at 500, so a value of 500 means "at least 500") |
| `schemabot.operator.stuck_pending_scan_failures` | Counter | environment | Failed stuck-pending apply scans (liveness signal for the gauge above) |
| `schemabot.operator.stranded_operations_reaped_total` | Counter | database, deployment, environment, parent_state | Pending apply operations the reaper settled from an already-settled parent apply. `deployment` is the reaped operation's own. A one-time burst is the historical backlog draining; a climbing rate means a producer is terminalizing parents without settling their children |
| `schemabot.engine.unrecognized_task_status_total` | Counter | database, database_type, engine, environment | Engine- or data-plane-reported task statuses with no task-state mapping. The fail-open default renders the affected work as Running, so any sustained rate means an engine or data-plane version introduced a status SchemaBot cannot classify — add an explicit mapping in `pkg/state`. The status itself is not an attribute: it is engine-controlled text with no bound on distinct values, and this counter fires only during a mapping gap. The paired drive warn carries the raw status with the task identifiers |
| `schemabot.storage_schema.destructive_refusals_total` | Counter | table, operation, scope, environment | Destructive storage-schema DDL statements the startup bootstrap (`EnsureSchema`) refused to execute. `scope` says whether the safe clauses of the statement still ran. A nonzero rate means a starting binary's embedded schema no longer declares a table or column that exists in the storage database — expected briefly from older pods during a rolling deploy or rollback. `environment` is always `unknown`: the bootstrap precedes any schema change environment |
| `schemabot.drop_table.already_absent_total` | Counter | database, environment | DROP TABLE targets that were already absent when the apply reached them |
| `schemabot.pending_drops.tables_moved_total` | Counter | database, environment | Dropped tables quarantined into the pending drops database |
| `schemabot.pending_drops.cleanup_dropped_total` | Counter | database, environment | Expired quarantined tables permanently dropped by the cleaner |
| `schemabot.pending_drops.cleanup_skipped_total` | Counter | database, environment | Quarantined tables skipped by the cleaner due to unparseable names |
| `schemabot.pending_drops.cleanup_lock_skipped_total` | Counter | database, environment | Cleanup target passes skipped because another instance held the per-target advisory lock |
| `schemabot.pending_drops.cleanup_errors_total` | Counter | database, environment, reason | Pending drops cleanup failures (retried on the next pass) |

> **Deprecated aliases:** the `schemabot.scheduler.*` series (`resumed_total`, `resume_failures_total`, `claim_failures_total`, `claim_duration_seconds`) is still emitted alongside the `schemabot.operator.*` series for one release so dashboards and alerts can migrate. The scheduler-named series will be removed afterward.

> **Dispatch started-vs-outcomes ledger:** every durable dispatch claim records the `durable_dispatch_started` status on `schemabot.webhook.events_total` and exactly one outcome on `schemabot.webhook.dispatch_duration_seconds`, so a sustained gap between the two means drivers are dying mid-claim without recording anything. The two instruments only share the `event_type` dimension (`events_total` additionally carries `app_name`, `action`, and `repository`; the duration histogram deliberately carries none of those — a histogram costs an order of magnitude more series per attribute combination than a counter), so the subtraction is only valid after aggregating both sides down to `event_type`. In Datadog:
>
> ```
> sum:schemabot.webhook.events_total{status:durable_dispatch_started} by {event_type}.as_count()
>   - sum:schemabot.webhook.dispatch_duration_seconds.count{*} by {event_type}.as_count()
> ```
>
> The ledger cannot be sliced by app, action, or repository — the started side has those dimensions but the outcome side does not. To attribute a gap to a repo or PR, pivot to the driver logs: every dispatch outcome path logs `delivery_id`, `repo`, and `pr`.

> **`stuck_pending_applies` is per-pod, not fleet-wide:** every pod running the server runs the stuck-pending monitor (there is no leader election, matching the inbox-depth and health monitors), so each pod reports the same DB-wide count as its own series. Aggregate with `max()`/`avg()`, not `sum()`, and expect the paired WARN log to fire from every pod each scan while anything is stuck.

### Attribute Values

**status** (plans): `success`, `error`

**status** (applies): `success`, `error`, `rejected`, `conflict`

**status** (deployment diff): `ok`, `errored`

**action** (schema freshness): `plan`, `apply`, `apply_confirm`, `unknown`

**action** (stale plan): `apply_confirm`

**operation** (source policy): `plan`, `apply`, `unknown`

**reason** (source policy): `missing_server_config`, `missing_database_config`, `missing_repository`, `missing_pull_request`, `missing_schema_path`, `unauthorized_repo`, `unauthorized_schema_dir`, `unknown`

**command** (PR command actor authorization): `apply`, `apply_confirm`, `rollback`, `rollback_confirm`, `unlock`, `cutover`, `stop`, `start`, `revert`, `skip_revert`, `unknown`

**status** (PR command actor authorization): `allowed`, `denied`, `error`, `skipped`, `unknown`

**reason** (PR command actor authorization): `disabled`, `allowed_admin_team`, `allowed_admin_user`, `allowed_operator_team`, `allowed_operator_user`, `missing_actor`, `missing_server_config`, `missing_database_config`, `no_configured_principal`, `not_authorized`, `github_error`, `unknown`

**operation** (check ownership): `apply_finished`, `apply_cancelled_finished`, `rollback_finished`

**operation** (status checks): `plan_check_recorded`, `apply_started`, `apply_finished`, `apply_cancelled_finished`, `rollback_finished`, `aggregate_check_sync`, `stale_check_cleanup`, `stale_check_reconciliation`, `schema_config_discovery`, `schema_config_source_policy`, `schema_config_environment_validation`

**status** (status checks): `success`, `error`, `skipped`, `stale`, `noop`, `blocked` (operation outcome, not GitHub Check Run conclusion)

**operation** (control): `cutover`, `stop`, `start`, `revert`, `skip_revert`, `release`, `rollback_plan`

**status** (control): `success`, `error`, `rejected`

**engine** (control): the engine that rejected the command, as recorded on the apply (for example `spirit`, `planetscale`)

**operation** (locks): `acquire`, `release`

**status** (locks): `success`, `conflict`, `not_found`, `not_owned`, `error`

**event_type** (webhooks): `create`, `issues`, `issue_comment`, `pull_request`, `pull_request_review`, `pull_request_review_comment`, `check_run`, `check_suite`, `ping`, `push`

**action** (webhooks): common GitHub actions for the subscribed webhook events, such as `created`, `opened`, `synchronize`, `submitted`, `edited`, `closed`, `requested`, `completed` (omitted for events without actions like `ping` and `push`)

**status** (webhooks): `processed`, `invalid_signature`, `ignored`, `repo_not_configured`, `app_repo_mismatch`, `installation_resolution_failed`, `durable_enqueue_failed`, `durable_command_not_ready`, `durable_command_routing_blocked`, `durable_command_unrouted`, `durable_dispatch_started`, `durable_dispatch_retrying`, `durable_dispatch_failed`, `durable_dispatch_failed_permanent`, `durable_dispatch_completed`, `durable_dispatch_superseded`

**state** (webhook inbox): `pending`, `processing`, `failed_retryable`, `completed`, `failed`, `failed_permanent`, `superseded`, `unknown`

**outcome** (webhook dispatch): `completed`, `failed`, `failed_permanent`, `retrying`, `superseded`, `released`, `lease_lost`, `finish_error`, `unknown`

**operation** (GitHub API): `add_comment_reaction`, `create_check_run`, `create_issue_comment`, `create_installation_access_token`, `edit_issue_comment`, `fetch_app_slug`, `fetch_blob`, `fetch_file_content`, `fetch_git_tree`, `fetch_pull_request`, `get_combined_status`, `get_team_membership`, `graphql_minimize_comment`, `graphql_status_check_rollup`, `list_check_runs_for_ref`, `list_pr_files`, `list_reviews`, `list_team_members`, `request_reviewers`, `unknown`, `update_check_run`

**category** (GitHub API): `auth`, `read`, `write`, `unknown`

**resource** (GitHub API): `core`, `graphql`, `search`, `code_search`, `integration_manifest`, `source_import`, `code_scanning_upload`, `actions_runner_registration`, `scim`, `dependency_snapshots`, `dependency_sbom`, `audit_log`

**status** (GitHub API): `success`, `error`, `not_found` (a 404 answer to a read operation — an expected "no" from probes like schemabot.yaml lookups; 404s on auth and write operations stay `error`), `transport_error` (no HTTP response: dial/TLS failure or deadline — the signature of GitHub being unreachable), `cancelled` (SchemaBot cancelled the request itself, e.g. during shutdown), `unknown`

**reason** (operator claim failures): `expire_retryable_error`, `missing_lease_token`, `operation_storage_error`, `missing_operation_lease_token`, `operation_set_list_error`, `operation_set_missing`, `operation_task_inspect_error`, `operation_cutover_storage_error`, `missing_operation_cutover_lease_token`, `operation_cutover_set_list_error`, `operation_cutover_set_invalid`, `operation_parent_load_error`, `operation_parent_missing`, `operation_parent_claim_error`, `operation_parent_not_claimable`, `operation_parent_release_error`, `operation_lease_release_error`, `operation_lease_recheck_error`, `operation_lease_recheck_missing`, `operation_lease_rotated`, `operation_lease_released_by_peer`, `missing_operation_deployment`, `stop_reconciliation_claim_error`, `stop_reconciliation_missing_lease_token`, `operation_projection_claim_error`, `operation_projection_missing_lease_token`, `stranded_reaper_error`, `stranded_task_reaper_error`, `unknown`

**reason** (operator resume failures): `missing_deployment`, `no_client`, `resume_error`, `lease_lost`, `retry_budget_exhausted`, `recovery_window_expired`

**operation** (storage schema refusals): `alter`, `drop` — the statement types Spirit's unsafe vocabulary can flag

**scope** (storage schema refusals): `split` (a mixed ALTER executed its safe clauses and refused only the destructive remainder), `whole` (nothing in the statement ran — either the whole statement was destructive or its clauses could not be partitioned)

### Webhook Ownership Rejections

A webhook whose signing App does not own the target repository is rejected with
401 and counted on `schemabot.webhook.events_total` under one of two statuses.
They separate routine traffic from an actionable trust-boundary signal, so alert
on the second and never on the first:

| Status | Meaning | Operator action |
|---|---|---|
| `repo_not_configured` | The repository has no entry in `repos:` at all. A shared App forwards deliveries for every repository it is installed on, so this is expected background traffic. | None. Onboard the repository if the deliveries were meant to be handled. |
| `app_repo_mismatch` | The repository *is* declared, but the App that signed the delivery is not the App configured to own it. This is config drift or a hostile install. | Investigate: compare the delivery's `app_name` against the repository's `github_app:` mapping. |

`repo_not_configured` deliberately carries **no `repository` attribute** —
unmanaged repository names are unbounded and would blow up the metric's
cardinality. Do not build a query that facets `repo_not_configured` by
repository; the rejection's debug log line carries the repository name for
one-off triage. `app_repo_mismatch` does carry `repository`, because a declared
repository is a bounded set.

Both statuses are rejections: the request fails closed with 401 either way, and
the severity split exists only so a warn-level triage scan stays actionable.

### Check Ownership Misses

`schemabot.check_ownership_misses_total` should normally be near zero. A spike
means an apply or rollback driver reached a terminal path after the stored check
state had already moved to a different owner, usually because a new commit,
newer apply, rollback, pod restart, or recovery path raced with the older driver.
The guarded update prevented the stale driver from overwriting current merge-gate
state, so the metric is a near-miss signal rather than proof that check state was
corrupted.

Operation values:

| Operation | Meaning |
|---|---|
| `apply_finished` | A driver tried to record a terminal apply result, but the stored check state no longer belonged to that apply. |
| `apply_cancelled_finished` | A driver tried to record a cancelled forward apply result, but a newer apply superseded the cancellation. |
| `rollback_finished` | A rollback driver tried to mark the check `action_required`, but the stored check state no longer belonged to that rollback apply. |

A spike is still dangerous because the live database can keep changing after the
PR's desired schema has moved on. For example, an apply can start for commit A,
an agent can push commit B that removes the schema change, and commit A's apply
can still reach the database. The guard prevents the old apply driver from
marking the current check successful, but it does not undo live-schema drift.

Operator response:

1. Group by `repository`, `environment`, `database_type`, `database`, and
   `operation` to identify whether the spike is isolated or global.
2. For an isolated PR/database, inspect the PR timeline for new commits while an
   apply was running, then compare the latest commit on the PR branch, stored
   check state, and active apply state before allowing merge.
3. For a global spike, check recent deploys, pod restarts, recovery activity, and
   webhook redeliveries. A broad spike can indicate duplicate drivers or a
   service-level race, not just user commit churn.
4. If the live schema may now differ from the PR's current declarative schema,
   re-plan the current head and decide whether to apply again, roll back, or
   hold the PR until drift is resolved.

### Promotion Config Error Blocks

`schemabot.promotion.config_error_blocks_total` should always be zero. It
increments only when a scoped SchemaBot instance (with `allowed_environments`
configured) is asked to apply to an environment that is absent from the
configured promotion order. The staging-first gate cannot determine which
environments must be applied before the target, so it fails closed and blocks
the apply.

This is an operator misconfiguration, not user commit churn. Group by
`repository`, `database`, and `environment` to find the affected instance, then
add the blocked `environment` to that instance's `environment_order` so the gate
knows where it sits in the promotion sequence. The matching warn log carries the
`promotion_order` currently in effect.

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
| `apply_cancelled_finished` | SchemaBot finalized a cancelled forward apply, either releasing ownership when no task completed or retaining a terminal failure when part of the schema change reached the target. |
| `rollback_finished` | SchemaBot marked stored check state `action_required` after a rollback succeeded because the PR's desired schema is no longer present in that environment. |
| `aggregate_check_sync` | SchemaBot tried to make the visible aggregate GitHub Check Run match stored per-database check state. The status label says whether it created/updated, skipped, blocked, or failed. |
| `stale_check_cleanup` | SchemaBot handled stored check state for a database that is no longer touched by the latest commit on the PR branch. Plan-only state can be cleared; apply-owned state stays blocked. |
| `stale_check_reconciliation` | SchemaBot repaired stale `in_progress` stored check state by comparing it with authoritative apply state after a driver restart, crash, or race. |
| `schema_config_discovery` | SchemaBot discovered managed schema configs for the PR before deciding what to plan or which aggregate checks to publish. `status="blocked"` means discovery could not run against a complete changed-file list — today that is a PR over GitHub's per-PR file cap — so the aggregate fails closed instead of planning from a partial diff. |
| `schema_config_source_policy` | SchemaBot evaluated whether a discovered `schemabot.yaml` path is inside this repository's server-owned `allowed_dirs` boundary. `status="skipped"` means the config is outside the managed paths and was ignored; `status="error"` means the config is in a managed path but cannot be routed safely. |
| `schema_config_environment_validation` | SchemaBot found schema changes but none of the database's server-configured environments are allowed for this deployment, so it failed the aggregate check closed. |

A spike in `status="blocked"` for `operation="aggregate_check_sync"` or
`operation="stale_check_cleanup"` means SchemaBot is fail-closing instead of
allowing the latest commit on the PR branch to pass while earlier check state
still matters. For example, commit A can add a schema change and start an apply,
then commit B can remove that schema change before the apply finishes. SchemaBot
blocks the aggregate Check Run for commit B until an operator decides whether
the target environment needs another apply, a rollback, or manual reconciliation.

`status="blocked"` for `operation="schema_config_discovery"` is a different
signal: it counts PRs so large that GitHub will not report their full
changed-file list, so it tracks PR size rather than stored-state trouble. It is
deterministic per PR — the same PR blocks on every delivery until it is split —
and the author is told to split the PR, so no operator action is needed unless
the rate is high enough to suggest a repository routinely opening PRs SchemaBot
cannot plan. Pair it with `schemabot.github.pr_file_cap_exceeded_total`, whose
`schema_visible` label says how many of those PRs looked schema-related.

A spike in `status="error"` usually points to storage or GitHub API failures and
should be investigated before relying on branch-protection state.

### GitHub API Usage

`schemabot.github.rate_limit.remaining` is the most direct primary-rate-limit
signal. Group it by `github_app`, `installation_id`, and `resource` to see which
GitHub App deployment, installation, and rate-limit bucket is closest to
exhaustion. The same labels on `schemabot.github.rate_limit.limit` and
`schemabot.github.rate_limit.used` show the observed budget size and consumption
for the latest response.

`schemabot.github.requests_total` is the request-volume signal. Group it by
`category` to separate ordinary reads from content-generating writes. The
`write` category is the closest SchemaBot-side proxy for GitHub's secondary
content-generation limits, because those limits are enforced separately from
the primary hourly remaining count and are not exposed as a remaining-budget
header.

For error alerting, `status:error` covers HTTP failure responses (including
404s on auth and write operations, such as token exchanges against a suspended
App installation) and `status:transport_error` covers GitHub being unreachable.
Alert on both. `not_found` and `cancelled` are expected in routine operation
and should stay out of error-rate alerts; the `operation` label disaggregates
when a `not_found` rate looks anomalous.

Useful dashboard views:

1. Minimum `schemabot.github.rate_limit.remaining` by `github_app`,
   `installation_id`, and `resource`.
2. Request rate from `schemabot.github.requests_total` by `category`,
   `operation`, and `status`.
3. Write request rate by `github_app` and `installation_id` to spot
   progress-comment or check-run update bursts before GitHub starts returning
   secondary-limit errors.

### Control Operations

`schemabot.control_operations_total` tracks operator commands that act on an
existing apply or generate a rollback plan.

Operation values:

| Operation | Meaning |
|---|---|
| `cutover` | Trigger cutover for an apply that is waiting for cutover confirmation. |
| `stop` | Request that the engine stop an apply. |
| `start` | Request that the engine start or resume an apply. |
| `revert` | Request that the engine revert an apply. |
| `skip_revert` | Skip the post-deploy revert window for an apply. |
| `release` | Release a rollout paused after a failure so the remaining work proceeds. |
| `rollback_plan` | Generate a rollback plan from a previous completed apply. |

`schemabot.remote_control_requests.rejected_total` counts the same operations
from the other side: a control request the data plane acknowledged and its own
driver then failed. Acceptance only means the request was queued, so without
this mirror the operator is told the command succeeded and never learns the
effect did not land. A non-zero rate names the operation and `engine` that is
refusing operator commands — chart it by operation to see which control surface
is unsupported or broken on that engine, and read the apply log entry recorded
alongside it for the engine's own reason.

`schemabot.remote_control_requests.stale_resends_total` counts the case in
between: the data plane accepted a stop or cancel and has neither consumed nor
failed it past the stale threshold, so the control plane retransmits it. A
rejection is an answer; this is silence. A non-zero rate means an accepted
operator command is not taking effect and the apply will not converge until the
data plane's own driver can consume it — its logs carry the failing consume
error.

### Remote Apply Attaches

When a dispatch's idempotency key already maps to an apply, the data plane
resolves the dispatch by its derived operation key: a matching operation row is
an idempotent replay (counted on `schemabot.remote_apply_dedup_total`, with
`outcome` naming the path that resolved the key — `hit`, `conflict_race`,
`create_race`, or `key_collision_refused`), and a missing row attaches the
dispatch as a new sibling
operation, counted on `schemabot.remote_apply_attach_total`:

| Outcome | Meaning | Operator action |
|---|---|---|
| `attached` | A sibling dispatch added its operation and tasks to the deployment's shared keyed apply. | None — this is the normal fan-out event; its rate tracks sharded dispatch volume. |
| `attach_race` | A concurrent same-operation attach lost the unique-index insert and was resolved to the winner's row. | None in isolation — the dispatch replayed the winner. A sustained rate means a caller is double-dispatching the same operation; the paired info log carries the apply and operation key. |
| `terminal_refused` | The shared apply was already terminal, so the attach was refused fail-closed. | Investigate: the deployment's remaining operations cannot join a finished apply. The caller must re-dispatch under a fresh generation; the paired warn log names the apply, its state, and the refused operation key. |

On the control plane, `schemabot.remote_apply_key_echo_mismatch_total` counts
dispatches whose accepted response echoed a different operation key than the
request's shape derives to, so the control plane refused the response's remote
ids and failed the dispatch closed. A non-zero rate means a data plane is
answering deployment-keyed dispatches without resolving them by operation key —
most often a data plane running a version that predates sibling-operation
attach. The operator action is to upgrade (or roll back) the named database's
data plane so both planes derive the same key, then retry the blocked applies;
the paired error log carries the apply, the dispatched operation key, and the
echoed key.

`schemabot.remote_apply_deployment_id_conflict_total` counts dispatch results
the control plane refused because storing them would correlate one deployment
to two remote applies. All operations of a deployment attach into the
deployment's single data-plane apply and record the same remote apply id, so a
second id means the planes diverged — an in-flight apply spanning a
dispatch-key rollout, or a data plane that lost its keyed apply and minted a
fresh one. The operator action is to inspect the named database's apply
operations (the paired error log carries the recorded and refused ids), decide
which remote apply is authoritative, and re-dispatch under a fresh generation
once the planes agree.

`schemabot.lock_operations_total` tracks database-level lock acquisition and
release attempts.

Operation values:

| Operation | Meaning |
|---|---|
| `acquire` | Try to acquire the database lock for a plan/apply workflow. |
| `release` | Try to release the database lock, either by owner or administrative override. |

### Direct Write Authorization

`schemabot.direct_write_authorization.total` tracks per-database direct-write
(CLI/API) authorization decisions made at the handler layer, after the
forward-auth middleware has admitted the caller to the write tier. A spike in
`status=denied` means scoped operators are attempting operations outside their
grant — the `reason` attribute says whether the target database, the
environment, or the group membership mismatched.

Status values: `allowed`, `denied`, and `skipped` (the decision could not reach
an authorization outcome — for example the stored plan lookup failed — and the
request was rejected by the operation's own error path instead).

Reason values:

| Reason | Meaning |
|---|---|
| `scoped_lane_disabled` | No database has `operator_groups`; the decision is the plain admin gate. |
| `admin_allow` | Allowed via deployment `write_groups` membership. |
| `scoped_allow` | Allowed via the target database's `operator_groups` grant. |
| `missing_identity` | No authenticated user in the request context. |
| `not_admin` | Caller is not in `write_groups` (and, for admin-only operations, has no other path). |
| `not_database_operator` | Caller's groups do not match the target database's `operator_groups`. |
| `environment_not_allowed` | Target environment is outside `operator_environments`. |
| `missing_database_config` | The target database is not configured. |

Operation names mirror the mutating endpoints (`plan`, `apply`, control
operations, `lock_acquire`/`lock_release`/`lock_force_release`, and the
admin-only `checks_*`, `webhook_redrive`, `settings_set`). Unrecognized
operation, status, or reason values are recorded as `unknown` to keep
cardinality bounded. `database` and `environment` use the `unknown` sentinel
when the operation has no such dimension (admin-only operations have no single
target database; lock operations have no environment) or when the request
failed before the target resolved.

### Rate Limits

`schemabot.rate_limit_decisions.total` tracks the request-budget decisions made
by the API's rate-limited endpoints (today `POST /api/pull`). Both outcomes are
counted, so `decision=limit` has a denominator and a client approaching its
budget shows up as a rising share of one endpoint's traffic before any request
is actually refused.

`scope` says which of the two budgets was consulted, and they answer different
questions:

| Scope | Keyed on | A sustained `limit` rate means |
|---|---|---|
| `caller` | The authenticated subject (an operator's identity, or a service caller's SPIFFE ID) | One client is looping. Find it in the paired WARN log's `caller` attribute before raising the budget. |
| `target` | The database and environment being read | A database is absorbing more schema reads than the budget allows, from any number of clients. Check whether the reads are legitimate before raising it. |

The caller identity and target database are deliberately not metric
attributes — there are hundreds of each, and the counter would carry a series
per client per database. Every limited request logs both alongside the
advertised retry delay, so a spike on this counter is triaged from the WARN
logs, not by slicing the metric.

`environment` arrives in the request body, so it is clamped to a configured
environment before it is recorded and appears as `unconfigured` otherwise. A
budget is still keyed on the environment the request named — an unroutable
request spends budget like any other — so a rising `unconfigured` share means
clients are asking for environments this server does not serve. The unclamped
name is in the log.

Budgets are enforced per server process, so a fleet-wide `limit` rate is the sum
across replicas and the effective ceiling is `replicas ×` the configured rate.
See [Rate Limits](../../docs/configuration.md#rate-limits) for the config.

## HTTP Server Metrics

The `otelhttp` middleware automatically produces standard HTTP metrics for every endpoint:

| Metric | Type | Attributes | Description |
|---|---|---|---|
| `http.server.request.duration` | Histogram | environment, http.request.method, http.response.status_code | Request latency by method and status code |
| `http.server.request.body.size` | Histogram | environment | Request body sizes |
| `http.server.response.body.size` | Histogram | environment | Response body sizes |

SchemaBot attaches `environment="unknown"` to these process-wide HTTP metrics
because routing-level request metrics do not belong to one schema change
environment. Environment-specific operation metrics use the real environment.

## Adding New Metrics

### Does it belong in a metric?

Metrics earn their place on volume. A rate, a trend, or a threshold only means
something when the underlying event happens often enough to have a baseline:
retry storms, claim contention, throttling, error rates on a hot path. A counter
for a rare dangerous branch reads zero for weeks, so there is nothing to alert
against, and an increment tells an operator far less than the `slog.Warn` or
`slog.Error` beside it, which already carries the identifiers, the affected
objects, and the reason. Log the rare branch well and leave it there — a
log-based monitor can still count it if that day arrives.

Adding a status, reason, or outcome value to an existing counter is a different
decision from adding an instrument, and is usually the right one. The hot-path
counter already has the baseline, so the rare case becomes an alertable slice of
it instead of a series that is almost always zero: `app_repo_mismatch` on
`schemabot.webhook.events_total` is the shape to copy — it shares a series with
every other webhook outcome, which is what makes "the second one, never the
first" expressible as an alert.

Several counters here predate this bar and would not be added today —
`schemabot.check_ownership_misses_total` and
`schemabot.promotion.config_error_blocks_total` among them. They stay because
removing an emitted series breaks whatever consumes it, not because they are the
pattern to follow.

This is the reasoning behind the *Metrics are for what can go haywire* item in
[AGENTS.md](../../AGENTS.md) § PR Self-Review Bar.

### The recipe

Define recording functions in `metrics.go` following the existing pattern:

```go
func RecordXxx(ctx context.Context, database, environment, status string) {
    meter := otel.Meter(meterName)
    counter, err := meter.Int64Counter("schemabot.xxx.total",
        otelmetric.WithDescription("Description"),
        otelmetric.WithUnit("{unit}"),
    )
    if err != nil {
        slog.Warn("failed to create counter", "error", err)
        return
    }
    counter.Add(ctx, 1, otelmetric.WithAttributes(
        attribute.String("database", database),
        EnvironmentAttribute(environment),
        attribute.String("status", status),
    ))
}
```

The OTel SDK deduplicates instruments with the same name, so calling `Int64Counter` on every invocation is safe and cheap after the first registration.
