# Check Runs

SchemaBot uses GitHub Check Runs as the PR safety gate for schema changes. A
required SchemaBot check blocks merge until every schema change represented by
the PR has either been applied successfully or resolved by a new plan.

## GitHub Check Concepts

GitHub uses [status checks][github-status-checks] to show whether a commit is
ready to merge. Common examples are CI, linting, security scans, deployment
gates, and SchemaBot's schema safety gate. When repository owners configure
[required status checks][github-protected-branches] in branch protection, GitHub
will not merge a PR until the required checks on the PR's current head commit
are passing.

There are two main ways external systems report status to GitHub:

| Concept | What it is | Why use it |
| --- | --- | --- |
| Commit status | The older status API for setting a simple state on a commit. | Good for simple pass/fail integrations. |
| Check run | The GitHub App-backed status object created through the [Checks API][github-check-runs]. | Better for apps that need named checks, detailed output, annotations, reruns, or richer state. |

A check suite is GitHub's grouping of check runs created by one GitHub App for a
commit. SchemaBot mostly cares about the individual check run because that is the
named object shown in branch protection, such as `SchemaBot` or
`SchemaBot (staging)`.

Why SchemaBot uses check runs:

- **Merge gating:** branch protection can require `SchemaBot` before merge.
- **Commit-scoped safety:** every new PR head needs its own check result, so a
  stale result from an older commit cannot approve a newer schema shape.
- **Operator visibility:** the check summary can explain which databases and
  environments are pending, running, failed, or clean.
- **GitHub App integration:** SchemaBot is a GitHub App, and check runs are the
  native status primitive for GitHub Apps.

For SchemaBot, the important mechanics are:

- A check run belongs to one commit SHA. Updating a check run from an older SHA
  does not satisfy branch protection for a newer PR head.
- A check run has a `status`, such as `in_progress` or `completed`.
- A completed check run has a `conclusion`, such as `success`, `failure`, or
  `action_required`.
- If a SchemaBot check is required by branch protection, non-passing states
  block merge.

## Safety Philosophy

Check runs are a tier 0 feature for SchemaBot. They connect declarative schema
review to GitHub branch protection, so a missing check or an incorrect passing
check can allow an unguarded schema change to merge.

SchemaBot fails closed. When it cannot safely discover config, read or write its
stored check state, verify the current PR head SHA, or reconcile an in-progress
apply, it should leave branch protection blocked and surface an
operator-visible error. Failing open would mean treating unknown state as safe:
skipping a required check, publishing success without proving the database is
clean, or letting stale work update the current PR head.

Passing states must be explicit. A check can pass when a current plan finds no
changes, when the apply represented by the stored check state completes
successfully, or when the current PR head no longer touches that managed
database. Missing storage, a stale SHA, or ambiguous apply ownership is a
failure to represent safety, not a reason to skip.

## Published Checks

SchemaBot publishes aggregate GitHub checks, not one visible check per database.
The aggregate check rolls up internal per-database state into one stable check
name that can be required in branch protection.

When all environments are owned by one SchemaBot deployment, require this check:

```text
SchemaBot
```

When `allowed_environments` is configured, SchemaBot publishes one aggregate per
owned environment. Require each environment check that applies to the repository:

```text
SchemaBot (staging)
SchemaBot (production)
```

Per-database state is stored internally and shown in the aggregate check summary.

## Managed Schema Configs

A managed schema config is a `schemabot.yaml` file discovered in the repository.
It tells SchemaBot which database, database type, environments, and schema
directory a set of declarative SQL files belongs to.

When a PR changes SQL files under a directory owned by a discovered
`schemabot.yaml`, SchemaBot treats that database as managed work for the PR. It
plans the desired declarative schema from those files and writes internal check
records for each affected environment/database.

SQL files outside an onboarded schema directory are not managed by SchemaBot for
that PR. In that case, SchemaBot publishes passing aggregate checks so branch
protection is not left waiting for a check it cannot produce.

See [namespaces](./namespaces.md) for the schema directory and namespace layout.

## Internal Records

The `checks` MySQL table stores one internal record per
`repository`, `pull_request`, `environment`, `database_type`, and `database_name`.
Aggregate check records use the same unique key, with `_aggregate` as a
sentinel for `database_type` and `database_name`.

For environment-scoped aggregates, `environment` is the real environment:

```text
environment=staging
database_type=_aggregate
database_name=_aggregate
```

For the global aggregate, all three dimensions use `_aggregate`:

```text
environment=_aggregate
database_type=_aggregate
database_name=_aggregate
```

Important columns:

| Column | Purpose |
| --- | --- |
| `head_sha` | Commit SHA the record represents. GitHub only displays checks for the current PR head. |
| `status` | `in_progress` or `completed`. |
| `conclusion` | `success`, `failure`, or `action_required` when complete. |
| `apply_id` | Storage apply ID represented by the row. It is set when an apply or rollback starts, and normal apply completion leaves it set so later cleanup can tell that live work started. |
| `blocking_reason` | Stable machine-readable reason for fail-closed states that operators may need to triage. |
| `check_run_id` | GitHub check run ID for aggregate records. |

The internal records let SchemaBot answer safety questions without repeatedly
querying GitHub:

- Which databases in this PR still need an apply?
- Is a prior environment already clean?
- Which checks should contribute to the aggregate?
- Did a terminal apply still own the check it is trying to complete?

Only aggregate rows map to visible GitHub Check Runs. Per-database rows are
internal state that SchemaBot uses to compute the aggregate summary and protect
race-sensitive apply transitions.

## Lifecycle

### Pull Request Events

SchemaBot listens to these GitHub `pull_request` webhook actions:

| Action | Meaning | SchemaBot behavior |
| --- | --- | --- |
| `opened` | PR was opened. | Discover managed schema configs, auto-plan, and publish aggregate checks on the PR head SHA. |
| `synchronize` | PR head changed, usually from a pushed commit or force-push. | Treat the new head SHA as the source of truth, auto-plan affected configs, clean up stale records, and publish checks on the new SHA. |
| `reopened` | Closed PR was reopened. | Re-discover configs and auto-plan the current head. Stored rows deleted on close are not restored. |
| `closed` | PR was closed or merged. | Release locks held by the PR and delete internal check records. In-flight applies continue in Tern. |

Other `pull_request` actions are ignored by the check-run lifecycle.

### Auto-Plan

A PR is synchronized when GitHub sends a `pull_request` webhook with action
`synchronize`, usually because the PR branch received a new commit or was
force-pushed. In SchemaBot docs, "synchronized" means "the PR has a new head
SHA."

```text
PR opened or synchronized
        |
        v
Discover managed schema configs touched by the PR
        |
        v
Filter to environments this deployment is allowed to process
        |
        +-- schema changes but no allowed configured envs
        |        |
        |        v
        |   aggregate completed/failure
        |
        v
Run plan for each affected environment/database
        |
        +-- no changes ---------> per-db completed/success
        |
        +-- changes found ------> per-db completed/action_required
        |
        +-- plan/config error --> aggregate completed/failure
        |
        v
Recompute aggregate check on the current PR head SHA
```

### Apply

```text
per-db completed/action_required
        |
        | schemabot apply -e <env>
        v
Reconcile stale in_progress rows
        |
        v
Re-plan, acquire lock, post confirmation
        |
        v
per-db completed/action_required
waiting for apply-confirm
        |
        | schemabot apply-confirm -e <env>
        v
Tern accepts apply
        |
        v
per-db in_progress with apply_id
aggregate in_progress
        |
        v
Apply reaches terminal state
        |
        +-- completed ----------> per-db completed/success
        |
        +-- failed/stopped/etc -> per-db completed/failure
        |
        v
Recompute aggregate check
```

Apply completion is conditional on ownership: the watcher can only update the
row if the row still represents that `apply_id` and no newer apply exists for
the same PR/environment/database.

### Rollback

```text
previous apply completed
        |
        | schemabot rollback <apply-id> -e <env>
        v
Generate rollback plan and acquire lock
        |
        | schemabot rollback-confirm -e <env>
        v
Tern accepts rollback apply
        |
        v
per-db in_progress with rollback apply_id
aggregate in_progress
        |
        v
Rollback reaches terminal state
        |
        +-- completed ----------> per-db completed/action_required
        |
        +-- failed/stopped/etc -> per-db completed/failure
        |
        v
Recompute aggregate check
```

A successful rollback returns the check to `action_required` because the target
environment no longer contains the PR's desired schema changes.

### Unlock And Close

```text
schemabot unlock
        |
        v
Release lock only
        |
        v
Check still blocks if unapplied schema changes remain
```

`schemabot unlock` releases locks. It does not make the required aggregate check
pass; if the PR still contains unapplied schema changes, the aggregate remains
blocked until a new plan shows no changes or the changes are applied.

```text
PR closed
        |
        v
Release locks held by the PR
        |
        v
Delete internal check records for the PR
        |
        v
In-flight applies continue in Tern
```

## Check States

GitHub check runs have a `status` and, once completed, a `conclusion`. SchemaBot
uses the same model for its internal per-database rows, then rolls those rows up
into the aggregate GitHub check run. See GitHub's
[create a check run documentation][github-create-check-run] for the allowed
Check Run API values.

SchemaBot uses these aggregate states:

| GitHub state | SchemaBot meaning | Merge effect when required |
| --- | --- | --- |
| `in_progress` | A schema apply is running or SchemaBot is still representing active work. | Blocks merge. |
| `completed` / `success` | The current PR head has no unapplied managed schema changes for the rolled-up scope. | Passes branch protection. |
| `completed` / `action_required` | SchemaBot needs a human decision or another command before merge is safe. | Blocks merge. |
| `completed` / `failure` | Planning, applying, reconciliation, or check-state handling failed. | Blocks merge. |

Aggregate logic is intentionally conservative. First match wins:

| Internal state | Aggregate result |
| --- | --- |
| Any record is `in_progress` | `in_progress` |
| Any record has conclusion `failure` | `completed` / `failure` |
| Any record has conclusion `action_required` | `completed` / `action_required` |
| All records have conclusion `success` | `completed` / `success` |

SchemaBot uses `action_required` for completed checks that are not system
failures, but still need an operator action before merge is safe. Common cases:

- A plan found unapplied schema changes.
- `schemabot apply` created a lock and is waiting for `apply-confirm`.
- A rollback succeeded, so the PR's desired schema is no longer present in the
  target environment.
- A schema change was removed from the PR after an apply started, so the live
  database and declarative schema may need reconciliation.

When the aggregate check is required by branch protection, `action_required`
blocks merge. That is intentional: it distinguishes "SchemaBot needs a human
decision" from `failure`, which means SchemaBot or the apply encountered an
error.

If a PR has no managed schema files, SchemaBot posts passing aggregate checks on
the current head SHA so branch protection is not left waiting for a check that
will never be produced.

## Blocking Reasons

Some fail-closed states store a stable `blocking_reason` in the internal check
record. This value is for logs, metrics, and operator triage. The GitHub Check
Run output remains human-readable and may change.

| `blocking_reason` | Stored on | Meaning |
| --- | --- | --- |
| `schema_removed_after_apply_started` | Per-database row | A newer commit removed schema changes after an apply or rollback had already started. The check stays blocked until an operator reconciles the live schema and PR state. |
| `rollback_completed` | Per-database row | A rollback succeeded, so the target environment no longer contains the PR's desired schema. |
| `github_schema_config_discovery_unavailable` | Aggregate row | SchemaBot knew the PR head SHA, but GitHub was unavailable while SchemaBot inspected changed files or repository contents. |
| `schema_config_discovery_failed` | Aggregate row | SchemaBot could reach GitHub, but could not determine the managed schema configuration or schema files. |
| `no_allowed_configured_environments` | Aggregate row | Schema files changed, but none of the environments declared by `schemabot.yaml` are allowed for this deployment. |

Generic plan and apply errors can still publish `completed` / `failure` without
a stable `blocking_reason` when the error is not one of the explicit classes
above.

## Operator Guidance

When this document says to retry SchemaBot from a PR, it means posting a
SchemaBot command as a PR comment. The GitHub Check Run "Re-run" button is not
the recovery path today because SchemaBot does not handle GitHub's
`check_run.rerequested` webhook event.

Use these PR comment commands for normal retry paths:

```text
schemabot plan -e <environment>   # Re-plan one environment and refresh checks
schemabot plan                    # Re-plan all configured environments
schemabot apply -e <environment>  # Re-run apply gating and create a new apply plan
```

A stored check ownership miss means a worker tried to update a stored check row,
but the row no longer represents the apply that worker is completing. This is
usually healthy race protection: a newer plan, apply, rollback, or stale-check
reconciliation pass has become the source of truth for that PR/environment/
database, so the older worker must not overwrite it.

Common fail-closed scenarios:

| Scenario | Why SchemaBot blocks | Operator action |
| --- | --- | --- |
| PR branch moved while SchemaBot was processing | The work was based on an older commit SHA. Publishing that result on the current PR could be misleading. | Usually wait for the `synchronize` webhook to auto-plan the latest commit. If the required check stays missing or stale, comment `schemabot plan -e <environment>` on the PR, or `schemabot plan` for all configured environments. |
| Aggregate update skipped for a stale SHA | A background worker tried to publish status for a commit that is no longer the PR head. | Confirm the latest commit has a SchemaBot aggregate check. If it does not, comment `schemabot plan -e <environment>` or `schemabot plan`. |
| GitHub unavailable during config discovery | SchemaBot cannot safely read PR metadata or repository contents. | Wait for GitHub API recovery, then comment `schemabot plan -e <environment>` or `schemabot plan`. CLI operations can still manage live schema changes while GitHub is down, but they do not make branch protection pass. |
| Config discovery failed for a non-GitHub reason | SchemaBot could read GitHub, but could not determine the managed schema config or schema files. | Fix `schemabot.yaml`, the schema file layout, or the invalid SQL/config state. Push the fix, or comment `schemabot plan -e <environment>` after fixing the PR. Use logs with `blocking_reason=schema_config_discovery_failed` for the underlying read or parse failure. |
| Schema changes found but no configured environments are allowed | The repo's `schemabot.yaml` asks for environments this deployment is not allowed to process. Publishing success would hide a deployment/config mismatch. | Align `schemabot.yaml` and this deployment's `allowed_environments`, then push the config fix or comment `schemabot plan -e <environment>`. |
| Accepted apply cannot be tracked | The engine accepted work, but SchemaBot could not store or reload the apply ID needed for progress and check ownership. | Treat this as a storage or apply-tracking incident. Inspect engine state and the `applies` table before retrying; do not rely on branch protection until a new plan reflects the live schema. |
| Accepted apply could not update required check state | The apply may be running, but SchemaBot could not mark the stored check row `in_progress` with the accepted `apply_id`. | Inspect the accepted apply, storage health, and aggregate check. Retry only after confirming the live schema state and stored check state agree. |
| Prior-environment check state could not be read | SchemaBot cannot prove that an earlier environment is clean. | Treat this as a SchemaBot storage health issue. Restore storage access, then repeat the blocked command, for example `schemabot apply -e production`. Do not bypass the promotion gate unless this is an explicit breakglass decision. |
| Stored check ownership miss | A newer plan or apply owns the stored check state for the same PR, environment, and database. Letting the older worker write would overwrite newer safety state. | Inspect the newest apply for that repo/PR/environment/database with the CLI, for example `schemabot status -d <database> -e <environment>` or `schemabot progress <apply-id>`. Let the newest apply finish, or reconcile it with operator commands before retrying PR comments. |
| Schema changes were removed while an apply may still be running | The live database may still change even though the current PR no longer represents that change. | Inspect the in-flight apply in Tern or with the CLI. If the change reached the live database, either put the schema change back in the PR and comment `schemabot plan -e <environment>` before applying again, or roll back/reconcile the live schema first. |
| Stale in-progress row after a pod crash | Stored check state says an apply is running, but the watcher may have died before publishing the terminal result. | Comment `schemabot plan -e <environment>` or `schemabot apply -e <environment>` to trigger stale-check reconciliation. If reconciliation fails, inspect SchemaBot storage and the latest apply for that database. |

[github-create-check-run]: https://docs.github.com/en/rest/checks/runs#create-a-check-run
[github-check-runs]: https://docs.github.com/en/rest/checks/runs
[github-protected-branches]: https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches
[github-status-checks]: https://docs.github.com/articles/about-statuses

## SHA Handling

GitHub check runs are tied to a commit SHA. Updating a check run from an older
SHA does not satisfy branch protection on a newer commit.

On new commits:

- Aggregate checks are created or updated on the new PR head SHA.
- Auto-plan writes new internal records for still-affected databases.
- Stale internal records for databases no longer affected by the PR are marked
  `success` and the aggregate is recomputed only when no apply or rollback has
  started for that row.
- Stale internal records with a started apply or rollback remain blocking
  because the live database may still change.
- A plan result for the same head SHA does not overwrite an in-progress apply
  check. A plan result for a new head SHA clears `apply_id` and becomes the new
  source of truth for databases still managed by the new PR head.

## Edge Cases

### PR opened

An `opened` webhook triggers auto-plan for every managed schema config touched by
the PR. For each affected database and environment, SchemaBot stores an internal
per-database record and then publishes or updates the aggregate check on the PR
head SHA.

If auto-plan finds changes, the internal records become `action_required` and
the aggregate blocks. If auto-plan finds no changes, the records become
`success` and the aggregate passes. Auto-plan skips the PR comment when every
environment has no changes and no errors, but it still writes the check state.

### PR touches no managed schema files

If the PR has no managed schema files, SchemaBot publishes passing aggregate
checks on the current head SHA. This keeps branch protection from waiting for a
SchemaBot check that would otherwise never be created.

This also covers SQL files outside an onboarded schema directory: without a
matching `schemabot.yaml`, SchemaBot has no managed schema config to plan.

### Config discovery fails

If SchemaBot cannot inspect schema files or config safely, it publishes failing
aggregate checks instead of silently skipping the PR. That keeps the branch
protection gate fail-closed and gives operators a visible failure to triage.

### GitHub unavailable

If GitHub is unavailable, SchemaBot cannot receive webhooks, read the current PR
head, post comments, or create and update check runs. It must not invent a
passing state to compensate. Existing required checks may remain pending, stale,
or failed until GitHub recovers.

If SchemaBot can verify the current PR head SHA but GitHub becomes unavailable
while reading changed files or repository contents, SchemaBot publishes a failing
aggregate check on that verified SHA. If SchemaBot cannot verify the current PR
head SHA at all, it does not publish a new check run because it cannot know which
commit GitHub branch protection is evaluating.

SchemaBot's CLI and API can still manage schema changes when the SchemaBot
service, storage, and schema engine are reachable. Operators can inspect active
work, watch logs, stop or resume an apply, trigger cutover, roll back a terminal
apply, and manage locks without using GitHub PR comments. These actions do not
make GitHub branch protection pass while GitHub is unavailable; once GitHub
recovers, operators should re-plan or otherwise refresh the PR state so check
runs reflect the live database state again.

In a true emergency, repository administrators can temporarily relax or remove
the required SchemaBot check from branch protection. Prefer waiting for GitHub
to recover when the schema change is not urgent. If branch protection is relaxed
for breakglass, restore it after the incident and audit any direct CLI actions.

### New commit pushed

A GitHub `pull_request.synchronize` webhook represents a new PR head SHA.
SchemaBot never tries to satisfy the new commit by updating a check run from the
old SHA.

On the new head:

- Databases still affected by the PR get fresh plan results.
- Databases no longer affected by the PR are cleaned up according to whether an
  apply already started.
- Passing aggregates are recreated for PRs with no managed schema work only
  when no stored per-database state still blocks.
- Any stale webhook work for an older SHA is ignored when SchemaBot can verify
  that the PR head has moved on.

If a new plan is for the same head SHA as a running apply, it does not overwrite
the running `apply_id`. If a new plan is for a newer head SHA, it clears the old
ownership marker and becomes the source of truth for that PR head.

### New commit pushed while an apply is in progress

An in-flight apply is not cancelled when a new commit is pushed. It continues in
Tern, and the database lock remains relevant until the apply reaches a terminal
state or an operator releases it.

#### New head still contains managed schema changes

The `synchronize` webhook auto-plans the new PR head. Because GitHub branch
protection evaluates checks on the current head commit, SchemaBot writes
aggregate checks for the new SHA.

For affected databases, the new plan result becomes the merge-gating source of
truth for that head. If the new plan still has unapplied changes, the aggregate
is `action_required`. If the live schema already matches the new desired schema,
the aggregate can pass.

When the old apply watcher eventually finishes, it can complete the check only
if the stored row still belongs to that `apply_id` and no newer apply exists for
the same PR, environment, database type, and database name. If a new-head plan
has replaced the row, the old watcher cannot mark the new PR head success or
failure based on work started for an older commit.

This is conservative for commits that keep touching the same managed database:
the new head must be proven by a plan against the current desired schema, not by
blindly reusing a terminal result from work started on an older commit.

#### New head removes the schema changes before apply starts

If the new commit removes the schema changes and no apply was started, that
database no longer has managed schema work in the current PR head. SchemaBot
marks the previous per-database storage record `success` and `has_changes=false`
for the new head, then recomputes the aggregate and publishes the aggregate
GitHub check run on the new SHA. On a `synchronize` webhook this usually creates
a new aggregate check run rather than updating the old one, because GitHub check
runs are tied to a commit and branch protection evaluates the current head
commit.

#### New head removes the schema changes after apply starts

If an apply was already started, removing the schema changes must not make the
aggregate pass. While the apply is still running, SchemaBot keeps the
current-head aggregate `in_progress` because the active schema change still owns
the check. When that apply reaches a terminal state, SchemaBot completes the
current-head check as blocking instead of green. A failed apply records
`failure`; a successful apply records `action_required` because the live
database may now contain a change that the PR no longer represents.

Operators should treat that as an explicit operational follow-up: roll back the
apply if the live database should return to its previous shape, or open a new PR
that makes the declarative schema match the intended live state.

#### Stale webhook work arrives late

Webhook delivery and background work can be reordered. Before SchemaBot writes
plan, cleanup, or aggregate state, it verifies the PR's current head SHA when it
can. If the work belongs to an older head, SchemaBot skips the write instead of
publishing a misleading check on the current commit.

### PR closed

A `closed` webhook releases locks held by that PR and deletes SchemaBot's stored
check records for the PR. It does not cancel in-flight applies. Those applies
continue to their terminal state in Tern.

GitHub check runs already published to commits remain in GitHub history, but
SchemaBot removes its internal PR safety state because the PR is no longer
active.

### PR reopened

A `reopened` webhook is handled like `opened`: SchemaBot discovers the current
schema configs, runs auto-plan, recreates internal records, and publishes
aggregate checks on the current PR head SHA.

Reopen does not restore the old stored rows deleted on close. The new plan is
the source of truth.

### Manual plan

`schemabot plan -e <environment>` first reconciles stale in-progress records,
then plans the requested environment. `schemabot plan` without an environment
does the same reconciliation, then plans all configured environments. The plan
result writes per-database records and updates the aggregate.

If the plan finds no changes, that environment's record becomes `success`. If it
finds changes, the record becomes `action_required`. If planning fails, SchemaBot
posts a failure comment; when every environment in a multi-environment plan
fails, it also publishes a failing aggregate check.

### Apply requested

`schemabot apply -e <environment>` re-plans before acquiring a lock. If changes
exist and pass safety checks, SchemaBot acquires a lock, posts a confirmation
comment, stores `action_required`, and updates the aggregate.

If the apply command finds no changes, SchemaBot posts a no-change plan comment
and does not acquire a lock. A later plan on the current head records the passing
state if the stored check still needs to be updated.

### Apply confirmed

`schemabot apply-confirm -e <environment>` verifies review and PR-check gates,
verifies the lock, re-plans for drift, and then submits the apply.

When Tern accepts the apply, SchemaBot marks the internal record `in_progress`
and stores the accepted `apply_id`. Accepted applies must have a stored apply ID;
without one SchemaBot cannot safely track progress or prove check ownership. If
the apply is accepted but SchemaBot cannot store, load, or attach that ID to the
check row, it posts an operator-visible error when it can and leaves branch
protection blocked.

When the apply reaches a terminal state:

- `completed` sets the internal record to `success` and lets the aggregate pass
  if every other record is also successful.
- `failed`, `stopped`, `cancelled`, or other non-success terminal states set the
  internal record to `failure` and the aggregate fails.

Completion is conditional on ownership: the watcher can only update the row if
the row still represents that `apply_id` and no newer apply exists for the same
PR/environment/database.

### Auto-confirm apply

`schemabot apply -y` still verifies that the stored plan is current. If the PR
head changed or the re-plan DDL differs from the stored plan, SchemaBot
downgrades to manual confirmation and leaves the check in `action_required`.

### Rollback requested

`schemabot rollback <apply-id> -e <environment>` generates a rollback plan and
acquires a lock, but it does not change the required check yet. The check changes
only after `rollback-confirm` starts the rollback apply.

Rollbacks are not gated by prior environment ordering because they are
operator-facing controls for emergencies and direct maintenance.

### Rollback confirmed

`schemabot rollback-confirm -e <environment>` submits the rollback apply and
marks the check `in_progress` with the rollback apply's `apply_id`.

Like normal applies, accepted rollbacks must have a stored apply ID before
SchemaBot can safely watch progress or update the required check state.

If the rollback apply fails, the watcher records `failure`. If the rollback
apply succeeds, SchemaBot returns the check to `action_required` and recomputes
the aggregate. That is intentional: the target environment no longer contains
the PR's desired schema changes, so the PR must apply them again or remove them.

Rollback completion uses the same ownership guard as apply completion. An old
rollback watcher cannot overwrite a newer plan or newer apply result.

### Unlock

`schemabot unlock` releases the PR's lock. It does not mark the aggregate check
as passing. If the PR still contains unapplied schema changes, the aggregate
continues to block until a new plan shows no changes or the changes are applied.

### Schema changes but no configured environments are allowed

When `allowed_environments` is configured, a SchemaBot deployment may discover
that the PR touches a managed database but none of the environments declared by
that database's `schemabot.yaml` are allowed for this deployment. That is treated
as a configuration mismatch, not a no-op. SchemaBot publishes a failing aggregate
for the environments this deployment is responsible for, stores
`blocking_reason=no_allowed_configured_environments`, and does not post a plan
comment because no environment reached planning.

This is different from a PR that touches no managed schema files. A PR with no
managed schema files gets a passing aggregate so branch protection is not left
waiting for SchemaBot. A PR with managed schema changes but no allowed configured
environment gets a failing aggregate because SchemaBot found work it cannot
safely plan.

### Stale in-progress check

If a pod crashes while watching an apply, the apply may be terminal while the
internal check row remains `in_progress`. Before plan and apply commands,
SchemaBot reconciles those rows by looking at the latest apply for the same
PR/environment/database. It only completes the row if that latest apply is
terminal.

If reconciliation cannot read or write the safety state it needs, PR commands
fail closed.

## Stale Check Reconciliation

SchemaBot can run in multiple pods, and a pod can crash while watching an apply.
That can leave an internal record stuck in `in_progress` even though the apply
already completed.

Before PR plan and apply commands, SchemaBot reconciles stale in-progress records:

1. Load internal checks for the PR.
2. Load applies for the PR.
3. For each in-progress per-database record, find the latest apply for the same
   environment, database type, and database.
4. If that apply is terminal, complete the check conditionally.
5. Recompute the aggregate on the current PR head SHA.

If reconciliation cannot read or write the safety state it needs, PR commands
fail closed and surface an error instead of proceeding with an unguarded apply.

## Race Safety

Status checks are safety-critical. A read-then-write sequence is not enough
because another SchemaBot pod can change the same row between those operations.

SchemaBot uses `apply_id` as a durable ownership marker:

- Apply start sets the internal record to `in_progress` and stores the apply ID.
- Apply completion only updates the row if it is still `in_progress`, still has
  that same `apply_id`, and no newer apply exists for the same
  PR/environment/database.
- Normal apply completion leaves `apply_id` set on the terminal row. That keeps
  later stale-cleanup code from treating started live work as plan-only state.
- Rollback start uses the same `in_progress` ownership marker as apply start.
- Rollback completion only returns the row to `action_required` if the rollback
  apply still owns the row and no newer apply exists.

This prevents an older apply watcher, stale reconciliation pass, or rollback
watcher from overwriting a newer plan or newer apply result.

## Environment Ordering

Environment ordering is enforced for PR comment commands. The set of enabled
environments comes from `schemabot.yaml`, but the order comes from server config:

```yaml
environment_order:
  - staging
  - production
```

If `environment_order` is omitted, SchemaBot defaults to staging before
production.

For clients, `schemabot.yaml` environments are strictly an opt-in mechanism:

```yaml
environments:
  - production
  - staging
```

The order in that file is ignored for apply gating. In the example above, the
repository has opted into both environments. SchemaBot still treats staging as
the prior environment for production because the server-owned order says staging
comes first.

Before applying to an environment, SchemaBot checks all prior environments for
the same database that are also enabled in `schemabot.yaml`:

| Prior environment state | Apply allowed? | Reason |
| --- | --- | --- |
| `success` | Yes | Prior environment is clean. |
| `action_required` | No | Apply the prior environment first. |
| `in_progress` | No | Wait for the prior environment to finish. |
| `failure` | No | Fix and re-apply the prior environment. |
| No record | No | SchemaBot cannot prove the prior environment is clean. |

The lookup path depends on which SchemaBot deployment owns the prior
environment:

- If the same deployment owns the prior environment, SchemaBot reads its local
  check storage for the same PR, database type, database, and prior
  environment.
- If another deployment owns the prior environment, SchemaBot reads the
  environment aggregate check run from GitHub, such as `SchemaBot (staging)`.

Remote prior-environment checks use the aggregate because the current deployment
does not have access to the other deployment's per-database storage. That makes
the remote check stricter than the local check: a production apply can be
blocked by any pending or failed staging SchemaBot aggregate, not only by the
same database's staging row.

In split deployments, GitHub check runs are the central authority for
cross-environment gating. A production SchemaBot instance does not need staging
target identifiers in its server config. Its apply flow is:

```text
schemabot apply -e production
        |
        v
Read enabled environments from schemabot.yaml
and promotion order from server config
        |
        v
For each prior environment:
  - if this instance owns it, read SchemaBot storage
  - if another instance owns it, read that environment's GitHub aggregate check
        |
        v
Only resolve the production target from this instance's ConfigMap
```

This means environment-scoped ConfigMaps can stay environment-local: the staging
ConfigMap contains staging targets, the production ConfigMap contains production
targets, and GitHub check runs connect the promotion gate between them.

A missing prior-environment record or aggregate blocks the apply because
SchemaBot cannot prove the prior environment is clean. Unknown prior-environment
state is not treated as safe.

Rollback commands, such as `schemabot rollback <apply-id> -e <environment>`,
are not gated by prior environment checks. They are operator-facing controls for
emergencies and direct maintenance.

The CLI is operator-facing and does not enforce PR environment ordering. This is
intentional so operators can handle emergencies and maintenance tasks directly.

## Breakglass Operations

PR comments and check runs are the normal path for PR-driven schema changes, but
operators need a direct path when GitHub is degraded, a webhook is delayed, a
lock is stuck, or an active apply needs intervention. The CLI talks to the
SchemaBot API directly, so it does not depend on GitHub webhooks or PR comments
for those operator actions.

Common breakglass capabilities:

- Inspect active work with `schemabot status`, `schemabot status -d <database>
  -e <environment>`, `schemabot progress <apply-id>`, and `schemabot logs
  <apply-id>`.
- Control active work with `schemabot stop <apply-id>`, `schemabot start
  <apply-id>`, `schemabot cutover <apply-id>`, `schemabot revert <apply-id>`,
  and `schemabot skip-revert <apply-id>`.
- Apply or roll back directly with `schemabot apply -s <schema-dir> -e
  <environment>` and `schemabot rollback <apply-id>`.
- Inspect and release locks with `schemabot locks`, `schemabot unlock -d
  <database> -t <type>`, and `schemabot unlock -d <database> -t <type>
  --force`.

Breakglass is still safety-critical. Operators should prefer the least
surprising action, record what was done, and update the declarative schema or PR
afterward so the source of truth matches the live database. Direct CLI work can
bypass PR-comment-only checks such as environment ordering, so it should be
treated as an explicit operator decision rather than an automatic merge signal.

CLI success also does not make GitHub checks pass by itself. Required checks
pass only after SchemaBot can publish current check-run state for the PR head.
After GitHub or webhook recovery, run a new plan or otherwise refresh the PR so
branch protection reflects the current database state.

## Branch Protection Setup

Require the aggregate SchemaBot check in branch protection.

When all environments are owned by one SchemaBot deployment, require:

```text
SchemaBot
```

For deployments that split ownership by `allowed_environments`, require each
published environment aggregate:

```text
SchemaBot (staging)
SchemaBot (production)
```

GitHub UI setup:

1. Open repository settings.
2. Go to Branches and edit the branch protection rule.
3. Enable "Require status checks to pass before merging".
4. Select the SchemaBot aggregate check names used by the deployment.

GitHub API example for a single aggregate:

```bash
gh api repos/{owner}/{repo}/branches/{branch}/protection \
  --method PUT \
  --input - <<'EOF'
{
  "required_status_checks": {
    "strict": false,
    "contexts": ["SchemaBot"]
  },
  "enforce_admins": null,
  "required_pull_request_reviews": null,
  "restrictions": null
}
EOF
```
