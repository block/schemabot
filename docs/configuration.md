# Configuration

SchemaBot loads config from the `SCHEMABOT_CONFIG_FILE` environment variable.

## Local Mode

The SchemaBot process runs the engine directly. This is best for most users and recommended to start with.

```yaml
storage:
  dsn: "env:SCHEMABOT_DSN"

databases:
  mydb:
    type: mysql
    environments:
      staging:
        dsn: "env:STAGING_DSN"
      production:
        dsn: "file:/run/secrets/prod-dsn"
```

## gRPC Mode

SchemaBot delegates to remote services that implement the Tern proto. This is useful for distributed deployments where schema changes need to run in separate isolated environments.

```yaml
storage:
  dsn: "env:SCHEMABOT_DSN"

databases:
  payments:
    type: mysql
    environments:
      staging:
        target: "payments-staging"
        deployment: tenant1
      production:
        target: "payments-production"
        deployment: tenant1

tern_deployments:
  tenant1:
    staging: "tern-staging:9090"
    production: "tern-production:9090"
```

In gRPC mode, `target` is an opaque identifier understood by the remote Tern service. SchemaBot stores the resolved `target` and `deployment` on each plan, then reuses that stored route during apply. Callers do not send target or deployment in plan/apply requests.

The example above is a single SchemaBot deployment that owns both environments. In environment-isolated deployments, each SchemaBot config should contain only the targets for the environments that instance owns. Use `allowed_environments` to scope each instance.

## Environment Order

For clients, `schemabot.yaml` environments are strictly an opt-in mechanism. They control which environments a repository opts into; they do not control promotion order. SchemaBot enforces promotion order from server config:

```yaml
environment_order:
  - staging
  - production
```

If omitted, SchemaBot defaults to `staging` before `production`. The order of values in `schemabot.yaml` is ignored for apply gating, so these two repo configs are equivalent:

```yaml
environments:
  - staging
  - production
```

```yaml
environments:
  - production
  - staging
```

Both enable staging and production. When applying production, SchemaBot checks staging first because the server-owned `environment_order` says staging precedes production.

## Hybrid Mode

Both modes can be used simultaneously. Each database environment in the `databases` section chooses one route: local mode with `dsn`, or gRPC mode with `target` and `deployment`. This is useful when some databases are co-located with SchemaBot and others are in remote environments.

```yaml
storage:
  dsn: "env:SCHEMABOT_DSN"

databases:
  local-db:
    type: mysql
    environments:
      staging:
        dsn: "env:LOCAL_DB_DSN"
  remote-db:
    type: mysql
    environments:
      staging:
        target: "remote-db-staging"
        deployment: default

tern_deployments:
  default:
    staging: "tern-staging:9090"
    production: "tern-production:9090"
```

Routing is always server-side. A database environment with `dsn` uses local mode. A database environment with `target` and `deployment` uses gRPC mode through the matching `tern_deployments` endpoint. A database that is not listed in `databases` is not routable.

## Scheduler Workers

SchemaBot runs a background scheduler for apply work that needs server-side coordination. By default, four workers poll for claimable work so a server can make progress across independent databases and environments concurrently.

```yaml
scheduler_workers: 4
```

Increase `scheduler_workers` when one SchemaBot server should make scheduler progress across independent databases or environments concurrently. More workers help high-scale installations with many schema changes because each worker can claim and resume a different target during the same scheduler tick. The scheduler still excludes overlapping work for the same database and environment, so this improves concurrency across independent targets, not parallel execution against one target.

A scheduler claim means selecting one stale apply and refreshing its heartbeat in the same storage transaction. That heartbeat refresh is the worker's lease while it reloads state and resumes the apply.

## Repository Allowlist

By default, any repository with the GitHub App installed can use SchemaBot. Adding a `repos` section creates an allowlist — only listed repositories are permitted.

```yaml
# Local mode — repos as allowlist only
repos:
  myorg/payments-service: {}
  myorg/user-service: {}

databases:
  payments:
    type: mysql
    environments:
      staging:
        dsn: "env:STAGING_DSN"
```

```yaml
# gRPC mode — repos as allowlist, database targets as routing
repos:
  myorg/payments-service: {}
  myorg/user-service: {}

databases:
  payments:
    type: mysql
    environments:
      staging:
        target: "payments-staging"
        deployment: tenant1
  users:
    type: mysql
    environments:
      staging:
        target: "users-staging"
        deployment: tenant2

tern_deployments:
  tenant1:
    staging: "tern-staging:9090"
  tenant2:
    staging: "tern2-staging:9090"
```

When a webhook arrives from an unlisted repository:
- If the user invoked a SchemaBot command (e.g., `schemabot plan`), a PR comment explains the repo is not registered.
- Auto-plan events (PR open/sync) are ignored without a PR comment because the
  repository is outside this SchemaBot instance's ownership.

If `repos` is not configured or empty, all repositories are allowed.

## PR Checks Gate

By default, SchemaBot blocks `apply` and `apply-confirm` when non-SchemaBot PR checks are failing. This prevents applying schema changes on a PR with broken CI, linters, or security scans.

```yaml
# Block apply when PR checks are failing (default: true)
require_passing_checks: true
```

Apply is blocked in two cases: checks that have **failed** (`failure`, `error`, `timed_out`) and checks that are **still running** (`in_progress`, `queued`, `pending`). Each case shows a distinct message — failing checks prompt the user to fix them, while in-progress checks prompt the user to wait. Checks with conclusion `neutral` or `skipped` are ignored. SchemaBot's own checks (names starting with "SchemaBot") are always excluded.

When checks are failing, SchemaBot posts a comment listing the failing checks and instructs the user to fix them before retrying.

If the GitHub API is unreachable when checking statuses, the apply is blocked (fail-closed). SchemaBot posts a comment explaining the error and suggests retrying.

Set `require_passing_checks: false` to disable this gate.

## Multi-Environment Deployment

For organizations that need isolated infrastructure per environment (e.g., separate staging and production deployments with their own GitHub Apps and databases), SchemaBot supports scoping each instance to a subset of environments using `allowed_environments`.

### Staging Instance

```yaml
storage:
  dsn: "env:STAGING_SCHEMABOT_DSN"

allowed_environments:
  - staging
respond_to_unscoped: false  # production instance handles help and invalid commands

databases:
  payments:
    type: mysql
    environments:
      staging:
        dsn: "env:STAGING_PAYMENTS_DSN"

github:
  app-id: "env:STAGING_GITHUB_APP_ID"
  private-key: "file:/run/secrets/staging-github-key.pem"
  webhook-secret: "env:STAGING_WEBHOOK_SECRET"
```

### Production Instance

```yaml
storage:
  dsn: "env:PROD_SCHEMABOT_DSN"

allowed_environments:
  - production

databases:
  payments:
    type: mysql
    environments:
      production:
        dsn: "env:PROD_PAYMENTS_DSN"

github:
  app-id: "env:PROD_GITHUB_APP_ID"
  private-key: "file:/run/secrets/prod-github-key.pem"
  webhook-secret: "env:PROD_WEBHOOK_SECRET"
```

### Environment-local gRPC targets

For remote Tern deployments, keep the target registry environment-local too. The staging instance only needs staging target identifiers:

```yaml
storage:
  dsn: "env:STAGING_SCHEMABOT_DSN"

allowed_environments:
  - staging

databases:
  payments:
    type: mysql
    environments:
      staging:
        target: "payments-staging"
        deployment: primary

tern_deployments:
  primary:
    staging: "tern-staging:9090"
```

The production instance only needs production target identifiers:

```yaml
storage:
  dsn: "env:PROD_SCHEMABOT_DSN"

allowed_environments:
  - production

databases:
  payments:
    type: mysql
    environments:
      production:
        target: "payments-production"
        deployment: primary

tern_deployments:
  primary:
    production: "tern-production:9090"
```

Do not require the staging ConfigMap to contain production targets, or the production ConfigMap to contain staging targets. Each instance resolves only the environments it owns.

### How it works

- **Environment scoping:** When `allowed_environments` is set, the instance only processes commands targeting those environments. Commands for other environments (e.g., `schemabot apply -e production` sent to the staging instance) are accepted without a PR response by this deployment. A deployment that allows the requested environment must process its own webhook delivery.

- **Per-environment aggregate checks:** Each instance creates its own aggregate check run scoped to its environments (e.g., `SchemaBot (staging)`, `SchemaBot (production)`) instead of the default `SchemaBot` aggregate. Configure branch protection to require both aggregates.

- **Cross-instance environment verification:** Environment ordering (e.g., staging must succeed before production) works across instances. The production instance queries the GitHub Checks API for the staging instance's `SchemaBot (staging)` aggregate check to verify the prior environment completed successfully. GitHub check runs are the shared authority for cross-environment state; the production instance does not need staging target configuration to enforce the staging gate.

- **Separate GitHub Apps:** Each instance needs its own GitHub App installation. Both Apps must be installed on the same repositories and configured to receive the same webhook events. GitHub delivers webhooks to all installed Apps independently.

- **Unscoped command routing:** With two Apps on the same repo, commands not scoped to an environment (`help`, invalid commands) would get duplicate responses. Set `respond_to_unscoped: false` on the staging instance so only the production instance responds to these. Environment-scoped commands (`plan -e`, `apply -e`) are unaffected — they're already routed by `allowed_environments`.

- **Backwards compatible:** When `allowed_environments` is not set or empty, the instance handles all environments and creates a single `SchemaBot` aggregate check — the same behavior as a single-instance deployment.

### Auto-plan behavior

When a PR is opened or updated, each instance auto-plans only the environments it owns. The staging instance plans staging, the production instance plans production. Each posts its own plan comment and creates its own per-environment aggregate check.

If a PR changes managed schema files but a deployment's `allowed_environments`
does not overlap any environment declared by the matching `schemabot.yaml`, that
deployment publishes a failing aggregate check instead of treating the PR as
safe. This usually means the repo config and server config disagree. Align the
`schemabot.yaml` environments with the deployment's `allowed_environments`, then
run `schemabot plan -e <environment>` or push a new commit to refresh checks.

## Secret Resolution

DSN values support secret resolution prefixes:

| Prefix | Example | Description |
|---|---|---|
| `env:` | `env:STAGING_DSN` | Read from environment variable |
| `file:` | `file:/run/secrets/prod-dsn` | Read from file |
| `secretsmanager:` | `secretsmanager:prod/db-dsn` | Read from AWS Secrets Manager |

See [`pkg/secrets`](../pkg/secrets/) for implementation details.
