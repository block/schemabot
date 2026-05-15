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

tern_deployments:
  tenant1:
    staging: "tern-staging:9090"
    production: "tern-production:9090"
  tenant2:
    production: "tern1-production:9090"
```

## Hybrid Mode

Both modes can be used simultaneously. Databases configured in the `databases` section use local (direct) connections, while everything else routes through `tern_deployments`. This is useful when some databases are co-located with SchemaBot and others are in remote environments.

```yaml
storage:
  dsn: "env:SCHEMABOT_DSN"

# Direct connections for co-located databases
databases:
  local-db:
    type: mysql
    environments:
      staging:
        dsn: "env:LOCAL_DB_DSN"

# Remote Tern services for databases in other environments
tern_deployments:
  default:
    staging: "tern-staging:9090"
    production: "tern-production:9090"
```

Routing is automatic — when a plan or apply request arrives, SchemaBot checks the `databases` config first. If the database name matches, it uses a direct connection. Otherwise, it routes to the selected Tern deployment via gRPC (defaulting to the `default` deployment key, or the repo-specific deployment configured via `repos.*.default_tern_deployment`).

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
# gRPC mode — repos as allowlist + routing
repos:
  myorg/payments-service:
    default_tern_deployment: tenant1
  myorg/user-service:
    default_tern_deployment: tenant2

tern_deployments:
  tenant1:
    staging: "tern-staging:9090"
  tenant2:
    staging: "tern2-staging:9090"
```

When a webhook arrives from an unlisted repository:
- If the user invoked a SchemaBot command (e.g., `schemabot plan`), a PR comment explains the repo is not registered.
- Auto-plan events (PR open/sync) are silently ignored.

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

### How it works

- **Environment scoping:** When `allowed_environments` is set, the instance only processes commands targeting those environments. Commands for other environments (e.g., `schemabot apply -e production` sent to the staging instance) are silently ignored — the other instance handles them from its own webhook delivery.

- **Per-environment aggregate checks:** Each instance creates its own aggregate check run scoped to its environments (e.g., `SchemaBot (staging)`, `SchemaBot (production)`) instead of the default `SchemaBot` aggregate. Configure branch protection to require both aggregates.

- **Cross-instance environment verification:** Environment ordering (e.g., staging must succeed before production) works across instances. The production instance queries the GitHub Checks API for the staging instance's `SchemaBot (staging)` aggregate check to verify the prior environment completed successfully.

- **Separate GitHub Apps:** Each instance needs its own GitHub App installation. Both Apps must be installed on the same repositories and configured to receive the same webhook events. GitHub delivers webhooks to all installed Apps independently.

- **Unscoped command routing:** With two Apps on the same repo, commands not scoped to an environment (`help`, invalid commands) would get duplicate responses. Set `respond_to_unscoped: false` on the staging instance so only the production instance responds to these. Environment-scoped commands (`plan -e`, `apply -e`) are unaffected — they're already routed by `allowed_environments`.

- **Backwards compatible:** When `allowed_environments` is not set or empty, the instance handles all environments and creates a single `SchemaBot` aggregate check — the same behavior as a single-instance deployment.

### Auto-plan behavior

When a PR is opened or updated, each instance auto-plans only the environments it owns. The staging instance plans staging, the production instance plans production. Each posts its own plan comment and creates its own per-environment aggregate check.

## Secret Resolution

DSN values support secret resolution prefixes:

| Prefix | Example | Description |
|---|---|---|
| `env:` | `env:STAGING_DSN` | Read from environment variable |
| `file:` | `file:/run/secrets/prod-dsn` | Read from file |
| `secretsmanager:` | `secretsmanager:prod/db-dsn` | Read from AWS Secrets Manager |

See [`pkg/secrets`](../pkg/secrets/) for implementation details.
