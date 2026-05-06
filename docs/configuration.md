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

## Secret Resolution

DSN values support secret resolution prefixes:

| Prefix | Example | Description |
|---|---|---|
| `env:` | `env:STAGING_DSN` | Read from environment variable |
| `file:` | `file:/run/secrets/prod-dsn` | Read from file |
| `secretsmanager:` | `secretsmanager:prod/db-dsn` | Read from AWS Secrets Manager |

See [`pkg/secrets`](../pkg/secrets/) for implementation details.
