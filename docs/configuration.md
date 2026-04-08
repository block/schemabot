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

## Secret Resolution

DSN values support secret resolution prefixes:

| Prefix | Example | Description |
|---|---|---|
| `env:` | `env:STAGING_DSN` | Read from environment variable |
| `file:` | `file:/run/secrets/prod-dsn` | Read from file |
| `secretsmanager:` | `secretsmanager:prod/db-dsn` | Read from AWS Secrets Manager |

See [`pkg/secrets`](../pkg/secrets/) for implementation details.
