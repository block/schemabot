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

## Storage Connection

The `storage` section configures SchemaBot's internal database. You can provide the connection as a single DSN string or as individual fields.

### DSN (default)

A single connection string — the simplest option for most deployments:

```yaml
storage:
  dsn: "user:pass@tcp(localhost:3306)/schemabot?parseTime=true"
```

### Individual Fields

When credentials come from different sources (e.g., cloud-managed database passwords stored in a separate secret), use individual connection fields instead:

```yaml
storage:
  host: "file:/secrets/db/host"
  port: "file:/secrets/db/port"
  database: "file:/secrets/db/database"
  username: "file:/secrets/db/username"
  password: "file:/secrets/db/password"
```

Each field supports secret resolution prefixes (see below). SchemaBot assembles the DSN at startup. Port defaults to 3306 if not specified. This pattern works well with Kubernetes External Secrets Operator, which can sync multiple secrets into a single mounted volume.

The `dsn` field and individual fields are mutually exclusive — set one or the other, not both.

If `storage` is not configured at all, SchemaBot falls back to the `MYSQL_DSN` environment variable.

## Secret Resolution

All config values that accept secret references support these prefixes:

| Prefix | Example | Description |
|---|---|---|
| `env:` | `env:STAGING_DSN` | Read from environment variable |
| `file:` | `file:/run/secrets/prod-dsn` | Read from file |
| `secretsmanager:` | `secretsmanager:prod/db-dsn` | Read from AWS Secrets Manager |

### AWS Secrets Manager Authentication

The `secretsmanager:` resolver uses the [AWS SDK default credential chain](https://docs.aws.amazon.com/sdkref/latest/guide/standardized-credentials.html). It does not require any SchemaBot-specific configuration — it uses whatever AWS credentials are available to the process:

1. **Environment variables** (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) — useful for local development
2. **Shared credentials file** (`~/.aws/credentials`) — useful for local development with named profiles
3. **ECS/App Runner task role** — automatic when running on AWS compute with an attached IAM role
4. **EKS IAM Roles for Service Accounts (IRSA)** — automatic when the pod's service account is annotated with an IAM role

The IAM role or credentials must have `secretsmanager:GetSecretValue` permission on the secrets being referenced. No additional SchemaBot configuration is needed beyond ensuring the runtime environment has valid AWS credentials.

See [`pkg/secrets`](../pkg/secrets/) for implementation details.
