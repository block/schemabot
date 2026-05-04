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

## Authentication

SchemaBot supports OIDC-based authentication for API endpoints. When enabled, read endpoints require a valid JWT Bearer token, and write endpoints (plan, apply, cutover, stop, start) additionally require membership in a configured admin group.

```yaml
auth:
  type: oidc
  issuer: "https://your-oidc-provider.example.com"
  audience: "schemabot"       # optional — skip audience validation if empty
  admin_group: "schema-admins"
  groups_claim: "groups"      # optional — defaults to "groups"
```

When `auth.type` is empty or `"none"`, authentication is disabled and all API requests are allowed (the default for backwards compatibility).

### Testing OIDC Locally with Dex

The local development stack includes an optional [Dex](https://dexidp.io/) OIDC provider for testing authentication end-to-end without external dependencies.

**Start the stack with OIDC:**

```bash
docker compose --profile oidc up
```

This starts a Dex instance alongside the standard services, plus a `schemabot-oidc` service that validates tokens against Dex. The OIDC-enabled SchemaBot runs on port `13380` (the standard SchemaBot on `13370` remains available without auth).

**Dex endpoints (from host):**

| Endpoint | URL |
|---|---|
| Discovery | `http://localhost:5556/dex/.well-known/openid-configuration` |
| JWKS | `http://localhost:5556/dex/keys` |
| Authorization | `http://localhost:5556/dex/auth` |
| Token | `http://localhost:5556/dex/token` |

**Test users** (defined in `deploy/local/config/dex.yaml`):

| Email | Password | Groups | Access |
|---|---|---|---|
| `admin@example.com` | `password` | `admins` | Read + Write |
| `viewer@example.com` | `password` | `viewers` | Read only |

**Get a test token:**

```bash
# Interactive browser flow — opens Dex login page, returns a JWT.
TOKEN=$(./scripts/get-oidc-token.sh)

# Verify auth is working on read endpoints:
curl -H "Authorization: Bearer $TOKEN" http://localhost:13380/api/databases

# Verify write access (admin user):
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  http://localhost:13380/api/databases/testapp/environments/staging/plan

# Verify unauthenticated requests are rejected:
curl http://localhost:13380/api/databases
# => {"error":"invalid or missing authentication token"}
```

The Dex configuration is at `deploy/local/config/dex.yaml` and the OIDC-enabled SchemaBot config is at `deploy/local/config/local-oidc.yaml`.

## Secret Resolution

DSN values support secret resolution prefixes:

| Prefix | Example | Description |
|---|---|---|
| `env:` | `env:STAGING_DSN` | Read from environment variable |
| `file:` | `file:/run/secrets/prod-dsn` | Read from file |
| `secretsmanager:` | `secretsmanager:prod/db-dsn` | Read from AWS Secrets Manager |

See [`pkg/secrets`](../pkg/secrets/) for implementation details.
