# Configuration

- [Config Files](#config-files)
- [Local](#local)
- [Server Deployment](#server-deployment)
- [Profiles](#profiles)
- [Secret Resolution](#secret-resolution)

## Config Files

SchemaBot uses different config files depending on how you run it:

| File | Location | Purpose | Used by |
|------|----------|---------|---------|
| `schemabot.yaml` | In your schema directory | Declares the database name and type | CLI (plan, apply, pull) |
| `~/.schemabot/config.yaml` | Home directory | CLI profiles and local database connections | CLI (all commands) |
| Server config (`SCHEMABOT_CONFIG_FILE`) | Anywhere | Storage DSN, database configs, Tern endpoints | `schemabot serve` |

`schemabot.yaml` — declares which database your schema files belong to:

```yaml
database: mydb
type: mysql
```

`~/.schemabot/config.yaml` — tells the CLI how to connect:

```yaml
default_profile: local

profiles:
  staging:
    endpoint: http://localhost:8080

local:
  mydb:
    type: mysql
    environments:
      staging:
        dsn: "root@tcp(localhost:3306)/mydb"
```

Server config (`SCHEMABOT_CONFIG_FILE`) — only used when deploying SchemaBot as a server:

```yaml
storage:
  dsn: "env:SCHEMABOT_DSN"
databases:
  mydb:
    type: mysql
    environments:
      staging:
        dsn: "env:STAGING_DSN"
```

The first two are for CLI usage. The server config is separate — see [Server Deployment](#server-deployment) for details.

## Local

When using SchemaBot without a server deployment, the CLI auto-starts a lightweight background server on your machine. Database connection details are stored in the `local` section of the config. The `pull` command sets this up automatically.

```yaml
# ~/.schemabot/config.yaml
local:
  mydb:
    type: mysql
    environments:
      staging:
        dsn: "root@tcp(localhost:3306)/mydb"
      production:
        dsn: "env:PRODUCTION_DSN"
  mypsdb:
    type: vitess
    environments:
      production:
        organization: myorg
        token: "env:PLANETSCALE_SERVICE_TOKEN"
```

### Adding a database

```bash
# Pull creates the local config entry automatically
schemabot pull --dsn "root@tcp(localhost:3306)/mydb" -e staging -o ./schema
```

This writes the schema files and adds `mydb/staging` to `~/.schemabot/config.yaml`.

### Managing the background server

```bash
schemabot local status   # show server state, PID, port
schemabot local stop     # stop the background server
schemabot local reset    # stop server and drop _schemabot database
```

## Server Deployment

For GitHub PR integration, team coordination, or managing many databases, deploy SchemaBot as a server.

The server loads config from a YAML file. Set `SCHEMABOT_CONFIG_FILE` to the path:

```bash
export SCHEMABOT_CONFIG_FILE=/etc/schemabot/config.yaml
schemabot serve
```

### Single-process (recommended for most deployments)

The server runs the schema change engine directly:

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

### Distributed (gRPC)

For deployments where schema changes need to run in separate isolated environments, SchemaBot delegates to remote services that implement the Tern proto:

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

## Profiles

CLI profiles let you switch between local mode and server deployments. Use `--profile` to select one, or set a default.

```yaml
# ~/.schemabot/config.yaml
default_profile: local

profiles:
  staging:
    endpoint: http://localhost:8080
  production:
    endpoint: https://schemabot.example.com
```

```bash
schemabot plan -s ./schema -e staging                    # uses default profile
schemabot plan -s ./schema -e staging --profile staging   # explicit server profile
schemabot plan -s ./schema -e staging --profile local     # force local mode
```

`local` is a reserved profile name that triggers local mode. Set `default_profile: local` to make it the default.

```bash
schemabot configure   # interactive profile setup
```

## Secret Resolution

DSN and credential values support secret resolution prefixes:

| Prefix | Example | Description |
|---|---|---|
| `env:` | `env:STAGING_DSN` | Read from environment variable |
| `file:` | `file:/run/secrets/prod-dsn` | Read from file |
| `secretsmanager:` | `secretsmanager:prod/db-dsn` | Read from AWS Secrets Manager |

See [`pkg/secrets`](../pkg/secrets/) for implementation details.
