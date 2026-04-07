# GitHub App Setup

This guide walks through creating a GitHub App, configuring SchemaBot to receive webhooks, and adding the `schemabot.yaml` config to your repositories.

## 1. Create a GitHub App

Go to **Settings > Developer settings > GitHub Apps > New GitHub App** ([direct link](https://github.com/settings/apps/new)).

### Basic Information

| Field | Value |
|-------|-------|
| **GitHub App name** | `SchemaBot` (or your preferred name) |
| **Homepage URL** | Your SchemaBot deployment URL |
| **Description** | Declarative schema change orchestration via PR comments |

### Webhook

| Field | Value |
|-------|-------|
| **Active** | Checked |
| **Webhook URL** | `https://your-domain.com/webhook` |
| **Webhook secret** | Generate a random secret (save it for later) |

Generate a webhook secret:

```bash
openssl rand -hex 32
```

### Permissions

Under **Repository permissions**, grant:

| Permission | Access | Used For |
|-----------|--------|----------|
| **Checks** | Read & Write | Create check runs on PRs showing plan results |
| **Contents** | Read | Read `schemabot.yaml` config and schema files from repos |
| **Issues** | Read & Write | Post PR comments and add reactions |
| **Metadata** | Read | Required (granted automatically) |
| **Pull requests** | Read & Write | Fetch PR info (head SHA, changed files) |

No organization or account permissions are needed.

### Subscribe to Events

| Event | Purpose |
|-------|---------|
| **Issue comment** | Receive `schemabot plan`, `schemabot help`, etc. from PR comments |

Future phases will also use **Check run** (action buttons) and **Pull request** (auto-plan on open/sync).

### Where Can This GitHub App Be Installed?

Choose **Only on this account** for private use, or **Any account** if you plan to share the app.

### Create the App

Click **Create GitHub App**. Note the **App ID** shown on the next page.

## 2. Generate a Private Key

On the app settings page, scroll to **Private keys** and click **Generate a private key**.

A `.pem` file will be downloaded. Store it securely — this is your app's authentication credential.

## 3. Install the App

Go to your app's settings page, click **Install App** in the sidebar, and install it on your organization or account.

You can restrict it to specific repositories or grant access to all repositories.

## 4. Configure SchemaBot

Add the `github:` section to your SchemaBot server config (`config.yaml`):

```yaml
storage:
  dsn: "env:SCHEMABOT_DSN"

github:
  app_id: "123456"                                 # From step 1
  private_key: "file:/path/to/private-key.pem"    # PEM file
  webhook_secret: "env:GITHUB_WEBHOOK_SECRET"      # From step 1

databases:
  mydb:
    type: mysql
    environments:
      staging:
        dsn: "env:STAGING_DSN"
      production:
        dsn: "env:PRODUCTION_DSN"
```

The `private_key` and `webhook_secret` fields support secret references — the same format used for DSNs:

| Format | Example |
|--------|---------|
| Direct value | `"my-secret"` |
| Environment variable | `"env:GITHUB_WEBHOOK_SECRET"` |
| File | `"file:/run/secrets/github-key.pem"` |
| AWS Secrets Manager | `"secretsmanager:my-app/github#private_key"` |

For AWS deployments, see [deploy/aws/](../deploy/aws/) which stores credentials in Secrets Manager.

## 5. Start SchemaBot

```bash
schemabot serve
```

You should see:

```
{"level":"INFO","msg":"GitHub webhook endpoint registered"}
{"level":"INFO","msg":"starting server","port":"8080"}
```

## 6. Add `schemabot.yaml` Config to Your Repository

Create a `schemabot.yaml` file in the directory containing your schema SQL files:

```
my-repo/
  schema/
    schemabot.yaml      <-- config file
    users.sql
    orders.sql
    products.sql
```

```yaml
database: mydb
type: mysql
environments:
  - staging
  - production
```

| Field | Required | Description |
|-------|----------|-------------|
| `database` | Yes | Must match a database name in your SchemaBot server config |
| `type` | Yes | `"mysql"` or `"vitess"` |
| `environments` | No | Defaults to `["staging"]`. Valid values: `"staging"`, `"production"` |

### Schema File Layout

**MySQL** (flat structure):

```
schema/
  schemabot.yaml
  users.sql
  orders.sql
```

**Vitess** (keyspace subdirectories):

```
schema/
  schemabot.yaml
  commerce/
    users.sql
    orders.sql
    vschema.json
  lookup/
    lookup_table.sql
    vschema.json
```

Each `.sql` file should contain a single `CREATE TABLE` statement using the canonical format that matches `SHOW CREATE TABLE` output:

```sql
CREATE TABLE `users` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `email` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `email` (`email`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
```

## 7. Test It

Open a PR that modifies a schema file, then comment:

```
schemabot plan -e staging
```

SchemaBot will:
1. React with :eyes: to acknowledge the command
2. Fetch the `schemabot.yaml` config and schema files from the PR branch
3. Diff the desired schema against the live database
4. Post a comment with the DDL plan
5. Create a GitHub Check Run showing the result

Other commands:

```
schemabot help                  # Show command reference
schemabot plan                  # Plan for all configured environments
schemabot plan -d mydb          # Plan for a specific database (multi-db repos)
```

## Environment Variables Reference

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SCHEMABOT_CONFIG_FILE` | Yes | — | Path to server config YAML |
| `GITHUB_APP_ID` | No | — | Fallback if `github.app_id` is not set in config |
| `PORT` | No | `8080` | HTTP server port |
| `LOG_LEVEL` | No | `info` | `debug`, `info`, `warn`, `error` |

GitHub credentials (`private_key`, `webhook_secret`) are configured in the YAML config file using secret references, not environment variables. This keeps all configuration in one place and supports any secret backend.

## Webhook Signature Validation

If `webhook_secret` is set in the config, SchemaBot validates the `X-Hub-Signature-256` header on every webhook request using HMAC-SHA256. Requests with invalid or missing signatures are rejected with HTTP 401.

If the secret is not set, signature validation is skipped (useful for local development).

## Troubleshooting

**Webhook not receiving events**: Check that the webhook URL is reachable from GitHub. Use the **Recent Deliveries** tab on your GitHub App's settings page to see delivery attempts and response codes.

**401 Unauthorized on webhook**: The webhook secret in your GitHub App settings doesn't match `GITHUB_WEBHOOK_SECRET`. Regenerate and update both.

**"No schemabot.yaml config found" comment**: SchemaBot couldn't find a `schemabot.yaml` file in the PR's changed file directories. Make sure the file exists and is committed to the PR branch.

**"database not found" comment**: The `database` field in `schemabot.yaml` doesn't match any database in your SchemaBot server config. The names must match exactly.
