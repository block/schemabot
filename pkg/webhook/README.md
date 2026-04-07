# GitHub Webhook Integration

SchemaBot processes GitHub PR comments (`schemabot plan -e staging`) and creates Check Runs. This document describes the architecture of the webhook integration.

## Package Layout

```
pkg/github/                    GitHub API client + repo content fetching
  client.go                    GitHub App auth (ghinstallation) + API methods
  config.go                    schemabot.yaml config discovery and parsing
  schema.go                    Schema file fetching from PRs → ternv1.SchemaFiles

pkg/webhook/                   Webhook event handling
  handler.go                   HTTP handler, HMAC-SHA256 signature validation, event routing
  commands.go                  Regex-based command parser
  issue_comment.go             PR comment event handler
  plan.go                      Plan command execution + error routing
  check_runs.go                GitHub Check Run creation/update
  templates/                   Markdown templates for PR comments
    help.go                    Command reference
    plan.go                    Plan result rendering
    errors.go                  Config/schema error messages
```

## Request Flow

```
GitHub sends POST /webhook
  │
  ├─ Validate HMAC-SHA256 signature (X-Hub-Signature-256 header)
  ├─ Route by X-GitHub-Event header
  │
  └─ issue_comment event
       │
       ├─ Filter: only "created" actions on PRs, ignore bots
       ├─ Parse command from comment body (CommandParser)
       ├─ Add "eyes" reaction for instant acknowledgment
       │
       └─ Route by command:
            │
            ├─ "plan -e staging"
            │    ├─ Discover schemabot.yaml config
            │    ├─ Fetch schema files (Tree API + parallel blobs)
            │    ├─ Call service.ExecutePlan() → Tern → Spirit/PlanetScale
            │    ├─ Post plan comment on PR
            │    └─ Create GitHub Check Run
            │
            ├─ "plan" (no -e)
            │    └─ Run plan for each environment in config
            │
            ├─ "help"
            │    └─ Post help comment
            │
            └─ Other commands (apply, cancel, etc.)
                 └─ Phase 2 — acknowledged with CLI fallback message
```

## GitHub App Authentication

```
Server startup (serve.go)
  │
  ├─ Read GITHUB_APP_ID, GITHUB_PRIVATE_KEY, GITHUB_WEBHOOK_SECRET env vars
  ├─ Create github.Client(appID, privateKey)
  └─ Register webhook.Handler at POST /webhook
```

Per-request auth uses `ghinstallation/v2`:

```go
// Client creates per-installation clients from the webhook payload's installation_id.
// ghinstallation handles JWT signing, token exchange, caching, and refresh automatically.
client.ForInstallation(installationID) → *InstallationClient
```

The installation ID comes from the webhook payload (`installation.id`), so each webhook event uses the correct GitHub App installation token for that organization.

## Config Discovery

The `schemabot.yaml` config file marks a directory as containing schema files:

```yaml
database: payments_db
type: mysql
environments:
  - staging
  - production
```

Three discovery strategies, tried in order:

1. **`-d` flag specified** → `FindConfigByDatabaseName()`: Tree API scan for all `schemabot.yaml` files, match by `database` field (case-insensitive).

2. **Changed schema files in PR** → `FindConfigForPR()`: List PR files, filter to `.sql`/`vschema.json`, search parent directories for `schemabot.yaml`.

3. **Fallback** → `FindConfigInRepo()`: Full Tree API scan. Returns single config or `ErrMultipleConfigs` if ambiguous.

## Schema File Fetching

Uses the Tree API for efficiency (single API call to list all files):

```
FetchGitTree(headSHA)          1 API call — entire repo tree
  │
  ├─ Filter by path prefix (schemaPath/) and extension (.sql, vschema.json)
  ├─ MySQL:  flat structure    schema/*.sql
  └─ Vitess: keyspace subdirs  schema/keyspace/*.sql + vschema.json
  │
  └─ Parallel blob fetching    N API calls (concurrency limit: 10)
```

Files are grouped into `map[string]*ternv1.SchemaFiles` — the format expected by `ExecutePlan`.

## Plan Execution

The webhook handler calls `service.ExecutePlan()` — the same code path as the HTTP `POST /api/plan` endpoint:

```
ExecutePlan(ctx, PlanRequest)
  ├─ Resolve Tern deployment from database name
  ├─ Get or create Tern client (local or gRPC)
  ├─ Call client.Plan() → DDL diff against live database
  ├─ Store plan in storage.Plans (idempotent)
  └─ Return PlanResponse with tables, lint warnings, errors
```

## Check Runs

Check Runs are GitHub pre-merge status checks that appear on the PR's "Checks" tab. They block merging when configured as required status checks, ensuring schema changes are applied before the PR lands.

After a plan, a Check Run is created:

| Plan Result | Conclusion | Blocks Merge? |
|---|---|---|
| Changes detected | `action_required` | Yes (if required) |
| No changes | `success` | No |
| Errors | `failure` | Yes (if required) |

The check run output text contains the full DDL statements, truncated at 65,530 characters (GitHub API limit).

## Command Syntax

```
schemabot <command> [-e <environment>] [-d <database>] [--enable-revert] [--defer-cutover]
```

| Command | `-e` required | Description |
|---|---|---|
| `plan` | No (runs all envs) | Preview schema changes |
| `apply` | Yes | Lock plan for deployment |
| `apply-confirm` | Yes | Execute locked plan |
| `unlock` | No | Discard locked plans |
| `cancel` | Yes | Cancel in-progress apply |
| `cutover` | Yes | Complete deferred cutover |
| `revert` | Yes | Revert (within revert window) |
| `skip-revert` | Yes | Make changes permanent |
| `rollback` | No (runs all envs) | Generate rollback plan |
| `rollback-confirm` | Yes | Execute rollback |
| `fix-lint` | No | Auto-fix lint warnings |
| `help` | No | Show command reference |

## Error Handling

Schema request errors are mapped to specific GitHub comment templates:

| Error | Template | User Action |
|---|---|---|
| `ErrNoConfig` | "No Schema Changes Detected" | Add `schemabot.yaml` file or use `-d` |
| `ErrInvalidConfig` | "No Valid Configuration Found" | Fix `schemabot.yaml` fields |
| `ErrMultipleConfigs` | "Multiple Databases Detected" | Use `-d <database>` |
| `DatabaseNotFoundError` | "Database Not Found" | Check database name |
| Other | "Plan Failed" | Check error details |

## Phase 2 (Future)

- **Apply via PR comments**: `apply`, `apply-confirm`, lock management
- **Check Run action buttons**: "Apply Changes" / "Unlock" button clicks
- **Auto-plan on PR open/sync**: Automatic plan when PRs are opened or updated
- **Apply progress**: Live comment editing with progress updates
- **Lock conflict detection**: Cross-PR blocking notifications
