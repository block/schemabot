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

```text
GitHub sends POST /webhook
  |
  +-- Validate HMAC-SHA256 signature (X-Hub-Signature-256 header)
  |
  +-- issue_comment.created on a PR
  |     |
  |     +-- Ignore bots and commands for environments this deployment is not
  |     |   allowed to process
  |     +-- Parse command from comment body
  |     +-- Add an "eyes" reaction after this deployment accepts the command
  |     |
  |     +-- plan [-e <env>]
  |     |     +-- Discover schemabot.yaml config
  |     |     +-- Fetch schema files
  |     |     +-- Execute plan through Tern
  |     |     +-- Store per-database check state
  |     |     +-- Post plan comment and sync the aggregate Check Run
  |     |
  |     +-- apply -e <env>
  |     |     +-- Re-plan, enforce gates, acquire lock, and post confirmation
  |     |
  |     +-- apply-confirm -e <env>
  |     |     +-- Revalidate lock/plan, start apply, store apply_id ownership,
  |     |         and post progress updates
  |     |
  |     +-- rollback <apply-id> -e <env>
  |     |     +-- Generate rollback plan and acquire lock
  |     |
  |     +-- rollback-confirm -e <env>
  |     |     +-- Start rollback apply and keep the required check blocked
  |
  +-- pull_request.opened / synchronize / reopened
  |     |
  |     +-- Discover changed managed schema configs
  |     +-- Auto-plan each affected database
  |     +-- Clean up stale check state for databases no longer touched
  |     +-- Publish aggregate Check Runs on the current PR head SHA
  |
  +-- pull_request.closed
  |     |
  |     +-- Release locks held by the PR and delete stored check state
  |
  +-- check_run.rerequested
  |     |
  |     +-- Re-plan the PR head when a human clicks "Re-run" on a
  |         SchemaBot check
  |
  +-- check_suite.requested
        |
        +-- Enqueue a grace-deferred recovery pass that synthesizes the
            auto-plan delivery for open PR heads whose pull_request
            delivery was lost
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
```

Environment availability and order are resolved from server config for the
configured database.

Three discovery strategies, tried in order:

1. **`-d` flag specified** → `FindConfigByDatabaseName()`: Tree API scan for all `schemabot.yaml` files, match by `database` field (case-insensitive).

2. **Changed schema files in PR** → `FindConfigForPR()`: List PR files, keep schema files (`.sql`, `vschema.json`), changed `schemabot.yaml` files, and environment or namespace symlinks, and search parent directories for `schemabot.yaml`.

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
  ├─ Resolve database + environment through server config
  ├─ Get or create Tern client (local or gRPC)
  ├─ Call client.Plan() → DDL diff against live database
  ├─ Store plan and route metadata in storage.Plans (idempotent)
  └─ Return PlanResponse with tables, lint warnings, errors
```

## Check Runs

Check Runs are GitHub pre-merge status checks that appear on the PR's "Checks" tab. SchemaBot publishes an aggregate check (`SchemaBot`, or `SchemaBot (<environment>)` when `allowed_environments` is configured) and stores per-database state internally. Deployments can override the visible base name with `github.check-name`.

After a plan, the aggregate reflects the rolled-up internal state:

| Plan Result | Conclusion | Blocks Merge? |
|---|---|---|
| Changes detected | `action_required` | Yes (if required) |
| No changes | `success` | No |
| Errors | `failure` | Yes (if required) |

See [Check Runs](../../docs/check-runs.md) for the current lifecycle, `apply_id` ownership model, stale check reconciliation, and branch protection setup.

## Command Syntax

```text
schemabot <command> [args] [-e <environment>] [-d <database>] [--defer-cutover] [--allow-unsafe] [--skip-revert] [--enable-revert] [--override "<reason>"]
```

| Command | `-e` required | Description |
|---|---|---|
| `plan` | No (runs all envs) | Preview schema changes |
| `apply` | Yes | Lock and apply after safety rechecks |
| `apply-confirm` | Yes | Confirm a downgraded locked plan |
| `unlock` | No | Discard locked plans |
| `cancel` | Yes | Cancel in-progress apply |
| `cutover` | Yes | Complete deferred cutover |
| `revert` | Yes | Revert (within revert window) |
| `skip-revert` | Yes | Make changes permanent |
| `rollback <apply-id>` | Yes | Generate rollback plan |
| `rollback-confirm` | Yes | Execute rollback |
| `freeze approve <ovr-id>` | No (the request names it) | Approve a freeze override request from the pull request it was made on |
| `fix-lint` | No | Auto-fix lint warnings |
| `help` | No | Show command reference |

PR comments for `stop`, `cutover`, `revert`, and `skip-revert` are parsed so
SchemaBot can return a clear "not yet available" response. Use the CLI for
those operator controls today.

### Freeze override

`apply`, `apply-confirm`, `start` and `cutover` take `--override "<reason>"` (also
`--override=<reason>` and single quotes). Quotes group only this flag's value;
without them the reason is the next word. When a freeze refuses the command,
SchemaBot records an override request for the commenter and the pull request's
author, and the comment names it. The reason is trimmed, must not be blank, and
is at most 500 characters. `apply-confirm` takes it because an apply that must
discard an unfinished copy stops for that confirmation before the freeze gate
runs. A refused `apply-confirm` releases the pending confirmation, so the
comment tells the commenter to run `apply` again and then `apply-confirm`.
`rollback` and `revert` do not take the flag, and rollbacks skip the freeze
gate.

A request from a pull request is recorded as `github:<login>` for both the
commenter and the author, and only a pull request approves it: the API route
refuses it, since a GitHub login cannot be matched to an API subject. The
command that approves it is `schemabot freeze approve <ovr-id>`, commented on
that pull request by a member of a `freeze.github_teams` team
(`handleFreezeApproveCommand`). It takes no `-e` and does not plan or check
anything; the delivery's signature is the only authorization it needs, since
approvers are their own list and not the admins and operators of PR commands.
`api.Service.ApprovePullRequestOverride` decides it: the ID's shape, the team
(read through the repository's installation, refusing when it cannot be read),
that the request came from this pull request, and storage's refusals of the
requester, the author, an approved request and an expired one. Approval is
recorded as `github:<login>`. On an aggregate repo the request lives in one
deployment, so the others stay silent on an unknown ID; the refusal therefore
names the approve command with the deployment's `--tenant`, so a pasted
command is always answered.

A refusal is its own comment (`templates.RenderChangeGateRefusal`). It lists
each freeze with how long it holds (`ui.FreezeUntil`, the phrase the API and
the CLI use too) and its reason. Without `--override`, a command that takes the
flag is told how to ask for one and which teams (`freeze.github_teams`) may
approve it; with no team configured, nobody can approve a pull request's
request, so the comment offers none. With a recorded request, it shows the
reason the approver sees, @-mentions the approver teams, gives the approve
command, and says the commenter then runs the command again, since an approved
override admits only the person who asked. Freeze and override reasons are a
person's text, written so a mention in them notifies nobody
(`templates.markdownReason`).

## Error Handling

Schema request errors are mapped to specific GitHub comment templates:

| Error | Template | User Action |
|---|---|---|
| `ErrNoConfig` | "No Schema Changes Detected" | Add `schemabot.yaml` file or use `-d` |
| `ErrInvalidConfig` | "No Valid Configuration Found" | Fix `schemabot.yaml` fields |
| `ErrMultipleConfigs` | "Multiple Databases Detected" | Use `-d <database>` |
| `DatabaseNotFoundError` | "Database Not Found" | Check database name; on a repository too large to search in full, the comment lists the configured directories that were searched |
| `DatabaseNotFoundError` (`RepositoryNotAccepted`) | "Database Not Available to This Repository" | Add the repository to the database's `allowed_repos` if it should manage the database |
| `api.DatabaseNotConfiguredError` | "Database Not Configured" | Ask an operator to configure the database on the server |
| `ErrConfigDiscoveryTruncated` | "Repository Too Large to Search" | Ask an operator to bound the database's config location with `allowed_dirs` |
| Other | "Plan Failed" | Check error details |

## Not Implemented In Webhooks

- PR-comment control operations for `stop`, `cutover`, `revert`, and
  `skip-revert`; use the CLI for those operator actions.
