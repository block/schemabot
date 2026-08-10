# CLI Client

The `schemabot` binary is both the server and the client: `schemabot serve` runs the server, and every other command is a client call against a running server's HTTP API. This page covers the client side — how the CLI finds a server, authenticates, what the everyday commands look like, and how to wrap the CLI for your own environment.

## Profiles and the config file

Client settings live in `~/.schemabot/config.yaml`. Each **profile** names a server endpoint (and, on auth-enabled servers, a cached token):

```yaml
default_profile: staging
profiles:
  staging:
    endpoint: https://schemabot.staging.example.com
  production:
    endpoint: https://schemabot.example.com
```

Set up a profile interactively with `schemabot configure` (add `--profile <name>` for a non-default profile), and inspect what the CLI currently resolves with `schemabot configure show`.

When any profile holds a token, the config file must not be group- or world-readable (`chmod 600 ~/.schemabot/config.yaml`); the CLI refuses to load it otherwise.

## Resolution precedence

Each setting resolves independently, first match wins:

| Setting | Order |
|---|---|
| Profile | `--profile` flag → `SCHEMABOT_PROFILE` → `default_profile` in the config → `default` |
| Endpoint | `--endpoint` flag → `SCHEMABOT_ENDPOINT` → the resolved profile's `endpoint` |
| Token | `--token` flag → `SCHEMABOT_TOKEN` → the resolved profile's cached token |

## Authentication

Three access models are common; pick the one that matches how the server is deployed.

**OIDC login (built-in).** Against a server running with auth enabled, log in once per profile:

```bash
schemabot login                # opens a browser (OIDC auth-code + PKCE)
schemabot login --no-browser   # print the URL instead (headless/remote hosts)
```

The profile needs an `oidc` block naming the public client the server accepts:

```yaml
profiles:
  production:
    endpoint: https://schemabot.example.com
    oidc:
      issuer: https://idp.example.com
      client_id: schemabot-cli
```

`login` caches the ID token and refresh token on the profile; subsequent commands attach the token automatically and refresh it when it nears expiry. `--issuer`, `--client-id`, and `--redirect-port` override the profile's `oidc` block per invocation.

**An authenticating proxy in front of the server.** Instead of tokens in the CLI, place the server behind an identity-aware proxy or ingress that authenticates at the edge (forward auth against your SSO). The profile's endpoint points at the proxy and the CLI sends no token at all; who may reach the server — and at what tier — is enforced before requests arrive. This is how Block's main deployment works: `sq schemabot` talks to an employee ingress that authenticates the operator, with access tiers enforced server-side.

**Network isolation.** For a cluster with no ingress at all, reach the server over a `kubectl port-forward` (or equivalent tunnel) and point the CLI at `http://127.0.0.1:<port>`; whoever can open the tunnel — Kubernetes RBAC, in practice — is the access control. A wrapper can start and reuse the tunnel automatically (see below).

## Making a change: plan and apply

`plan` diffs the schema directory's desired state against the live database and prints the DDL that would run — nothing is executed:

```console
$ schemabot plan -s ./schema -e staging

╭─────────────────────────────────────────────╮
│  MySQL Schema Change Plan                   │
│                                             │
│  Database: testapp                          │
│  Environment: staging                       │
│  Schema name: testapp                       │
╰─────────────────────────────────────────────╯

     + users
       CREATE TABLE `users` (
           `id` bigint NOT NULL AUTO_INCREMENT,
           `email` varchar(255) NOT NULL,
           `created_at` timestamp DEFAULT current_timestamp(),
           PRIMARY KEY(`id`),
           INDEX `idx_email`(`email`)
       );

     ~ products
       ALTER TABLE `products` ADD INDEX `idx_category_price`(`category`, `price`);

⚠️  Lint Warnings:
  - [users] no_default: Column added without DEFAULT value

📋 Plan: 1 table to create, 1 table to alter
```

`apply` re-plans, shows the same plan, asks for confirmation (`-y` to skip), takes the per-database lock, and then watches live progress until the change completes (`--no-watch` to return immediately):

```console
$ schemabot apply -s ./schema -e staging

╭─────────────────────────────────────────────╮
│  MySQL Schema Change Plan                   │
│                                             │
│  Database: testapp                          │
│  Environment: staging                       │
│  Schema name: testapp                       │
╰─────────────────────────────────────────────╯

     + users
       CREATE TABLE `users` (
           `id` bigint NOT NULL AUTO_INCREMENT,
           `email` varchar(255) NOT NULL,
           `created_at` timestamp DEFAULT current_timestamp(),
           PRIMARY KEY(`id`),
           INDEX `idx_email`(`email`)
       );

     ~ products
       ALTER TABLE `products` ADD INDEX `idx_category_price`(`category`, `price`);

📋 Plan: 1 table to create, 1 table to alter

Do you want to apply these changes? Only 'yes' will be accepted: yes
🔒 Lock acquired for testapp (mysql)

Applying changes...

   users: ████████████████████ ✓ Complete
products: ████████████████████ ✓ Complete


✓ Apply complete! Changes: 1 created, 1 altered. Apply ID: apply-40893dfa7c37468d
```

The flags that shape an apply:

| Flag | What it does |
|---|---|
| `--defer-cutover` | Copy the data but hold the final table swap until an explicit `cutover`. |
| `--skip-revert` | Finalize immediately instead of holding a revert window after completion. Vitess only. |
| `--allow-unsafe` | Permit destructive DDL (drops, type narrowing); blocked otherwise. |

One apply runs per database at a time. A second apply against a locked database is refused with the owner and a way out:

```console
❌ Apply Blocked: Database Locked

┌───────────────────────────────────┐
│  Database:   testapp (mysql)      │
│  Locked by:  acme/inventory#42    │
│  Since:      2 hours ago          │
│  PR:         acme/inventory#42    │
└───────────────────────────────────┘

Another schema change is in progress for this database.

Options:
  • Wait for the current schema change to complete
  • Ask the lock owner to release: schemabot unlock -d testapp
  • Force unlock: schemabot unlock -d testapp --force
```

## Everyday reads

`status` is the command to know: recent schema changes on an environment, with state and caller.

```console
$ schemabot status -e staging
1 active schema change
209 total: 180 Completed · 18 Failed · 7 Cancelled · 3 Reverted · 1 Running

  APPLY ID                DATABASE   ENV      STATE      STARTED       CALLER
  apply-9879515433044309  inventory  staging  Running    2 days ago    github:octocat
  apply-40893dfa7c37468d  checkout   staging  Completed  4 hours ago   github:octocat
  apply-ee8d2f3be4fb430e  payments   staging  Completed  1 day ago     github:hubot
  apply-b82af18eed784603  sessions   staging  Failed     2 days ago    github:octocat
  apply-964ff356ae4d4d05  sessions   staging  Reverted   2 days ago    github:octocat
  ...

Showing the 20 most recent schema changes. Use --limit N to show more.
Use 'schemabot status <apply_id>' to view details
```

Pass an apply id for the detail view, with per-table progress, the DDL being applied, and an ETA:

```console
$ schemabot status -e staging apply-9879515433044309
┌──────────────────────────────────────────────────┐
│  Apply ID:     apply-9879515433044309            │
│  Database:     inventory                         │
│  Environment:  staging                           │
│  State:        Running                           │
│  Caller:       github:octocat@acme/inventory#42  │
│  Started:      Aug 7 17:10:58 UTC                │
│  Duration:     2d 2h                             │
└──────────────────────────────────────────────────┘


  ── inventory ──

     ~ transfers: 🟦🟦🟦🟦🟦🟦🟦🟦🟦🟦🟦🟦🟦⬜⬜⬜⬜⬜⬜⬜ 68%
       ALTER TABLE `transfers`
           ADD INDEX `recipient`(`recipient_token`, `created_at`),
           ADD INDEX `sender`(`sender_token`, `created_at`);
       • Rows: 2,448,104,708 / 3,598,342,961 · ETA: 1d 12h
```

`progress <apply-id>` is the live version of the detail view: it watches an in-flight change until completion (`--no-watch` for a one-shot snapshot). On a `--defer-cutover` apply it waits at the staged state and offers the cutover interactively:

```console
Row copy complete. All data has been copied and new writes
continue to be replicated to keep the shadow table in sync.

Press Enter to proceed with cutover (or Ctrl+C to detach): _
```

`logs` shows an apply's log entries — what happened and when, from the server's point of view:

```bash
schemabot logs <apply-id>                        # newest 50 entries (-n for more)
schemabot logs <apply-id> -f                     # tail and follow until interrupted
schemabot logs <apply-id> --deployment <name>    # engine logs from the data plane
                                                 # (row-copy detail, cutover, errors)
schemabot logs -d inventory -e staging           # by database instead of apply id
```

`databases` answers "is this database configured on the server, and under what key?":

```console
$ schemabot databases -e production
DATABASE   TYPE    ENVIRONMENTS  DEPLOYMENTS
checkout   vitess  production    production: east
identity   mysql   production    production: west
inventory  mysql   production    production: east
orders     vitess  production    production: east
sessions   mysql   production    production: east
```

`pull` returns a database's live schema as JSON (the same read `onboard` uses to bootstrap a schema directory):

```console
$ schemabot pull -d inventory -e production
{
  "database": "inventory",
  "type": "mysql",
  "environment": "production",
  "namespaces": {
    "inventory": {
      "tables": {
        "products":   "CREATE TABLE `products` (\n  `id` bigint unsigned NOT NULL AUTO_INCREMENT, ...",
        "warehouses": "CREATE TABLE `warehouses` (..."
      }
    }
  },
  "table_count": 6
}
```

Most read commands take `-d <database>` to scope to one database and `--json` for scripting.

## Control operations

Every verb below also works as a PR comment on the PR that started the apply; the CLI is the same set of controls with no dependency on GitHub being available. Pass the apply id (find it with `status`).

The escalation ladder, gentlest first:

```bash
# Change is straining the database or misbehaving: stop it (checkpointed, resumable)
schemabot stop -e production <apply-id>
schemabot start -e production <apply-id>    # when ready to resume

# Change is staged and waiting for cutover: fire it when the owner is ready
schemabot cutover -e production <apply-id>

# Change is wrong or dangerous: abort permanently (NOT resumable)
schemabot cancel -e production <apply-id>

# Change completed but must be undone (Vitess only, and only while the revert
# window is open; for MySQL changes use rollback instead)
schemabot revert -e production <apply-id>
```

| Command | What it does |
|---|---|
| `stop` | Pause an in-flight apply. Checkpointed, resumable — the first response to a change causing trouble. |
| `start` | Resume a stopped apply from its checkpoint. |
| `cutover` | Swap tables on a `--defer-cutover` apply. |
| `volume -v <1-11>` | Adjust copy speed mid-apply (1=slowest, 11=fastest). For speeding up a healthy change; to relieve a straining database, use `stop`. |
| `cancel` | Cancel permanently. Terminal, no resume — the copy work is thrown away. |
| `revert` | Undo a completed apply while its revert window is open. Vitess only; MySQL changes have no revert window (use `rollback`). |
| `skip-revert` | Finalize immediately, closing the revert window. Vitess only. |
| `release` | Release a rollout paused by an `on_failure=pause` failure. |
| `unlock -d <database>` | Release the per-database lock (`--force` to take it from another owner). |

Rules of thumb: **`stop` first, `stop` before `cancel`** — stop takes effect fast, keeps its checkpoint, and resumes where it left off, while cancel throws the copy work away. After any control command, confirm the effect landed with `status`.

### Rollback

`rollback` restores the previous schema state as a new schema change — it re-plans in reverse and runs the resulting DDL through the normal apply machinery, so it works on any engine and at any time after completion (unlike `revert`, which is bounded by the revert window):

```console
$ schemabot rollback -e staging apply-40893dfa7c37468d

Rollback Plan
=============
Database: inventory
Environment: staging

The following changes will be applied to rollback:

  transfers (alter):
    ALTER TABLE `transfers` DROP INDEX `recipient`;

Do you want to apply this rollback? Only 'yes' will be accepted:
```

Because the reverse of an additive change is destructive (the plan above drops an index), rollback shows the full plan and requires interactive confirmation before running.

## When GitHub is down: checks backfill

If you run SchemaBot's GitHub integration, its Check Runs are a required merge gate — and the gate fails closed. During a GitHub outage (API 503s, dropped webhook deliveries) the `SchemaBot (staging)` / `SchemaBot (production)` check never gets created on new commits, so PRs look stuck: authors cannot merge, and nothing self-heals until the next PR event. `checks backfill` is the recovery tool. It scans open PRs for missing or stuck SchemaBot Check Runs and synthesizes the missing ones — producing the same Check Runs the webhook path would have created.

The working rhythm after an outage: sweep everything, dry-run first.

```bash
# 1. Dry-run: report what would be synthesized, write nothing to GitHub.
#    (During the outage itself this may still 503; it doubles as a recovery probe.)
schemabot checks backfill --all-repos -e staging --last 2h --dry-run

# 2. Once GitHub recovers, run it for real.
schemabot checks backfill --all-repos -e staging --last 2h

# 3. Repeat for each environment.
schemabot checks backfill --all-repos -e production --last 2h
```

A dry-run pass reports per repo as it scans, then summarizes what it found:

```console
$ schemabot checks backfill --all-repos -e staging --last 2h --dry-run
listing the repositories declared on the server...
repo 1/6 acme/checkout: 4 PRs scanned (4 across all repos) — 0 missing, 0 stuck Check Runs
repo 2/6 acme/inventory: 9/~120 PRs scanned (13 across all repos) — 2 missing, 0 stuck Check Runs
repo 4/6 acme/payments: 3/~45 PRs scanned (16 across all repos) — 0 missing, 1 stuck Check Runs
...
Scanned 21 open PRs updated in the last 2h in 6 repositories for SchemaBot (staging).

Stuck Check Runs — uncompleted for over 1h (backfill does not act on existing Check Runs; investigate the apply or plan that owns each):
PR                                        CHECK                STATUS       AGE
https://github.com/acme/payments/pull/88  SchemaBot (staging)  in_progress  54h1m0s

2 missing SchemaBot Check Runs would be synthesized.
Error: checks backfill left 1 stuck Check Run needing attention
```

The command exits non-zero whenever findings remain, so a clean pass is provable in scripts. Read the stuck table before acting: backfill never touches an existing Check Run, and a check can sit `in_progress` legitimately because its apply is still running (the row above is a multi-day row copy) — confirm with `schemabot status <apply-id>` before treating it as a problem. `no schema files in PR` is the normal outcome for most synthesized rows: the check is created and passes without needing a plan. And backfill never routes around a legitimately held PR — a PR blocked because an apply started and the schema change was later removed stays blocked until an operator reconciles the target environment.

The flags that matter:

| Flag | What it does |
|---|---|
| `--dry-run` | Report missing and stuck checks, write nothing to GitHub. Drop it to synthesize the fixes. |
| `--all-repos` | Scan every repository declared in the server's repos config instead of one `owner/name` at a time. |
| `--last 1h\|2h\|1d` | Only PRs updated inside the window; size it to the outage. Omit it to sweep every open PR (the one-time convergence pass for a repo that predates check enablement — slow on large repos). |
| `--stuck-after 1h` | Report an existing-but-uncompleted Check Run as stuck once it has sat this long (default 1h). |
| `--rate-limit-floor 20` | Pause whenever the GitHub API budget drops below this percent of its limit, so live webhook traffic keeps headroom. |

Outside an incident, the same dry-run pair doubles as a cheap fleet health sweep: run it after a server deploy or a GitHub wobble, and a clean pass proves the check gate is healthy before anyone gets blocked.

## Wrapping the CLI

Organizations often front the CLI with their own wrapper command that bakes in the environment — resolving internal endpoints, running SSO, starting a port-forward — so operators never configure any of it by hand. At Block, we build an internal wrapper around the schemabot CLI to target internal deployments, which we denote as `sq schemabot`; other schemabot users may want to follow suit. Two styles work:

**Exec the binary.** The wrapper resolves routing and credentials, then execs `schemabot`, injecting them as flags:

```bash
# inside the wrapper, after resolving the endpoint and token:
exec schemabot --cli-name "sq schemabot" --endpoint "$ENDPOINT" --token "$TOKEN" "$@"
```

**Embed the Go packages.** For a wrapper with real logic of its own — per-environment routing tables, automatic port-forwards, extra commands — import the command structs and build your own [kong](https://github.com/alecthomas/kong) CLI around them. This is how `sq schemabot` is built:

```go
import (
    "github.com/alecthomas/kong"

    "github.com/block/schemabot/pkg/cmd/cliname"
    "github.com/block/schemabot/pkg/cmd/commands"
)

type CLI struct {
    commands.Globals

    Plan   commands.PlanCmd   `cmd:"" help:"Create a schema change plan."`
    Apply  commands.ApplyCmd  `cmd:"" help:"Apply schema changes."`
    Status commands.StatusCmd `cmd:"" help:"Show schema change status."`
    // ...the rest of the upstream commands, plus your own.
}

func main() {
    cliname.Set("sq schemabot")

    var cli CLI
    ctx := kong.Parse(&cli, kong.Name("sq schemabot"))

    // Resolve routing however your environment requires — a direct endpoint
    // behind your ingress, or a port-forward started here — then inject it:
    cli.Globals.Endpoint = resolveEndpoint()

    ctx.FatalIfErrorf(ctx.Run(&cli.Globals))
}
```

Either way, the **tool name** matters: every pasteable command hint the CLI prints (`Force unlock: ... unlock -d <db> --force`, `use '... status ...'`, usage text) starts with it, and hints are pasteable as printed — a pasted hint should re-enter the wrapper, not an unconfigured bare binary. An exec-style wrapper passes `--cli-name`; an embedding wrapper calls `cliname.Set` before parsing. Without either, hints render the default `schemabot` name, so direct use of the binary is unchanged.
