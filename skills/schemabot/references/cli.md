# The SchemaBot CLI

The `schemabot` CLI shows what SchemaBot is running on a database: each apply's state, per-table progress, ETA, and logs, in any environment. It reads from the SchemaBot server, not from GitHub, so it still works when PR comments stop updating.

These are terminal commands, not PR comments. Reading status, progress, history, plans, and logs needs only read access, and you can run them without asking each time. Commands that change something, such as `apply`, `stop`, or `cutover`, follow the same [command policy](command-policy.md) in the terminal as on the PR.

## Set up

Install a released binary from the [releases page](https://github.com/block/schemabot/releases), then point it at your team's server:

```bash
schemabot configure
schemabot status
```

If the server requires authentication, `schemabot login` caches a token. Your team may wrap the CLI under another name or set it up for you; use their instructions if so. [Connect to a server](https://github.com/block/schemabot/blob/main/docs/cli.md#connect-to-a-server) has the details.

An authentication or permission error (`401` or `403`) means the account lacks access. Say which permission is missing and ask the user or an operator. Never try other credentials or work around the check.

## Track a schema change

Find the apply ID in the PR's latest progress comment, or list the active applies for the environment. The `SOURCE` column is the PR that started each one:

```bash
schemabot status -e production
```

```text
  APPLY ID          DATABASE  ENV         STATE    STARTED         SOURCE
  apply-3f9a2c71  shop      production  Running  10 minutes ago  https://github.com/acme/store/pull/42
```

Then look at that apply:

```bash
schemabot status apply-3f9a2c71
```

`State` is the apply as a whole, and each table shows its DDL, progress, and ETA. To follow it live, `progress` keeps the same view updating until the apply finishes:

```bash
schemabot progress apply-3f9a2c71
```

`Running` with progress moving is healthy: leave it to finish. `Completed` means that environment is done. For `Failed`, or `Running` with progress that has stopped moving, read the logs:

```bash
schemabot logs apply-3f9a2c71
```

## Use it from an agent

- Prefer `--json` where a command offers it (`databases`, `status`, `list-plans`, `logs`, `plan`), and `status --json` for a snapshot instead of `progress`.
- `progress` and a watching `apply` run until the change finishes. Don't block on them; take a `status` snapshot and check again later.
- [Use the CLI from scripts and agents](https://github.com/block/schemabot/blob/main/docs/cli.md#use-the-cli-from-scripts-and-agents) covers exit codes and output formats.
