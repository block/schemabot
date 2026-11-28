# The SchemaBot CLI

The `schemabot` CLI shows what SchemaBot is running on a database: each apply's state, per-table progress, ETA, and logs, in any environment. It reads from the SchemaBot server, not from GitHub, so it still works when PR comments stop updating.

These are terminal commands, not PR comments. Reading status, progress, history, plans, and logs needs only read access, and you can run them without asking each time. Run `progress` with `--no-watch`: watching, it has keys that stop or cancel the apply, or run its cutover. Commands that change something, such as `stop` or `cutover`, follow the same [command policy](command-policy.md) in the terminal as on the PR. Apply or roll back a PR's change from the PR: hand over any terminal `apply` or `rollback`, because it skips the PR's environment order and confirmation, and can skip the database lock.

## Set up

Install a released binary from the [releases page](https://github.com/block/schemabot/releases), then point it at your team's server:

```bash
schemabot configure
schemabot status
```

If the server requires authentication, `schemabot login` caches a token. [Connect to a server](https://github.com/block/schemabot/blob/main/docs/cli.md#connect-to-a-server) has the details. If your team runs the CLI through its own wrapper, use that instead; see [Your team's wrapper](#your-teams-wrapper).

A `403` means the account lacks a permission. Say which one, and ask the user or an operator to grant it. A `401` means the token is missing, expired, or invalid, so the user needs to log in again or fix their token. In either case, never try other credentials or work around the check.

## Your team's wrapper

Many teams run the CLI through a wrapper, such as `acme schemabot`. The wrapper knows the team's servers and handles sign-in, so the same command reaches the right server from any laptop. When your team has one, use it for every terminal command:

- **Swap only the leading name.** Keep the subcommand and flags from this guide, so `schemabot status -e production` becomes `acme schemabot status -e production`.
- **Find the name in your team's skill or docs, or in SchemaBot's own hints.** The server can be configured to print the wrapper's name in the CLI hints it puts in PR comments, and a wrapper does the same for the commands the CLI prints. A hint that reads `acme schemabot …` is the sign a wrapper exists.
- **Copy printed commands as written.** A wrapper usually picks the server from `-e` and may add its own flags, such as one that selects which of the team's SchemaBot deployments to reach. A command the CLI prints for you to run next carries the flags the current run used.
- **Let the wrapper sign in.** Don't aim the bare `schemabot` binary at a team server with `--endpoint` or a token you found to get around the wrapper. Its sign-in is how the team decides who has access.
- **PR comment commands don't change.** They always start with `schemabot`, whatever the wrapper is called.

A wrapper can trail the latest SchemaBot release. If a command or flag in this guide is missing, check the wrapper's `--version` before deciding SchemaBot can't do it. To build a wrapper, see [Wrap the CLI for your team](https://github.com/block/schemabot/blob/main/docs/cli.md#wrap-the-cli-for-your-team).

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

`State` is the apply as a whole, and each table shows its DDL, progress, and ETA. `progress --no-watch` prints the progress view once. Without `--no-watch` it keeps updating until the apply finishes, and its keys stop or cancel the apply, or run its cutover; leave watching to a human.

```bash
schemabot progress apply-3f9a2c71 --no-watch
```

`Running` with progress moving is healthy: leave it to finish. `Completed` means that environment is done. For `Failed`, or `Running` with progress that has stopped moving, read the logs:

```bash
schemabot logs apply-3f9a2c71
```

## Use it from an agent

- Prefer `--json` where a command offers it (`databases`, `status`, `progress`, `list-plans`, `logs`, `locks`, `plan`, `pull`), and `status --json` or `progress --json` for a snapshot.
- Never run a watching `progress`: it runs until the change finishes, and its keys change the apply. Take a `status`, `progress --no-watch`, or `progress --json` snapshot and check again later.
- [Use the CLI from scripts and agents](https://github.com/block/schemabot/blob/main/docs/cli.md#use-the-cli-from-scripts-and-agents) covers exit codes and output formats.
