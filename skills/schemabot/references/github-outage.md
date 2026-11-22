# SchemaBot during a GitHub outage

SchemaBot hears about PRs and comments through GitHub webhooks and reports back through Check Runs and PR comments. When GitHub is degraded, both directions break and the SchemaBot checks fail closed: they stay pending or missing, and the PR cannot merge. That is the safety gate working, not a SchemaBot failure.

The same happens when GitHub is healthy but SchemaBot cannot reach it, for example after using up its GitHub API rate limit. Everything below applies to both.

## Recognize it

- The SchemaBot check is missing, or stuck as "Expected" or pending, on a PR that changes schema files.
- A `schemabot` comment gets no reply.
- A progress comment stops updating partway through an apply.
- A merge queue stops moving because its entries wait on the SchemaBot check.
- Other PRs and other repos show the same thing at the same time.

Check [githubstatus.com](https://www.githubstatus.com) for webhook, Actions, or API incidents.

If GitHub looks healthy and only one PR is affected, it isn't an outage: use [PR locks and blockers](pr-blockers.md). If GitHub looks healthy but many PRs across repos are stuck, the problem is on the SchemaBot side: tell the people who run your SchemaBot server, with one or two example PRs and when it started.

## What keeps working

| Keeps working | Stops or stalls |
|---|---|
| Schema changes that already started. They run against the database and don't need GitHub. | New plans, applies, and other PR comment commands |
| `schemabot status`, `progress`, and `logs`, which read from the SchemaBot server | Check Runs and progress comments on the PR |

## What to do

1. **Don't merge the PR, and don't bypass or remove the SchemaBot check.** Merging a schema PR before it is applied in every environment leaves the databases behind the files on the base branch. If GitHub lets the PR merge anyway, still wait until the SchemaBot checks on its latest commit pass.
2. **Retry the stuck PR, once.** Push a new commit or comment `schemabot plan`. Either one asks SchemaBot to recompute the plan and the check, so a retry that lands once SchemaBot can reach GitHub again clears it. Retries change nothing on the database, but don't retry in a loop: each one is another request to GitHub, and a flood of them slows recovery for everyone. If it stays stuck, try again in a few minutes.
3. **Follow a running change with the CLI.** `schemabot status` and `schemabot progress` keep working while comments are stalled. See [The CLI](cli.md).
4. **If a running change must stop,** the CLI can stop or cancel it while GitHub is down (`schemabot stop <apply-id> -e <env>`). The [command policy](command-policy.md) still decides who runs it.

## After GitHub recovers

SchemaBot reconciles missing checks on its own, and an operator can sweep for any it missed with `schemabot checks backfill` (see [Recover missing GitHub checks](https://github.com/block/schemabot/blob/main/docs/cli.md#recover-missing-github-checks)). Most stuck PRs clear without anything from the author.

1. Read the PR's newest SchemaBot comments and run `schemabot status <apply-id>` to see where the change stands before posting the next command.
2. If one PR's check is still stuck, use the fixes in [PR locks and blockers](pr-blockers.md#common-blockers), starting with pushing a new commit.
