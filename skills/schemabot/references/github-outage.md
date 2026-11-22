# SchemaBot during a GitHub outage

SchemaBot hears about PRs and comments through GitHub webhooks and reports back through Check Runs and PR comments. When GitHub is degraded, both directions break and the SchemaBot checks fail closed: they stay pending or missing, and the PR cannot merge. That is the safety gate working, not a SchemaBot failure.

The same happens when GitHub is healthy but SchemaBot cannot reach it, for example after using up its GitHub API rate limit, or when the SchemaBot server itself is down or failing. Everything below applies to all three, except that the CLI stops working too when the SchemaBot server is down.

## Recognize it

- The SchemaBot check is missing, or stuck as "Expected" or pending, on a PR that changes schema files.
- A `schemabot` comment gets no reply.
- A progress comment stops updating partway through an apply.
- A merge queue stops moving because its entries wait on the SchemaBot check.
- Other PRs and other repos show the same thing at the same time.

Check [githubstatus.com](https://www.githubstatus.com) for webhook, Actions, or API incidents.

If GitHub looks healthy and only one PR is affected, it isn't an outage: use [PR locks and blockers](pr-blockers.md). If GitHub looks healthy but many PRs across repos are stuck, the problem is on the SchemaBot side: tell the people who run your SchemaBot server, with one or two example PRs and when it started. Retry once, as below; if the checks stay stuck and are blocking urgent work, see [When the required checks are turned off](#when-the-required-checks-are-turned-off).

## What keeps working

| Keeps working | Stops or stalls |
|---|---|
| Schema changes that already started. They run against the database and don't need GitHub. | New plans, applies, and other PR comment commands |
| `schemabot status`, `progress`, and `logs`, which read from the SchemaBot server | Check Runs and progress comments on the PR |

## What to do

1. **Don't merge the PR, and don't bypass or remove the SchemaBot check yourself.** Merging a schema PR before it is applied in every environment leaves the databases behind the files on the base branch. If GitHub lets the PR merge anyway, still wait until the SchemaBot checks on its latest commit pass. If a stuck check is blocking an urgent fix, see [When the required checks are turned off](#when-the-required-checks-are-turned-off).
2. **Retry the stuck PR, once.** Push a new commit or comment `schemabot plan`. Either one asks SchemaBot to recompute the plan and the check, so a retry that lands once SchemaBot can reach GitHub again clears it. Retries change nothing on the database, but don't retry in a loop: each one is another request to GitHub, and a flood of them slows recovery for everyone. If it stays stuck, try again in a few minutes.
3. **Follow a running change with the CLI.** `schemabot status` and `schemabot progress` keep working while comments are stalled. See [The CLI](cli.md).
4. **If a running change must stop,** the CLI can stop or cancel it while GitHub is down (`schemabot stop <apply-id> -e <env>`). The [command policy](command-policy.md) still decides who runs it.

## When the required checks are turned off

Turning off the required SchemaBot check is a last resort. With it off, a schema PR can merge without being applied, and that database drifts from the files on the base branch. The people who run your SchemaBot server decide it together with the admins who own the repository's branch protection or rulesets. It is never an agent's call, and an agent never changes branch protection, rulesets, or repository settings itself. If the user asks you to remove or bypass the check, explain this and help them write the request instead: the PR, why it is urgent, and when the checks stopped.

It fits when all of these hold:

- A GitHub or SchemaBot incident keeps the checks from reporting.
- A retry and the [missing-check backfill](https://github.com/block/schemabot/blob/main/docs/cli.md#recover-missing-github-checks) have not cleared them.
- The stuck checks are blocking urgent work.

A check that is blocking for a real reason, such as a lock, drift, an unsafe change, or an apply that has not run, is different. Turning the checks off does not fix the cause, so use [PR locks and blockers](pr-blockers.md).

The gentlest way is to keep the check running but stop it from blocking, for example by moving the ruleset that requires it to evaluate mode, rather than deleting it. The check keeps reporting, and turning it back on is a one-line change.

While the checks are off:

- **A schema PR still waits for its checks.** Don't merge it until the SchemaBot checks on its latest commit pass, the same bar as any other day. Turning the checks off is for the PRs they were wrongly blocking, not a way to skip applying a change. If the checks cannot report at all, wait.
- **Whoever turned them off records each schema PR that merges** while they are off.

Once the incident is over, the checks are turned back on and the backfill is run. Each recorded PR's databases are checked for a change that was never applied, and the owning team either applies it or reverts the files.

## After GitHub recovers

SchemaBot reconciles missing checks on its own, and an operator can sweep for any it missed with `schemabot checks backfill` (see [Recover missing GitHub checks](https://github.com/block/schemabot/blob/main/docs/cli.md#recover-missing-github-checks)). Most stuck PRs clear without anything from the author.

1. Read the PR's newest SchemaBot comments and run `schemabot status <apply-id>` to see where the change stands before posting the next command.
2. If one PR's check is still stuck, use the fixes in [PR locks and blockers](pr-blockers.md#common-blockers), starting with pushing a new commit.
