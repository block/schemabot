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

1. **Don't merge the PR, and don't bypass or remove the SchemaBot check yourself.** Merging a schema PR before it is applied in every environment leaves the databases behind the files on the base branch. If GitHub lets the PR merge anyway, still wait until the SchemaBot checks on its latest commit pass. If GitHub is healthy, SchemaBot is the cause, and stuck checks block urgent work, see [When the required checks are turned off](#when-the-required-checks-are-turned-off).
2. **Retry the stuck PR, once.** Push a new commit or comment `schemabot plan`. Either one asks SchemaBot to recompute the plan and the check, so a retry that lands once SchemaBot can reach GitHub again clears it. Retries change nothing on the database, but don't retry in a loop: each one is another request to GitHub, and a flood of them slows recovery for everyone. If it stays stuck, try again in a few minutes.
3. **Follow a running change with the CLI.** `schemabot status` and `schemabot progress` keep working while comments are stalled. See [The CLI](cli.md).
4. **If a running change must stop,** the CLI can stop or cancel it while GitHub is down (`schemabot stop <apply-id> -e <env>`). The [command policy](command-policy.md) still decides who runs it.

## When the required checks are turned off

If GitHub is healthy and SchemaBot is the problem, turning off the required SchemaBot check is the way to let blocked PRs merge. That covers a failing SchemaBot server, webhook processing that has stopped, or SchemaBot's GitHub API quota running out. It does nothing for a GitHub outage, where GitHub itself is what is down: wait for it to recover.

**The cost: with the check off, a schema PR can merge without being applied.** That database then drifts from the files on the base branch until someone applies the change, and the next PR that touches it plans the missing change along with its own. So this is a last resort, for when all of these hold:

- SchemaBot cannot report the checks, and it is not a GitHub outage.
- A retry and the [missing-check backfill](https://github.com/block/schemabot/blob/main/docs/cli.md#recover-missing-github-checks) have not cleared them, or the backfill cannot safely run yet, for example while the quota is exhausted.
- The stuck checks block urgent work, or many PRs.

If only a few PRs are blocked and none is urgent, unblock them one at a time with the [single-PR fixes](pr-blockers.md#common-blockers) instead. A check that is blocking for a real reason, such as a lock, drift, an unsafe change, or an apply that has not run, is different again: turning the checks off does not fix the cause, so use [PR locks and blockers](pr-blockers.md).

**Who decides:** the people who run your SchemaBot server, with the teams whose PRs are blocked. The admins who own the repositories' branch protection or rulesets make the change. It is never an agent's call, and an agent never changes branch protection, rulesets, or repository settings itself. If the user asks you to remove or bypass the check, explain the cost and help them write the request: the PRs, why they are urgent, and when the checks stopped.

**How:** prefer keeping the check running but stopping it from blocking, for example by moving the ruleset that requires it to evaluate mode, over deleting it. The check keeps reporting, and turning it back on restores exactly what was there. A repository that also requires the check in its own ruleset or classic branch protection needs that removed separately, so write down which repositories lost which checks; recovery restores exactly those.

**At-risk PRs** are open ones with a schema change in flight: an apply still running, or applied in an earlier environment but not yet in the next. `status` lists every active apply but only the most recent finished ones, so page through all of them for each environment, with `--last` reaching back far enough to cover the open schema PRs:

```bash
schemabot status -e <env> --all --last 14d
```

The `SOURCE` column is the PR that started each apply. Keep the PRs that are still open and have not yet applied in every environment. Ask each author not to merge until the SchemaBot checks report again, then tell them again when it is over. Posting that request is something you can do, under the usual rules for PR comments.

**Keep the window as short as you can.** Every minute the checks are off is time an unapplied schema PR can merge, and every PR that merges in that window is one more database to check afterward. Turn them off only where they are blocking, and back on as soon as SchemaBot can report again, not at the end of the day.

**While the checks are off,** a schema PR still waits for its own checks: don't merge one until the SchemaBot checks on its latest commit pass, the same bar as any other day. Whoever turned the checks off keeps a list of every schema PR that merges in the meantime.

**As soon as SchemaBot can report again,** run the backfill, turn the checks back on, and confirm each repository requires the same checks it did before. Then find and reconcile the drift.

1. **Find every schema PR that merged while the checks were off.** Start from the list, but don't trust it to be complete: also search each affected repository for PRs merged in that window that touch a SchemaBot schema directory, for example with `gh pr list --state merged --search "merged:>=2026-01-15"`, then each PR's changed files.
2. **Check every database each one touched, in every environment.** From an up-to-date checkout of the base branch, plan it from the CLI:

   ```bash
   schemabot plan -s schema -e production
   ```

   No changes means that environment matches the merged files. Any DDL means that environment is missing a schema change. Don't rely on a `Completed` apply in `schemabot status` instead: the PR may have changed after that apply ran.
3. **Reconcile each drifted environment with the owning team.** Either apply the missing change, through a PR with a [nonce edit](pr-blockers.md#reconciling-a-database-that-is-behind-its-files) since the files are already correct, or, if it should not land, revert the files in a PR so they match the database again. Until then, the next PR that touches that database plans the missing change along with its own, so tell anyone with an open PR there. Every apply here follows the [command policy](command-policy.md) like any other.

## After GitHub recovers

SchemaBot reconciles missing checks on its own, and an operator can sweep for any it missed with `schemabot checks backfill` (see [Recover missing GitHub checks](https://github.com/block/schemabot/blob/main/docs/cli.md#recover-missing-github-checks)). Most stuck PRs clear without anything from the author.

1. Read the PR's newest SchemaBot comments and run `schemabot status <apply-id>` to see where the change stands before posting the next command.
2. If one PR's check is still stuck, use the fixes in [PR locks and blockers](pr-blockers.md#common-blockers), starting with pushing a new commit.
