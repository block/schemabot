# PR workflow and commands

## Contents

- [The PR workflow](#the-pr-workflow)
- [Lifecycle commands](#lifecycle-commands)
- [Reading the check](#reading-the-check)
- [Review and approval](#review-and-approval)
- [Driving the PR as an agent](#driving-the-pr-as-an-agent)

## The PR workflow

1. **Edit the declarative schema** to the desired end state. See [Authoring](authoring.md).
2. **Open a PR.** SchemaBot plans on its own when the PR opens, when commits are pushed, and when it is reopened. It posts a plan comment with the DDL it will run and sets the check. A push that changes no schema file refreshes the checks without a new comment. When the files already match the live database, no plan comment is posted and the passing check is the signal. `schemabot plan` runs the same plan on demand.
3. **Review the plan.** Confirm the DDL matches the intent. Drops are flagged and blocked; a narrowing type change or a removed `ENUM` value is not, so check those against the data yourself. See [Unsafe changes and drops](change-behavior.md#unsafe-changes-and-drops).
4. **Apply to each environment in order**, by commenting the command from the plan comment, such as `schemabot apply -e staging`. Who posts it is set by the [command policy](command-policy.md). SchemaBot plans again, takes the database lock, and applies when the new plan matches the reviewed one. When it cannot verify that, it posts the new plan, moves the check to `action_required`, and waits for `schemabot apply-confirm -e staging`. Ask the user about that confirmation, because it means the plan changed.
5. **Watch progress.** SchemaBot updates a progress comment as the apply moves: running, copying rows with a percentage and ETA for large tables, cutover, then applied. The comment always shows the current state and the next command. When the comments stop updating, use [the CLI](cli.md).
6. **Merge** once the required checks pass. If the PR's base branch changed, comment `schemabot plan` first and wait for its results and any applies it calls for: a check that passed before the base changed can still satisfy GitHub.

**Don't assume the environment list.** The environments a database has, and their order, live in the server's configuration, not in the repo. Some databases have only production. Until the plan posts, say "the environments the plan lists"; after it posts, read them from the plan comment and the checks. A later environment's apply is refused until the earlier ones have the change.

## Lifecycle commands

Each takes the apply ID and environment from the progress comment. Which ones apply depends on the engine and the apply's state; see the [engine capability matrix](https://github.com/block/schemabot/blob/main/docs/engines.md#capability-matrix).

| Command | What it does | Engines |
|---|---|---|
| `schemabot stop <apply-id> -e <env>` | Pause a running change, keeping its progress | `mysql` |
| `schemabot start <apply-id> -e <env>` | Resume a stopped change from where it stopped | `mysql`; on `vitess`, a deferred deploy only |
| `schemabot cancel <apply-id> -e <env>` | End a change for good; nothing resumes it | `mysql`, `vitess`; on `postgres`, a concurrent index build only |
| `schemabot cutover <apply-id> -e <env>` | Run the cutover of a change applied with `--defer-cutover` | `mysql`, `vitess` |
| `schemabot release <apply-id> -e <env>` | Let a rollout continue after it paused on a failure | any rollout configured to pause |
| `schemabot revert <apply-id> -e <env>` | Undo a completed change while its revert window is open | `vitess` |
| `schemabot skip-revert <apply-id> -e <env>` | Close the revert window early | `vitess` |
| `schemabot rollback <apply-id> -e <env>` | Post a plan that returns to the previous schema; confirm with `schemabot rollback-confirm -e <env>` | any, while the PR is open; not every plan can be rolled back |

`stop` on Vitess is refused with a pointer to `cancel`, because a deploy request cannot be paused. On PostgreSQL, `stop`, `start`, `cutover`, `revert`, and `skip-revert` are not supported; the [PostgreSQL guide](https://github.com/block/schemabot/blob/main/docs/postgresql.md#unsupported-workflow-features) says what to do instead.

Undoing is covered in [Recovery](recovery.md).

## Reading the check

The SchemaBot check is the merge gate. It stays blocking until the change is applied to every required environment, and anything uncertain keeps it blocking rather than letting it pass. [Check Runs](https://github.com/block/schemabot/blob/main/docs/check-runs.md) explains every state.

GitHub and `gh pr checks` show an `action_required` check as a red ✗ or "fail". It is not a failure: the check is waiting for something, and its summary says what. Read it before choosing a command:

- **An apply or a confirmation.** The next environment needs its apply, or a plan that changed needs `apply-confirm`.
- **A fresh plan after a rollback.** A completed rollback never turns the check green, because the database no longer matches the PR. The check passes only once a plan finds nothing to change, so bring the PR's files back to what the database now holds, or change them to what you want next. Never reapply a change that was rolled back on purpose.
- **A person reconciling an environment.** An apply failed or was cancelled after it changed something, or a commit removed a change after its apply started. SchemaBot never clears this on its own. Once the apply settles, a person makes the PR and the target agree, by applying the change again or changing the PR to match the target, and then comments the plan the check names. That plan clears the block once the two match. Don't post that plan yourself: hand it to whoever is reconciling the target.

Don't report "the SchemaBot check failed" from the mark alone. Report what it is waiting for.

## Review and approval

Production applies usually need an approved PR, but this is server configuration: the [review gate](https://github.com/block/schemabot/blob/main/docs/configuration.md#review-gate) can be on for the whole server, for one database, or off.

- When it is on and approval is missing, SchemaBot posts a "Review Required" comment naming who can approve. Read it rather than guessing.
- The PR author's own approval never counts.
- The server can require a fresh approval when the schema change differs from the one approved.
- Who may comment commands and who may approve the PR are set separately. An approval from someone who isn't an authorized reviewer doesn't satisfy the gate, and an approval doesn't make the commenter authorized to apply. The refusal names which list is not satisfied.

## Driving the PR as an agent

**Take the change all the way to a PR.** Nothing happens until a PR exists: no plan, no check, nothing to apply. After the schema edit and any repo tooling finish, say which branch you used and open the PR, or ask "want me to open a draft PR?". Don't end with "once this is opened as a PR…" and leave nobody holding the next step.

**Watch for the plan.** Once the PR is open, poll its comments and checks for a few minutes rather than handing off with "SchemaBot hasn't posted yet". Report what the plan says: the DDL, the environments, and the exact apply command. Then post or hand over that command under the [command policy](command-policy.md). If no plan appears, say so and load [PR locks and blockers](pr-blockers.md).

**Announce repo tooling before running it.** Code generation and validation steps can take minutes. Say what you are running and why in one line, then say what it produced.
