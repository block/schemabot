# Refusals, blocks, and failures

What each outcome looks like, and what to do next. The [command policy](command-policy.md#when-schemabot-blocks-or-refuses-a-change) sets the rule for all of them: stop, give the user SchemaBot's reason, and never route around it.

## Contents

- [Apply states](#apply-states)
- [Freezes](#freezes)
- [Blocked plans](#blocked-plans)
- [PostgreSQL](#postgresql)
- [Vitess](#vitess)

## Apply states

- `failed`, `cancelled`, and `reverted` are final. The next step is always a fresh plan, never another command on the same apply.
- `failed_retryable` means wait. SchemaBot retries it on its own. Planning again while it retries is refused, because the apply still holds its target.
- Name an apply by its apply ID, such as `apply-3f9a2c71`, when you check status, read logs, or send a control command.
- Control commands (`stop`, `start`, `cancel`, `cutover`, and the rest) are the only way to change a running apply. SchemaBot records each one before it acknowledges it, so it survives a restart. Send it once, then watch the status or the PR comment for the effect.

## Freezes

A freeze is a decision people made, for a launch, a holiday, or an incident. Read it and report it; never work around it.

Recognize a refusal by its signal, not its wording:

- **CLI:** exit code 1 with the `❄️ <environment> is frozen` line, and nothing started.
- **API:** `409` with `error_code: "change_gate_refused"`. `blockers` lists the freezes.
- **PR:** a comment whose heading starts with `❄️` and contains `Blocked by` (`Blocked by a Freeze` or `Blocked by Freezes`). The plan comment also says the change was not applied.

When a freeze refuses a change:

1. Stop. Do not retry in a loop, rewrite the change, or try another environment, deployment, or credential.
2. Tell the user which freeze holds the change, its reason, and when it ends. `freeze list` and `freeze show` tell you.
3. If the user says the change can't wait, hand over the exact `--override` command. The override reason is recorded as theirs, so they write it and run it.

A plan comment's `❄️ <environment> is frozen` notice is a warning, not a refusal. The same rules hold: tell the user, and the apply command with `--override` in that comment is theirs to post.

`change_gate_unavailable` is different: SchemaBot could not read its freeze storage. Retry with backoff, as for any `503`.

## Blocked plans

On every engine, a blocked plan fails the plan Check Run, and the plan comment names each blocked table and the reason. `--allow-unsafe` accepts a destructive change; it never makes a blocked change runnable.

A plan that drops a column and adds one on the same table is almost always a rename, and it deletes the old column's values. Say so before anyone applies it, and offer the safe steps in [Renaming a column or table](change-behavior.md#renaming-a-column-or-table).

## PostgreSQL

pg-sprite has already decided which changes are safe to run. Never work around that decision.

| Outcome | Signal | Next |
|---|---|---|
| Refused at apply time | The apply fails and names what to change. Applying the same plan again meets the same refusal | Rewrite the schema files into the steps it names, plan again, and show the user the new plan |
| No safe form | The reason names no steps, such as a column type change that rewrites the table | Tell the user. That change needs a separately reviewed process a person runs |
| Retryable failure | The apply names what it waits for, such as an index build still running | Report it and check again. SchemaBot retries it |
| Changed since review | A table, column, or constraint the plan drops changed after review, so the drop does not run. An automatic apply posts a new plan in a new comment. A confirmed apply posts nothing | Nobody has reviewed the new plan. After an automatic apply, ask the user before `apply-confirm` on the new comment. After a confirmed apply, run `apply` again under the command policy to show the current plan |

A failed index build can leave an invalid index behind. Leave it: the next apply removes it once it proves no build still holds it.

## Vitess

| Outcome | Signal | Next |
|---|---|---|
| Rejected on the branch | The apply fails with the statement Vitess refused. Nothing reached the production branch | Applying the same plan again meets the same rejection. Tell the user which statement was refused. Rewrite the schema files only into a form the user agrees to, then plan again |
| Cancelled | The deploy request is closed for good | Nothing resumes. A new apply starts a new deploy request |
| Reverted in the console | The apply reports the revert. SchemaBot reads the deploy request's state and does not overwrite it | Tell the user. Plan again if the change is still wanted |

`cancel` is the one Vitess control command that cannot be undone, so the command policy's rules for it apply in full.
