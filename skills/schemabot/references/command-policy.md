# Agent command policy

A SchemaBot command posted from the user's GitHub account is recorded as the user's. SchemaBot cannot tell whether the user typed it or their agent did. This policy decides which commands you post, which you ask about first, and which you only hand over. It follows [Working with AI agents](https://github.com/block/schemabot/blob/main/docs/ai-agents.md).

## Every command at a glance

| Command | Production | Other environments |
|---|---|---|
| `plan`, `help` | Post freely: they change nothing on the database | Post freely |
| `apply`, `stop`, `start`, `cutover`, `cancel` | Hand over | Post or hand over, by [who the database affects](#other-environments-decide-who-it-affects) |
| `apply-confirm`, `release`, `unlock`, `rollback`, `rollback-confirm`, `revert`, `skip-revert` | Hand over | [Ask first](#ask-before-posting-these-in-any-environment) |
| Any command carrying `--allow-unsafe`, `--skip-revert`, `--override`, or `--force` | Hand over | [Hand over](#never-post-these-in-any-environment) |
| `freeze approve` | Hand over | Hand over: approving an override of a freeze is a freeze approver's decision |
| `fix-lint` | Run it in the terminal, not as a comment: it rewrites the schema files in the worktree | Same |

`unlock` takes no `-e`, so it is never only a non-production command; see below.

## Production: never post

Never post a production command, on any database. Hand the human the exact command and stop.

Production usually requires an approved PR. The command comment is the act that authorizes the change, and SchemaBot records its author as the accountable operator. Posting it from the user's account signs for them. A 🤖 disclosure does not change that.

"Go ahead and apply it to production" means "give me the command", not "post it for me". This holds when the user asks you to post it, says they approve, or says they have already reviewed the change.

## Never post these, in any environment

- A command carrying `--allow-unsafe`. Accepting a destructive change is a human decision. Show the change and the command and let the human post it.
- A command carrying `--skip-revert`. It gives up the Vitess revert window before anyone has seen the change land, which is the same decision as `skip-revert` made up front.
- A command carrying `--override`. When a freeze refuses a command, SchemaBot says to run it again with `--override` and a reason. By default that lets the change through the freeze at once, on a server set to `approval` it asks a freeze approver, and either way the reason is recorded as the user's. Tell the user about the freeze and let them decide whether to override it.
- `unlock --force`. It releases a database's lock even when no PR holds it, such as the lock of an apply started from the CLI, in any environment.

## Ask before posting these, in any environment

Say what the command will do and let the user answer. If they would rather post it themselves, hand it over.

- **`apply-confirm`.** SchemaBot waits for it when it could not verify that the plan is the one that was reviewed, or when the change needs explicit sign-off. Confirming it yourself approves a plan nobody has read. Say what changed.
- **`release`.** A rollout pauses here after a failure. Releasing it declares the failure acceptable and lets the remaining members go. Say what failed.
- **`unlock`.** It takes no `-e`: it releases every lock the PR holds, which can include a production one. Name the locks it would release.
- **`rollback` and `rollback-confirm`.** A rollback applies the previous schema, so undoing an additive change drops the column or table it added, with any data written to it. Say what will be dropped. A rollback that drops something needs `--allow-unsafe` on `rollback-confirm`, and that makes the confirm a hand-over: see [Never post these](#never-post-these-in-any-environment).
- **`revert`.** Same effect as a rollback, inside the Vitess revert window.
- **`skip-revert`.** Closes the revert window early and gives up the undo. Say what option is being closed.
- **Closing the PR, even to reopen it.** With no apply running, closing a PR releases its locks and clears its stored check state, as `unlock` does, and throws away a plan waiting for `apply-confirm`. Say what it would release.

## Other environments: decide who it affects

A staging or other non-production environment can still be shared. An apply there can break other people's tests, and it holds the database lock while it runs. Before your first command on a database, work out who else depends on it.

If a wrapper skill names a lookup for this decision, run it and follow its result; without one, decide from what the repo and the user tell you, and treat anything you cannot confirm as shared.

| What you found | What you do |
|---|---|
| The database is shared with other teams or services, or is critical to them | Hand the command over |
| You cannot tell who depends on it | Hand the command over |
| The database is new or isolated, with little impact on anyone else | Say what you are about to do, then post it |

This decides `apply` and the in-flight commands `stop`, `start`, `cutover`, and `cancel`. Every rule above still applies on top of it.

When you hand over a command because the database is shared or you could not tell, say which one it was. There is no question to ask: "post it anyway" doesn't change who the database affects. If the user tells you something you didn't know, such as that the database is a scratch copy only they use, decide again with that.

When you do post, announce it in the turn before: "applying to staging now, I'll report what the plan says". Then read the plan and progress comments back to the user. Never post as a side effect of unrelated work.

## Terminal commands that change SchemaBot itself

The rules above apply to lifecycle commands in the terminal too: a `stop` or `cutover` run from the CLI is the same decision as the comment. A terminal `apply` or `rollback` is different. It runs outside the PR, so it skips the PR's environment order and checks, and its flags can drop safeguards a comment always keeps: `--no-lock` takes no database lock, `--force` breaks another owner's lock, and `--auto-approve` skips the confirmation prompt, so a rollback runs without waiting for `rollback-confirm`. Apply or roll back a PR's change from the PR, and hand over any terminal `apply` or `rollback`, in every environment.

A terminal `unlock --force` is the same decision as the `unlock --force` comment in the never-post list: never run it. The CLI asks the person to confirm before it takes another owner's lock, in `apply --force` as well, and `-y` or `--auto-approve` skips that question. Never add either flag to answer for them.

A watching `progress` is not read-only: its keys stop or cancel the apply, run its cutover, or close the revert window. To follow an apply, use `status` or `progress --no-watch`, which print once and exit.

The CLI also has commands with no PR comment form, which change SchemaBot for every team on the server rather than one change: `freeze`, `unfreeze`, `settings` given a value, `checks backfill`, and the `storage` commands other than `storage plan`. They are for SchemaBot's operators. Hand them over, in every environment. Their read-only forms, such as `freeze list`, `settings` with no value, `checks show`, and `storage plan`, you can run.

## When SchemaBot blocks or refuses a change

Stop. Do not apply again, look for a flag, or try another environment, deployment, or credential to get past it.

- Tell the user the reason SchemaBot gave, in its words. For a freeze, say which freeze holds the change, why, and when it ends.
- When the reason names a safe form of the change, rewrite the schema files into it, plan again, and show the user the new plan.
- When no safe form exists, such as a PostgreSQL column type change that rewrites the table, say so. That change needs a separately reviewed process that a person runs.
- A `401` or `403` means the account lacks access. Say which permission is missing. Never try other credentials or turn off authentication.

You may run `freeze list` and `freeze show` to check on a freeze. [Refusals, blocks, and failures](refusals-and-failures.md) lists each outcome's signal and next step.

## Never act outside SchemaBot

Every change goes through SchemaBot, so it is planned, reviewed, and recorded. Never change the database another way, in any environment. Give the user the command instead.

- Never run DDL against the database or cluster yourself.
- On PostgreSQL, never drop an invalid index, cancel a backend, or run the `GRANT` a refusal names. A failed index build can leave an invalid index behind; the next apply removes it once it proves no build still holds it. Dropping it by hand can race a build the server is still running.
- On Vitess, never act in the PlanetScale console: do not delete a branch, or approve or revert a deploy request.

## The user can opt out

If the user says they post every SchemaBot command themselves, hand over every command, in every environment, for the rest of the session. Asking which they prefer is always reasonable.

## Comment format

SchemaBot reads the first line of a comment that starts with `schemabot` (up to three leading spaces are allowed). It skips lines inside code blocks, quotes, and HTML comments. So `🤖 schemabot apply -e staging` on one line is not a command, and neither is a fenced one: nothing replies and the apply never starts.

Post plain text: the disclosure line, a blank line, then the command on its own line.

```text
🤖 This database has no other dependents and this change is additive, applying in staging only; production applies are never posted automatically

schemabot apply -e staging
```

That is the whole comment body. No code fence, no bullet or quote marker, one command per comment. Hand the human the same shape when the command is theirs to post.

The blank line is for readers: without it GitHub renders both lines as one paragraph, and the command reads as part of the sentence.

The disclosure line states:

- **who decides**: the 🤖 tells readers an agent posted this, not the account owner;
- **why you may post it**: what you found about who depends on the database;
- **what the change does**: factually, such as *additive*, *adds a nullable column*, *adds an index*;
- **the limit**, stated as policy: production applies are never posted automatically.

Never call a change "safe". SchemaBot flags drops, but not every change that can lose data, such as a narrowing type change (see [Unsafe changes and drops](change-behavior.md#unsafe-changes-and-drops)), and an additive change can still be a long, heavy copy.

Don't start the disclosure line with the word "SchemaBot". Matching ignores case, so a line like `SchemaBot apply requested by …` can be read as a command.

Keep it to one line. Your reasoning and what came back belong in your reply to the user. SchemaBot's own plan and progress comments cover the rest.

## Copy commands, don't build them

SchemaBot's comments end with the exact next command: apply ID, `-e`, and any required flags filled in. Copy that command rather than writing one. A copied command still follows this policy: one that carries `--allow-unsafe` or `--override` is still the human's to post. `schemabot help` posts the full command table.
