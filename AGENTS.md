# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

## Project Overview

SchemaBot is a declarative schema GitOps orchestrator for MySQL via Spirit and Vitess via PlanetScale. It makes running database schema changes safe and easy through GitHub PR comments and an Admin CLI.

## Build and Test Commands

```bash
make build                # Build binary → bin/schemabot
make test                 # Run ALL tests (unit + integration + e2e)
make test-unit            # Unit tests only (fast, no containers)
make test-consumer-module # Consumer-module startup tests (fast, no containers)
make test-integration     # Integration tests (testcontainers)
make test-e2e             # E2E tests (docker-compose: local + gRPC)
make test-e2e-local       # E2E local tests only
make test-e2e-grpc        # E2E gRPC tests only
make lint                 # golangci-lint via Docker
```

**Always run `make test` (full suite)** to validate changes before a PR push when the scope of the change is unclear or crosses package boundaries. Only use `make test-unit` or `make test-integration` when you are confident the change is scoped to a single layer.

**Never assume test failures are unrelated to your changes.** Always investigate failures deeply — even if they appear in code you didn't touch — and fix them if possible.

**Detecting flakes:** Use `scripts/test-flaky.sh <TestName> [iterations] [package]` to run a specific test multiple times and confirm stability. Always use this after fixing a flaky test to verify the fix holds (e.g., `scripts/test-flaky.sh TestMultiKeyspaceDDLDeploy 5 ./pkg/localscale/...`).

**Never increase timeouts to fix flakes.** If a test is slow or flaky in CI, find and fix the root cause — don't mask it by increasing deadlines or poll timeouts. Common root causes: resource contention, missing cleanup between tests, container startup races, unbounded retries.

**Never skip tests in CI.** All tests must run in every CI job. If a test fails because of a missing dependency (Docker image, container, service), fix the CI workflow to provide it — don't skip the test. Prefer fixing underlying issues over shortcuts or workarounds.

## Test Architecture

Four test layers, each with its own make target:

- **Unit** (`make test-unit`, `./...`) — No containers, no network. Fast. Tests individual functions and packages with `-race`.
- **Consumer module** (`make test-consumer-module`, `e2e/consumermodule/`) — No containers, no network, no build tags. A nested Go module that embeds schemabot the way a host binary does, pinning newer dependency versions than this repo to prove the public startup surface tolerates version skew. Runs in the Unit Tests CI job. Module-boundary tests belong here.
- **Integration** (`make test-integration`, `-tags=integration ./...`) — A mix of Docker containers (MySQL via testcontainers) and in-process server/gRPC components. Tests cross-package interactions without requiring a full deployment. Lives in `pkg/` and `integration/`.
- **E2E** (`make test-e2e`, `-tags=e2e ./e2e/...`) — Full docker-compose stack. All components (SchemaBot server, MySQL instances) are real containers — nothing is in-process or mocked. Tests the CLI against a running system. Lives in `e2e/local/` and `e2e/grpc/`.

Integration tests are the workhorse — most test coverage lives here since they're cheaper to run than full e2e. E2E tests are more expensive (docker-compose setup/teardown) but essential for validating the CLI against a real running system. Robust automated tests across all layers are the only safe way to evolve SchemaBot.

CI mirrors local dev — every CI test job runs make targets, not bespoke commands. The Unit Tests job runs `make test-unit` followed by `make test-consumer-module`; the other jobs each run their single corresponding target. The Integration job is the exception: it splits the packages across parallel runners, leaves out the LocalScale packages that have their own job, and gives Go a timeout below the job's so a hung test panics with a goroutine dump naming the culprit. `make test-integration` runs everything in one pass on the make target's own timeout, so a CI-only timeout will not necessarily reproduce locally.

## Git

- **Always verify the active branch before starting work.** The deployed code may be on a feature branch (e.g., a worktree), not `main`. Before implementing, check which branch has the code you're building on: `git log --oneline <branch> -- <relevant-file>`. Never branch from `main` for features that depend on unreleased work — branch from (or commit directly to) the active development branch.
- **Branch names must describe the work.** Use `<your-handle>/<short-topic>` — the topic being the change itself, in words a reviewer recognizes (`.../reaper-lease-gate`, `.../pull-api-rate-limit`). A branch name is a permanent label: it is what shows up in the PR list, in CI job names, in `git log --graph`, and in every link back to the work months later. Generated or placeholder names (`claude/intelligent-moser-4a453a`, `feature-1`, `patch-2`, a bare ticket id) say nothing, so every reader has to open the diff to find out what the branch is. When a tool creates the branch for you, rename it before the first push (`git branch -m <new-name>`); if it has already been pushed, push the new name and delete the old remote branch.
- **Never bypass pre-commit hooks.** Do not use `--no-verify` or `core.hooksPath=/dev/null`. If the hook fails, fix the issue.
- **PR summaries should be concise** — a short paragraph or bullet list highlighting key changes and why, not low-level implementation details. Do not include test plans, checklists, or verification details. Never reference internal company details (specific database names, staging environments, team names, internal URLs, deployment hostnames, org names, or test repo names) in PR titles or descriptions — this is a public OSS repo.
- **Every PR summary carries an ASCII diagram when the change has a shape to show.** Any change to flow, architecture, state transitions, concurrency, lifecycle, or ownership gets one, so a reviewer sees the new behavior without reconstructing it from the diff. Skip it when the change has no such shape — a comment, a test, a copy edit, a dependency bump — or when the shape is small enough that one sentence states it in full, such as a one-line predicate change. Beyond that, "the prose covers it" is not a reason to skip: a flow that takes a paragraph to describe is one the reviewer needs to see.
- **Show before and after when the change rewrites behavior rather than adding it.** Complex state changes and reworked logic are where the contrast carries the point: an after-diagram alone leaves the reviewer to hold the old behavior in their head and spot the difference themselves, which is exactly the step where a reviewer misses that a transition moved or a branch went away. Label the two, and draw them to the same shape and node order so the difference between them is the only thing that stands out:

  ```
  Before                     After

  ┌───────────┐              ┌───────────┐
  │  running  │              │  running  │
  └─────┬─────┘              └─────┬─────┘
        │ cancel                   │ cancel
        ▼                          ▼
  ┌───────────┐              ┌───────────┐
  │ cancelled │              │ releasing │
  └───────────┘              └─────┬─────┘
                                   │ copy quarantined
                                   ▼
                             ┌───────────┐
                             │ cancelled │
                             └───────────┘
  ```
- **Align the diagram in a second pass.** Put every diagram in a fenced code block (outside one, Markdown collapses the whitespace the alignment depends on), then re-read the finished block as a grid and fix the columns: box borders and corners line up column-for-column, a box is as wide as its widest content, and every connector meets the edge it points at. Ragged box lines are harder to read than no diagram, and they are invisible while writing one — checking is a separate pass over the rendered result, not something to trust to the first draft.
- **UX changes show the rendered result, collapsed.** When a PR changes what users see (PR comments, check summaries, CLI output), embed the new rendering in the PR summary inside collapsed `<details>` blocks — one per scenario, using the TEMPLATES.md preview output where one exists — so reviewers see the actual UX without checking out the branch. Links to TEMPLATES.md anchors are not a substitute; the rendering belongs in the summary itself.
- **Agent PR disclosures belong at the bottom.** When an agent writes or updates a PR summary/body, put the agent disclosure line after the summary content, not at the top.
- **Do not create PRs automatically.** Wait for the user to explicitly ask before running `gh pr create`. Pushing a branch is fine; creating the PR is a separate decision.
- **Create PRs in draft mode** (`gh pr create --draft`) by default. The author will mark it ready for review.
- **After pushing new commits**, check if the PR title and summary need updating to reflect the new changes. If a human has edited the summary, leave it alone.
- **Never squash after a human has reviewed** (comments or approval) — add new commits so reviewers can see incremental changes. Before human review, squash freely to keep the PR clean.
- **Upstreaming large branches:** Use the "leaf approach" — map the dependency graph of changes, peel off leaf nodes (changes with no downstream dependents) as small independent PRs first, ordered by topological dependencies. After leaves merge, the remaining bulk is smaller and more focused.
- **Never delete tests from other PRs when squashing.** Before squashing, diff against `origin/main` and verify you're not removing tests, functions, or code that was added by other merged PRs. If `origin/main` has moved ahead, rebase first to pick up the new code. A squash that removes existing tests is a regression.
- **Agent-generated issues:** When filing a GitHub issue, always include the agent name and model at the end of the body (e.g., "*This issue was generated by [agent name] ([model]).*"). **Only file issues when explicitly asked** — default to implementing the fix directly.
- **Copilot review comments:** GitHub Copilot auto-reviews every PR. When comments are available, review them before declaring the PR ready. Its suggestions are generally good — incorporate them when valid. But always verify they are contextually correct before implementing; don't blindly apply them. Copilot is an automated reviewer, not a human, so — unlike human review threads (see the next bullet) — you may reply to and resolve its comments without separate approval, provided that for each comment you:
  1. Reply describing the fix you made and linking the commit that made it (or, if you concluded the suggestion was wrong, reply explaining why instead of applying it).
  2. Prefix the reply with 🤖 and end it with the agent disclosure (agent name and model).
  3. Resolve the thread only after that reply is posted.
- **Never post PR comments without explicit approval.** Do not reply to review comments, post explanations, or resolve threads automatically. Show the user what you want to say and let them decide. PR comments are visible to all reviewers and represent the author's voice — agents should not speak on behalf of the author without permission. (Exception: automated Copilot review comments may be replied to and resolved without separate approval, per the Copilot rule above. This exception does not extend to human reviewers.)
- **Never post Amp thread comments as a work update.** Keep agent status, handoffs, and explanations in the current chat or in committed tracking/docs files when explicitly requested. Do not add comments to Amp threads or linked Amp conversations unless the user explicitly asks for that exact action.

### Commit Message Scopes

Use `fix(github)` or `feat(github)` for changes that affect the GitHub PR UX (comments, check runs, reactions) — even if the code lives in `pkg/webhook/`. The scope describes *what the user sees*, not the package name.

Use `fix(observability)` or `feat(observability)` for changes that affect telemetry, metrics, tracing, logging, dashboards, or operator-facing observability behavior — even if the code lives in another package. Use `feat(observability)` when adding or expanding observable signal, such as a new metric, label/dimension, trace attribute, or structured log field.

### Commit Checklist (MANDATORY — do these every time before `git commit`)

1. `gofmt -w` on all changed `.go` files
2. `go build ./...` passes
3. Ensure `~/go/bin/golangci-lint` is on PATH (built with the project's Go version)
4. `git commit` (NO `--no-verify` — EVER)
5. If hook fails: fix the issue, re-stage, commit again

### Pre-commit Hook (`scripts/lint-fix.sh`)

The pre-commit hook runs `golangci-lint` on staged Go files. It works in two passes:

1. **Auto-fix pass**: `golangci-lint run --fix` fixes formatting issues (gofmt, imports) in the working tree, then re-stages the fixed files.
2. **Verify pass**: `golangci-lint run` (no `--fix`) confirms no remaining issues. If issues remain, the commit is blocked.

When a commit fails due to lint:
- Read the error output to identify the issues (usually gofmt formatting or `usetesting` lint).
- Fix the issues in your code (e.g., run `gofmt -w <file>` for formatting, replace `context.Background()` with `t.Context()` in tests).
- Re-stage and commit again.

The hook uses `--new-from-rev` to only flag issues introduced by the current branch (not pre-existing issues in the codebase).

**Hook setup:** Run `make setup` to configure git hooks (sets `core.hooksPath` to `.githooks/`). This uses a relative path so it works in worktrees too. After creating a worktree with `git worktree add`, run `scripts/worktree-init.sh` to ensure hooks and tooling are configured.

## Guidelines

**Database integrity comes first, for every database.** SchemaBot runs in front of everything from a vibe-coded weekend experiment to a tier-0 production system, and it treats them all the same way: the integrity of the database behind it outranks convenience, speed, and feature scope. Do not frame anything, in code, docs, or review, in terms of "databases that matter" or shortcuts that are "fine for a small database". Every database matters, and a change that would be unsafe in front of production is unsafe everywhere. When a tradeoff pits integrity against anything else, integrity wins.

**Keep the schema intelligence guide in sync with the API.** When changing the request or response payloads, field semantics, filters, limits, or availability of `GET /api/databases`, `POST /api/pull`, `GET /api/status`, `GET /api/history/{database}`, `GET /api/plans`, `GET /api/plans/{plan_id}`, `GET /api/progress/apply/{apply_id}`, `GET /api/logs`, `GET /api/logs/{database}`, or `GET /api/locks`, update [docs/schema-intelligence.md](docs/schema-intelligence.md) in the same PR. Update the affected explanations and request/response examples, including catalog, lint, and deployment-specific payloads. Changes to the corresponding CLI commands or rendering must also update the guide's command/output examples. Every command example must have matching visible output; use fictional values and the actual response types or CLI formatters.

**Terminology:** NEVER use the word "migration" in code, comments, CLI output, or error messages. ALWAYS use "schema change" instead.

**"schema change" is never hyphenated.** In prose — PR comments, check summaries, CLI output, error messages, docs, code comments — write "schema change", never "schema-change".

This holds **including where it modifies a noun**: "the schema change engine", not "the schema-change engine". That is a deliberate departure from the usual compound-adjective rule, which would hyphenate it. "schema change" is a fixed term in this product and it does not split, so do not "correct" the unhyphenated attributive form back — in either direction, in review or in a docs pass.

Constructions that only read correctly when hyphenated are rewritten rather than hyphenated:

| Write | Not |
|---|---|
| a logger scoped to this schema change | a schema-change-scoped logger |
| a PR with no schema changes | a no-schema-changes PR |
| restarts part-way through a schema change | restarts mid-schema-change |

The rule is about prose only. Identifiers keep whatever form they need: file and script names (`scripts/generate-schema-change.sh`), CLI subcommands, external URLs, generated markdown anchors, and test fixture values are left alone. So is the literal inside a longer word: `VSchema-changed` is a different term and is not a hit.

`make check-terminology` (`scripts/check-terminology.sh`) enforces this in CI on every PR, docs-only ones included. It strips URLs and anchor targets, matches only at a word boundary, and allowlists the identifiers above by value, so a new legitimate hyphenated identifier means extending that allowlist, not weakening the rule.

**Driver terminology:** A *driver* is the operator worker that claims an apply via `FOR UPDATE SKIP LOCKED`, holds its lease (`LeaseOwner` / `LeaseToken`), and *drives* it to a terminal state. Use *driver* (noun) and *drive* (verb) — not *worker* — in new code, comments, logs, docs, and tests for the lease-holding goroutine and the work it performs. This is distinct from the **Go MySQL driver** (`github.com/go-sql-driver/mysql`, `database/sql/driver`): always refer to that as the "Go MySQL driver" or "SQL driver" and keep it import-qualified so the two senses never collide.

**OSS-ready code:** Never reference internal company names or proprietary details in code or comments.

### Runtime Invariants

[docs/invariants.md](docs/invariants.md) is the canonical registry of the MUST-statements that make SchemaBot safe in front of tier-0 databases. It is organized into families — merge gating (MG), apply state machine (ST), ownership and leases (OW), control operations (CO), operator surfaces (UX), recovery (RC), review and validation (RV), authorization (AZ), and availability (AV) — and each entry states the rule, what breaks if it is violated, and where it is enforced. Every entry describes shipped behavior, not intent.

**Before changing safety-relevant code, read the invariants that govern it.** Anything touching check state, apply state transitions, lease claims, control requests, reconciliation, DDL classification, authorization, or the drive loop is governed by at least one entry. Find them first — the failure mode of these paths is not a wrong answer, it is a confidently wrong one that unblocks a merge or half-changes a database, and a change that reads correct in isolation can still dissolve an invariant that spans two files.

**Cite the invariant by ID** in the PR summary and in review findings, so a reviewer can check the change against the stated rule instead of re-deriving it. "This preserves MG-5" and "this violates CO-2 — the failed request is retried forever" are both reviewable claims; "this is safe" is not.

When a change interacts with an invariant, one of these is true, and the PR should say which:

- **It upholds it.** Nothing further is needed beyond a test that would fail if it stopped holding.
- **It extends the enforcement** to a new engine, dialect, or path. The invariant text usually needs no change — check whether its *Enforced:* line still names where enforcement lives.
- **It establishes a new invariant.** Add the entry in the same PR. Give it the next ID in its family, state the rule and an *Enforced:* line naming where enforcement actually lives, and prefer a completeness test over the relevant registry to a hand-maintained list, so the next regression fails CI instead of review. Add a *Breaks if violated:* line where the consequence is not obvious from the rule; most entries do not need one and do not have one.
- **It weakens or removes one.** This is a deliberate safety decision, never a side effect. Say so explicitly at the top of the PR summary with the reasoning, and update or delete the entry in the same PR — an invariant the code no longer honors is worse than no entry at all, because reviewers rely on the registry being true.
- **It moves the enforcement without changing it.** Renaming, moving, splitting, or merging a file or package named in an `*Enforced:*` line is not a behavior change, but it makes the registry wrong. Update the citation in the same PR. This is the likeliest way an entry rots, precisely because nothing about the change looks safety-relevant, so it is the case least likely to prompt anyone to open the registry.

**An enforcement pointer can be wrong in two ways, and only one of them is obvious.** It can dangle, naming a file that no longer exists. It can also resolve cleanly to the wrong file, when enforcement moved and the old file stayed behind, or when the pointer was mistaken to begin with. The first is mechanical. The second needs someone to ask whether the named file actually contains the mechanism the entry describes, which is the question worth asking whenever you touch a pointer: not "does this path exist" but "is this still where the rule is enforced".

**Reviewing:** check the diff against the invariants its blast radius touches, not only against the ticket. An invariant broken silently is the most expensive class of defect here, since the registry is what future reviewers will trust. If a PR summary claims an invariant holds, verify the claim in the code rather than accepting it.

Do not add aspirational entries. If the behavior is not enforced in shipped code, it belongs in a design doc or a TODO, not in the registry. Process-level engineering rules (how we write and review code) belong in this file, not there.

### PR Self-Review Bar

Before handing a PR to the user, review it the way an operator will experience it during an incident:

- **Invariants first.** Identify which entries in [docs/invariants.md](docs/invariants.md) the change touches and state, per *Runtime Invariants* above, whether it upholds, extends, establishes, or weakens each one.
- **Safety gates first.** GitHub Check Runs are a tier-0 SchemaBot safety feature. Any code that creates, stores, reconciles, or aggregates check state must fail closed. Storage uncertainty, GitHub API uncertainty, ownership ambiguity, or in-flight apply ambiguity must never be converted into a passing check.
- **Check Run vs stored check state.** Use precise terms. A GitHub Check Run is the visible object on the PR commit. Stored check state is SchemaBot's database record used to decide what Check Run to create or update. Comments, logs, tests, and docs should make clear which one is being changed.
- **TOCTOU review.** For async flows, webhooks, reconciliation, and background watchers, ask what happens if the latest commit on the PR branch changes, another pod writes first, an apply starts or finishes concurrently, or a rollback changes the target schema. Use conditional storage updates, ownership identifiers, and final state reloads where stale workers could otherwise overwrite newer state.
- **Started applies remain authoritative.** Once an apply has started, a later PR commit that removes the schema change must not make the aggregate check pass by cleanup alone. Example: commit A adds a column and starts an apply; commit B removes that column before the apply finishes. The stored state must continue to block merge until an operator reconciles the target environment.
- **Logs must answer the triage question.** Error and warning logs on critical paths should include the identifiers an operator needs: repo, PR, head SHA when relevant, environment, database type, database, apply ID or identifier, check ID or Check Run ID, and the operation being attempted. For apply/operation/task logs, build these with the `LogAttrs()` helpers (see *Go Style → Logging*) rather than hand-listing identifiers.
- **Metrics are for what can go haywire; logs are for what is rare.** Add a new instrument only when its subject has enough volume for a rate, a trend, or a threshold to mean something and you would alert on it — retry storms, claim contention, throttling, error rates on a hot path. Log a rare dangerous branch well instead; when it should be countable, the answer is usually a new status or reason value on an existing hot-path counter rather than a counter of its own. Several existing counters predate this bar, so don't extend the pattern by copying a neighbor. When a metric does earn its place, the operator action it implies should be obvious from its name, attributes, and nearby code comments or docs. The reasoning, and which existing counters are the exception, is in [Adding New Metrics](pkg/metrics/README.md#adding-new-metrics).
- **Readability is a review gate.** Extract named helpers for compound state predicates, separate error handling from state decisions, and keep comments focused on invariants and operator-facing behavior. Avoid clever SQL, dense boolean expressions, and control flow that requires reconstructing the state machine in your head.
- **Tests must prove documented behavior.** If a PR summary or docs mention an edge case, add focused tests for it or explicitly call out why it cannot be tested in this layer. Prefer integration-style webhook/storage tests for check-run lifecycle behavior.

### Control Operations

Every operator control operation (stop, start, cutover, cancel, revert, skip-revert — and any new ones) must provide, on every engine:

- **A durable request path.** Record the request in storage (`apply_control_requests`) before acknowledging it, and have the driver consume it from storage. A pod restart, lease handover, or webhook redelivery must never lose an acknowledged command. In-memory flags are per-drive optimizations layered over the durable record, never the source of truth.
- **Comment rotation when the effect lands.** When the operation takes effect on a still-active apply, the comment observer posts a fresh progress comment at the bottom of the PR timeline and freezes the superseded comment into a collapsed details block pointing at its successor — operators look at the bottom of the PR for the effect of the command they just issued, not at an older edited comment. The rotation signal must be durable and cross-pod safe (recorded on the tracked comment row, e.g. `posted_phase`), so a fresh observer on a later drive claim rotates at most once per recorded signal value. Rotation exists to surface an operator's command: when the same apply state is also reachable without one (e.g. skipping-revert via revert-window expiry), gate the rotation on the operator-origin signal — the durable control request — so routine applies transition in place. Operations that resolve directly to a terminal state (e.g. cancel) surface through the terminal summary comment instead, which already lands at the bottom of the PR — do not rotate on top of it.
- **Engine-agnostic rotation signals.** Key rotation on apply state (`pkg/state`) or other engine-agnostic stored fields, not engine-specific metadata, so every engine gets the same PR UX.

**Engine-agnostic interfaces:** The engine interface (`pkg/engine`), tern proto (`pkg/proto`), and API types (`pkg/apitypes`) must not contain engine-specific fields (e.g., PlanetScale branch names, Spirit checkpoint details). Use generic `Metadata map[string]string` fields for engine-specific data. Engine-specific types belong in the engine package (e.g., `pkg/engine/planetscale/`).

**Never silently fail:** Prefer returning errors over silently swallowing them. If a function encounters a condition it can't handle, return an error — don't log and continue. Callers should decide how to handle errors, not the callee. The `Canonicalize` / `FormatDDL` functions in `pkg/ddl/format.go` are an exception: they are best-effort display formatters where returning the original string on parse failure is acceptable.

**Error early, never swallow.** Functions must return errors, not log and swallow them. A function that logs an error and returns void forces every caller to silently proceed as if nothing went wrong — this leads to corrupted state and subtle bugs. Patterns to avoid:
- `func doThing() { if err != nil { log(err) } }` — should be `func doThing() error`
- `if err != nil { log(err); continue }` in loops — return error unless the iteration is truly independent (e.g., polling different databases where one failure should not block others)
- `_ = someFunc()` — never discard errors; check and propagate them
- Background goroutines that cannot return errors to callers should transition to an error state (e.g., `dr.Error`) and return, not silently continue with partial results

**Do not conflate causes in one branch.** Avoid guards like `if err != nil || value == nil` when the two cases have different meanings, severity, or log fields. Separate handling is always better for triage: operators should be able to tell whether storage failed, a row was missing, ownership changed, or an invariant was violated. This also avoids misleading logs such as `"error": nil`.

**Do not dedupe unrelated branches with `||`.** A shared implementation is only better when the reasons for taking the branch are the same. If different engines, states, or modes need the same action for different reasons, prefer separate branches that call a shared helper, or extract a named policy helper that explains why the cases belong together.

**Name compound predicates.** When a conditional combines several checks, especially state-machine checks or 3+ boolean terms, consider extracting a small helper with a behavior-focused name. Keep error handling separate from state predicates so the caller can log or return the right cause, and the helper can read like the invariant being checked.

**Wrap errors with context:** Always wrap errors with `fmt.Errorf("context: %w", err)` instead of returning bare `err`. The context should include *what was being done* and *key identifiers* — not just the method name. For example, `fmt.Errorf("proxy query %s: %w", query, err)` tells you which query failed, while `fmt.Errorf("exec query: %w", err)` just restates the function name. Include the data that will help someone debug the issue from the error message alone.

**Info logging on critical paths:** Add `slog.Info` logging at key decision points and state transitions in critical-path code (server startup, request handling, background processors). Logs should include relevant identifiers (org, database, keyspace, branch) so operators can trace the flow. Don't log in hot loops or purely internal helpers — focus on boundaries and state changes.

**No silent branch cases.** Every `continue`, `return`, or early-exit in a conditional branch must have a log statement explaining why. Use `slog.Debug` if the case is expected and frequent (e.g., polling loops), `slog.Warn` if it indicates a surprising or degraded condition. Never silently skip work — someone debugging a production issue needs to see why a code path was taken. This includes:
- `if !ok { return }` after helper calls — the helper must log, or the caller must
- `if err != nil { continue }` in loops — always log the error before continuing
- `_ = someFunc()` — never discard errors silently; log them even if the operation is best-effort
- Fallback branches (e.g., "assume changed on error") — log why the fallback was taken

**No ambiguous log messages.** Log messages must state what *will* happen, not what *might* happen. During a sev, operators need to know the impact immediately — "may be blocked" wastes time investigating whether it actually is. Use definitive language: "check gate will block PR applies" not "PR applies may be blocked".

**Never render untrusted error strings in PR-/operator-facing markdown.** Raw error text — dial failures, DSN fragments, hostnames, IPs, driver internals — leaks internal infrastructure detail into a public PR comment and can break table layout. In PR comments, check summaries, and previews, show a fixed sanitized line (e.g. `"diff failed; see server logs"`) and log the raw error server-side with the deployment/target identifiers so the root cause is still triageable. When a short summary must include caller-influenced text, sanitize it first — collapse newlines, neutralize the `|` cell separator, and clamp to the column width (see `clampDriftSummary` in `pkg/webhook/plan_drift.go`). This is a corollary of *error early, never swallow*: the error is still surfaced (in logs), just never rendered raw to a public surface.

**No bug references in code:** When fixing a bug, write code and tests as if the correct behavior always existed. Don't add comments like `// this is where the bug was`, `// regression test for #123`, or `// without this fix, X breaks`. The git history has the context. Code and test comments should describe *what* and *why*, not the history of what was wrong.

**No "what changed" comments:** Don't add comments explaining what was removed, moved, or changed. Comments like `// Previously this was X, now it's Y` or `// The first progress poll transitions to the correct state` describe the change, not the code. If the code is clear without the comment, omit it. The git diff shows what changed.

**No PR/issue links in code comments.** Don't reference GitHub issues or PRs (e.g., `// fixes #147`) in code comments — they go stale and provide no value to future readers. The git history links commits to issues; code comments should describe *what* and *why*, not *when* or *which ticket*.

**Preserve useful comments.** When moving or refactoring code, move the relevant comments with the code or update them to match the new structure. Delete comments only when they are truly out of date, misleading, or redundant after the change. Comments that explain operational behavior, invariants, or rationale should not be dropped just because the code moved.

**No fragile comments.** Don't include specific counts, thresholds, external project names, or implementation details that will go stale when the code changes. For example, `// 100k rows` will be wrong when someone changes the count. `// uses the misk pattern` means nothing to someone who doesn't know misk. Comments should describe *intent* — the *what* and *why* — not restate the code or reference external context that may not persist.

**No `nolint` directives.** Always fix the underlying lint issue rather than suppressing it with `//nolint:`. If the linter flags something, the code should be changed to satisfy it.

**Instant DDL terminology:** In MySQL, "instant DDL" has a specific meaning — `ALTER TABLE` operations that only modify metadata (e.g., adding a column with `ALGORITHM=INSTANT`). `CREATE TABLE` and `DROP TABLE` are **not** instant DDL even though they complete quickly. Don't describe them as instant in code or comments.

### SQL Parsing: Real-Parser Requirement

All SQL statements processed by SchemaBot **must be parseable by the dialect's real parser**. For the MySQL family (MySQL/Vitess/Strata) that is the TiDB parser (via Spirit's `statement.New()` and `statement.ParseCreateTable()`); for PostgreSQL it is libpg_query (via the `pkg/ddl` `StatementParser` obtained from `ParserForDialect`). This is a hard requirement, not a best-effort behavior, and the same discipline applies to both grammars.

- **Do not** add fallback logic (e.g., `strings.Split(content, ";")`) when the parser fails. If a statement cannot be parsed, that is an error that must be surfaced to the caller.
- **Do not** silently skip unparseable statements with patterns like `if err != nil { continue }` unless the error is an expected type-filtering condition (e.g., `ParseCreateTable` returning an error for an `ALTER TABLE` statement).
- Schema files are expected to contain DDL valid for their target dialect. If the dialect's parser cannot handle a valid construct, that is a bug to fix in the parser dependency (the TiDB parser / Spirit, or libpg_query), not something to work around with string splitting.

### Upstream First: Fix the Engine, Not the Consumer

The rule above is one instance of a general one. **When a change reaches into a dependency's territory, first ask whether the fix belongs in the dependency.** Parser semantics, DDL diffing and canonicalization, schema introspection, progress and checkpoint state, and anything that has to know how an engine works internally are the domain of [`block/spirit`](https://github.com/block/spirit) and [`block/pg-sprite`](https://github.com/block/pg-sprite), not of SchemaBot. Both are ours to change. A fix that lands upstream fixes every consumer, gets the engine's own test suite behind it, and stays correct when the engine evolves. A fix that lands here has to re-derive the engine's behavior from the outside and breaks the next time the engine changes it.

Prefer the upstream change whenever it makes consumption simpler: a new exported function or option, a typed error instead of an error string to match on, a field on a status or result struct instead of a value to infer, a hook where SchemaBot currently peeks. Shortcuts that reach around the dependency are brittle by construction and are the last resort, not the first draft:

- parsing the engine's log lines or error text to recover a fact the engine already knows
- re-implementing part of its parser, differ, or introspection on our side, or string-manipulating DDL it already parses
- copying a private helper out of it instead of asking for it to be exported
- inferring engine state from side effects (table names, sentinel rows, timing) instead of from an API

When the upstream fix is right but cannot wait for a release, land the smallest workaround here with a comment stating what upstream capability retires it, and open the upstream change in the same sitting so the workaround has an expiry. The Vitess fork (see *Vitess Dependency*) follows the same rule for the code we carry on top of upstream Vitess.

## AWS Infrastructure

- **Always deploy from the worktree you're working in.** The deploy script uses `git rev-parse --show-toplevel` to resolve paths, so it builds whatever code is in your current working tree. Verify with `pwd` and `git branch --show-current` before deploying.
- **Always commit before deploying.** The Docker image is tagged by commit SHA — uncommitted changes are easy to lose and make it unclear what was deployed.
- **Never run `terraform destroy` directly.** Use `deploy/aws/scripts/destroy.sh` which requires explicit confirmation.
- **Always get user approval** before destroying AWS infrastructure, even when asked to "clean up" or "start fresh".
- Use the canonical scripts for all AWS operations. Two deployment layouts exist: `deploy/aws/` (single-env) and `deploy/aws-multi-env/` (multi-env with separate staging/production instances). Each has the same scripts:
  - `scripts/bootstrap.sh [-y]` — create/update all infrastructure
  - `scripts/deploy.sh [--skip-build]` — build and deploy new image
  - `scripts/destroy.sh` — tear down all infrastructure (always interactive)
  - `scripts/bootstrap-planetscale.sh` — bootstrap PlanetScale database (multi-env only)

## Go Style

### Testing

- **Always use testify** (`require` and `assert` from `github.com/stretchr/testify`) in tests. Never use raw `if err != nil { t.Fatalf(...) }` patterns.
  - Use `require` for preconditions and setup where failure should stop the test.
  - Use `assert` for verification checks where you want all assertions to run (e.g., checking multiple fields).
- **Use `t.Context()`** instead of `context.Background()` or `context.TODO()` in tests. This ties the context to the test lifecycle — the context is cancelled when the test ends. Test helper functions that need a context must accept `*testing.T` and use `t.Context()` — never `context.Background()`. **Exception: cleanup code** (`t.Cleanup` callbacks and deferred functions that run after the test finishes) must use `context.Background()` because `t.Context()` is already cancelled by the time cleanup runs.
- **Prefer integration tests** with MySQL testcontainers over unit tests when possible.
- **No `time.Sleep` for readiness waits.** Never use a fixed sleep to wait for a server or service to be ready. Instead, poll with a deadline (e.g., retry a health check endpoint in a loop with a short sleep between attempts). For testcontainers, use built-in wait strategies.
- **All timeouts must hard-fail.** Every operation in test code must have a bounded timeout (max 30s for any single operation). When a timeout fires, the test must fail immediately with a clear error — never silently continue or hang. No test should ever run longer than 30s for a single operation. Use `context.WithTimeout` and `require.NoError` on the result.
- **Use named polling deadlines.** When a test waits for async work and there is no case-specific reason for a custom duration, use the closest shared or package-level polling deadline constant instead of inline `30*time.Second` / `time.After(30 * time.Second)`. Use the constant for the correct test layer; `e2e/testutil.PollDeadline` is for real `e2e/` tests, not package-level integration tests such as `pkg/webhook/*_e2e_test.go`.
- **Explain scenario tests.** New E2E tests, webhook E2E tests, and integration tests for operator-facing flows should have a concise human-readable comment above the test that explains the scenario being exercised and the expected safety/UX outcome. Describe the behavior as if it always existed; do not reference PRs, bugs, or implementation history.
- **Do not add negative regression tests for removed behavior.** When deleting an API, flag, fallback, route, or compatibility path, remove its tests and references instead of adding tests that assert the old behavior is gone. Those tests preserve dead concepts in the codebase and add review/maintenance noise. Cover the supported replacement behavior and any safety invariant that remains.
- **Reduce duplication.** Look for opportunities to extract shared helpers when the same pattern appears 3+ times. In tests, prefer small composable helpers over copy-pasting setup boilerplate.
- **Assert on specific values, not just existence.** Don't check `assert.NotEmpty(result)` when you know what the value should be. Verify the specific table name, column name, DDL content, and state. For example, check both `strings.Contains(ddl, "ADD COLUMN")` and `strings.Contains(ddl, "email")` — not just one. Vague assertions pass when they shouldn't and make failures harder to diagnose.
- **Seed enough test data to avoid races.** When testing progress or in-flight behavior, seed enough rows that the operation takes measurably longer than a poll interval. If Spirit can finish before the first progress poll, the test is racy. Use `testutil.SeedRows` with 500k+ rows for operations that need to be observed mid-flight.

### State Comparisons

- **Always use `pkg/state` constants and helpers** for state comparisons. Never use raw string matching (`strings.Contains(strings.ToUpper(s), "STOPPED")`).
  - Use `state.Apply.Stopped`, `state.Apply.Running`, etc. for apply states.
  - Use `state.IsState(s, state.Apply.Stopped)` for case-insensitive, normalized comparison (handles proto prefixes like `STATE_STOPPED`).
  - Use `state.IsTerminalApplyState(s)` to check if a state is terminal.

### Logging

- **Use the `LogAttrs()` helpers for apply/operation/task logs.** When a `*storage.Apply`, `*storage.ApplyOperation`, or `*storage.Task` is in scope, build the triage attributes with its `LogAttrs()` method instead of hand-listing identifiers:
  ```go
  logger.Error("failed to drive apply", append(apply.LogAttrs(), "error", err)...)
  ```
  The helpers (`pkg/storage/logattrs.go`) return the canonical triage set — apply/operation/task identifier, database, database type, environment, deployment, state, and (when set) repo, pr, external id(s), caller. This keeps key names consistent and lets a failed or stalled apply be triaged from logs alone. Append call-specific attrs (`"error", err`, etc.) after the spread, and don't repeat identifiers the helper already provides (avoid duplicate keys).
- **Never log the internal numeric row ID** (`apply.ID`, `op.ApplyID`, `task.ApplyID`) — it is confusable with the user-facing string identifier and is not a useful triage handle. Log the string identifier (`apply_id` = `ApplyIdentifier`), which the helpers already do. When only an `apply_operation` is in scope (parent apply not loaded), `ApplyOperation.LogAttrs()` is sufficient on its own.
- **The operation's own deployment is the routing key.** When a log is scoped to an `apply_operation` but uses the parent `Apply.LogAttrs()` (whose `deployment` is the apply's), also add `"operation_deployment", op.Deployment` — the two can differ.

### Imports

- **No re-exports or wrapper functions.** Don't create functions that just delegate to another package (e.g., `func FormatNumber(n int64) string { return ui.FormatNumber(n) }`). Callers should import the source package directly. Re-exports add indirection without value. Exception: wrappers that add transformation or special-case handling beyond a plain delegation are fine.

### Resource Cleanup

- **Database resources and Spirit objects** (`*sql.DB`, `*sql.Rows`, `*sql.Conn`, Spirit runners, services): **always** use `defer utils.CloseAndLog(x)` (from `github.com/block/spirit/pkg/utils`). Never use `defer func() { _ = x.Close() }()` — it silently discards errors.
- **HTTP response bodies**: use `defer resp.Body.Close()`. The `bodyclose` linter requires an explicit close on the response — `utils.CloseAndLog` doesn't satisfy it.
- **One owner closes a handle; a redundant closer must not report the double close.** A handle (`*sql.DB` etc.) passed to a constructor that closes it in its own `Close()` is owned by that component — e.g. a store built with `New(db)` closes `db` when the service holding the store is closed. Prefer a single owner per handle, and add a second closer — commonly a test's open-time `t.Cleanup`, registered so an early failure can't leak the handle — only for early-exit leak safety. What that redundant closer may do depends on whether the handle's `Close()` is idempotent:
  - **`*sql.DB` is idempotent** (`sql.DB.Close()` returns `nil` on every close after the first, by design), so a redundant close is silent. Use `utils.CloseAndLog(db)` there like anywhere else — it logs nothing when the owner already closed the pool, and still surfaces a real close error. Do not reach for `_ = db.Close()`; `closeandlog` flags it, and there is no double-close error to hide.
  - **`*sql.Conn` is not** — a second `Close()` returns `sql.ErrConnDone`, and `utils.CloseAndLog` would log it on every teardown. Do not paper over that with a discarded close either. Restructure so the guard closes the handle only when the owner has not (e.g. an `atomic.Bool` set by the function that closes it), which keeps the close error checked and states the ownership in the code. `pkg/namedlock/namedlock_integration_test.go` shows the shape.

  Only the sole owner is expected to produce a close error; a redundant closer that can report one is a signal to fix the ownership, not to silence the report.

### Concurrency

- Use `wg.Go(func() { ... })` instead of `wg.Add(1); go func() { defer wg.Done(); ... }()`.
- For background goroutines that must outlive the request context, use `context.WithCancel(context.WithoutCancel(ctx))` to preserve context values (tracing) without inheriting the request deadline.
- Protect shared state with mutex: snapshot all fields needed from a struct in a single lock acquisition rather than multiple lock/unlock cycles.

### Database Connections

- Use `mysqlconn.Open()` from `github.com/block/schemabot/pkg/mysqlconn` for SchemaBot-managed MySQL connections. It centralizes DSN normalization, including required TLS settings for RDS targets. Use raw `sql.Open("mysql", ...)` only for local test/dev infrastructure (for example LocalScale) or engine-specific connection paths that intentionally manage their own TLS config (for example PlanetScale/Vitess mTLS).
- Use `postgresconn.Open()` from `github.com/block/schemabot/pkg/postgresconn` for SchemaBot-managed PostgreSQL connections — it is the Postgres counterpart of `mysqlconn` (DSN normalization, required `sslmode` for RDS targets). Reserve `postgresconn.OpenReloadable()` for the single long-lived storage pool. Do not open Postgres connections with raw `sql.Open("pgx", ...)` outside local test/dev infrastructure.
- After opening a database with `mysqlconn.Open()`, `postgresconn.Open()`, or raw `sql.Open(...)`, always call `db.PingContext(ctx)` to verify the connection works (Go's sql driver lazy-loads connections). An immediately error-checked `db.Conn(ctx)` right after open also satisfies this rule — for a freshly opened pool it is equivalent to a ping, since the empty pool forces a real dial and auth handshake. Outside that right-after-open window `db.Conn(ctx)` can hand back an already-pooled connection and verifies nothing, so it is not a general ping substitute.
- Always backtick-quote SQL identifiers: `` USE `db` ``, `` SHOW CREATE TABLE `tbl` ``.
- **Never manipulate DSN strings with `strings.Replace`.** Use `mysql.ParseDSN()` / `cfg.FormatDSN()` from `github.com/go-sql-driver/mysql` to parse, modify fields, and re-serialize. String manipulation is fragile and breaks on DSNs with passwords containing `/` or other special characters.

### Spirit Integration

- Use `status.State.String()` for Spirit state names instead of maintaining a separate mapping function.
- Use `utils.CloseAndLog(runner)` for Spirit runner cleanup instead of `_ = runner.Close()`.
- Use `table.LoadSchemaFromDB(ctx, db)` (from `github.com/block/spirit/pkg/table`) to load all table schemas from a database instead of manual `SHOW TABLES` + `SHOW CREATE TABLE` loops. Filter results with `ddl.IsSpiritInternalTable` when Spirit internal tables should be excluded.
- **Use `statement.New()` for DDL parsing** (from `github.com/block/spirit/pkg/statement`). Never hand-parse ALTER TABLE statements with string splitting — use Spirit's `AbstractStatement` which gives you `Table`, `Alter`, `Type`, and `Schema` fields. Use `statement.Classify()` for lightweight type detection without full parsing.
- **Use `ddl.ClassifyStatement()` for DDL type detection on MySQL-family SQL only.** Returns `(ddl.StatementType, tableName, error)` — handles the `Classify()` boilerplate (nil check, empty results). It classifies with the package's default parser (the TiDB/Spirit implementation), so it must never be used where the target's dialect is not fixed to the MySQL family: resolve the target's parser with `ddl.ParserForDialect` (or `LocalClient.statementParser` in `pkg/tern`) and call its `Classify` for anything dialect-sensitive. Use the typed constants (`ddl.StatementCreateTable`, `ddl.StatementAlterTable`, `ddl.StatementDropTable`) for branching, and convert between types and strings at storage/API boundaries via `ddl.StatementTypeToOp()` / `ddl.OpToStatementType()`.

### Vitess Dependency

- This project uses a fork of Vitess at [`github.com/block/vitess`](https://github.com/block/vitess), branch `release-23.0`, with per-shard sidecar database support for vtcombo and a MySQL topology server for Strata support.
- The fork is referenced via a `replace` directive in `go.mod`. When updating, point to the latest commit on `release-23.0` (not `main`).
- The fork adds two features on top of upstream Vitess v23.0.3:
  - `--per-shard-sidecar` flag for vtcombo/vttestserver, which gives each shard its own `_vt_{keyspace}_{shard}` sidecar database — required for online DDL and VReplication to work correctly in multi-shard test environments.
  - A MySQL-backed topology server implementation for Strata support.

### Storage Schema (Self-Bootstrapping)

SchemaBot's storage schema is self-bootstrapping via `EnsureSchema` (`pkg/api/ensure_schema.go`), which runs on every server startup before accepting traffic and routes to a per-dialect bootstrapper. On MySQL it reads all embedded SQL files from `pkg/schema/mysql/`, diffs them against the live database using Spirit, and applies any DDL needed — adding a new table or column to `pkg/schema/mysql/` is all that's needed; the next deploy picks it up automatically. On PostgreSQL (`pkg/api/ensure_schema_postgres.go`) it transactionally creates missing tables, columns, and indexes under the bootstrap advisory lock. PostgreSQL convergence is additive-only: it tolerates extra objects and checks columns by presence. Only missing tables, metadata-only columns, and standalone indexes converge automatically; a missing column that is `NOT NULL` without a `DEFAULT`, generated or identity, `UNIQUE`, `REFERENCES` with a `DEFAULT`, or carries an unclassified constraint shape fails startup with a manual-remediation error, as does a live index under an expected name that is invalid or non-unique where the schema requires uniqueness. Keep the two dialect directories in lockstep; the schema parity tests in `pkg/schema` pin this.

### SQL Schema

- **Every CREATE TABLE** must use the canonical `SHOW CREATE TABLE` format that Spirit expects (see `pkg/statement.ParseCreateTable`): `ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci`.
- Use `BIGINT UNSIGNED AUTO_INCREMENT` for primary keys.
- **No redundant indexes.** A single-column index is redundant if it's a left-prefix of a composite index (e.g., `INDEX (a)` is redundant when `INDEX (a, b)` exists).
