# gRPC Control Edge Cases

This document tracks gRPC Tern control-operation edge cases so reliability work
can stay explicit as `stop`, `start`, and `cutover` evolve across CLI and PR
comment workflows.

The gRPC path has two state sources:

- **Remote Tern state** — what the remote data-plane worker reports through
  `Progress`, and what it accepts through `Stop`, `Start`, or `Cutover` RPCs.
- **Stored SchemaBot state** — apply state, durable control requests, apply logs,
  and user-facing progress shown by CLI watch, PR comments, and check runs.

The scheduler is responsible for reconciling these state sources. When they
disagree, SchemaBot should fail closed: preserve operator intent, keep errors
visible, and avoid unbounded retries without a new operator request.

## Remote apply ID invariant

SchemaBot and remote Tern use different apply IDs in gRPC mode:

- `apply_identifier` is SchemaBot's public ID. CLI, PR comments, HTTP APIs,
  apply logs, checks, and storage lookups use this ID.
- `external_id` is the remote Tern apply ID returned by the remote `Apply` RPC.
  Every gRPC `Progress`, `Stop`, `Start`, and `Cutover` request must use this
  ID once it exists.

Apply-scoped API handlers must decode the user-facing `apply_identifier`, load
the stored apply row, and route remote gRPC calls with `external_id`. If the API
needs a remote progress sample to validate a control request, it must use the
same remote ID mapping as the scheduler poller. Sending SchemaBot's public
`apply_identifier` to remote Tern is a routing bug: remote Tern only knows its
own `external_id`, so the CLI or PR comment may receive a rejected control
response even though the remote apply is healthy and ready.

## Control request path

Control requests are operator intent. The API stores that intent durably, along
with caller metadata, before the scheduler performs remote control work. The
scheduler is the single owner that claims pending or stale work, executes the
request against remote Tern, and records the result back in SchemaBot storage.

```text
╭─────────────╮   ╭─────────────╮   ╭─────────────╮   ╭─────────────╮   ╭─────────────╮
│ Operator /  │   │ SchemaBot   │   │ SchemaBot   │   │ Scheduler   │   │ Remote Tern │
│ automation  │──▶│ API         │──▶│ storage     │──▶│ worker      │──▶│ data plane  │
╰─────────────╯   ╰─────────────╯   ╰─────────────╯   ╰─────────────╯   ╰─────────────╯
     request        validate +       durable intent     claim +          Stop / Start /
                    accept           caller metadata    execute          Cutover RPC
```

## Progress polling path

Progress polling is observation, not a new operator request. A remote progress
sample may confirm, complete, or fail a stored control request, but it should
not erase newer durable intent. CLI watch, PR comments, and checks read stored
state; they do not infer state directly from remote Tern.

```text
╭─────────────╮   ╭─────────────╮   ╭─────────────╮   ╭────────────────╮
│ Scheduler   │   │ Remote Tern │   │ SchemaBot   │   │ User-visible   │
│ worker      │──▶│ data plane  │──▶│ storage     │──▶│ observers      │
╰─────────────╯   ╰─────────────╯   ╰─────────────╯   ╰────────────────╯
     poll           progress         reconciled        CLI watch,
                    sample           state, errors,    PR comments,
                                     request result    checks
```

Progress polling also uses the remote apply ID invariant: the scheduler polls
remote Tern with `external_id`, then writes the reconciled state to SchemaBot
storage under the public `apply_identifier`.

## Current gRPC stop/start scenarios

Common setup: an operator requests `/stop`; SchemaBot records the stop request,
the scheduler stops remote Tern, and stopped progress is stored. `/start` is a
later, separate operator request. SchemaBot does not automatically start after a
completed stop request.

### Start RPC succeeds

| Step | Actor | What happens | User-visible result |
| --- | --- | --- | --- |
| 1 | Operator / CLI | Requests `/start` | CLI receives `start request accepted` |
| 2 | API | Records a durable start request | Start work can be recovered by the scheduler |
| 3 | Scheduler | Calls remote Tern `Start` | Remote Tern acknowledges the Start RPC |
| 4 | Scheduler | Polls remote progress | Remote reports running or completed progress |
| 5 | Storage / observer | Stores progress and completes the start request | CLI watch / PR comment / check show running or completed |

**End state:** apply is running or completed; the start request is completed.

### Start RPC fails or times out

| Step | Actor | What happens | User-visible result |
| --- | --- | --- | --- |
| 1 | Operator / CLI | Requests `/start` | CLI receives `start request accepted` |
| 2 | API | Records a durable start request | Start work can be recovered by the scheduler |
| 3 | Scheduler | Calls remote Tern `Start` | Remote call errors or times out |
| 4 | Scheduler | Stores stopped progress, warning apply log, and error message | CLI watch / PR comment show stopped with the error |
| 5 | Storage | Marks the start request failed | Operator can issue a new `/start` after investigating |

**End state:** apply remains stopped; the error is visible; the start request is
failed, so the scheduler does not retry forever.

### Start RPC succeeds, but remote progress remains stopped

| Step | Actor | What happens | User-visible result |
| --- | --- | --- | --- |
| 1 | Operator / CLI | Requests `/start` | CLI receives `start request accepted` |
| 2 | API | Records a durable start request | Start work can be recovered by the scheduler |
| 3 | Scheduler | Calls remote Tern `Start` | Remote Tern acknowledges the Start RPC |
| 4 | Scheduler | Polls remote progress during the bounded grace window | Previous stopped state does not cancel the start request |
| 5a | Remote Tern | Reports running or completed before grace expires | Storage adopts progress and completes the start request |
| 5b | Remote Tern | Still reports stopped when grace expires | Storage records stopped progress, warning apply log, and error message |

**End state:** either running/completed with the start request completed, or
stopped with a visible error and the start request failed, so the scheduler does
not retry forever.

## Current gRPC cutover scenarios

Common setup: the apply was started with `--defer-cutover`. The remote data
plane reaches a cutover-ready state, while SchemaBot storage may briefly lag
behind the latest remote progress sample. A cutover request is forward progress,
so it must be rejected while a stop request is pending.

When an apply is not started with `--defer-cutover`, no later cutover control
request is required. MySQL/Spirit proceeds through cutover as part of the apply,
and Vitess/PlanetScale creates the deploy request with auto-cutover enabled.
The durable cutover request path is for explicit operator cutover after a
deferred apply has paused.

### Cutover request accepted and remote Cutover succeeds

| Step | Actor | What happens | User-visible result |
| --- | --- | --- | --- |
| 1 | Operator / CLI | Requests `/cutover` using SchemaBot's apply ID | CLI receives `cutover request accepted` |
| 2 | API | Loads the stored apply, maps to remote `external_id`, and verifies readiness | Request is accepted if stored state or remote progress is cutover-ready |
| 3 | API | Records a durable cutover request with caller metadata | Cutover work can be recovered by the scheduler |
| 4 | Scheduler | Claims the apply and sends remote Tern `Cutover` with `external_id` | Remote Tern acknowledges the Cutover RPC |
| 5 | Scheduler | Polls remote progress until cutover resolves | Storage moves through cutting-over to revert window or completed |
| 6 | Storage / observer | Completes the cutover request | CLI watch / PR comment / check show the post-cutover state |

**End state:** apply is in revert window or completed; the cutover request is
completed.

For MySQL/Spirit, remote Tern may run behind a Kubernetes Service with multiple
data-plane replicas. The `Cutover` RPC can land on a replica that does not own
the in-memory Spirit runner. That must still succeed after durable readiness
checks, because Spirit's cutover signal is the database sentinel table rather
than process-local memory.

### Apply was not started with deferred cutover

| Step | Actor | What happens | User-visible result |
| --- | --- | --- | --- |
| 1 | Operator / automation | Starts apply without `--defer-cutover` | No later cutover command is needed |
| 2 | MySQL / Spirit | Runs the online schema change and proceeds through cutover without waiting on the sentinel | Progress advances to completed unless the apply fails or is stopped |
| 3 | Vitess / PlanetScale | Creates a deploy request with auto-cutover enabled | Progress advances through deploy/cutover to completed or revert window |
| 4 | SchemaBot scheduler | Polls and stores progress | CLI watch / PR comment / check show the normal apply lifecycle |

**End state:** the apply completes without a durable cutover request. If an
operator sends `/cutover` after the apply has completed, SchemaBot should treat
that as an already-resolved apply rather than starting new work.

### Cutover readiness is visible remotely before stored state catches up

| Step | Actor | What happens | User-visible result |
| --- | --- | --- | --- |
| 1 | Remote Tern | Reports `waiting_for_cutover` or `cutting_over` for `external_id` | Scheduler has not stored that sample yet |
| 2 | Operator / CLI | Requests `/cutover` using SchemaBot's apply ID | CLI should not be rejected only because local storage is stale |
| 3 | API | Checks remote progress using `external_id` | Remote readiness is accepted as evidence for queuing cutover |
| 4 | API | Records durable cutover request | Scheduler performs cutover through the single-owner path |

**End state:** cutover request is pending until the scheduler owner executes it.
The API must not call remote Tern with the SchemaBot apply ID during the
readiness check. A remote-only `cutting_over` sample does not by itself prove
SchemaBot already has a durable cutover request; if the stored apply is still
`running` or `waiting_for_cutover`, SchemaBot must still record the durable
request so the scheduler owner can reconcile completion.

### Cutover RPC fails or times out

| Step | Actor | What happens | User-visible result |
| --- | --- | --- | --- |
| 1 | Operator / CLI | Requests `/cutover` | CLI receives `cutover request accepted` |
| 2 | API | Records a durable cutover request | Cutover work can be recovered by the scheduler |
| 3 | Scheduler | Calls remote Tern `Cutover` with `external_id` | Remote call errors, times out, or is rejected |
| 4 | Scheduler | Stores an error apply log and failure reason | CLI watch / PR comment show the apply still waiting for cutover with the error |
| 5 | Storage | Marks the cutover request failed | Operator can issue a new `/cutover` after investigating |

**End state:** apply remains waiting for cutover or failed according to remote
progress; the error is visible; the cutover request is failed, so the scheduler
does not retry forever.

### Stored cutover is already in progress

| Step | Actor | What happens | User-visible result |
| --- | --- | --- | --- |
| 1 | Scheduler | Cutover has already been accepted and the stored apply is `cutting_over` | Storage shows cutover in progress |
| 2 | Operator / CLI | Requests `/cutover` again | CLI receives an accepted response that says cutover is already in progress |
| 3 | API | Treats the duplicate request as already in progress | No new durable cutover request is queued and no second Cutover RPC is sent |

**End state:** the original cutover continues. Duplicate cutover requests are
visible in apply logs and return `status=already_in_progress`, but do not create
another control request. This case is only idempotent when SchemaBot storage is
already `cutting_over`; remote-only `cutting_over` with stale stored state
follows the remote-readiness scenario above.

## Edge-case checklist

| # | Scenario | Expected behavior | Status |
| --- | --- | --- | --- |
| 1 | Duplicate `/start` while start is already pending | Return already-accepted semantics; preserve the original durable request and caller visibility | Covered for durable requests |
| 2 | Failed `/start` followed by scheduler recovery | Do not claim or retry the failed request automatically | Covered by storage integration test |
| 3 | Failed `/start` followed by a new operator retry | Reset the failed request to pending and make the apply claimable again | Covered by storage integration test |
| 4 | Start RPC fails or times out | Keep apply stopped, store error message, add warning apply log, fail the start request | Covered |
| 5 | Start RPC succeeds but progress remains stopped past grace | Keep apply stopped, store timeout reason, add warning apply log, fail the start request | Covered |
| 6 | Remote reports stale stopped progress shortly after Start RPC succeeds | Ignore stopped samples only during the bounded grace window | Covered |
| 7 | Remote reports running/completed during start grace | Adopt remote progress and complete the start request | Covered |
| 8 | Concurrent stop and start requests | Stop intent wins unless the start is a later explicit retry after stopped state is established | Covered for stop priority; keep auditing new call paths |
| 9 | Multiple operators request stop during an incident | Preserve caller visibility through durable request metadata and apply logs | Covered for stop logs; keep caller fields in future PR-comment path |
| 10 | Scheduler owner dies after accepting a durable control request | Next owner claims stale work and processes the pending request before forward progress | Covered by durable request model |
| 11 | Storage write fails after remote control RPC succeeds | Return/log error and avoid converting uncertainty into a passing or completed state | Audit when adding new control RPCs |
| 12 | Cutover requested while stop is pending | Reject forward-progress control while stop intent is pending | Covered for current cutover path |
| 13 | Remote cutover readiness is ahead of stored state | Check remote progress with `external_id`; never send SchemaBot `apply_identifier` to remote Tern | Covered for current cutover path |
| 14 | Cutover RPC fails or times out | Keep the apply blocked, store error message, add warning apply log, fail the cutover request | Covered for current cutover path |
| 15 | Apply was not started with `--defer-cutover` | Do not require a cutover request; MySQL/Spirit and Vitess/PlanetScale should auto-cutover through normal apply progress | Covered by engine behavior; keep explicit in reviews |
| 16 | Remote reports `cutting_over` but stored apply is not `cutting_over` | Treat as remote cutover readiness and queue the durable request so scheduler reconciliation is preserved | Covered for current cutover path |
| 17 | Cutover requested while stored apply is already `cutting_over` | Return accepted/idempotent with `status=already_in_progress`, log the caller, and do not queue another durable request | Covered for current cutover path |
| 18 | MySQL/Spirit cutover RPC lands on a non-owner data-plane replica | After durable readiness checks, any replica can drop the sentinel table; the owner runner observes the DB-side signal | Covered by K8s e2e |
| 19 | PR comment stop support | Same semantics as CLI stop: durable request first, caller visible, stop priority preserved | Covered |
| 20 | PR comment cutover support | Same safety gate as CLI/API cutover, including pending-stop rejection, plus durable cutover intent if async ownership requires it | Covered |
| 21 | Spirit checkpoint resume loses prior copy progress after restart | Surface lost-progress reason through Spirit resume status APIs instead of inferring from sentinel tables | Follow-up after Spirit dependency exposes the API |

## Review questions for new gRPC control changes

- What is the remote state source, and what stored state should become
  authoritative after each RPC result?
- Which apply ID does each call use? Remote gRPC calls must use `external_id`;
  user-facing logs, comments, checks, and HTTP requests use `apply_identifier`.
- Is every accepted operator request durable before any process-local side
  effect?
- If the process dies after accepting the request, which scheduler owner will
  recover it?
- If the remote RPC succeeds but storage fails, what does the operator see?
- If the remote RPC fails but storage succeeds, what does the operator see?
- Does a failed durable request require a new operator request before retrying?
- Are `apply_logs` stored for accepted, completed, failed, skipped, and
  already-accepted control requests, including caller metadata when available?
- Are CLI watch, PR comments, check runs, apply logs, and stored error messages
  consistent for terminal or blocked outcomes?
- Are polling windows bounded, and do timeout branches fail visibly instead of
  looping forever?
