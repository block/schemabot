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
| 13 | PR comment stop support | Same semantics as CLI stop: durable request first, caller visible, stop priority preserved | Follow-up |
| 14 | PR comment cutover support | Same safety gate as CLI cutover, plus durable cutover intent if async ownership requires it | Follow-up |

## Review questions for new gRPC control changes

- What is the remote state source, and what stored state should become
  authoritative after each RPC result?
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
