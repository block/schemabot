package templates

import (
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

var ansiEscape = regexp.MustCompile(`\x1b\[[0-9;]*m`)

// addRegion is the change every fixture target runs unless it says otherwise.
const addRegion = "ALTER TABLE `orders` ADD COLUMN `region` varchar(32)"

// rolloutTarget is one fixture target of the prod deployment: its operation
// state, and the orders table's progress on it (no table when status is "").
type rolloutTarget struct {
	opState    string
	status     string
	ddl        string
	rowsCopied int64
	rowsTotal  int64
	eta        int64
	err        string
	externalID string
	// externalOperationID is the target's own data-plane operation.
	externalOperationID string
	isInstant           bool
	// startedAt is when a driver started the target; a completed target
	// without one already held the change when the apply was created.
	startedAt string
}

// targetRolloutData builds a running apply of orders to production whose prod
// deployment addresses one target per entry, named payments-001 onward.
func targetRolloutData(targets []rolloutTarget) ProgressData {
	data := ProgressData{
		ApplyID:     "apply-7f3c",
		Database:    "orders",
		Environment: "production",
		Engine:      "Spirit",
		State:       state.Apply.Running,
	}
	for i, target := range targets {
		name := fmt.Sprintf("payments-%03d", i+1)
		data.Operations = append(data.Operations, ProgressOperation{
			Deployment:          "prod",
			Target:              name,
			State:               target.opState,
			CutoverPolicy:       storage.CutoverPolicyParallel,
			OnFailure:           storage.OnFailureContinue,
			ErrorMessage:        target.err,
			ExternalID:          target.externalID,
			ExternalOperationID: target.externalOperationID,
			StartedAt:           target.startedAt,
		})
		if target.status == "" {
			continue
		}
		ddl := target.ddl
		if ddl == "" {
			ddl = addRegion
		}
		percent := 0
		if target.rowsTotal > 0 {
			percent = int(target.rowsCopied * 100 / target.rowsTotal)
		}
		data.Tables = append(data.Tables, TableProgress{
			Deployment: "prod", Target: name, Namespace: "orders", TableName: "orders",
			ChangeType: "alter", DDL: ddl, Status: target.status,
			RowsCopied: target.rowsCopied, RowsTotal: target.rowsTotal, ETASeconds: target.eta, PercentComplete: percent,
			IsInstant: target.isInstant,
		})
	}
	return data
}

func renderRollout(t *testing.T, data ProgressData) string {
	t.Helper()
	return ansiEscape.ReplaceAllString(captureStdout(t, func() { WriteProgress(data) }), "")
}

func completedTarget() rolloutTarget {
	return rolloutTarget{opState: state.ApplyOperation.Completed, status: state.Task.Completed, rowsCopied: 1000, rowsTotal: 1000}
}

func copyingTarget(copied int64) rolloutTarget {
	return rolloutTarget{opState: state.ApplyOperation.Running, status: state.Task.Running, rowsCopied: copied, rowsTotal: 1000, eta: 120}
}

func queuedTarget() rolloutTarget {
	return rolloutTarget{opState: state.ApplyOperation.Pending}
}

// wideRollout is a 64-target rollout part-way through: 40 targets done, one
// failed, 19 copying and 4 not started, continuing past the failure.
func wideRollout() ProgressData {
	var targets []rolloutTarget
	for i := range 64 {
		switch {
		case i < 40:
			targets = append(targets, completedTarget())
		case i == 40:
			targets = append(targets, rolloutTarget{
				opState: state.ApplyOperation.Failed, status: state.Task.Failed, rowsCopied: 300, rowsTotal: 1000,
				err: "Error 1062: Duplicate entry 'x' for key 'orders.idx'", externalID: "spirit-041",
				externalOperationID: "spirit-op-041",
			})
		case i < 60:
			targets = append(targets, copyingTarget(int64(100+i*10)))
		default:
			targets = append(targets, queuedTarget())
		}
	}
	return targetRolloutData(targets)
}

// A deployment of three targets renders as one section: the table's progress
// summed across the targets copying or done, a line per target, the target
// not yet started counted rather than guessed at, and stop as the one command
// while targets are still copying.
func TestWriteProgress_ThreeTargetRolloutIsOneSection(t *testing.T) {
	out := renderRollout(t, targetRolloutData([]rolloutTarget{completedTarget(), copyingTarget(400), queuedTarget()}))

	assert.Contains(t, out, "Targets:      1 completed · 1 running · 1 queued")
	assert.Contains(t, out, "🔄 prod — 1 completed · 1 running · 1 queued (3 targets)")
	assert.Equal(t, 1, strings.Count(out, "ADD COLUMN `region`"), "the shared change's DDL is shown once:\n%s", out)
	assert.Contains(t, out, "70.00%", "the bar sums the rows of the targets copying or done")
	assert.Contains(t, out, "Rows: 1,400 / 2,000 · ETA: 2m 0s")
	assert.Contains(t, out, "• Targets: 2 (1 complete, 1 copying)")
	assert.Contains(t, out, "✓ payments-001: 1,000 rows")
	assert.Contains(t, out, "◉ payments-002: 40.00% (400/1,000 rows) ETA 2m 0s")
	assert.Contains(t, out, "1 of 3 targets have not reported progress yet.")
	assert.NotContains(t, out, "prod/payments-001 —", "a rolled-up target has no section of its own")
	assert.True(t, strings.HasSuffix(out, "To stop this schema change:\n  schemabot stop apply-7f3c -e production\n"),
		"stop closes the output:\n%s", out)
}

// A target that already held the change is settled completed without a
// driver ever starting it, so it reports no table progress. A finished apply
// says so rather than claiming the target has yet to report, and a target
// that failed before reporting is named in the attention list, not counted as
// waiting.
func TestWriteProgress_TargetRollupCountsSettledTargetsApart(t *testing.T) {
	ran := completedTarget()
	ran.startedAt = "2026-09-30T12:00:00Z"
	converged := rolloutTarget{opState: state.ApplyOperation.Completed}

	finished := targetRolloutData([]rolloutTarget{ran, ran, converged})
	finished.State = state.Apply.Completed
	out := renderRollout(t, finished)
	assert.Contains(t, out, "✅ prod — 3 completed (3 targets)")
	assert.Contains(t, out, "1 of 3 targets already had this schema; nothing ran there.")
	assert.NotContains(t, out, "have not reported progress yet", "a finished target is not waiting to report:\n%s", out)

	failedEarly := rolloutTarget{opState: state.ApplyOperation.Failed, startedAt: "2026-09-30T12:00:00Z", err: "connection refused"}
	running := targetRolloutData([]rolloutTarget{ran, copyingTarget(400), failedEarly, queuedTarget()})
	out = renderRollout(t, running)
	assert.Contains(t, out, "1 of 4 targets have not reported progress yet.", "only the queued target is waiting to report:\n%s", out)
	assert.NotContains(t, out, "already had this schema")
	assertLess(t, out, "Targets needing attention:", "payments-003 — failed: connection refused")
}

// Sixty-four targets stay one screen: copying targets are sampled with the
// rest counted, the failed target is always named with its error and the
// data-plane apply to look at, and the table keeps showing the rows still
// moving rather than reading as failed while 19 targets copy.
func TestWriteProgress_SixtyFourTargetRolloutStaysOneScreen(t *testing.T) {
	out := renderRollout(t, wideRollout())

	assert.Less(t, strings.Count(out, "\n"), 50, "the rollout is not a section per target:\n%s", out)
	assert.Contains(t, out, "❌ prod — 40 completed · 19 running · 4 queued · 1 failed (64 targets)")
	assert.Equal(t, 1, strings.Count(out, "ADD COLUMN `region`"))
	assert.NotContains(t, out, "❌ Failed", "a table still copying on 19 targets does not read as failed")
	assert.Contains(t, out, "• Targets: 60 (40 complete, 19 copying, 1 failed)")
	assert.Contains(t, out, "✗ payments-041: failed")
	assert.Contains(t, out, "... 14 more copying targets")
	assert.Contains(t, out, "... 40 complete")
	assert.Contains(t, out, "4 of 64 targets have not reported progress yet.")
	assertLess(t, out, "Targets needing attention:", "❌ payments-041 — failed: Error 1062: Duplicate entry 'x' for key 'orders.idx'")
	assertLess(t, out, "payments-041 — failed", "External operation ID: spirit-op-041")
	assertLess(t, out, "External operation ID: spirit-op-041", "External apply ID: spirit-041")
	assert.True(t, strings.HasSuffix(out, "schemabot stop apply-7f3c -e production\n"), "%s", out)
}

// A change that failed on all 64 targets still stays short: the table names
// the first failed targets and counts the rest, and the attention list caps
// its own entries, so the output does not grow by a line per target.
func TestWriteProgress_AllTargetsFailedStaysBounded(t *testing.T) {
	var targets []rolloutTarget
	for range 64 {
		targets = append(targets, rolloutTarget{
			opState: state.ApplyOperation.Failed, status: state.Task.Failed, rowsCopied: 300, rowsTotal: 1000,
			err: "Error 1062: Duplicate entry",
		})
	}
	data := targetRolloutData(targets)
	data.State = state.Apply.Failed

	out := renderRollout(t, data)
	assert.Contains(t, out, "• Targets: 64 (64 failed)")
	assert.Contains(t, out, "✗ payments-010: failed")
	assert.NotContains(t, out, "✗ payments-011: failed", "failed targets past the cap are counted, not listed:\n%s", out)
	assert.Contains(t, out, "... 54 more failed targets")
	assert.Contains(t, out, "…and 44 more")
	assert.Less(t, strings.Count(out, "\n"), 60, "the output does not grow by a line per failed target:\n%s", out)
}

// With no target copying, the table's line reads as its most urgent state.
func TestWriteProgress_TargetRollupWithNothingCopyingReadsAsFailed(t *testing.T) {
	data := targetRolloutData([]rolloutTarget{
		completedTarget(),
		{opState: state.ApplyOperation.Failed, status: state.Task.Failed, rowsCopied: 300, rowsTotal: 1000, err: "Error 1062: Duplicate entry"},
	})

	out := renderRollout(t, data)
	assert.Contains(t, out, "❌ Failed")
	assert.NotContains(t, out, "schemabot stop", "a failed apply refuses stop")
}

// Targets that hold different schemas run different changes, so the rollup
// splits them by what applies where. Once every target of an apply that
// defers cutover waits for it, the cutover is the one command and stop is not
// offered beside it.
func TestWriteProgress_DivergedTargetsWaitingForCutover(t *testing.T) {
	waiting := rolloutTarget{opState: state.ApplyOperation.WaitingForCutover, status: state.Task.WaitingForCutover, rowsCopied: 1000, rowsTotal: 1000}
	zone := waiting
	zone.ddl = "ALTER TABLE `orders` ADD COLUMN `zone` varchar(8)"
	data := targetRolloutData([]rolloutTarget{waiting, waiting, zone})
	data.State = state.Apply.WaitingForCutover
	data.Options = map[string]string{"defer_cutover": "true"}
	for i := range data.Operations {
		data.Operations[i].CutoverPolicy = storage.CutoverPolicyRolling
		data.Operations[i].OnFailure = storage.OnFailureHalt
	}

	out := renderRollout(t, data)
	assertLess(t, out, "▸ targets payments-001, payments-002", "ADD COLUMN `region`")
	assertLess(t, out, "ADD COLUMN `region`", "▸ target payments-003")
	assertLess(t, out, "▸ target payments-003", "ADD COLUMN `zone`")
	assert.True(t, strings.HasSuffix(out, "To cut over prod/payments-001:\n  schemabot cutover apply-7f3c -e production\n"),
		"the cutover closes the output:\n%s", out)
	assert.NotContains(t, out, "schemabot stop", "targets only waiting for cutover keep the cutover as the one command")
}

// A rollout paused after a failure waits for an operator to choose: release
// lets the held targets proceed and stop parks the apply, so both are offered.
func TestFormatRolloutFooter_PausedOffersReleaseThenStop(t *testing.T) {
	data := targetRolloutData([]rolloutTarget{
		{opState: state.ApplyOperation.Failed, status: state.Task.Failed, err: "Error 1062"},
		queuedTarget(),
	})
	for i := range data.Operations {
		data.Operations[i].CutoverPolicy = storage.CutoverPolicyRolling
		data.Operations[i].OnFailure = storage.OnFailurePause
	}

	out := renderRollout(t, data)
	assertLess(t, out, "to let the held deployments proceed:\n  schemabot release apply-7f3c -e production",
		"To stop this schema change:\n  schemabot stop apply-7f3c -e production")
}

// An engine whose control command is cancel is offered cancel, not stop.
func TestFormatRolloutFooter_PlanetScaleOffersCancel(t *testing.T) {
	data := targetRolloutData([]rolloutTarget{completedTarget(), copyingTarget(400)})
	data.Engine = "PlanetScale"

	out := renderRollout(t, data)
	assert.Contains(t, out, "To cancel this schema change:\n  schemabot cancel apply-7f3c -e production")
	assert.NotContains(t, out, "schemabot stop")
}

// An engine decides per target whether an ALTER applies instantly, so the
// rolled-up table reads "Applied instantly" only when every target that ran
// the change reports it instant; one target that copied rows makes it read
// complete instead.
func TestWriteProgress_TargetRollupIsInstantOnlyWhenEveryTargetIs(t *testing.T) {
	instant := completedTarget()
	instant.isInstant = true
	completedRollout := func(targets ...rolloutTarget) ProgressData {
		data := targetRolloutData(targets)
		data.State = state.Apply.Completed
		return data
	}

	mixed := renderRollout(t, completedRollout(instant, completedTarget()))
	assert.NotContains(t, mixed, "Applied instantly", "a target that copied rows keeps the change from reading instant:\n%s", mixed)
	assert.Contains(t, mixed, "✓ Complete")

	allInstant := renderRollout(t, completedRollout(instant, instant))
	assert.Contains(t, allInstant, "⚡ Applied instantly")
}

// A target that has finished its row copy and waits for cutover still counts
// toward the table's rows, so a rollout with one target waiting at 100% and
// one copying at 20% reads 60% copied, not the 20% of the copying target
// alone.
func TestWriteProgress_TargetRollupSumsTargetsPastRowCopy(t *testing.T) {
	waiting := rolloutTarget{opState: state.ApplyOperation.WaitingForCutover, status: state.Task.WaitingForCutover, rowsCopied: 1000, rowsTotal: 1000}

	out := renderRollout(t, targetRolloutData([]rolloutTarget{waiting, copyingTarget(200)}))
	assert.Contains(t, out, "60.00%", "the bar sums every target past the start of its copy:\n%s", out)
	assert.Contains(t, out, "Rows: 1,200 / 2,000")
	assert.Contains(t, out, "• Targets: 2 (1 waiting for cutover, 1 copying)")
}

// A wide rollout past its row copy names the phase each target is in: the
// summary counts catching up and checksumming targets, and the lines sample
// each phase with the rest counted, rather than reading "(none)" with no
// target named.
func TestWriteProgress_WideTargetRollupNamesEveryPhase(t *testing.T) {
	var targets []rolloutTarget
	for i := range 12 {
		status := state.Task.Checksumming
		if i < 3 {
			status = state.Task.CatchingUp
		}
		targets = append(targets, rolloutTarget{opState: state.ApplyOperation.Running, status: status, rowsCopied: 1000, rowsTotal: 1000})
	}

	out := renderRollout(t, targetRolloutData(targets))
	assert.Contains(t, out, "• Targets: 12 (3 catching up, 9 checksumming)", "%s", out)
	assert.NotContains(t, out, "(none)")
	for _, name := range []string{"payments-001", "payments-002", "payments-003"} {
		assert.Contains(t, out, "○ "+name+": catching up")
	}
	for _, name := range []string{"payments-004", "payments-005", "payments-006"} {
		assert.Contains(t, out, "○ "+name+": checksumming")
	}
	assert.NotContains(t, out, "payments-007:", "past the sample, a phase's targets are counted")
	assert.Contains(t, out, "... 6 more checksumming")
	assert.NotContains(t, out, "more catching up", "every catching-up target is already named")
}

// An apply that does not defer cutover is cut over by SchemaBot as each
// member becomes ready, so the footer says so instead of handing the operator
// a cutover command, and keeps stop while a target is still copying.
func TestFormatRolloutFooter_CutoverNotDeferredNeedsNoAction(t *testing.T) {
	waiting := rolloutTarget{opState: state.ApplyOperation.WaitingForCutover, status: state.Task.WaitingForCutover, rowsCopied: 1000, rowsTotal: 1000}
	data := targetRolloutData([]rolloutTarget{waiting, copyingTarget(400)})
	for i := range data.Operations {
		data.Operations[i].CutoverPolicy = storage.CutoverPolicyRolling
		data.Operations[i].OnFailure = storage.OnFailureHalt
	}

	out := renderRollout(t, data)
	assertLess(t, out, "SchemaBot will cut over prod/payments-001 next — no action needed.",
		"To stop this schema change:\n  schemabot stop apply-7f3c -e production")
	assert.NotContains(t, out, "schemabot cutover", "%s", out)
}

// The same table changed in two namespaces is two changes, so the rollup
// heads each with its namespace rather than printing two identical blocks.
func TestWriteProgress_TargetRollupGroupsTablesByNamespace(t *testing.T) {
	data := targetRolloutData([]rolloutTarget{copyingTarget(400), copyingTarget(600)})
	var tables []TableProgress
	for _, table := range data.Tables {
		for _, ns := range []string{"orders", "billing"} {
			table.Namespace, table.TableName = ns, "users"
			table.DDL = "ALTER TABLE `users` ADD COLUMN `" + ns + "_region` varchar(32)"
			tables = append(tables, table)
		}
	}
	data.Tables = tables

	out := renderRollout(t, data)
	assertLess(t, out, "── orders ──", "ADD COLUMN `orders_region`")
	assertLess(t, out, "ADD COLUMN `orders_region`", "── billing ──")
	assertLess(t, out, "── billing ──", "ADD COLUMN `billing_region`")
	assert.Equal(t, 1, strings.Count(out, "── orders ──"), "%s", out)
	assert.Equal(t, 1, strings.Count(out, "── billing ──"), "%s", out)
}

// A table still in flight on any target reads as in flight, never as failed,
// whatever phase the live targets are in; and it reads as the earliest phase a
// target is in, not as a later one only some targets have reached.
func TestRollupTaskStatus_InFlightTargetsDecide(t *testing.T) {
	checksummingPastFailure := append([]string{state.Task.Failed}, slices.Repeat([]string{state.Task.Checksumming}, 19)...)
	assert.Equal(t, state.Task.Checksumming, rollupTaskStatus(checksummingPastFailure))
	assert.Equal(t, state.Task.Checksumming, rollupTaskStatus([]string{state.Task.WaitingForCutover, state.Task.Checksumming}))
	assert.Equal(t, state.Task.Checksumming, rollupTaskStatus([]string{state.Task.Checksumming, state.Task.WaitingForCutover}))
	assert.Equal(t, state.Task.Running, rollupTaskStatus([]string{state.Task.CatchingUp, state.Task.Running}))
	assert.Equal(t, state.Task.Failed, rollupTaskStatus([]string{state.Task.Completed, state.Task.Failed, state.Task.Pending}))
	assert.Equal(t, state.Task.Pending, rollupTaskStatus([]string{state.Task.Completed, state.Task.Pending}))
	assert.Equal(t, state.Task.Completed, rollupTaskStatus([]string{state.Task.Completed, state.Task.Completed}))
}

// The phase order names every in-flight task state and nothing else, so a new
// in-flight state cannot fall through to the failure-first branch.
func TestInFlightRollupOrder_CoversEveryInFlightTaskState(t *testing.T) {
	tasks := reflect.ValueOf(state.Task)
	var inFlight []string
	for _, field := range tasks.Fields() {
		if s := field.String(); state.IsInFlightTaskState(s) {
			inFlight = append(inFlight, s)
		}
	}
	require.NotEmpty(t, inFlight)
	assert.ElementsMatch(t, inFlight, inFlightRollupOrder)
}

// A table on which no target is still working and one has halted reads as
// its halt, the way the PR comment's table line does. Its summed rows would
// cover only the targets that finished, so no bar or rows line speaks for the
// table; each target's line says where that target finished or halted, and
// "not started" is said only when no target got as far as a row.
func TestWriteProgress_HaltedTargetRollupReadsAsItsHalt(t *testing.T) {
	stoppedAt := func(copied int64) rolloutTarget {
		return rolloutTarget{opState: state.ApplyOperation.Stopped, status: state.Task.Stopped, rowsCopied: copied, rowsTotal: 1000, startedAt: "2026-09-30T12:00:00Z"}
	}
	failedAt := func(copied int64) rolloutTarget {
		return rolloutTarget{opState: state.ApplyOperation.Failed, status: state.Task.Failed, rowsCopied: copied, rowsTotal: 1000, err: "Error 1062: Duplicate entry"}
	}
	cancelledBeforeCopying := rolloutTarget{opState: state.ApplyOperation.Cancelled, status: state.Task.Cancelled, rowsTotal: 1000}
	for name, tc := range map[string]struct {
		targets  []rolloutTarget
		headline string
		lines    []string
	}{
		"two targets stopped part-way": {
			targets:  []rolloutTarget{stoppedAt(400), stoppedAt(400)},
			headline: "~ orders: ⏹️ Stopped\n",
			lines:    []string{"○ payments-001: stopped at 40.00% (400/1,000 rows)", "○ payments-002: stopped at 40.00% (400/1,000 rows)"},
		},
		"one target done and one stopped part-way": {
			targets:  []rolloutTarget{completedTarget(), stoppedAt(400)},
			headline: "~ orders: ⏹️ Stopped\n",
			lines:    []string{"✓ payments-001: 1,000 rows", "○ payments-002: stopped at 40.00% (400/1,000 rows)"},
		},
		"one target done and one failed part-way": {
			targets:  []rolloutTarget{completedTarget(), failedAt(300)},
			headline: "~ orders: ❌ Failed\n",
			lines:    []string{"✓ payments-001: 1,000 rows", "✗ payments-002: failed"},
		},
		"one target done and one cancelled before copying": {
			targets:  []rolloutTarget{completedTarget(), cancelledBeforeCopying},
			headline: "~ orders: 🚫 Cancelled\n",
			lines:    []string{"✓ payments-001: 1,000 rows", "○ payments-002: cancelled"},
		},
		"every target cancelled before copying": {
			targets:  []rolloutTarget{cancelledBeforeCopying, cancelledBeforeCopying},
			headline: "~ orders: 🚫 Cancelled (not started)\n",
			lines:    []string{"○ payments-001: cancelled", "○ payments-002: cancelled"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			out := renderRollout(t, targetRolloutData(tc.targets))
			assert.Contains(t, out, tc.headline, "the table reads as its halt, with no bar:\n%s", out)
			assertLess(t, out, tc.headline, tc.lines[0])
			assertLess(t, out, tc.lines[0], tc.lines[1])
			assert.NotContains(t, out, "Rows:", "no summed rows speak for a halted table:\n%s", out)
			assert.NotContains(t, out, "was waiting for cutover")
			if !strings.Contains(tc.headline, "not started") {
				assert.NotContains(t, out, "not started")
			}
		})
	}
}

// A halting failure beside a target a driver already started leaves the
// rollout active, and a new apply is refused until it settles: the footer
// offers stop first and says retry opens once the apply finishes or is
// stopped, whether the sibling is still copying or only waits for cutover.
// The PR comment's footer decides the same way.
func TestFormatRolloutFooter_FailureOnActiveRolloutOffersStopFirst(t *testing.T) {
	waiting := rolloutTarget{opState: state.ApplyOperation.WaitingForCutover, status: state.Task.WaitingForCutover, rowsCopied: 1000, rowsTotal: 1000}
	for name, sibling := range map[string]rolloutTarget{
		"beside a target still copying":       copyingTarget(400),
		"beside a target waiting for cutover": waiting,
	} {
		t.Run(name, func(t *testing.T) {
			data := targetRolloutData([]rolloutTarget{
				{opState: state.ApplyOperation.Failed, status: state.Task.Failed, rowsCopied: 300, rowsTotal: 1000, err: "Error 1062"},
				sibling,
			})
			data.Options = map[string]string{"defer_cutover": "true"}
			for i := range data.Operations {
				data.Operations[i].OnFailure = storage.OnFailureHalt
			}

			out := renderRollout(t, data)
			assert.True(t, strings.HasSuffix(out, "To stop this schema change:\n  schemabot stop apply-7f3c -e production\n"+presentation.RetryOnceSettledNote+"\n"),
				"stop closes the footer, followed by when retry opens up:\n%s", out)
			assert.NotContains(t, out, "schemabot apply", "a new apply is refused until this one settles:\n%s", out)
		})
	}
}

// Once a failed rollout is terminal its footer is the retry, and the label
// says that the new apply reprocesses only the tables that haven't completed.
func TestFormatRolloutFooter_TerminalFailureOffersRetryThatResumes(t *testing.T) {
	data := targetRolloutData([]rolloutTarget{
		completedTarget(),
		{opState: state.ApplyOperation.Failed, status: state.Task.Failed, rowsCopied: 300, rowsTotal: 1000, err: "Error 1062"},
	})

	out := renderRollout(t, data)
	assert.True(t, strings.HasSuffix(out, "To retry once the failure above is resolved — a new apply reprocesses only the tables that haven't completed:\n  schemabot apply -s <schema_dir> -e production\n"),
		"the retry closes the footer:\n%s", out)
	assert.NotContains(t, out, "schemabot stop")
	assert.NotContains(t, out, presentation.RetryOnceSettledNote)
}
