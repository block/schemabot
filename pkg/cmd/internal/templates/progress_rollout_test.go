package templates

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

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
	isInstant  bool
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
			Deployment:    "prod",
			Target:        name,
			State:         target.opState,
			CutoverPolicy: storage.CutoverPolicyParallel,
			OnFailure:     storage.OnFailureContinue,
			ErrorMessage:  target.err,
			ExternalID:    target.externalID,
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
	assertLess(t, out, "payments-041 — failed", "External apply ID: spirit-041")
	assert.True(t, strings.HasSuffix(out, "schemabot stop apply-7f3c -e production\n"), "%s", out)
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
// splits them by what applies where. Once every target waits for cutover, the
// cutover is the one command and stop is not offered beside it.
func TestWriteProgress_DivergedTargetsWaitingForCutover(t *testing.T) {
	waiting := rolloutTarget{opState: state.ApplyOperation.WaitingForCutover, status: state.Task.WaitingForCutover, rowsCopied: 1000, rowsTotal: 1000}
	zone := waiting
	zone.ddl = "ALTER TABLE `orders` ADD COLUMN `zone` varchar(8)"
	data := targetRolloutData([]rolloutTarget{waiting, waiting, zone})
	data.State = state.Apply.WaitingForCutover
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
