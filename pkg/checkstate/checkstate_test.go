package checkstate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
)

const (
	headSHA  = "43da12bb"
	olderSHA = "e22e4cef"
)

// Diagnose separates the two questions an operator is actually asking: is this
// row holding the merge gate open, and will SchemaBot clear it without them.
// Every stored shape has to answer both, because a row that is blocking and
// self-converging means wait, and one that is blocking and not means act.
func TestDiagnose(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		check          *storage.Check
		apply          *storage.Apply
		reason         string
		blocking       bool
		selfConverging bool
	}{
		{
			name:           "plan result on the gating commit",
			check:          &storage.Check{HeadSHA: headSHA, Status: StatusCompleted, Conclusion: ConclusionSuccess},
			reason:         ReasonResolved,
			selfConverging: true,
		},
		{
			name:           "plan result on the gating commit that concluded blocking",
			check:          &storage.Check{HeadSHA: headSHA, Status: StatusCompleted, Conclusion: ConclusionActionRequired},
			reason:         ReasonBlocked,
			blocking:       true,
			selfConverging: false,
		},
		{
			name:           "plan result recorded for an earlier commit",
			check:          &storage.Check{HeadSHA: olderSHA, Status: StatusCompleted, Conclusion: ConclusionSuccess},
			reason:         ReasonAwaitingPlan,
			blocking:       true,
			selfConverging: true,
		},
		{
			name:           "successful apply whose row names an earlier commit",
			check:          &storage.Check{HeadSHA: olderSHA, ApplyID: 42, Status: StatusCompleted, Conclusion: ConclusionSuccess},
			apply:          &storage.Apply{State: "completed"},
			reason:         ReasonAwaitingReplanAfterApply,
			blocking:       true,
			selfConverging: true,
		},
		{
			name:           "terminal apply that left a block",
			check:          &storage.Check{HeadSHA: headSHA, ApplyID: 42, Status: StatusCompleted, Conclusion: ConclusionActionRequired, BlockingReason: BlockRollbackCompleted},
			apply:          &storage.Apply{State: "rolled_back"},
			reason:         ReasonReconciliationOwed,
			blocking:       true,
			selfConverging: false,
		},
		{
			name:           "terminal apply that left a block, on an earlier commit",
			check:          &storage.Check{HeadSHA: olderSHA, ApplyID: 42, Status: StatusCompleted, Conclusion: ConclusionFailure},
			apply:          &storage.Apply{State: "failed"},
			reason:         ReasonReconciliationOwed,
			blocking:       true,
			selfConverging: false,
		},
		{
			name:           "apply running against the gating commit",
			check:          &storage.Check{HeadSHA: headSHA, ApplyID: 42, Status: StatusInProgress},
			apply:          &storage.Apply{State: "running"},
			reason:         ReasonApplyRunning,
			blocking:       true,
			selfConverging: true,
		},
		{
			name:           "apply running against an earlier commit",
			check:          &storage.Check{HeadSHA: olderSHA, ApplyID: 42, Status: StatusInProgress},
			apply:          &storage.Apply{State: "running"},
			reason:         ReasonApplyRunningOnOlderCommit,
			blocking:       true,
			selfConverging: true,
		},
		{
			name:           "no result recorded yet",
			check:          &storage.Check{HeadSHA: headSHA, Status: StatusQueued},
			reason:         ReasonAwaitingPlan,
			blocking:       true,
			selfConverging: true,
		},
		{
			name:           "aggregate holding the gate open",
			check:          &storage.Check{HeadSHA: headSHA, DatabaseType: AggregateSentinel, DatabaseName: AggregateSentinel, Status: StatusInProgress},
			reason:         ReasonAggregateRollup,
			blocking:       true,
			selfConverging: true,
		},
		{
			name:           "aggregate passing on the gating commit",
			check:          &storage.Check{HeadSHA: headSHA, DatabaseType: AggregateSentinel, DatabaseName: AggregateSentinel, Status: StatusCompleted, Conclusion: ConclusionSuccess},
			reason:         ReasonAggregateRollup,
			selfConverging: true,
		},
		{
			name:           "aggregate recorded for an earlier commit",
			check:          &storage.Check{HeadSHA: olderSHA, DatabaseType: AggregateSentinel, DatabaseName: AggregateSentinel, Status: StatusCompleted, Conclusion: ConclusionSuccess},
			reason:         ReasonAggregateRollup,
			blocking:       true,
			selfConverging: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := Diagnose(tt.check, headSHA, tt.apply)
			assert.Equal(t, tt.reason, got.Reason)
			assert.Equal(t, tt.blocking, got.Blocking)
			assert.Equal(t, tt.selfConverging, got.SelfConverging)
			assert.NotEmpty(t, got.Summary, "every disposition states what the row says")
			assert.NotEmpty(t, got.Remedy, "every disposition states what moves it, even if that is waiting")
		})
	}
}

// A completed row naming no conclusion has not recorded an outcome. Reading it
// as passing would invent one, so it is treated as the block it might be.
func TestDiagnoseTreatsAMissingConclusionAsABlock(t *testing.T) {
	t.Parallel()

	got := Diagnose(&storage.Check{HeadSHA: headSHA, Status: StatusCompleted}, headSHA, nil)
	assert.Equal(t, ReasonBlocked, got.Reason)
	assert.True(t, got.Blocking)
}

// An unknown head is not evidence that a row is stale, so it must not turn a
// resolved row into a blocking one.
func TestDiagnoseWithoutAHeadReadsRowsAsCurrent(t *testing.T) {
	t.Parallel()

	got := Diagnose(&storage.Check{HeadSHA: olderSHA, Status: StatusCompleted, Conclusion: ConclusionSuccess}, "", nil)
	require.Equal(t, ReasonResolved, got.Reason)
	assert.False(t, got.Blocking)
}

// A durable blocking reason is the writer saying the gate stays closed until
// something outside the check changes, so it decides the reading before status
// or ownership does. A rollback clears its apply ownership on the way out, so
// reading ownership first would file a reconciliation as an ordinary plan
// verdict a newer plan supersedes.
func TestDiagnoseReadsDurableBlockingReasonsFirst(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		reason         string
		want           string
		selfConverging bool
	}{
		{"rollback clears ownership but not the block", BlockRollbackCompleted, ReasonReconciliationOwed, false},
		{"schema removed after an apply started", BlockSchemaRemovedAfterApplyStarted, ReasonReconciliationOwed, false},
		{"cancelled after a task completed", BlockApplyCancelledAfterTaskCompleted, ReasonReconciliationOwed, false},
		// Drift is a guard, not a reconciliation: a re-plan that re-evaluates
		// the rollup lifts it once the deployments match again.
		{"deployment drift found at review time", BlockReviewTimeDeploymentDrift, ReasonGuardBlocked, false},
		{"a guard failed the check closed", BlockManagedDirMissingConfig, ReasonGuardBlocked, false},
		{"an apply cancelled before it reached the target", BlockApplyCancelled, ReasonGuardBlocked, false},
		{"another deployment owes the result", BlockParticipantUnresolved, ReasonAwaitingParticipantResult, true},
		{"a reason this reader does not know", "something_new", ReasonGuardBlocked, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Ownership cleared and the head moved on: the two conditions that
			// would otherwise route this row to a plan-clearable disposition.
			got := Diagnose(&storage.Check{
				HeadSHA: olderSHA, Status: StatusCompleted,
				Conclusion: ConclusionActionRequired, BlockingReason: tt.reason,
			}, headSHA, nil)
			assert.Equal(t, tt.want, got.Reason)
			assert.True(t, got.Blocking)
			assert.Equal(t, tt.selfConverging, got.SelfConverging)
		})
	}
}

// A stopped apply keeps its check in progress deliberately and settles only
// when an operator starts or cancels it, so it is never something to wait out.
func TestDiagnoseReadsAStoppedApplyAsOperatorOwned(t *testing.T) {
	t.Parallel()

	check := &storage.Check{HeadSHA: headSHA, ApplyID: 42, Status: StatusInProgress}
	got := Diagnose(check, headSHA, &storage.Apply{State: "stopped"})
	assert.Equal(t, ReasonApplyStopped, got.Reason)
	assert.True(t, got.Blocking)
	assert.False(t, got.SelfConverging)
	assert.Contains(t, got.Remedy, "cancel")
}

// An apply that owns a row but could not be read leaves the one question that
// decides the answer unanswered, so the row is reported as unknown rather than
// as one more apply in flight.
func TestDiagnoseReadsAnUnreadableOwnerAsUnknown(t *testing.T) {
	t.Parallel()

	check := &storage.Check{HeadSHA: headSHA, ApplyID: 42, Status: StatusInProgress}
	got := Diagnose(check, headSHA, nil)
	assert.Equal(t, ReasonApplyOwnerUnknown, got.Reason)
	assert.True(t, got.Blocking)
	assert.False(t, got.SelfConverging)
}

// The rollup is identified by both sentinel fields. A database named like the
// sentinel is still a database, and reading its result as a rollup would
// discard a real finding.
func TestIsAggregateRequiresBothSentinels(t *testing.T) {
	t.Parallel()

	assert.True(t, IsAggregate(&storage.Check{DatabaseType: AggregateSentinel, DatabaseName: AggregateSentinel}))
	assert.False(t, IsAggregate(&storage.Check{DatabaseType: "mysql", DatabaseName: AggregateSentinel}))
	assert.False(t, IsAggregate(&storage.Check{DatabaseType: AggregateSentinel, DatabaseName: "widgets"}))

	blocked := &storage.Check{
		DatabaseType: "mysql", DatabaseName: AggregateSentinel, HeadSHA: headSHA,
		Status: StatusCompleted, Conclusion: ConclusionActionRequired,
	}
	assert.Equal(t, ReasonBlocked, Diagnose(blocked, headSHA, nil).Reason,
		"a database named like the sentinel keeps its own finding")
}
