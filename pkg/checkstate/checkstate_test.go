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
			// The rollup carries nothing that says whether the rows under it
			// clear on their own. Some do not — a completed rollback leaves a
			// row only a reconcile clears — and from the rollup that is
			// indistinguishable from a running apply, so it never promises
			// SchemaBot will get there alone: selfConverging stays false.
			name:     "aggregate holding the gate open",
			check:    &storage.Check{HeadSHA: headSHA, DatabaseType: AggregateSentinel, DatabaseName: AggregateSentinel, Status: StatusInProgress},
			reason:   ReasonAggregateRollup,
			blocking: true,
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
			got := Diagnose(tt.check, headSHA, tt.apply, "", "")
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

	got := Diagnose(&storage.Check{HeadSHA: headSHA, Status: StatusCompleted}, headSHA, nil, "", "")
	assert.Equal(t, ReasonBlocked, got.Reason)
	assert.True(t, got.Blocking)
}

// Branch protection accepts neutral and skipped from a Check Run, but a stored
// row concluding either of those is not a passing result. Success is the only
// conclusion a writer stores to mean a database's own result was clean, so a
// row carrying another value recorded something else or was written by nothing
// SchemaBot has — and a reading invented for a shape it does not write is one
// nothing would ever correct.
func TestDiagnoseClearsOnlyOnSuccess(t *testing.T) {
	t.Parallel()

	for _, conclusion := range []string{ConclusionNeutral, ConclusionSkipped} {
		t.Run(conclusion, func(t *testing.T) {
			t.Parallel()

			require.True(t, ConclusionClearsGate(conclusion), "precondition: GitHub accepts %q", conclusion)
			got := Diagnose(&storage.Check{HeadSHA: headSHA, Status: StatusCompleted, Conclusion: conclusion}, headSHA, nil, "", "")
			assert.Equal(t, ReasonBlocked, got.Reason)
			assert.True(t, got.Blocking)
		})
	}
}

// An unknown head is not evidence that a row is stale, so it must not turn a
// resolved row into a blocking one.
func TestDiagnoseWithoutAHeadReadsRowsAsCurrent(t *testing.T) {
	t.Parallel()

	got := Diagnose(&storage.Check{HeadSHA: olderSHA, Status: StatusCompleted, Conclusion: ConclusionSuccess}, "", nil, "", "")
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
			}, headSHA, nil, "", "")

			assert.Equal(t, tt.want, got.Reason)
			assert.True(t, got.Blocking)
			assert.Equal(t, tt.selfConverging, got.SelfConverging)
		})
	}
}

// A guard block stored on the rollup is released only when a re-run of the
// check or a new commit re-verifies the whole pull request: every fold skips
// it, a plan command included. An operator reading its diagnosis is told
// exactly that, never to run a plan that succeeds and leaves the gate shut.
func TestDiagnoseTellsARollupGuardBlockToReverifyThePullRequest(t *testing.T) {
	t.Parallel()

	for _, reason := range []string{BlockPlanFailedUnrecorded, BlockPlanRefusedBySourcePolicy, BlockNamespacePlacementRefused, BlockConfigDiscoveryFailed} {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()
			got := Diagnose(&storage.Check{
				HeadSHA: headSHA, Environment: "production",
				DatabaseType: AggregateSentinel, DatabaseName: AggregateSentinel,
				Status: StatusCompleted, Conclusion: ConclusionFailure, BlockingReason: reason,
			}, headSHA, nil, "", "")

			assert.Equal(t, ReasonGuardBlocked, got.Reason)
			assert.True(t, got.Blocking)
			assert.False(t, got.SelfConverging)
			assert.NotContains(t, got.Remedy, "schemabot plan")
			assert.Contains(t, got.Remedy, "re-run the check or push a commit")
		})
	}

	// The same reason on a database's own row is lifted by that row's plan.
	row := Diagnose(&storage.Check{
		HeadSHA: headSHA, Environment: "production", DatabaseType: "mysql", DatabaseName: "orders",
		Status: StatusCompleted, Conclusion: ConclusionFailure, BlockingReason: BlockNamespacePlacementRefused,
	}, headSHA, nil, "", "")

	assert.Equal(t, ReasonGuardBlocked, row.Reason)
	assert.Contains(t, row.Remedy, "`schemabot plan -e <environment>`")
}

// A stopped apply keeps its check in progress deliberately and settles only
// when an operator starts or cancels it, so it is never something to wait out.
func TestDiagnoseReadsAStoppedApplyAsOperatorOwned(t *testing.T) {
	t.Parallel()

	check := &storage.Check{HeadSHA: headSHA, ApplyID: 42, Status: StatusInProgress}
	got := Diagnose(check, headSHA, &storage.Apply{State: "stopped"}, "", "")
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
	got := Diagnose(check, headSHA, nil, "", "")
	assert.Equal(t, ReasonApplyOwnerUnknown, got.Reason)
	assert.True(t, got.Blocking)
	assert.False(t, got.SelfConverging)
}

// A settled row keeps its own reading when the apply it names has been pruned.
// A terminal write leaves ownership behind deliberately, so consulting the
// apply on a row that already recorded its outcome would turn an absent apply
// into a block on a row with nothing left to wait for.
func TestDiagnoseReadsASettledRowWithoutItsApply(t *testing.T) {
	t.Parallel()

	resolved := Diagnose(&storage.Check{
		HeadSHA: headSHA, ApplyID: 42, Status: StatusCompleted, Conclusion: ConclusionSuccess,
	}, headSHA, nil, "", "")

	assert.Equal(t, ReasonResolved, resolved.Reason)
	assert.False(t, resolved.Blocking)

	owed := Diagnose(&storage.Check{
		HeadSHA: headSHA, ApplyID: 42, Status: StatusCompleted, Conclusion: ConclusionActionRequired,
	}, headSHA, nil, "", "")

	assert.Equal(t, ReasonReconciliationOwed, owed.Reason,
		"a terminal block stays a reconciliation, not an unknown owner")
	assert.True(t, owed.Blocking)
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
	assert.Equal(t, ReasonBlocked, Diagnose(blocked, headSHA, nil, "", "").Reason,
		"a database named like the sentinel keeps its own finding")
}

// Every reader has to agree with Diagnose about which commit a row speaks for.
// An unknown head reads as covering, so a caller that compares the two SHAs
// itself would report a row as recorded for another commit while Diagnose
// reports it as current.
func TestCoversHeadAgreesWithDiagnoseOnAnUnknownHead(t *testing.T) {
	t.Parallel()

	row := &storage.Check{HeadSHA: olderSHA, Status: StatusCompleted, Conclusion: ConclusionSuccess}
	require.True(t, CoversHead(row, ""), "an unknown head is not evidence a row is stale")
	assert.Equal(t, ReasonResolved, Diagnose(row, "", nil, "", "").Reason)

	require.False(t, CoversHead(row, headSHA))
	assert.Equal(t, ReasonAwaitingPlan, Diagnose(row, headSHA, nil, "", "").Reason)
}

// The blocks only a rollup may clear and the blocks that say work may have
// reached the target are disjoint, durable reasons: a writer that preserves
// both sets keeps every block it did not evaluate.
func TestPreservedBlockingReasonSetsAreDisjointAndDurable(t *testing.T) {
	assert.Equal(t, []string{BlockNamespacePlacementRefused, BlockReviewTimeDeploymentDrift}, RollupBlockingReasons())
	reconciliation := ReconciliationBlockingReasons()
	assert.Equal(t, []string{BlockApplyCancelledAfterTaskCompleted, BlockRollbackCompleted, BlockSchemaRemovedAfterApplyStarted}, reconciliation)
	for _, reason := range RollupBlockingReasons() {
		assert.Equal(t, blockGuard, blockClasses[reason], reason)
		assert.NotContains(t, reconciliation, reason)
	}
}

// An operator removed an environment from a database's server config while a
// PR was open. A plan-only row in that environment on an earlier commit cannot
// be replaced by a plan of that environment, which the server refuses, so the
// diagnosis names the action that does settle it: a plan of any configured
// environment, or a push. Rows an apply owns, rows the current commit already
// covers, and rows with a durable block keep their own reading, because a plan
// does not settle any of them to a pass.
func TestDiagnoseRowInEnvironmentNotConfigured(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		check  *storage.Check
		apply  *storage.Apply
		reason string
	}{
		{
			name:   "failed plan on an earlier commit",
			check:  &storage.Check{HeadSHA: olderSHA, Status: StatusCompleted, Conclusion: ConclusionFailure},
			reason: ReasonEnvironmentNotConfigured,
		},
		{
			name:   "successful plan on an earlier commit",
			check:  &storage.Check{HeadSHA: olderSHA, Status: StatusCompleted, Conclusion: ConclusionSuccess},
			reason: ReasonEnvironmentNotConfigured,
		},
		{
			name:   "row already settled onto the gating commit",
			check:  &storage.Check{HeadSHA: headSHA, Status: StatusCompleted, Conclusion: ConclusionSuccess},
			reason: ReasonResolved,
		},
		{
			name:   "row an apply owns",
			check:  &storage.Check{HeadSHA: olderSHA, ApplyID: 42, Status: StatusCompleted, Conclusion: ConclusionFailure},
			apply:  &storage.Apply{State: "failed"},
			reason: ReasonReconciliationOwed,
		},
		{
			name:   "plan still running on an earlier commit",
			check:  &storage.Check{HeadSHA: olderSHA, Status: StatusInProgress},
			reason: ReasonAwaitingPlan,
		},
		{
			name:   "row with a durable block",
			check:  &storage.Check{HeadSHA: olderSHA, Status: StatusCompleted, Conclusion: ConclusionActionRequired, BlockingReason: "schema_removed_after_apply_started"},
			reason: Diagnose(&storage.Check{HeadSHA: olderSHA, Status: StatusCompleted, Conclusion: ConclusionActionRequired, BlockingReason: "schema_removed_after_apply_started"}, headSHA, nil, "", "").Reason,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := Diagnose(tc.check, headSHA, tc.apply, "staging", "")
			assert.Equal(t, tc.reason, got.Reason)
			if tc.reason != ReasonEnvironmentNotConfigured {
				return
			}
			assert.True(t, got.Blocking)
			assert.False(t, got.SelfConverging, "the row clears only after someone plans another environment or pushes")
			assert.Contains(t, got.Remedy, "`schemabot plan -e staging`", "the remedy names an environment the database is still planned in")
			assert.Contains(t, got.Remedy, "push a commit")
		})
	}
}

// A reconciliation names the plan of the row's own database, so the command an
// operator pastes from `checks show` addresses the database that is blocked.
func TestDiagnoseNamesTheBlockedDatabasesPlan(t *testing.T) {
	t.Parallel()

	got := Diagnose(&storage.Check{
		HeadSHA: headSHA, Environment: "staging", DatabaseType: "mysql", DatabaseName: "inventory",
		ApplyID: 42, Status: StatusCompleted, Conclusion: ConclusionFailure, BlockingReason: BlockRollbackCompleted,
	}, headSHA, nil, "", "")

	assert.Equal(t, ReasonReconciliationOwed, got.Reason)
	assert.Contains(t, got.Remedy, "comment `schemabot plan -e staging -d inventory` on the pull request")

	terminal := Diagnose(&storage.Check{
		HeadSHA: headSHA, Environment: "production", DatabaseType: "mysql", DatabaseName: "orders",
		ApplyID: 42, Status: StatusCompleted, Conclusion: ConclusionFailure,
	}, headSHA, nil, "", "")

	assert.Equal(t, ReasonReconciliationOwed, terminal.Reason)
	assert.Contains(t, terminal.Remedy, "comment `schemabot plan -e production -d orders` on the pull request")
}

// A database the pull request stopped changing after its apply started owes a
// reconciliation, but no apply from this pull request can reach it, so the
// remedy has the person change the pull request to match the target rather
// than apply the change again, then plan it. The plan command carries the
// deployment's tenant, as the Check Run's does.
func TestDiagnoseSchemaRemovedNamesTheTenantsPlan(t *testing.T) {
	t.Parallel()

	check := &storage.Check{
		HeadSHA: headSHA, Environment: "staging", DatabaseType: "mysql", DatabaseName: "orders",
		ApplyID: 42, Status: StatusCompleted, Conclusion: ConclusionFailure, BlockingReason: BlockSchemaRemovedAfterApplyStarted,
	}
	got := Diagnose(check, headSHA, &storage.Apply{ApplyIdentifier: "apply-example-12", State: "failed"}, "", "tenant-a")
	assert.Equal(t, ReasonReconciliationOwed, got.Reason)
	assert.True(t, got.Blocking)
	assert.False(t, got.SelfConverging)
	assert.Equal(t, MatchTargetRemedy(PlanCommand("staging", "orders", "tenant-a")), got.Remedy)
	assert.Contains(t, got.Remedy, "`schemabot plan -e staging -d orders --tenant tenant-a`")
	assert.Contains(t, got.Remedy, "change the pull request to match the target")
	assert.NotContains(t, got.Remedy, "apply the change again")
}

// Each reconciliation reason gets the remedy that fits it: a change a rollback
// undid on purpose, or one the pull request no longer carries, is never offered
// as something to apply again, while a failed or cancelled apply may be.
func TestReconciliationRemedyFitsTheBlockingReason(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		reason      string
		offersRerun bool
	}{
		{"rolled back on purpose", BlockRollbackCompleted, false},
		{"no longer in the pull request", BlockSchemaRemovedAfterApplyStarted, false},
		{"cancelled after a task completed", BlockApplyCancelledAfterTaskCompleted, true},
		{"terminal apply with no recorded reason", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := Diagnose(&storage.Check{
				HeadSHA: headSHA, Environment: "staging", DatabaseType: "mysql", DatabaseName: "orders",
				ApplyID: 42, Status: StatusCompleted, Conclusion: ConclusionFailure, BlockingReason: tc.reason,
			}, headSHA, nil, "", "")
			require.Equal(t, ReasonReconciliationOwed, got.Reason)
			plan := PlanCommand("staging", "orders", "")
			if tc.offersRerun {
				assert.Equal(t, ReconciliationRemedy(plan), got.Remedy)
				assert.Contains(t, got.Remedy, "apply the change again")
				return
			}
			assert.Equal(t, MatchTargetRemedy(plan), got.Remedy)
			assert.NotContains(t, got.Remedy, "apply the change again")
		})
	}
}
