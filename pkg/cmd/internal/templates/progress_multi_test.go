package templates

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/presentation"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

func TestWriteProgressMultiDeploymentRendersAggregateAndSections(t *testing.T) {
	output := captureStdout(t, func() {
		WriteProgress(ProgressData{
			ApplyID:     "apply-test",
			Environment: "staging",
			Caller:      "octocat",
			State:       state.Apply.Running,
			StartedAt:   "2026-06-16T10:00:00Z",
			Operations: []ProgressOperation{
				{Deployment: "region-a", ExternalID: "remote-apply-region-a", ExternalOperationID: "remote-region-a", Target: "orders-a", State: state.ApplyOperation.Completed, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
				{Deployment: "region-b", Target: "orders-b", State: state.ApplyOperation.Failed, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt, ErrorMessage: "duplicate column name 'region'"},
				{Deployment: "region-c", Target: "orders-c", State: state.ApplyOperation.Pending, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
			},
			Tables: []TableProgress{
				{Deployment: "region-a", Target: "orders-a", TableName: "users_a", ChangeType: "alter", DDL: "ALTER TABLE `users_a` ADD COLUMN `region` varchar(20)", Status: state.Task.Completed},
				{Deployment: "region-b", Target: "orders-b", TableName: "users_b", ChangeType: "alter", DDL: "ALTER TABLE `users_b` ADD COLUMN `region` varchar(20)", Status: state.Task.Failed},
				{Deployment: "region-c", Target: "orders-c", TableName: "users_c", ChangeType: "alter", DDL: "ALTER TABLE `users_c` ADD COLUMN `region` varchar(20)", Status: state.Task.Running},
			},
		})
	})

	assert.Contains(t, output, "Apply ID:")
	assert.Contains(t, output, "apply-test")
	assert.Contains(t, output, "State:")
	assert.Contains(t, output, "failed")
	assert.Contains(t, output, "Caller:")
	assert.Contains(t, output, "octocat")
	assert.Contains(t, output, "Deployments:")
	assert.Contains(t, output, "1 completed · 1 halted · 1 failed")
	assert.Contains(t, output, "First failure: region-b — duplicate column name 'region'")
	assert.True(t, strings.HasSuffix(output, presentation.RetryLabel+":\n  "+ANSICyan+"schemabot apply -s <schema_dir> -e staging"+ANSIReset+"\n"),
		"the one next command closes the output:\n%s", output)
	assert.NotContains(t, output, "schemabot stop", "a failed apply refuses stop, so none is offered")
	assertLess(t, output, "✅ region-a — completed", "❌ region-b — failed")
	assert.Contains(t, output, "External operation ID: remote-region-a")
	assert.Contains(t, output, "External apply ID: remote-apply-region-a")
	assertLess(t, output, "❌ region-b — failed", "⏸️ region-c — halted — region-b failed")
	assert.Contains(t, output, "users_a")
	assert.Contains(t, output, "users_b")
	assert.Contains(t, output, "users_c")
}

// A keyed apply runs many operations on one deployment, distinguished only by
// operation key. Each section must render its own operation's key and external
// operation ID — never a sibling's — so an operator can correlate every section
// with its stored apply_operation. The external apply ID is shared across the
// deployment, so an operation that has not dispatched yet still shows it.
func TestWriteProgressKeyedApplySectionsCarryOwnOperationIdentity(t *testing.T) {
	output := captureStdout(t, func() {
		WriteProgress(ProgressData{
			ApplyID:     "apply-keyed",
			Environment: "staging",
			State:       state.Apply.Running,
			Operations: []ProgressOperation{
				{Deployment: "cake", OperationKey: "commerce/-80/users", ExternalID: "remote-apply-shared", ExternalOperationID: "remote-op-1", Target: "commerce-db", State: state.ApplyOperation.Completed, CutoverPolicy: storage.CutoverPolicyParallel, OnFailure: storage.OnFailureHalt},
				{Deployment: "cake", OperationKey: "commerce/80-/users", ExternalOperationID: "remote-op-2", State: state.ApplyOperation.Running, CutoverPolicy: storage.CutoverPolicyParallel, OnFailure: storage.OnFailureHalt},
			},
		})
	})

	assertLess(t, output, "✅ cake · commerce/-80/users — completed (commerce-db)", "External operation ID: remote-op-1")
	assertLess(t, output, "External operation ID: remote-op-1", "🔄 cake · commerce/80-/users — running table copy")
	assertLess(t, output, "🔄 cake · commerce/80-/users — running table copy", "External operation ID: remote-op-2")
	assert.Equal(t, 2, strings.Count(output, "External apply ID: remote-apply-shared"),
		"both sections must show the deployment's shared external apply ID, including the operation that has not recorded one itself")
	assert.Equal(t, 2, strings.Count(output, "(commerce-db)"),
		"the second operation's section must inherit the deployment target from its sibling")
}

// Under on_failure continue a failed deployment with a still-running sibling
// holds the rollout running_degraded: the aggregate shows "running (degraded)"
// rather than a premature "failed", surfaces the first failure, and offers no
// retry while the rollout is still in flight: stop is its one command.
func TestWriteProgressMultiDeploymentContinueFailureShowsRunningDegraded(t *testing.T) {
	output := captureStdout(t, func() {
		WriteProgress(ProgressData{
			ApplyID:     "apply-degraded",
			Environment: "production",
			State:       state.Apply.RunningDegraded,
			Operations: []ProgressOperation{
				{Deployment: "region-a", Target: "orders-a", State: state.ApplyOperation.Failed, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureContinue, ErrorMessage: "duplicate column name 'region'"},
				{Deployment: "region-b", Target: "orders-b", State: state.ApplyOperation.Running, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureContinue},
			},
			Tables: []TableProgress{
				{Deployment: "region-a", Target: "orders-a", TableName: "users_a", ChangeType: "alter", DDL: "ALTER TABLE `users_a` ADD COLUMN `region` varchar(20)", Status: state.Task.Failed},
				{Deployment: "region-b", Target: "orders-b", TableName: "users_b", ChangeType: "alter", DDL: "ALTER TABLE `users_b` ADD COLUMN `region` varchar(20)", Status: state.Task.Running},
			},
		})
	})

	assert.Contains(t, output, "State:")
	assert.Contains(t, output, "running (degraded)")
	assert.Contains(t, output, "1 running · 1 failed")
	assert.Contains(t, output, "First failure: region-a — duplicate column name 'region'")
	assert.NotContains(t, output, "To retry")
	assert.Contains(t, output, "schemabot stop apply-degraded -e production")
}

// Under on_failure pause a failed deployment with a held sibling renders the
// paused "release or stop" guidance; once the apply-level release latch is set
// (Released), the same rollout renders running degraded instead — the CLI
// applies the latch so it matches what the operator will claim next.
func TestWriteProgressMultiDeploymentReleasedPauseRendersDegradedNotPaused(t *testing.T) {
	data := ProgressData{
		ApplyID:     "apply-paused",
		Environment: "production",
		State:       state.Apply.Paused,
		Operations: []ProgressOperation{
			{Deployment: "region-a", Target: "orders-a", State: state.ApplyOperation.Failed, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailurePause, ErrorMessage: "duplicate column name 'region'"},
			{Deployment: "region-b", Target: "orders-b", State: state.ApplyOperation.Pending, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailurePause},
		},
	}

	paused := captureStdout(t, func() { WriteProgress(data) })
	assert.Contains(t, paused, "paused — region-a failed; release or stop")

	data.State = state.Apply.RunningDegraded
	data.Released = true
	released := captureStdout(t, func() { WriteProgress(data) })
	assert.NotContains(t, released, "paused — region-a failed; release or stop")
	assert.Contains(t, released, "running (degraded)")
}

func TestWriteProgressSingleDeploymentDoesNotRenderMultiDeploymentAggregate(t *testing.T) {
	data := ProgressData{
		ApplyID:     "apply-single",
		Database:    "orders",
		Environment: "staging",
		State:       state.Apply.Running,
		Operations: []ProgressOperation{
			{Deployment: "region-a", Target: "orders-a", State: state.ApplyOperation.Running, CutoverPolicy: storage.CutoverPolicyBarrier, OnFailure: storage.OnFailureHalt},
		},
		Tables: []TableProgress{
			{Deployment: "region-a", TableName: "users", ChangeType: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `region` varchar(20)", Status: state.Task.Running},
		},
	}

	withOperation := captureStdout(t, func() { WriteProgress(data) })
	data.Operations = nil
	withoutOperation := captureStdout(t, func() { WriteProgress(data) })

	assert.Equal(t, withoutOperation, withOperation)
	assert.Contains(t, withOperation, "Database:")
	assert.Contains(t, withOperation, "orders")
	assert.Contains(t, withOperation, "users")
	assert.NotContains(t, withOperation, "Deployments:")
	assert.NotContains(t, withOperation, "region-a —")
}

func assertLess(t *testing.T, output, left, right string) {
	t.Helper()
	leftIndex := strings.Index(output, left)
	rightIndex := strings.Index(output, right)
	assert.NotEqual(t, -1, leftIndex, "expected output to contain %q", left)
	assert.NotEqual(t, -1, rightIndex, "expected output to contain %q", right)
	assert.Less(t, leftIndex, rightIndex, "expected %q before %q", left, right)
}

// One deployment can address several targets, each running its own copy of the
// change. The deployment renders as one rollup section counting its targets,
// while a sibling deployment that addresses a single target keeps a section of
// its own under its plain name.
func TestWriteProgressMultiTargetDeploymentRollsUpBesideASingleTargetSibling(t *testing.T) {
	output := captureStdout(t, func() {
		WriteProgress(ProgressData{
			ApplyID:     "apply-multi-target",
			Environment: "staging",
			State:       state.Apply.Running,
			Operations: []ProgressOperation{
				{Deployment: "primary", Target: "testapp-001", State: state.ApplyOperation.Completed, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
				{Deployment: "primary", Target: "testapp-002", State: state.ApplyOperation.Running, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
				{Deployment: "eu-west", Target: "orders-eu", State: state.ApplyOperation.Pending, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
			},
		})
	})

	assert.Contains(t, output, "Targets:", "the counts count targets once a deployment addresses several")
	assert.Contains(t, output, "🔄 primary — 1 completed · 1 running (2 targets)")
	assert.Contains(t, output, "⏳ eu-west — waiting for primary/testapp-002 (orders-eu)")
	assertLess(t, output, "primary — 1 completed", "eu-west — waiting")
	assert.NotContains(t, output, "primary/testapp-001 —", "a rolled-up target has no section of its own")
}

// A keyed apply runs several operations of one deployment through one
// data-plane apply, so an operation that has not dispatched yet is labelled
// with the apply ID its siblings already carry rather than with nothing.
func TestSectionExternalID_KeyedApplyBorrowsTheDeploymentsSharedID(t *testing.T) {
	ops := []ProgressOperation{
		{Deployment: "eu", Target: "orders-eu", OperationKey: "shard-1", ExternalID: "ps-1"},
		{Deployment: "eu", Target: "orders-eu", OperationKey: "shard-2"},
	}

	assert.Equal(t, "ps-1", SectionExternalID(ops[1], ops))
	assert.Equal(t, "orders-eu", sectionTarget(ops[1], ops))
}

// A deployment addressing several targets runs each member through its own
// data-plane apply, so there is no shared ID to borrow. A member that has not
// dispatched shows nothing rather than a sibling target's apply ID, which would
// send an operator to watch a member they did not ask about; the member's own
// ID appears on the next poll once it dispatches.
func TestSectionExternalID_MultiTargetMemberBorrowsNothing(t *testing.T) {
	ops := []ProgressOperation{
		{Deployment: "primary", Target: "testapp-001", ExternalID: "ps-1"},
		{Deployment: "primary", Target: "testapp-002", ExternalID: "ps-2"},
		{Deployment: "primary", Target: "testapp-003"},
	}

	assert.Equal(t, "", SectionExternalID(ops[2], ops),
		"a member must not be labelled with another target's apply ID")
	assert.Equal(t, "ps-2", SectionExternalID(ops[1], ops), "a member's own ID still wins")

	// A rollout dispatches its members one at a time, so for most of an apply
	// exactly one of them carries an ID. The members still waiting must show
	// nothing: there is no second ID for theirs to disagree with, and the one
	// that exists belongs to a member they are not watching.
	firstDispatched := []ProgressOperation{
		{Deployment: "primary", Target: "testapp-001", ExternalID: "ps-1"},
		{Deployment: "primary", Target: "testapp-002"},
	}
	assert.Equal(t, "", SectionExternalID(firstDispatched[1], firstDispatched),
		"a member waiting its turn must not inherit the dispatched member's apply ID")

	undispatched := ProgressOperation{Deployment: "primary"}
	assert.Equal(t, "", SectionExternalID(undispatched, ops))
	assert.Equal(t, "", sectionTarget(undispatched, ops),
		"a member must not be labelled with another target of its deployment")
}

// A sibling of another deployment never supplies either value, whatever it
// carries.
func TestSectionExternalID_IgnoresOtherDeployments(t *testing.T) {
	ops := []ProgressOperation{
		{Deployment: "eu", Target: "orders-eu", ExternalID: "ps-1"},
		{Deployment: "us"},
	}

	assert.Equal(t, "", SectionExternalID(ops[1], ops))
	assert.Equal(t, "", sectionTarget(ops[1], ops))
}

// Two targets of one deployment each run their own copy of the change against
// their own schema. The rollup reads each target's own table rows: targets
// that copied different tables are split by what applies where, each table is
// shown once under the target that copied it, and no target is credited with
// its sibling's copy.
func TestWriteProgressMultiTargetRollupIsMemberScoped(t *testing.T) {
	output := captureStdout(t, func() {
		WriteProgress(ProgressData{
			ApplyID:     "apply-multi-target",
			Environment: "staging",
			State:       state.Apply.Running,
			Operations: []ProgressOperation{
				{Deployment: "primary", Target: "testapp-001", ExternalID: "remote-apply-001", State: state.ApplyOperation.Completed, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
				{Deployment: "primary", Target: "testapp-002", State: state.ApplyOperation.Running, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
			},
			Tables: []TableProgress{
				{Deployment: "primary", Target: "testapp-001", TableName: "users_001", ChangeType: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `region` varchar(20)", Status: state.Task.Completed},
				{Deployment: "primary", Target: "testapp-002", TableName: "users_002", ChangeType: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `region` varchar(20)", Status: state.Task.Running},
			},
		})
	})

	assertLess(t, output, "▸ target testapp-001", "users_001")
	assertLess(t, output, "users_001", "▸ target testapp-002")
	assertLess(t, output, "▸ target testapp-002", "users_002")
	assert.Equal(t, 1, strings.Count(output, "users_001:"))
	assert.Equal(t, 1, strings.Count(output, "users_002:"))
	assert.NotContains(t, output, "remote-apply-001", "a healthy target's apply ID is not lifted into the rollup")
}

// A keyed apply's operations share one target, and an operation that has not
// dispatched yet has not recorded it. Its header inherits the target from the
// sibling that has, but its tables are still selected on the empty target the
// rows themselves carry — selecting on the inherited value would look for a
// target no row has and leave the member showing no table progress at all.
func TestWriteProgressKeyedMemberListsTablesUnderAnInheritedTarget(t *testing.T) {
	output := captureStdout(t, func() {
		WriteProgress(ProgressData{
			ApplyID:     "apply-keyed",
			Environment: "staging",
			State:       state.Apply.Running,
			Operations: []ProgressOperation{
				{Deployment: "eu", Target: "orders-eu", OperationKey: "shard-1", State: state.ApplyOperation.Running, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
				{Deployment: "eu", OperationKey: "shard-2", State: state.ApplyOperation.Running, CutoverPolicy: storage.CutoverPolicyRolling, OnFailure: storage.OnFailureHalt},
			},
			Tables: []TableProgress{
				{Deployment: "eu", Target: "orders-eu", TableName: "orders_1", ChangeType: "alter", DDL: "ALTER TABLE `orders` ADD COLUMN `region` varchar(20)", Status: state.Task.Running},
				{Deployment: "eu", TableName: "orders_2", ChangeType: "alter", DDL: "ALTER TABLE `orders` ADD COLUMN `region` varchar(20)", Status: state.Task.Running},
			},
		})
	})

	// The header shows the inherited target on the operation that has none.
	assert.Equal(t, 2, strings.Count(output, "(orders-eu)"))
	// Each operation still lists its own table.
	assertLess(t, output, "shard-1", "orders_1")
	assertLess(t, output, "shard-2", "orders_2")
	assert.Equal(t, 1, strings.Count(output, "orders_1"))
	assert.Equal(t, 1, strings.Count(output, "orders_2"))
}

// TestProgressOperationsForPresentation_SettlesLikeStoredState verifies that the
// progress output and watch view read a rollout's state from the same facts the
// stored derivation does. Under continue, shard -80 of payments-001 fails and
// payments-002 completes, leaving payments-001's orders finalizer pending with
// nothing that will start it: the header reads failed, as the stored apply
// does. Under halt, region-a fails and a stop caught region-b before any driver
// claimed it: the header reads failed, as it would with region-b still pending.
// A failed shard of payments-002 orphans only payments-002's finalizer, so the
// header stays running_degraded while payments-001's finalizer can still start.
func TestProgressOperationsForPresentation_SettlesLikeStoredState(t *testing.T) {
	const started = "2026-09-30T12:00:00Z"
	shard := func(target, shardName, opState string) *apitypes.ProgressOperationResponse {
		return &apitypes.ProgressOperationResponse{
			Deployment:    "payments-a",
			Target:        target,
			OperationKey:  storage.TargetOperationKey(target, storage.ShardOperationKey("orders", shardName, "orders")),
			OperationKind: storage.ApplyOperationKindWork,
			State:         opState,
			CutoverPolicy: storage.CutoverPolicyRolling,
			OnFailure:     storage.OnFailureContinue,
			StartedAt:     started,
		}
	}
	finalizer := func(target, opState, startedAt string) *apitypes.ProgressOperationResponse {
		return &apitypes.ProgressOperationResponse{
			Deployment:    "payments-a",
			Target:        target,
			OperationKey:  storage.TargetOperationKey(target, "orders/group_finalizer"),
			OperationKind: storage.ApplyOperationKindGroupFinalizer,
			State:         opState,
			CutoverPolicy: storage.CutoverPolicyRolling,
			OnFailure:     storage.OnFailureContinue,
			StartedAt:     startedAt,
		}
	}
	region := func(deployment, opState, startedAt string) *apitypes.ProgressOperationResponse {
		return &apitypes.ProgressOperationResponse{
			Deployment:    deployment,
			OperationKey:  "orders",
			OperationKind: storage.ApplyOperationKindWork,
			State:         opState,
			CutoverPolicy: storage.CutoverPolicyRolling,
			OnFailure:     storage.OnFailureHalt,
			StartedAt:     startedAt,
		}
	}

	cases := []struct {
		name string
		ops  []*apitypes.ProgressOperationResponse
		want string
	}{
		{
			name: "continue past a finalizer its own failed work orphaned",
			want: state.Apply.Failed,
			ops: []*apitypes.ProgressOperationResponse{
				shard("payments-001", "-80", state.ApplyOperation.Failed),
				shard("payments-001", "80-", state.ApplyOperation.Completed),
				finalizer("payments-001", state.ApplyOperation.Pending, ""),
				shard("payments-002", "-80", state.ApplyOperation.Completed),
				shard("payments-002", "80-", state.ApplyOperation.Completed),
				finalizer("payments-002", state.ApplyOperation.Completed, started),
			},
		},
		{
			name: "continue while another target's finalizer can still start",
			want: state.Apply.RunningDegraded,
			ops: []*apitypes.ProgressOperationResponse{
				shard("payments-001", "-80", state.ApplyOperation.Completed),
				shard("payments-001", "80-", state.ApplyOperation.Completed),
				finalizer("payments-001", state.ApplyOperation.Pending, ""),
				shard("payments-002", "-80", state.ApplyOperation.Failed),
				shard("payments-002", "80-", state.ApplyOperation.Completed),
				finalizer("payments-002", state.ApplyOperation.Pending, ""),
			},
		},
		{
			name: "halt past work stopped before it started",
			want: state.Apply.Failed,
			ops: []*apitypes.ProgressOperationResponse{
				region("region-a", state.ApplyOperation.Failed, started),
				region("region-b", state.ApplyOperation.Stopped, ""),
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := ParseProgressResponse(&apitypes.ProgressResponse{State: state.Apply.Failed, Operations: tc.ops})
			model := presentation.Derive(ProgressOperationsForPresentation(data.Operations, data.Released))
			assert.Equal(t, tc.want, model.State)
		})
	}
}
