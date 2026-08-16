package templates

import (
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
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
				{Deployment: "region-a", TableName: "users_a", ChangeType: "alter", DDL: "ALTER TABLE `users_a` ADD COLUMN `region` varchar(20)", Status: state.Task.Completed},
				{Deployment: "region-b", TableName: "users_b", ChangeType: "alter", DDL: "ALTER TABLE `users_b` ADD COLUMN `region` varchar(20)", Status: state.Task.Failed},
				{Deployment: "region-c", TableName: "users_c", ChangeType: "alter", DDL: "ALTER TABLE `users_c` ADD COLUMN `region` varchar(20)", Status: state.Task.Running},
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
	assert.Contains(t, output, "Next: review failure in region-b")
	assertLess(t, output, "✅ region-a — completed", "❌ region-b — failed")
	assert.Contains(t, output, "External operation ID: remote-region-a")
	assert.Contains(t, output, "External apply ID: remote-apply-region-a")
	assertLess(t, output, "❌ region-b — failed", "⏸ region-c — halted — region-b failed")
	assert.Contains(t, output, "users_a")
	assert.Contains(t, output, "users_b")
	assert.Contains(t, output, "users_c")
}

// Under on_failure continue a failed deployment with a still-running sibling
// holds the rollout running_degraded: the aggregate shows "running (degraded)"
// rather than a premature "failed", surfaces the first failure, and offers no
// review-failure next action while the rollout is still in flight.
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
				{Deployment: "region-a", TableName: "users_a", ChangeType: "alter", DDL: "ALTER TABLE `users_a` ADD COLUMN `region` varchar(20)", Status: state.Task.Failed},
				{Deployment: "region-b", TableName: "users_b", ChangeType: "alter", DDL: "ALTER TABLE `users_b` ADD COLUMN `region` varchar(20)", Status: state.Task.Running},
			},
		})
	})

	assert.Contains(t, output, "State:")
	assert.Contains(t, output, "running (degraded)")
	assert.Contains(t, output, "1 running · 1 failed")
	assert.Contains(t, output, "First failure: region-a — duplicate column name 'region'")
	assert.NotContains(t, output, "Next: review failure")
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

// An engine can fan one deployment out into several operations — per-shard
// work applies plus group finalizers — all sharing the deployment name. The
// status view must render that as one deployment section whose operation lines
// each carry their own kind and external identifiers (so an operator can find
// the exact remote apply), with the deployment's tables rendered once, and the
// header histogram labeled as operations rather than deployments.
func TestWriteProgressGroupsOperationsSharingADeployment(t *testing.T) {
	output := captureStdout(t, func() {
		WriteProgress(ProgressData{
			ApplyID:     "apply-sharded",
			Environment: "staging",
			State:       state.Apply.Completed,
			StartedAt:   "2026-06-16T10:00:00Z",
			CompletedAt: "2026-06-16T10:00:23Z",
			Operations: []ProgressOperation{
				{Deployment: "cake", ExternalID: "remote-apply-w1", ExternalOperationID: "304", OperationKind: "work", Target: "orders-primary", State: state.ApplyOperation.Completed, CutoverPolicy: storage.CutoverPolicyParallel, OnFailure: storage.OnFailureHalt},
				{Deployment: "cake", ExternalID: "remote-apply-w2", ExternalOperationID: "305", OperationKind: "work", Target: "orders-primary", State: state.ApplyOperation.Completed, CutoverPolicy: storage.CutoverPolicyParallel, OnFailure: storage.OnFailureHalt},
				{Deployment: "cake", ExternalID: "remote-apply-f1", ExternalOperationID: "306", OperationKind: "group_finalizer", Target: "orders-primary", State: state.ApplyOperation.Completed, CutoverPolicy: storage.CutoverPolicyParallel, OnFailure: storage.OnFailureHalt},
			},
			Tables: []TableProgress{
				{Deployment: "cake", TableName: "mutes", Namespace: "orders_sharded", ChangeType: "alter", DDL: "ALTER TABLE `mutes` ADD COLUMN `flag` varchar(32)", Status: state.Task.Completed},
			},
		})
	})

	assert.Contains(t, output, "Operations:")
	assert.NotContains(t, output, "Deployments:")
	assert.Contains(t, output, "3 completed")
	assert.Equal(t, 1, strings.Count(output, "✅ cake — "), "one section per deployment, not per operation")
	assert.Contains(t, output, "(orders-primary)")
	assert.Contains(t, output, "external operation ID 304")
	assert.Contains(t, output, "external apply ID remote-apply-w1")
	assert.Contains(t, output, "external operation ID 305")
	assert.Contains(t, output, "external apply ID remote-apply-w2")
	assert.Contains(t, output, "external operation ID 306")
	assert.Contains(t, output, "external apply ID remote-apply-f1")
	assert.Equal(t, 2, strings.Count(output, "work — completed"))
	assert.Equal(t, 1, strings.Count(output, "group_finalizer — completed"))
	assert.Equal(t, 1, strings.Count(output, "mutes:"), "deployment tables render once, not per operation")
	assert.Equal(t, 1, strings.Count(output, "orders_sharded"), "keyspace header renders once, not per operation")
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
