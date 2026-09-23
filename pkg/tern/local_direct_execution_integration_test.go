//go:build integration

package tern

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
)

// A data plane runs statements for databases it holds no registration for, so
// the policy a refused statement is judged under has to arrive with the
// dispatch. The apply records it at creation: the drive that eventually routes
// the statement can be a later one, on another pod, after this server has been
// reconfigured, and it must route under the policy the dispatch was admitted
// with rather than whatever the server's configuration says by then.
func TestDispatchRecordsTheStatedDirectExecutionPolicyOnTheApply(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newAdoptTestFixture(t, map[string]string{
		"users": "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255))",
	})

	resp, err := f.client.Apply(t.Context(), &ternv1.ApplyRequest{
		PlanId:         f.planID,
		Environment:    localClientTestEnvironment,
		Database:       "testdb",
		Type:           storage.DatabaseTypeMySQL,
		IdempotencyKey: "schemabot:v1:direct-execution-stated",
		DirectExecution: &ternv1.DirectExecutionPolicy{
			Enabled:                       true,
			MaxTableRows:                  10000,
			LockAcquisitionTimeoutSeconds: 5,
		},
	})
	require.NoError(t, err)
	require.True(t, resp.Accepted, "dispatch must be accepted: %s", resp.ErrorMessage)

	apply, err := f.stor.Applies().GetByApplyIdentifier(t.Context(), resp.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)

	options := apply.GetOptions()
	require.NotNil(t, options.DirectExecution, "the apply must record the policy it was admitted under")
	assert.Equal(t, &storage.DirectExecutionPolicy{
		Enabled:                       true,
		MaxTableRows:                  10000,
		LockAcquisitionTimeoutSeconds: 5,
	}, options.DirectExecution)

	// What the drive hands the engine is the same policy, spelled in the
	// metadata keys the engine reads it from.
	driveOptions := effectiveCopyDriveOptions(apply, false, nil).Map()
	assert.Equal(t, "true", driveOptions[engine.MetadataDirectExecution])
	assert.Equal(t, "10000", driveOptions[engine.MetadataDirectExecutionMaxTableRows])
	assert.Equal(t, "5", driveOptions[engine.MetadataDirectExecutionLockAcquisitionTimeoutSeconds])
}

// The option map a dispatch carries is operator-supplied. A caller that could
// name its own direct execution policy there would be granting itself the
// thing the server's configuration exists to bound, so the apply records the
// policy from the request's own field and from nowhere else.
func TestDispatchRefusesADirectExecutionPolicyCarriedInTheOptionMap(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	f := newAdoptTestFixture(t, map[string]string{
		"users": "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255))",
	})

	resp, err := f.client.Apply(t.Context(), &ternv1.ApplyRequest{
		PlanId:         f.planID,
		Environment:    localClientTestEnvironment,
		Database:       "testdb",
		Type:           storage.DatabaseTypeMySQL,
		IdempotencyKey: "schemabot:v1:direct-execution-smuggled",
		Options: map[string]string{
			engine.MetadataDirectExecution:             "true",
			engine.MetadataDirectExecutionMaxTableRows: "100000000",
		},
	})
	require.NoError(t, err)
	require.True(t, resp.Accepted, "dispatch must be accepted: %s", resp.ErrorMessage)

	apply, err := f.stor.Applies().GetByApplyIdentifier(t.Context(), resp.ApplyId)
	require.NoError(t, err)
	require.NotNil(t, apply)
	assert.Nil(t, apply.GetOptions().DirectExecution,
		"an option the caller wrote must not become the policy the apply is admitted under")

	driveOptions := effectiveCopyDriveOptions(apply, false, nil).Map()
	assert.NotContains(t, driveOptions, engine.MetadataDirectExecution,
		"the drive must reach the engine with no policy, leaving refused statements blocked")
}
