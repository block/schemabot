//go:build integration

package localscale_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/engine/planetscale"
)

// A planned change can already be in place on main by the time it is applied:
// here a table rebuild (`ENGINE=InnoDB`) that leaves the schema exactly as it
// is. The branch carries no difference, so PlanetScale reports the deploy
// request as having no changes. The engine still accepts the apply with a
// resume state naming that deploy request, and the drive's next progress poll
// reads it as completed, so the apply settles as completed rather than failing
// for want of a resume state to follow.
func TestPlanetScaleConvergedApplySettlesAsCompleted(t *testing.T) {
	cleanupActiveDeployRequests(t, t.Context())
	deferCleanupActiveDeployRequests(t)
	ctx := t.Context()
	const keyspace = "testapp"
	const applyIdentifier = "apply-0c0ffee0c0ffee00"

	mainTables := loadBranchSchema(t, ctx, "main", keyspace)
	require.NotEmpty(t, mainTables, "main must carry a table to rebuild in place")
	target := mainTables[0].Name
	rebuild := fmt.Sprintf("ALTER TABLE `%s` ENGINE=InnoDB", target)

	eng := resumeTestEngineWith(testClient)
	creds := newRecoveryCredentials(t, ctx)
	result, err := eng.Apply(ctx, &engine.ApplyRequest{
		ApplyIdentifier: applyIdentifier,
		Database:        testDB,
		PlanID:          "plan-converged",
		Credentials:     creds,
		SchemaFiles:     desiredSchemaWith(t, ctx, keyspace),
		Changes: []engine.SchemaChange{{
			Namespace: keyspace,
			TableChanges: []engine.TableChange{
				{Table: target, Operation: ddl.StatementAlterTable, DDL: rebuild},
			},
		}},
		Options:     map[string]string{},
		ResumeState: &engine.ResumeState{MigrationContext: applyIdentifier},
	})
	require.NoError(t, err, "an apply whose change is already on main must be accepted")
	require.NotNil(t, result)
	assert.True(t, result.Accepted)
	assert.Equal(t, "no changes detected", result.Message)
	require.NotNil(t, result.ResumeState, "the drive needs a resume state to follow the converged deploy request")
	assert.Equal(t, applyIdentifier, result.ResumeState.MigrationContext)
	branch, err := planetscale.BranchNameFromResumeMetadata(result.ResumeState.Metadata)
	require.NoError(t, err)
	assert.NotEmpty(t, branch, "the resume state names the branch the deploy request was cut from")

	// Progress reports a resume state without a deploy request as still setting
	// up, so a completed read is what shows the deploy request is named.
	progress, err := eng.Progress(ctx, &engine.ProgressRequest{
		Database:    testDB,
		Credentials: creds,
		ResumeState: result.ResumeState,
	})
	require.NoError(t, err)
	assert.Equal(t, engine.StateCompleted, progress.State, "the converged deploy request settles the apply as completed")
	assert.Equal(t, "No changes detected", progress.Message)
}
