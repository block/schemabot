package planetscale

import (
	"database/sql/driver"
	"fmt"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	ps "github.com/planetscale/planetscale-go/planetscale"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
)

func TestDeployStateToEngineState(t *testing.T) {
	tests := []struct {
		deployState   string
		expectedState engine.State
	}{
		{"pending", engine.StatePending},
		{"ready", engine.StatePending},
		{"no_changes", engine.StateCompleted},
		{"queued", engine.StateRunning},
		{"submitting", engine.StateRunning},
		{"in_progress", engine.StateRunning},
		{"in_progress_vschema", engine.StateRunning},
		{"pending_cutover", engine.StateWaitingForCutover},
		{"in_progress_cutover", engine.StateCuttingOver},
		{"complete", engine.StateCompleted},
		{"complete_pending_revert", engine.StateRevertWindow},
		{"complete_error", engine.StateFailed},
		{"error", engine.StateFailed},
		{"failed", engine.StateFailed},
		{"in_progress_cancel", engine.StateStopped},
		{"cancelled", engine.StateStopped},
		{"complete_cancel", engine.StateStopped},
		{"in_progress_revert", engine.StateRunning},
		{"in_progress_revert_vschema", engine.StateRunning},
		{"complete_revert", engine.StateReverted},
		{"complete_revert_error", engine.StateFailed},
		{"unknown_state", engine.StateRunning},
	}

	for _, tt := range tests {
		t.Run(tt.deployState, func(t *testing.T) {
			got := deployStateToEngineState(tt.deployState)
			assert.Equal(t, tt.expectedState, got)
		})
	}
}

func TestDeployStateToMessage(t *testing.T) {
	tests := []struct {
		deployState string
		contains    string
	}{
		{"pending", "Validating"},
		{"ready", "validation complete"},
		{"no_changes", "No changes"},
		{"queued", "queued"},
		{"submitting", "Submitting"},
		{"in_progress", "in progress"},
		{"in_progress_vschema", "VSchema"},
		{"pending_cutover", "cutover"},
		{"in_progress_cutover", "Cutover"},
		{"complete", "complete"},
		{"complete_pending_revert", "revert available"},
		{"failed", "failed"},
		{"cancelled", "cancelled"},
		{"in_progress_revert", "Revert in progress"},
		{"complete_revert", "reverted"},
		{"complete_revert_error", "Revert failed"},
		{"something_new", "something_new"},
	}

	for _, tt := range tests {
		t.Run(tt.deployState, func(t *testing.T) {
			msg := deployStateToMessage(tt.deployState)
			assert.Contains(t, msg, tt.contains)
		})
	}
}

func TestGenerateBranchName(t *testing.T) {
	tests := []struct {
		name     string
		database string
		planID   string
		expected string
	}{
		{
			name:     "basic",
			database: "mydb",
			planID:   "plan-12345678",
			expected: "schemabot-mydb-12345678",
		},
		{
			name:     "underscores replaced",
			database: "my_cool_db",
			planID:   "plan-abcdefgh",
			expected: "schemabot-my-cool-db-abcdefgh",
		},
		{
			name:     "long database name truncated",
			database: "this_is_a_very_long_database_name",
			planID:   "plan-xyz12345",
			expected: "schemabot-this-is-a-very-long--xyz12345",
		},
		{
			name:     "short plan ID",
			database: "db",
			planID:   "abc",
			expected: "schemabot-db-abc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := generateBranchName(tt.database, tt.planID)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestPSMetadataEncodeDecode(t *testing.T) {
	original := &psMetadata{
		BranchName:       "schemabot-mydb-12345678",
		DeployRequestID:  42,
		DeployRequestURL: "https://app.planetscale.com/org/db/deploy-requests/42",
	}

	encoded, err := encodePSMetadata(original)
	require.NoError(t, err)
	assert.Contains(t, encoded, "schemabot-mydb-12345678")
	assert.Contains(t, encoded, "42")

	decoded, err := decodePSMetadata(encoded)
	require.NoError(t, err)
	assert.Equal(t, original.BranchName, decoded.BranchName)
	assert.Equal(t, original.DeployRequestID, decoded.DeployRequestID)
	assert.Equal(t, original.DeployRequestURL, decoded.DeployRequestURL)
}

func TestPSMetadataEncodeDecode_DeferredDeploy(t *testing.T) {
	original := &psMetadata{
		BranchName:       "schemabot-mydb-12345678",
		DeployRequestID:  42,
		DeployRequestURL: "https://app.planetscale.com/org/db/deploy-requests/42",
		IsInstant:        true,
		DeferredDeploy:   true,
	}

	encoded, err := encodePSMetadata(original)
	require.NoError(t, err)

	decoded, err := decodePSMetadata(encoded)
	require.NoError(t, err)
	assert.Equal(t, original.BranchName, decoded.BranchName)
	assert.Equal(t, original.DeployRequestID, decoded.DeployRequestID)
	assert.True(t, decoded.IsInstant)
	assert.True(t, decoded.DeferredDeploy)
}

func TestDecodePSMetadata_Empty(t *testing.T) {
	_, err := decodePSMetadata("")
	assert.Error(t, err)
}

func TestDecodePSMetadata_Invalid(t *testing.T) {
	_, err := decodePSMetadata("not json")
	assert.Error(t, err)
}

func TestBuildControlResumeStateRequiresDeployRequestMetadata(t *testing.T) {
	_, err := BuildControlResumeState(ResumeData{
		BranchName:       "schemabot-mydb-12345678",
		MigrationContext: "ctx-123",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "deploy request metadata is incomplete")
	assert.Contains(t, err.Error(), "deploy_request_id")
	assert.Contains(t, err.Error(), "deploy_request_url")
}

func TestValidateControlResumeStateIncludesOperation(t *testing.T) {
	resumeState, err := BuildResumeState(ResumeData{
		BranchName:       "schemabot-mydb-12345678",
		MigrationContext: "ctx-123",
	})
	require.NoError(t, err)

	err = validateControlResumeState(engine.ControlCutover, resumeState)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "cutover control resume state is incomplete")
	assert.Contains(t, err.Error(), "deploy_request_id")
}

func TestSplitStatements(t *testing.T) {
	stmts, err := ddl.SplitStatements("CREATE TABLE `a` (id INT); ALTER TABLE `b` ADD COLUMN x INT;")
	require.NoError(t, err)
	assert.Len(t, stmts, 2)

	// Empty input
	stmts, err = ddl.SplitStatements("")
	require.NoError(t, err)
	assert.Empty(t, stmts)

	// Semicolons with no valid statements are a parse error
	_, err = ddl.SplitStatements("  ;  ;  ")
	assert.Error(t, err)
}

func TestIsSnapshotInProgress(t *testing.T) {
	assert.True(t, isSnapshotInProgress(fmt.Errorf("Cannot update VSchema while a schema snapshot is in progress.")))
	assert.True(t, isSnapshotInProgress(fmt.Errorf("wrapped: schema snapshot is in progress")))
	assert.False(t, isSnapshotInProgress(fmt.Errorf("connection refused")))
	assert.False(t, isSnapshotInProgress(nil))
}

func TestIsRetryableEngineError(t *testing.T) {
	t.Run("PS SDK ErrRetry is retryable", func(t *testing.T) {
		err := &ps.Error{Code: ps.ErrRetry}
		assert.True(t, isRetryableEngineError(err))
	})

	t.Run("PS SDK ErrInternal is retryable", func(t *testing.T) {
		err := &ps.Error{Code: ps.ErrInternal}
		assert.True(t, isRetryableEngineError(err))
	})

	t.Run("PS SDK ErrResponseMalformed is retryable", func(t *testing.T) {
		err := &ps.Error{Code: ps.ErrResponseMalformed}
		assert.True(t, isRetryableEngineError(err))
	})

	t.Run("PS SDK ErrNotFound is NOT retryable", func(t *testing.T) {
		err := &ps.Error{Code: ps.ErrNotFound}
		assert.False(t, isRetryableEngineError(err))
	})

	t.Run("PS SDK ErrPermission is NOT retryable", func(t *testing.T) {
		err := &ps.Error{Code: ps.ErrPermission}
		assert.False(t, isRetryableEngineError(err))
	})

	t.Run("PS SDK ErrInvalid is NOT retryable", func(t *testing.T) {
		err := &ps.Error{Code: ps.ErrInvalid}
		assert.False(t, isRetryableEngineError(err))
	})

	t.Run("snapshot in progress is retryable", func(t *testing.T) {
		err := fmt.Errorf("Cannot update VSchema while a schema snapshot is in progress.")
		assert.True(t, isRetryableEngineError(err))
	})

	t.Run("connection refused is retryable", func(t *testing.T) {
		err := fmt.Errorf("connection refused")
		assert.True(t, isRetryableEngineError(err))
	})

	t.Run("wrapped network error is retryable", func(t *testing.T) {
		err := fmt.Errorf("apply failed: %w", fmt.Errorf("i/o timeout"))
		assert.True(t, isRetryableEngineError(err))
	})

	t.Run("MySQL invalid connection is retryable", func(t *testing.T) {
		assert.True(t, isRetryableEngineError(mysql.ErrInvalidConn))
	})

	t.Run("wrapped bad connection is retryable", func(t *testing.T) {
		err := fmt.Errorf("execute DDL: %w", driver.ErrBadConn)
		assert.True(t, isRetryableEngineError(err))
	})

	t.Run("DDL syntax error is NOT retryable", func(t *testing.T) {
		err := fmt.Errorf("Error 1064 (42000): You have an error in your SQL syntax")
		assert.False(t, isRetryableEngineError(err))
	})

	t.Run("nil is NOT retryable", func(t *testing.T) {
		assert.False(t, isRetryableEngineError(nil))
	})
}

func TestRetryDelay(t *testing.T) {
	t.Run("normal backoff is exponential", func(t *testing.T) {
		d0 := retryDelay(0, fmt.Errorf("connection refused"))
		d1 := retryDelay(1, fmt.Errorf("connection refused"))
		d2 := retryDelay(2, fmt.Errorf("connection refused"))
		// Base: 2s, 4s, 8s (plus up to 2s jitter)
		assert.GreaterOrEqual(t, d0, 2*time.Second)
		assert.Less(t, d0, 5*time.Second)
		assert.GreaterOrEqual(t, d1, 4*time.Second)
		assert.GreaterOrEqual(t, d2, 8*time.Second)
	})

	t.Run("snapshot backoff is longer", func(t *testing.T) {
		snapshotErr := fmt.Errorf("schema snapshot is in progress")
		d0 := retryDelay(0, snapshotErr)
		d1 := retryDelay(1, snapshotErr)
		// Base: 10s, 20s (plus up to 5s jitter)
		assert.GreaterOrEqual(t, d0, 10*time.Second)
		assert.Less(t, d0, 16*time.Second)
		assert.GreaterOrEqual(t, d1, 20*time.Second)
	})

	t.Run("snapshot backoff caps at 60s", func(t *testing.T) {
		snapshotErr := fmt.Errorf("schema snapshot is in progress")
		d10 := retryDelay(10, snapshotErr)
		assert.LessOrEqual(t, d10, 65*time.Second)
	})
}

func TestIsRetryablePSError(t *testing.T) {
	t.Run("PS SDK ErrRetry is retryable", func(t *testing.T) {
		assert.True(t, isRetryablePSError(&ps.Error{Code: ps.ErrRetry}))
	})

	t.Run("PS SDK ErrInternal is retryable", func(t *testing.T) {
		assert.True(t, isRetryablePSError(&ps.Error{Code: ps.ErrInternal}))
	})

	t.Run("PS SDK ErrResponseMalformed is retryable", func(t *testing.T) {
		assert.True(t, isRetryablePSError(&ps.Error{Code: ps.ErrResponseMalformed}))
	})

	t.Run("PS SDK ErrNotFound is NOT retryable", func(t *testing.T) {
		assert.False(t, isRetryablePSError(&ps.Error{Code: ps.ErrNotFound}))
	})

	t.Run("PS SDK ErrPermission is NOT retryable", func(t *testing.T) {
		assert.False(t, isRetryablePSError(&ps.Error{Code: ps.ErrPermission}))
	})

	t.Run("snapshot in progress is retryable", func(t *testing.T) {
		assert.True(t, isRetryablePSError(fmt.Errorf("schema snapshot is in progress")))
	})

	t.Run("plain error is NOT retryable", func(t *testing.T) {
		assert.False(t, isRetryablePSError(fmt.Errorf("DDL syntax error")))
	})

	t.Run("nil is NOT retryable", func(t *testing.T) {
		assert.False(t, isRetryablePSError(nil))
	})
}

func TestFormatDeployRequestError(t *testing.T) {
	t.Run("without lint errors", func(t *testing.T) {
		dr := &ps.DeployRequest{
			Number:          42,
			DeploymentState: "error",
		}
		msg := formatDeployRequestError(dr)
		assert.Equal(t, "deploy request #42 failed during preparation (state: error)", msg)
	})

	t.Run("with lint errors", func(t *testing.T) {
		dr := &ps.DeployRequest{
			Number:          102,
			DeploymentState: "error",
			Deployment: &ps.Deployment{
				LintErrors: []*ps.DeploymentLintError{
					{
						LintError:        "INVALID_VSCHEMA",
						SubjectType:      "vschema_error",
						ErrorDescription: "table t1 has a changed column vindex",
						Keyspace:         "ks_sharded",
						Table:            "t1",
					},
				},
			},
		}
		msg := formatDeployRequestError(dr)
		assert.Contains(t, msg, "deploy request #102 failed during preparation")
		assert.Contains(t, msg, "INVALID_VSCHEMA")
		assert.Contains(t, msg, "table t1 has a changed column vindex")
		assert.Contains(t, msg, "keyspace: ks_sharded")
		assert.Contains(t, msg, "table: t1")
	})

	t.Run("with multiple lint errors", func(t *testing.T) {
		dr := &ps.DeployRequest{
			Number:          200,
			DeploymentState: "error",
			Deployment: &ps.Deployment{
				LintErrors: []*ps.DeploymentLintError{
					{
						LintError:        "INVALID_VSCHEMA",
						SubjectType:      "vschema_error",
						ErrorDescription: "changed vindex on t1",
						Keyspace:         "ks1",
						Table:            "t1",
					},
					{
						LintError:        "INVALID_VSCHEMA",
						SubjectType:      "vschema_error",
						ErrorDescription: "changed vindex on t2",
						Keyspace:         "ks1",
						Table:            "t2",
					},
				},
			},
		}
		msg := formatDeployRequestError(dr)
		assert.Contains(t, msg, "changed vindex on t1")
		assert.Contains(t, msg, "changed vindex on t2")
	})

	t.Run("with nil deployment", func(t *testing.T) {
		dr := &ps.DeployRequest{
			Number:          99,
			DeploymentState: "error",
			Deployment:      nil,
		}
		msg := formatDeployRequestError(dr)
		assert.Equal(t, "deploy request #99 failed during preparation (state: error)", msg)
	})
}

func TestNextVSchemaStatus(t *testing.T) {
	tests := []struct {
		name     string
		current  string
		drState  string
		expected string
	}{
		{"enters vschema phase", "", deployState.InProgressVSchema, vschemaStatusApplying},
		{"stays applying mid-phase", vschemaStatusApplying, deployState.InProgressVSchema, vschemaStatusApplying},
		{"applied on complete after phase", vschemaStatusApplying, deployState.Complete, vschemaStatusApplied},
		{"applied on complete_pending_revert after phase", vschemaStatusApplying, deployState.CompletePendingRevert, vschemaStatusApplied},
		{"applied on complete without observed phase", "", deployState.Complete, vschemaStatusApplied},
		{"unrelated state leaves applying untouched", vschemaStatusApplying, deployState.InProgress, vschemaStatusApplying},
		{"unrelated state leaves empty untouched", "", deployState.Queued, ""},
		{"already applied is stable on complete", vschemaStatusApplied, deployState.Complete, vschemaStatusApplied},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, nextVSchemaStatus(tc.current, tc.drState))
		})
	}
}
