package templates

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/state"
)

func TestParseProgressResponseIncludesOperationsAndTableDeployments(t *testing.T) {
	result := &apitypes.ProgressResponse{
		State: state.Apply.Running,
		Operations: []*apitypes.ProgressOperationResponse{
			{
				Deployment:          "deploy-a",
				ExternalID:          "remote-apply-a",
				ExternalOperationID: "remote-operation-a",
				Target:              "target-a",
				State:               "STATE_RUNNING",
				CutoverPolicy:       "barrier",
				OnFailure:           "continue",
				ErrorCode:           "engine_error_retryable",
				ErrorMessage:        "retryable failure",
				StartedAt:           "2026-06-16T10:00:00Z",
				CompletedAt:         "2026-06-16T10:05:00Z",
			},
		},
		Tables: []*apitypes.TableProgressResponse{
			{
				TableName:  "users",
				Deployment: "deploy-a",
				Keyspace:   "testdb",
				ChangeType: "alter",
				DDL:        "ALTER TABLE users ADD COLUMN email varchar(255)",
				Status:     "STATE_RUNNING",
			},
		},
	}

	data := ParseProgressResponse(result)

	require.Len(t, data.Operations, 1)
	assert.Equal(t, "deploy-a", data.Operations[0].Deployment)
	assert.Equal(t, "remote-apply-a", data.Operations[0].ExternalID)
	assert.Equal(t, "remote-operation-a", data.Operations[0].ExternalOperationID)
	assert.Equal(t, "target-a", data.Operations[0].Target)
	assert.Equal(t, state.Apply.Running, data.Operations[0].State)
	assert.Equal(t, "barrier", data.Operations[0].CutoverPolicy)
	assert.Equal(t, "continue", data.Operations[0].OnFailure)
	assert.Equal(t, "engine_error_retryable", data.Operations[0].ErrorCode)
	assert.Equal(t, "retryable failure", data.Operations[0].ErrorMessage)
	assert.Equal(t, "2026-06-16T10:00:00Z", data.Operations[0].StartedAt)
	assert.Equal(t, "2026-06-16T10:05:00Z", data.Operations[0].CompletedAt)
	require.Len(t, data.Tables, 1)
	assert.Equal(t, "deploy-a", data.Tables[0].Deployment)
	assert.Equal(t, state.Task.Running, data.Tables[0].Status)
}

func TestParseProgressResponseWithoutOperationsKeepsDeploymentEmpty(t *testing.T) {
	result := &apitypes.ProgressResponse{
		State: state.Apply.Completed,
		Tables: []*apitypes.TableProgressResponse{
			{
				TableName:  "users",
				Keyspace:   "testdb",
				ChangeType: "alter",
				DDL:        "ALTER TABLE users ADD COLUMN email varchar(255)",
				Status:     state.Task.Completed,
			},
		},
	}

	data := ParseProgressResponse(result)

	assert.Empty(t, data.Operations)
	require.Len(t, data.Tables, 1)
	assert.Empty(t, data.Tables[0].Deployment)
	assert.Equal(t, "users", data.Tables[0].TableName)
	assert.Equal(t, state.Task.Completed, data.Tables[0].Status)
}
