package tern

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// A drive that polls another plane sees state, not liveness: a data-plane
// driver whose lease expires mid-copy hands the schema change to another driver
// without changing any state the poller reads. The handover is recorded so an
// operator reading the control-plane timeline can tell a long healthy copy from
// one that lost its driver.
func TestRecordRemoteDriverChange(t *testing.T) {
	apply := &storage.Apply{ID: 5, ApplyIdentifier: "apply-5", Database: "appdb", Deployment: "appdb", Environment: "staging"}

	t.Run("the first holder is a baseline, not a handover", func(t *testing.T) {
		logs := &mockApplyLogStore{}
		client := &GRPCClient{storage: &mockStorage{logs: logs}}

		got := client.recordRemoteDriverChange(t.Context(), apply, map[string]string{"driver": "driver-1"}, "")

		assert.Equal(t, "driver-1", got)
		assert.Empty(t, logs.logs)
	})

	t.Run("a changed holder is recorded on the timeline", func(t *testing.T) {
		logs := &mockApplyLogStore{}
		client := &GRPCClient{storage: &mockStorage{logs: logs}}

		got := client.recordRemoteDriverChange(t.Context(), apply, map[string]string{"driver": "driver-2"}, "driver-1")

		assert.Equal(t, "driver-2", got)
		require.Len(t, logs.logs, 1)
		assert.Equal(t, storage.LogLevelWarn, logs.logs[0].Level)
		assert.Contains(t, logs.logs[0].Message, "changed drivers")
		assert.Contains(t, logs.logs[0].Message, "resumes from its checkpoint")
	})

	t.Run("an unchanged holder is silent", func(t *testing.T) {
		logs := &mockApplyLogStore{}
		client := &GRPCClient{storage: &mockStorage{logs: logs}}

		got := client.recordRemoteDriverChange(t.Context(), apply, map[string]string{"driver": "driver-1"}, "driver-1")

		assert.Equal(t, "driver-1", got)
		assert.Empty(t, logs.logs)
	})

	t.Run("a data plane that reports no holder claims nothing", func(t *testing.T) {
		logs := &mockApplyLogStore{}
		client := &GRPCClient{storage: &mockStorage{logs: logs}}

		got := client.recordRemoteDriverChange(t.Context(), apply, nil, "driver-1")

		assert.Equal(t, "driver-1", got, "an absent field is no evidence the driver changed")
		assert.Empty(t, logs.logs)
	})
}

// Progress names the driver holding the schema change. A caller on another
// plane sees only that progress keeps arriving, so without the holder a lease
// that expired and was re-claimed is indistinguishable from one unbroken drive.
func TestLocalClient_ProgressNamesTheDriverHoldingTheApply(t *testing.T) {
	apply := &storage.Apply{
		ID:              51,
		ApplyIdentifier: "apply-driver-named",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Running,
		LeaseOwner:      "operator-7",
	}
	task := &storage.Task{
		ID: 11, ApplyID: apply.ID, TaskIdentifier: "task-users", Database: "testdb",
		DatabaseType: storage.DatabaseTypeMySQL, Engine: storage.EngineSpirit,
		TableName: "users", State: state.Task.Running, DDLAction: "alter",
	}
	client := &LocalClient{
		config: LocalConfig{Database: "testdb", Type: storage.DatabaseTypeMySQL},
		storage: &exactProgressStorage{
			applies: &exactProgressApplyStore{apply: apply},
			tasks:   &exactProgressTaskStore{tasks: []*storage.Task{task}},
		},
		logger: slog.Default(),
	}

	progress, err := client.Progress(t.Context(), &ternv1.ProgressRequest{ApplyId: apply.ApplyIdentifier, Environment: "staging"})
	require.NoError(t, err)
	assert.Equal(t, "operator-7", progress.Metadata["driver"])

	apply.LeaseOwner = ""
	unclaimed, err := client.Progress(t.Context(), &ternv1.ProgressRequest{ApplyId: apply.ApplyIdentifier, Environment: "staging"})
	require.NoError(t, err)
	assert.NotContains(t, unclaimed.Metadata, "driver", "an unclaimed apply names no holder rather than an empty one")
}
