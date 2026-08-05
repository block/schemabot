package api

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

type staticGetApplyStore struct {
	storage.ApplyStore
	apply *storage.Apply
}

func (s *staticGetApplyStore) Get(context.Context, int64) (*storage.Apply, error) {
	return s.apply, nil
}

type capturingCheckRefreshStore struct {
	storage.CheckRefreshRequestStore
	recorded []*storage.CheckRefreshRequest
}

func (s *capturingCheckRefreshStore) Record(_ context.Context, req *storage.CheckRefreshRequest) (bool, error) {
	s.recorded = append(s.recorded, req)
	return true, nil
}

type mockStorageWithCheckRefresh struct {
	mockStorage
	applies      storage.ApplyStore
	checkRefresh storage.CheckRefreshRequestStore
}

func (m *mockStorageWithCheckRefresh) Applies() storage.ApplyStore { return m.applies }
func (m *mockStorageWithCheckRefresh) CheckRefreshRequests() storage.CheckRefreshRequestStore {
	return m.checkRefresh
}

// TestRecordCheckRefreshGatedOnConsumer verifies the drive tail records a
// check refresh request only when a refresh consumer is registered. A server
// with no GitHub runtime — a gRPC/CLI-only deployment — has no PR check state
// to refresh and no processor to drain requests, so a recorded row would sit
// pending forever; the drive tail must skip recording entirely there. With a
// consumer registered, the request is recorded with the apply's target and
// attribution and the consumer is woken.
func TestRecordCheckRefreshGatedOnConsumer(t *testing.T) {
	newService := func() (*Service, *capturingCheckRefreshStore) {
		refreshStore := &capturingCheckRefreshStore{}
		st := &mockStorageWithCheckRefresh{
			applies: &staticGetApplyStore{apply: &storage.Apply{
				ID:              7,
				ApplyIdentifier: "apply-gate-test",
				Database:        "gate_db",
				DatabaseType:    "mysql",
				Environment:     "staging",
				Repository:      "octocat/hello-world",
				PullRequest:     1,
				Caller:          "cli:tester@host",
				State:           state.Apply.Completed,
			}},
			checkRefresh: refreshStore,
		}
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
		return New(st, testServerConfig(), nil, logger), refreshStore
	}

	t.Run("no consumer registered skips recording", func(t *testing.T) {
		svc, refreshStore := newService()

		svc.recordCheckRefreshIfApplyResolved(t.Context(), 0, 7)

		assert.Empty(t, refreshStore.recorded,
			"a server without a refresh consumer must not record requests nothing will drain")
	})

	t.Run("registered consumer records and is woken", func(t *testing.T) {
		svc, refreshStore := newService()
		woken := 0
		svc.OnCheckRefreshRecorded = func() { woken++ }

		svc.recordCheckRefreshIfApplyResolved(t.Context(), 0, 7)

		require.Len(t, refreshStore.recorded, 1)
		recorded := refreshStore.recorded[0]
		assert.Equal(t, "apply-gate-test", recorded.ApplyIdentifier)
		assert.Equal(t, "gate_db", recorded.DatabaseName)
		assert.Equal(t, "mysql", recorded.DatabaseType)
		assert.Equal(t, "staging", recorded.Environment)
		assert.Equal(t, "octocat/hello-world", recorded.Repository)
		assert.Equal(t, 1, recorded.PullRequest)
		assert.Equal(t, "cli:tester@host", recorded.RequestedBy)
		assert.Equal(t, 1, woken, "the drive tail wakes the consumer exactly once per recording")
	})
}
