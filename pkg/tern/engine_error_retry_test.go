package tern

import (
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// A failed engine attempt pauses for operator recovery only on database types
// whose engine resumes a re-claimed attempt from a durable checkpoint — MySQL
// and Strata both drive Spirit, so recovery continues the same in-flight work.
// Other types classify engine errors as permanent, and a typed permanent error
// is never paused for retry regardless of type.
func TestShouldRetryEngineErrorByDatabaseType(t *testing.T) {
	transient := fmt.Errorf("engine lost its connection to the target")
	permanent := engine.NewPermanentError("plan references a dropped table")

	cases := []struct {
		name         string
		databaseType string
		err          error
		want         bool
	}{
		{"mysql transient pauses for retry", storage.DatabaseTypeMySQL, transient, true},
		{"strata transient pauses for retry", storage.DatabaseTypeStrata, transient, true},
		{"vitess transient fails permanently", storage.DatabaseTypeVitess, transient, false},
		{"postgres transient fails permanently", storage.DatabaseTypePostgres, transient, false},
		{"mysql permanent error fails permanently", storage.DatabaseTypeMySQL, permanent, false},
		{"strata permanent error fails permanently", storage.DatabaseTypeStrata, permanent, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := &LocalClient{config: LocalConfig{Type: tc.databaseType}}
			assert.Equal(t, tc.want, client.shouldRetryEngineError(tc.err))
		})
	}
}

// An error observing or driving work the engine already accepted pauses for
// operator recovery wherever a recovery claim continues that same work: from a
// checkpoint (MySQL, Strata) or by reattaching to the work the engine keeps
// running outside SchemaBot (Vitess deploy requests). Postgres and unknown
// types keep the conservative disposition, and a permanent error always fails.
func TestShouldRetryAcceptedWorkErrorByDatabaseType(t *testing.T) {
	transient := fmt.Errorf("get deploy request: rate limited")
	permanent := engine.NewPermanentError("deploy request not found")

	cases := []struct {
		name         string
		databaseType string
		err          error
		want         bool
	}{
		{"mysql transient pauses for retry", storage.DatabaseTypeMySQL, transient, true},
		{"strata transient pauses for retry", storage.DatabaseTypeStrata, transient, true},
		{"vitess transient pauses for retry", storage.DatabaseTypeVitess, transient, true},
		{"postgres transient fails permanently", storage.DatabaseTypePostgres, transient, false},
		{"embedder-registered type fails permanently", "customengine", transient, false},
		{"vitess permanent error fails permanently", storage.DatabaseTypeVitess, permanent, false},
		{"mysql permanent error fails permanently", storage.DatabaseTypeMySQL, permanent, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := &LocalClient{config: LocalConfig{Type: tc.databaseType}}
			assert.Equal(t, tc.want, client.shouldRetryAcceptedWorkError(tc.err))
		})
	}
}

// A pause rewrites every non-terminal task as failed_retryable, so it never
// lands while the revert phase is in flight. The apply row and the task rows
// are written separately, and a drive can hold either one in the revert phase
// without the other, so each alone keeps the apply from pausing.
func TestShouldPauseAcceptedWorkNeverPausesInTheRevertPhase(t *testing.T) {
	transient := fmt.Errorf("get deploy request: rate limited")
	cases := []struct {
		name       string
		applyState string
		taskState  string
		want       bool
	}{
		{"neither in the revert phase pauses", state.Apply.Running, state.Task.Running, true},
		{"apply alone in the revert window fails", state.Apply.RevertWindow, state.Task.Running, false},
		{"task alone in the revert window fails", state.Apply.Running, state.Task.RevertWindow, false},
		{"task alone reverting fails", state.Apply.Running, state.Task.Reverting, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := &LocalClient{config: LocalConfig{Type: storage.DatabaseTypeVitess}, logger: slog.New(slog.DiscardHandler)}
			apply := &storage.Apply{ApplyIdentifier: "apply-revert-guard", State: tc.applyState}
			tasks := []*storage.Task{{TaskIdentifier: "task-revert-guard", State: tc.taskState}}
			assert.Equal(t, tc.want, client.shouldPauseAcceptedWork(apply, tasks, transient))
		})
	}
}

func TestEngineDispositionsByDatabaseType(t *testing.T) {
	cases := []struct {
		name                        string
		databaseType                string
		wantNamespaceCredentials    bool
		wantCheckpointBasedRecovery bool
	}{
		{"mysql", storage.DatabaseTypeMySQL, true, true},
		{"vitess", storage.DatabaseTypeVitess, false, false},
		{"strata", storage.DatabaseTypeStrata, false, true},
		{"postgres", storage.DatabaseTypePostgres, false, false},
		{"unset", "", false, false},
		{"embedder-registered type gets conservative defaults", "customengine", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.wantNamespaceCredentials, usesPerNamespaceCredentials(tc.databaseType))
			assert.Equal(t, tc.wantCheckpointBasedRecovery, recoveryResumesFromCheckpoint(tc.databaseType))
		})
	}
}
