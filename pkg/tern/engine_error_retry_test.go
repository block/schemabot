package tern

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/storage"
)

// A failed engine attempt pauses for operator recovery only on database types
// whose engine resumes a re-claimed attempt from a durable checkpoint — MySQL
// and Strata both drive Spirit, so recovery continues the same in-flight work.
// Other types classify engine errors as permanent, and a typed permanent error
// is never paused for retry regardless of type. An unknown outcome pauses on
// every type, because the work may be running and recovery re-observes it.
func TestShouldRetryEngineErrorByDatabaseType(t *testing.T) {
	transient := fmt.Errorf("engine lost its connection to the target")
	permanent := engine.NewPermanentError("plan references a dropped table")
	unknown := engine.NewOutcomeUnknownError("deploy outcome unknown: %w", transient)
	unrecordedUnknown := engine.NewPermanentError("deploy not named in resume state: %w", unknown)

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
		{"vitess unknown outcome pauses for retry", storage.DatabaseTypeVitess, unknown, true},
		{"postgres unknown outcome pauses for retry", storage.DatabaseTypePostgres, unknown, true},
		{"mysql unknown outcome pauses for retry", storage.DatabaseTypeMySQL, unknown, true},
		{"vitess permanent unknown outcome fails permanently", storage.DatabaseTypeVitess, unrecordedUnknown, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := &LocalClient{config: LocalConfig{Type: tc.databaseType}}
			assert.Equal(t, tc.want, client.shouldRetryEngineError(tc.err))
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
