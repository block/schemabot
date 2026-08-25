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
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.wantNamespaceCredentials, usesPerNamespaceCredentials(tc.databaseType))
			assert.Equal(t, tc.wantCheckpointBasedRecovery, recoveryResumesFromCheckpoint(tc.databaseType))
		})
	}
}

func TestEngineDispositionsRejectUnknownDatabaseType(t *testing.T) {
	assert.PanicsWithValue(t,
		`namespace credential disposition is not declared for database type "unknown"`,
		func() { usesPerNamespaceCredentials("unknown") },
	)
	assert.PanicsWithValue(t,
		`checkpoint recovery disposition is not declared for database type "unknown"`,
		func() { recoveryResumesFromCheckpoint("unknown") },
	)
}
