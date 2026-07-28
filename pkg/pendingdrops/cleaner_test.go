package pendingdrops

import (
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCleaner_TargetWithoutLockerFailsClosed verifies a target missing its
// advisory-lock implementation is rejected before any connection is opened:
// cleanup must not run unserialized against a target, and must not assume
// MySQL lock semantics for an engine the producer did not declare.
func TestCleaner_TargetWithoutLockerFailsClosed(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	targets := []Target{{Database: "testdb", Environment: "staging", DSN: "root:pw@tcp(127.0.0.1:1)/testdb?timeout=2s"}}
	cleaner := NewCleaner(targets, DefaultRetention, false, logger)

	err := cleaner.Run(t.Context())
	require.ErrorContains(t, err, "no advisory locker")
}
