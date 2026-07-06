//go:build e2e || integration

package testutil

import (
	"context"
	"time"
)

// CleanupContext returns a bounded context for teardown that must outlive the
// test: t.Cleanup callbacks and deferred cleanup run after the test's own
// context is already cancelled, so they need a lifetime of their own. The
// caller must call the returned cancel.
func CleanupContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), timeout)
}
