// Package testctx provides the context that test teardown runs on.
//
// It is a leaf package on purpose — tests in every layer need it, so it must
// stay importable from a plain unit test without dragging in containers or
// any other test infrastructure.
package testctx

import (
	"context"
	"testing"
	"time"
)

// Cleanup returns a bounded context for teardown that must outlive the test.
// The testing package cancels the test's own context just before it runs the
// Cleanup-registered functions, so teardown needs a lifetime of its own or
// every operation it performs fails on an already-cancelled context. The
// returned context keeps the test context's values while shedding its
// cancellation. The caller must call the returned cancel.
func Cleanup(tb testing.TB, timeout time.Duration) (context.Context, context.CancelFunc) {
	tb.Helper()
	return context.WithTimeout(context.WithoutCancel(tb.Context()), timeout)
}
