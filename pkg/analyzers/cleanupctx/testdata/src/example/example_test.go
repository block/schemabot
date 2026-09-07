package example

import (
	"context"
	"testing"
	"time"
)

func TestCalledInsideCleanup(t *testing.T) {
	t.Cleanup(func() {
		use(t.Context()) // want "t.Context\\(\\) is already cancelled inside t.Cleanup"
	})
}

func TestCapturedFromEnclosingTest(t *testing.T) {
	ctx := t.Context()
	use(ctx)
	t.Cleanup(func() {
		use(ctx) // want "ctx is the test context and is already cancelled inside t.Cleanup"
	})
}

func TestDerivedInsideCleanup(t *testing.T) {
	t.Cleanup(func() {
		// Deriving from the test context does not revive it.
		c, cancel := context.WithTimeout(t.Context(), time.Second) // want "t.Context\\(\\) is already cancelled inside t.Cleanup"
		defer cancel()
		use(c)
	})
}

func TestNestedInsideCleanup(t *testing.T) {
	t.Cleanup(func() {
		func() {
			use(t.Context()) // want "t.Context\\(\\) is already cancelled inside t.Cleanup"
		}()
	})
}

// A helper receiving testing.TB is subject to the same rule.
func helperReceiver(tb testing.TB) {
	tb.Cleanup(func() {
		use(tb.Context()) // want "tb.Context\\(\\) is already cancelled inside tb.Cleanup"
	})
}

func TestDetachedIsAccepted(t *testing.T) {
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Second)
		defer cancel()
		use(c)
	})
}

func TestBareDetachedIsAccepted(t *testing.T) {
	t.Cleanup(func() {
		use(context.WithoutCancel(t.Context()))
	})
}

// Outside a cleanup callback the test context is the correct one to use, so
// neither the call nor a variable holding its result is reported.
func TestOutsideCleanupIsAccepted(t *testing.T) {
	ctx := t.Context()
	use(ctx)
	use(t.Context())
	t.Cleanup(func() {
		use(context.Background())
	})
}

// A variable that merely shares the name of a test context is not reported;
// the rule tracks the object, not the identifier.
func TestUnrelatedVariableIsAccepted(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		use(ctx)
	})
}

func use(ctx context.Context) { _ = ctx }

var _ = helperReceiver
