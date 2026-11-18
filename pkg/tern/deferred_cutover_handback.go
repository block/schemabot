package tern

import (
	"context"
	"errors"
	"log/slog"

	"github.com/block/schemabot/pkg/storage"
)

// ErrDeferredCutoverDriveHandedBack is returned by a drive running under
// WithControlDeliveryOnlyForDeferredCutover once it has delivered the apply's
// pending stop or cancel and the request did not end the drive: a refused or
// declined stop, or a stop with a start already queued behind it. The drive
// does no engine work, and the caller hands its claim back so a driver that
// runs deferred work resumes the apply.
var ErrDeferredCutoverDriveHandedBack = errors.New("drive delivered the pending control request and handed the apply back: it defers its cutover, and this driver does not drive deferred work")

type controlDeliveryOnlyForDeferredCutoverKey struct{}

// WithControlDeliveryOnlyForDeferredCutover marks ctx as the drive of a driver
// that skips deferred-cutover work. Such a driver claims an apply deferring its
// cutover only to deliver a pending stop or cancel, so its drive delivers the
// request and stops there instead of running the apply to its held cutover.
func WithControlDeliveryOnlyForDeferredCutover(ctx context.Context) context.Context {
	return context.WithValue(ctx, controlDeliveryOnlyForDeferredCutoverKey{}, true)
}

// ControlDeliveryOnlyForDeferredCutover reports whether ctx carries
// WithControlDeliveryOnlyForDeferredCutover.
func ControlDeliveryOnlyForDeferredCutover(ctx context.Context) bool {
	marked, _ := ctx.Value(controlDeliveryOnlyForDeferredCutoverKey{}).(bool)
	return marked
}

// handBackAfterControlDelivery reports whether a drive that has just delivered
// its pending stop or cancel, without standing down, must hand the apply back
// rather than drive it on. A cutover drive never asks: it is how the deferred
// cutover ends, not deferred work.
func handBackAfterControlDelivery(ctx context.Context, apply *storage.Apply, logger *slog.Logger) bool {
	if !ControlDeliveryOnlyForDeferredCutover(ctx) {
		return false
	}
	if !apply.GetOptions().DeferCutover {
		return false
	}
	logger.Info("delivered the pending control request on an apply that defers its cutover; handing the apply back to a driver that runs deferred work",
		apply.MutableLogAttrs()...)
	return true
}
