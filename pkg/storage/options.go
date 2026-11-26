package storage

import (
	"log/slog"

	"github.com/block/schemabot/pkg/changeevents"
)

// Options carries deployment-tunable storage behavior. It holds only settings
// the storage layer itself enforces — things a claim query or a store decides,
// not anything the caller could apply on its own.
type Options struct {
	ChangeEvents   *changeevents.Sender
	ChangeEventApp func(database string) string
	ChangeEventLog *slog.Logger

	// MaxDriversPerApply caps how many operation leases one apply may hold at
	// once. Zero or negative selects DefaultMaxDriversPerApply; there is no
	// uncapped mode, because an uncapped claim path is what lets one wide
	// fan-out take every driver on the plane.
	MaxDriversPerApply int
}

// Option adjusts Options. The per-dialect constructors take options variadically
// so the common case stays a single db argument.
type Option func(*Options)

// WithMaxDriversPerApply sets the per-apply driver cap. Zero or negative selects
// DefaultMaxDriversPerApply.
func WithMaxDriversPerApply(n int) Option {
	return func(o *Options) {
		o.MaxDriversPerApply = n
	}
}

// WithChangeEvents observes committed lifecycle writes on the server's store.
func WithChangeEvents(sender *changeevents.Sender, app func(database string) string, logger *slog.Logger) Option {
	return func(o *Options) {
		o.ChangeEvents = sender
		o.ChangeEventApp = app
		o.ChangeEventLog = logger
	}
}

// BuildOptions applies opts over the defaults and returns the resolved settings.
func BuildOptions(opts ...Option) Options {
	var resolved Options
	for _, opt := range opts {
		opt(&resolved)
	}
	if resolved.MaxDriversPerApply <= 0 {
		resolved.MaxDriversPerApply = DefaultMaxDriversPerApply
	}
	return resolved
}

// OperationClaimFilter narrows which operations FindNextApplyOperation may
// claim. The zero value claims anything claimable.
type OperationClaimFilter struct {
	// SkipDeferredCutoverWork leaves out every claim that starts, resumes, or
	// recovers an operation of an apply deferring its cutover, so the claiming
	// driver cannot end up holding at a cutover only an operator can release.
	// Claims that deliver a stop or cancel request to such an operation still
	// match, because they end its work rather than start it, and so does
	// recovering a crashed drive of one while its apply has a stop or cancel
	// pending, since that recovery is what delivers the request. The driver's
	// drive hands such an apply back once the request is delivered
	// (tern.ErrDeferredCutoverDriveHandedBack).
	SkipDeferredCutoverWork bool
	// SkipOperationIDs leaves these operations out of every claim that starts
	// or resumes work. A driver lists the operations it recently held back
	// unstarted, so the claim order, oldest first, reaches other work instead
	// of handing it the same held operation on every pass. Claims that
	// recover a crashed drive or deliver a stop or cancel still match.
	SkipOperationIDs []int64
	// ReportContention makes a claim that takes nothing tell "nothing is
	// claimable" apart from "a claimable operation was row-locked by another
	// transaction". A claim skips locked rows instead of waiting on them, and
	// it can lose a row it should take to a transaction that holds it for an
	// instant: a peer claim that scans it and turns it down, or a write that
	// locks every operation of the apply. With this set, a claim that takes
	// nothing repeats its read without locking rows, and returns
	// ErrOperationClaimContended when that read finds an operation. It costs a
	// second query whenever the claim takes nothing, which on an idle plane is
	// every claim, so a caller sets it only when it has reason to expect
	// claimable work.
	ReportContention bool
}

// OperationClaimOption adjusts an OperationClaimFilter.
type OperationClaimOption func(*OperationClaimFilter)

// SkipDeferredCutoverWork sets OperationClaimFilter.SkipDeferredCutoverWork.
func SkipDeferredCutoverWork() OperationClaimOption {
	return func(f *OperationClaimFilter) {
		f.SkipDeferredCutoverWork = true
	}
}

// SkipOperations adds ids to OperationClaimFilter.SkipOperationIDs.
func SkipOperations(ids ...int64) OperationClaimOption {
	return func(f *OperationClaimFilter) {
		f.SkipOperationIDs = append(f.SkipOperationIDs, ids...)
	}
}

// ReportClaimContention sets OperationClaimFilter.ReportContention.
func ReportClaimContention() OperationClaimOption {
	return func(f *OperationClaimFilter) {
		f.ReportContention = true
	}
}

// BuildOperationClaimFilter applies opts over the zero filter.
func BuildOperationClaimFilter(opts ...OperationClaimOption) OperationClaimFilter {
	var filter OperationClaimFilter
	for _, opt := range opts {
		opt(&filter)
	}
	return filter
}
