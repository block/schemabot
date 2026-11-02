package storage

// Options carries deployment-tunable storage behavior. It holds only settings
// the storage layer itself enforces — things a claim query or a store decides,
// not anything the caller could apply on its own.
type Options struct {
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
	// match, because they end its work rather than start it.
	SkipDeferredCutoverWork bool
}

// OperationClaimOption adjusts an OperationClaimFilter.
type OperationClaimOption func(*OperationClaimFilter)

// SkipDeferredCutoverWork sets OperationClaimFilter.SkipDeferredCutoverWork.
func SkipDeferredCutoverWork() OperationClaimOption {
	return func(f *OperationClaimFilter) {
		f.SkipDeferredCutoverWork = true
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
