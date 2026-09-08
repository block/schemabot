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
