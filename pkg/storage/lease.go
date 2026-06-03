package storage

import "context"

type applyLeaseContextKey struct{}

// ApplyLease is the ownership capability for one claimed apply.
// Storage writes that receive this lease through context must match the current
// token on the apply row before mutating lease-owned state.
type ApplyLease struct {
	ApplyID int64
	Owner   string
	Token   string
}

// Valid reports whether the lease can be used as a write token.
func (l ApplyLease) Valid() bool {
	return l.ApplyID > 0 && l.Token != ""
}

// WithApplyLease attaches an apply lease to ctx for storage methods that enforce
// lease-owned writes.
func WithApplyLease(ctx context.Context, lease ApplyLease) context.Context {
	return context.WithValue(ctx, applyLeaseContextKey{}, lease)
}

// ApplyLeaseFromContext returns the apply lease attached to ctx.
func ApplyLeaseFromContext(ctx context.Context) (ApplyLease, bool) {
	lease, ok := ctx.Value(applyLeaseContextKey{}).(ApplyLease)
	return lease, ok
}
