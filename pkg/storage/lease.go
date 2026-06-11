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

type operationLeaseContextKey struct{}

// OperationLease is the ownership capability for one claimed apply_operation.
// It is distinct from ApplyLease: it identifies the operation row and carries
// the operation's own token, so storage writes can guard on the operation's
// claim instead of the parent apply's. Keeping the two leases separate lets a
// worker hold both at once during the migration to operation-scoped writes
// without conflating an operation token with the parent apply token.
type OperationLease struct {
	ApplyID     int64
	OperationID int64
	Owner       string
	Token       string
}

// Valid reports whether the lease can be used as a write token.
func (l OperationLease) Valid() bool {
	return l.ApplyID > 0 && l.OperationID > 0 && l.Token != ""
}

// WithOperationLease attaches an operation lease to ctx for storage methods that
// enforce operation-owned writes.
func WithOperationLease(ctx context.Context, lease OperationLease) context.Context {
	return context.WithValue(ctx, operationLeaseContextKey{}, lease)
}

// OperationLeaseFromContext returns the operation lease attached to ctx.
func OperationLeaseFromContext(ctx context.Context) (OperationLease, bool) {
	lease, ok := ctx.Value(operationLeaseContextKey{}).(OperationLease)
	return lease, ok
}
