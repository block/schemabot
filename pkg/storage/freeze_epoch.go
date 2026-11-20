package storage

import "context"

// FreezeEpochGuard ties an admission write to the freeze check that allowed
// it. The change gate reads the environment's freeze epoch before it lists
// the freezes in effect; creating a freeze bumps the epoch in the same
// transaction that inserts it. The transaction that admits the change locks
// the epoch row and refuses the write with ErrFreezeEpochChanged when the
// epoch moved, so a freeze created after the check and before the write
// cannot be missed. The lock holds a freeze being created until the write
// commits, so the freeze is acknowledged only after the change it did not
// see.
type FreezeEpochGuard struct {
	Environment string
	Epoch       int64
}

type freezeEpochGuardKey struct{}

// WithFreezeEpochGuard returns ctx carrying guard, for the admission write
// that follows a passed change gate.
func WithFreezeEpochGuard(ctx context.Context, guard FreezeEpochGuard) context.Context {
	guard.Environment = CanonicalKey(guard.Environment)
	return context.WithValue(ctx, freezeEpochGuardKey{}, guard)
}

// FreezeEpochGuardFrom returns the guard ctx carries, if any.
func FreezeEpochGuardFrom(ctx context.Context) (FreezeEpochGuard, bool) {
	guard, ok := ctx.Value(freezeEpochGuardKey{}).(FreezeEpochGuard)
	return guard, ok
}
