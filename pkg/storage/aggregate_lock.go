package storage

import "context"

// AggregateCheckLocker serializes an entire read/fold/publication across
// publishers sharing storage. Implementations must release on cancellation and
// process exit; holding an in-memory mutex alone is insufficient across pods.
type AggregateCheckLocker interface {
	WithAggregateCheckLock(context.Context, string, int, func(context.Context) error) error
}
