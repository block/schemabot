package drain

import (
	"sync"
	"time"
)

// minimumSlice is the smallest wait a budget hands a stage, including one it
// has nothing left for.
//
// It is not a budget for the work. It is the moment it takes to observe that
// the work has already finished: a wait of exactly zero races the goroutine
// that reads the group, so a stage with nothing still running would report
// abandoning it about half the time. A spent budget still has to tell the truth
// about what it walked away from, and this is what that costs.
const minimumSlice = 100 * time.Millisecond

// Budget is one deadline shared by every stage of a shutdown.
//
// Each stage still carries a bound of its own, chosen for what that stage is
// waiting on. The budget is what relates them: a stage waits its own bound or
// whatever is left of the budget, whichever is less, so the stages cost their
// maximum rather than their sum. Without it the bounds are individually
// reasonable and collectively unbounded — nine stages that each wait a few
// seconds outlive a termination grace period between them, and the process is
// killed part-way through the last one.
//
// A nil *Budget is unbudgeted: every stage gets its own bound in full, which is
// the behavior of a caller that never armed one. Every method is safe to call
// on nil.
type Budget struct {
	deadline time.Time
}

// NewBudget returns a budget that expires total from now.
func NewBudget(total time.Duration) *Budget {
	return &Budget{deadline: time.Now().Add(total)}
}

// Allot returns how long a stage whose own bound is own may wait: its own
// bound, or what is left of the budget, whichever is less, and never less than
// minimumSlice.
func (b *Budget) Allot(own time.Duration) time.Duration {
	if b == nil {
		return own
	}
	remaining := b.Remaining()
	if remaining < own {
		own = remaining
	}
	if own < minimumSlice {
		return minimumSlice
	}
	return own
}

// Remaining reports how much of the budget is left, and zero once it is spent.
func (b *Budget) Remaining() time.Duration {
	if b == nil {
		return 0
	}
	remaining := time.Until(b.deadline)
	if remaining < 0 {
		return 0
	}
	return remaining
}

// Spent reports whether the budget has run out, which is what an expired stage
// wants in its log: a stage that overran its own bound and a stage that arrived
// to find the budget already gone are different shutdowns, and only the second
// one says the stages before it were the slow ones.
func (b *Budget) Spent() bool {
	return b != nil && b.Remaining() == 0
}

// Wait waits for wg within Allot(own), reporting whether it finished.
func (b *Budget) Wait(wg *sync.WaitGroup, own time.Duration) bool {
	return Wait(wg, b.Allot(own))
}

// WaitFor waits for done within own, or within what is left of the budget when
// that is less.
func (b *Budget) WaitFor(done <-chan struct{}, own time.Duration) bool {
	return WaitFor(done, b.Allot(own))
}
