package drain

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A stage whose own bound fits inside what is left of the budget waits its own
// bound. The budget is a ceiling on the whole of shutdown, not a reallocation
// of it: a shutdown where nothing hangs behaves exactly as it did before there
// was a budget.
func TestAllotLeavesAStageThatFitsAlone(t *testing.T) {
	budget := NewBudget(time.Minute)

	assert.Equal(t, 10*time.Second, budget.Allot(10*time.Second))
}

// A stage that would outlast the budget waits for what is left of it instead.
// This is what makes the bounds cost their maximum rather than their sum.
func TestAllotShortensAStageToWhatIsLeft(t *testing.T) {
	budget := NewBudget(500 * time.Millisecond)

	allotted := budget.Allot(10 * time.Second)

	assert.Less(t, allotted, 500*time.Millisecond)
	assert.Greater(t, allotted, 100*time.Millisecond)
}

// A budget with nothing left still gives each remaining stage the moment it
// takes to see that its work has already returned. A wait of exactly zero
// races the goroutine reading the group, so a stage with nothing running would
// report abandoning it about half the time — and a shutdown that is out of
// budget still has to say truthfully what it walked away from.
func TestAllotGivesASpentBudgetsStagesTheMinimumSlice(t *testing.T) {
	budget := NewBudget(time.Nanosecond)
	time.Sleep(time.Millisecond)

	require.True(t, budget.Spent())
	assert.Equal(t, minimumSlice, budget.Allot(10*time.Second))

	var wg sync.WaitGroup
	assert.True(t, budget.Wait(&wg, 10*time.Second), "a stage with nothing running is not reported abandoned")
}

// A nil budget is unbudgeted, not spent: every stage waits its own bound in
// full. Embedders that never arm a budget keep the behavior they had.
func TestANilBudgetLeavesEveryStageItsOwnBound(t *testing.T) {
	var budget *Budget

	assert.Equal(t, 10*time.Second, budget.Allot(10*time.Second))
	assert.False(t, budget.Spent())

	var wg sync.WaitGroup
	assert.True(t, budget.Wait(&wg, 10*time.Second))
}

// An earlier stage that overran shortens every stage after it. This is the
// property the budget exists for: the stages share one deadline, so a shutdown
// costs the budget however many of its stages hang.
func TestAnOverrunningStageShortensTheStagesAfterIt(t *testing.T) {
	budget := NewBudget(300 * time.Millisecond)

	var stuck sync.WaitGroup
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	stuck.Go(func() { <-release })

	start := time.Now()
	require.False(t, budget.Wait(&stuck, 10*time.Second), "the stuck stage is given up on")
	require.False(t, budget.Wait(&stuck, 10*time.Second), "so is the next one, on what little is left")
	elapsed := time.Since(start)

	assert.Less(t, elapsed, time.Second, "two stages that would each wait 10s cost the budget, not 20s")
}

// Remaining never runs negative, so a budget that is past its deadline reads as
// spent rather than as one handing out negative waits.
func TestRemainingFloorsAtZero(t *testing.T) {
	budget := NewBudget(-time.Minute)

	assert.Equal(t, time.Duration(0), budget.Remaining())
	assert.True(t, budget.Spent())
}
