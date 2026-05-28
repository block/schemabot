package clock

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestReal_NowAdvances(t *testing.T) {
	r := Real{}
	first := r.Now()
	// Real clock is monotonic and should not jump backward.
	second := r.Now()
	assert.False(t, second.Before(first), "Real.Now must be monotonic non-decreasing")
}

func TestFake_NowReturnsPinnedStart(t *testing.T) {
	start := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	f := NewFake(start)
	assert.Equal(t, start, f.Now())
	assert.Equal(t, start, f.Now(), "Fake.Now must not advance on its own")
}

func TestFake_AdvanceMovesForward(t *testing.T) {
	start := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	f := NewFake(start)
	f.Advance(7 * time.Second)
	assert.Equal(t, start.Add(7*time.Second), f.Now())
	f.Advance(2 * time.Minute)
	assert.Equal(t, start.Add(7*time.Second+2*time.Minute), f.Now())
}

func TestFake_SetPinsToTime(t *testing.T) {
	f := NewFake(time.Unix(0, 0))
	pin := time.Date(2030, 6, 15, 12, 0, 0, 0, time.UTC)
	f.Set(pin)
	assert.Equal(t, pin, f.Now())
}

func TestDefault_NilInterfaceReturnsReal(t *testing.T) {
	got := Default(nil)
	assert.IsType(t, Real{}, got)
	assert.NotPanics(t, func() { _ = got.Now() })
}

func TestDefault_TypedNilFakeReturnsReal(t *testing.T) {
	var f *Fake // typed-nil
	got := Default(f)
	assert.IsType(t, Real{}, got)
	assert.NotPanics(t, func() { _ = got.Now() })
}

func TestDefault_NonNilPassesThrough(t *testing.T) {
	f := NewFake(time.Unix(42, 0))
	got := Default(f)
	assert.Same(t, f, got)
}

func TestFake_ConcurrentAdvanceAndNowIsRaceFree(t *testing.T) {
	f := NewFake(time.Unix(0, 0))
	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() { f.Advance(time.Millisecond) })
		wg.Go(func() { _ = f.Now() })
	}
	wg.Wait()
	assert.Equal(t, time.Unix(0, 0).Add(100*time.Millisecond), f.Now())
}
