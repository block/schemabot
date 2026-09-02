package ratelimit

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/clock"
)

var testStart = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestNewRejectsUnusableBudgets(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
	}{
		{"zero rate", Config{RequestsPerMinute: 0, Burst: 10}},
		{"negative rate", Config{RequestsPerMinute: -1, Burst: 10}},
		{"zero burst", Config{RequestsPerMinute: 60, Burst: 0}},
		{"negative burst", Config{RequestsPerMinute: 60, Burst: -1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Nil(t, New(tc.cfg, clock.NewFake(testStart)))
		})
	}
}

// A nil limiter is the disabled state, so it admits every request without a
// wait rather than blocking everything.
func TestNilLimiterAdmits(t *testing.T) {
	var l *Limiter
	for range 100 {
		allowed, retryAfter := l.Allow("caller")
		require.True(t, allowed)
		assert.Zero(t, retryAfter)
	}
}

func TestAllowSpendsBurstThenLimits(t *testing.T) {
	fake := clock.NewFake(testStart)
	l := New(Config{RequestsPerMinute: 60, Burst: 3}, fake)
	require.NotNil(t, l)

	for i := range 3 {
		allowed, retryAfter := l.Allow("caller")
		require.True(t, allowed, "request %d should be within the burst", i)
		assert.Zero(t, retryAfter)
	}

	allowed, retryAfter := l.Allow("caller")
	require.False(t, allowed, "the request after the burst is exhausted must be limited")
	// At 60 per minute one token refills every second.
	assert.Equal(t, time.Second, retryAfter)
}

// A limited request costs the key nothing, so a client that keeps retrying
// against a dry bucket does not push its own recovery further out.
func TestLimitedRequestsDoNotExtendTheWait(t *testing.T) {
	fake := clock.NewFake(testStart)
	l := New(Config{RequestsPerMinute: 60, Burst: 1}, fake)
	require.NotNil(t, l)

	allowed, _ := l.Allow("caller")
	require.True(t, allowed)

	for range 10 {
		allowed, retryAfter := l.Allow("caller")
		require.False(t, allowed)
		assert.Equal(t, time.Second, retryAfter)
	}
}

func TestBudgetRefillsAsTimeAdvances(t *testing.T) {
	fake := clock.NewFake(testStart)
	l := New(Config{RequestsPerMinute: 60, Burst: 2}, fake)
	require.NotNil(t, l)

	for range 2 {
		allowed, _ := l.Allow("caller")
		require.True(t, allowed)
	}
	allowed, _ := l.Allow("caller")
	require.False(t, allowed)

	fake.Advance(time.Second)
	allowed, retryAfter := l.Allow("caller")
	require.True(t, allowed, "one token should have refilled after a second")
	assert.Zero(t, retryAfter)

	allowed, _ = l.Allow("caller")
	require.False(t, allowed, "only one token refilled, so the next request is limited again")
}

// Each key has its own budget: one exhausted caller must not deny another.
func TestKeysAreIndependent(t *testing.T) {
	fake := clock.NewFake(testStart)
	l := New(Config{RequestsPerMinute: 60, Burst: 1}, fake)
	require.NotNil(t, l)

	allowed, _ := l.Allow("caller-a")
	require.True(t, allowed)
	allowed, _ = l.Allow("caller-a")
	require.False(t, allowed)

	allowed, retryAfter := l.Allow("caller-b")
	require.True(t, allowed, "a different key has its own budget")
	assert.Zero(t, retryAfter)
}

// The burst cap holds no matter how large the configured rate is, so a single
// key cannot spend an unbounded number of requests at one instant.
func TestBurstBoundsASingleInstant(t *testing.T) {
	fake := clock.NewFake(testStart)
	l := New(Config{RequestsPerMinute: 6000, Burst: 5}, fake)
	require.NotNil(t, l)

	admitted := 0
	for range 50 {
		if allowed, _ := l.Allow("caller"); allowed {
			admitted++
		}
	}
	assert.Equal(t, 5, admitted)
}

func TestIdleBucketsAreEvicted(t *testing.T) {
	fake := clock.NewFake(testStart)
	l := New(Config{RequestsPerMinute: 60, Burst: 1}, fake)
	require.NotNil(t, l)

	allowed, _ := l.Allow("idle-caller")
	require.True(t, allowed)
	require.Len(t, l.buckets, 1)

	// Short of the TTL the bucket is retained, even once a sweep has run.
	fake.Advance(sweepInterval + time.Second)
	allowed, _ = l.Allow("active-caller")
	require.True(t, allowed)
	assert.Len(t, l.buckets, 2, "a bucket within its TTL must be retained")

	fake.Advance(idleTTL + time.Second)
	allowed, _ = l.Allow("active-caller")
	require.True(t, allowed)
	assert.Len(t, l.buckets, 1, "buckets idle beyond the TTL are dropped")
	assert.Contains(t, l.buckets, "active-caller")
}

// An evicted key starts from a full budget, which is why eviction is safe: a
// bucket idle for longer than the TTL has already refilled.
func TestEvictedKeyStartsFromAFullBudget(t *testing.T) {
	fake := clock.NewFake(testStart)
	l := New(Config{RequestsPerMinute: 60, Burst: 2}, fake)
	require.NotNil(t, l)

	for range 2 {
		allowed, _ := l.Allow("caller")
		require.True(t, allowed)
	}

	fake.Advance(idleTTL + time.Second)
	for i := range 2 {
		allowed, _ := l.Allow("caller")
		require.True(t, allowed, "request %d after eviction should be within a fresh burst", i)
	}
}

func TestAllowIsSafeUnderConcurrency(t *testing.T) {
	l := New(Config{RequestsPerMinute: 6000, Burst: 100}, clock.Real{})
	require.NotNil(t, l)

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			for j := range 50 {
				l.Allow("caller")
				l.Allow(string(rune('a'+i)) + string(rune('0'+j%10)))
			}
		})
	}
	wg.Wait()
}
