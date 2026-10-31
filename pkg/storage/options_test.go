package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestBuildOptions_MaxDriversPerApply covers how the fan-out cap resolves. Every
// deployment that omits max_drivers_per_apply passes the zero value through from
// its server config, so the zero case is the common one rather than an edge —
// and there is no uncapped mode, because an uncapped claim path is what lets one
// wide fan-out take every driver on the plane.
func TestBuildOptions_MaxDriversPerApply(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []Option
		want int
	}{
		{name: "unset", want: DefaultMaxDriversPerApply},
		{name: "zero from an omitted config key", opts: []Option{WithMaxDriversPerApply(0)}, want: DefaultMaxDriversPerApply},
		{name: "negative", opts: []Option{WithMaxDriversPerApply(-3)}, want: DefaultMaxDriversPerApply},
		{name: "configured", opts: []Option{WithMaxDriversPerApply(6)}, want: 6},
		{name: "last option wins", opts: []Option{WithMaxDriversPerApply(6), WithMaxDriversPerApply(3)}, want: 3},
		{name: "one is a valid cap, not a disabled one", opts: []Option{WithMaxDriversPerApply(1)}, want: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, BuildOptions(tc.opts...).MaxDriversPerApply)
		})
	}
}

// TestBuildOptions_DriversPerProcess covers how the process pool size resolves.
// A deployment that omits drivers passes the zero value through, and must get
// the same pool the operator starts by default.
func TestBuildOptions_DriversPerProcess(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []Option
		want int
	}{
		{name: "unset", want: DefaultDriversPerProcess},
		{name: "zero from an omitted config key", opts: []Option{WithDriversPerProcess(0)}, want: DefaultDriversPerProcess},
		{name: "negative", opts: []Option{WithDriversPerProcess(-1)}, want: DefaultDriversPerProcess},
		{name: "configured", opts: []Option{WithDriversPerProcess(16)}, want: 16},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, BuildOptions(tc.opts...).DriversPerProcess)
		})
	}
}

// TestProcessDriverShare covers the per-process share: half the pool, rounded
// down, and never zero, since a pool of one still has to run work.
func TestProcessDriverShare(t *testing.T) {
	for pool, want := range map[int]int{1: 1, 2: 1, 3: 1, 4: 2, 5: 2, 16: 8} {
		assert.Equal(t, want, ProcessDriverShare(pool), "pool of %d", pool)
	}
}
