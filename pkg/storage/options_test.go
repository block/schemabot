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
