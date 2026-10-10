package ui

import (
	"testing"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/stretchr/testify/assert"
)

// TestFormatBuildWork walks a btree build the way PostgreSQL reports it: each
// phase's counters stay populated into the phases that follow, and the line
// must describe only the phase in flight. The fixtures pair each phase with
// the counters a real build carries into it.
func TestFormatBuildWork(t *testing.T) {
	build := func(phase string) apitypes.BuildWork {
		return apitypes.BuildWork{Operation: "concurrent-index-build", ServerPhase: phase}
	}

	tests := []struct {
		name string
		work apitypes.BuildWork
		want string
	}{
		{
			name: "other operations render nothing",
			work: apitypes.BuildWork{Operation: "alter-table", ServerPhase: "building index: scanning table", BlocksDone: 5, BlocksTotal: 10},
			want: "",
		},
		{
			name: "no phase yet renders nothing",
			work: build(""),
			want: "",
		},
		{
			name: "a phase the engine does not know renders nothing",
			work: func() apitypes.BuildWork {
				w := build("future phase")
				w.BlocksDone, w.BlocksTotal = 5, 10
				return w
			}(),
			want: "",
		},
		{
			name: "scanning table before a block total is published renders nothing",
			work: build("building index: scanning table"),
			want: "",
		},
		{
			name: "outstanding lockers while waiting for writers before build",
			work: func() apitypes.BuildWork {
				w := build("waiting for writers before build")
				w.LockersDone, w.LockersTotal = 1, 3
				return w
			}(),
			want: "waiting on 2 of 3 lockers",
		},
		{
			name: "btree initializing names the phase",
			work: build("building index: initializing"),
			want: "building index: initializing",
		},
		{
			name: "scanning table reports blocks",
			work: func() apitypes.BuildWork {
				w := build("building index: scanning table")
				w.BlocksDone, w.BlocksTotal = 2_500, 10_000
				return w
			}(),
			want: "building index: 25% of blocks (2,500/10,000)",
		},
		{
			name: "blocks done past total clamps to 100 percent",
			work: func() apitypes.BuildWork {
				w := build("building index: scanning table")
				w.BlocksDone, w.BlocksTotal = 10_250, 10_000
				return w
			}(),
			want: "building index: 100% of blocks (10,250/10,000)",
		},
		{
			name: "sorting live tuples names the phase instead of the finished scan",
			work: func() apitypes.BuildWork {
				w := build("building index: sorting live tuples")
				w.BlocksDone, w.BlocksTotal = 10_000, 10_000
				return w
			}(),
			want: "building index: sorting live tuples",
		},
		{
			name: "loading tuples reports tuples, not the finished scan's blocks",
			work: func() apitypes.BuildWork {
				w := build("building index: loading tuples in tree")
				w.BlocksDone, w.BlocksTotal, w.TuplesDone, w.TuplesTotal = 10_000, 10_000, 12_000, 50_000
				return w
			}(),
			want: "building index: 12,000/50,000 tuples",
		},
		{
			name: "an access method without sub-phases reports blocks while it has a block total",
			work: func() apitypes.BuildWork {
				w := build("building index")
				w.BlocksDone, w.BlocksTotal = 1, 4
				return w
			}(),
			want: "building index: 25% of blocks (1/4)",
		},
		{
			name: "an access method without sub-phases reports tuples otherwise",
			work: func() apitypes.BuildWork {
				w := build("building index")
				w.TuplesDone, w.TuplesTotal = 10, 20
				return w
			}(),
			want: "building index: 10/20 tuples",
		},
		{
			name: "carried build counters are omitted while waiting for writers before validation",
			work: func() apitypes.BuildWork {
				w := build("waiting for writers before validation")
				w.BlocksDone, w.BlocksTotal, w.TuplesDone, w.TuplesTotal, w.LockersDone, w.LockersTotal = 10_000, 10_000, 50_000, 50_000, 1, 4
				return w
			}(),
			want: "waiting on 3 of 4 lockers",
		},
		{
			name: "validation index scan is labelled as validation",
			work: func() apitypes.BuildWork {
				w := build("index validation: scanning index")
				w.BlocksDone, w.BlocksTotal, w.TuplesDone, w.TuplesTotal = 93, 100, 50_000, 50_000
				return w
			}(),
			want: "validating index: 93% of blocks (93/100)",
		},
		{
			name: "validation sort names the phase",
			work: func() apitypes.BuildWork {
				w := build("index validation: sorting tuples")
				w.BlocksDone, w.BlocksTotal = 100, 100
				return w
			}(),
			want: "validating index: sorting tuples",
		},
		{
			name: "validation table scan is labelled as validation",
			work: func() apitypes.BuildWork {
				w := build("index validation: scanning table")
				w.BlocksDone, w.BlocksTotal = 7_500, 10_000
				return w
			}(),
			want: "validating index: 75% of blocks (7,500/10,000)",
		},
		{
			name: "lockers done past total shows none outstanding",
			work: func() apitypes.BuildWork {
				w := build("waiting for old snapshots")
				w.BlocksDone, w.BlocksTotal, w.LockersDone, w.LockersTotal = 10_000, 10_000, 5, 3
				return w
			}(),
			want: "waiting on 0 of 3 lockers",
		},
		{
			name: "waiting phase without a locker count renders nothing",
			work: func() apitypes.BuildWork {
				w := build("waiting for readers before dropping")
				w.LockersDone = 2
				return w
			}(),
			want: "",
		},
		{
			name: "first attempt is not announced",
			work: func() apitypes.BuildWork {
				w := build("building index: loading tuples in tree")
				w.Attempt, w.TuplesDone, w.TuplesTotal = 1, 10, 20
				return w
			}(),
			want: "building index: 10/20 tuples",
		},
		{
			name: "retry appends the attempt",
			work: func() apitypes.BuildWork {
				w := build("waiting for writers before validation")
				w.Attempt, w.LockersDone, w.LockersTotal = 2, 0, 1
				return w
			}(),
			want: "waiting on 1 of 1 lockers · attempt 2",
		},
		{
			name: "retry is announced even before the phase publishes counters",
			work: func() apitypes.BuildWork {
				w := build("building index: scanning table")
				w.Attempt = 2
				return w
			}(),
			want: "attempt 2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, FormatBuildWork(tt.work))
		})
	}
}
