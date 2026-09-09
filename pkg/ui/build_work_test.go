package ui

import (
	"testing"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/stretchr/testify/assert"
)

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
			work: apitypes.BuildWork{Operation: "alter-table", BlocksDone: 5, BlocksTotal: 10, TuplesDone: 1, TuplesTotal: 2},
			want: "",
		},
		{
			name: "concurrent build with no counters renders nothing",
			work: build("building index: scanning table"),
			want: "",
		},
		{
			name: "blocks and tuples while scanning",
			work: func() apitypes.BuildWork {
				w := build("building index: scanning table")
				w.BlocksDone, w.BlocksTotal, w.TuplesDone, w.TuplesTotal = 2_500, 10_000, 12_000, 50_000
				return w
			}(),
			want: "building index: 25% of blocks (2,500/10,000) · 12,000/50,000 tuples",
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
			name: "lockers are omitted while not in a waiting phase",
			work: func() apitypes.BuildWork {
				w := build("building index: scanning table")
				w.BlocksDone, w.BlocksTotal, w.LockersDone, w.LockersTotal = 1, 4, 0, 3
				return w
			}(),
			want: "building index: 25% of blocks (1/4)",
		},
		{
			name: "outstanding lockers while waiting for writers",
			work: func() apitypes.BuildWork {
				w := build("waiting for writers before build")
				w.LockersDone, w.LockersTotal = 1, 3
				return w
			}(),
			want: "waiting on 2 of 3 lockers",
		},
		{
			name: "lockers done past total shows none outstanding",
			work: func() apitypes.BuildWork {
				w := build("waiting for old snapshots")
				w.LockersDone, w.LockersTotal = 5, 3
				return w
			}(),
			want: "waiting on 0 of 3 lockers",
		},
		{
			name: "waiting phase without a locker count renders nothing",
			work: func() apitypes.BuildWork {
				w := build("waiting for readers before marking dead")
				w.LockersDone = 2
				return w
			}(),
			want: "",
		},
		{
			name: "first attempt is not announced",
			work: func() apitypes.BuildWork {
				w := build("building index: scanning table")
				w.Attempt, w.TuplesDone, w.TuplesTotal = 1, 10, 20
				return w
			}(),
			want: "10/20 tuples",
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, FormatBuildWork(tt.work))
		})
	}
}
