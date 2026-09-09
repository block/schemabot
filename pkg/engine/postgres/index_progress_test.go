package postgres

import (
	"testing"

	"github.com/block/pg-sprite/pkg/progress"
	"github.com/stretchr/testify/assert"
)

// TestConcurrentIndexPercent pins the whole-build scale: each phase of a
// concurrent index build owns a band in PostgreSQL's phase order, counters
// interpolate within the band, a phase without counters sits at the band's
// start, and no phase reaches 100 while the build runs. Percentages are
// derived from the band table by hand, not from the function.
func TestConcurrentIndexPercent(t *testing.T) {
	cases := []struct {
		name    string
		phase   string
		work    progress.Work
		want    int
		derived bool
	}{
		{name: "initializing sits at the start", phase: "initializing", want: 0, derived: true},
		{name: "waiting for writers before build has lockers but no scale", phase: "waiting for writers before build",
			work: progress.Work{LockersTotal: 3, LockersDone: 1}, want: 0, derived: true},
		{name: "first heap scan interpolates within 0–40", phase: "building index: scanning table",
			work: progress.Work{BlocksDone: 25, BlocksTotal: 40}, want: 25, derived: true},
		{name: "first heap scan complete stops at 40", phase: "building index: scanning table",
			work: progress.Work{BlocksDone: 40, BlocksTotal: 40}, want: 40, derived: true},
		{name: "sorting live tuples sits at 40", phase: "building index: sorting live tuples", want: 40, derived: true},
		{name: "loading tuples interpolates on the tuple counters", phase: "building index: loading tuples in tree",
			work: progress.Work{TuplesDone: 10, TuplesTotal: 20}, want: 50, derived: true},
		{name: "an access method without sub-phases uses the whole build band", phase: "building index",
			work: progress.Work{BlocksDone: 25, BlocksTotal: 40}, want: 38, derived: true},
		{name: "an unlisted build sub-phase falls back to the build band", phase: "building index: some other access method step",
			work: progress.Work{BlocksDone: 1, BlocksTotal: 4}, want: 15, derived: true},
		{name: "waiting for writers before validation sits at 60", phase: "waiting for writers before validation",
			work: progress.Work{LockersTotal: 2}, want: 60, derived: true},
		{name: "index scan interpolates within 60–75", phase: "index validation: scanning index",
			work: progress.Work{BlocksDone: 1, BlocksTotal: 3}, want: 65, derived: true},
		{name: "index scan without a total yet sits at 60", phase: "index validation: scanning index", want: 60, derived: true},
		{name: "sorting tuples sits at 75", phase: "index validation: sorting tuples", want: 75, derived: true},
		{name: "second heap scan complete stays below 100", phase: "index validation: scanning table",
			work: progress.Work{BlocksDone: 40, BlocksTotal: 40}, want: 95, derived: true},
		{name: "an over-reported counter is held inside the band", phase: "index validation: scanning table",
			work: progress.Work{BlocksDone: 50, BlocksTotal: 40}, want: 95, derived: true},
		{name: "waiting for old snapshots sits at 95", phase: "waiting for old snapshots", want: 95, derived: true},
		{name: "a phase only a reindex reports derives nothing", phase: "waiting for readers before dropping",
			work: progress.Work{BlocksDone: 40, BlocksTotal: 40}, derived: false},
		{name: "an empty phase derives nothing", phase: "", derived: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := concurrentIndexPercent(tc.phase, tc.work)
			assert.Equal(t, tc.derived, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}
