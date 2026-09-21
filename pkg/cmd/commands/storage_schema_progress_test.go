package commands

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
)

// clockedPrinter is a printer over a clock a test moves by hand, so the
// heartbeat intervals can be exercised without any test waiting for them.
func clockedPrinter(out *strings.Builder, now *time.Time) *storageProgressPrinter {
	p := newStorageProgressPrinter(out, false)
	p.now = func() time.Time { return *now }
	return p
}

// The convergence polls ten times a second and mostly sees the same thing.
// One line per poll is a scrollback nobody reads, so an observation that says
// what the last one said prints nothing.
func TestStorageProgressPrinter_RepeatsPrintOnce(t *testing.T) {
	var out strings.Builder
	clock := time.Now()
	printer := clockedPrinter(&out, &clock)

	observation := api.StorageConvergenceProgress{State: "copying", Percent: 40, DDLCount: 1}
	for range 20 {
		printer.observe(observation)
		clock = clock.Add(100 * time.Millisecond)
	}

	assert.Equal(t, 1, strings.Count(out.String(), "progress=40%"),
		"twenty identical polls are one thing happening, and one line")
}

// Rows copied moves on every poll, so a line per change would be a line per
// poll under a different name. Changes that are only numbers wait out the
// heartbeat interval — the first one briefly, for early confirmation, and the
// rest at log mode's own cadence.
func TestStorageProgressPrinter_HeartbeatsMovingNumbers(t *testing.T) {
	var out strings.Builder
	clock := time.Now()
	printer := clockedPrinter(&out, &clock)

	copying := func(rows int64) api.StorageConvergenceProgress {
		return api.StorageConvergenceProgress{
			State:   "copying",
			Percent: 40,
			Tables:  []api.StorageConvergenceTableProgress{{Table: "applies", State: "copying", RowsCopied: rows}},
		}
	}

	for rows := int64(1); rows <= 10; rows++ {
		printer.observe(copying(rows * 1000))
		clock = clock.Add(100 * time.Millisecond)
	}
	require.Equal(t, 1, strings.Count(out.String(), "table=applies"),
		"a second of row counts is the announcement and nothing more")

	clock = clock.Add(logFirstHeartbeat)
	printer.observe(copying(99000))
	require.Equal(t, 1, strings.Count(out.String(), "Copying rows"),
		"the first heartbeat comes quickly, so an operator sees early that it is moving")

	clock = clock.Add(logFirstHeartbeat)
	printer.observe(copying(120000))
	assert.Equal(t, 1, strings.Count(out.String(), "Copying rows"),
		"after the first, heartbeats settle to the longer interval")

	clock = clock.Add(logHeartbeatDefault)
	printer.observe(copying(150000))
	assert.Equal(t, 2, strings.Count(out.String(), "Copying rows"),
		"past that interval the numbers are worth a line again")
}

// A change of state is the transition an operator is waiting for — the copy
// finishing, the cutover starting — and there are only a handful in a whole
// run. It prints whatever the heartbeat interval says.
func TestStorageProgressPrinter_AlwaysPrintsAStateChange(t *testing.T) {
	var out strings.Builder
	clock := time.Now()
	printer := clockedPrinter(&out, &clock)

	printer.observe(api.StorageConvergenceProgress{State: "copying", Percent: 99})
	clock = clock.Add(10 * time.Millisecond)
	printer.observe(api.StorageConvergenceProgress{State: "cutting_over", Percent: 99})

	assert.Contains(t, out.String(), "Convergence started")
	assert.Contains(t, out.String(), "Convergence cutting_over",
		"a state change must not be swallowed by the heartbeat interval")
}

// Every line is log mode's own shape — a timestamp and key=value pairs — so an
// operator who reads `apply --output log` reads this without learning a second
// format, and the measurements a dialect could not take are simply absent
// rather than printed as a zero nobody measured.
func TestStorageProgressPrinter_EmitsLogfmt(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		observations []api.StorageConvergenceProgress
		want         []string
		absent       []string
	}{
		"a table mid-copy": {
			observations: []api.StorageConvergenceProgress{{
				State:   "copying",
				Percent: 62,
				Tables: []api.StorageConvergenceTableProgress{
					{Table: "applies", State: "copying", Percent: 62, RowsCopied: 1203441},
				},
			}},
			want: []string{"Table started", "table=applies", "status=copying", "progress=62%", "rows_copied=1,203,441"},
		},
		"a dialect with no partial state": {
			observations: []api.StorageConvergenceProgress{{
				State:   "running",
				Percent: 50,
				Tables:  []api.StorageConvergenceTableProgress{{Table: "checks", State: "running"}},
			}},
			want:   []string{"Table started", "table=checks", "status=running"},
			absent: []string{"progress=", "rows_copied="},
		},
		"the convergence before any table progress exists": {
			observations: []api.StorageConvergenceProgress{{State: "pending", Percent: 3}},
			want:         []string{"Convergence started", "status=pending", "progress=3%"},
			absent:       []string{"table="},
		},
		"a transition carries how long the subject has been running": {
			observations: []api.StorageConvergenceProgress{
				{Tables: []api.StorageConvergenceTableProgress{{Table: "applies", State: "copying"}}},
				{Tables: []api.StorageConvergenceTableProgress{{Table: "applies", State: "complete", Percent: 100}}},
			},
			want: []string{"Table complete", "table=applies", "duration=", "progress=100%"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var out strings.Builder
			clock := time.Now()
			printer := clockedPrinter(&out, &clock)
			for _, o := range tc.observations {
				printer.observe(o)
				clock = clock.Add(time.Second)
			}
			got := stripANSI(out.String())
			for _, want := range tc.want {
				assert.Contains(t, got, want)
			}
			for _, absent := range tc.absent {
				assert.NotContains(t, got, absent,
					"a measurement this dialect never took must not be printed as a zero")
			}
		})
	}
}

// An observation carrying nothing prints nothing at all, rather than a bare
// timestamp per poll.
func TestStorageProgressPrinter_SaysNothingAboutAnEmptyObservation(t *testing.T) {
	var out strings.Builder
	clock := time.Now()
	printer := clockedPrinter(&out, &clock)

	printer.observe(api.StorageConvergenceProgress{DDLCount: 3})

	assert.Empty(t, out.String())
}

// A convergence over several tables tracks each one on its own, so one table's
// heartbeat cannot suppress another table's transition.
func TestStorageProgressPrinter_TracksEachTableSeparately(t *testing.T) {
	var out strings.Builder
	clock := time.Now()
	printer := clockedPrinter(&out, &clock)

	printer.observe(api.StorageConvergenceProgress{
		State: "copying",
		Tables: []api.StorageConvergenceTableProgress{
			{Table: "applies", State: "copying"},
			{Table: "checks", State: "copying"},
		},
	})
	clock = clock.Add(10 * time.Millisecond)
	printer.observe(api.StorageConvergenceProgress{
		State: "copying",
		Tables: []api.StorageConvergenceTableProgress{
			{Table: "applies", State: "copying"},
			{Table: "checks", State: "complete"},
		},
	})

	got := stripANSI(out.String())
	assert.Contains(t, got, "Table started table=applies")
	assert.Contains(t, got, "Table started table=checks")
	assert.Contains(t, got, "Table complete table=checks")
	assert.NotContains(t, got, "Table complete table=applies")
}

// A line carries a time and no zone, so the two log-mode surfaces have to
// agree on which clock it is. The apply watcher stamps in UTC.
func TestNewStorageProgressPrinter_StampsInUTC(t *testing.T) {
	t.Parallel()

	printer := newStorageProgressPrinter(&strings.Builder{}, false)

	assert.Equal(t, time.UTC, printer.now().Location(),
		"a host outside UTC would otherwise print a different clock than apply --output log for the same instant")
}

// Progress goes to stderr so that stdout stays a document a caller can parse,
// which makes redirecting it to a file the obvious thing to do with it. A file
// is not a terminal, and escape bytes in one are noise no operator asked for —
// so the stream being written to is what decides, not stdout's terminal-ness
// and not an assumption that a person is watching.
func TestNewStorageProgressPrinter_LeavesEscapesOutOfARedirectedStream(t *testing.T) {
	t.Parallel()

	var plain strings.Builder
	printer := newStorageProgressPrinter(&plain, false)
	printer.observe(api.StorageConvergenceProgress{
		State:  "copyRows",
		Tables: []api.StorageConvergenceTableProgress{{Table: "applies", State: "copyRows", Percent: 12}},
	})

	assert.NotContains(t, plain.String(), "\x1b[", "a redirected stream gets the line without escapes")
	assert.Contains(t, plain.String(), "table=applies", "and it is otherwise the same line")
	assert.Contains(t, plain.String(), "progress=12%")

	var colored strings.Builder
	terminal := newStorageProgressPrinter(&colored, true)
	terminal.observe(api.StorageConvergenceProgress{
		State:  "copyRows",
		Tables: []api.StorageConvergenceTableProgress{{Table: "applies", State: "copyRows", Percent: 12}},
	})
	assert.Contains(t, colored.String(), "\x1b[", "a terminal still gets color")
}
