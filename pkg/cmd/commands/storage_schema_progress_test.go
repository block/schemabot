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
// interval rule can be exercised without any test waiting for it.
func clockedPrinter(out *strings.Builder, now *time.Time) *storageProgressPrinter {
	p := newStorageProgressPrinter(out)
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

	assert.Equal(t, 1, strings.Count(out.String(), "copying 40%"),
		"twenty identical polls are one thing happening, and one line")
}

// Rows copied moves on every poll, so a line per change would be a line per
// poll under a different name. Changes that are only numbers wait out the
// interval.
func TestStorageProgressPrinter_RateLimitsMovingNumbers(t *testing.T) {
	var out strings.Builder
	clock := time.Now()
	printer := clockedPrinter(&out, &clock)

	for rows := int64(1); rows <= 10; rows++ {
		printer.observe(api.StorageConvergenceProgress{
			State:   "copying",
			Percent: 40,
			Tables:  []api.StorageConvergenceTableProgress{{Table: "applies", State: "copying", RowsCopied: rows * 1000}},
		})
		clock = clock.Add(100 * time.Millisecond)
	}
	require.Equal(t, 1, strings.Count(out.String(), "applies copying"),
		"a second of row counts is one line")

	clock = clock.Add(storageProgressMinInterval)
	printer.observe(api.StorageConvergenceProgress{
		State:   "copying",
		Percent: 40,
		Tables:  []api.StorageConvergenceTableProgress{{Table: "applies", State: "copying", RowsCopied: 99000}},
	})
	assert.Equal(t, 2, strings.Count(out.String(), "applies copying"),
		"past the interval the numbers are worth a line again, so an operator can see it moving")
}

// A change of state is the transition an operator is waiting for — the copy
// finishing, the cutover starting — and there are only a handful in a whole
// run. It prints whatever the interval says.
func TestStorageProgressPrinter_AlwaysPrintsAStateChange(t *testing.T) {
	var out strings.Builder
	clock := time.Now()
	printer := clockedPrinter(&out, &clock)

	printer.observe(api.StorageConvergenceProgress{State: "copying", Percent: 99})
	clock = clock.Add(10 * time.Millisecond)
	printer.observe(api.StorageConvergenceProgress{State: "cutover", Percent: 99})

	assert.Contains(t, out.String(), "copying 99%")
	assert.Contains(t, out.String(), "cutover 99%",
		"a state change must not be swallowed by the rate limit")
}

// An observation renders what the dialect could say and leaves out what it
// could not. A zero printed as "0%" is a measurement the convergence never
// took, and on PostgreSQL — which converges a table per transaction and has no
// partial state — it would be on every line.
func TestStorageProgressLine_LeavesOutWhatWasNotMeasured(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		observation api.StorageConvergenceProgress
		want        string
	}{
		"state and percent": {
			observation: api.StorageConvergenceProgress{State: "copying", Percent: 40},
			want:        "  copying 40%",
		},
		"state without a measurable percent": {
			observation: api.StorageConvergenceProgress{
				State:  "running",
				Tables: []api.StorageConvergenceTableProgress{{Table: "checks", State: "running"}},
			},
			want: "  running · checks running",
		},
		"a table mid-copy": {
			observation: api.StorageConvergenceProgress{
				State:   "copying",
				Percent: 62,
				Tables: []api.StorageConvergenceTableProgress{
					{Table: "applies", State: "copying", Percent: 62, RowsCopied: 1203441},
				},
			},
			want: "  copying 62% · applies copying 62% (1,203,441 rows)",
		},
		"nothing to say": {
			observation: api.StorageConvergenceProgress{DDLCount: 3},
			want:        "",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, storageProgressLine(tc.observation))
		})
	}
}

// An observation carrying nothing prints nothing at all, rather than an empty
// indented line per poll.
func TestStorageProgressPrinter_SaysNothingAboutAnEmptyObservation(t *testing.T) {
	var out strings.Builder
	clock := time.Now()
	printer := clockedPrinter(&out, &clock)

	printer.observe(api.StorageConvergenceProgress{DDLCount: 3})

	assert.Empty(t, out.String())
}
