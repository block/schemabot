package postgres

import (
	"math"
	"strings"

	"github.com/block/pg-sprite/pkg/progress"
)

// concurrentIndexPhaseBand places one phase of a concurrent index build on
// the whole build's percent scale. PostgreSQL scopes the block and tuple
// counters it publishes to the current phase and resets them at every phase
// boundary, so a counter ratio on its own would run to 100 during the first
// heap scan and fall back to 0 when validation begins. Each phase instead
// owns a fixed band of the whole, in the order PostgreSQL documents for
// CREATE INDEX CONCURRENTLY; a phase with counters interpolates within its
// band and a phase without them reports the band's start.
type concurrentIndexPhaseBand struct {
	phase      string
	start, end int
}

// concurrentIndexPhaseBands lists the phases of a concurrent index build in
// execution order. The btree access method reports its build as three
// sub-phases, so those appear alongside the bare phase that other access
// methods report. The band widths are a display heuristic: the two heap scans
// and the index scan dominate a build's wall time, and every band ends below
// 100 so a running build never reads as finished. pg-sprite reporting a
// whole-build percent of its own would retire this table.
var concurrentIndexPhaseBands = []concurrentIndexPhaseBand{
	{phase: "initializing", start: 0, end: 0},
	{phase: "waiting for writers before build", start: 0, end: 0},
	{phase: "building index: scanning table", start: 0, end: 40},
	{phase: "building index: sorting live tuples", start: 40, end: 40},
	{phase: "building index: loading tuples in tree", start: 40, end: 60},
	{phase: buildingIndexPhase, start: 0, end: 60},
	{phase: "waiting for writers before validation", start: 60, end: 60},
	{phase: "index validation: scanning index", start: 60, end: 75},
	{phase: "index validation: sorting tuples", start: 75, end: 75},
	{phase: "index validation: scanning table", start: 75, end: 95},
	{phase: "waiting for old snapshots", start: 95, end: 95},
}

// concurrentIndexPercent derives a whole-build percent from the phase and
// counters PostgreSQL publishes for a concurrent index build. The phase is
// matched exactly, then by its "building index" family for an access method
// whose sub-phase this table does not know. Within a band the fraction comes
// from the block counters when the phase publishes a block total and from
// the tuple counters otherwise; an over-reported counter is clamped so the
// value never leaves the band. A phase this table does not know reports
// false so the caller keeps the percent it already has.
func concurrentIndexPercent(serverPhase string, work progress.Work) (int, bool) {
	band, ok := lookupConcurrentIndexPhaseBand(serverPhase)
	if !ok {
		return 0, false
	}
	fraction := 0.0
	switch {
	case work.BlocksTotal > 0:
		fraction = float64(work.BlocksDone) / float64(work.BlocksTotal)
	case work.TuplesTotal > 0:
		fraction = float64(work.TuplesDone) / float64(work.TuplesTotal)
	}
	fraction = math.Min(math.Max(fraction, 0), 1)
	return band.start + int(math.Round(fraction*float64(band.end-band.start))), true
}

// buildingIndexPhase is the phase PostgreSQL reports while the access method
// builds the index. An access method that reports progress appends its
// sub-phase after a colon, so the bare phase also serves as the family
// fallback for a sub-phase the band table does not list.
const buildingIndexPhase = "building index"

func lookupConcurrentIndexPhaseBand(serverPhase string) (concurrentIndexPhaseBand, bool) {
	if band, ok := exactConcurrentIndexPhaseBand(serverPhase); ok {
		return band, true
	}
	if strings.HasPrefix(serverPhase, buildingIndexPhase+": ") {
		return exactConcurrentIndexPhaseBand(buildingIndexPhase)
	}
	return concurrentIndexPhaseBand{}, false
}

func exactConcurrentIndexPhaseBand(serverPhase string) (concurrentIndexPhaseBand, bool) {
	for _, band := range concurrentIndexPhaseBands {
		if band.phase == serverPhase {
			return band, true
		}
	}
	return concurrentIndexPhaseBand{}, false
}
