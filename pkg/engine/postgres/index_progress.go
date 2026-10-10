package postgres

import (
	"math"

	"github.com/block/pg-sprite/pkg/progress"

	"github.com/block/schemabot/pkg/engine/postgres/indexphase"
)

// concurrentIndexPercent derives a whole-build percent from the phase and
// counters PostgreSQL publishes for a concurrent index build. Each phase owns
// a band of the whole in indexphase.Phases; the fraction within the band comes
// from the counter pair that phase publishes, so a completed scan's counters
// carried into a sort or a wait cannot move it off its band's start. An
// over-reported counter is clamped so the value never leaves the band. A
// phase the table does not know reports false so the caller keeps the percent
// it already has.
func concurrentIndexPercent(serverPhase string, work progress.Work) (int, bool) {
	phase, ok := indexphase.Lookup(serverPhase)
	if !ok {
		return 0, false
	}
	fraction := 0.0
	switch phase.Counter {
	case indexphase.Blocks:
		fraction = ratio(work.BlocksDone, work.BlocksTotal)
	case indexphase.Tuples:
		fraction = ratio(work.TuplesDone, work.TuplesTotal)
	case indexphase.BlocksOrTuples:
		if work.BlocksTotal > 0 {
			fraction = ratio(work.BlocksDone, work.BlocksTotal)
		} else {
			fraction = ratio(work.TuplesDone, work.TuplesTotal)
		}
	case indexphase.None, indexphase.Lockers:
	}
	fraction = math.Min(math.Max(fraction, 0), 1)
	return phase.Start + int(math.Round(fraction*float64(phase.End-phase.Start))), true
}

// ratio is done over total, or zero while the server has published no total.
func ratio(done, total uint64) float64 {
	if total == 0 {
		return 0
	}
	return float64(done) / float64(total)
}
