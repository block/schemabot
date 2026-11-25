// Package indexphase is the one description of the phases PostgreSQL reports
// for a concurrent index build: the order they run in, the counter pair that
// measures each one, and the prose a renderer leads with.
//
// PostgreSQL scopes the block, tuple and locker counters of
// pg_stat_progress_create_index to a phase, but a completed phase's values
// persist into the phase that follows: the heap scan's finished block counters
// remain while live tuples are sorted, and the validation scan's remain while
// the build waits for old snapshots. A consumer that reads a counter without
// asking which phase owns it therefore shows finished work as current. Every
// renderer describing the work in flight reads this table instead of keeping
// a phase list of its own, so the phases stay in step with each other and with
// the server. The whole-build percent is pg-sprite's, not derived here.
//
// The package depends on nothing, so a renderer imports it without pulling in
// the executor.
package indexphase

import "strings"

// Counter names the pg_stat_progress_create_index counter pair that measures
// a phase's work.
type Counter int

const (
	// None marks a phase whose work PostgreSQL does not count: initializing,
	// or a sort.
	None Counter = iota
	// Blocks marks a phase that scans a relation block by block.
	Blocks
	// Tuples marks a phase that writes index tuples.
	Tuples
	// Lockers marks a phase that waits for sessions holding a conflicting
	// lock or an old snapshot.
	Lockers
	// BlocksOrTuples marks the build phase of an access method that reports
	// no sub-phases, where the phase alone cannot say which counter is live:
	// blocks are read when a block total is published and tuples otherwise.
	BlocksOrTuples
)

// Phase is one row of the table.
type Phase struct {
	// Name is the phase column of pg_stat_progress_create_index, verbatim.
	Name string
	// Label is the fixed prose a renderer leads the phase's work with —
	// "building index" or "validating index". Empty for a phase that does no
	// work of the index's own: initializing, or waiting on other sessions.
	Label string
	// Counter is the counter pair that measures the phase.
	Counter Counter
	// Detail is fixed prose for the work of a labelled phase that counts
	// nothing — a sort, or the access method's own initialization — so a
	// renderer can still say what the build is doing. Empty otherwise.
	Detail string
}

const (
	// BuildingIndex is the phase PostgreSQL reports while the access method
	// builds the index. An access method that reports progress appends its
	// sub-phase after a colon; the bare phase is what every other access
	// method reports, and the family fallback for a sub-phase this table does
	// not list.
	BuildingIndex = "building index"

	validatingIndexLabel = "validating index"
)

// Phases lists the phases of a concurrent index build in execution order,
// with the btree sub-phases of the build alongside the bare phase other
// access methods report.
var Phases = []Phase{
	{Name: "initializing", Counter: None},
	{Name: "waiting for writers before build", Counter: Lockers},
	{Name: "building index: initializing", Label: BuildingIndex, Counter: None, Detail: "initializing"},
	{Name: "building index: scanning table", Label: BuildingIndex, Counter: Blocks},
	{Name: "building index: sorting live tuples", Label: BuildingIndex, Counter: None, Detail: "sorting live tuples"},
	{Name: "building index: sorting dead tuples", Label: BuildingIndex, Counter: None, Detail: "sorting dead tuples"},
	{Name: "building index: loading tuples in tree", Label: BuildingIndex, Counter: Tuples},
	{Name: BuildingIndex, Label: BuildingIndex, Counter: BlocksOrTuples},
	{Name: "waiting for writers before validation", Counter: Lockers},
	{Name: "index validation: scanning index", Label: validatingIndexLabel, Counter: Blocks},
	{Name: "index validation: sorting tuples", Label: validatingIndexLabel, Counter: None, Detail: "sorting tuples"},
	{Name: "index validation: scanning table", Label: validatingIndexLabel, Counter: Blocks},
	{Name: "waiting for old snapshots", Counter: Lockers},
	{Name: "waiting for readers before marking dead", Counter: Lockers},
	{Name: "waiting for readers before dropping", Counter: Lockers},
}

// Lookup finds the phase PostgreSQL reported. The name is matched exactly,
// then by its "building index" family for an access method whose sub-phase
// the table does not list. A name the table does not know reports false.
func Lookup(serverPhase string) (Phase, bool) {
	if phase, ok := exact(serverPhase); ok {
		return phase, true
	}
	if strings.HasPrefix(serverPhase, BuildingIndex+": ") {
		return exact(BuildingIndex)
	}
	return Phase{}, false
}

func exact(serverPhase string) (Phase, bool) {
	for _, phase := range Phases {
		if phase.Name == serverPhase {
			return phase, true
		}
	}
	return Phase{}, false
}
