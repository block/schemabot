// Package statetest holds what a test needs to pin how a ranking or a rollup
// treats every state in package state: the list of a state struct's values, a
// check of a ranking against a table of expected ranks, and a check of a
// rollup against tiers of precedence.
//
// A test built on these fails when a state is added to package state, until
// the ranking or rollup it covers says where the new state goes.
package statetest

import (
	"reflect"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every lists the value of every field of a state constant struct such as
// state.Task.
func Every(t *testing.T, constants any) []string {
	t.Helper()
	var states []string
	for _, field := range reflect.ValueOf(constants).Fields() {
		require.NotEmpty(t, field.String())
		states = append(states, field.String())
	}
	require.NotEmpty(t, states)
	return states
}

// AssertRanks checks that want names every state exactly and that rank gives
// each state the value want pins for it.
func AssertRanks(t *testing.T, states []string, want map[string]int, rank func(string) int) {
	t.Helper()
	assert.Len(t, want, len(states), "the expected ranks must name every state, and only those")
	for _, s := range states {
		wantRank, ok := want[s]
		if !assert.Truef(t, ok, "no expected rank for state %q", s) {
			continue
		}
		assert.Equalf(t, wantRank, rank(s), "rank of state %q", s)
	}
}

// RollupTier is a group of states that a rollup prefers over every state in a
// later tier. Within a tier, the rollup prefers the state listed first.
type RollupTier struct {
	States []string
}

// AssertRollup checks that tiers name every state exactly once, and that for
// every ordered pair of states rollup returns the state the tiers prefer.
// rollup is given the states two targets report, in the order they report
// them.
func AssertRollup(t *testing.T, states []string, tiers []RollupTier, rollup func(first, second string) string) {
	t.Helper()
	var tiered []string
	for _, tier := range tiers {
		tiered = append(tiered, tier.States...)
	}
	require.ElementsMatch(t, states, tiered, "the rollup tiers must name every state exactly once")

	for _, first := range states {
		for _, second := range states {
			assert.Equalf(t, preferred(tiers, first, second), rollup(first, second),
				"targets report %q then %q", first, second)
		}
	}
}

// preferred is the state tiers prefer when two targets report first and then
// second. Both states must be in a tier.
func preferred(tiers []RollupTier, first, second string) string {
	tierOf := func(s string) (tier, position int) {
		for i, candidate := range tiers {
			if j := slices.Index(candidate.States, s); j >= 0 {
				return i, j
			}
		}
		return -1, -1
	}
	firstTier, firstPosition := tierOf(first)
	secondTier, secondPosition := tierOf(second)
	switch {
	case firstTier != secondTier:
		if firstTier < secondTier {
			return first
		}
		return second
	case firstPosition <= secondPosition:
		return first
	default:
		return second
	}
}
