package indexphase

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLookup(t *testing.T) {
	t.Run("a listed phase is matched exactly", func(t *testing.T) {
		phase, ok := Lookup("index validation: scanning index")
		require.True(t, ok)
		assert.Equal(t, Phase{Name: "index validation: scanning index", Label: "validating index", Counter: Blocks, Start: 60, End: 75}, phase)
	})
	t.Run("a listed build sub-phase is matched before the family fallback", func(t *testing.T) {
		phase, ok := Lookup("building index: sorting live tuples")
		require.True(t, ok)
		assert.Equal(t, None, phase.Counter)
		assert.Equal(t, 40, phase.Start)
	})
	t.Run("an unlisted build sub-phase falls back to the bare build phase", func(t *testing.T) {
		phase, ok := Lookup("building index: some other access method step")
		require.True(t, ok)
		assert.Equal(t, BuildingIndex, phase.Name)
		assert.Equal(t, BlocksOrTuples, phase.Counter)
	})
	t.Run("the family fallback needs the colon separator", func(t *testing.T) {
		_, ok := Lookup("building indexes")
		assert.False(t, ok)
	})
	t.Run("an unknown phase reports false", func(t *testing.T) {
		_, ok := Lookup("waiting for the moon")
		assert.False(t, ok)
	})
	t.Run("an empty phase reports false", func(t *testing.T) {
		_, ok := Lookup("")
		assert.False(t, ok)
	})
}

// TestPhasesAreWellFormed pins the properties the consumers rely on: names are
// unique, every band fits the scale and ends below 100, a phase that counts
// nothing or waits on lockers owns a zero-width band so a carried counter
// cannot move it, a phase with a label leads its work with fixed prose that
// names the index work rather than the wait, and a labelled phase that counts
// nothing carries the prose for what it does.
func TestPhasesAreWellFormed(t *testing.T) {
	seen := make(map[string]bool, len(Phases))
	for _, phase := range Phases {
		t.Run(phase.Name, func(t *testing.T) {
			assert.False(t, seen[phase.Name], "duplicate phase")
			seen[phase.Name] = true
			assert.LessOrEqual(t, phase.Start, phase.End)
			assert.GreaterOrEqual(t, phase.Start, 0)
			assert.Less(t, phase.End, 100)
			switch phase.Counter {
			case None, Lockers:
				assert.Equal(t, phase.Start, phase.End, "a phase without a live scale must not move")
			case Blocks, Tuples, BlocksOrTuples:
				assert.Less(t, phase.Start, phase.End, "a phase with a live counter must have room to move")
			}
			if phase.Counter == Lockers {
				assert.Empty(t, phase.Label, "a wait reports the lockers, not index work")
			} else if phase.Name != "initializing" {
				assert.NotEmpty(t, phase.Label)
			}
			if phase.Counter == None && phase.Label != "" {
				assert.NotEmpty(t, phase.Detail, "a labelled phase that counts nothing must still say what it does")
			} else {
				assert.Empty(t, phase.Detail, "detail is only for a phase without a live counter")
			}
		})
	}
}
