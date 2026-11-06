package livee2e

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Seed's row count is the table's final size, so rows already in the table,
// such as from an earlier attempt, count toward it.
func TestSeedRowsNeeded(t *testing.T) {
	needed, err := seedRowsNeeded(0, 5)
	require.NoError(t, err)
	assert.Equal(t, int64(5), needed)

	needed, err = seedRowsNeeded(3, 5)
	require.NoError(t, err)
	assert.Equal(t, int64(2), needed)

	needed, err = seedRowsNeeded(5, 5)
	require.NoError(t, err)
	assert.Equal(t, int64(0), needed)

	_, err = seedRowsNeeded(8, 5)
	require.ErrorContains(t, err, "more than the 5 requested")
}
