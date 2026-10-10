package apitypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseProgressStep(t *testing.T) {
	t.Run("absent position is the zero value", func(t *testing.T) {
		step, err := ParseProgressStep(map[string]string{"percent": "40"})
		require.NoError(t, err)
		assert.Equal(t, ProgressStep{}, step)
	})

	t.Run("nil metadata is the zero value", func(t *testing.T) {
		step, err := ParseProgressStep(nil)
		require.NoError(t, err)
		assert.Equal(t, ProgressStep{}, step)
	})

	t.Run("valid position carries the statement", func(t *testing.T) {
		step, err := ParseProgressStep(map[string]string{
			ProgressStepMetadataKey:       "2",
			ProgressStepsTotalMetadataKey: "3",
			ProgressStatementMetadataKey:  "CREATE INDEX CONCURRENTLY idx_users_email ON users (email)",
		})
		require.NoError(t, err)
		assert.Equal(t, ProgressStep{
			Step:       2,
			StepsTotal: 3,
			Statement:  "CREATE INDEX CONCURRENTLY idx_users_email ON users (email)",
		}, step)
	})

	t.Run("last step equals total", func(t *testing.T) {
		step, err := ParseProgressStep(map[string]string{
			ProgressStepMetadataKey:       "3",
			ProgressStepsTotalMetadataKey: "3",
		})
		require.NoError(t, err)
		assert.Equal(t, ProgressStep{Step: 3, StepsTotal: 3}, step)
	})

	malformed := map[string]map[string]string{
		"step without total":   {ProgressStepMetadataKey: "1"},
		"total without step":   {ProgressStepsTotalMetadataKey: "4"},
		"non-integer step":     {ProgressStepMetadataKey: "two", ProgressStepsTotalMetadataKey: "3"},
		"non-integer total":    {ProgressStepMetadataKey: "1", ProgressStepsTotalMetadataKey: "3.0"},
		"zero step":            {ProgressStepMetadataKey: "0", ProgressStepsTotalMetadataKey: "3"},
		"negative total":       {ProgressStepMetadataKey: "1", ProgressStepsTotalMetadataKey: "-3"},
		"step exceeds total":   {ProgressStepMetadataKey: "4", ProgressStepsTotalMetadataKey: "3"},
		"whitespace in values": {ProgressStepMetadataKey: " 1", ProgressStepsTotalMetadataKey: "3"},
	}
	for name, metadata := range malformed {
		t.Run(name, func(t *testing.T) {
			step, err := ParseProgressStep(metadata)
			require.Error(t, err)
			assert.Equal(t, ProgressStep{}, step)
		})
	}
}
