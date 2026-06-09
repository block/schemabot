package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOperatorWorkersConfig(t *testing.T) {
	t.Run("default workers", func(t *testing.T) {
		config := &ServerConfig{}
		assert.Equal(t, 0, config.OperatorWorkers)
		assert.Equal(t, 4, DefaultOperatorWorkers)
	})

	t.Run("configured workers", func(t *testing.T) {
		config := &ServerConfig{OperatorWorkers: 3}
		assert.Equal(t, 3, config.OperatorWorkers)
	})

	t.Run("deprecated scheduler_workers folds into operator_workers", func(t *testing.T) {
		config := &ServerConfig{SchedulerWorkers: 2}
		require.NoError(t, config.resolveDeprecatedOperatorWorkers())
		assert.Equal(t, 2, config.OperatorWorkers)
		assert.Equal(t, 0, config.SchedulerWorkers)
	})

	t.Run("setting both keys is rejected", func(t *testing.T) {
		config := &ServerConfig{OperatorWorkers: 4, SchedulerWorkers: 2}
		assert.Error(t, config.resolveDeprecatedOperatorWorkers())
	})
}
