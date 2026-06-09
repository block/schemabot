package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSchedulerWorkersConfig(t *testing.T) {
	t.Run("default workers", func(t *testing.T) {
		config := &ServerConfig{}
		assert.Equal(t, 0, config.SchedulerWorkers)
		assert.Equal(t, 4, DefaultSchedulerWorkers)
	})

	t.Run("configured workers", func(t *testing.T) {
		config := &ServerConfig{SchedulerWorkers: 3}
		assert.Equal(t, 3, config.SchedulerWorkers)
	})
}
