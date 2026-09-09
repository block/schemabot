package apitypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseBuildWork(t *testing.T) {
	t.Run("absent operation is the zero value", func(t *testing.T) {
		work, err := ParseBuildWork(map[string]string{"blocks_done": "4"})
		require.NoError(t, err)
		assert.Equal(t, BuildWork{}, work)
	})

	t.Run("valid work", func(t *testing.T) {
		work, err := ParseBuildWork(map[string]string{
			"executor_operation": "concurrent-index-build", "server_phase": "building index: scanning table",
			"attempt": "2", "blocks_done": "40", "blocks_total": "100",
			"tuples_done": "300", "tuples_total": "500", "lockers_done": "1", "lockers_total": "3",
		})
		require.NoError(t, err)
		assert.Equal(t, BuildWork{
			Operation: "concurrent-index-build", ServerPhase: "building index: scanning table", Attempt: 2,
			BlocksDone: 40, BlocksTotal: 100, TuplesDone: 300, TuplesTotal: 500, LockersDone: 1, LockersTotal: 3,
		}, work)
		assert.True(t, work.IsConcurrentIndexBuild())
	})

	t.Run("malformed blocks done", func(t *testing.T) {
		work, err := ParseBuildWork(map[string]string{"executor_operation": "concurrent-index-build", "blocks_done": "many"})
		require.Error(t, err)
		assert.Equal(t, BuildWork{}, work)
	})

	t.Run("present empty counter", func(t *testing.T) {
		work, err := ParseBuildWork(map[string]string{"executor_operation": "concurrent-index-build", "blocks_done": "", "blocks_total": "10"})
		require.ErrorContains(t, err, `blocks_done "" is not an integer`)
		assert.Equal(t, BuildWork{}, work)
	})

	t.Run("negative counter", func(t *testing.T) {
		work, err := ParseBuildWork(map[string]string{"executor_operation": "concurrent-index-build", "lockers_done": "-1"})
		require.ErrorContains(t, err, "lockers_done -1 is negative")
		assert.Equal(t, BuildWork{}, work)
	})

	t.Run("other operation keeps its counters", func(t *testing.T) {
		work, err := ParseBuildWork(map[string]string{"executor_operation": "alter-table", "attempt": "3"})
		require.NoError(t, err)
		assert.Equal(t, BuildWork{Operation: "alter-table", Attempt: 3}, work)
		assert.False(t, work.IsConcurrentIndexBuild())
	})
}

func TestBuildWorkWaitingOnLockers(t *testing.T) {
	for _, phase := range []string{
		"waiting for writers before build", "waiting for writers before validation",
		"waiting for old snapshots", "waiting for readers before marking dead",
	} {
		t.Run(phase, func(t *testing.T) {
			assert.True(t, BuildWork{ServerPhase: phase}.WaitingOnLockers())
		})
	}
	assert.False(t, BuildWork{ServerPhase: "building index: scanning table"}.WaitingOnLockers())
}
