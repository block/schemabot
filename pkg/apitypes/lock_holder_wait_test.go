package apitypes

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseBuildWorkLockHolderWait(t *testing.T) {
	t.Run("no wait", func(t *testing.T) {
		work, err := ParseBuildWork(map[string]string{"step": "1"})
		require.NoError(t, err)
		assert.False(t, work.HolderWait.Waiting())
	})

	t.Run("a wait without an operation", func(t *testing.T) {
		work, err := ParseBuildWork(map[string]string{
			LockHolderPIDMetadataKey: "9433", LockHolderTableMetadataKey: "public.t",
			LockHolderWaitedMetadataKey: "7944", LockHolderLimitMetadataKey: "30000",
		})
		require.NoError(t, err)
		assert.Equal(t, BuildWork{HolderWait: LockHolderWait{
			PID: 9433, Table: "public.t", Waited: 7944 * time.Millisecond, Limit: 30 * time.Second,
		}}, work)
		assert.True(t, work.HolderWait.Waiting())
	})

	t.Run("a wait beside an operation", func(t *testing.T) {
		work, err := ParseBuildWork(map[string]string{"executor_operation": "native-statement", LockHolderPIDMetadataKey: "12"})
		require.NoError(t, err)
		assert.Equal(t, "native-statement", work.Operation)
		assert.Equal(t, LockHolderWait{PID: 12}, work.HolderWait)
	})

	for name, metadata := range map[string]map[string]string{
		"non-numeric pid":   {LockHolderPIDMetadataKey: "session"},
		"zero pid":          {LockHolderPIDMetadataKey: "0"},
		"negative waited":   {LockHolderPIDMetadataKey: "12", LockHolderWaitedMetadataKey: "-1"},
		"non-numeric limit": {LockHolderPIDMetadataKey: "12", LockHolderLimitMetadataKey: "soon"},
	} {
		t.Run(name, func(t *testing.T) {
			work, err := ParseBuildWork(metadata)
			require.Error(t, err)
			assert.Equal(t, BuildWork{}, work)
		})
	}

	// A malformed value costs only its own part, so one bad field does not
	// hide a build's progress or the session it waits on.
	t.Run("a malformed wait keeps the build's counters", func(t *testing.T) {
		work, err := ParseBuildWork(map[string]string{
			"executor_operation": "concurrent-index-build", "blocks_done": "2500", "blocks_total": "10000",
			LockHolderPIDMetadataKey: "session",
		})
		require.Error(t, err)
		assert.Equal(t, BuildWork{Operation: "concurrent-index-build", BlocksDone: 2500, BlocksTotal: 10000}, work)
	})

	t.Run("a malformed counter keeps the wait and the operation", func(t *testing.T) {
		work, err := ParseBuildWork(map[string]string{
			"executor_operation": "concurrent-index-build", "server_phase": "building index: scanning table", "blocks_done": "many",
			LockHolderPIDMetadataKey: "12",
		})
		require.Error(t, err)
		assert.Equal(t, BuildWork{Operation: "concurrent-index-build", ServerPhase: "building index: scanning table", HolderWait: LockHolderWait{PID: 12}}, work)
	})
}
