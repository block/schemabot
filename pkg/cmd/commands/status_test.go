package commands

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
)

func TestStatusCmdStateAndFailedAreMutuallyExclusive(t *testing.T) {
	cmd := &StatusCmd{State: "running", Failed: true}
	err := cmd.Run(&Globals{Endpoint: "http://localhost:1"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "--state cannot be combined with --failed")
}

// The status list prints each failed apply's error, which comes from the
// server, so escape sequences in it are removed before it is shown.
func TestActiveApplyDataDropsTerminalEscapesFromTheError(t *testing.T) {
	data := activeApplyDataFromResponse(&apitypes.ActiveApplyResponse{
		ApplyID:      "apply-abc",
		State:        "failed",
		ErrorMessage: "lock wait timeout\x1b[2J\rcompleted",
	})

	assert.Equal(t, "lock wait timeout[2Jcompleted", data.ErrorMessage)
}
