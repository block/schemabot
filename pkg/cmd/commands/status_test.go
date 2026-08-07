package commands

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStatusCmdStateAndFailedAreMutuallyExclusive(t *testing.T) {
	cmd := &StatusCmd{State: "running", Failed: true}
	err := cmd.Run(&Globals{Endpoint: "http://localhost:1"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "--state cannot be combined with --failed")
}
