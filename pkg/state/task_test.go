package state

import (
	"testing"

	spiritstatus "github.com/block/spirit/pkg/status"
	"github.com/stretchr/testify/assert"
)

func TestNormalizeTaskStatus_SpiritStates(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{spiritstatus.Initial.String(), Task.Running},
		{spiritstatus.CopyRows.String(), Task.Running},
		{spiritstatus.ApplyChangeset.String(), Task.Running},
		{spiritstatus.RestoreSecondaryIndexes.String(), Task.Running},
		{spiritstatus.AnalyzeTable.String(), Task.Running},
		{spiritstatus.Checksum.String(), Task.Running},
		{spiritstatus.PostChecksum.String(), Task.Running},
		{spiritstatus.ErrCleanup.String(), Task.Running},
		{spiritstatus.WaitingOnSentinelTable.String(), Task.WaitingForCutover},
		{spiritstatus.CutOver.String(), Task.CuttingOver},
		{spiritstatus.Close.String(), Task.Completed},
	}

	for _, tc := range tests {
		assert.Equal(t, tc.expected, NormalizeTaskStatus(tc.input), "NormalizeTaskStatus(%q)", tc.input)
	}
}

func TestNormalizeTaskStatus_VitessStates(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"requested", Task.Pending},
		{"queued", Task.Pending},
		{"ready", Task.Pending},
		{"running", Task.Running},
		{"complete", Task.Completed},
		{"failed", Task.Failed},
		{"cancelled", Task.Cancelled},
		{"ready_to_complete", Task.WaitingForCutover},
	}

	for _, tc := range tests {
		assert.Equal(t, tc.expected, NormalizeTaskStatus(tc.input), "NormalizeTaskStatus(%q)", tc.input)
	}
}

func TestNormalizeTaskStatus_PassThrough(t *testing.T) {
	for _, s := range []string{
		Task.Pending, Task.Running, Task.Stopped, Task.Failed,
		Task.RevertWindow, Task.Reverted,
		Task.WaitingForDeploy, Task.WaitingForCutover, Task.CuttingOver, Task.Cancelled,
	} {
		assert.Equal(t, s, NormalizeTaskStatus(s), "NormalizeTaskStatus(%q)", s)
	}
}

func TestNormalizeTaskStatus_StatePrefix(t *testing.T) {
	assert.Equal(t, Task.Running, NormalizeTaskStatus("STATE_running"))
	assert.Equal(t, Task.Completed, NormalizeTaskStatus("state_complete"))
}

func TestNormalizeTaskStatus_StorageCompleted(t *testing.T) {
	assert.Equal(t, Task.Completed, NormalizeTaskStatus("completed"))
}

func TestNormalizeTaskStatus_UnknownDefaultsToRunning(t *testing.T) {
	assert.Equal(t, Task.Running, NormalizeTaskStatus("something_unknown"))
}
