package state

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEffectiveVitessState(t *testing.T) {
	tests := []struct {
		name            string
		status          string
		readyToComplete bool
		want            string
	}{
		{name: "complete flag true", status: Vitess.Complete, readyToComplete: true, want: Vitess.Complete},
		{name: "failed flag true", status: Vitess.Failed, readyToComplete: true, want: Vitess.Failed},
		{name: "cancelled flag true", status: Vitess.Cancelled, readyToComplete: true, want: Vitess.Cancelled},
		{name: "uppercase terminal flag true", status: "COMPLETE", readyToComplete: true, want: Vitess.Complete},
		{name: "uppercase running flag false", status: "RUNNING", readyToComplete: false, want: Vitess.Running},
		{name: "running flag true", status: Vitess.Running, readyToComplete: true, want: Vitess.ReadyToComplete},
		{name: "queued flag true", status: Vitess.Queued, readyToComplete: true, want: Vitess.ReadyToComplete},
		{name: "requested flag true", status: Vitess.Requested, readyToComplete: true, want: Vitess.ReadyToComplete},
		{name: "ready flag true", status: Vitess.Ready, readyToComplete: true, want: Vitess.ReadyToComplete},
		{name: "unknown flag true", status: "unknown", readyToComplete: true, want: Vitess.ReadyToComplete},
		{name: "running flag false", status: Vitess.Running, readyToComplete: false, want: Vitess.Running},
		{name: "unknown flag false", status: "unknown", readyToComplete: false, want: "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, EffectiveVitessState(tt.status, tt.readyToComplete))
		})
	}
}

func TestIsKnownVitessState(t *testing.T) {
	for _, s := range []string{
		Vitess.Requested, Vitess.Cancelled, Vitess.Queued, Vitess.Ready,
		Vitess.Running, Vitess.Complete, Vitess.Failed, Vitess.ReadyToComplete,
		"COMPLETE", "Running",
	} {
		assert.True(t, IsKnownVitessState(s), "status %q", s)
	}
	for _, s := range []string{"", "unknown", "vreplicating"} {
		assert.False(t, IsKnownVitessState(s), "status %q", s)
	}
}
