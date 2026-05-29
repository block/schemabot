package planetscale

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
)

func TestStart_RejectsNonDeferredDeploy(t *testing.T) {
	e := &Engine{}

	// Non-deferred metadata — Start should return "not supported"
	meta, err := encodePSMetadata(&psMetadata{
		BranchName:      "schemabot-mydb-abc",
		DeployRequestID: 1,
		DeferredDeploy:  false,
	})
	require.NoError(t, err)

	_, err = e.Start(t.Context(), &engine.ControlRequest{
		ResumeState: &engine.ResumeState{Metadata: meta},
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")
}

func TestStart_AcceptsDeferredDeploy(t *testing.T) {
	// Start with DeferredDeploy=true should attempt to deploy
	// (will fail because no PS client, but validates dispatch logic)
	e := &Engine{}

	meta, err := encodePSMetadata(&psMetadata{
		BranchName:      "schemabot-mydb-abc",
		DeployRequestID: 1,
		IsInstant:       true,
		DeferredDeploy:  true,
	})
	require.NoError(t, err)

	_, err = e.Start(t.Context(), &engine.ControlRequest{
		ResumeState: &engine.ResumeState{Metadata: meta},
	})
	// Fails because no PS client configured — but proves it didn't reject as "not supported"
	assert.Error(t, err)
	assert.NotContains(t, err.Error(), "not supported")
}
