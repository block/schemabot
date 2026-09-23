package tern

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
)

// credentialRecordingEngine captures the credentials each call was judged
// under, which is how the policy reaches an engine.
type credentialRecordingEngine struct {
	engine.Engine
	planCredentials  *engine.Credentials
	applyCredentials *engine.Credentials
}

func (e *credentialRecordingEngine) Plan(_ context.Context, req *engine.PlanRequest) (*engine.PlanResult, error) {
	e.planCredentials = req.Credentials
	return &engine.PlanResult{}, nil
}

func (e *credentialRecordingEngine) Apply(_ context.Context, req *engine.ApplyRequest) (*engine.ApplyResult, error) {
	e.applyCredentials = req.Credentials
	return &engine.ApplyResult{}, nil
}

func directExecutionTestClient(metadata map[string]string) *LocalClient {
	return &LocalClient{
		config: LocalConfig{
			Database:  "appdb",
			Type:      storage.DatabaseTypeMySQL,
			TargetDSN: "root@tcp(localhost:3306)/",
			Metadata:  metadata,
		},
		logger: slog.Default(),
	}
}

// A control plane states the policy its own configuration resolved, and the
// server that runs the statement judges the plan under that policy rather
// than under whatever its own configuration says — which for a target it
// resolved from an opaque identifier is nothing at all.
func TestPlanUsesTheCallersStatedDirectExecutionPolicy(t *testing.T) {
	eng := &credentialRecordingEngine{}
	client := directExecutionTestClient(nil)

	_, err := client.planNamespaceWithEngine(t.Context(), eng, &ternv1.PlanRequest{
		DirectExecution: &ternv1.DirectExecutionPolicy{
			Enabled:                       true,
			MaxTableRows:                  10000,
			LockAcquisitionTimeoutSeconds: 5,
		},
	}, "appdb", schema.SchemaFiles{}, client.credentials())
	require.NoError(t, err)

	require.NotNil(t, eng.planCredentials)
	assert.Equal(t, map[string]string{
		engine.MetadataDirectExecution:                              "true",
		engine.MetadataDirectExecutionMaxTableRows:                  "10000",
		engine.MetadataDirectExecutionLockAcquisitionTimeoutSeconds: "5",
	}, eng.planCredentials.Metadata)
}

// A caller's policy replaces the executing server's whole rather than merging
// into it, so an enabled flag can never pair with a row bound the caller did
// not send.
func TestPlanReplacesTheExecutingServersPolicyWhole(t *testing.T) {
	eng := &credentialRecordingEngine{}
	client := directExecutionTestClient(map[string]string{
		engine.MetadataDirectExecution:                              "true",
		engine.MetadataDirectExecutionMaxTableRows:                  "5000000",
		engine.MetadataDirectExecutionLockAcquisitionTimeoutSeconds: "30",
		"pending_drops": "true",
	})

	_, err := client.planNamespaceWithEngine(t.Context(), eng, &ternv1.PlanRequest{
		DirectExecution: &ternv1.DirectExecutionPolicy{Enabled: true, MaxTableRows: 10000},
	}, "appdb", schema.SchemaFiles{}, client.credentials())
	require.NoError(t, err)

	require.NotNil(t, eng.planCredentials)
	assert.Equal(t, map[string]string{
		engine.MetadataDirectExecution:             "true",
		engine.MetadataDirectExecutionMaxTableRows: "10000",
		"pending_drops":                            "true",
	}, eng.planCredentials.Metadata)
}

// A caller that states the policy disabled turns direct execution off for its
// request, even on a server whose own configuration enables it.
func TestPlanHonorsAStatedDisabledPolicy(t *testing.T) {
	eng := &credentialRecordingEngine{}
	client := directExecutionTestClient(map[string]string{
		engine.MetadataDirectExecution:             "true",
		engine.MetadataDirectExecutionMaxTableRows: "10000",
	})

	_, err := client.planNamespaceWithEngine(t.Context(), eng, &ternv1.PlanRequest{
		DirectExecution: &ternv1.DirectExecutionPolicy{Enabled: false},
	}, "appdb", schema.SchemaFiles{}, client.credentials())
	require.NoError(t, err)

	require.NotNil(t, eng.planCredentials)
	assert.Empty(t, eng.planCredentials.Metadata)
}

// A caller that states no policy leaves the executing server's own
// configuration in force, which is what keeps a control plane that predates
// forwarding working against a server configured directly.
func TestPlanKeepsTheServersOwnPolicyWhenTheCallerStatesNone(t *testing.T) {
	eng := &credentialRecordingEngine{}
	serverMetadata := map[string]string{
		engine.MetadataDirectExecution:             "true",
		engine.MetadataDirectExecutionMaxTableRows: "10000",
	}
	client := directExecutionTestClient(serverMetadata)

	_, err := client.planNamespaceWithEngine(t.Context(), eng, &ternv1.PlanRequest{}, "appdb", schema.SchemaFiles{}, client.credentials())
	require.NoError(t, err)

	require.NotNil(t, eng.planCredentials)
	assert.Equal(t, serverMetadata, eng.planCredentials.Metadata)
}

// The client's metadata is shared by every request the target serves, so a
// per request policy is written onto a copy and never into it.
func TestStatedPolicyDoesNotMutateTheClientsMetadata(t *testing.T) {
	eng := &credentialRecordingEngine{}
	serverMetadata := map[string]string{engine.MetadataDirectExecution: "true", engine.MetadataDirectExecutionMaxTableRows: "10000"}
	client := directExecutionTestClient(serverMetadata)

	_, err := client.planNamespaceWithEngine(t.Context(), eng, &ternv1.PlanRequest{
		DirectExecution: &ternv1.DirectExecutionPolicy{Enabled: true, MaxTableRows: 42},
	}, "appdb", schema.SchemaFiles{}, client.credentials())
	require.NoError(t, err)

	assert.Equal(t, map[string]string{
		engine.MetadataDirectExecution:             "true",
		engine.MetadataDirectExecutionMaxTableRows: "10000",
	}, client.config.Metadata)
}

// The drive routes the apply under the policy the dispatch was admitted with,
// read back from the apply's durable options rather than re-derived — the
// drive can be a later one, on another pod, after the server's configuration
// has changed.
func TestApplyUsesThePolicyRecordedOnTheApply(t *testing.T) {
	eng := &credentialRecordingEngine{}
	client := directExecutionTestClient(nil)

	admitted := storage.ApplyOptions{
		DirectExecution: &storage.DirectExecutionPolicy{
			Enabled:                       true,
			MaxTableRows:                  10000,
			LockAcquisitionTimeoutSeconds: 5,
		},
	}
	_, err := client.applyWithEngine(t.Context(), eng, &engine.ApplyRequest{
		Database:    "appdb",
		Options:     admitted.Map(),
		Credentials: client.credentials(),
	})
	require.NoError(t, err)

	require.NotNil(t, eng.applyCredentials)
	assert.Equal(t, map[string]string{
		engine.MetadataDirectExecution:                              "true",
		engine.MetadataDirectExecutionMaxTableRows:                  "10000",
		engine.MetadataDirectExecutionLockAcquisitionTimeoutSeconds: "5",
	}, eng.applyCredentials.Metadata)
}

// An apply admitted under a disabled policy stays blocked when it is driven,
// even on a server whose own configuration has since enabled the policy.
func TestApplyHonorsADisabledPolicyRecordedOnTheApply(t *testing.T) {
	eng := &credentialRecordingEngine{}
	client := directExecutionTestClient(map[string]string{
		engine.MetadataDirectExecution:             "true",
		engine.MetadataDirectExecutionMaxTableRows: "10000",
	})

	admitted := storage.ApplyOptions{DirectExecution: &storage.DirectExecutionPolicy{Enabled: false}}
	_, err := client.applyWithEngine(t.Context(), eng, &engine.ApplyRequest{
		Database:    "appdb",
		Options:     admitted.Map(),
		Credentials: client.credentials(),
	})
	require.NoError(t, err)

	require.NotNil(t, eng.applyCredentials)
	assert.Empty(t, eng.applyCredentials.Metadata)
}

// An apply that recorded no policy leaves the executing server's own
// configuration in force.
func TestApplyKeepsTheServersOwnPolicyWhenTheApplyRecordedNone(t *testing.T) {
	eng := &credentialRecordingEngine{}
	serverMetadata := map[string]string{
		engine.MetadataDirectExecution:             "true",
		engine.MetadataDirectExecutionMaxTableRows: "10000",
	}
	client := directExecutionTestClient(serverMetadata)

	_, err := client.applyWithEngine(t.Context(), eng, &engine.ApplyRequest{
		Database:    "appdb",
		Options:     storage.ApplyOptions{DeferCutover: true}.Map(),
		Credentials: client.credentials(),
	})
	require.NoError(t, err)

	require.NotNil(t, eng.applyCredentials)
	assert.Equal(t, serverMetadata, eng.applyCredentials.Metadata)
}

// The policy survives the gRPC hop in both directions unchanged, which is
// what lets a control plane's configuration decide a verdict reached on a
// data plane.
func TestDirectExecutionPolicyRoundTripsThroughTheWire(t *testing.T) {
	resolved := &storage.DirectExecutionPolicy{
		Enabled:                       true,
		MaxTableRows:                  10000,
		LockAcquisitionTimeoutSeconds: 5,
	}
	assert.Equal(t, resolved, DirectExecutionPolicyFromProto(DirectExecutionPolicyProto(resolved)))
	assert.Nil(t, DirectExecutionPolicyProto(nil))
	assert.Nil(t, DirectExecutionPolicyFromProto(nil))
}
