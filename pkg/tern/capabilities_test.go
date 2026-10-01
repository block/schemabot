package tern

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

func TestPolicyStatesByteBound(t *testing.T) {
	for name, tc := range map[string]struct {
		policy *ternv1.DirectExecutionPolicy
		want   bool
	}{
		"no policy":               {policy: nil, want: false},
		"disabled with bytes":     {policy: &ternv1.DirectExecutionPolicy{Enabled: false, MaxTableBytes: 1 << 20}, want: false},
		"enabled with rows":       {policy: &ternv1.DirectExecutionPolicy{Enabled: true, MaxTableRows: 10000}, want: false},
		"enabled with bytes":      {policy: &ternv1.DirectExecutionPolicy{Enabled: true, MaxTableBytes: 1 << 20}, want: true},
		"enabled, unusable bytes": {policy: &ternv1.DirectExecutionPolicy{Enabled: true, MaxTableBytes: -1}, want: true},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, policyStatesByteBound(tc.policy))
		})
	}
}

func TestDataPlaneReadsByteBound(t *testing.T) {
	assert.False(t, dataPlaneReadsByteBound(nil))
	assert.False(t, dataPlaneReadsByteBound(&ternv1.HealthResponse{Status: "ok"}),
		"a data plane that predates capability reporting supports nothing")
	assert.False(t, dataPlaneReadsByteBound(&ternv1.HealthResponse{Capabilities: []string{"something.else"}}))
	assert.True(t, dataPlaneReadsByteBound(&ternv1.HealthResponse{Capabilities: []string{CapabilityDirectExecutionMaxTableBytes}}))
}

func TestPolicyGrantsWithoutKnownBound(t *testing.T) {
	assert.False(t, policyGrantsWithoutKnownBound(nil))
	assert.False(t, policyGrantsWithoutKnownBound(&ternv1.DirectExecutionPolicy{Enabled: false}))
	assert.False(t, policyGrantsWithoutKnownBound(&ternv1.DirectExecutionPolicy{Enabled: true, MaxTableRows: 10}))
	assert.False(t, policyGrantsWithoutKnownBound(&ternv1.DirectExecutionPolicy{Enabled: true, MaxTableBytes: 10}))
	assert.True(t, policyGrantsWithoutKnownBound(&ternv1.DirectExecutionPolicy{Enabled: true, LockAcquisitionTimeoutSeconds: 5}))
}

func TestRenderableVersion(t *testing.T) {
	assert.Equal(t, "v0.1.75", renderableVersion("v0.1.75"))
	assert.Equal(t, "v0.0.0-20261001120000-abcdef123456+dirty", renderableVersion("v0.0.0-20261001120000-abcdef123456+dirty"))
	assert.Empty(t, renderableVersion(""))
	assert.Empty(t, renderableVersion("v1 | **bold**"), "markdown never reaches a PR from the wire")
	assert.Empty(t, renderableVersion("v1\nnext line"))
	assert.Empty(t, renderableVersion("v"+string(make([]byte, maxRenderedVersionLength))))
}

func TestUnsupportedDirectExecutionErrorMessage(t *testing.T) {
	assert.Equal(t,
		`direct_execution max_table_bytes is configured, but the data plane for deployment "west" (a SchemaBot release that predates capability reporting) does not advertise support for it, `+
			`and a data plane that cannot read it refuses every plan as having no size bound. `+
			`Upgrade the data plane before using max_table_bytes, or bound the policy with max_table_rows until then`,
		(&UnsupportedDirectExecutionError{Deployment: "west"}).Error())
	assert.Contains(t, (&UnsupportedDirectExecutionError{Deployment: "west", Version: "v0.2.0"}).Error(),
		`the data plane for deployment "west" (SchemaBot v0.2.0) does not advertise support for it`)
}

// This release's data plane advertises the byte bound and its version, which
// is what a control plane reads before stating one.
func TestServerHealthAdvertisesCapabilitiesAndVersion(t *testing.T) {
	server := NewServer(healthErrorClient{}, nil, WithVersion("v0.1.75"))

	resp, err := server.Health(t.Context(), &ternv1.HealthRequest{})

	require.NoError(t, err)
	assert.Equal(t, "ok", resp.GetStatus())
	assert.Equal(t, "v0.1.75", resp.GetVersion())
	assert.Equal(t, []string{CapabilityDirectExecutionMaxTableBytes}, resp.GetCapabilities())
}

// statedPolicyServer records what reaches the client behind the server, so a
// test can tell a request refused at the boundary from one the engine saw.
type statedPolicyServer struct {
	Client
	planCalls  int
	applyCalls int
}

func (c *statedPolicyServer) Plan(context.Context, *ternv1.PlanRequest) (*ternv1.PlanResponse, error) {
	c.planCalls++
	return &ternv1.PlanResponse{PlanId: "plan-1"}, nil
}

func (c *statedPolicyServer) PlanDiff(context.Context, *ternv1.PlanRequest) (*ternv1.PlanDiffResponse, error) {
	c.planCalls++
	return &ternv1.PlanDiffResponse{}, nil
}

func (c *statedPolicyServer) Apply(context.Context, *ternv1.ApplyRequest) (*ternv1.ApplyResponse, error) {
	c.applyCalls++
	return &ternv1.ApplyResponse{Accepted: true}, nil
}

// A control plane newer than this data plane can state a bound in a field
// this data plane does not know; the field is dropped on the way in and the
// policy arrives enabled with no bound. The data plane refuses it at the
// boundary with an error that names the likely skew, rather than letting the
// engine refuse it under a message about bound keys nobody configured.
func TestServerRefusesAStatedPolicyWithNoBoundItReads(t *testing.T) {
	unbounded := &ternv1.DirectExecutionPolicy{Enabled: true, LockAcquisitionTimeoutSeconds: 5}
	client := &statedPolicyServer{}
	server, logs := newHealthTestServer(client)

	_, planErr := server.Plan(t.Context(), &ternv1.PlanRequest{Database: "payments", Environment: "staging", DirectExecution: unbounded})
	_, diffErr := server.PlanDiff(t.Context(), &ternv1.PlanRequest{Database: "payments", Environment: "staging", DirectExecution: unbounded})
	_, applyErr := server.Apply(t.Context(), &ternv1.ApplyRequest{Database: "payments", Environment: "staging", DirectExecution: unbounded})

	for name, err := range map[string]error{"Plan": planErr, "PlanDiff": diffErr, "Apply": applyErr} {
		require.Error(t, err, name)
		assert.Equal(t, codes.InvalidArgument, status.Code(err), name)
		assert.Contains(t, err.Error(), "this likely means a newer control plane sent a bound this data plane does not support", name)
	}
	assert.Zero(t, client.planCalls, "a refused plan never reaches the engine")
	assert.Zero(t, client.applyCalls, "a refused apply is never admitted")
	assert.Contains(t, logs.String(), "database=payments")
	assert.Contains(t, logs.String(), "environment=staging")

	// A bounded or disabled policy passes the boundary untouched.
	_, err := server.Plan(t.Context(), &ternv1.PlanRequest{DirectExecution: &ternv1.DirectExecutionPolicy{Enabled: true, MaxTableBytes: 1 << 20}})
	require.NoError(t, err)
	_, err = server.Plan(t.Context(), &ternv1.PlanRequest{DirectExecution: &ternv1.DirectExecutionPolicy{Enabled: false}})
	require.NoError(t, err)
	assert.Equal(t, 2, client.planCalls)
}

// capabilityTernServer answers Health with a fixed response or error and
// records whether the RPCs that carry a policy reached it.
type capabilityTernServer struct {
	ternv1.UnimplementedTernServer
	health     *ternv1.HealthResponse
	healthErr  error
	applyCalls int
}

func (s *capabilityTernServer) Health(context.Context, *ternv1.HealthRequest) (*ternv1.HealthResponse, error) {
	return s.health, s.healthErr
}

func (s *capabilityTernServer) Apply(context.Context, *ternv1.ApplyRequest) (*ternv1.ApplyResponse, error) {
	s.applyCalls++
	return &ternv1.ApplyResponse{Accepted: true, ApplyId: "remote-1"}, nil
}

func newCapabilityTestClient(t *testing.T, server *capabilityTernServer) *GRPCClient {
	t.Helper()
	client := newRetryTestClient(t, server)
	client.deployment = "west"
	return client
}

// The Apply RPC carries the same policy a plan does, so it is gated the same
// way: a data plane that does not advertise the byte bound never receives a
// request stating one.
func TestGRPCClientApplyRefusesAByteBoundTheDataPlaneCannotRead(t *testing.T) {
	server := &capabilityTernServer{health: &ternv1.HealthResponse{Status: "ok"}}
	client := newCapabilityTestClient(t, server)

	_, err := client.Apply(t.Context(), &ternv1.ApplyRequest{
		PlanId:          "plan-1",
		Database:        "payments",
		DirectExecution: &ternv1.DirectExecutionPolicy{Enabled: true, MaxTableBytes: 1 << 20},
	})

	var refusal *UnsupportedDirectExecutionError
	require.ErrorAs(t, err, &refusal)
	assert.Equal(t, "west", refusal.Deployment)
	assert.Zero(t, server.applyCalls)
}

// A data plane that reports its capabilities is believed only for what it
// lists, and its self-reported version is named in the refusal.
func TestGRPCClientNamesTheDataPlaneVersionWhenItReportsOne(t *testing.T) {
	server := &capabilityTernServer{health: &ternv1.HealthResponse{Status: "ok", Version: "v0.2.0", Capabilities: []string{"some.other.feature"}}}
	client := newCapabilityTestClient(t, server)

	_, err := client.Plan(t.Context(), &ternv1.PlanRequest{
		Database:        "payments",
		DirectExecution: &ternv1.DirectExecutionPolicy{Enabled: true, MaxTableBytes: 1 << 20},
	})

	var refusal *UnsupportedDirectExecutionError
	require.ErrorAs(t, err, &refusal)
	assert.Equal(t, "v0.2.0", refusal.Version)
	assert.Contains(t, err.Error(), `(SchemaBot v0.2.0) does not advertise support for it`)
}

// A capability read that fails is not a verdict either way. The request is
// not sent, and the error keeps the Health call's gRPC status, so the control
// plane still reports an unreachable data plane as unreachable.
func TestGRPCClientDoesNotSendAByteBoundWhenCapabilitiesCannotBeRead(t *testing.T) {
	server := &capabilityTernServer{healthErr: status.Error(codes.Unavailable, "service unavailable")}
	client := newCapabilityTestClient(t, server)

	_, err := client.Apply(t.Context(), &ternv1.ApplyRequest{
		PlanId:          "plan-1",
		DirectExecution: &ternv1.DirectExecutionPolicy{Enabled: true, MaxTableBytes: 1 << 20},
	})

	require.Error(t, err)
	var refusal *UnsupportedDirectExecutionError
	assert.False(t, errors.As(err, &refusal), "an unreadable capability list is not a refusal")
	assert.Equal(t, codes.Unavailable, status.Code(err))
	assert.Contains(t, err.Error(), `read capabilities of the data plane for deployment "west"`)
	assert.Zero(t, server.applyCalls)
}

// A policy that does not depend on the byte bound costs no capability read,
// so a data plane whose Health is failing still receives it.
func TestGRPCClientSendsAPolicyWithoutAByteBoundWithoutReadingCapabilities(t *testing.T) {
	server := &capabilityTernServer{healthErr: status.Error(codes.Unavailable, "service unavailable")}
	client := newCapabilityTestClient(t, server)

	_, err := client.Apply(t.Context(), &ternv1.ApplyRequest{
		PlanId:          "plan-1",
		DirectExecution: &ternv1.DirectExecutionPolicy{Enabled: true, MaxTableRows: 10000},
	})

	require.NoError(t, err)
	assert.Equal(t, 1, server.applyCalls)
}

// An apply admitted under a byte-bound policy whose data plane was rolled
// back to a release that cannot read it fails at dispatch with the refusal,
// rather than reaching a data plane that would refuse it under a misleading
// error.
func TestGRPCClientDispatchFailsAnApplyWhoseDataPlaneCannotReadItsPolicy(t *testing.T) {
	server := &capturingTernServer{remoteApplyID: "remote-should-not-exist"}
	client, cleanup := testCapturingGRPCClient(t, server)
	defer cleanup()
	client.deployment = "west"

	apply := &storage.Apply{
		ID:              7,
		ApplyIdentifier: "apply-byte-bound",
		PlanID:          99,
		Database:        "payments",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Environment:     "staging",
		State:           state.Apply.Pending,
	}
	apply.SetOptions(storage.ApplyOptions{
		Target:          "payments-target",
		DirectExecution: &storage.DirectExecutionPolicy{Enabled: true, MaxTableBytes: 1 << 20},
	})
	task := &storage.Task{
		ID:             11,
		TaskIdentifier: "task-users",
		ApplyID:        apply.ID,
		TableName:      "users",
		DDL:            "ALTER TABLE users ADD COLUMN email varchar(255)",
		DDLAction:      "alter",
		Namespace:      "default",
		State:          state.Task.Pending,
	}
	applyStore := &mockApplyStore{apply: apply}
	client.storage = &mockStorage{
		applies: applyStore,
		tasks:   &mockTaskStore{tasks: []*storage.Task{task}},
		logs:    &mockApplyLogStore{},
		plans:   &mockPlanStore{plan: &storage.Plan{ID: apply.PlanID, PlanIdentifier: "plan-byte-bound"}},
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	err := client.ResumeApply(ctx, apply)

	var refusal *UnsupportedDirectExecutionError
	require.ErrorAs(t, err, &refusal)
	assert.Nil(t, server.getApplyRequest(), "the apply must not be dispatched to a data plane that would misread its policy")
	assert.Equal(t, state.Apply.Failed, applyStore.apply.State)
	assert.Contains(t, applyStore.apply.ErrorMessage, `the data plane for deployment "west"`)
	assert.Equal(t, state.Task.Failed, task.State)
}
