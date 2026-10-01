package tern

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

const byteBoundCapability = "direct_execution.max_table_bytes"

var byteBoundPolicy = &ternv1.DirectExecutionPolicy{Enabled: true, MaxTableBytes: 1 << 20}

// A server advertises exactly the capabilities its compiled schema declares, so
// annotating a field in tern.proto is the whole registration.
func TestServerCapabilitiesAreTheSchemaAnnotations(t *testing.T) {
	assert.Equal(t, []string{byteBoundCapability}, serverCapabilities())
}

func TestServerHealthAdvertisesCapabilitiesAndVersion(t *testing.T) {
	server := NewServer(healthErrorClient{}, nil, WithVersion("v0.1.75"))

	resp, err := server.Health(t.Context(), &ternv1.HealthRequest{})

	require.NoError(t, err)
	assert.Equal(t, "ok", resp.GetStatus())
	assert.Equal(t, "v0.1.75", resp.GetVersion())
	assert.Equal(t, []string{byteBoundCapability}, resp.GetCapabilities())
}

func TestRequiredCapabilities(t *testing.T) {
	for name, tc := range map[string]struct {
		req  proto.Message
		want []string
	}{
		"plan with no policy":     {req: &ternv1.PlanRequest{Database: "payments"}, want: nil},
		"plan with a row bound":   {req: &ternv1.PlanRequest{DirectExecution: &ternv1.DirectExecutionPolicy{Enabled: true, MaxTableRows: 10000}}, want: nil},
		"plan with a byte bound":  {req: &ternv1.PlanRequest{DirectExecution: byteBoundPolicy}, want: []string{byteBoundCapability}},
		"apply with a byte bound": {req: &ternv1.ApplyRequest{PlanId: "plan-1", DirectExecution: byteBoundPolicy}, want: []string{byteBoundCapability}},
		"unusable negative bound": {req: &ternv1.PlanRequest{DirectExecution: &ternv1.DirectExecutionPolicy{Enabled: true, MaxTableBytes: -1}}, want: []string{byteBoundCapability}},
		"rpc without the field":   {req: &ternv1.ProgressRequest{ApplyId: "apply-1"}, want: nil},
		"disabled policy as sent": {
			req:  &ternv1.PlanRequest{DirectExecution: DirectExecutionPolicyProto(&storage.DirectExecutionPolicy{Enabled: false, MaxTableBytes: 1 << 20})},
			want: nil,
		},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, requiredCapabilities(tc.req))
		})
	}
}

func TestMissingCapabilities(t *testing.T) {
	required := []string{"a.one", "b.two"}
	assert.Equal(t, required, missingCapabilities(required, nil), "a server that advertises nothing supports nothing")
	assert.Equal(t, []string{"b.two"}, missingCapabilities(required, []string{"a.one", "c.three"}))
	assert.Empty(t, missingCapabilities(required, []string{"b.two", "a.one"}))
}

func TestRenderableVersion(t *testing.T) {
	assert.Equal(t, "v0.1.75", renderableVersion("v0.1.75"))
	assert.Equal(t, "v0.0.0-20261001120000-abcdef123456+dirty", renderableVersion("v0.0.0-20261001120000-abcdef123456+dirty"))
	assert.Empty(t, renderableVersion(""))
	assert.Empty(t, renderableVersion("v1 | **bold**"), "markdown never reaches a PR from the wire")
	assert.Empty(t, renderableVersion("v1\nnext line"))
	assert.Empty(t, renderableVersion("v"+strings.Repeat("1", maxRenderedVersionLength)))
}

func TestMissingCapabilityErrorMessage(t *testing.T) {
	assert.Equal(t,
		`the data plane for deployment "west" (a SchemaBot release that predates capability reporting) does not advertise direct_execution.max_table_bytes, `+
			`which this request depends on, so the request was not sent: a data plane without it would misread the request. `+
			`Upgrade the data plane before using direct_execution.max_table_bytes`,
		(&MissingCapabilityError{Deployment: "west", Missing: []string{byteBoundCapability}}).Error())
	assert.Contains(t, (&MissingCapabilityError{Deployment: "west", Version: "v0.2.0", Missing: []string{"a.one", "b.two"}}).Error(),
		`the data plane for deployment "west" (SchemaBot v0.2.0) does not advertise a.one, b.two, which this request depends on`)
	assert.True(t, strings.HasPrefix((&MissingCapabilityError{Missing: []string{"a.one"}}).Error(), "the data plane (a SchemaBot release"))
}

// capabilityTernServer answers Health with a fixed response or error and
// counts what reaches it.
type capabilityTernServer struct {
	ternv1.UnimplementedTernServer
	health        *ternv1.HealthResponse
	healthErr     error
	healthCalls   atomic.Int32
	planCalls     atomic.Int32
	applyCalls    atomic.Int32
	progressCalls atomic.Int32
}

func (s *capabilityTernServer) Health(context.Context, *ternv1.HealthRequest) (*ternv1.HealthResponse, error) {
	s.healthCalls.Add(1)
	return s.health, s.healthErr
}

func (s *capabilityTernServer) Plan(context.Context, *ternv1.PlanRequest) (*ternv1.PlanResponse, error) {
	s.planCalls.Add(1)
	return &ternv1.PlanResponse{PlanId: "plan-1"}, nil
}

func (s *capabilityTernServer) PlanDiff(context.Context, *ternv1.PlanRequest) (*ternv1.PlanDiffResponse, error) {
	s.planCalls.Add(1)
	return &ternv1.PlanDiffResponse{}, nil
}

func (s *capabilityTernServer) Apply(context.Context, *ternv1.ApplyRequest) (*ternv1.ApplyResponse, error) {
	s.applyCalls.Add(1)
	return &ternv1.ApplyResponse{Accepted: true, ApplyId: "remote-1"}, nil
}

func (s *capabilityTernServer) Progress(context.Context, *ternv1.ProgressRequest) (*ternv1.ProgressResponse, error) {
	s.progressCalls.Add(1)
	return &ternv1.ProgressResponse{State: ternv1.State_STATE_COMPLETED}, nil
}

// newCapabilityTestClient connects to server through the production
// constructor, which is what installs the capability gate, under the
// deployment name "west".
func newCapabilityTestClient(t *testing.T, server ternv1.TernServer, store storage.Storage) *GRPCClient {
	t.Helper()
	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "localhost:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	ternv1.RegisterTernServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(lis) }()

	client, err := NewGRPCClient(Config{Address: lis.Addr().String(), Deployment: "west", Storage: store})
	require.NoError(t, err)
	t.Cleanup(func() {
		utils.CloseAndLog(client)
		grpcServer.Stop()
	})
	return client
}

// A data plane that predates capability reporting answers Health with a status
// and nothing else. Every RPC that sets an annotated field is refused before it
// is sent, whichever call site sends it.
func TestCapabilityGateRefusesEveryRPCThatSetsAnUnadvertisedField(t *testing.T) {
	server := &capabilityTernServer{health: &ternv1.HealthResponse{Status: "ok"}}
	client := newCapabilityTestClient(t, server, nil)

	_, planErr := client.Plan(t.Context(), &ternv1.PlanRequest{Database: "payments", DirectExecution: byteBoundPolicy})
	_, diffErr := client.PlanDiff(t.Context(), &ternv1.PlanRequest{Database: "payments", DirectExecution: byteBoundPolicy})
	_, applyErr := client.Apply(t.Context(), &ternv1.ApplyRequest{PlanId: "plan-1", DirectExecution: byteBoundPolicy})

	for name, err := range map[string]error{"Plan": planErr, "PlanDiff": diffErr, "Apply": applyErr} {
		var refusal *MissingCapabilityError
		require.ErrorAs(t, err, &refusal, name)
		assert.Equal(t, "west", refusal.Deployment, name)
		assert.Empty(t, refusal.Version, name)
		assert.Equal(t, []string{byteBoundCapability}, refusal.Missing, name)
		assert.Equal(t, codes.FailedPrecondition, status.Code(err), name)
		assert.False(t, isRetryableRemoteApplyError(err), "%s: resending fails the same way until the data plane is upgraded", name)
		assert.False(t, isAmbiguousRemoteCallError(err), "%s: a refused request was definitely not sent", name)
	}
	assert.Zero(t, server.planCalls.Load(), "a refused request never reaches the data plane")
	assert.Zero(t, server.applyCalls.Load(), "a refused request never reaches the data plane")
}

// An endpoint that does not serve Health advertises nothing either.
func TestCapabilityGateTreatsAnUnimplementedHealthAsNoCapabilities(t *testing.T) {
	server := &capabilityTernServer{healthErr: status.Error(codes.Unimplemented, "unknown method Health")}
	client := newCapabilityTestClient(t, server, nil)

	_, err := client.Plan(t.Context(), &ternv1.PlanRequest{DirectExecution: byteBoundPolicy})

	var refusal *MissingCapabilityError
	require.ErrorAs(t, err, &refusal)
	assert.Equal(t, []string{byteBoundCapability}, refusal.Missing)
	assert.Zero(t, server.planCalls.Load())
}

// A data plane that reports its capabilities is believed only for what it
// lists, and its self-reported version is named in the refusal.
func TestCapabilityGateNamesTheDataPlaneVersionWhenItReportsOne(t *testing.T) {
	server := &capabilityTernServer{health: &ternv1.HealthResponse{Status: "ok", Version: "v0.2.0", Capabilities: []string{"some.other.feature"}}}
	client := newCapabilityTestClient(t, server, nil)

	_, err := client.Plan(t.Context(), &ternv1.PlanRequest{DirectExecution: byteBoundPolicy})

	var refusal *MissingCapabilityError
	require.ErrorAs(t, err, &refusal)
	assert.Equal(t, "v0.2.0", refusal.Version)
	assert.Contains(t, err.Error(), `(SchemaBot v0.2.0) does not advertise direct_execution.max_table_bytes`)
	assert.Zero(t, server.planCalls.Load())
}

// A data plane that advertises the capability receives the request intact.
func TestCapabilityGateSendsWhatTheDataPlaneAdvertises(t *testing.T) {
	server := &capabilityTernServer{health: &ternv1.HealthResponse{Status: "ok", Capabilities: serverCapabilities()}}
	client := newCapabilityTestClient(t, server, nil)

	_, err := client.Plan(t.Context(), &ternv1.PlanRequest{DirectExecution: byteBoundPolicy})

	require.NoError(t, err)
	assert.Equal(t, int32(1), server.planCalls.Load())
}

// A capability read that fails is not a verdict either way. The request is not
// sent, and the error keeps the Health call's gRPC status, so the caller still
// reports an unreachable data plane as unreachable.
func TestCapabilityGateDoesNotSendWhenCapabilitiesCannotBeRead(t *testing.T) {
	server := &capabilityTernServer{healthErr: status.Error(codes.Unavailable, "service unavailable")}
	client := newCapabilityTestClient(t, server, nil)

	_, err := client.Apply(t.Context(), &ternv1.ApplyRequest{PlanId: "plan-1", DirectExecution: byteBoundPolicy})

	require.Error(t, err)
	var refusal *MissingCapabilityError
	assert.False(t, errors.As(err, &refusal), "an unreadable capability list is not a refusal")
	assert.Equal(t, codes.Unavailable, status.Code(err))
	assert.Contains(t, err.Error(), `read capabilities of the data plane for deployment "west" before sending a request that depends on direct_execution.max_table_bytes`)
	assert.Zero(t, server.applyCalls.Load())
}

// A request that sets no annotated field costs no capability read, so a data
// plane whose Health is failing still receives it.
func TestCapabilityGateCostsNothingForRequestsWithoutAnnotatedFields(t *testing.T) {
	server := &capabilityTernServer{healthErr: status.Error(codes.Unavailable, "service unavailable")}
	client := newCapabilityTestClient(t, server, nil)

	_, err := client.Apply(t.Context(), &ternv1.ApplyRequest{PlanId: "plan-1", DirectExecution: &ternv1.DirectExecutionPolicy{Enabled: true, MaxTableRows: 10000}})
	require.NoError(t, err)
	_, err = client.Progress(t.Context(), &ternv1.ProgressRequest{ApplyId: "remote-1"})
	require.NoError(t, err)

	assert.Equal(t, int32(1), server.applyCalls.Load())
	assert.Equal(t, int32(1), server.progressCalls.Load())
	assert.Zero(t, server.healthCalls.Load())
}

// An apply admitted under a byte-bound policy, whose data plane was then rolled
// back to a release that does not advertise the bound, fails at dispatch with
// the refusal instead of reaching a data plane that would misread its policy.
func TestCapabilityGateFailsADispatchTheDataPlaneWouldMisread(t *testing.T) {
	server := &capabilityTernServer{health: &ternv1.HealthResponse{Status: "ok"}}
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
	client := newCapabilityTestClient(t, server, &mockStorage{
		applies: applyStore,
		tasks:   &mockTaskStore{tasks: []*storage.Task{task}},
		logs:    &mockApplyLogStore{},
		plans:   &mockPlanStore{plan: &storage.Plan{ID: apply.PlanID, PlanIdentifier: "plan-byte-bound"}},
	})

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	err := client.ResumeApply(ctx, apply)

	var refusal *MissingCapabilityError
	require.ErrorAs(t, err, &refusal)
	assert.Zero(t, server.applyCalls.Load(), "the apply must not be dispatched to a data plane that would misread its policy")
	assert.Equal(t, state.Apply.Failed, applyStore.apply.State)
	assert.Contains(t, applyStore.apply.ErrorMessage, `the data plane for deployment "west"`)
	assert.Equal(t, state.Task.Failed, task.State)
}

// withUnknownField gives m a field numbered number that its schema does not
// declare, the way a request from a newer caller decodes on an older server.
func withUnknownField[M proto.Message](m M, number protowire.Number) M {
	raw := protowire.AppendTag(nil, number, protowire.VarintType)
	raw = protowire.AppendVarint(raw, 1)
	m.ProtoReflect().SetUnknown(raw)
	return m
}

func TestUnknownFieldPaths(t *testing.T) {
	req := withUnknownField(&ternv1.ApplyRequest{
		DirectExecution: withUnknownField(&ternv1.DirectExecutionPolicy{Enabled: true}, 50),
		DdlChanges:      []*ternv1.TableChange{{TableName: "a"}, withUnknownField(&ternv1.TableChange{TableName: "b"}, 60)},
		SchemaFiles:     map[string]*ternv1.SchemaFiles{"payments": withUnknownField(&ternv1.SchemaFiles{}, 70)},
	}, 99)

	assert.Equal(t, []string{"#99", "ddl_changes#60", "direct_execution#50", "schema_files#70"}, unknownFieldPaths(req))
	assert.Empty(t, unknownFieldPaths(&ternv1.PlanRequest{Database: "payments", DirectExecution: byteBoundPolicy}))
}

// failingPlanClient refuses every plan, as an engine refusing the request would.
type failingPlanClient struct {
	Client
}

func (failingPlanClient) Plan(context.Context, *ternv1.PlanRequest) (*ternv1.PlanResponse, error) {
	return nil, errors.New("resolve direct execution policy: direct_execution_max_table_rows must be positive, got 0")
}

// A request from a newer caller can set a field this data plane drops, and a
// failure caused by the drop then names only the fields the data plane did
// read. The unknown field crosses the wire here as it would from that caller. The failure also says which fields were not understood, that the
// caller is newer, and what to upgrade.
func TestServerNotesUnknownFieldsOnAFailedRequest(t *testing.T) {
	server, logs := newHealthTestServer(failingPlanClient{})
	client := newCapabilityTestClient(t, server, nil)

	_, err := client.Plan(t.Context(), &ternv1.PlanRequest{
		Database:        "payments",
		Environment:     "staging",
		DirectExecution: withUnknownField(&ternv1.DirectExecutionPolicy{Enabled: true}, 50),
	})

	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Contains(t, err.Error(), "must be positive, got 0 (the request also set fields this data plane does not know: direct_execution#50, "+
		"so it came from a newer caller; if the failure concerns what those fields carry, upgrade this data plane)")
	assert.Contains(t, logs.String(), "unknown_fields=[direct_execution#50]")
	assert.Contains(t, logs.String(), "database=payments")
	assert.Contains(t, logs.String(), "environment=staging")

	_, err = client.Plan(t.Context(), &ternv1.PlanRequest{Database: "payments", DirectExecution: &ternv1.DirectExecutionPolicy{Enabled: true}})
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "does not know", "a request with nothing unknown fails with the engine's own message")
}
