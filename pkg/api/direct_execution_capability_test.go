package api

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"

	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
)

// predatesCapabilityReporting stands in for a data plane released before
// Health reported capabilities: it answers Health exactly as that release
// did, with a status and nothing else, and serves every other RPC as this
// branch's server does.
type predatesCapabilityReporting struct {
	*tern.Server
}

func (s predatesCapabilityReporting) Health(context.Context, *ternv1.HealthRequest) (*ternv1.HealthResponse, error) {
	return &ternv1.HealthResponse{Status: "ok"}, nil
}

// serveDataPlane serves server on a local listener and returns its address.
func serveDataPlane(t *testing.T, server ternv1.TernServer) string {
	t.Helper()
	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "localhost:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	ternv1.RegisterTernServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)
	return lis.Addr().String()
}

// planAgainstDataPlane runs a control plane holding policy against a data
// plane at address reached through the deployment "west", and returns the
// plan's error.
func planAgainstDataPlane(t *testing.T, address string, policy *DirectExecutionConfig) error {
	t.Helper()
	config := &ServerConfig{
		DirectExecution: policy,
		Databases: map[string]DatabaseConfig{
			"payments": {
				Type:         storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{"staging": {Target: "payments-staging-target", Deployment: "west"}},
			},
		},
		TernDeployments: TernConfig{"west": {"staging": address}},
	}
	require.NoError(t, policy.Validate("server config"), "the fixture is a policy the server would load")
	svc := New(&mockStorageWithPlanLookup{plans: &capturingPlanStore{}}, config, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() { utils.CloseAndLog(svc) })
	_, err := svc.ExecutePlan(t.Context(), PlanRequest{
		Database:    "payments",
		Environment: "staging",
		Type:        storage.DatabaseTypeMySQL,
		SchemaFiles: map[string]*ternv1.SchemaFiles{
			"payments": {Files: map[string]string{"users.sql": "CREATE TABLE users (id bigint primary key)"}},
		},
	})
	return err
}

// A control plane configured with a byte-bound direct execution policy sits
// in front of a data plane that predates the byte bound. That data plane
// would drop max_table_bytes off the request, read an enabled policy with no
// bound, and fail every plan with an error about a row bound nobody set. The
// field is annotated as requiring a capability, so the control plane reads
// the data plane's capabilities first and refuses the plan itself, naming the
// deployment, the capability, and the remedy. The request never reaches the
// data plane, and the plan fails closed for a stated reason.
func TestPlanWithByteBoundIsRefusedBeforeReachingADataPlaneThatCannotReadIt(t *testing.T) {
	dataPlane := &mockTernClient{planResp: &ternv1.PlanResponse{PlanId: "plan-direct"}}
	address := serveDataPlane(t, predatesCapabilityReporting{tern.NewServer(dataPlane, nil)})

	err := planAgainstDataPlane(t, address, &DirectExecutionConfig{Enabled: true, MaxTableBytes: "100MiB"})
	require.Error(t, err)

	var refusal *tern.MissingCapabilityError
	require.ErrorAs(t, err, &refusal)
	assert.Equal(t, "west", refusal.Deployment)
	assert.Empty(t, refusal.Version, "a data plane that predates capability reporting reports no version")
	assert.Equal(t, []string{"direct_execution.max_table_bytes"}, refusal.Missing)
	assert.Contains(t, err.Error(), `the data plane for deployment "west" (a SchemaBot release that predates capability reporting) `+
		`does not advertise direct_execution.max_table_bytes, which this request depends on, so the request was not sent`)
	assert.Contains(t, err.Error(), "Upgrade the data plane before using direct_execution.max_table_bytes")
	assert.NotContains(t, err.Error(), address, "the refusal is rendered on pull requests and must not carry the data plane's address")
	assert.Nil(t, dataPlane.planReq, "the plan request must not be sent to a data plane that would misread its policy")
}

// The same old data plane still serves a row-bound policy, which every
// release since forwarding began reads correctly: the capability check costs
// only the requests that set a field the data plane would misread.
func TestPlanWithRowBoundStillReachesADataPlaneThatPredatesCapabilities(t *testing.T) {
	dataPlane := &mockTernClient{planResp: &ternv1.PlanResponse{PlanId: "plan-direct"}}
	address := serveDataPlane(t, predatesCapabilityReporting{tern.NewServer(dataPlane, nil)})

	require.NoError(t, planAgainstDataPlane(t, address, &DirectExecutionConfig{Enabled: true, MaxTableRows: 10000}))

	require.NotNil(t, dataPlane.planReq)
	policy := dataPlane.planReq.GetDirectExecution()
	require.NotNil(t, policy)
	assert.True(t, policy.GetEnabled())
	assert.Equal(t, int64(10000), policy.GetMaxTableRows())
	assert.Zero(t, policy.GetMaxTableBytes())
}

// A data plane from this release advertises the byte bound, so the plan goes
// through with the bound intact.
func TestPlanWithByteBoundReachesADataPlaneThatAdvertisesIt(t *testing.T) {
	dataPlane := &mockTernClient{planResp: &ternv1.PlanResponse{PlanId: "plan-direct"}}
	address := serveDataPlane(t, tern.NewServer(dataPlane, nil, tern.WithVersion("v9.9.9")))

	require.NoError(t, planAgainstDataPlane(t, address, &DirectExecutionConfig{Enabled: true, MaxTableBytes: "100MiB"}))

	require.NotNil(t, dataPlane.planReq)
	policy := dataPlane.planReq.GetDirectExecution()
	require.NotNil(t, policy)
	assert.True(t, policy.GetEnabled())
	assert.Equal(t, int64(100<<20), policy.GetMaxTableBytes())
	assert.Zero(t, policy.GetMaxTableRows())
}
