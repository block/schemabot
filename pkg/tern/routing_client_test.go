package tern

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/storage"
)

type routingResolverFunc func(context.Context, routing.Request) ([]routing.ExecutionTarget, error)

func (f routingResolverFunc) ResolveTargets(ctx context.Context, req routing.Request) ([]routing.ExecutionTarget, error) {
	return f(ctx, req)
}

type routingPlanLookup map[string]*storage.Plan

func (l routingPlanLookup) Get(_ context.Context, planIdentifier string) (*storage.Plan, error) {
	return l[planIdentifier], nil
}

type routingApplyLookup map[string]*storage.Apply

func (l routingApplyLookup) GetByApplyIdentifier(_ context.Context, applyIdentifier string) (*storage.Apply, error) {
	return l[applyIdentifier], nil
}

type routingApplyOperationLookup map[int64]*storage.ApplyOperation

func (l routingApplyOperationLookup) Get(_ context.Context, id int64) (*storage.ApplyOperation, error) {
	return l[id], nil
}

func (l routingApplyOperationLookup) ListByApply(_ context.Context, applyID int64) ([]*storage.ApplyOperation, error) {
	var ops []*storage.ApplyOperation
	for _, op := range l {
		if op != nil && op.ApplyID == applyID {
			ops = append(ops, op)
		}
	}
	return ops, nil
}

type routingRecordingClient struct {
	Client

	pullSchemaReq      *ternv1.PullSchemaRequest
	planReq            *ternv1.PlanRequest
	applyReq           *ternv1.ApplyRequest
	applyErr           error
	progressReq        *ternv1.ProgressRequest
	resumeApply        *storage.Apply
	resumeOperationID  int64
	pendingObserverSet bool
	pendingObserver    ProgressObserver
	observerApplyID    int64
	isRemote           bool
}

func (c *routingRecordingClient) PullSchema(_ context.Context, req *ternv1.PullSchemaRequest) (*ternv1.PullSchemaResponse, error) {
	c.pullSchemaReq = req
	return &ternv1.PullSchemaResponse{Database: req.Database}, nil
}

func (c *routingRecordingClient) Plan(_ context.Context, req *ternv1.PlanRequest) (*ternv1.PlanResponse, error) {
	c.planReq = req
	return &ternv1.PlanResponse{PlanId: "plan-routed"}, nil
}

func (c *routingRecordingClient) Apply(_ context.Context, req *ternv1.ApplyRequest) (*ternv1.ApplyResponse, error) {
	c.applyReq = req
	if c.applyErr != nil {
		return nil, c.applyErr
	}
	return &ternv1.ApplyResponse{Accepted: true, ApplyId: "apply-routed"}, nil
}

func (c *routingRecordingClient) Progress(_ context.Context, req *ternv1.ProgressRequest) (*ternv1.ProgressResponse, error) {
	c.progressReq = req
	return &ternv1.ProgressResponse{ApplyId: req.ApplyId}, nil
}

func (c *routingRecordingClient) ResumeApply(_ context.Context, apply *storage.Apply) error {
	c.resumeApply = apply
	return nil
}

func (c *routingRecordingClient) ResumeApplyOperation(_ context.Context, apply *storage.Apply, applyOperationID int64) error {
	c.resumeApply = apply
	c.resumeOperationID = applyOperationID
	return nil
}

func (c *routingRecordingClient) SetPendingObserver(observer ProgressObserver) {
	c.pendingObserverSet = observer != nil
	c.pendingObserver = observer
}

func (c *routingRecordingClient) IsRemote() bool {
	return c.isRemote
}

func (c *routingRecordingClient) SetObserver(applyID int64, _ ProgressObserver) {
	c.observerApplyID = applyID
}

type routingNoopObserver struct{}

func (routingNoopObserver) OnProgress(*storage.Apply, []*storage.Task) {}

func (routingNoopObserver) OnTerminal(*storage.Apply, []*storage.Task) {}

type namedRoutingObserver struct {
	routingNoopObserver
	name string
}

func TestRoutingClientPullSchemaResolvesSingleExecutionTarget(t *testing.T) {
	clients := map[string]*routingRecordingClient{
		"east/staging": {},
	}
	routingClient := newTestRoutingClient(t, RoutingClientConfig{
		Resolver: routingResolverFunc(func(_ context.Context, req routing.Request) ([]routing.ExecutionTarget, error) {
			assert.Equal(t, routing.Request{Database: "logical-db", Environment: "staging"}, req)
			return []routing.ExecutionTarget{{
				DatabaseType: storage.DatabaseTypeMySQL,
				Deployment:   "east",
				Target:       "target-123",
			}}, nil
		}),
		PlanLookup:          routingPlanLookup{},
		ApplyLookup:         routingApplyLookup{},
		ClientForDeployment: testDeploymentClientFunc(clients),
	})

	resp, err := routingClient.PullSchema(t.Context(), &ternv1.PullSchemaRequest{
		Database:    "logical-db",
		Environment: "staging",
		Type:        storage.DatabaseTypeMySQL,
		Target:      "caller-supplied-target",
	})

	require.NoError(t, err)
	assert.Equal(t, "logical-db", resp.Database)
	require.NotNil(t, clients["east/staging"].pullSchemaReq)
	assert.Equal(t, "logical-db", clients["east/staging"].pullSchemaReq.Database)
	assert.Equal(t, "staging", clients["east/staging"].pullSchemaReq.Environment)
	assert.Equal(t, storage.DatabaseTypeMySQL, clients["east/staging"].pullSchemaReq.Type)
	assert.Equal(t, "target-123", clients["east/staging"].pullSchemaReq.Target)
}

func TestRoutingClientPullSchemaRejectsInvalidRequestAsInvalidPullSchemaRequest(t *testing.T) {
	routingClient := newTestRoutingClient(t, RoutingClientConfig{
		Resolver:            routingResolverFunc(func(context.Context, routing.Request) ([]routing.ExecutionTarget, error) { return nil, nil }),
		PlanLookup:          routingPlanLookup{},
		ApplyLookup:         routingApplyLookup{},
		ClientForDeployment: testDeploymentClientFunc(nil),
	})

	_, err := routingClient.PullSchema(t.Context(), nil)

	require.Error(t, err)
	assert.ErrorContains(t, err, "pull schema request is required")
	assert.True(t, errors.Is(err, ErrPullSchemaInvalidRequest))
}

func TestRoutingClientPullSchemaRejectsTypeMismatchAsInvalidPullSchemaRequest(t *testing.T) {
	routingClient := newTestRoutingClient(t, RoutingClientConfig{
		Resolver: routingResolverFunc(func(context.Context, routing.Request) ([]routing.ExecutionTarget, error) {
			return []routing.ExecutionTarget{{
				DatabaseType: storage.DatabaseTypeMySQL,
				Deployment:   "east",
				Target:       "target-123",
			}}, nil
		}),
		PlanLookup:          routingPlanLookup{},
		ApplyLookup:         routingApplyLookup{},
		ClientForDeployment: testDeploymentClientFunc(nil),
	})

	_, err := routingClient.PullSchema(t.Context(), &ternv1.PullSchemaRequest{
		Database:    "logical-db",
		Environment: "staging",
		Type:        storage.DatabaseTypeVitess,
	})

	require.Error(t, err)
	assert.ErrorContains(t, err, `request type "vitess" does not match resolved database type "mysql"`)
	assert.True(t, errors.Is(err, ErrPullSchemaInvalidRequest))
}

func TestRoutingClientPlanResolvesSingleExecutionTarget(t *testing.T) {
	clients := map[string]*routingRecordingClient{
		"east/staging": {},
	}
	routingClient := newTestRoutingClient(t, RoutingClientConfig{
		Resolver: routingResolverFunc(func(_ context.Context, req routing.Request) ([]routing.ExecutionTarget, error) {
			assert.Equal(t, routing.Request{Database: "logical-db", Environment: "staging"}, req)
			return []routing.ExecutionTarget{{
				DatabaseType: storage.DatabaseTypeMySQL,
				Deployment:   "east",
				Target:       "target-123",
			}}, nil
		}),
		PlanLookup:          routingPlanLookup{},
		ApplyLookup:         routingApplyLookup{},
		ClientForDeployment: testDeploymentClientFunc(clients),
	})

	resp, err := routingClient.Plan(t.Context(), &ternv1.PlanRequest{
		Database:    "logical-db",
		Environment: "staging",
		Type:        storage.DatabaseTypeVitess,
		Target:      "caller-supplied-target",
	})

	require.NoError(t, err)
	assert.Equal(t, "plan-routed", resp.PlanId)
	require.NotNil(t, clients["east/staging"].planReq)
	assert.Equal(t, "logical-db", clients["east/staging"].planReq.Database)
	assert.Equal(t, "staging", clients["east/staging"].planReq.Environment)
	assert.Equal(t, storage.DatabaseTypeMySQL, clients["east/staging"].planReq.Type)
	assert.Equal(t, "target-123", clients["east/staging"].planReq.Target)
}

func TestRoutingClientPlanRejectsMultiTargetRoute(t *testing.T) {
	clients := map[string]*routingRecordingClient{
		"east/staging": {},
	}
	routingClient := newTestRoutingClient(t, RoutingClientConfig{
		Resolver: routingResolverFunc(func(context.Context, routing.Request) ([]routing.ExecutionTarget, error) {
			return []routing.ExecutionTarget{{Deployment: "east"}, {Deployment: "west"}}, nil
		}),
		PlanLookup:          routingPlanLookup{},
		ApplyLookup:         routingApplyLookup{},
		ClientForDeployment: testDeploymentClientFunc(clients),
	})

	_, err := routingClient.Plan(t.Context(), &ternv1.PlanRequest{
		Database:    "logical-db",
		Environment: "staging",
	})

	require.Error(t, err)
	assert.ErrorContains(t, err, "resolved to 2 execution targets")
	assert.Nil(t, clients["east/staging"].planReq)
}

func TestRoutingClientApplyRoutesByStoredPlan(t *testing.T) {
	clients := map[string]*routingRecordingClient{
		"west/production": {},
	}
	routingClient := newTestRoutingClient(t, RoutingClientConfig{
		Resolver: routingResolverFunc(func(context.Context, routing.Request) ([]routing.ExecutionTarget, error) {
			return nil, fmt.Errorf("apply must route from stored plan")
		}),
		PlanLookup: routingPlanLookup{
			"plan-123": {
				PlanIdentifier: "plan-123",
				Database:       "stored-db",
				DatabaseType:   storage.DatabaseTypeVitess,
				Deployment:     "west",
				Target:         "stored-target",
				Environment:    "production",
			},
		},
		ApplyLookup:         routingApplyLookup{},
		ClientForDeployment: testDeploymentClientFunc(clients),
	})
	require.NoError(t, routingClient.SetPendingObserverForPlan("plan-123", routingNoopObserver{}))

	resp, err := routingClient.Apply(t.Context(), &ternv1.ApplyRequest{
		PlanId:      "plan-123",
		Database:    "request-db",
		Type:        storage.DatabaseTypeMySQL,
		Environment: "production",
		Target:      "request-target",
		Options: map[string]string{
			"target": "request-target",
		},
	})

	require.NoError(t, err)
	assert.True(t, resp.Accepted)
	assert.True(t, clients["west/production"].pendingObserverSet)
	require.NotNil(t, clients["west/production"].applyReq)
	assert.Equal(t, "stored-db", clients["west/production"].applyReq.Database)
	assert.Equal(t, storage.DatabaseTypeVitess, clients["west/production"].applyReq.Type)
	assert.Equal(t, "production", clients["west/production"].applyReq.Environment)
	assert.Equal(t, "stored-target", clients["west/production"].applyReq.Target)
	assert.Equal(t, "stored-target", clients["west/production"].applyReq.Options["target"])
}

func TestRoutingClientApplyConsumesPlanScopedPendingObserver(t *testing.T) {
	clients := map[string]*routingRecordingClient{
		"west/production": {},
	}
	routingClient := newTestRoutingClient(t, RoutingClientConfig{
		Resolver: routingResolverFunc(func(context.Context, routing.Request) ([]routing.ExecutionTarget, error) {
			return nil, fmt.Errorf("apply must route from stored plan")
		}),
		PlanLookup: routingPlanLookup{
			"plan-123": {
				PlanIdentifier: "plan-123",
				Database:       "stored-db",
				DatabaseType:   storage.DatabaseTypeMySQL,
				Deployment:     "west",
				Target:         "stored-target",
				Environment:    "production",
			},
			"plan-456": {
				PlanIdentifier: "plan-456",
				Database:       "stored-db",
				DatabaseType:   storage.DatabaseTypeMySQL,
				Deployment:     "west",
				Target:         "stored-target",
				Environment:    "production",
			},
		},
		ApplyLookup:         routingApplyLookup{},
		ClientForDeployment: testDeploymentClientFunc(clients),
	})
	planObserver := namedRoutingObserver{name: "plan-456"}
	require.NoError(t, routingClient.SetPendingObserverForPlan("plan-456", planObserver))

	_, err := routingClient.Apply(t.Context(), &ternv1.ApplyRequest{
		PlanId:      "plan-123",
		Environment: "production",
	})
	require.NoError(t, err)
	assert.False(t, clients["west/production"].pendingObserverSet)

	_, err = routingClient.Apply(t.Context(), &ternv1.ApplyRequest{
		PlanId:      "plan-456",
		Environment: "production",
	})
	require.NoError(t, err)
	require.NotNil(t, clients["west/production"].pendingObserver)
	assert.Equal(t, ProgressObserver(planObserver), clients["west/production"].pendingObserver)

	clients["west/production"].pendingObserverSet = false
	clients["west/production"].pendingObserver = nil
	_, err = routingClient.Apply(t.Context(), &ternv1.ApplyRequest{
		PlanId:      "plan-456",
		Environment: "production",
	})
	require.NoError(t, err)
	assert.False(t, clients["west/production"].pendingObserverSet)
}

func TestRoutingClientSetPendingObserverForPlanValidatesPlanIdentifier(t *testing.T) {
	routingClient := newTestRoutingClient(t, RoutingClientConfig{
		Resolver:            routingResolverFunc(func(context.Context, routing.Request) ([]routing.ExecutionTarget, error) { return nil, nil }),
		PlanLookup:          routingPlanLookup{},
		ApplyLookup:         routingApplyLookup{},
		ClientForDeployment: testDeploymentClientFunc(nil),
	})

	err := routingClient.SetPendingObserverForPlan("", routingNoopObserver{})

	require.Error(t, err)
	assert.ErrorContains(t, err, "plan identifier is required")
}

func TestRoutingClientSetPendingObserverDoesNotAttachUnscopedObserver(t *testing.T) {
	clients := map[string]*routingRecordingClient{
		"west/production": {},
	}
	routingClient := newTestRoutingClient(t, RoutingClientConfig{
		Resolver: routingResolverFunc(func(context.Context, routing.Request) ([]routing.ExecutionTarget, error) {
			return nil, fmt.Errorf("apply must route from stored plan")
		}),
		PlanLookup: routingPlanLookup{
			"plan-123": {
				PlanIdentifier: "plan-123",
				Database:       "stored-db",
				DatabaseType:   storage.DatabaseTypeMySQL,
				Deployment:     "west",
				Target:         "stored-target",
				Environment:    "production",
			},
		},
		ApplyLookup:         routingApplyLookup{},
		ClientForDeployment: testDeploymentClientFunc(clients),
	})
	routingClient.SetPendingObserver(routingNoopObserver{})

	_, err := routingClient.Apply(t.Context(), &ternv1.ApplyRequest{
		PlanId:      "plan-123",
		Environment: "production",
	})

	require.NoError(t, err)
	assert.False(t, clients["west/production"].pendingObserverSet)
}

func TestRoutingClientSetPendingObserverForPlanClearsObserver(t *testing.T) {
	clients := map[string]*routingRecordingClient{
		"west/production": {},
	}
	routingClient := newTestRoutingClient(t, RoutingClientConfig{
		Resolver: routingResolverFunc(func(context.Context, routing.Request) ([]routing.ExecutionTarget, error) {
			return nil, fmt.Errorf("apply must route from stored plan")
		}),
		PlanLookup: routingPlanLookup{
			"plan-123": {
				PlanIdentifier: "plan-123",
				Database:       "stored-db",
				DatabaseType:   storage.DatabaseTypeMySQL,
				Deployment:     "west",
				Target:         "stored-target",
				Environment:    "production",
			},
		},
		ApplyLookup:         routingApplyLookup{},
		ClientForDeployment: testDeploymentClientFunc(clients),
	})
	require.NoError(t, routingClient.SetPendingObserverForPlan("plan-123", routingNoopObserver{}))
	require.NoError(t, routingClient.SetPendingObserverForPlan("plan-123", nil))

	_, err := routingClient.Apply(t.Context(), &ternv1.ApplyRequest{
		PlanId:      "plan-123",
		Environment: "production",
	})

	require.NoError(t, err)
	assert.False(t, clients["west/production"].pendingObserverSet)
}

func TestRoutingClientApplyRejectsEnvironmentMismatch(t *testing.T) {
	clients := map[string]*routingRecordingClient{
		"west/production": {},
	}
	routingClient := newTestRoutingClient(t, RoutingClientConfig{
		Resolver: routingResolverFunc(func(context.Context, routing.Request) ([]routing.ExecutionTarget, error) {
			return nil, fmt.Errorf("apply must route from stored plan")
		}),
		PlanLookup: routingPlanLookup{
			"plan-123": {
				PlanIdentifier: "plan-123",
				Database:       "stored-db",
				DatabaseType:   storage.DatabaseTypeVitess,
				Deployment:     "west",
				Target:         "stored-target",
				Environment:    "production",
			},
		},
		ApplyLookup:         routingApplyLookup{},
		ClientForDeployment: testDeploymentClientFunc(clients),
	})

	_, err := routingClient.Apply(t.Context(), &ternv1.ApplyRequest{
		PlanId:      "plan-123",
		Environment: "staging",
	})

	require.Error(t, err)
	assert.ErrorContains(t, err, "was created for environment \"production\", not \"staging\"")
	assert.Nil(t, clients["west/production"].applyReq)
}

func TestRoutingClientApplyClearsPendingObserverOnError(t *testing.T) {
	clients := map[string]*routingRecordingClient{
		"west/production": {applyErr: fmt.Errorf("delegate apply failed")},
	}
	routingClient := newTestRoutingClient(t, RoutingClientConfig{
		Resolver: routingResolverFunc(func(context.Context, routing.Request) ([]routing.ExecutionTarget, error) {
			return nil, fmt.Errorf("apply must route from stored plan")
		}),
		PlanLookup: routingPlanLookup{
			"plan-123": {
				PlanIdentifier: "plan-123",
				Database:       "stored-db",
				DatabaseType:   storage.DatabaseTypeVitess,
				Deployment:     "west",
				Target:         "stored-target",
				Environment:    "production",
			},
		},
		ApplyLookup:         routingApplyLookup{},
		ClientForDeployment: testDeploymentClientFunc(clients),
	})
	require.NoError(t, routingClient.SetPendingObserverForPlan("plan-123", routingNoopObserver{}))

	_, err := routingClient.Apply(t.Context(), &ternv1.ApplyRequest{
		PlanId:      "plan-123",
		Environment: "production",
	})

	require.Error(t, err)
	assert.ErrorContains(t, err, "delegate apply failed")
	assert.False(t, clients["west/production"].pendingObserverSet)
}

func TestRoutingClientProgressRoutesByStoredApply(t *testing.T) {
	clients := map[string]*routingRecordingClient{
		"west/production": {},
	}
	routingClient := newTestRoutingClient(t, RoutingClientConfig{
		Resolver: routingResolverFunc(func(context.Context, routing.Request) ([]routing.ExecutionTarget, error) {
			return nil, fmt.Errorf("progress must route from stored apply")
		}),
		PlanLookup: routingPlanLookup{},
		ApplyLookup: routingApplyLookup{
			"apply-123": {
				ID:              42,
				ApplyIdentifier: "apply-123",
				ExternalID:      "remote-apply-456",
				Deployment:      "west",
				Environment:     "production",
			},
		},
		ClientForDeployment: testDeploymentClientFunc(clients),
	})
	routingClient.SetObserver(42, routingNoopObserver{})

	resp, err := routingClient.Progress(t.Context(), &ternv1.ProgressRequest{
		ApplyId:     "apply-123",
		Environment: "production",
	})

	require.NoError(t, err)
	assert.Equal(t, "remote-apply-456", resp.ApplyId)
	assert.Equal(t, int64(42), clients["west/production"].observerApplyID)
	require.NotNil(t, clients["west/production"].progressReq)
	assert.Equal(t, "remote-apply-456", clients["west/production"].progressReq.ApplyId)
	assert.Equal(t, "production", clients["west/production"].progressReq.Environment)
}

func TestRoutingClientProgressRejectsEnvironmentMismatch(t *testing.T) {
	clients := map[string]*routingRecordingClient{
		"west/production": {},
	}
	routingClient := newTestRoutingClient(t, RoutingClientConfig{
		Resolver:   routingResolverFunc(func(context.Context, routing.Request) ([]routing.ExecutionTarget, error) { return nil, nil }),
		PlanLookup: routingPlanLookup{},
		ApplyLookup: routingApplyLookup{
			"apply-123": {
				ID:              42,
				ApplyIdentifier: "apply-123",
				ExternalID:      "remote-apply-456",
				Deployment:      "west",
				Environment:     "production",
			},
		},
		ClientForDeployment: testDeploymentClientFunc(clients),
	})

	_, err := routingClient.Progress(t.Context(), &ternv1.ProgressRequest{
		ApplyId:     "apply-123",
		Environment: "staging",
	})

	require.Error(t, err)
	assert.ErrorContains(t, err, "stored for environment \"production\", not \"staging\"")
	assert.Nil(t, clients["west/production"].progressReq)
}

func TestRoutingClientProgressRejectsRemoteApplyWithoutExternalID(t *testing.T) {
	clients := map[string]*routingRecordingClient{
		"west/production": {isRemote: true},
	}
	routingClient := newTestRoutingClient(t, RoutingClientConfig{
		Resolver:   routingResolverFunc(func(context.Context, routing.Request) ([]routing.ExecutionTarget, error) { return nil, nil }),
		PlanLookup: routingPlanLookup{},
		ApplyLookup: routingApplyLookup{
			"apply-123": {
				ID:              42,
				ApplyIdentifier: "apply-123",
				Deployment:      "west",
				Environment:     "production",
			},
		},
		ApplyOperationLookup: routingApplyOperationLookup{
			1: {ID: 1, ApplyID: 42, Deployment: "west"},
		},
		ClientForDeployment: testDeploymentClientFunc(clients),
	})

	_, err := routingClient.Progress(t.Context(), &ternv1.ProgressRequest{
		ApplyId:     "apply-123",
		Environment: "production",
	})

	require.Error(t, err)
	assert.ErrorContains(t, err, "has not been accepted by remote deployment \"west\" yet")
	assert.Nil(t, clients["west/production"].progressReq)
}

func TestRoutingClientProgressFailsClosedForMultiOpRemoteApply(t *testing.T) {
	// An apply-scoped control/progress call carries no operation context. For a
	// multi-operation remote apply there is no single authoritative remote apply
	// id (each deployment's id lives on its own operation row), so routing must
	// fail closed rather than guess one.
	clients := map[string]*routingRecordingClient{
		"west/production": {isRemote: true},
	}
	routingClient := newTestRoutingClient(t, RoutingClientConfig{
		Resolver:   routingResolverFunc(func(context.Context, routing.Request) ([]routing.ExecutionTarget, error) { return nil, nil }),
		PlanLookup: routingPlanLookup{},
		ApplyLookup: routingApplyLookup{
			"apply-123": {
				ID:              42,
				ApplyIdentifier: "apply-123",
				ExternalID:      "remote-apply-456",
				Deployment:      "west",
				Environment:     "production",
			},
		},
		ApplyOperationLookup: routingApplyOperationLookup{
			1: {ID: 1, ApplyID: 42, Deployment: "west"},
			2: {ID: 2, ApplyID: 42, Deployment: "east"},
		},
		ClientForDeployment: testDeploymentClientFunc(clients),
	})

	_, err := routingClient.Progress(t.Context(), &ternv1.ProgressRequest{
		ApplyId:     "apply-123",
		Environment: "production",
	})

	require.Error(t, err)
	assert.ErrorContains(t, err, "spans 2 operations")
	assert.Nil(t, clients["west/production"].progressReq)
}

func TestRoutingClientResumeApplyAttachesObserver(t *testing.T) {
	clients := map[string]*routingRecordingClient{
		"east/staging": {},
	}
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-123",
		Deployment:      "east",
		Environment:     "staging",
	}
	routingClient := newTestRoutingClient(t, RoutingClientConfig{
		Resolver:            routingResolverFunc(func(context.Context, routing.Request) ([]routing.ExecutionTarget, error) { return nil, nil }),
		PlanLookup:          routingPlanLookup{},
		ApplyLookup:         routingApplyLookup{},
		ClientForDeployment: testDeploymentClientFunc(clients),
	})
	routingClient.SetObserver(42, routingNoopObserver{})

	err := routingClient.ResumeApply(t.Context(), apply)

	require.NoError(t, err)
	assert.Same(t, apply, clients["east/staging"].resumeApply)
	assert.Equal(t, int64(42), clients["east/staging"].observerApplyID)
}

func TestRoutingClientResumeApplyOperationRoutesByStoredOperation(t *testing.T) {
	clients := map[string]*routingRecordingClient{
		"east/staging": {},
		"west/staging": {},
	}
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-123",
		Deployment:      "east",
		Environment:     "staging",
	}
	apply.SetOptions(storage.ApplyOptions{Target: "target-east"})
	routingClient := newTestRoutingClient(t, RoutingClientConfig{
		Resolver:    routingResolverFunc(func(context.Context, routing.Request) ([]routing.ExecutionTarget, error) { return nil, nil }),
		PlanLookup:  routingPlanLookup{},
		ApplyLookup: routingApplyLookup{},
		ApplyOperationLookup: routingApplyOperationLookup{
			7: {
				ID:         7,
				ApplyID:    42,
				Deployment: "west",
				Target:     "target-west",
			},
		},
		ClientForDeployment: testDeploymentClientFunc(clients),
	})
	routingClient.SetObserver(42, routingNoopObserver{})

	err := routingClient.ResumeApplyOperation(t.Context(), apply, 7)

	require.NoError(t, err)
	assert.Nil(t, clients["east/staging"].resumeApply)
	require.NotNil(t, clients["west/staging"].resumeApply)
	assert.NotSame(t, apply, clients["west/staging"].resumeApply)
	assert.Equal(t, "west", clients["west/staging"].resumeApply.Deployment)
	assert.Equal(t, "target-west", clients["west/staging"].resumeApply.GetOptions().Target)
	assert.Equal(t, "east", apply.Deployment)
	assert.Equal(t, "target-east", apply.GetOptions().Target)
	assert.Equal(t, int64(7), clients["west/staging"].resumeOperationID)
	assert.Equal(t, int64(42), clients["west/staging"].observerApplyID)
}

func TestRoutingClientResumeApplyOperationRejectsOperationApplyMismatch(t *testing.T) {
	clients := map[string]*routingRecordingClient{
		"west/staging": {},
	}
	apply := &storage.Apply{
		ID:              42,
		ApplyIdentifier: "apply-123",
		Deployment:      "east",
		Environment:     "staging",
	}
	routingClient := newTestRoutingClient(t, RoutingClientConfig{
		Resolver:    routingResolverFunc(func(context.Context, routing.Request) ([]routing.ExecutionTarget, error) { return nil, nil }),
		PlanLookup:  routingPlanLookup{},
		ApplyLookup: routingApplyLookup{},
		ApplyOperationLookup: routingApplyOperationLookup{
			7: {
				ID:         7,
				ApplyID:    100,
				Deployment: "west",
				Target:     "target-west",
			},
		},
		ClientForDeployment: testDeploymentClientFunc(clients),
	})

	err := routingClient.ResumeApplyOperation(t.Context(), apply, 7)

	require.Error(t, err)
	assert.ErrorContains(t, err, "belongs to apply 100, not apply 42")
	assert.Nil(t, clients["west/staging"].resumeApply)
}

func newTestRoutingClient(t *testing.T, config RoutingClientConfig) *RoutingClient {
	t.Helper()
	client, err := NewRoutingClient(config)
	require.NoError(t, err)
	return client
}

func testDeploymentClientFunc(clients map[string]*routingRecordingClient) DeploymentClientFunc {
	return func(_ context.Context, deployment, environment string) (Client, error) {
		client := clients[deployment+"/"+environment]
		if client == nil {
			return nil, fmt.Errorf("unexpected deployment %s/%s", deployment, environment)
		}
		return client, nil
	}
}
