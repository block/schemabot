package webhook

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/block/schemabot/pkg/api"
	ghclient "github.com/block/schemabot/pkg/github"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/tern"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplySchemaAdmissionRefusalReleasesOnlyAutomaticIntent(t *testing.T) {
	verificationErr := errors.New("GitHub read failed")
	for _, tc := range []struct {
		name           string
		failAdmission  int
		err            error
		rollout        bool
		manual         bool
		replacedIntent bool
		cancelContext  bool
	}{
		{name: "primary hold", failAdmission: 1, err: errPRSchemaCapHeld},
		{name: "rollout verification failed", failAdmission: 2, err: verificationErr, rollout: true},
		{name: "queue revision moved", failAdmission: 2, err: errPRSchemaRevisionMoved},
		{name: "queue preserves manual confirmation", failAdmission: 2, err: errPRSchemaCapHeld, manual: true},
		{name: "queue preserves replacement pin", failAdmission: 2, err: errPRSchemaCapHeld, replacedIntent: true},
		{name: "verification cleanup survives cancellation", failAdmission: 1, err: context.Canceled, cancelContext: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			logger := testLogger()
			planClient := &applyAdmissionPlanClient{}
			cfg := newPlanRetryTestHandler(planClient, time.Millisecond).config()
			cfg.PRSchemaCap = api.PRSchemaCapPolicy{Limit: 5, Label: capTestLabel}
			if tc.rollout {
				cfg.Databases["orders"].Environments["staging"] = api.EnvironmentConfig{
					Deployment: "remote",
					Targets:    []api.TargetEntry{{Target: "orders-target"}, {Target: "other-target"}},
				}
			}
			store := &applyAdmissionStorage{plans: applyAdmissionPlanStore{plans: make(map[string]*storage.Plan)}}
			service := api.New(store, cfg, map[string]tern.Client{"remote/staging": planClient}, logger)
			github, mux := setupGitHubServer(t)
			registerApplyDiscoveryEndpoints(t, mux, "orders")
			registerBaseFreshnessRef(t, mux)
			mux.HandleFunc("GET /repos/"+capTestRepo+"/compare/base-tip-sha...abc123", func(w http.ResponseWriter, _ *http.Request) {
				_, err := fmt.Fprint(w, `{"merge_base_commit":{"sha":"base-tip-sha"}}`)
				assert.NoError(t, err)
			})
			mux.HandleFunc("GET /repos/"+capTestRepo+"/git/trees/base-tip-sha", func(w http.ResponseWriter, _ *http.Request) {
				_, err := fmt.Fprint(w, `{"tree":[{"path":"schema","type":"tree","sha":"schema-tree"}]}`)
				assert.NoError(t, err)
			})
			comments := make(chan string, 4)
			mux.HandleFunc("POST /repos/"+capTestRepo+"/issues/1/comments", commentRecorder(t, comments))
			client := ghclient.NewInstallationClient(github, logger)
			h := NewHandler(service, &fakeClientFactory{client: client}, nil, logger)

			// Generate the same plan that the operator was shown before the final
			// apply re-plan. Both carry a change so a successful re-plan can
			// continue to the queue admission check.
			planReq := planRetryTestRequest()
			planReq.Repository = capTestRepo
			planReq.PullRequest = new(int32(1))
			planReq.HeadSHA = new("abc123")
			planReq.SchemaPath = "schema"
			planReq.SourceTrusted = true
			service.OnPRSchemaAdmission = func(context.Context, string, int, string) error { return nil }
			_, planResp, err := service.ExecutePlanProto(ctx, planReq)
			require.NoError(t, err)
			pinned := store.plans.plans[planResp.PlanID]
			require.NotNil(t, pinned)
			store.locks.lock = &storage.Lock{
				DatabaseName: planReq.Database, DatabaseType: planReq.Type,
				Owner: capTestRepo + "#1", PendingPlanID: pinned.PlanIdentifier,
			}
			replacement := rollbackPendingPlanID("new-plan")
			admissions := 0
			service.OnPRSchemaAdmission = func(context.Context, string, int, string) error {
				admissions++
				if admissions != tc.failAdmission {
					return nil
				}
				if tc.replacedIntent {
					store.locks.lock.PendingPlanID = replacement
				}
				if tc.cancelContext {
					cancel()
				}
				return tc.err
			}
			schemaResult := &ghclient.SchemaRequestResult{
				Database: planReq.Database, Type: planReq.Type, SchemaFiles: planReq.SchemaFiles,
				SchemaPath: planReq.SchemaPath, HeadSHA: *planReq.HeadSHA,
			}
			storedPlan := pinned
			if tc.manual {
				storedPlan = nil
			}
			h.executeApply(ctx, client, capTestRepo, 1, schemaResult, "staging", 42, "mona",
				CommandResult{}, storedPlan, pinned, pinned.PlanIdentifier, nil)

			assert.Equal(t, tc.failAdmission, admissions)
			assert.False(t, service.HasPendingObserver("orders", "", "staging"))
			switch {
			case tc.manual:
				require.NotNil(t, store.locks.lock)
				assert.Equal(t, pinned.PlanIdentifier, store.locks.lock.PendingPlanID)
				assert.Zero(t, store.locks.releaseAttempts)
			case tc.replacedIntent:
				require.NotNil(t, store.locks.lock)
				assert.Equal(t, replacement, store.locks.lock.PendingPlanID)
				assert.Equal(t, 1, store.locks.releaseAttempts)
			default:
				assert.Nil(t, store.locks.lock)
				assert.Equal(t, 1, store.locks.releaseAttempts)
			}
			if !isPRSchemaCapStop(tc.err) {
				require.Len(t, comments, 1)
				assert.Contains(t, <-comments, "SchemaBot could not verify the PR schema limit. Nothing was applied. Retry the command.")
			} else {
				assert.Empty(t, comments)
			}
		})
	}
}

type applyAdmissionPlanClient struct {
	tern.Client
	calls int
}

func (c *applyAdmissionPlanClient) Plan(context.Context, *ternv1.PlanRequest) (*ternv1.PlanResponse, error) {
	c.calls++
	return &ternv1.PlanResponse{
		PlanId: fmt.Sprintf("plan-%d", c.calls),
		Changes: []*ternv1.SchemaChange{{
			Namespace: "orders",
			TableChanges: []*ternv1.TableChange{{
				TableName:  "users",
				Ddl:        "ALTER TABLE `users` ADD COLUMN `active` bool",
				ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
			}},
		}},
	}, nil
}

func (c *applyAdmissionPlanClient) IsRemote() bool   { return true }
func (c *applyAdmissionPlanClient) Endpoint() string { return "remote:9090" }

type applyAdmissionStorage struct {
	emptyStorage
	plans applyAdmissionPlanStore
	locks applyAdmissionLockStore
}

func (s *applyAdmissionStorage) Plans() storage.PlanStore { return &s.plans }
func (s *applyAdmissionStorage) Locks() storage.LockStore { return &s.locks }

type applyAdmissionPlanStore struct {
	storage.PlanStore
	plans map[string]*storage.Plan
}

func (s *applyAdmissionPlanStore) Create(_ context.Context, plan *storage.Plan) (int64, error) {
	s.plans[plan.PlanIdentifier] = plan
	return int64(len(s.plans)), nil
}

func (s *applyAdmissionPlanStore) Get(_ context.Context, planID string) (*storage.Plan, error) {
	return s.plans[planID], nil
}

func (s *applyAdmissionPlanStore) List(context.Context, storage.ListPlansOptions) ([]*storage.Plan, error) {
	return nil, nil
}

type applyAdmissionLockStore struct {
	storage.LockStore
	lock            *storage.Lock
	releaseAttempts int
}

func (s *applyAdmissionLockStore) ReleaseIfPendingPlanID(ctx context.Context, database, dbType, owner, pendingPlanID string) (bool, error) {
	s.releaseAttempts++
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if s.lock == nil || s.lock.DatabaseName != database || s.lock.DatabaseType != dbType ||
		s.lock.Owner != owner || s.lock.PendingPlanID != pendingPlanID {
		return false, nil
	}
	s.lock = nil
	return true, nil
}
