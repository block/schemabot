//go:build integration

package api

import (
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/block/mysql"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

// A CLI plan of a two-target environment, made with --pull-request but no
// --repository, stores the primary's plan and a plan for the second target
// stamped with the primary's identifier. POST /api/apply of that plan from the
// CLI, which renders the rollout, pairs the second target with its own stored
// plan through real storage, and ignores a plan stamped by another round.
func TestHandleApply_CLIRolloutPairsEachMemberWithItsStoredPlan(t *testing.T) {
	ctx := t.Context()
	dsn := newStorageDatabaseWithSchema(t).DSN
	db, err := sql.Open("block-mysql", dsn)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(ctx))
	t.Cleanup(func() {
		utils.CloseAndLog(db)
	})
	st := mysqlstore.New(db)

	alter := storage.TableChange{Namespace: "testapp", Table: "users", Operation: "alter", DDL: "ALTER TABLE `users` ADD COLUMN `email` varchar(255)"}
	cliPlan := func(identifier, target, primary string) *storage.Plan {
		return &storage.Plan{
			PlanIdentifier:        identifier,
			PrimaryPlanIdentifier: primary,
			Database:              "testapp",
			DatabaseType:          storage.DatabaseTypeMySQL,
			Deployment:            "eu",
			Target:                target,
			Environment:           "production",
			PullRequest:           42,
			Namespaces:            map[string]*storage.NamespacePlanData{"testapp": {Tables: []storage.TableChange{alter}}},
			CreatedAt:             time.Now(),
		}
	}
	_, err = st.Plans().Create(ctx, cliPlan("plan-cli-primary", "testapp-001", ""))
	require.NoError(t, err)
	memberPlanID, err := st.Plans().Create(ctx, cliPlan("plan-cli-second", "testapp-002", "plan-cli-primary"))
	require.NoError(t, err)
	_, err = st.Plans().Create(ctx, cliPlan("plan-other-second", "testapp-002", "plan-other-primary"))
	require.NoError(t, err)

	cfg := &ServerConfig{
		Databases: map[string]DatabaseConfig{
			"testapp": {
				Type:         storage.DatabaseTypeMySQL,
				Environments: map[string]EnvironmentConfig{"production": multiTargetEnv()},
			},
		},
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := New(st, cfg, map[string]tern.Client{"eu/production": &mockTernClient{isRemote: true}}, logger)
	t.Cleanup(func() {
		utils.CloseAndLog(svc)
	})

	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/apply",
		strings.NewReader(`{"plan_id":"plan-cli-primary","environment":"production","renders_rollout":true}`))
	rec := httptest.NewRecorder()
	svc.handleApply(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp apitypes.ApplyResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.True(t, resp.Accepted, resp.ErrorMessage)
	apply, err := st.Applies().GetByApplyIdentifier(ctx, resp.ApplyID)
	require.NoError(t, err)
	require.NotNil(t, apply)

	ops, err := st.ApplyOperations().ListByApply(ctx, apply.ID)
	require.NoError(t, err)
	planByTarget := map[string]int64{}
	for _, op := range ops {
		planByTarget[op.Target] = op.PlanID
	}
	require.Len(t, planByTarget, 2, "the apply has one operation per target")
	assert.Zero(t, planByTarget["testapp-001"], "the primary runs the plan the apply was created from")
	assert.Equal(t, memberPlanID, planByTarget["testapp-002"], "the second target runs the plan stored for it in this round")
}
