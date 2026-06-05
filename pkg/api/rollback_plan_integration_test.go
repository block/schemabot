//go:build integration

package api

import (
	"database/sql"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/block/spirit/pkg/utils"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
	"github.com/block/schemabot/pkg/testutil"
)

func TestExecuteRollbackPlanForApplyUsesRequestedApplyOriginalFiles(t *testing.T) {
	ctx := t.Context()
	container, err := mysql.Run(ctx,
		"mysql:8.4",
		mysql.WithDatabase("schemabot_test"),
		mysql.WithUsername("root"),
		mysql.WithPassword("test"),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, testcontainers.TerminateContainer(container))
	})

	dsn, err := testutil.ContainerConnectionString(ctx, container, "parseTime=true")
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	require.NoError(t, EnsureSchema(dsn, logger))

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(ctx))
	t.Cleanup(func() {
		utils.CloseAndLog(db)
	})

	st := mysqlstore.New(db)
	mock := &mockTernClient{
		planResp: &ternv1.PlanResponse{
			PlanId: "plan-rollback-requested-apply",
			Engine: ternv1.Engine_ENGINE_SPIRIT,
			Changes: []*ternv1.SchemaChange{
				{
					Namespace: "shop",
					TableChanges: []*ternv1.TableChange{
						{
							TableName:  "users",
							Ddl:        "ALTER TABLE `users` DROP COLUMN `email`",
							ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
						},
					},
					OriginalFiles: map[string]string{
						"users.sql": "CREATE TABLE `users` (`id` int NOT NULL, `email` varchar(255), PRIMARY KEY (`id`)) ENGINE=InnoDB",
					},
					OriginalFilesCaptured: true,
				},
			},
		},
	}
	svc := New(st, testServerConfig(), map[string]tern.Client{"default/staging": mock}, logger)
	t.Cleanup(func() {
		utils.CloseAndLog(svc)
	})

	oldPlanID, err := st.Plans().Create(ctx, &storage.Plan{
		PlanIdentifier: "plan-original-target",
		Database:       "shop",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     DefaultDeployment,
		Target:         "shop-target",
		Repository:     "octocat/hello-world",
		PullRequest:    1,
		Environment:    "staging",
		Namespaces: map[string]*storage.NamespacePlanData{
			"shop": {
				OriginalFiles: map[string]string{
					"users.sql": "CREATE TABLE `users` (`id` int NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB",
				},
				OriginalFilesCaptured: true,
			},
		},
		CreatedAt: time.Now().Add(-2 * time.Hour),
	})
	require.NoError(t, err)

	newerPlanID, err := st.Plans().Create(ctx, &storage.Plan{
		PlanIdentifier: "plan-newer-completed",
		Database:       "shop",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     DefaultDeployment,
		Target:         "shop-target",
		Repository:     "octocat/hello-world",
		PullRequest:    1,
		Environment:    "staging",
		Namespaces: map[string]*storage.NamespacePlanData{
			"shop": {
				OriginalFiles: map[string]string{
					"users.sql": "CREATE TABLE `users` (`id` int NOT NULL, `email` varchar(255), PRIMARY KEY (`id`)) ENGINE=InnoDB",
				},
				OriginalFilesCaptured: true,
			},
		},
		CreatedAt: time.Now().Add(-1 * time.Hour),
	})
	require.NoError(t, err)

	oldCompleted := time.Now().Add(-90 * time.Minute)
	newerCompleted := time.Now().Add(-30 * time.Minute)
	requestedApplyID, err := st.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply-requested",
		PlanID:          oldPlanID,
		Database:        "shop",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Repository:      "octocat/hello-world",
		PullRequest:     1,
		Environment:     "staging",
		Deployment:      DefaultDeployment,
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Completed,
		Options:         []byte("{}"),
		CompletedAt:     &oldCompleted,
	})
	require.NoError(t, err)
	_, err = st.Applies().Create(ctx, &storage.Apply{
		ApplyIdentifier: "apply-newer",
		PlanID:          newerPlanID,
		Database:        "shop",
		DatabaseType:    storage.DatabaseTypeMySQL,
		Repository:      "octocat/hello-world",
		PullRequest:     1,
		Environment:     "staging",
		Deployment:      DefaultDeployment,
		Engine:          storage.EngineSpirit,
		State:           state.Apply.Completed,
		Options:         []byte("{}"),
		CompletedAt:     &newerCompleted,
	})
	require.NoError(t, err)

	requestedApply, err := st.Applies().Get(ctx, requestedApplyID)
	require.NoError(t, err)

	resp, err := svc.ExecuteRollbackPlanForApply(ctx, requestedApply)
	require.NoError(t, err)
	require.Equal(t, "plan-rollback-requested-apply", resp.PlanID)

	require.NotNil(t, mock.planReq)
	assert.Equal(t, "shop", mock.planReq.Database)
	assert.Equal(t, storage.DatabaseTypeMySQL, mock.planReq.Type)
	assert.Equal(t, "staging", mock.planReq.Environment)
	assert.Equal(t, "shop-target", mock.planReq.Target)
	assert.Equal(t, "CREATE TABLE `users` (`id` int NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB",
		mock.planReq.SchemaFiles["shop"].Files["users.sql"])

	storedRollbackPlan, err := st.Plans().Get(ctx, "plan-rollback-requested-apply")
	require.NoError(t, err)
	require.NotNil(t, storedRollbackPlan)
	assert.Equal(t, "staging", storedRollbackPlan.Environment)
	assert.Equal(t, DefaultDeployment, storedRollbackPlan.Deployment)
	assert.Equal(t, "shop-target", storedRollbackPlan.Target)
	assert.Equal(t, "CREATE TABLE `users` (`id` int NOT NULL, `email` varchar(255), PRIMARY KEY (`id`)) ENGINE=InnoDB",
		storedRollbackPlan.Namespaces["shop"].OriginalFiles["users.sql"])
}
