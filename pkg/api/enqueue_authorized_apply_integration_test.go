//go:build integration

package api

import (
	"database/sql"
	"errors"
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

	"github.com/block/schemabot/pkg/auth"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
	"github.com/block/schemabot/pkg/testutil"
)

// A data-plane host queues control-plane-authorized dispatches through
// EnqueueAuthorizedApply against real storage. The queued apply must be durable (apply,
// task, and operation rows committed in one transaction), claimable by an
// operator driver, and covered by the one-active-apply-per-target invariant —
// while the gated ExecuteApply path on the same service keeps failing closed
// because the deployment has no database config to evaluate source policy
// against.
func TestEnqueueAuthorizedApplyQueuesDurableApplyAgainstStorage(t *testing.T) {
	ctx := t.Context()

	container, err := mysql.Run(ctx,
		"mysql:8.0",
		mysql.WithDatabase("schemabot_test"),
		mysql.WithUsername("root"),
		mysql.WithPassword("test"),
	)
	require.NoError(t, err, "failed to start mysql")
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	})

	dsn, err := testutil.ContainerConnectionString(ctx, container, "parseTime=true")
	require.NoError(t, err, "failed to get connection string")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	require.NoError(t, EnsureSchema(dsn, logger), "failed to ensure schema")

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "failed to open database")
	require.NoError(t, db.PingContext(ctx), "failed to ping database")
	t.Cleanup(func() { utils.CloseAndLog(db) })

	stor := mysqlstore.New(db)

	plan := &storage.Plan{
		PlanIdentifier: "plan-enqueue-integration",
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "default",
		Target:         "testdb-staging-target",
		Repository:     "octocat/hello-world",
		PullRequest:    1,
		SchemaPath:     "schema/testdb",
		Environment:    "staging",
		Namespaces: map[string]*storage.NamespacePlanData{
			"testdb": {
				Tables: []storage.TableChange{
					{
						Namespace: "testdb",
						Table:     "users",
						DDL:       "ALTER TABLE users ADD COLUMN email varchar(255)",
						Operation: "alter",
					},
				},
			},
		},
		CreatedAt: time.Now(),
	}
	_, err = stor.Plans().Create(ctx, plan)
	require.NoError(t, err, "failed to store trusted plan")

	cfg := &ServerConfig{
		TernDeployments: TernConfig{
			"default": {"staging": "localhost:9090"},
		},
	}
	mockClient := &mockTernClient{isRemote: true}
	svc := New(stor, cfg, map[string]tern.Client{"default/staging": mockClient}, logger)

	// The gated path fails closed: this deployment has no database config to
	// evaluate the trusted plan's source against.
	_, _, err = svc.ExecuteApply(ctx, ApplyRequest{
		PlanID:      plan.PlanIdentifier,
		Environment: "staging",
	})
	var policyErr *SourcePolicyError
	require.True(t, errors.As(err, &policyErr), "ExecuteApply must fail closed without database config")
	assert.Equal(t, SourcePolicyReasonMissingDatabaseConfig, policyErr.Reason)

	resp, applyID, err := svc.EnqueueAuthorizedApply(ctx, ApplyRequest{
		PlanID:      plan.PlanIdentifier,
		Environment: "staging",
		Caller:      "control-plane",
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.True(t, resp.Accepted)
	require.Positive(t, applyID)

	apply, err := stor.Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, apply)
	assert.Equal(t, state.Apply.Pending, apply.State)
	assert.Equal(t, "testdb", apply.Database)
	assert.Equal(t, "staging", apply.Environment)
	assert.Equal(t, "default", apply.Deployment)
	assert.Equal(t, "testdb-staging-target", apply.GetOptions().Target)
	assert.Equal(t, resp.ApplyID, apply.ApplyIdentifier)

	tasks, err := stor.Tasks().GetByApplyID(ctx, applyID)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, state.Task.Pending, tasks[0].State)
	assert.Equal(t, "users", tasks[0].TableName)
	assert.Contains(t, tasks[0].DDL, "ADD COLUMN")
	assert.Contains(t, tasks[0].DDL, "email")

	operations, err := stor.ApplyOperations().ListByApply(ctx, applyID)
	require.NoError(t, err)
	require.Len(t, operations, 1)
	assert.Equal(t, "default", operations[0].Deployment)
	assert.Equal(t, "testdb-staging-target", operations[0].Target)

	// One active apply per target: a second dispatch is rejected while the
	// first is still pending.
	_, _, err = svc.EnqueueAuthorizedApply(ctx, ApplyRequest{
		PlanID:      plan.PlanIdentifier,
		Environment: "staging",
	})
	require.ErrorIs(t, err, storage.ErrActiveApplyExists)

	// The committed apply and task rows make the queued apply claimable by an
	// operator driver.
	claimed, err := stor.Applies().FindNextApply(ctx, "driver-test")
	require.NoError(t, err)
	require.NotNil(t, claimed, "queued apply must be operator-claimable")
	assert.Equal(t, applyID, claimed.ID)
	assert.Equal(t, state.Apply.Pending, claimed.State)
}

// When API auth is enabled, an authorized write persists an apply attributed to
// the authenticated caller — not the client-supplied Caller. This is the
// storage-backed counterpart to the tier checks: it confirms a write-authorized
// request actually creates a durable apply row, and that a stamp-free break-glass
// write is auditable to the verified identity.
func TestEnqueueAuthorizedApplyRecordsAuthenticatedCaller(t *testing.T) {
	ctx := t.Context()

	container, err := mysql.Run(ctx,
		"mysql:8.4",
		mysql.WithDatabase("schemabot_test"),
		mysql.WithUsername("root"),
		mysql.WithPassword("test"),
	)
	require.NoError(t, err, "failed to start mysql")
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	})

	dsn, err := testutil.ContainerConnectionString(ctx, container, "parseTime=true")
	require.NoError(t, err, "failed to get connection string")

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	require.NoError(t, EnsureSchema(dsn, logger), "failed to ensure schema")

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "failed to open database")
	require.NoError(t, db.PingContext(ctx), "failed to ping database")
	t.Cleanup(func() { utils.CloseAndLog(db) })

	stor := mysqlstore.New(db)

	plan := &storage.Plan{
		PlanIdentifier: "plan-authenticated-caller",
		Database:       "testdb",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Deployment:     "default",
		Target:         "testdb-staging-target",
		Repository:     "octocat/hello-world",
		PullRequest:    1,
		SchemaPath:     "schema/testdb",
		Environment:    "staging",
		Namespaces: map[string]*storage.NamespacePlanData{
			"testdb": {
				Tables: []storage.TableChange{
					{
						Namespace: "testdb",
						Table:     "users",
						DDL:       "ALTER TABLE users ADD COLUMN email varchar(255)",
						Operation: "alter",
					},
				},
			},
		},
		CreatedAt: time.Now(),
	}
	_, err = stor.Plans().Create(ctx, plan)
	require.NoError(t, err, "failed to store plan")

	cfg := &ServerConfig{
		TernDeployments: TernConfig{"default": {"staging": "localhost:9090"}},
	}
	svc := New(stor, cfg, map[string]tern.Client{"default/staging": &mockTernClient{isRemote: true}}, logger)

	// A request from an authenticated caller (as the auth middleware would set)
	// carrying a different, untrusted Caller in the body — the authenticated
	// identity must win.
	authCtx := auth.WithUser(ctx, &auth.User{Subject: "bob@example.com"})
	resp, applyID, err := svc.EnqueueAuthorizedApply(authCtx, ApplyRequest{
		PlanID:      plan.PlanIdentifier,
		Environment: "staging",
		Caller:      "client-supplied-should-be-ignored",
	})
	require.NoError(t, err)
	require.True(t, resp.Accepted)
	require.Positive(t, applyID)

	// Storage confirmation: the apply row is durable, and its caller is the
	// authenticated subject rather than the client-supplied request Caller.
	apply, err := stor.Applies().Get(ctx, applyID)
	require.NoError(t, err)
	require.NotNil(t, apply)
	assert.Equal(t, "bob@example.com", apply.Caller,
		"apply must be attributed to the authenticated caller, not the client-supplied Caller")
}
