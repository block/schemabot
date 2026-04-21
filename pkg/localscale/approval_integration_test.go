//go:build integration

package localscale_test

import (
	"fmt"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/engine/planetscale"
	"github.com/block/schemabot/pkg/psclient"
	"github.com/block/schemabot/pkg/schema"
)

// TestApply_RequireApproval_ReturnsError verifies that the PlanetScale engine
// returns a clear error when the database has deploy request approvals enabled.
// Uses the shared TestMain container's "approvaldb" database.
func TestApply_RequireApproval_ReturnsError(t *testing.T) {
	ctx := t.Context()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	require.NoError(t, testContainer.SeedVSchema(ctx, testOrg, "approvaldb", "testkeyspace", []byte(`{"sharded": false}`)))
	require.NoError(t, testContainer.SeedDDL(ctx, testOrg, "approvaldb", "testkeyspace",
		"CREATE TABLE users (id bigint NOT NULL PRIMARY KEY, name varchar(255)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"))

	edgeAddr := getEdgeAddr(t, testContainer.URL(), testOrg+"/approvaldb")

	psEngine := planetscale.NewWithClient(logger, func(tokenName, tokenValue string) (psclient.PSClient, error) {
		return psclient.NewPSClientWithBaseURL(tokenName, tokenValue, testContainer.URL())
	})

	schemaFiles := schema.SchemaFiles{
		"testkeyspace": &schema.Namespace{
			Files: map[string]string{
				"users.sql": "CREATE TABLE users (id bigint NOT NULL PRIMARY KEY, name varchar(255), email varchar(255)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
			},
		},
	}

	planResult, err := psEngine.Plan(ctx, &engine.PlanRequest{
		Database:    "approvaldb",
		SchemaFiles: schemaFiles,
		Credentials: &engine.Credentials{
			Organization: testOrg,
			TokenName:    "test",
			TokenValue:   "test",
			DSN:          fmt.Sprintf("root@tcp(%s)/testkeyspace?interpolateParams=true", edgeAddr),
		},
	})
	require.NoError(t, err, "Plan")
	require.False(t, planResult.NoChanges)

	_, err = psEngine.Apply(ctx, &engine.ApplyRequest{
		Database:    "approvaldb",
		Changes:     planResult.Changes,
		SchemaFiles: schemaFiles,
		Credentials: &engine.Credentials{
			Organization: testOrg,
			TokenName:    "test",
			TokenValue:   "test",
			DSN:          fmt.Sprintf("root@tcp(%s)/testkeyspace?interpolateParams=true", edgeAddr),
		},
	})
	require.Error(t, err, "Apply should fail when approvals are required")
	assert.Contains(t, err.Error(), "not supported",
		"error should mention approvals are not supported")
	assert.Contains(t, err.Error(), "Require administrator approval",
		"error should tell user which setting to disable")
}
