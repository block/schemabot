//go:build integration

package localscale_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"

	ps "github.com/planetscale/planetscale-go/planetscale"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/engine/planetscale"
	"github.com/block/schemabot/pkg/psclient"
	"github.com/block/schemabot/pkg/schema"
)

// A PlanetScale plan decorates each keyspace's table changes with display-only
// size context from the branch table metrics API. Operators read these figures
// on the PR to judge how long a change will run, so on a single-keyspace
// branch, where the endpoint's bare table names are attributable, a change to
// an existing table must carry the exact byte figure the endpoint reports —
// while row counts stay absent, because the endpoint reports bytes only.
func TestPlanAttachesTableSizesFromBranchMetrics(t *testing.T) {
	ctx := t.Context()
	const (
		database = singleKeyspaceDB
		keyspace = "testkeyspace"
		table    = "plan_size_items"
	)
	require.NoError(t, testContainer.SeedVSchema(ctx, testOrg, database, keyspace, []byte(`{"sharded": false}`)))
	require.NoError(t, testContainer.SeedDDL(ctx, testOrg, database, keyspace,
		"CREATE TABLE IF NOT EXISTS `"+table+"` (`id` bigint NOT NULL PRIMARY KEY, `name` varchar(255)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"))

	eng := planetscale.NewWithClient(planTestLogger(),
		func(_, _ string) (psclient.PSClient, error) { return testClient, nil })

	result, err := eng.Plan(ctx, &engine.PlanRequest{
		Database:    database,
		Credentials: planCredentials(),
		SchemaFiles: desiredSchemaWithProbeColumn(t, ctx, database, keyspace, table),
	})
	require.NoError(t, err, "plan")
	require.False(t, result.NoChanges, "the probe column must produce a change")

	change := findTableChange(t, result, keyspace, table)
	assert.Zero(t, change.ShardCount, "an unsharded keyspace renders no shard count")

	// The byte figure on the change must be exactly what the branch table
	// metrics endpoint reports for the table, proving the plan read the
	// endpoint rather than inventing a value.
	metrics, err := testClient.BranchTableMetrics(ctx, testOrg, database, "main")
	require.NoError(t, err, "read branch table metrics")
	require.Contains(t, metrics, table, "metrics must cover the seeded table")
	assert.Positive(t, metrics[table], "an InnoDB table reports a non-zero footprint")
	require.NotNil(t, change.EstimatedBytes, "a change to an existing table must carry its byte estimate")
	assert.Equal(t, metrics[table], *change.EstimatedBytes)

	assert.Nil(t, change.EstimatedRows, "the metrics endpoint reports bytes only; row counts stay absent on PlanetScale")
	assert.Nil(t, change.LargestShardRows, "the metrics endpoint has no per-shard breakdown")
}

// On a branch with several keyspaces the metrics endpoint's bare table names
// cannot be attributed to the keyspace that owns them, so the plan carries the
// keyspace's real shard count and withholds the byte estimate rather than
// risk showing another keyspace's table size.
func TestPlanWithholdsTableBytesOnMultiKeyspaceBranch(t *testing.T) {
	ctx := t.Context()
	const keyspace = "testapp_sharded"

	eng := planetscale.NewWithClient(planTestLogger(),
		func(_, _ string) (psclient.PSClient, error) { return testClient, nil })

	result, err := eng.Plan(ctx, &engine.PlanRequest{
		Database:    testDB,
		Credentials: planCredentials(),
		SchemaFiles: desiredSchemaWithProbeColumn(t, ctx, testDB, keyspace, "users"),
	})
	require.NoError(t, err, "plan")
	require.False(t, result.NoChanges, "the probe column must produce a change")

	change := findTableChange(t, result, keyspace, "users")
	assert.Equal(t, 2, change.ShardCount, "testapp_sharded spans two shards")
	assert.Nil(t, change.EstimatedBytes, "bytes are withheld when the branch has more than one keyspace")
}

// Table sizes are display-only plan context, so a missing or failing metrics
// endpoint must never fail the plan: the plan still computes its changes,
// each change still carries the shard count from the keyspace listing, and
// only the byte estimate is omitted.
func TestPlanSucceedsWithoutBranchTableMetrics(t *testing.T) {
	ctx := t.Context()
	const keyspace = "testapp_sharded"

	eng := planetscale.NewWithClient(planTestLogger(),
		func(_, _ string) (psclient.PSClient, error) {
			return metricsUnavailableClient{testClient}, nil
		})

	result, err := eng.Plan(ctx, &engine.PlanRequest{
		Database:    testDB,
		Credentials: planCredentials(),
		SchemaFiles: desiredSchemaWithProbeColumn(t, ctx, testDB, keyspace, "users"),
	})
	require.NoError(t, err, "plan must succeed with the metrics endpoint failing")
	require.False(t, result.NoChanges, "the probe column must produce a change")

	change := findTableChange(t, result, keyspace, "users")
	assert.Equal(t, 2, change.ShardCount, "shard counts come from the keyspace listing, not the metrics endpoint")
	assert.Nil(t, change.EstimatedBytes, "no byte estimate without the metrics endpoint")
	assert.Nil(t, change.EstimatedRows, "row counts stay absent on PlanetScale")
}

// A failing keyspace listing leaves the plan without any size context, and
// still never fails it: the change is planned exactly as it would be with the
// API healthy, carrying no shard count and no byte estimate.
func TestPlanSucceedsWithoutKeyspaceListing(t *testing.T) {
	ctx := t.Context()
	const keyspace = "testapp_sharded"

	eng := planetscale.NewWithClient(planTestLogger(),
		func(_, _ string) (psclient.PSClient, error) {
			return keyspaceListingUnavailableClient{testClient}, nil
		})

	result, err := eng.Plan(ctx, &engine.PlanRequest{
		Database:    testDB,
		Credentials: planCredentials(),
		SchemaFiles: desiredSchemaWithProbeColumn(t, ctx, testDB, keyspace, "users"),
	})
	require.NoError(t, err, "plan must succeed with the keyspace listing failing")
	require.False(t, result.NoChanges, "the probe column must produce a change")

	change := findTableChange(t, result, keyspace, "users")
	assert.Contains(t, change.DDL, "ADD COLUMN `plan_size_probe_col`", "the planned change is unaffected by the missing size context")
	assert.Zero(t, change.ShardCount, "no shard count without the keyspace listing")
	assert.Nil(t, change.EstimatedBytes, "no byte estimate without the keyspace listing")
}

// keyspaceListingUnavailableClient serves every PlanetScale API call from the
// real LocalScale-backed client except the keyspace listing, which fails.
type keyspaceListingUnavailableClient struct {
	psclient.PSClient
}

func (c keyspaceListingUnavailableClient) ListKeyspaces(_ context.Context, req *ps.ListKeyspacesRequest) ([]*ps.Keyspace, error) {
	return nil, fmt.Errorf("keyspace listing unavailable for %s/%s branch %s", req.Organization, req.Database, req.Branch)
}

// singleKeyspaceDB is the shared container's database with exactly one
// keyspace, the only topology on which branch table bytes are attributable.
const singleKeyspaceDB = "approvaldb"

// metricsUnavailableClient serves every PlanetScale API call from the real
// LocalScale-backed client except the branch table metrics endpoint, which
// fails — standing in for an API deployment that does not serve
// /metrics/tables.
type metricsUnavailableClient struct {
	psclient.PSClient
}

func (c metricsUnavailableClient) BranchTableMetrics(_ context.Context, org, database, branch string) (map[string]int64, error) {
	return nil, fmt.Errorf("table metrics unavailable for %s/%s branch %s", org, database, branch)
}

func planTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
}

// planCredentials returns credentials for planning against the main branch.
// LocalScale reports safe schema changes enabled, so the plan reads current
// schema through the PlanetScale schema API and needs no vtgate DSN.
func planCredentials() *engine.Credentials {
	return &engine.Credentials{
		Metadata: map[string]string{
			"organization": testOrg,
			"main_branch":  "main",
			"token_name":   "test",
			"token_value":  "test",
		},
	}
}

// desiredSchemaWithProbeColumn snapshots the keyspace's live schema and adds
// one column to the named table, so the plan diffs to exactly one ALTER on an
// existing, seeded table regardless of what earlier tests changed on the
// shared container.
func desiredSchemaWithProbeColumn(t *testing.T, ctx context.Context, database, keyspace, tableName string) schema.SchemaFiles {
	t.Helper()
	current, err := testClient.GetBranchSchema(ctx, &ps.BranchSchemaRequest{
		Organization: testOrg,
		Database:     database,
		Branch:       "main",
		Keyspace:     keyspace,
	})
	require.NoError(t, err, "snapshot current schema for %s", keyspace)
	require.NotEmpty(t, current, "keyspace %s must have tables to diff against", keyspace)

	files := make(map[string]string, len(current))
	found := false
	for _, tbl := range current {
		raw := tbl.Raw
		if tbl.Name == tableName {
			// The first "(" opens the column list; a fresh leading column is
			// the smallest desired-schema edit that diffs to one ALTER.
			raw = strings.Replace(raw, "(", "(\n  `plan_size_probe_col` varchar(32) DEFAULT NULL,", 1)
			found = true
		}
		files[tbl.Name+".sql"] = raw
	}
	require.True(t, found, "table %s must exist in keyspace %s", tableName, keyspace)
	return schema.SchemaFiles{keyspace: {Files: files}}
}

// findTableChange returns the single planned change for the given table in the
// given keyspace, failing the test if it is absent.
func findTableChange(t *testing.T, result *engine.PlanResult, keyspace, tableName string) engine.TableChange {
	t.Helper()
	for _, sc := range result.Changes {
		if sc.Namespace != keyspace {
			continue
		}
		for _, tc := range sc.TableChanges {
			if tc.Table == tableName {
				return tc
			}
		}
	}
	require.Failf(t, "planned change not found", "no change for table %s in keyspace %s", tableName, keyspace)
	return engine.TableChange{}
}
