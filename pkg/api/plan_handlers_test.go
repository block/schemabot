package api

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
)

func TestBuildApplyTaskCopiesExecutionVerdict(t *testing.T) {
	change := storage.TableChange{
		Namespace: "testdb", Table: "orders", Operation: "alter",
		ExecutionMode: "blocked", ModeReason: "requires privileges unavailable to the engine",
	}
	task := buildApplyTask(&storage.Plan{ID: 7, DatabaseType: storage.DatabaseTypeMySQL}, change, "production", storage.ApplyOptions{}, "", time.Now())

	assert.Equal(t, "blocked", task.ExecutionMode)
	assert.Equal(t, "requires privileges unavailable to the engine", task.ModeReason)
}

// Apply tasks carry the table's planned size and row count so progress can
// show them beside the row counts, and before the copy reports any. A task
// created for one shard of a table carries neither, and a drop carries the
// table's size but no rows, since it copies none.
func TestBuildApplyTaskCarriesPlanEstimates(t *testing.T) {
	bytes := int64(23_400_000_000)
	rows := int64(48_200_000)
	change := storage.TableChange{Namespace: "testdb", Table: "orders", Operation: "alter", EstimatedBytes: &bytes, EstimatedRows: &rows}
	plan := &storage.Plan{ID: 7, DatabaseType: storage.DatabaseTypeMySQL}

	task := buildApplyTask(plan, change, "production", storage.ApplyOptions{}, "", time.Now())
	require.NotNil(t, task.EstimatedBytes)
	assert.Equal(t, bytes, *task.EstimatedBytes)
	require.NotNil(t, task.EstimatedRows)
	assert.Equal(t, rows, *task.EstimatedRows)

	shardTask := buildApplyTask(plan, change, "production", storage.ApplyOptions{}, "-80", time.Now())
	assert.Nil(t, shardTask.EstimatedBytes)
	assert.Nil(t, shardTask.EstimatedRows)

	drop := change
	drop.Operation = "drop"
	dropTask := buildApplyTask(plan, drop, "production", storage.ApplyOptions{}, "", time.Now())
	require.NotNil(t, dropTask.EstimatedBytes)
	assert.Equal(t, bytes, *dropTask.EstimatedBytes)
	assert.Nil(t, dropTask.EstimatedRows, "a drop copies no rows")
}

// TestPullNamespaces exercises the API-boundary pull gate: requested
// namespaces must be well-formed, concrete, unique, and not reserved for the
// target's dialect. A system or platform-managed schema must be rejected on
// its own dialect, while the same name is a legal user namespace on the other
// dialect.
func TestPullNamespaces(t *testing.T) {
	tests := []struct {
		name       string
		dialect    schema.Dialect
		namespaces []string
		want       []string
		wantErr    string
	}{
		{name: "no namespaces pulls the default namespace", dialect: schema.DialectMySQL, namespaces: nil, want: []string{""}},
		{name: "app namespaces pass through in order", dialect: schema.DialectMySQL, namespaces: []string{"orders", "billing"}, want: []string{"orders", "billing"}},

		// Reserved namespaces are rejected per dialect.
		{name: "mysql system schema rejected on mysql", dialect: schema.DialectMySQL, namespaces: []string{"sys"}, wantErr: "reserved and cannot be pulled"},
		{name: "aws-managed schema rejected on postgres", dialect: schema.DialectPostgres, namespaces: []string{"aws_s3"}, wantErr: "reserved and cannot be pulled"},
		{name: "pg_catalog rejected on postgres", dialect: schema.DialectPostgres, namespaces: []string{"pg_catalog"}, wantErr: "reserved and cannot be pulled"},

		// A namespace reserved on one dialect is a legal user namespace on the other.
		{name: "mysql is a legal user namespace on postgres", dialect: schema.DialectPostgres, namespaces: []string{"mysql"}, want: []string{"mysql"}},
		{name: "aws_s3 is a legal user namespace on mysql", dialect: schema.DialectMySQL, namespaces: []string{"aws_s3"}, want: []string{"aws_s3"}},

		// Shape validation is dialect-independent.
		{name: "empty namespace rejected", dialect: schema.DialectMySQL, namespaces: []string{""}, wantErr: "non-empty"},
		{name: "surrounding whitespace rejected", dialect: schema.DialectMySQL, namespaces: []string{" orders "}, wantErr: "whitespace"},
		{name: "path traversal rejected", dialect: schema.DialectMySQL, namespaces: []string{"a..b"}, wantErr: "single path component"},
		{name: "path separator rejected", dialect: schema.DialectMySQL, namespaces: []string{`a/b`}, wantErr: "single path component"},
		{name: "unresolved env placeholder rejected", dialect: schema.DialectMySQL, namespaces: []string{"orders_$ENV"}, wantErr: "$ENV"},
		{name: "unresolved brace env placeholder rejected", dialect: schema.DialectMySQL, namespaces: []string{"orders_{env}"}, wantErr: "{env}"},
		{name: "duplicate namespace rejected", dialect: schema.DialectMySQL, namespaces: []string{"orders", "orders"}, wantErr: "duplicate"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := pullNamespaces(tc.dialect, tc.namespaces)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// A pull request apply of the whole rollout holds back the members whose own
// plans cannot run and runs every other one, in rollout order. It never holds
// back the primary, which the apply is laid out from, never holds back a
// member outside the rollout, and an apply narrowed to one member holds back
// nothing.
func TestWithoutHeldMembers(t *testing.T) {
	plan := &storage.Plan{Database: "orders"}
	targets := []routing.ExecutionTarget{
		{Deployment: "eu", Target: "orders-001"},
		{Deployment: "us", Target: "orders-002"},
		{Deployment: "ap", Target: "orders-003"},
	}
	held := func(members ...string) ApplyRequest {
		req := ApplyRequest{Environment: "production"}
		for _, member := range members {
			req.HeldMembers = append(req.HeldMembers, storage.HeldMember{Member: member, Reason: "its plan cannot run"})
		}
		return req
	}

	kept, err := withoutHeldMembers(plan, held(), targets, "")
	require.NoError(t, err)
	assert.Equal(t, targets, kept, "an apply holding nothing back runs every member")

	kept, err = withoutHeldMembers(plan, held("us/orders-002"), targets, "")
	require.NoError(t, err)
	assert.Equal(t, []routing.ExecutionTarget{targets[0], targets[2]}, kept)

	_, err = withoutHeldMembers(plan, held("eu/orders-001"), targets, "")
	require.ErrorContains(t, err, "holds back its primary target eu/orders-001")

	_, err = withoutHeldMembers(plan, held("us/orders-009"), targets, "")
	require.ErrorContains(t, err, "holds back [us/orders-009], which are not members of its rollout")

	_, err = withoutHeldMembers(plan, held("us/orders-002"), targets[1:2], "us/orders-002")
	require.ErrorContains(t, err, "a narrowed apply runs one member and holds back nothing")
}
