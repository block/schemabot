package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/schema"
)

// A report crosses the wire to a control plane and back without losing
// anything: the control plane renders what the data plane decided, so a field
// dropped in conversion would be a statement an operator never sees.
func TestStorageSchemaReport_ProtoRoundTrip(t *testing.T) {
	report := &StorageSchemaReport{
		Dialect:      schema.DialectPostgres,
		Database:     "schemabot",
		Host:         "storage.db.example:5432",
		SchemaSource: "release v0.1.68",
		Version:      "v0.1.67",
		Outstanding: []StorageSchemaStatement{{
			Table:     "apply_operations",
			Operation: postgresOpAddColumn,
			DDL:       `ALTER TABLE "apply_operations" ADD COLUMN "operation_kind" varchar(32) NOT NULL DEFAULT 'work'`,
		}},
		Destructive: []StorageSchemaStatement{{
			Table:     "vitess_tasks",
			Operation: storageSchemaOpDropTable,
			DDL:       "DROP TABLE `vitess_tasks`",
			Reason:    "DROP TABLE destroys data",
		}},
		DestructiveAllowed: true,
		Manual: []StorageSchemaStatement{{
			Table:     "checks",
			Operation: postgresOpAddColumn,
			DDL:       `ALTER TABLE "checks" ADD COLUMN "head_sha" varchar(64) NOT NULL`,
			Reason:    "column is NOT NULL without a DEFAULT",
		}},
		ConvergenceInFlight: true,
	}

	wire := StorageSchemaReportProto(report)
	require.NotNil(t, wire)

	// Named field by field, and not by the round trip alone: a round trip is
	// blind to any swap the two conversions make symmetrically, and the swap
	// that matters here is a refused DROP arriving under `outstanding`, which
	// an operator reads as a statement that runs on its own.
	assert.Equal(t, "postgres", wire.GetDialect())
	assert.Equal(t, "schemabot", wire.GetDatabase())
	assert.Equal(t, "storage.db.example:5432", wire.GetHost())
	assert.Equal(t, "release v0.1.68", wire.GetSchemaSource())
	assert.Equal(t, "v0.1.67", wire.GetVersion())
	assert.True(t, wire.GetDestructiveAllowed())
	require.Len(t, wire.GetOutstanding(), 1)
	assert.Equal(t, "apply_operations", wire.GetOutstanding()[0].GetTable())
	assert.Equal(t, postgresOpAddColumn, wire.GetOutstanding()[0].GetOperation())
	assert.Contains(t, wire.GetOutstanding()[0].GetDdl(), "operation_kind")
	assert.Empty(t, wire.GetOutstanding()[0].GetReason(), "a statement that runs has nothing to explain")
	require.Len(t, wire.GetDestructive(), 1)
	assert.Equal(t, "vitess_tasks", wire.GetDestructive()[0].GetTable())
	assert.Equal(t, storageSchemaOpDropTable, wire.GetDestructive()[0].GetOperation())
	assert.Equal(t, "DROP TABLE destroys data", wire.GetDestructive()[0].GetReason())
	require.Len(t, wire.GetManual(), 1)
	assert.Equal(t, "checks", wire.GetManual()[0].GetTable())
	assert.Equal(t, "column is NOT NULL without a DEFAULT", wire.GetManual()[0].GetReason())
	assert.True(t, wire.GetConvergenceInFlight(),
		"a control plane that dropped this would render a mid-convergence database as an idle one")

	round := StorageSchemaReportFromProto(wire)
	require.NotNil(t, round)
	assert.Equal(t, report, round)
}

// A nil wire report converts to a nil report rather than to an empty one. An
// RPC that answered with no report at all is a different condition from one
// that reported convergence, and inventing convergence here would turn a
// broken answer into a green light.
func TestStorageSchemaReport_NilProtoIsNotConvergence(t *testing.T) {
	assert.Nil(t, StorageSchemaReportProto(nil))
	assert.Nil(t, StorageSchemaReportFromProto(nil))
}
