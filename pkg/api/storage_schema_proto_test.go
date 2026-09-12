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
		Dialect:  schema.DialectPostgres,
		Database: "schemabot",
		Version:  "v0.1.67",
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
	}

	round := StorageSchemaReportFromProto(StorageSchemaReportProto(report))
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
