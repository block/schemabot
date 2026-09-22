package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
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
		BootRemovalPolicy:   apitypes.BootRemovalRemoves,
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
	assert.Equal(t, ternv1.BootRemovalPolicy_BOOT_REMOVAL_POLICY_REMOVES, wire.GetBootRemovalPolicy(),
		"a control plane that dropped this would tell an operator their pre-applied state survives a fleet that drops it")

	round := StorageSchemaReportFromProto(wire)
	require.NotNil(t, round)
	assert.Equal(t, report, round)
}

// A data plane that does not set the boot removal policy is reporting that it
// could not say, and the control plane must not read that as preservation.
//
// An instance older than the field leaves it at the wire's zero value while
// still answering everything else, including destructive_allowed. Resolving
// that silence into "your fleet keeps what you pre-apply" is how an operator on
// a deployment that drops it is told to go ahead.
func TestStorageSchemaReport_AnUnsetBootPolicyStaysUnknown(t *testing.T) {
	older := &ternv1.StorageSchemaReport{
		Dialect:            "mysql",
		Database:           "schemabot",
		SchemaSource:       "release v0.1.68",
		DestructiveAllowed: true,
	}

	report := StorageSchemaReportFromProto(older)
	require.NotNil(t, report)
	assert.Equal(t, apitypes.BootRemovalUnknown, report.BootRemovalPolicy)
	assert.NotEqual(t, apitypes.BootRemovalPreserves, report.BootRemovalPolicy,
		"an absent answer is not the reassuring one")
	assert.True(t, report.DestructiveAllowed, "the fields an older release does set still arrive")
	assert.Equal(t, apitypes.BootRemovalUnknown, report.APIType().BootRemovalPolicy,
		"and the unknown survives the hop to the shape the CLI reads")
}

// A policy this binary has no name for is unknown, not preservation. The
// answering side is newer and has a case this one cannot act on, which is the
// same position as being told nothing.
func TestStorageSchemaReport_AnUnrecognizedBootPolicyStaysUnknown(t *testing.T) {
	assert.Equal(t, apitypes.BootRemovalUnknown, bootRemovalPolicyFromProto(ternv1.BootRemovalPolicy(99)))
	assert.Equal(t, ternv1.BootRemovalPolicy_BOOT_REMOVAL_POLICY_UNSPECIFIED,
		bootRemovalPolicyProto(apitypes.BootRemovalPolicy("a policy from a later release")))
}

// A nil wire report converts to a nil report rather than to an empty one. An
// RPC that answered with no report at all is a different condition from one
// that reported convergence, and inventing convergence here would turn a
// broken answer into a green light.
func TestStorageSchemaReport_NilProtoIsNotConvergence(t *testing.T) {
	assert.Nil(t, StorageSchemaReportProto(nil))
	assert.Nil(t, StorageSchemaReportFromProto(nil))
}
