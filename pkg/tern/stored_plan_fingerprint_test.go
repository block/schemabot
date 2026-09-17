package tern

import (
	"testing"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	storedAddEmail = "ALTER TABLE `customers` ADD COLUMN `email` VARCHAR(255)"
	storedAddZip   = "ALTER TABLE `customers` ADD COLUMN `zip` VARCHAR(16)"
)

func storedAlter(ddl string) storage.TableChange {
	return storage.TableChange{Namespace: "shop", Table: "customers", DDL: ddl, Operation: "alter"}
}

func storedPlanOf(namespaces map[string]*storage.NamespacePlanData, shards []storage.ShardPlan) *storage.Plan {
	return &storage.Plan{
		ID:             1,
		PlanIdentifier: "plan_test",
		DatabaseType:   storage.DatabaseTypeMySQL,
		Namespaces:     namespaces,
		Shards:         shards,
	}
}

func fingerprintOf(t *testing.T, plan *storage.Plan) string {
	t.Helper()
	fingerprint, err := StoredPlanFingerprint(schema.DialectMySQL, plan)
	require.NoError(t, err)
	return fingerprint
}

// Two plans that would run the same change set key the same, so a caller can
// group the members running them without comparing every pair.
func TestStoredPlanFingerprint_SameWorkKeysTheSame(t *testing.T) {
	a := storedPlanOf(map[string]*storage.NamespacePlanData{
		"shop": {Tables: []storage.TableChange{storedAlter(storedAddEmail)}},
	}, nil)
	b := storedPlanOf(map[string]*storage.NamespacePlanData{
		"shop": {Tables: []storage.TableChange{storedAlter(storedAddEmail)}},
	}, nil)

	assert.Equal(t, fingerprintOf(t, a), fingerprintOf(t, b))
}

// A plan carrying a change its sibling does not keys differently, so members
// that really do run different work are never grouped together.
func TestStoredPlanFingerprint_ExtraChangeKeysDifferently(t *testing.T) {
	a := storedPlanOf(map[string]*storage.NamespacePlanData{
		"shop": {Tables: []storage.TableChange{storedAlter(storedAddEmail)}},
	}, nil)
	b := storedPlanOf(map[string]*storage.NamespacePlanData{
		"shop": {Tables: []storage.TableChange{storedAlter(storedAddEmail), storedAlter(storedAddZip)}},
	}, nil)

	assert.NotEqual(t, fingerprintOf(t, a), fingerprintOf(t, b))
}

// The key is the canonicalized change set, not the DDL as written, so two plans
// that spell one change differently are still one change set.
func TestStoredPlanFingerprint_KeysTheChangeNotItsSpelling(t *testing.T) {
	a := storedPlanOf(map[string]*storage.NamespacePlanData{
		"shop": {Tables: []storage.TableChange{storedAlter(storedAddEmail)}},
	}, nil)
	b := storedPlanOf(map[string]*storage.NamespacePlanData{
		"shop": {Tables: []storage.TableChange{storedAlter("alter table customers add column email varchar(255)")}},
	}, nil)

	assert.Equal(t, fingerprintOf(t, a), fingerprintOf(t, b))
}

// A sharded namespace is keyed off its shard rows, which are its authoritative
// representation. The namespace-collapsed view of the same changes dedupes
// tables across shards, so keying off it would call two rollouts identical that
// change a different number of shards.
func TestStoredPlanFingerprint_ShardedNamespaceKeysOffItsShards(t *testing.T) {
	collapsed := []storage.TableChange{storedAlter(storedAddEmail)}
	twoShards := storedPlanOf(
		map[string]*storage.NamespacePlanData{"shop": {Tables: collapsed}},
		[]storage.ShardPlan{
			{Shard: "-80", Namespace: "shop", Changes: []storage.TableChange{storedAlter(storedAddEmail)}},
			{Shard: "80-", Namespace: "shop", Changes: []storage.TableChange{storedAlter(storedAddEmail)}},
		},
	)
	oneShard := storedPlanOf(
		map[string]*storage.NamespacePlanData{"shop": {Tables: collapsed}},
		[]storage.ShardPlan{
			{Shard: "-80", Namespace: "shop", Changes: []storage.TableChange{storedAlter(storedAddEmail)}},
		},
	)

	assert.NotEqual(t, fingerprintOf(t, twoShards), fingerprintOf(t, oneShard))
}

// A namespace whose VSchema changes keys differently from one whose does not,
// even when the table DDL matches: the VSchema change is work the other member
// is not doing.
func TestStoredPlanFingerprint_VSchemaChangeKeysDifferently(t *testing.T) {
	tables := []storage.TableChange{storedAlter(storedAddEmail)}
	withVSchema := storedPlanOf(map[string]*storage.NamespacePlanData{
		"shop": {Tables: tables, Metadata: map[string]string{storage.PlanMetadataVSchemaChanged: "true"}},
	}, nil)
	withoutVSchema := storedPlanOf(map[string]*storage.NamespacePlanData{
		"shop": {Tables: tables},
	}, nil)

	assert.NotEqual(t, fingerprintOf(t, withVSchema), fingerprintOf(t, withoutVSchema))
}

// A stored plan keys to the same value as the change set it was stored from, so
// a caller reading plan rows and a caller holding the live diff agree about
// which members run the same work.
func TestStoredPlanFingerprint_AgreesWithTheChangeSetItWasStoredFrom(t *testing.T) {
	stored := storedPlanOf(map[string]*storage.NamespacePlanData{
		"shop": {Tables: []storage.TableChange{storedAlter(storedAddEmail), storedAlter(storedAddZip)}},
	}, nil)
	live := ChangeSet{Changes: []*ternv1.SchemaChange{{
		Namespace: "shop",
		TableChanges: []*ternv1.TableChange{
			{TableName: "customers", Ddl: storedAddEmail, ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER},
			{TableName: "customers", Ddl: storedAddZip, ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER},
		},
	}}}

	liveFingerprint, err := ChangeSetFingerprint(schema.DialectMySQL, live)
	require.NoError(t, err)

	assert.Equal(t, liveFingerprint, fingerprintOf(t, stored))
}

// The stored operation is part of the key, not decoration the DDL could stand
// in for. It is the engine's own classification round-tripped through storage,
// and the live side prefers that classification to the parser's — so a plan read
// back from storage keys off it too, rather than off whatever the parser would
// make of the same statement.
func TestStoredPlanFingerprint_KeysOffTheStoredOperation(t *testing.T) {
	asAlter := storedPlanOf(map[string]*storage.NamespacePlanData{
		"shop": {Tables: []storage.TableChange{{Namespace: "shop", Table: "customers", DDL: storedAddEmail, Operation: "alter"}}},
	}, nil)
	asCreate := storedPlanOf(map[string]*storage.NamespacePlanData{
		"shop": {Tables: []storage.TableChange{{Namespace: "shop", Table: "customers", DDL: storedAddEmail, Operation: "create"}}},
	}, nil)

	assert.NotEqual(t, fingerprintOf(t, asAlter), fingerprintOf(t, asCreate))
}

// A plan whose DDL cannot be canonicalized is reported as unkeyable rather than
// keyed on something weaker, so a caller never groups it with a plan it was
// never compared against.
func TestStoredPlanFingerprint_UnparseableDDLErrors(t *testing.T) {
	plan := storedPlanOf(map[string]*storage.NamespacePlanData{
		"shop": {Tables: []storage.TableChange{storedAlter("this is not DDL")}},
	}, nil)

	_, err := StoredPlanFingerprint(schema.DialectMySQL, plan)

	require.Error(t, err)
}

// A namespace key with no plan data behind it is a malformed row, not an empty
// change set.
func TestStoredPlanFingerprint_NamespaceWithoutDataErrors(t *testing.T) {
	plan := storedPlanOf(map[string]*storage.NamespacePlanData{"shop": nil}, nil)

	_, err := StoredPlanFingerprint(schema.DialectMySQL, plan)

	require.ErrorContains(t, err, "shop")
}

func TestStoredPlanFingerprint_NoPlanErrors(t *testing.T) {
	_, err := StoredPlanFingerprint(schema.DialectMySQL, nil)

	require.Error(t, err)
}
