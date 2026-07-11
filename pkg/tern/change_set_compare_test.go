package tern

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
)

func protoAlterUsersEmail() *ternv1.TableChange {
	return &ternv1.TableChange{
		TableName:  "users",
		Ddl:        "ALTER TABLE `users` ADD COLUMN `email` varchar(255)",
		ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
		Namespace:  "testapp",
	}
}

func protoAlterUsersPhone() *ternv1.TableChange {
	return &ternv1.TableChange{
		TableName:  "users",
		Ddl:        "ALTER TABLE `users` ADD COLUMN `phone` varchar(255)",
		ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
		Namespace:  "testapp",
	}
}

func protoNonShardedSet(changes ...*ternv1.TableChange) ChangeSet {
	return ChangeSet{
		Changes: []*ternv1.SchemaChange{{Namespace: "testapp", TableChanges: changes}},
	}
}

// A non-sharded deployment that would plan exactly the reviewed changes is a
// clean match, even when the DDL differs only in whitespace/backtick style.
func TestCompareChangeSets_NonShardedMatch(t *testing.T) {
	baseline := protoNonShardedSet(protoAlterUsersEmail())
	candidate := ChangeSet{Changes: []*ternv1.SchemaChange{{
		Namespace: "testapp",
		TableChanges: []*ternv1.TableChange{{
			TableName:  "users",
			Ddl:        "ALTER TABLE users ADD COLUMN email varchar(255)",
			ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
			Namespace:  "testapp",
		}},
	}}}

	diff, err := CompareChangeSets(baseline, candidate)
	require.NoError(t, err)
	assert.True(t, diff.Empty(), "canonically identical change sets should match: %+v", diff)
}

// A deployment missing a reviewed change surfaces it as missing from the
// candidate so the review gate can block.
func TestCompareChangeSets_MissingChange(t *testing.T) {
	baseline := protoNonShardedSet(protoAlterUsersEmail())
	candidate := protoNonShardedSet()

	diff, err := CompareChangeSets(baseline, candidate)
	require.NoError(t, err)
	require.False(t, diff.Empty())
	require.Len(t, diff.MissingFromCandidate, 1)
	assert.Equal(t, "users", diff.MissingFromCandidate[0].Table)
	assert.Equal(t, "alter", diff.MissingFromCandidate[0].Operation)
	assert.Empty(t, diff.UnexpectedInCandidate)
}

// A deployment that would plan a change nobody reviewed surfaces it as
// unexpected.
func TestCompareChangeSets_UnexpectedChange(t *testing.T) {
	baseline := protoNonShardedSet(protoAlterUsersEmail())
	candidate := protoNonShardedSet(protoAlterUsersEmail(), protoAlterUsersPhone())

	diff, err := CompareChangeSets(baseline, candidate)
	require.NoError(t, err)
	require.Len(t, diff.UnexpectedInCandidate, 1)
	assert.Equal(t, "users", diff.UnexpectedInCandidate[0].Table)
	assert.Contains(t, diff.UnexpectedInCandidate[0].DDL, "phone")
	assert.Empty(t, diff.MissingFromCandidate)
}

// The same table/operation with different DDL is drift: the change is both
// missing (reviewed DDL absent) and unexpected (candidate DDL not reviewed).
func TestCompareChangeSets_SameTableDifferentDDL(t *testing.T) {
	baseline := protoNonShardedSet(protoAlterUsersEmail())
	candidate := protoNonShardedSet(protoAlterUsersPhone())

	diff, err := CompareChangeSets(baseline, candidate)
	require.NoError(t, err)
	require.Len(t, diff.MissingFromCandidate, 1)
	require.Len(t, diff.UnexpectedInCandidate, 1)
	assert.Contains(t, diff.MissingFromCandidate[0].DDL, "email")
	assert.Contains(t, diff.UnexpectedInCandidate[0].DDL, "phone")
}

// Sharded deployments compare per shard on the authoritative Shards rows, so
// identical per-shard changes match.
func TestCompareChangeSets_ShardedMatch(t *testing.T) {
	shard := func() []*ternv1.ShardPlan {
		return []*ternv1.ShardPlan{
			{Shard: "-80", Namespace: "testapp", Changes: []*ternv1.TableChange{protoAlterUsersEmail()}},
			{Shard: "80-", Namespace: "testapp", Changes: []*ternv1.TableChange{protoAlterUsersEmail()}},
		}
	}
	baseline := ChangeSet{
		Changes: []*ternv1.SchemaChange{{Namespace: "testapp", TableChanges: []*ternv1.TableChange{protoAlterUsersEmail()}}},
		Shards:  shard(),
	}
	candidate := ChangeSet{
		Changes: []*ternv1.SchemaChange{{Namespace: "testapp", TableChanges: []*ternv1.TableChange{protoAlterUsersEmail()}}},
		Shards:  shard(),
	}

	diff, err := CompareChangeSets(baseline, candidate)
	require.NoError(t, err)
	assert.True(t, diff.Empty(), "identical sharded change sets should match: %+v", diff)
}

// Drift on a single shard is caught even when the lossy namespace-collapsed
// Changes view is identical between the two deployments.
func TestCompareChangeSets_ShardDriftCaughtDespiteCollapsedParity(t *testing.T) {
	collapsed := []*ternv1.SchemaChange{{Namespace: "testapp", TableChanges: []*ternv1.TableChange{protoAlterUsersEmail()}}}
	baseline := ChangeSet{
		Changes: collapsed,
		Shards: []*ternv1.ShardPlan{
			{Shard: "-80", Namespace: "testapp", Changes: []*ternv1.TableChange{protoAlterUsersEmail()}},
			{Shard: "80-", Namespace: "testapp", Changes: []*ternv1.TableChange{protoAlterUsersEmail()}},
		},
	}
	candidate := ChangeSet{
		Changes: collapsed,
		Shards: []*ternv1.ShardPlan{
			{Shard: "-80", Namespace: "testapp", Changes: []*ternv1.TableChange{protoAlterUsersEmail()}},
			{Shard: "80-", Namespace: "testapp", Changes: []*ternv1.TableChange{protoAlterUsersPhone()}},
		},
	}

	diff, err := CompareChangeSets(baseline, candidate)
	require.NoError(t, err)
	require.False(t, diff.Empty(), "per-shard drift must be caught despite identical collapsed views")
	require.Len(t, diff.MissingFromCandidate, 1)
	require.Len(t, diff.UnexpectedInCandidate, 1)
	assert.Equal(t, "80-", diff.MissingFromCandidate[0].Shard)
	assert.Equal(t, "80-", diff.UnexpectedInCandidate[0].Shard)
}

// A database mixing a sharded and an unsharded namespace compares each namespace
// in its authoritative representation; the unsharded namespace is not dropped.
func TestCompareChangeSets_MixedShardedAndUnsharded(t *testing.T) {
	build := func(unshardedDDL string) ChangeSet {
		return ChangeSet{
			Changes: []*ternv1.SchemaChange{
				{Namespace: "sharded", TableChanges: []*ternv1.TableChange{protoAlterUsersEmail()}},
				{Namespace: "unsharded", TableChanges: []*ternv1.TableChange{{
					TableName:  "orders",
					Ddl:        unshardedDDL,
					ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
					Namespace:  "unsharded",
				}}},
			},
			Shards: []*ternv1.ShardPlan{
				{Shard: "-80", Namespace: "sharded", Changes: []*ternv1.TableChange{protoAlterUsersEmail()}},
			},
		}
	}
	baseline := build("ALTER TABLE `orders` ADD COLUMN `total` int")
	candidate := build("ALTER TABLE `orders` ADD COLUMN `discount` int")

	diff, err := CompareChangeSets(baseline, candidate)
	require.NoError(t, err)
	require.Len(t, diff.MissingFromCandidate, 1)
	require.Len(t, diff.UnexpectedInCandidate, 1)
	assert.Equal(t, "unsharded", diff.MissingFromCandidate[0].Namespace)
	assert.Empty(t, diff.MissingFromCandidate[0].Shard, "unsharded namespace change has no shard")
}

// VSchema parity is compared per namespace: a deployment that would not change
// the vschema when the reviewed plan would is drift.
func TestCompareChangeSets_VSchemaParity(t *testing.T) {
	baseline := ChangeSet{Changes: []*ternv1.SchemaChange{{
		Namespace: "testapp",
		Metadata:  map[string]string{"vschema_changed": "true"},
	}}}
	candidate := ChangeSet{Changes: []*ternv1.SchemaChange{{Namespace: "testapp"}}}

	diff, err := CompareChangeSets(baseline, candidate)
	require.NoError(t, err)
	require.Equal(t, []string{"testapp"}, diff.MissingVSchema)
	assert.Empty(t, diff.UnexpectedVSchema)
}

// Unparseable DDL fails closed so a comparison that cannot be trusted is never
// mistaken for agreement.
func TestCompareChangeSets_UnparseableDDLFailsClosed(t *testing.T) {
	baseline := protoNonShardedSet(protoAlterUsersEmail())
	candidate := protoNonShardedSet(&ternv1.TableChange{
		TableName:  "users",
		Ddl:        "this is not sql",
		ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
		Namespace:  "testapp",
	})

	_, err := CompareChangeSets(baseline, candidate)
	require.Error(t, err)
}

// A shard row with an empty shard name is malformed and fails closed.
func TestCompareChangeSets_EmptyShardNameFailsClosed(t *testing.T) {
	baseline := protoNonShardedSet(protoAlterUsersEmail())
	candidate := ChangeSet{Shards: []*ternv1.ShardPlan{
		{Shard: "", Namespace: "testapp", Changes: []*ternv1.TableChange{protoAlterUsersEmail()}},
	}}

	_, err := CompareChangeSets(baseline, candidate)
	require.Error(t, err)
}

// A namespace whose collapsed view carries table changes but whose only shard
// rows are empty is an inconsistent shape and fails closed.
func TestCompareChangeSets_InconsistentShardShapeFailsClosed(t *testing.T) {
	baseline := protoNonShardedSet(protoAlterUsersEmail())
	candidate := ChangeSet{
		Changes: []*ternv1.SchemaChange{{Namespace: "testapp", TableChanges: []*ternv1.TableChange{protoAlterUsersEmail()}}},
		Shards:  []*ternv1.ShardPlan{{Shard: "-80", Namespace: "testapp"}},
	}

	_, err := CompareChangeSets(baseline, candidate)
	require.Error(t, err)
}

// A vschema change represented as a table DDL entry (rather than via metadata)
// is malformed plan/proto input and fails closed.
func TestCompareChangeSets_VSchemaTableChangeFailsClosed(t *testing.T) {
	baseline := protoNonShardedSet(protoAlterUsersEmail())
	candidate := ChangeSet{Changes: []*ternv1.SchemaChange{{
		Namespace: "testapp",
		TableChanges: []*ternv1.TableChange{{
			TableName:  "users",
			Ddl:        "",
			ChangeType: ternv1.ChangeType_CHANGE_TYPE_VSCHEMA,
			Namespace:  "testapp",
		}},
	}}}

	_, err := CompareChangeSets(baseline, candidate)
	require.Error(t, err)
}

// A namespace represented by per-shard rows on one side but only the collapsed
// namespace view on the other diverges rather than accidentally matching: the
// per-shard and collapsed keys occupy disjoint key spaces (shard name vs empty).
func TestCompareChangeSets_OneSideShardedDiverges(t *testing.T) {
	sharded := ChangeSet{
		Changes: []*ternv1.SchemaChange{{Namespace: "testapp", TableChanges: []*ternv1.TableChange{protoAlterUsersEmail()}}},
		Shards:  []*ternv1.ShardPlan{{Shard: "-80", Namespace: "testapp", Changes: []*ternv1.TableChange{protoAlterUsersEmail()}}},
	}
	collapsedOnly := protoNonShardedSet(protoAlterUsersEmail())

	diff, err := CompareChangeSets(sharded, collapsedOnly)
	require.NoError(t, err)
	require.False(t, diff.Empty(), "sharded-vs-collapsed for the same namespace must diverge")
	require.Len(t, diff.MissingFromCandidate, 1)
	require.Len(t, diff.UnexpectedInCandidate, 1)
	assert.Equal(t, "-80", diff.MissingFromCandidate[0].Shard)
	assert.Empty(t, diff.UnexpectedInCandidate[0].Shard)
}

// Duplicate shard rows are counted as a multiset, so an asymmetric duplication
// diverges rather than being deduped into a false match.
func TestCompareChangeSets_DuplicateShardRowsAsymmetricDiverges(t *testing.T) {
	baseline := ChangeSet{Shards: []*ternv1.ShardPlan{
		{Shard: "-80", Namespace: "testapp", Changes: []*ternv1.TableChange{protoAlterUsersEmail()}},
		{Shard: "-80", Namespace: "testapp", Changes: []*ternv1.TableChange{protoAlterUsersEmail()}},
	}}
	candidate := ChangeSet{Shards: []*ternv1.ShardPlan{
		{Shard: "-80", Namespace: "testapp", Changes: []*ternv1.TableChange{protoAlterUsersEmail()}},
	}}

	diff, err := CompareChangeSets(baseline, candidate)
	require.NoError(t, err)
	require.False(t, diff.Empty(), "asymmetric duplicate shard rows must diverge")
	require.Len(t, diff.MissingFromCandidate, 1)
	assert.Equal(t, "-80", diff.MissingFromCandidate[0].Shard)
}

// VSchema parity is symmetric: a namespace the candidate changes the vschema for
// but the baseline does not surfaces in the unexpected direction.
func TestCompareChangeSets_VSchemaUnexpectedDirection(t *testing.T) {
	baseline := ChangeSet{Changes: []*ternv1.SchemaChange{{Namespace: "testapp"}}}
	candidate := ChangeSet{Changes: []*ternv1.SchemaChange{{
		Namespace: "testapp",
		Metadata:  map[string]string{"vschema_changed": "true"},
	}}}

	diff, err := CompareChangeSets(baseline, candidate)
	require.NoError(t, err)
	require.Equal(t, []string{"testapp"}, diff.UnexpectedVSchema)
	assert.Empty(t, diff.MissingVSchema)
}
