package tern

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
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

	diff, err := CompareChangeSets(schema.DialectMySQL, baseline, candidate)
	require.NoError(t, err)
	assert.True(t, diff.Empty(), "canonically identical change sets should match: %+v", diff)
}

// A deployment missing a reviewed change surfaces it as missing from the
// candidate so the review gate can block.
func TestCompareChangeSets_MissingChange(t *testing.T) {
	baseline := protoNonShardedSet(protoAlterUsersEmail())
	candidate := protoNonShardedSet()

	diff, err := CompareChangeSets(schema.DialectMySQL, baseline, candidate)
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

	diff, err := CompareChangeSets(schema.DialectMySQL, baseline, candidate)
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

	diff, err := CompareChangeSets(schema.DialectMySQL, baseline, candidate)
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

	diff, err := CompareChangeSets(schema.DialectMySQL, baseline, candidate)
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

	diff, err := CompareChangeSets(schema.DialectMySQL, baseline, candidate)
	require.NoError(t, err)
	require.False(t, diff.Empty(), "per-shard drift must be caught despite identical collapsed views")
	require.Len(t, diff.MissingFromCandidate, 1)
	require.Len(t, diff.UnexpectedInCandidate, 1)
	assert.Equal(t, "80-", diff.MissingFromCandidate[0].Shard)
	assert.Equal(t, "80-", diff.UnexpectedInCandidate[0].Shard)
}

// The authoritative walk yields each change once: per shard for a namespace the
// shard rows carry, from the collapsed view for an unsharded namespace, and it
// agrees with the number of changes the comparison counts in the same set.
func TestChangeSet_AuthoritativeTableChanges(t *testing.T) {
	orders := &ternv1.TableChange{
		TableName:  "orders",
		Ddl:        "ALTER TABLE `orders` ADD COLUMN `total` int",
		ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
		Namespace:  "unsharded",
	}
	cs := ChangeSet{
		Changes: []*ternv1.SchemaChange{
			{Namespace: "sharded", TableChanges: []*ternv1.TableChange{protoAlterUsersEmail()}},
			{Namespace: "unsharded", TableChanges: []*ternv1.TableChange{orders}},
		},
		Shards: []*ternv1.ShardPlan{
			{Shard: "-80", Namespace: "sharded", Changes: []*ternv1.TableChange{protoAlterUsersEmail()}},
			{Shard: "80-", Namespace: "sharded", Changes: []*ternv1.TableChange{protoAlterUsersPhone()}},
		},
	}

	got := cs.AuthoritativeTableChanges()
	require.Len(t, got, 3, "two shard rows for the sharded namespace plus the unsharded change")
	assert.Equal(t, protoAlterUsersEmail().Ddl, got[0].Ddl)
	assert.Equal(t, protoAlterUsersPhone().Ddl, got[1].Ddl)
	assert.Same(t, orders, got[2])

	diff, err := CompareChangeSets(schema.DialectMySQL, cs, ChangeSet{})
	require.NoError(t, err)
	assert.Len(t, diff.MissingFromCandidate, len(got), "the walk and the comparison count the same changes")

	// Nil rows are the comparison's failure to report; the walk skips them.
	withNils := ChangeSet{Changes: append(cs.Changes, nil), Shards: append(cs.Shards, nil)}
	assert.Equal(t, got, withNils.AuthoritativeTableChanges())
}

// A namespace with shard rows that carry nothing is read from its collapsed
// view; the walk does not lose the change because empty shard rows exist.
func TestChangeSet_AuthoritativeTableChangesEmptyShardRows(t *testing.T) {
	cs := ChangeSet{
		Changes: []*ternv1.SchemaChange{{Namespace: "testapp", TableChanges: []*ternv1.TableChange{protoAlterUsersEmail()}}},
		Shards:  []*ternv1.ShardPlan{{Shard: "-80", Namespace: "testapp"}},
	}

	got := cs.AuthoritativeTableChanges()
	require.Len(t, got, 1)
	assert.Equal(t, protoAlterUsersEmail().Ddl, got[0].Ddl)
}

// A change set has work when it carries a table change in either
// representation or a VSchema change under either signal, and has none when it
// carries only empty rows.
func TestChangeSet_HasWork(t *testing.T) {
	for _, tc := range []struct {
		name string
		cs   ChangeSet
		want bool
	}{
		{"empty", ChangeSet{}, false},
		{"only empty rows", ChangeSet{
			Changes: []*ternv1.SchemaChange{{Namespace: "testapp"}, nil},
			Shards:  []*ternv1.ShardPlan{{Shard: "-80", Namespace: "testapp"}, nil},
		}, false},
		{"collapsed table change", ChangeSet{
			Changes: []*ternv1.SchemaChange{{Namespace: "testapp", TableChanges: []*ternv1.TableChange{protoAlterUsersEmail()}}},
		}, true},
		{"shard table change", ChangeSet{
			Shards: []*ternv1.ShardPlan{{Shard: "-80", Namespace: "testapp", Changes: []*ternv1.TableChange{protoAlterUsersEmail()}}},
		}, true},
		{"vschema flag", ChangeSet{
			Changes: []*ternv1.SchemaChange{{Namespace: "testapp", Metadata: map[string]string{"vschema_changed": "true"}}},
		}, true},
		{"vschema diff", ChangeSet{
			Changes: []*ternv1.SchemaChange{{Namespace: "testapp", Metadata: map[string]string{"vschema": "+ tables.users"}}},
		}, true},
		{"vschema flag false", ChangeSet{
			Changes: []*ternv1.SchemaChange{{Namespace: "testapp", Metadata: map[string]string{"vschema_changed": "false"}}},
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.cs.HasWork())
		})
	}
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

	diff, err := CompareChangeSets(schema.DialectMySQL, baseline, candidate)
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

	diff, err := CompareChangeSets(schema.DialectMySQL, baseline, candidate)
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

	_, err := CompareChangeSets(schema.DialectMySQL, baseline, candidate)
	require.Error(t, err)
}

// A shard row with an empty shard name is malformed and fails closed.
func TestCompareChangeSets_EmptyShardNameFailsClosed(t *testing.T) {
	baseline := protoNonShardedSet(protoAlterUsersEmail())
	candidate := ChangeSet{Shards: []*ternv1.ShardPlan{
		{Shard: "", Namespace: "testapp", Changes: []*ternv1.TableChange{protoAlterUsersEmail()}},
	}}

	_, err := CompareChangeSets(schema.DialectMySQL, baseline, candidate)
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

	_, err := CompareChangeSets(schema.DialectMySQL, baseline, candidate)
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

	_, err := CompareChangeSets(schema.DialectMySQL, baseline, candidate)
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

	diff, err := CompareChangeSets(schema.DialectMySQL, sharded, collapsedOnly)
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

	diff, err := CompareChangeSets(schema.DialectMySQL, baseline, candidate)
	require.NoError(t, err)
	require.False(t, diff.Empty(), "asymmetric duplicate shard rows must diverge")
	require.Len(t, diff.MissingFromCandidate, 1)
	assert.Equal(t, "-80", diff.MissingFromCandidate[0].Shard)
}

// A PostgreSQL deployment's change set is compared under the PostgreSQL
// grammar: DDL that differs only in formatting and identifier quoting matches
// cleanly, and DDL the MySQL parser cannot read is still comparable. The same
// DDL judged under the MySQL dialect fails closed instead of producing a
// meaningless comparison.
func TestCompareChangeSets_PostgresDialect(t *testing.T) {
	baseline := ChangeSet{Changes: []*ternv1.SchemaChange{{
		Namespace: "buzz",
		TableChanges: []*ternv1.TableChange{{
			TableName:  "users",
			Ddl:        "CREATE TABLE users (id uuid PRIMARY KEY, created_at timestamptz NOT NULL DEFAULT now())",
			ChangeType: ternv1.ChangeType_CHANGE_TYPE_CREATE,
			Namespace:  "buzz",
		}},
	}}}
	candidate := ChangeSet{Changes: []*ternv1.SchemaChange{{
		Namespace: "buzz",
		TableChanges: []*ternv1.TableChange{{
			TableName: "users",
			Ddl: `CREATE TABLE "users" (
				"id"         uuid        PRIMARY KEY,
				"created_at" timestamptz NOT NULL DEFAULT now()
			)`,
			ChangeType: ternv1.ChangeType_CHANGE_TYPE_CREATE,
			Namespace:  "buzz",
		}},
	}}}

	diff, err := CompareChangeSets(schema.DialectPostgres, baseline, candidate)
	require.NoError(t, err)
	assert.True(t, diff.Empty(), "formatting-equivalent PostgreSQL DDL must match: %+v", diff)

	_, err = CompareChangeSets(schema.DialectMySQL, baseline, candidate)
	require.Error(t, err, "PostgreSQL DDL judged under the MySQL grammar must fail closed")
}

// Two PostgreSQL targets that map the same canonical namespace to differently
// named physical schemas plan the same change with different schema
// qualifiers in the DDL. The namespace is already the key, so the qualifier
// is not drift; a real difference under the qualifiers still is.
func TestCompareChangeSets_PostgresPhysicalSchemaQualifierIsNotDrift(t *testing.T) {
	regionSet := func(physicalSchema, alterColumn string) ChangeSet {
		q := `"` + physicalSchema + `".`
		return ChangeSet{Changes: []*ternv1.SchemaChange{{
			Namespace: "orders",
			TableChanges: []*ternv1.TableChange{
				{
					TableName:  "carrier_config",
					Ddl:        "ALTER TABLE " + q + "carrier_config ADD COLUMN " + alterColumn + " text",
					ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
					Namespace:  "orders",
				},
				{
					TableName: "shipment",
					Ddl: "CREATE TABLE " + q + "shipment (id bigserial NOT NULL, tracking_code text, CONSTRAINT shipment_pkey PRIMARY KEY (id));\n" +
						"CREATE INDEX idx_shipment_tracking_code ON " + q + "shipment USING btree (tracking_code)",
					ChangeType: ternv1.ChangeType_CHANGE_TYPE_CREATE,
					Namespace:  "orders",
				},
			},
		}}}
	}

	diff, err := CompareChangeSets(schema.DialectPostgres, regionSet("orders-region-a", "tracking_prefix"), regionSet("orders-region-b", "tracking_prefix"))
	require.NoError(t, err)
	assert.True(t, diff.Empty(), "the same change on differently named physical schemas must match: %+v", diff)

	diff, err = CompareChangeSets(schema.DialectPostgres, regionSet("orders-region-a", "tracking_prefix"), regionSet("orders-region-b", "tracking_suffix"))
	require.NoError(t, err)
	require.Len(t, diff.MissingFromCandidate, 1, "a different column under a different qualifier must still diverge")
	require.Len(t, diff.UnexpectedInCandidate, 1)
	assert.Equal(t, "carrier_config", diff.MissingFromCandidate[0].Table)
	assert.Equal(t, "carrier_config", diff.UnexpectedInCandidate[0].Table)
}

// A dialect with no registered parser gives the comparison no grammar to
// canonicalize with, so it fails closed instead of guessing — even when both
// change sets are empty and would trivially match.
func TestCompareChangeSets_UnregisteredDialectFailsClosed(t *testing.T) {
	_, err := CompareChangeSets(schema.Dialect("oracle"), ChangeSet{}, ChangeSet{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `no statement parser registered for dialect "oracle"`)
}

// PostgreSQL-only constructs the MySQL parser cannot read are comparable under
// the PostgreSQL dialect, and a genuine difference still surfaces as drift.
func TestCompareChangeSets_PostgresOnlyConstructDiverges(t *testing.T) {
	index := func(cols string) ChangeSet {
		return ChangeSet{Changes: []*ternv1.SchemaChange{{
			Namespace: "buzz",
			TableChanges: []*ternv1.TableChange{{
				TableName:  "users",
				Ddl:        "CREATE INDEX CONCURRENTLY idx_users ON users (" + cols + ")",
				ChangeType: ternv1.ChangeType_CHANGE_TYPE_CREATE_INDEX,
				Namespace:  "buzz",
			}},
		}}}
	}

	diff, err := CompareChangeSets(schema.DialectPostgres, index("email"), index("email"))
	require.NoError(t, err)
	assert.True(t, diff.Empty())

	diff, err = CompareChangeSets(schema.DialectPostgres, index("email"), index("phone"))
	require.NoError(t, err)
	require.False(t, diff.Empty(), "different index columns must diverge")
	require.Len(t, diff.MissingFromCandidate, 1)
	require.Len(t, diff.UnexpectedInCandidate, 1)
}

// VSchema parity is symmetric: a namespace the candidate changes the vschema for
// but the baseline does not surfaces in the unexpected direction.
func TestCompareChangeSets_VSchemaUnexpectedDirection(t *testing.T) {
	baseline := ChangeSet{Changes: []*ternv1.SchemaChange{{Namespace: "testapp"}}}
	candidate := ChangeSet{Changes: []*ternv1.SchemaChange{{
		Namespace: "testapp",
		Metadata:  map[string]string{"vschema_changed": "true"},
	}}}

	diff, err := CompareChangeSets(schema.DialectMySQL, baseline, candidate)
	require.NoError(t, err)
	require.Equal(t, []string{"testapp"}, diff.UnexpectedVSchema)
	assert.Empty(t, diff.MissingVSchema)
}
