package tern

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
)

func fingerprint(t *testing.T, cs ChangeSet) string {
	t.Helper()
	fp, err := ChangeSetFingerprint(schema.DialectMySQL, cs)
	require.NoError(t, err)
	require.NotEmpty(t, fp)
	return fp
}

// Two members whose plans differ only in how the DDL is written run the same
// work, so they group together. This is the property that makes grouping by
// fingerprint sound: it keys on the canonicalized change, not on the text.
func TestChangeSetFingerprint_CanonicallyIdenticalSetsShareAKey(t *testing.T) {
	backticked := protoNonShardedSet(protoAlterUsersEmail())
	bare := protoNonShardedSet(&ternv1.TableChange{
		TableName:  "users",
		Ddl:        "ALTER TABLE users ADD COLUMN email varchar(255)",
		ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
		Namespace:  "testapp",
	})

	assert.Equal(t, fingerprint(t, backticked), fingerprint(t, bare))
}

// Members that would run different DDL must not be grouped together, however
// similar the change looks.
func TestChangeSetFingerprint_DifferentWorkGetsDifferentKeys(t *testing.T) {
	email := protoNonShardedSet(protoAlterUsersEmail())
	phone := protoNonShardedSet(protoAlterUsersPhone())

	assert.NotEqual(t, fingerprint(t, email), fingerprint(t, phone))
}

// The engine may return one member's changes in a different order than
// another's. Order is not work, so it must not split a group.
func TestChangeSetFingerprint_IgnoresChangeOrder(t *testing.T) {
	forward := protoNonShardedSet(protoAlterUsersEmail(), protoAlterUsersPhone())
	reversed := protoNonShardedSet(protoAlterUsersPhone(), protoAlterUsersEmail())

	assert.Equal(t, fingerprint(t, forward), fingerprint(t, reversed))
}

// Running a change once and running it twice are different amounts of work, so
// the key counts changes rather than collecting them into a set.
func TestChangeSetFingerprint_CountsDuplicateChanges(t *testing.T) {
	once := protoNonShardedSet(protoAlterUsersEmail())
	twice := protoNonShardedSet(protoAlterUsersEmail(), protoAlterUsersEmail())

	assert.NotEqual(t, fingerprint(t, once), fingerprint(t, twice))
}

// A member already at the desired schema plans nothing. That is a group of its
// own — "nothing to apply here" — not a member that failed to produce a key.
func TestChangeSetFingerprint_EmptySetIsItsOwnGroup(t *testing.T) {
	empty := fingerprint(t, ChangeSet{})
	noChanges := fingerprint(t, protoNonShardedSet())
	withWork := fingerprint(t, protoNonShardedSet(protoAlterUsersEmail()))

	assert.Equal(t, empty, noChanges, "a namespace planning nothing is the same work as no namespace at all")
	assert.NotEqual(t, empty, withWork)
}

// A vschema change carries no table DDL, so it would be invisible to a key built
// from table changes alone: two members would group together while only one of
// them rewrites the vschema.
func TestChangeSetFingerprint_VSchemaChangeSplitsAGroup(t *testing.T) {
	plain := ChangeSet{Changes: []*ternv1.SchemaChange{{
		Namespace:    "testapp",
		TableChanges: []*ternv1.TableChange{protoAlterUsersEmail()},
	}}}
	withVSchema := ChangeSet{Changes: []*ternv1.SchemaChange{{
		Namespace:    "testapp",
		TableChanges: []*ternv1.TableChange{protoAlterUsersEmail()},
		Metadata:     map[string]string{"vschema_changed": "true"},
	}}}

	assert.NotEqual(t, fingerprint(t, plain), fingerprint(t, withVSchema))
}

// The key joins fields that can each hold anything a schema author wrote, so it
// has to separate them. These two change sets run against different shards of
// different namespaces and share every other field, including the DDL — so the
// boundary between namespace and shard is the only thing telling them apart, and
// a key that concatenated its fields would group them as identical work.
func TestChangeSetFingerprint_SeparatesItsFields(t *testing.T) {
	sharded := func(namespace, shard string) ChangeSet {
		return ChangeSet{Shards: []*ternv1.ShardPlan{{
			Namespace: namespace,
			Shard:     shard,
			Changes:   []*ternv1.TableChange{protoAlterUsersEmail()},
		}}}
	}

	assert.NotEqual(t, fingerprint(t, sharded("app", "a1")), fingerprint(t, sharded("appa", "1")))
}

// A change set the comparison cannot canonicalize has no key. A caller must be
// able to tell that apart from a key, so it can refuse to group the member
// rather than group it with members it was never compared against.
func TestChangeSetFingerprint_MalformedSetHasNoKey(t *testing.T) {
	_, err := ChangeSetFingerprint(schema.DialectMySQL, ChangeSet{
		Changes: []*ternv1.SchemaChange{{Namespace: "testapp", TableChanges: []*ternv1.TableChange{nil}}},
	})
	require.Error(t, err)
}

// The contract the grouping rests on: two members share a key exactly when the
// comparison reports no difference between them. If these two ever disagree, a
// comment would either split one plan across several blocks or render two
// different plans as one.
func TestChangeSetFingerprint_AgreesWithCompareChangeSets(t *testing.T) {
	restyled := protoNonShardedSet(&ternv1.TableChange{
		TableName:  "users",
		Ddl:        "ALTER TABLE users ADD COLUMN email varchar(255)",
		ChangeType: ternv1.ChangeType_CHANGE_TYPE_ALTER,
		Namespace:  "testapp",
	})

	cases := []struct {
		name                string
		baseline, candidate ChangeSet
	}{
		{"identical", protoNonShardedSet(protoAlterUsersEmail()), protoNonShardedSet(protoAlterUsersEmail())},
		{"restyled DDL", protoNonShardedSet(protoAlterUsersEmail()), restyled},
		{"reordered", protoNonShardedSet(protoAlterUsersEmail(), protoAlterUsersPhone()), protoNonShardedSet(protoAlterUsersPhone(), protoAlterUsersEmail())},
		{"both empty", protoNonShardedSet(), ChangeSet{}},
		{"missing change", protoNonShardedSet(protoAlterUsersEmail()), protoNonShardedSet()},
		{"extra change", protoNonShardedSet(protoAlterUsersEmail()), protoNonShardedSet(protoAlterUsersEmail(), protoAlterUsersPhone())},
		{"different DDL", protoNonShardedSet(protoAlterUsersEmail()), protoNonShardedSet(protoAlterUsersPhone())},
		{"duplicated change", protoNonShardedSet(protoAlterUsersEmail()), protoNonShardedSet(protoAlterUsersEmail(), protoAlterUsersEmail())},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			diff, err := CompareChangeSets(schema.DialectMySQL, tc.baseline, tc.candidate)
			require.NoError(t, err)
			assert.Equal(t, diff.Empty(), fingerprint(t, tc.baseline) == fingerprint(t, tc.candidate),
				"fingerprint equality must agree with the comparison; diff: %+v", diff)
		})
	}
}
