package vschema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMutations_NoChange(t *testing.T) {
	mutations, err := Mutations(shardedVSchema, shardedVSchema, nil)
	require.NoError(t, err)
	assert.Empty(t, mutations)
}

func TestMutations_EmptyCurrent(t *testing.T) {
	// A keyspace with no VSchema yet reports nothing or the empty object. Its
	// first VSchema declaring the keyspace sharded is how a sharded keyspace
	// is onboarded, not a flip, so it is not a mutation even when the
	// keyspace already holds the tables the document lists.
	for _, current := range []string{"", "   ", "{}"} {
		mutations, err := Mutations(current, shardedVSchema, []string{"users"})
		require.NoError(t, err)
		assert.Empty(t, mutations, "current %q", current)
	}
}

func TestMutations_TypeChange(t *testing.T) {
	// Changing a vindex's type re-computes every row's keyspace id — an
	// effective re-shard of how queries route — so it must be disclosed even
	// though the vindex keeps its name.
	current := `{"sharded": true, "vindexes": {"user_idx": {"type": "hash"}}, "tables": {"users": {"column_vindexes": [{"column": "id", "name": "user_idx"}]}}}`
	desired := `{"sharded": true, "vindexes": {"user_idx": {"type": "xxhash"}}, "tables": {"users": {"column_vindexes": [{"column": "id", "name": "user_idx"}]}}}`
	mutations, err := Mutations(current, desired, nil)
	require.NoError(t, err)
	require.Len(t, mutations, 1)
	assert.Equal(t, MutationKindVindexType, mutations[0].Kind)
	assert.Equal(t, "user_idx", mutations[0].Name)
	assert.Contains(t, mutations[0].Reason, `"hash"`)
	assert.Contains(t, mutations[0].Reason, `"xxhash"`)
	assert.Contains(t, mutations[0].Reason, "keyspace id")
}

func TestMutations_LookupBackingTableRepoint(t *testing.T) {
	// Repointing a lookup vindex's backing table makes Vitess read and write
	// lookup rows in a different table immediately; the old table goes stale.
	desired := `{
		"sharded": true,
		"vindexes": {
			"hash": {"type": "hash"},
			"email_lookup": {
				"type": "consistent_lookup_unique",
				"params": {"table": "email_lookup_v2", "from": "email", "to": "keyspace_id"},
				"owner": "users"
			}
		},
		"tables": {
			"users": {
				"column_vindexes": [
					{"column": "id", "name": "hash"},
					{"column": "email", "name": "email_lookup"}
				]
			},
			"orders": {
				"column_vindexes": [
					{"column": "user_id", "name": "hash"}
				]
			}
		}
	}`
	mutations, err := Mutations(shardedVSchema, desired, nil)
	require.NoError(t, err)
	require.Len(t, mutations, 1)
	assert.Equal(t, MutationKindVindexParams, mutations[0].Kind)
	assert.Equal(t, "email_lookup", mutations[0].Name)
	assert.Contains(t, mutations[0].Reason, "repoints its backing table")
	assert.Contains(t, mutations[0].Reason, `"email_lookup"`)
	assert.Contains(t, mutations[0].Reason, `"email_lookup_v2"`)
}

func TestMutations_GenericParamChangeAddAndRemove(t *testing.T) {
	// Every changed, added, or removed param is reported individually, in
	// key order.
	current := `{"sharded": true, "vindexes": {"v": {"type": "region_json", "params": {"region_map": "/etc/a.json", "region_bytes": "1"}}}, "tables": {}}`
	desired := `{"sharded": true, "vindexes": {"v": {"type": "region_json", "params": {"region_map": "/etc/b.json", "extra": "x"}}}, "tables": {}}`
	mutations, err := Mutations(current, desired, nil)
	require.NoError(t, err)
	require.Len(t, mutations, 3)
	for _, m := range mutations {
		assert.Equal(t, MutationKindVindexParams, m.Kind)
		assert.Equal(t, "v", m.Name)
	}
	assert.Contains(t, mutations[0].Reason, `adds parameter "extra"`)
	assert.Contains(t, mutations[1].Reason, `removes parameter "region_bytes"`)
	assert.Contains(t, mutations[2].Reason, `changes parameter "region_map"`)
	assert.Contains(t, mutations[2].Reason, `"/etc/a.json"`)
	assert.Contains(t, mutations[2].Reason, `"/etc/b.json"`)
}

func TestMutations_EmptyValuedParamAddAndRemove(t *testing.T) {
	// A param's presence is part of its identity: adding or removing a key
	// whose value is the empty string still changes the vindex definition and
	// must be disclosed, not silently equated with the key being absent.
	current := `{"sharded": true, "vindexes": {"v": {"type": "region_json", "params": {"blank": ""}}}, "tables": {}}`
	desired := `{"sharded": true, "vindexes": {"v": {"type": "region_json", "params": {"empty": ""}}}, "tables": {}}`
	mutations, err := Mutations(current, desired, nil)
	require.NoError(t, err)
	require.Len(t, mutations, 2)
	assert.Contains(t, mutations[0].Reason, `removes parameter "blank"`)
	assert.Contains(t, mutations[1].Reason, `adds parameter "empty"`)
}

func TestMutations_TableParamChangeOnNonLookupDesired(t *testing.T) {
	// When the vindex is simultaneously retyped out of the lookup family, the
	// backing-table repoint framing no longer describes the desired state —
	// the table param change is reported with the generic reason, and the
	// type change carries its own disclosure.
	current := `{"sharded": true, "vindexes": {"l": {"type": "lookup_unique", "params": {"table": "l1", "from": "a", "to": "b"}, "owner": "users"}}, "tables": {}}`
	desired := `{"sharded": true, "vindexes": {"l": {"type": "region_json", "params": {"table": "l2", "from": "a", "to": "b"}, "owner": "users"}}, "tables": {}}`
	mutations, err := Mutations(current, desired, nil)
	require.NoError(t, err)
	require.Len(t, mutations, 2)
	assert.Equal(t, MutationKindVindexType, mutations[0].Kind)
	assert.Equal(t, MutationKindVindexParams, mutations[1].Kind)
	assert.Contains(t, mutations[1].Reason, `changes parameter "table"`)
	assert.NotContains(t, mutations[1].Reason, "repoints its backing table")
}

func TestMutations_OwnerChange(t *testing.T) {
	current := `{"sharded": true, "vindexes": {"l": {"type": "lookup", "params": {"table": "l", "from": "a", "to": "b"}, "owner": "users"}}, "tables": {}}`
	desired := `{"sharded": true, "vindexes": {"l": {"type": "lookup", "params": {"table": "l", "from": "a", "to": "b"}, "owner": "orders"}}, "tables": {}}`
	mutations, err := Mutations(current, desired, nil)
	require.NoError(t, err)
	require.Len(t, mutations, 1)
	assert.Equal(t, MutationKindVindexOwner, mutations[0].Kind)
	assert.Equal(t, "l", mutations[0].Name)
	assert.Contains(t, mutations[0].Reason, `"users"`)
	assert.Contains(t, mutations[0].Reason, `"orders"`)
}

func TestMutations_RemovedVindexNotDuplicated(t *testing.T) {
	// A vindex that disappears entirely is a removal — Deletions reports it,
	// Mutations stays silent so the same change is not disclosed twice.
	current := `{"sharded": true, "vindexes": {"gone": {"type": "hash"}}, "tables": {}}`
	desired := `{"sharded": true, "vindexes": {}, "tables": {}}`
	mutations, err := Mutations(current, desired, nil)
	require.NoError(t, err)
	assert.Empty(t, mutations)
}

func TestMutations_CombinedTypeParamsOwner(t *testing.T) {
	// A vindex changing several aspects at once reports each one, so the
	// operator sees the full blast radius: type, then params, then owner.
	current := `{"sharded": true, "vindexes": {"l": {"type": "lookup_unique", "params": {"table": "l1", "from": "a", "to": "b"}, "owner": "users"}}, "tables": {}}`
	desired := `{"sharded": true, "vindexes": {"l": {"type": "consistent_lookup_unique", "params": {"table": "l2", "from": "a", "to": "b"}, "owner": "orders"}}, "tables": {}}`
	mutations, err := Mutations(current, desired, nil)
	require.NoError(t, err)
	require.Len(t, mutations, 3)
	assert.Equal(t, MutationKindVindexType, mutations[0].Kind)
	assert.Equal(t, MutationKindVindexParams, mutations[1].Kind)
	assert.Contains(t, mutations[1].Reason, "repoints its backing table")
	assert.Equal(t, MutationKindVindexOwner, mutations[2].Kind)
}

func TestMutations_KeyspaceAndTableRouting(t *testing.T) {
	// Changes to the keyspace's sharded flag or to a table's type, primary
	// vindex, or auto-increment re-route rows or change where ids come from
	// the moment the VSchema lands, so each is disclosed. A new secondary
	// vindex is not.
	const vindexes = `"vindexes": {"hash": {"type": "hash"}, "xxhash": {"type": "xxhash"}}`
	usersTable := func(body string) string {
		return `{"sharded": true, ` + vindexes + `, "tables": {"users": {` + body + `}}}`
	}
	const hashID = `"column_vindexes": [{"column": "id", "name": "hash"}]`
	const hashIDThenXXHashEmail = `"column_vindexes": [{"column": "id", "name": "hash"}, {"column": "email", "name": "xxhash"}]`
	const xxhashEmailThenHashID = `"column_vindexes": [{"column": "email", "name": "xxhash"}, {"column": "id", "name": "hash"}]`
	const usersSeq = `"auto_increment": {"column": "id", "sequence": "users_seq"}`

	tests := []struct {
		name           string
		current        string
		desired        string
		wantKind       string
		wantName       string
		reasonContains []string
	}{
		{
			name:           "reordering column vindexes changes the primary vindex",
			current:        usersTable(hashIDThenXXHashEmail),
			desired:        usersTable(xxhashEmailThenHashID),
			wantKind:       MutationKindTablePrimaryVindex,
			wantName:       "users",
			reasonContains: []string{`primary vindex from "hash" on (id) to "xxhash" on (email)`, "keyspace id"},
		},
		{
			name:           "replacing the primary vindex on the same column",
			current:        usersTable(hashID),
			desired:        usersTable(`"column_vindexes": [{"column": "id", "name": "xxhash"}]`),
			wantKind:       MutationKindTablePrimaryVindex,
			wantName:       "users",
			reasonContains: []string{`from "hash" on (id) to "xxhash" on (id)`},
		},
		{
			name:           "removing the auto-increment",
			current:        usersTable(hashID + `, ` + usersSeq),
			desired:        usersTable(hashID),
			wantKind:       MutationKindTableAutoIncrement,
			wantName:       "users",
			reasonContains: []string{`stops using sequence "users_seq" for column "id"`, "not guaranteed to generate"},
		},
		{
			name:           "re-pointing the auto-increment sequence",
			current:        usersTable(hashID + `, ` + usersSeq),
			desired:        usersTable(hashID + `, "auto_increment": {"column": "id", "sequence": "users_seq_v2"}`),
			wantKind:       MutationKindTableAutoIncrement,
			wantName:       "users",
			reasonContains: []string{`from sequence "users_seq" on column "id" to sequence "users_seq_v2" on column "id"`},
		},
		{
			name:           "re-pointing the auto-increment column",
			current:        usersTable(hashID + `, ` + usersSeq),
			desired:        usersTable(hashID + `, "auto_increment": {"column": "user_id", "sequence": "users_seq"}`),
			wantKind:       MutationKindTableAutoIncrement,
			wantName:       "users",
			reasonContains: []string{`on column "id" to sequence "users_seq" on column "user_id"`},
		},
		{
			name:           "changing the table type",
			current:        `{"tables": {"countries": {}}}`,
			desired:        `{"tables": {"countries": {"type": "reference"}}}`,
			wantKind:       MutationKindTableType,
			wantName:       "countries",
			reasonContains: []string{`table "countries" changes type from the default table type to "reference"`},
		},
		{
			name:           "sharding an unsharded keyspace",
			current:        `{"tables": {"users": {}}}`,
			desired:        `{"sharded": true, ` + vindexes + `, "tables": {"users": {}}}`,
			wantKind:       MutationKindKeyspaceSharded,
			wantName:       "",
			reasonContains: []string{"from unsharded to sharded"},
		},
		{
			name:           "unsharding a sharded keyspace",
			current:        usersTable(hashID),
			desired:        `{` + vindexes + `, "tables": {"users": {` + hashID + `}}}`,
			wantKind:       MutationKindKeyspaceSharded,
			wantName:       "",
			reasonContains: []string{"from sharded to unsharded"},
		},
		{
			name:    "adding a secondary vindex is not a mutation",
			current: usersTable(hashID),
			desired: usersTable(hashIDThenXXHashEmail),
		},
		{
			name:           "adding an auto-increment changes the id source",
			current:        usersTable(hashID),
			desired:        usersTable(hashID + `, ` + usersSeq),
			wantKind:       MutationKindTableAutoIncrement,
			wantName:       "users",
			reasonContains: []string{`starts using sequence "users_seq" for column "id"`, "different source"},
		},
		{
			name:           "giving an existing table its first primary vindex re-keys it",
			current:        usersTable(``),
			desired:        usersTable(hashID),
			wantKind:       MutationKindTablePrimaryVindex,
			wantName:       "users",
			reasonContains: []string{`from no primary vindex to primary vindex "hash" on (id)`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mutations, err := Mutations(tt.current, tt.desired, nil)
			require.NoError(t, err)
			if tt.wantKind == "" {
				assert.Empty(t, mutations)
				return
			}
			require.Len(t, mutations, 1)
			assert.Equal(t, tt.wantKind, mutations[0].Kind)
			assert.Equal(t, tt.wantName, mutations[0].Name)
			for _, want := range tt.reasonContains {
				assert.Contains(t, mutations[0].Reason, want)
			}
		})
	}
}

func TestMutations_AutoIncrementChangesAreUnsafeInEveryKeyspace(t *testing.T) {
	// Vitess uses a configured sequence in sharded and unsharded keyspaces, so
	// every presence transition changes the id source.
	const seq = `, "auto_increment": {"column": "id", "sequence": "users_seq"}`
	unsharded := func(extra string) string { return `{"tables": {"users": {"column_vindexes": []` + extra + `}}}` }
	sharded := func(extra string) string {
		return `{"sharded": true, "vindexes": {"hash": {"type": "hash"}}, "tables": {"users": {"column_vindexes": [{"column": "id", "name": "hash"}]` + extra + `}}}`
	}

	tests := []struct {
		name      string
		current   string
		desired   string
		wantKinds []string
	}{
		{name: "remove while unsharded", current: unsharded(seq), desired: unsharded(""), wantKinds: []string{MutationKindTableAutoIncrement}},
		{name: "add while unsharded", current: unsharded(""), desired: unsharded(seq), wantKinds: []string{MutationKindTableAutoIncrement}},
		{name: "stays sharded", current: sharded(seq), desired: sharded(""), wantKinds: []string{MutationKindTableAutoIncrement}},
		{name: "unsharded to sharded", current: unsharded(seq), desired: sharded(""), wantKinds: []string{MutationKindKeyspaceSharded, MutationKindTablePrimaryVindex, MutationKindTableAutoIncrement}},
		{name: "sharded to unsharded", current: sharded(seq), desired: unsharded(""), wantKinds: []string{MutationKindKeyspaceSharded, MutationKindTableAutoIncrement}},
		{
			name:      "re-pointing the sequence in an unsharded keyspace",
			current:   unsharded(seq),
			desired:   unsharded(`, "auto_increment": {"column": "id", "sequence": "users_seq_v2"}`),
			wantKinds: []string{MutationKindTableAutoIncrement},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mutations, err := Mutations(tt.current, tt.desired, nil)
			require.NoError(t, err)
			var kinds []string
			for _, m := range mutations {
				kinds = append(kinds, m.Kind)
			}
			assert.Equal(t, tt.wantKinds, kinds)
		})
	}
}

// Sharding a keyspace and giving its tables their first primary vindex happen
// in the same document, and each re-routes rows on its own, so both are
// disclosed rather than folding the table's change into the keyspace flip.
func TestMutations_ShardingAlsoReportsFirstPrimaryVindex(t *testing.T) {
	current := `{"tables": {"users": {}}}`
	desired := `{"sharded": true, "vindexes": {"hash": {"type": "hash"}}, "tables": {"users": {"column_vindexes": [{"column": "id", "name": "hash"}]}}}`
	mutations, err := Mutations(current, desired, nil)
	require.NoError(t, err)
	require.Len(t, mutations, 2)
	assert.Equal(t, MutationKindKeyspaceSharded, mutations[0].Kind)
	assert.Equal(t, MutationKindTablePrimaryVindex, mutations[1].Kind)
	assert.Equal(t, "users", mutations[1].Name)
}

func TestMutations_PrimaryVindexRemovedAlsoReportsNewPrimary(t *testing.T) {
	// Dropping the primary vindex's association promotes the next one to
	// primary. Deletions discloses the lost association; Mutations discloses
	// that every row is now keyed by a different vindex.
	current := `{"sharded": true, "vindexes": {"hash": {"type": "hash"}, "xxhash": {"type": "xxhash"}}, "tables": {"users": {"column_vindexes": [{"column": "id", "name": "hash"}, {"column": "email", "name": "xxhash"}]}}}`
	desired := `{"sharded": true, "vindexes": {"hash": {"type": "hash"}, "xxhash": {"type": "xxhash"}}, "tables": {"users": {"column_vindexes": [{"column": "email", "name": "xxhash"}]}}}`
	mutations, err := Mutations(current, desired, nil)
	require.NoError(t, err)
	require.Len(t, mutations, 1)
	assert.Equal(t, MutationKindTablePrimaryVindex, mutations[0].Kind)
	assert.Contains(t, mutations[0].Reason, `from "hash" on (id) to "xxhash" on (email)`)
}

func TestMutations_RemovedTableNotDuplicated(t *testing.T) {
	// A table that leaves the VSchema is a removal reported by Deletions; its
	// primary vindex and auto-increment are not also reported as mutations.
	current := `{"sharded": true, "vindexes": {"hash": {"type": "hash"}}, "tables": {"users": {"column_vindexes": [{"column": "id", "name": "hash"}], "auto_increment": {"column": "id", "sequence": "users_seq"}}}}`
	desired := `{"sharded": true, "vindexes": {"hash": {"type": "hash"}}, "tables": {}}`
	mutations, err := Mutations(current, desired, nil)
	require.NoError(t, err)
	assert.Empty(t, mutations)
}

func TestMutations_UnparseableCurrentFailsClosed(t *testing.T) {
	_, err := Mutations("{not json", shardedVSchema, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse current VSchema")
}

func TestMutations_UnparseableDesiredFailsClosed(t *testing.T) {
	_, err := Mutations(shardedVSchema, "{not json", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse desired VSchema")
}

// unsafeChanges returns what the plan would disclose as unsafe: the removals
// and the in-place changes together.
func unsafeChanges(t *testing.T, current, desired string, liveTables []string) []string {
	t.Helper()
	deletions, err := Deletions(current, desired)
	require.NoError(t, err)
	mutations, err := Mutations(current, desired, liveTables)
	require.NoError(t, err)
	var reasons []string
	for _, d := range deletions {
		reasons = append(reasons, d.Reason)
	}
	for _, m := range mutations {
		reasons = append(reasons, m.Reason)
	}
	return reasons
}

// A pinned table routes every row to the shard owning its pinned keyspace id.
// Re-pinning it, or swapping the pin and a primary vindex in either
// direction, re-routes the whole table while the rows stay where they are.
// require_explicit_routing takes every table in the keyspace out of global
// routing, so unqualified queries against them stop resolving. A reference
// table's source names where its rows come from and its writes go.
func TestMutations_OtherRoutingFieldsAreUnsafe(t *testing.T) {
	const vindexes = `"vindexes": {"hash": {"type": "hash"}}`
	pinned := func(pin string) string {
		return `{"sharded": true, ` + vindexes + `, "tables": {"settings": {"pinned": "` + pin + `"}}}`
	}
	vindexed := `{"sharded": true, ` + vindexes + `, "tables": {"settings": {"column_vindexes": [{"column": "id", "name": "hash"}]}}}`
	reference := func(source string) string {
		return `{"sharded": true, ` + vindexes + `, "tables": {"countries": {"type": "reference", "source": "` + source + `"}}}`
	}

	t.Run("re-pin", func(t *testing.T) {
		mutations, err := Mutations(pinned("80"), pinned("c0"), nil)
		require.NoError(t, err)
		require.Len(t, mutations, 1)
		assert.Equal(t, MutationKindTablePinned, mutations[0].Kind)
		assert.Equal(t, "settings", mutations[0].Name)
		assert.Contains(t, mutations[0].Reason, `from keyspace id "80" to "c0"`)
	})
	t.Run("same pin is not a mutation", func(t *testing.T) {
		assert.Empty(t, unsafeChanges(t, pinned("80"), pinned("80"), nil))
	})
	t.Run("pin replaced by a primary vindex", func(t *testing.T) {
		mutations, err := Mutations(pinned("80"), vindexed, nil)
		require.NoError(t, err)
		require.Len(t, mutations, 1)
		assert.Equal(t, MutationKindTablePrimaryVindex, mutations[0].Kind)
		assert.Contains(t, mutations[0].Reason, `from a pin to keyspace id "80" to primary vindex "hash" on (id)`)
	})
	t.Run("primary vindex replaced by a pin", func(t *testing.T) {
		mutations, err := Mutations(vindexed, pinned("80"), nil)
		require.NoError(t, err)
		require.Len(t, mutations, 1)
		assert.Equal(t, MutationKindTablePrimaryVindex, mutations[0].Kind)
		assert.Contains(t, mutations[0].Reason, `from primary vindex "hash" on (id) to a pin to keyspace id "80"`)
		assert.Len(t, unsafeChanges(t, vindexed, pinned("80"), nil), 2, "the lost association is disclosed as well")
	})
	t.Run("require explicit routing", func(t *testing.T) {
		for name, tc := range map[string]struct{ current, desired, reason string }{
			"on":  {`{"tables": {"users": {}}}`, `{"require_explicit_routing": true, "tables": {"users": {}}}`, "starts requiring explicit routing"},
			"off": {`{"require_explicit_routing": true, "tables": {"users": {}}}`, `{"tables": {"users": {}}}`, "stops requiring explicit routing"},
		} {
			t.Run(name, func(t *testing.T) {
				mutations, err := Mutations(tc.current, tc.desired, nil)
				require.NoError(t, err)
				require.Len(t, mutations, 1)
				assert.Equal(t, MutationKindKeyspaceRequireExplicitRouting, mutations[0].Kind)
				assert.Empty(t, mutations[0].Name)
				assert.Contains(t, mutations[0].Reason, tc.reason)
			})
		}
	})
	t.Run("reference source re-pointed", func(t *testing.T) {
		mutations, err := Mutations(reference("geo"), reference("geo_v2"), nil)
		require.NoError(t, err)
		require.Len(t, mutations, 1)
		assert.Equal(t, MutationKindTableSource, mutations[0].Kind)
		assert.Equal(t, "countries", mutations[0].Name)
		assert.Contains(t, mutations[0].Reason, `from "geo" to "geo_v2"`)
	})
}

// An unsharded keyspace routes every table it holds whether or not its VSchema
// lists the table. Adding a sequence to a live table through a new entry, or
// through the keyspace's first VSchema, changes where that table's ids come
// from. A table the plan creates is not live yet, so its entry is an addition,
// and a sharded keyspace cannot route an unlisted table, so a new entry there
// is onboarding rather than a change.
func TestMutations_UnshardedNewEntryAutoIncrementIsUnsafe(t *testing.T) {
	const users = `"users": {"auto_increment": {"column": "id", "sequence": "users_seq"}}, "users_seq": {"type": "sequence"}`
	live := []string{"orders", "users"}

	t.Run("new entry", func(t *testing.T) {
		mutations, err := Mutations(`{"tables": {"orders": {}}}`, `{"tables": {"orders": {}, `+users+`}}`, live)
		require.NoError(t, err)
		require.Len(t, mutations, 1)
		assert.Equal(t, MutationKindTableAutoIncrement, mutations[0].Kind)
		assert.Equal(t, "users", mutations[0].Name)
		assert.Contains(t, mutations[0].Reason, `already exists in this unsharded keyspace and starts using sequence "users_seq" for column "id"`)
	})
	t.Run("first VSchema", func(t *testing.T) {
		mutations, err := Mutations(`{}`, `{"tables": {`+users+`}}`, live)
		require.NoError(t, err)
		require.Len(t, mutations, 1)
		assert.Equal(t, MutationKindTableAutoIncrement, mutations[0].Kind)
		assert.Equal(t, "users", mutations[0].Name)
	})
	t.Run("table the plan creates", func(t *testing.T) {
		assert.Empty(t, unsafeChanges(t, `{"tables": {"orders": {}}}`, `{"tables": {"orders": {}, `+users+`}}`, []string{"orders"}))
	})
	t.Run("new entry without a sequence", func(t *testing.T) {
		assert.Empty(t, unsafeChanges(t, `{"tables": {"orders": {}}}`, `{"tables": {"orders": {}, "users": {}}}`, live))
	})
	t.Run("sharded keyspace", func(t *testing.T) {
		const vindexes = `"vindexes": {"hash": {"type": "hash"}}`
		current := `{"sharded": true, ` + vindexes + `, "tables": {"orders": {"column_vindexes": [{"column": "id", "name": "hash"}]}}}`
		desired := `{"sharded": true, ` + vindexes + `, "tables": {"orders": {"column_vindexes": [{"column": "id", "name": "hash"}]}, "users": {"column_vindexes": [{"column": "id", "name": "hash"}], "auto_increment": {"column": "id", "sequence": "users_seq"}}}}`
		assert.Empty(t, unsafeChanges(t, current, desired, live))
	})
}

// The same vindex on a different column becomes the primary while the old
// association stays as a secondary, so no association is removed and only
// the primary-vindex comparison can see the change.
func TestMutations_SameVindexNewPrimaryColumn(t *testing.T) {
	current := `{"sharded": true, "vindexes": {"hash": {"type": "hash"}}, "tables": {"users": {"column_vindexes": [{"column": "id", "name": "hash"}]}}}`
	desired := `{"sharded": true, "vindexes": {"hash": {"type": "hash"}}, "tables": {"users": {"column_vindexes": [{"column": "user_id", "name": "hash"}, {"column": "id", "name": "hash"}]}}}`
	deletions, err := Deletions(current, desired)
	require.NoError(t, err)
	assert.Empty(t, deletions)
	mutations, err := Mutations(current, desired, nil)
	require.NoError(t, err)
	require.Len(t, mutations, 1)
	assert.Equal(t, MutationKindTablePrimaryVindex, mutations[0].Kind)
	assert.Contains(t, mutations[0].Reason, `from "hash" on (id) to "hash" on (user_id)`)
}
