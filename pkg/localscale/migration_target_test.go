package localscale

import (
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func migrationRow(keyspace, shard, uuid string) map[string]string {
	return map[string]string{"_keyspace": keyspace, "shard": shard, "migration_uuid": uuid}
}

func postponedRow(keyspace, shard, uuid, postponeCompletion string) map[string]string {
	row := migrationRow(keyspace, shard, uuid)
	row["postpone_completion"] = postponeCompletion
	return row
}

// Vitess clears postpone_completion as soon as it accepts a COMPLETE, so only
// a row that still carries it is waiting to be told to cut over. Telling a
// shard again while its cutover holds the tables locked is what wedges the
// deploy request, so a shard drops out of the next round the moment it is
// no longer postponed — one shard of a keyspace can still be waiting while
// its sibling has already begun.
func TestAwaitingCompletionSelectsOnlyPostponedShards(t *testing.T) {
	migrations := []map[string]string{
		postponedRow("testapp_sharded", "-80", "abc", "1"),
		postponedRow("testapp_sharded", "80-", "abc", "0"),
		migrationRow("testapp", "0", "no-column"),
	}

	assert.Equal(t, []map[string]string{
		postponedRow("testapp_sharded", "-80", "abc", "1"),
	}, filterMigrations(migrations, awaitingCompletion))
}

func allKeyspacesKnown(string) bool { return true }

// A migration on a sharded keyspace runs once per shard and reports one row
// per shard, all carrying the same UUID. Each row is one shard's piece of the
// work and has to resolve to its own target: grouping by keyspace alone leaves
// two sends addressed at the whole keyspace, and since vtgate scatters each of
// them, every shard receives the statement twice.
func TestGroupMigrationsByShardAddressesEveryShard(t *testing.T) {
	migrations := []map[string]string{
		migrationRow("testapp_sharded", "-80", "abc"),
		migrationRow("testapp_sharded", "80-", "abc"),
	}

	targets, err := groupMigrationsByShard(migrations, "ctx", allKeyspacesKnown, quietLogger())
	require.NoError(t, err)

	assert.Equal(t, map[migrationTarget][]string{
		{keyspace: "testapp_sharded", shard: "-80"}: {"abc"},
		{keyspace: "testapp_sharded", shard: "80-"}: {"abc"},
	}, targets)
}

func TestGroupMigrationsByShardKeepsKeyspacesApart(t *testing.T) {
	migrations := []map[string]string{
		migrationRow("testapp", "0", "one"),
		migrationRow("testapp_sharded", "-80", "two"),
		migrationRow("testapp_sharded", "80-", "two"),
		migrationRow("testapp_sharded", "-80", "three"),
	}

	targets, err := groupMigrationsByShard(migrations, "ctx", allKeyspacesKnown, quietLogger())
	require.NoError(t, err)

	assert.Equal(t, map[migrationTarget][]string{
		{keyspace: "testapp", shard: "0"}:           {"one"},
		{keyspace: "testapp_sharded", shard: "-80"}: {"two", "three"},
		{keyspace: "testapp_sharded", shard: "80-"}: {"two"},
	}, targets)
}

// A row that cannot be addressed stops the whole operation. Acting on the rows
// around it would complete some shards of a cutover and abandon the one whose
// address was unreadable, which is the state this addressing exists to avoid.
func TestGroupMigrationsByShardRejectsUnaddressableRows(t *testing.T) {
	tests := []struct {
		name string
		row  map[string]string
		want string
	}{
		{"no uuid", migrationRow("testapp_sharded", "-80", ""), "missing uuid"},
		{"no keyspace", migrationRow("", "-80", "abc"), "missing keyspace"},
		{"no shard", migrationRow("testapp_sharded", "", "abc"), "missing shard"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			targets, err := groupMigrationsByShard(
				[]map[string]string{migrationRow("testapp", "0", "fine"), tt.row},
				"ctx", allKeyspacesKnown, quietLogger())
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
			assert.Nil(t, targets, "no target may be returned alongside an error, or the caller acts on a subset")
		})
	}
}

// A row SchemaBot cannot act on is dropped rather than failing the operation:
// it belongs to a keyspace this backend does not serve, or carries a UUID that
// would not be safe to interpolate into the statement.
func TestGroupMigrationsByShardDropsRowsItCannotActOn(t *testing.T) {
	migrations := []map[string]string{
		migrationRow("testapp", "0", "keep"),
		migrationRow("elsewhere", "0", "other-keyspace"),
		migrationRow("testapp", "0", "bad'uuid"),
	}
	known := func(keyspace string) bool { return keyspace == "testapp" }

	targets, err := groupMigrationsByShard(migrations, "ctx", known, quietLogger())
	require.NoError(t, err)

	assert.Equal(t, map[migrationTarget][]string{
		{keyspace: "testapp", shard: "0"}: {"keep"},
	}, targets)
}
