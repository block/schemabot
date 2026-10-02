package sqlstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	mysqlApplySourceFence    = "a.id = (SELECT fence.id FROM applies fence WHERE fence.id = a.id AND fence.lease_token = ? FOR SHARE)"
	postgresApplySourceFence = "a.id = (SELECT fence.id FROM applies fence WHERE fence.id = a.id AND fence.lease_token = ? FOR SHARE)"
)

// The lease-guarded shard task insert selects its row from the lease row, so
// the lease row's ID and then the token bind after the inserted values, and
// the token check locks that row: a share lock on MySQL and an update lock on
// PostgreSQL, so the check does not depend on the session's isolation level.
func TestShardTaskInsertStatement(t *testing.T) {
	const insert = "INSERT INTO tasks (" + shardTaskInsertColumns + ") SELECT ?, ? "

	tests := []struct {
		name       string
		dialect    Dialect
		leaseTable string
		leaseAlias string
		want       string
	}{
		{
			name:       "operation lease selects from the share-locked operation row on MySQL",
			dialect:    MySQLDialect{},
			leaseTable: "apply_operations",
			leaseAlias: "ao",
			want:       insert + "FROM apply_operations ao WHERE ao.id = ? AND ao.id = (SELECT fence.id FROM apply_operations fence WHERE fence.id = ao.id AND fence.lease_token = ? FOR SHARE)",
		},
		{
			name:       "operation lease selects from the update-locked operation row on PostgreSQL",
			dialect:    PostgresDialect{},
			leaseTable: "apply_operations",
			leaseAlias: "ao",
			want:       insert + "FROM apply_operations ao WHERE ao.id = ? AND ao.id = (SELECT fence.id FROM apply_operations fence WHERE fence.id = ao.id AND fence.lease_token = ? FOR SHARE)",
		},
		{
			name:       "apply lease selects from the share-locked applies row on MySQL",
			dialect:    MySQLDialect{},
			leaseTable: "applies",
			leaseAlias: "a",
			want:       insert + "FROM applies a WHERE a.id = ? AND " + mysqlApplySourceFence,
		},
		{
			name:       "apply lease selects from the update-locked applies row on PostgreSQL",
			dialect:    PostgresDialect{},
			leaseTable: "applies",
			leaseAlias: "a",
			want:       insert + "FROM applies a WHERE a.id = ? AND " + postgresApplySourceFence,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, shardTaskInsertStatement(tc.dialect, "?, ?", tc.leaseTable, tc.leaseAlias))
		})
	}
}

// The leased log append selects its row from the leased applies row: the nine
// inserted columns bind first, then the apply ID and the token, and the token
// check locks the applies row on both dialects.
func TestApplyLogLeasedAppendStatement(t *testing.T) {
	const insert = "INSERT INTO apply_logs (apply_id, task_id, level, event_type, source, message, old_state, new_state, metadata) " +
		"SELECT ?, ?, ?, ?, ?, ?, ?, ?, ? FROM applies a WHERE a.id = ? AND "

	assert.Equal(t, insert+mysqlApplySourceFence, applyLogLeasedAppendStatement(MySQLDialect{}))
	assert.Equal(t, insert+postgresApplySourceFence, applyLogLeasedAppendStatement(PostgresDialect{}))
}

// A leased terminal check write admits the update only while the applies row
// still carries the token, read through a locking subquery on both dialects;
// the apply ID binds before the token.
func TestCheckApplyLeasePredicate(t *testing.T) {
	const exists = " AND EXISTS (SELECT 1 FROM applies lease_apply WHERE lease_apply.id = ? AND " +
		"lease_apply.id = (SELECT fence.id FROM applies fence WHERE fence.id = lease_apply.id AND fence.lease_token = ? "

	assert.Equal(t, exists+"FOR SHARE))", checkApplyLeasePredicate(MySQLDialect{}))
	assert.Equal(t, exists+"FOR SHARE))", checkApplyLeasePredicate(PostgresDialect{}))
}
