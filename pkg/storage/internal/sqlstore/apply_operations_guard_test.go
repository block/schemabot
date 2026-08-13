package sqlstore

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/block/schemabot/pkg/storage"
)

// updateStatement renders the guarded single-row UPDATE for each guard kind
// and dialect. These tests pin the exact SQL: the WHERE placeholders must
// follow the SET placeholders (callers append the row ID and then the guard
// args after their SET arguments), the updated_at heartbeat stamp must be
// appended to every rendering, and the lease predicates must match the guard
// kind.
func TestOperationWriteGuardUpdateStatement(t *testing.T) {
	assignments := []JoinedUpdateAssignment{
		{Column: "state", Expr: "?"},
		{Column: "started_at", Expr: "COALESCE(ao.started_at, NOW())"},
	}

	tests := []struct {
		name    string
		guard   operationWriteGuard
		dialect Dialect
		want    string
	}{
		{
			name:    "no guard renders an unguarded single-table update",
			guard:   operationWriteGuard{kind: operationGuardNone},
			dialect: MySQLDialect{},
			want:    "UPDATE apply_operations ao SET state = ?, started_at = COALESCE(ao.started_at, NOW()), updated_at = NOW() WHERE ao.id = ?",
		},
		{
			name:    "operation lease adds the row's own lease predicate",
			guard:   operationWriteGuard{kind: operationGuardOperation, opLease: storage.OperationLease{Token: "tok"}},
			dialect: MySQLDialect{},
			want:    "UPDATE apply_operations ao SET state = ?, started_at = COALESCE(ao.started_at, NOW()), updated_at = NOW() WHERE ao.id = ? AND ao.lease_token = ?",
		},
		{
			name:    "single-table rendering is dialect-independent",
			guard:   operationWriteGuard{kind: operationGuardOperation, opLease: storage.OperationLease{Token: "tok"}},
			dialect: PostgresDialect{},
			want:    "UPDATE apply_operations ao SET state = ?, started_at = COALESCE(ao.started_at, NOW()), updated_at = NOW() WHERE ao.id = ? AND ao.lease_token = ?",
		},
		{
			name:    "apply lease joins the parent applies row on MySQL",
			guard:   operationWriteGuard{kind: operationGuardApply, applyLease: storage.ApplyLease{Token: "tok"}},
			dialect: MySQLDialect{},
			want:    "UPDATE apply_operations ao JOIN applies a ON a.id = ao.apply_id SET ao.state = ?, ao.started_at = COALESCE(ao.started_at, NOW()), ao.updated_at = NOW() WHERE ao.id = ? AND ao.apply_id = ? AND a.lease_token = ?",
		},
		{
			name:    "apply lease joins the parent applies row on PostgreSQL and locks it",
			guard:   operationWriteGuard{kind: operationGuardApply, applyLease: storage.ApplyLease{Token: "tok"}},
			dialect: PostgresDialect{},
			want:    "UPDATE apply_operations ao SET state = ?, started_at = COALESCE(ao.started_at, NOW()), updated_at = NOW() FROM applies a WHERE (a.id = ao.apply_id) AND (ao.id = ? AND ao.apply_id = ? AND a.id = (SELECT fence.id FROM applies fence WHERE fence.id = a.id AND fence.lease_token = ? FOR UPDATE))",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.guard.updateStatement(tc.dialect, assignments))
		})
	}
}

// A nil assignment slice still stamps updated_at, so heartbeat-only writes
// renew the lease liveness signal on every dialect.
func TestOperationWriteGuardUpdateStatementHeartbeatOnly(t *testing.T) {
	guard := operationWriteGuard{kind: operationGuardOperation, opLease: storage.OperationLease{Token: "tok"}}
	assert.Equal(t,
		"UPDATE apply_operations ao SET updated_at = NOW() WHERE ao.id = ? AND ao.lease_token = ?",
		guard.updateStatement(MySQLDialect{}, nil))
}
