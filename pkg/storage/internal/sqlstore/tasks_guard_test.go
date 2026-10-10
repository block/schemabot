package sqlstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// taskUpdateStatement renders the task update for each lease guard and
// dialect. These tests pin the exact SQL: the task ID and then the guard's
// placeholders follow the SET placeholders, every rendering stamps updated_at,
// a leased rendering checks its token through the dialect's lease fence, which
// on PostgreSQL locks the lease row, and the absence rendering reads the
// operation's lease through the same gate the reapers use.
func TestTaskUpdateStatement(t *testing.T) {
	const sets = "state = ?, error_message = ?, options = ?, attempt = ?, rows_copied = ?, rows_total = ?, progress_percent = ?, eta_seconds = ?, checksum_rows_checked = ?, checksum_rows_total = ?, throttled = ?, throttle_reason = ?, execution_mode = ?, mode_reason = ?, cutover_attempts = ?, is_instant = ?, engine_migration_id = ?, ddl = ?, started_at = ?, completed_at = ?, updated_at = NOW()"
	const mysqlSets = "t.state = ?, t.error_message = ?, t.options = ?, t.attempt = ?, t.rows_copied = ?, t.rows_total = ?, t.progress_percent = ?, t.eta_seconds = ?, t.checksum_rows_checked = ?, t.checksum_rows_total = ?, t.throttled = ?, t.throttle_reason = ?, t.execution_mode = ?, t.mode_reason = ?, t.cutover_attempts = ?, t.is_instant = ?, t.engine_migration_id = ?, t.ddl = ?, t.started_at = ?, t.completed_at = ?, t.updated_at = NOW()"

	tests := []struct {
		name    string
		guard   taskLeaseGuard
		dialect Dialect
		want    string
	}{
		{
			name:    "no lease renders an unguarded single-table update on MySQL",
			guard:   taskGuardNone,
			dialect: MySQLDialect{},
			want:    "UPDATE tasks SET " + sets + " WHERE id = ?",
		},
		{
			name:    "no lease renders an unguarded single-table update on PostgreSQL",
			guard:   taskGuardNone,
			dialect: PostgresDialect{},
			want:    "UPDATE tasks SET " + sets + " WHERE id = ?",
		},
		{
			name:    "operation lease joins the task's operation row on MySQL",
			guard:   taskGuardOperation,
			dialect: MySQLDialect{},
			want:    "UPDATE tasks t JOIN apply_operations ao ON ao.id = t.apply_operation_id SET " + mysqlSets + " WHERE t.id = ? AND ao.id = ? AND ao.lease_token = ?",
		},
		{
			name:    "operation lease joins the task's operation row on PostgreSQL and locks it",
			guard:   taskGuardOperation,
			dialect: PostgresDialect{},
			want:    "UPDATE tasks t SET " + sets + " FROM apply_operations ao WHERE (ao.id = t.apply_operation_id) AND (t.id = ? AND ao.id = ? AND ao.id = (SELECT fence.id FROM apply_operations fence WHERE fence.id = ao.id AND fence.lease_token = ? FOR SHARE))",
		},
		{
			name:    "apply lease joins the parent applies row on MySQL",
			guard:   taskGuardApply,
			dialect: MySQLDialect{},
			want:    "UPDATE tasks t JOIN applies a ON a.id = t.apply_id SET " + mysqlSets + " WHERE t.id = ? AND a.lease_token = ?",
		},
		{
			name:    "apply lease joins the parent applies row on PostgreSQL and locks it",
			guard:   taskGuardApply,
			dialect: PostgresDialect{},
			want:    "UPDATE tasks t SET " + sets + " FROM applies a WHERE (a.id = t.apply_id) AND (t.id = ? AND a.id = (SELECT fence.id FROM applies fence WHERE fence.id = a.id AND fence.lease_token = ? FOR SHARE))",
		},
		{
			name:    "operation lease absence pins the task to its operation and admits it only while the operation is unleased on MySQL",
			guard:   taskGuardOperationAbsent,
			dialect: MySQLDialect{},
			want: "UPDATE tasks SET " + sets + " WHERE id = ? AND apply_id = ? AND apply_operation_id = ? AND NOT EXISTS (\n" +
				"\t\t\tSELECT 1\n" +
				"\t\t\tFROM apply_operations lease_holder\n" +
				"\t\t\tWHERE lease_holder.id = tasks.apply_operation_id\n" +
				"\t\t\t\tAND lease_holder.lease_owner <> ''\n" +
				"\t\t\t\tAND lease_holder.updated_at >= NOW() - INTERVAL 60000000 MICROSECOND\n" +
				"\t\t)",
		},
		{
			name:    "operation lease absence pins the task to its operation and admits it only while the operation is unleased on PostgreSQL",
			guard:   taskGuardOperationAbsent,
			dialect: PostgresDialect{},
			want: "UPDATE tasks SET " + sets + " WHERE id = ? AND apply_id = ? AND apply_operation_id = ? AND NOT EXISTS (\n" +
				"\t\t\tSELECT 1\n" +
				"\t\t\tFROM apply_operations lease_holder\n" +
				"\t\t\tWHERE lease_holder.id = tasks.apply_operation_id\n" +
				"\t\t\t\tAND lease_holder.lease_owner <> ''\n" +
				"\t\t\t\tAND lease_holder.updated_at >= now() - 60000000 * interval '1 microsecond'\n" +
				"\t\t)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, taskUpdateStatement(tc.dialect, tc.guard))
		})
	}
}

// An unknown guard must not fall back to an unguarded write.
func TestTaskUpdateStatementRejectsUnknownGuard(t *testing.T) {
	assert.Panics(t, func() { taskUpdateStatement(MySQLDialect{}, taskLeaseGuard(99)) })
}
