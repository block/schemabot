package sqlstore

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/state"
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
			want:    "UPDATE apply_operations ao SET state = ?, started_at = COALESCE(ao.started_at, NOW()), updated_at = NOW() FROM applies a WHERE (a.id = ao.apply_id) AND (ao.id = ? AND ao.apply_id = ? AND a.id = (SELECT fence.id FROM applies fence WHERE fence.id = a.id AND fence.lease_token = ? FOR SHARE))",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.guard.updateStatement(tc.dialect, assignments))
		})
	}
}

// First-start admission binds the earlier member's failure state ahead of the
// cutover policy arms, with the release exemption and then the table-step
// boundary bound last, on both dialects. The gate's behavior on each dialect is covered by the storage
// parity suite; this pins only the placeholder-to-argument alignment, which
// no behavioral test can catch when a binding shifts into a neighbor's slot
// of the same type.
func TestWorkStartGateFailureAdmission(t *testing.T) {
	for _, tc := range []struct {
		name    string
		dialect Dialect
	}{
		{name: "mysql", dialect: MySQLDialect{}},
		{name: "postgres", dialect: PostgresDialect{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gate := workStartGateSQL(tc.dialect)
			args := workStartGateArgs()
			assert.Equal(t, strings.Count(gate, "?"), len(args), "each admission placeholder has a positional argument")
			assert.Equal(t, []any{
				state.ApplyOperation.Failed,
				storage.CutoverPolicyBarrier,
				state.ApplyOperation.WaitingForCutover,
				state.ApplyOperation.CuttingOver,
				state.ApplyOperation.RevertWindow,
				state.ApplyOperation.Completed,
				storage.ApplyOperationKindGroupFinalizer,
				state.ApplyOperation.Pending,
				state.ApplyOperation.Running,
				storage.CutoverPolicyBarrier,
				storage.CutoverPolicyParallel,
				state.ApplyOperation.Completed,
				state.ApplyOperation.Failed,
				storage.ApplyOperationKindGroupFinalizer,
				state.ApplyOperation.Pending,
				state.ApplyOperation.Stopped,
				storage.ApplyOperationKindWork,
				state.ApplyOperation.Failed,
				storage.OnFailureContinue,
				storage.OnFailurePause,
				storage.ControlOperationRelease,
				storage.ControlRequestPending,
				storage.ControlRequestCompleted,
				state.ApplyOperation.Completed,
			}, args)
		})
	}
}

// The claim embeds the same first-start gate for pending and stopped-before-start
// work. Its already-started stopped-work arm stays ungated so failure admission
// never prevents an operator from resuming a copy already under way.
func TestOperationClaimFirstStartGateBindings(t *testing.T) {
	for _, tc := range []struct {
		name    string
		dialect Dialect
	}{
		{name: "mysql", dialect: MySQLDialect{}},
		{name: "postgres", dialect: PostgresDialect{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, connector := newRecordingRebindDB(t, &countingBinder{})
			store := &applyOperationStore{db: db, dialect: tc.dialect, maxDriversPerApply: 2}
			claimed, err := store.FindNextApplyOperation(t.Context(), "driver-a")
			require.NoError(t, err)
			require.Nil(t, claimed, "the recording driver returns no candidate rows")
			require.Len(t, connector.queries, 1)
			require.Len(t, connector.args, 1)
			query := connector.queries[0]
			assert.Equal(t, strings.Count(query, "?"), len(connector.args[0]), "the added failure placeholder cannot shift another claim arm's bindings")
			assert.Equal(t, 2, strings.Count(query, operationStartGateSQL(tc.dialect)),
				"pending and stopped-before-start rows share the admission gate")
			assert.Contains(t, strings.Join(strings.Fields(query), " "),
				"apply_operations.operation_kind <> ? AND apply_operations.started_at IS NOT NULL ) OR (",
				"a stopped work row that already started resumes without the admission gate")
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
