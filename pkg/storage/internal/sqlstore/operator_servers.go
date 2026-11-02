// operator_servers.go implements OperatorServerStore, the registry of servers
// running an operator, and the deferred cutover admission read from it.
package sqlstore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
)

// operatorServerStore implements storage.OperatorServerStore.
type operatorServerStore struct {
	db      *rebindDB
	dialect Dialect
}

// Heartbeat upserts the server's row, stamping updated_at, the row's
// heartbeat, at the database's clock, then removes
// rows not heard from within storage.OperatorServerRetention.
func (s *operatorServerStore) Heartbeat(ctx context.Context, server *storage.OperatorServer) error {
	if server == nil || server.ServerID == "" {
		return fmt.Errorf("heartbeat operator server: server ID is required")
	}
	now := s.dialect.CurrentTimestamp(TimestampPrecisionDefault)
	upsert := s.dialect.UpsertClause(
		[]string{"server_id"},
		[]UpsertAssignment{
			{Column: "drivers"},
			{Column: "deferred_cutover_drivers"},
			{Column: "updated_at", Expr: now},
		},
	)
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO operator_servers (server_id, drivers, deferred_cutover_drivers, updated_at)
		VALUES (?, ?, ?, `+now+`)
		`+upsert, server.ServerID, server.Drivers, server.DeferredCutoverDrivers); err != nil {
		return fmt.Errorf("heartbeat operator server %s: %w", server.ServerID, err)
	}
	if _, err := s.db.ExecContext(ctx, `
		DELETE FROM operator_servers
		WHERE updated_at < `+operatorServerCutoff(s.dialect, storage.OperatorServerRetention.Microseconds()),
	); err != nil {
		return fmt.Errorf("remove operator servers not heard from within %s: %w", storage.OperatorServerRetention, err)
	}
	return nil
}

// Deregister deletes the server's row.
func (s *operatorServerStore) Deregister(ctx context.Context, serverID string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM operator_servers WHERE server_id = ?`, serverID); err != nil {
		return fmt.Errorf("deregister operator server %s: %w", serverID, err)
	}
	return nil
}

// ListLive returns the servers that heartbeated within
// storage.OperatorServerLiveWindow.
func (s *operatorServerStore) ListLive(ctx context.Context) ([]*storage.OperatorServer, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT server_id, drivers, deferred_cutover_drivers, created_at, updated_at
		FROM operator_servers
		WHERE updated_at >= `+operatorServerCutoff(s.dialect, storage.OperatorServerLiveWindow.Microseconds())+`
		ORDER BY server_id`)
	if err != nil {
		return nil, fmt.Errorf("query live operator servers: %w", err)
	}
	defer utils.CloseAndLog(rows)

	var servers []*storage.OperatorServer
	for rows.Next() {
		var server storage.OperatorServer
		if err := rows.Scan(&server.ServerID, &server.Drivers, &server.DeferredCutoverDrivers, &server.CreatedAt, &server.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan live operator server: %w", err)
		}
		servers = append(servers, &server)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read live operator servers: %w", err)
	}
	return servers, nil
}

// operatorServerCutoff is the database-clock time micros before now.
func operatorServerCutoff(dialect Dialect, micros int64) string {
	return dialect.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime, LiteralIntervalAmount(uint64(micros)), IntervalMicrosecond)
}

// deferredCutoverAdmissionLockName serializes deferred cutover admission
// across every server. It is taken after the apply's target lock and before
// the create transaction begins, so the transaction's snapshot sees every
// apply an earlier admission stored, and no other path takes it, so the two
// locks are always taken in the same order.
const deferredCutoverAdmissionLockName = "schemabot_deferred_cutover_admission"

// rowsQueryer runs multi-row queries; both *rebindDB and *rebindTx satisfy it.
type rowsQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// admitDeferredCutover refuses apply with a *storage.DeferredCutoverCapacityError
// when its deferred cutover operations, added to those of the other admitted
// deferred applies of two or more operations, exceed the deferred cutover
// drivers of the live operator servers. An apply of one operation is ready to
// cut over as soon as its own operation waits, so it never holds a driver
// while waiting on another and is neither checked nor counted. The caller
// holds deferredCutoverAdmissionLockName.
func admitDeferredCutover(ctx context.Context, tx rowsQueryer, dialect Dialect, apply *storage.Apply) error {
	if apply.DeferredCutoverOperations <= 1 {
		return nil
	}
	refused := &storage.DeferredCutoverCapacityError{Operations: apply.DeferredCutoverOperations}
	var capacity sql.NullInt64
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*), SUM(deferred_cutover_drivers)
		FROM operator_servers
		WHERE updated_at >= `+operatorServerCutoff(dialect, storage.OperatorServerLiveWindow.Microseconds()),
	).Scan(&refused.LiveServers, &capacity); err != nil {
		return fmt.Errorf("read deferred cutover drivers of live operator servers for apply %s: %w", apply.ApplyIdentifier, err)
	}
	refused.Capacity = int(capacity.Int64)
	if refused.ExceedsCapacity() {
		return refused
	}

	widths, err := listActiveDeferredCutoverWidths(ctx, tx, dialect)
	if err != nil {
		return fmt.Errorf("admit deferred cutover of apply %s: %w", apply.ApplyIdentifier, err)
	}
	for _, width := range widths {
		if width.Operations <= 1 {
			continue
		}
		refused.Reserved += width.Operations
		refused.HoldingApplyIDs = append(refused.HoldingApplyIDs, width.ApplyIdentifier)
	}
	if refused.Operations+refused.Reserved > refused.Capacity {
		return refused
	}
	return nil
}
