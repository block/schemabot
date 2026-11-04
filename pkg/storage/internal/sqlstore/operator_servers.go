// operator_servers.go implements OperatorServerStore, the registry of servers
// running an operator.
package sqlstore

import (
	"context"
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
// heartbeat, at the database's clock.
func (s *operatorServerStore) Heartbeat(ctx context.Context, server *storage.OperatorServer) error {
	if server == nil || server.ServerID == "" {
		return fmt.Errorf("heartbeat operator server: server ID is required")
	}
	now := s.dialect.CurrentTimestamp(TimestampPrecisionDefault)
	upsert := s.dialect.UpsertClause(
		[]string{"server_id"},
		[]UpsertAssignment{
			{Column: "drivers"},
			{Column: "updated_at", Expr: now},
		},
	)
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO operator_servers (server_id, drivers, updated_at)
		VALUES (?, ?, `+now+`)
		`+upsert, server.ServerID, server.Drivers); err != nil {
		return fmt.Errorf("heartbeat operator server %s: %w", server.ServerID, err)
	}
	return nil
}

// PruneStale deletes the rows not heard from within
// storage.OperatorServerRetention, at the database's clock.
func (s *operatorServerStore) PruneStale(ctx context.Context) error {
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
		SELECT server_id, drivers, created_at, updated_at
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
		if err := rows.Scan(&server.ServerID, &server.Drivers, &server.CreatedAt, &server.UpdatedAt); err != nil {
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
