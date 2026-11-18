// driver_pools.go implements DriverPoolStore, the driver pool of each server
// running an operator.
package sqlstore

import (
	"context"
	"fmt"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
)

// driverPoolStore implements storage.DriverPoolStore.
type driverPoolStore struct {
	db      *rebindDB
	dialect Dialect
}

// Heartbeat upserts the server's row, stamping updated_at, the row's
// heartbeat, at the database's clock.
func (s *driverPoolStore) Heartbeat(ctx context.Context, server *storage.DriverPool) error {
	if server == nil || server.ServerID == "" {
		return fmt.Errorf("heartbeat driver pool: server ID is required")
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
		INSERT INTO driver_pools (server_id, drivers, updated_at)
		VALUES (?, ?, `+now+`)
		`+upsert, server.ServerID, server.Drivers); err != nil {
		return fmt.Errorf("heartbeat driver pool %s: %w", server.ServerID, err)
	}
	return nil
}

// PruneStale deletes the rows not heard from within
// storage.DriverPoolRetention, at the database's clock.
func (s *driverPoolStore) PruneStale(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `
		DELETE FROM driver_pools
		WHERE updated_at < `+driverPoolCutoff(s.dialect, storage.DriverPoolRetention.Microseconds()),
	); err != nil {
		return fmt.Errorf("remove driver pools not heard from within %s: %w", storage.DriverPoolRetention, err)
	}
	return nil
}

// Deregister deletes the server's row.
func (s *driverPoolStore) Deregister(ctx context.Context, serverID string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM driver_pools WHERE server_id = ?`, serverID); err != nil {
		return fmt.Errorf("deregister driver pool %s: %w", serverID, err)
	}
	return nil
}

// ListLive returns the servers that heartbeated within
// storage.DriverPoolLiveWindow.
func (s *driverPoolStore) ListLive(ctx context.Context) ([]*storage.DriverPool, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT server_id, drivers, created_at, updated_at
		FROM driver_pools
		WHERE updated_at >= `+driverPoolCutoff(s.dialect, storage.DriverPoolLiveWindow.Microseconds())+`
		ORDER BY server_id`)
	if err != nil {
		return nil, fmt.Errorf("query live driver pools: %w", err)
	}
	defer utils.CloseAndLog(rows)

	var servers []*storage.DriverPool
	for rows.Next() {
		var server storage.DriverPool
		if err := rows.Scan(&server.ServerID, &server.Drivers, &server.CreatedAt, &server.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan live driver pool: %w", err)
		}
		servers = append(servers, &server)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read live driver pools: %w", err)
	}
	return servers, nil
}

// driverPoolCutoff is the database-clock time micros before now.
func driverPoolCutoff(dialect Dialect, micros int64) string {
	return dialect.RelativeTime(TimestampPrecisionDefault, BeforeCurrentTime, LiteralIntervalAmount(uint64(micros)), IntervalMicrosecond)
}
