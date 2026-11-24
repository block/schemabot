package api

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/block/schemabot/pkg/schema"
)

// refusePopulatedPreStatusOutbox runs under the convergence advisory lock, before
// any DDL. Defaulting an existing intent's disposition could replay accepted or
// held notifications. Bootstrap changes shape only; it cannot infer row state.
func refusePopulatedPreStatusOutbox(ctx context.Context, conn *sql.Conn, dialect schema.Dialect) error {
	scope := "DATABASE()"
	if dialect == schema.DialectPostgres {
		scope = "current_schema()"
	}
	var columns, statuses int
	if err := conn.QueryRowContext(ctx, `
		SELECT COUNT(*), COALESCE(SUM(CASE WHEN column_name = 'status' THEN 1 ELSE 0 END), 0)
		FROM information_schema.columns
		WHERE table_schema = `+scope+` AND table_name = 'outbox_events'
	`).Scan(&columns, &statuses); err != nil {
		return fmt.Errorf("inspect notification status schema: %w", err)
	}
	if columns == 0 || statuses != 0 {
		slog.DebugContext(ctx, "notification status guard permits fresh or status-bearing storage", "columns", columns, "status_columns", statuses)
		return nil
	}
	var populated bool
	if err := conn.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM outbox_events)`).Scan(&populated); err != nil {
		return fmt.Errorf("inspect pre-status notification intent: %w", err)
	}
	if populated {
		return fmt.Errorf("unsupported pre-release storage: populated outbox_events has no status column; convergence refused without changing notification state or schema")
	}
	return nil
}
