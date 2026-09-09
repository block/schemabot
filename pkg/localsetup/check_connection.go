package localsetup

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/mysqlconn"
	"github.com/block/schemabot/pkg/postgresconn"
	"github.com/block/spirit/pkg/utils"
)

// CheckConnection verifies access without creating metadata or changing schemas.
func CheckConnection(ctx context.Context, engine, dsn string) error {
	if strings.TrimSpace(dsn) == "" {
		return fmt.Errorf("set a connection variable before checking the connection")
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	var db *sql.DB
	var err error
	switch engine {
	case "mysql":
		db, err = mysqlconn.Open(dsn)
	case "postgres":
		db, err = postgresconn.Open(dsn)
	default:
		return fmt.Errorf("connection checks are not available for this engine")
	}
	if err != nil {
		slog.DebugContext(ctx, "read setup connection failed", "engine", engine, "error", err)
		return fmt.Errorf("we couldn’t read that connection; check its format and try again")
	}
	defer utils.CloseAndLog(db)
	// An actual query also exercises already-established MySQL connections.
	var one int
	if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
		slog.DebugContext(ctx, "check setup connection failed", "engine", engine, "error", err)
		return fmt.Errorf("we couldn’t connect; check the address, credentials, and network, then try again")
	}
	return nil
}
