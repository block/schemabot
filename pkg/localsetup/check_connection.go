package localsetup

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
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
	case "vitess":
		return checkPlanetScaleEndpoint(ctx, dsn)
	default:
		return fmt.Errorf("connection checks are not available for this engine")
	}
	if err != nil {
		slog.DebugContext(ctx, "read setup connection failed", "engine", engine, "error", err)
		return &setupConnectionError{message: "we couldn’t read that connection; check its format and try again", cause: err}
	}
	defer utils.CloseAndLog(db)
	// An actual query also exercises already-established MySQL connections.
	var one int
	if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
		slog.DebugContext(ctx, "check setup connection failed", "engine", engine, "error", err)
		return connectionCheckError("we couldn’t connect; check the address, credentials, and network, then try again", err)
	}
	return nil
}

// checkPlanetScaleEndpoint confirms a PlanetScale-compatible API answers before
// anything is registered against it. A Vitess target is addressed over HTTP
// rather than a database connection, so there is no DSN to dial; the engine's
// own calls carry the service token, and this check only establishes that the
// endpoint is there.
func checkPlanetScaleEndpoint(ctx context.Context, endpoint string) error {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		slog.DebugContext(ctx, "read setup endpoint failed", "engine", "vitess", "error", err)
		return &setupConnectionError{message: "we couldn’t read that endpoint; give it as http://host:port or https://host"}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(endpoint, "/")+"/health", nil)
	if err != nil {
		return &setupConnectionError{message: "we couldn’t read that endpoint; give it as http://host:port or https://host", cause: err}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		slog.DebugContext(ctx, "check setup endpoint failed", "engine", "vitess", "error", err)
		return &setupConnectionError{message: "we couldn’t reach that endpoint; check the address and network, then try again", cause: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		slog.DebugContext(ctx, "check setup endpoint failed", "engine", "vitess", "status", resp.StatusCode)
		return &setupConnectionError{message: "that endpoint answered, but not as a healthy PlanetScale-compatible API; check the address and try again"}
	}
	return nil
}
