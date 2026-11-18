//go:build integration

package api

import (
	"database/sql"
	"log/slog"
	"os"
	"testing"

	_ "github.com/block/mysql"
	"github.com/block/spirit/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage/mysqlstore"
)

func TestNew_Integration(t *testing.T) {
	ctx := t.Context()

	dsn := newStorageDatabaseWithSchema(t).DSN
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	t.Run("successful connection", func(t *testing.T) {
		serverConfig := &ServerConfig{
			TernDeployments: TernConfig{
				"default": {
					"staging":    "tern-staging:9090",
					"production": "tern-production:9090",
				},
			},
		}

		db, err := sql.Open("block-mysql", dsn)
		require.NoError(t, err, "failed to open database")
		require.NoError(t, db.PingContext(ctx), "failed to ping database")

		storage := mysqlstore.New(db)
		svc := New(storage, serverConfig, nil, logger)
		defer utils.CloseAndLog(svc)

		// Verify storage is working
		assert.NoError(t, svc.storage.Ping(ctx), "Ping() error")
	})

	t.Run("invalid DSN ping fails", func(t *testing.T) {
		// Test that connecting to an invalid MySQL server fails appropriately.
		// This tests the database connection logic that main.go now handles.
		db, err := sql.Open("block-mysql", "invalid:invalid@tcp(localhost:12345)/invalid")
		if err != nil {
			// sql.Open may fail for malformed DSN - that's fine
			return
		}

		// sql.Open doesn't actually connect - Ping does
		assert.Error(t, db.PingContext(ctx), "expected PingContext() to fail for invalid DSN")
	})
}
