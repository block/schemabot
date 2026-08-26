package commands

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/postgresconn"
	"github.com/block/schemabot/pkg/schema"
)

// StorageCmd groups operator commands that act directly on SchemaBot's own
// storage database. Unlike the API-client commands, these connect to storage
// themselves and work while the server is down — they exist for maintenance
// windows such as a cross-dialect data move or a restore from a dump.
type StorageCmd struct {
	ResyncIdentitySequences ResyncIdentitySequencesCmd `cmd:"" name:"resync-identity-sequences" help:"Advance PostgreSQL identity sequences on storage tables past their columns' stored maxima after an explicit-id bulk load"`
}

// ResyncIdentitySequencesCmd resyncs the identity sequences of SchemaBot's
// PostgreSQL storage tables after an explicit-id bulk load — a data move
// that preserves ids, or a restore from a dump without sequence state —
// so default inserts resume above the loaded ids instead of colliding with
// them. Run it after the load has fully committed and before the server
// resumes default inserts. The resync is advance-only and idempotent, so
// rerunning it is safe.
//
// The storage DSN comes from --dsn directly, or from the server config
// (--config, falling back to $SCHEMABOT_CONFIG_FILE) whose storage dialect
// must be postgres.
type ResyncIdentitySequencesCmd struct {
	DSN    string `help:"PostgreSQL DSN of the storage database to resync; bypasses the server config"`
	Config string `help:"Server config file to resolve the storage DSN from; defaults to $SCHEMABOT_CONFIG_FILE when neither flag is set"`
}

// storagePingTimeout bounds the connection check so an unreachable storage
// database fails the command promptly instead of hanging it.
const storagePingTimeout = 10 * time.Second

func (cmd *ResyncIdentitySequencesCmd) Run(ctx context.Context) error {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel(),
	}))

	dsn, source, err := cmd.resolveStorageDSN()
	if err != nil {
		return err
	}
	logger.Info("resolved storage DSN", "source", source)

	db, err := postgresconn.Open(dsn)
	if err != nil {
		return fmt.Errorf("open storage database: %w", err)
	}
	defer utils.CloseAndLog(db)
	pingCtx, cancel := context.WithTimeout(ctx, storagePingTimeout)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		return fmt.Errorf("ping storage database: %w", err)
	}

	if err := api.ResyncPostgresIdentitySequences(ctx, db, logger); err != nil {
		return fmt.Errorf("resync identity sequences on storage tables: %w", err)
	}
	logger.Info("identity sequence resync complete")
	return nil
}

// resolveStorageDSN returns the storage DSN and a loggable description of
// where it came from. A direct --dsn is used as-is; otherwise the server
// config (--config, then $SCHEMABOT_CONFIG_FILE) is loaded and its resolved
// storage DSN is used, failing closed when the configured storage dialect is
// not postgres. The source never contains the DSN itself, which may embed
// credentials.
func (cmd *ResyncIdentitySequencesCmd) resolveStorageDSN() (string, string, error) {
	if cmd.DSN != "" && cmd.Config != "" {
		return "", "", fmt.Errorf("--dsn and --config are mutually exclusive; pass the storage DSN directly or resolve it from a server config, not both")
	}
	if cmd.DSN != "" {
		return cmd.DSN, "--dsn flag", nil
	}

	configPath := cmd.Config
	source := fmt.Sprintf("server config %s", configPath)
	if configPath == "" {
		configPath = os.Getenv("SCHEMABOT_CONFIG_FILE")
		if configPath == "" {
			return "", "", fmt.Errorf("no storage DSN source: set --dsn, --config, or the SCHEMABOT_CONFIG_FILE environment variable")
		}
		source = fmt.Sprintf("server config %s ($SCHEMABOT_CONFIG_FILE)", configPath)
	}

	cfg, err := api.LoadServerConfigFromFile(configPath)
	if err != nil {
		return "", "", fmt.Errorf("load %s: %w", source, err)
	}

	dialect, err := cfg.Storage.ResolveDialect()
	if err != nil {
		return "", "", fmt.Errorf("resolve storage dialect from %s: %w", source, err)
	}
	if dialect != schema.DialectPostgres {
		return "", "", fmt.Errorf("storage dialect in %s is %q; the identity sequence resync only applies to %q storage", source, dialect, schema.DialectPostgres)
	}

	dsn, err := cfg.StorageDSN()
	if err != nil {
		return "", "", fmt.Errorf("resolve storage DSN from %s: %w", source, err)
	}
	if dsn == "" {
		return "", "", fmt.Errorf("storage DSN not configured in %s (set storage.dsn in the config or the STORAGE_DSN env var)", source)
	}
	return dsn, source, nil
}
