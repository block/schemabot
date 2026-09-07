package commands

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
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
	ResyncIdentitySequences  ResyncIdentitySequencesCmd  `cmd:"" name:"resync-identity-sequences" help:"Advance PostgreSQL identity sequences on storage tables past their columns' stored maxima after an explicit-id bulk load; run after the load has fully committed and before default inserts resume — advance-only and safe to rerun."`
	CanonicalizeIdentityKeys CanonicalizeIdentityKeysCmd `cmd:"" name:"canonicalize-identity-keys" help:"Fold stored identity strings (repository, database, environment, deployment, lock owner) on PostgreSQL storage tables to canonical lowercase; run once, in a quiesced maintenance window, after every writer runs a release that folds identity strings at the write boundaries. The rewrite is one-way — original spellings are not recorded — so the command prompts unless --auto-approve is set; it only rewrites non-canonical rows, safe to rerun."`
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

func (cmd *ResyncIdentitySequencesCmd) Run(ctx context.Context, g *Globals) error {
	// A text handler, deliberately: this is a one-shot operator command read
	// at a terminal during a maintenance window, not a long-running server
	// whose stdout feeds a JSON log collector. Diagnostics go to stderr so
	// stdout stays free for machine-readable output.
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: logLevel(),
	})).With("schemabot_version", g.Version)

	dsn, source, err := resolveStorageDSN(cmd.DSN, cmd.Config, "the identity sequence resync")
	if err != nil {
		return err
	}
	logger.Info("resolved storage DSN", "source", source)

	// An operator-supervised one-shot over every storage table: it runs as long
	// as it needs to, bounded by ctx rather than by a statement budget. Stating
	// that explicitly keeps a platform statement_timeout — which hosted
	// providers set at the role or database level, tuned for API queries — from
	// cancelling the maintenance part-way through.
	db, err := postgresconn.Open(dsn, postgresconn.WithStatementTimeout(0))
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

// CanonicalizeIdentityKeysCmd folds the identity strings stored on
// SchemaBot's PostgreSQL storage tables — repository full names, database
// names, database types, environments, deployments, and lock owners — to
// their canonical lowercase spelling. Rows written by releases that did not
// fold identity strings at the write boundaries are invisible to the folded
// lookups on PostgreSQL's byte-comparing collation; run the fold once when
// upgrading to a release that folds — after every server and worker runs
// that release, and inside a quiesced maintenance window (the command works
// while the server is down). It only rewrites rows whose spelling is not
// already canonical, so rerunning it is safe; but the rewrite itself is
// one-way — original spellings are not recorded anywhere — so the command
// prompts for confirmation unless --auto-approve is set.
//
// The storage DSN comes from --dsn directly, or from the server config
// (--config, falling back to $SCHEMABOT_CONFIG_FILE) whose storage dialect
// must be postgres.
type CanonicalizeIdentityKeysCmd struct {
	DSN         string `help:"PostgreSQL DSN of the storage database to canonicalize; bypasses the server config"`
	Config      string `help:"Server config file to resolve the storage DSN from; defaults to $SCHEMABOT_CONFIG_FILE when neither flag is set"`
	AutoApprove bool   `short:"y" help:"Skip confirmation prompt" name:"auto-approve"`
}

func (cmd *CanonicalizeIdentityKeysCmd) Run(ctx context.Context, g *Globals) error {
	// A text handler, deliberately: this is a one-shot operator command read
	// at a terminal during a maintenance window, not a long-running server
	// whose stdout feeds a JSON log collector. Diagnostics go to stderr so
	// stdout stays free for machine-readable output.
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: logLevel(),
	})).With("schemabot_version", g.Version)

	dsn, source, err := resolveStorageDSN(cmd.DSN, cmd.Config, "the identity key canonicalization")
	if err != nil {
		return err
	}
	logger.Info("resolved storage DSN", "source", source)

	// The fold rewrites rows in place and the original spellings are not
	// recorded anywhere; require explicit confirmation unless auto-approved.
	if !cmd.AutoApprove {
		fmt.Printf("About to permanently fold stored identity strings to lowercase on the storage database from %s.\n", source)
		confirmed, err := confirmAction(
			"Original spellings are not recorded and cannot be restored. Only 'yes' will be accepted: ",
			"\nCanonicalization aborted.",
		)
		if err != nil {
			return err
		}
		if !confirmed {
			return nil
		}
	}

	// An operator-supervised one-shot over every storage table: it runs as long
	// as it needs to, bounded by ctx rather than by a statement budget. Stating
	// that explicitly keeps a platform statement_timeout — which hosted
	// providers set at the role or database level, tuned for API queries — from
	// cancelling the maintenance part-way through.
	db, err := postgresconn.Open(dsn, postgresconn.WithStatementTimeout(0))
	if err != nil {
		return fmt.Errorf("open storage database: %w", err)
	}
	defer utils.CloseAndLog(db)
	pingCtx, cancel := context.WithTimeout(ctx, storagePingTimeout)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		return fmt.Errorf("ping storage database: %w", err)
	}

	if err := api.CanonicalizePostgresIdentityKeys(ctx, db, logger); err != nil {
		return fmt.Errorf("canonicalize identity keys on storage tables: %w", err)
	}
	logger.Info("identity key canonicalization complete")
	return nil
}

// resolveStorageDSN returns the storage DSN and a loggable description of
// where it came from. A direct --dsn is used after verifying it parses as a
// PostgreSQL DSN; otherwise the server config (--config, then
// $SCHEMABOT_CONFIG_FILE) is loaded and its resolved storage DSN is used,
// failing closed when the configured storage dialect is not postgres —
// purpose names the operation in these errors. The source never contains
// the DSN itself, which may embed credentials.
func resolveStorageDSN(dsnFlag, configFlag, purpose string) (string, string, error) {
	directDSN := strings.TrimSpace(dsnFlag)
	if directDSN != "" && configFlag != "" {
		return "", "", fmt.Errorf("--dsn and --config are mutually exclusive; pass the storage DSN directly or resolve it from a server config, not both")
	}
	if dsnFlag != "" {
		if directDSN == "" {
			return "", "", fmt.Errorf("storage DSN not configured: --dsn contains only whitespace")
		}
		// The config path refuses non-postgres storage via the configured
		// dialect; the direct path has no dialect field, so refuse any DSN
		// that does not parse as PostgreSQL instead of failing later with an
		// opaque connection error — or worse, against the wrong server.
		if _, err := postgresconn.ConnectionDSN(directDSN); err != nil {
			return "", "", fmt.Errorf("storage DSN from --dsn is not a PostgreSQL DSN; %s only applies to %q storage: %w", purpose, schema.DialectPostgres, err)
		}
		return directDSN, "--dsn flag", nil
	}

	configPath := configFlag
	source := fmt.Sprintf("server config %s", configPath)
	if configPath == "" {
		configPath = os.Getenv("SCHEMABOT_CONFIG_FILE")
		if configPath == "" {
			return "", "", fmt.Errorf("no storage DSN source: set --dsn, --config, or the SCHEMABOT_CONFIG_FILE environment variable")
		}
		source = fmt.Sprintf("server config %s ($SCHEMABOT_CONFIG_FILE)", configPath)
	}

	var cfg *api.ServerConfig
	var err error
	if configFlag == "" {
		cfg, err = api.LoadServerConfig()
	} else {
		cfg, err = api.LoadServerConfigFromFile(configPath)
	}
	if err != nil {
		return "", "", fmt.Errorf("load %s: %w", source, err)
	}

	dialect, err := cfg.Storage.ResolveDialect()
	if err != nil {
		return "", "", fmt.Errorf("resolve storage dialect from %s: %w", source, err)
	}
	if dialect != schema.DialectPostgres {
		return "", "", fmt.Errorf("storage dialect in %s is %q; %s only applies to %q storage", source, dialect, purpose, schema.DialectPostgres)
	}

	dsn, err := cfg.StorageDSN()
	if err != nil {
		return "", "", fmt.Errorf("resolve storage DSN from %s: %w", source, err)
	}
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return "", "", fmt.Errorf("storage DSN not configured (set --dsn, config storage.dsn or storage.dsn_from, STORAGE_DSN, or MYSQL_DSN)")
	}
	if cfg.Storage.DSN == "" && cfg.Storage.DSNFrom == nil {
		if strings.TrimSpace(os.Getenv("STORAGE_DSN")) != "" {
			source = "STORAGE_DSN environment variable"
		} else if strings.TrimSpace(os.Getenv("MYSQL_DSN")) != "" {
			source = "MYSQL_DSN environment variable"
		}
	}
	return dsn, source, nil
}
