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

// StorageCmd groups operator commands that act on SchemaBot's own storage
// database rather than on a user's database.
//
// The maintenance commands connect to storage themselves and work while the
// server is down, for windows such as a cross-dialect data move or a restore
// from a dump. The schema commands do both: they read a storage database
// through the API by default, so an operator can reach a data plane's storage
// that no workstation can dial, and connect directly when the server is down —
// including when it is down because its own schema bootstrap is failing.
type StorageCmd struct {
	Plan                     StoragePlanCmd              `cmd:"" help:"Show the storage DDL outstanding between a live storage database and the release you name; read-only, safe at any time. Exits 0 when converged and 2 when statements are outstanding."`
	Apply                    StorageApplyCmd             `cmd:"" help:"Converge a SchemaBot instance's storage database by running the schema bootstrap it would run on its next boot, under the same advisory lock and the same destructive-statement refusal."`
	ResyncIdentitySequences  ResyncIdentitySequencesCmd  `cmd:"" name:"resync-identity-sequences" help:"Advance PostgreSQL identity sequences on storage tables past their columns' stored maxima after an explicit-id bulk load; run after the load has fully committed and before default inserts resume — advance-only and safe to rerun."`
	CanonicalizeIdentityKeys CanonicalizeIdentityKeysCmd `cmd:"" name:"canonicalize-identity-keys" help:"Fold stored identity strings (repository, database, environment, deployment, lock owner) on PostgreSQL storage tables to canonical lowercase; run once, in a quiesced maintenance window, after every writer runs a release that folds identity strings at the write boundaries. The rewrite is one-way — original spellings are not recorded — so the command prompts unless --auto-approve is set; it only rewrites non-canonical rows, safe to rerun."`
}

// UsesAPI reports whether the named storage subcommand will reach its target
// through the API, and so needs an endpoint resolved before it runs.
//
// Only the schema commands have both paths, and the flags say which is in use.
// The maintenance commands open the storage database themselves and never call
// the API, so an unknown subcommand answering false is the safe default: the
// cost is a command that resolves no endpoint it was not going to use.
func (c *StorageCmd) UsesAPI(subcommand string) bool {
	switch subcommand {
	case "plan":
		return !c.Plan.direct()
	case "apply":
		return !c.Apply.direct()
	default:
		return false
	}
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
// where it came from, for the commands that only apply to PostgreSQL storage —
// purpose names the operation in the refusal. The source never contains the
// DSN itself, which may embed credentials.
func resolveStorageDSN(dsnFlag, configFlag, purpose string) (string, string, error) {
	// A direct DSN is checked against the PostgreSQL grammar rather than handed
	// to the generic family inference: these commands take no --dialect, so
	// there is nothing for an ambiguous DSN to be resolved by, and the refusal
	// that helps is the one naming the operation that does not apply to it.
	if directDSN := strings.TrimSpace(dsnFlag); directDSN != "" && configFlag == "" {
		if _, err := postgresconn.ConnectionDSN(directDSN); err != nil {
			return "", "", fmt.Errorf("storage DSN from --dsn is not a PostgreSQL DSN; %s only applies to %q storage: %w", purpose, schema.DialectPostgres, err)
		}
		return directDSN, "--dsn flag", nil
	}
	target, err := resolveStorageTarget(dsnFlag, configFlag, "")
	if err != nil {
		return "", "", err
	}
	if target.dialect != schema.DialectPostgres {
		return "", "", fmt.Errorf("storage dialect in %s is %q; %s only applies to %q storage", target.source, target.dialect, purpose, schema.DialectPostgres)
	}
	return target.dsn, target.source, nil
}
