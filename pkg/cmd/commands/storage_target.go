package commands

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/mysqlconn"
	"github.com/block/schemabot/pkg/postgresconn"
	"github.com/block/schemabot/pkg/schema"
)

// Resolving a storage target is the step every direct-connection storage
// command takes before it does anything: which database to open, which family
// it belongs to, and under which storage policy.
//
// It is deliberately the only place that decides. A command that inferred its
// own dialect, or read its own environment variable, would be a second answer
// to a question that has to have one — and the failure mode of getting it wrong
// is connecting one family's driver to the other's port, which fails with a
// protocol error that says nothing about the real problem to an operator who is
// usually mid-incident.

// storageTarget is a resolved direct connection to a storage database: where to
// connect, which family the database belongs to, the storage policy that
// applies to it, and a loggable description of where the DSN came from. The
// description never contains the DSN itself, which may embed credentials.
type storageTarget struct {
	dsn     string
	dialect schema.Dialect
	source  string
	// allowDestructive is the deployment's standing storage policy
	// (storage.allow_destructive_schema_changes), when the DSN came from a
	// server config. A boot of that config converges under it, so an operator
	// convergence against the same database must too — otherwise "apply is what
	// a boot does" (AV-9) stops being true on exactly the deployments that
	// opted in. A DSN passed on the command line carries no config and so no
	// policy, leaving --allow-destructive as the only way to widen it.
	allowDestructive bool
	// postgresStatementTimeout is the config's statement budget, for the same
	// reason: a convergence run here has to read and write under the budget the
	// deployment's own bootstrap uses. Nil where no config was loaded, which
	// leaves the package default in place.
	postgresStatementTimeout *time.Duration
}

// allowsDestructive is whether destructive storage statements run against this
// target. The operator's per-command flag only ever widens the target's
// standing policy: a request may permit destructive statements on a deployment
// that does not, and must never refuse ones the deployment's own boot would
// run.
func (t *storageTarget) allowsDestructive(requestAllowDestructive bool) bool {
	return t.allowDestructive || requestAllowDestructive
}

// ensureSchemaOptions is the policy this target converges or diffs under.
func (t *storageTarget) ensureSchemaOptions(requestAllowDestructive bool) []api.EnsureSchemaOption {
	opts := []api.EnsureSchemaOption{
		api.WithDialect(t.dialect),
		api.WithAllowDestructiveSchemaChanges(t.allowsDestructive(requestAllowDestructive)),
	}
	if t.postgresStatementTimeout != nil {
		opts = append(opts, api.WithPostgresStatementTimeout(*t.postgresStatementTimeout))
	}
	return opts
}

// resolveStorageTarget resolves a storage database to connect to directly.
//
// A direct --dsn is taken as given, with its dialect from --dialect or inferred
// from the DSN's own form. Otherwise the server config (--config, then
// $SCHEMABOT_CONFIG_FILE) is loaded and both the DSN and the dialect come from
// it, which is the path that needs no inference at all: a server config states
// its storage dialect.
//
// dialectFlag is the operator's assertion and wins over inference, but it is
// only consulted on the direct path. A config that says postgres and a flag
// that says mysql is a contradiction, not a preference, so the config's own
// dialect stands and the mismatch is refused rather than resolved.
func resolveStorageTarget(dsnFlag, configFlag, dialectFlag string) (*storageTarget, error) {
	directDSN := strings.TrimSpace(dsnFlag)
	if directDSN != "" && configFlag != "" {
		return nil, fmt.Errorf("--dsn and --config are mutually exclusive; pass the storage DSN directly or resolve it from a server config, not both")
	}
	if dsnFlag != "" {
		if directDSN == "" {
			return nil, fmt.Errorf("storage DSN not configured: --dsn contains only whitespace")
		}
		dialect, err := directStorageDialect(directDSN, dialectFlag)
		if err != nil {
			return nil, err
		}
		return &storageTarget{dsn: directDSN, dialect: dialect, source: "--dsn flag"}, nil
	}

	configPath := configFlag
	source := fmt.Sprintf("server config %s", configPath)
	if configPath == "" {
		configPath = os.Getenv("SCHEMABOT_CONFIG_FILE")
		if configPath == "" {
			return nil, fmt.Errorf("no storage DSN source: set --dsn, --config, or the SCHEMABOT_CONFIG_FILE environment variable")
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
		return nil, fmt.Errorf("load %s: %w", source, err)
	}

	dialect, err := cfg.Storage.ResolveDialect()
	if err != nil {
		return nil, fmt.Errorf("resolve storage dialect from %s: %w", source, err)
	}
	if asserted := strings.TrimSpace(strings.ToLower(dialectFlag)); asserted != "" && schema.Dialect(asserted) != dialect {
		return nil, fmt.Errorf("--dialect says %q but %s configures %q storage; drop --dialect, which only applies to a DSN passed with --dsn", asserted, source, dialect)
	}

	dsn, err := cfg.StorageDSN()
	if err != nil {
		return nil, fmt.Errorf("resolve storage DSN from %s: %w", source, err)
	}
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return nil, fmt.Errorf("storage DSN not configured (set --dsn, config storage.dsn or storage.dsn_from, STORAGE_DSN, or MYSQL_DSN)")
	}
	if cfg.Storage.DSN == "" && cfg.Storage.DSNFrom == nil {
		if strings.TrimSpace(os.Getenv("STORAGE_DSN")) != "" {
			source = "STORAGE_DSN environment variable"
		} else if strings.TrimSpace(os.Getenv("MYSQL_DSN")) != "" {
			source = "MYSQL_DSN environment variable"
		}
	}
	statementTimeout := cfg.Postgres.StatementTimeoutOrDefault()
	return &storageTarget{
		dsn:                      dsn,
		dialect:                  dialect,
		source:                   source,
		allowDestructive:         cfg.Storage.AllowDestructiveSchemaChanges,
		postgresStatementTimeout: &statementTimeout,
	}, nil
}

// directStorageDialect decides which database family a DSN passed straight on
// the command line addresses.
//
// An explicit --dialect settles it. Without one, the two families' own
// connection-string parsers do: each one either accepts the string or does not,
// which is the same judgement that will be made when the command connects.
// Reading the string for family-specific substrings instead would misclassify a
// MySQL password containing "port=" and reject libpq forms spelled with
// keywords nobody thought to list.
//
// A postgres:// URL is settled by its scheme before either parser runs, so a
// malformed one is reported as the broken PostgreSQL DSN it is rather than as a
// string of no recognizable family.
//
// A DSN neither parser accepts is refused naming --dialect: connecting a MySQL
// driver to a PostgreSQL port fails with a protocol error that says nothing
// about the real problem, and the operator reaching for this is usually
// mid-incident.
func directStorageDialect(dsn, dialectFlag string) (schema.Dialect, error) {
	switch asserted := schema.Dialect(strings.TrimSpace(strings.ToLower(dialectFlag))); asserted {
	case schema.DialectMySQL, schema.DialectPostgres:
		return asserted, nil
	case "":
	default:
		return "", fmt.Errorf("unsupported storage dialect %q; SchemaBot stores its own state on %q or %q", asserted, schema.DialectMySQL, schema.DialectPostgres)
	}

	if hasPostgresURLScheme(dsn) {
		if _, err := postgresconn.ConnectionDSN(dsn); err != nil {
			return "", fmt.Errorf("--dsn is a PostgreSQL connection URL but does not parse as one: %w", err)
		}
		return schema.DialectPostgres, nil
	}
	// MySQL first, and the order carries the decision. The Go MySQL driver's
	// grammar is the narrow one — it wants user:pass@proto(addr)/dbname — while
	// libpq's keyword form tolerates words it does not recognize, so a valid
	// MySQL DSN can satisfy both parsers and a libpq string satisfies only one.
	// Asking the specific parser first is what keeps a password containing an
	// "=" from being read as a PostgreSQL keyword.
	if _, err := mysqlconn.ConnectionDSN(dsn); err == nil {
		return schema.DialectMySQL, nil
	}
	if _, err := postgresconn.ConnectionDSN(dsn); err == nil {
		return schema.DialectPostgres, nil
	}
	return "", fmt.Errorf("cannot tell which database family --dsn addresses: it parses neither as a PostgreSQL connection string (postgres://user@host:5432/db, or host=... dbname=...) nor as a Go MySQL driver DSN (user:pass@tcp(host:3306)/db); pass --dialect %s or --dialect %s to say which",
		schema.DialectMySQL, schema.DialectPostgres)
}

// hasPostgresURLScheme reports whether a DSN is written as a PostgreSQL
// connection URL. The scheme is unambiguous — no other family SchemaBot stores
// its state on spells one — so it classifies without connecting and without
// parsing.
func hasPostgresURLScheme(dsn string) bool {
	lowered := strings.ToLower(strings.TrimSpace(dsn))
	return strings.HasPrefix(lowered, "postgres://") || strings.HasPrefix(lowered, "postgresql://")
}
