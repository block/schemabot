package commands

import (
	"fmt"
	"os"
	"regexp"
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
// Every command that can address either family resolves it here, and none of
// them infers a dialect or reads an environment variable of its own: that would
// be a second answer to a question that has to have one, and the failure mode
// of getting it wrong is connecting one family's driver to the other's port,
// which fails with a protocol error that says nothing about the real problem to
// an operator who is usually mid-incident.
//
// The two PostgreSQL-only repair commands resolve their DSN through
// resolveStorageDSN instead, which refuses anything but PostgreSQL rather than
// deciding between families. It answers a narrower question, so it is not a
// competing answer to this one.

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
	// policy, leaving --allow-unsafe as the only way to widen it.
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
// An explicit --dialect settles it. Without one, a DSN written in either
// family's own form is settled by that form — a postgres:// URL by its scheme,
// a user@net(addr)/db string by the Go MySQL driver's grammar — and a DSN that
// does not parse within the family it is written for is reported as the broken
// DSN of that family, with its own parser's reason. Only a string in neither
// form is offered to both parsers, which is the same judgement that will be
// made when the command connects.
//
// The order matters more than it looks. The two parsers do not partition
// between them: libpq's keyword grammar accepts a MySQL DSN whole, so asking
// "which parser accepts this" moves a MySQL DSN with one bad parameter into
// the other family rather than reporting the parameter (see hasMySQLDSNForm).
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
	if hasMySQLDSNForm(dsn) {
		if _, err := mysqlconn.ConnectionDSN(dsn); err != nil {
			return "", fmt.Errorf("--dsn is written as a Go MySQL driver DSN but does not parse as one: %w", err)
		}
		return schema.DialectMySQL, nil
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

// mysqlDSNForm matches the Go MySQL driver's grammar as far as the database
// name: an optional user[:password], an "@", an optional network and address,
// and the "/" that introduces the database. Everything before that "/" carries
// no "=" and no space, which a libpq keyword/value string always has.
var mysqlDSNForm = regexp.MustCompile(`^[^=\s/]*@[a-zA-Z0-9]*(\([^()]*\))?/`)

// hasMySQLDSNForm reports whether a DSN is written in the Go MySQL driver's own
// form, the way hasPostgresURLScheme reports the other family's.
//
// Settling the family on the form rather than on which parser accepts the
// string is what keeps a bad parameter from moving a DSN between families. The
// two parsers do not partition: libpq's keyword grammar accepts a MySQL DSN
// whole, reading it as a set of words it does not recognize, so a MySQL DSN the
// MySQL parser rejects for `?timeout=30` or `?parseTime=yes` is still accepted
// by the PostgreSQL one — as a connection to a local socket under the operator's
// own account, naming no database they chose. Nothing about the command then
// mentions MySQL, and with --allow-unsafe the wrong family's bootstrap
// converges against whatever that resolves to.
//
// Recognizing the form first turns that into what it is: a MySQL DSN with a
// broken parameter, reported with the parser's own reason.
func hasMySQLDSNForm(dsn string) bool {
	return mysqlDSNForm.MatchString(strings.TrimSpace(dsn))
}
