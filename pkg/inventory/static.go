package inventory

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"strconv"
	"strings"

	"github.com/block/mysql"

	"github.com/block/schemabot/pkg/secrets"
)

// StaticConfig configures a resolver backed by static target entries.
type StaticConfig struct {
	Targets map[string]StaticTarget `yaml:"targets"`
}

// StaticTarget is a static connection entry for one target. Exactly one of DSN
// or DSNFrom must be set.
type StaticTarget struct {
	DatabaseType string `yaml:"type"`
	// DSN is a full connection string or secret reference. It is resolved once,
	// when the resolver is constructed, and cached. A PostgreSQL DSN is passed
	// through as written: the dsn_from transport gate (the sslmode allowlist
	// and CA resolution) does not apply, so an explicit weaker sslmode —
	// including disable — is honored. It is the deliberate escape hatch for
	// dev and compatibility setups; production PostgreSQL targets should use
	// DSNFrom. Mutually exclusive with DSNFrom.
	DSN string `yaml:"dsn,omitempty"`
	// DSNFrom assembles a DSN from separate config and password secret
	// references: a namespace-free MySQL DSN, or a PostgreSQL libpq URL carrying
	// the database name. Unlike DSN it is resolved fresh on every request so a
	// rotated target credential (for example one re-synced by the External
	// Secrets Operator) is picked up without restarting the worker. Mutually
	// exclusive with DSN.
	//
	// Intended backend: file: secret references (the ESO-synced case). The
	// per-request assembly runs before the target router's client-cache lookup,
	// and on a cache hit the freshly assembled DSN is discarded (the cached client
	// serves the request), so today dsn_from buys rotation-safety only at the next
	// cache miss. With secretsmanager: refs that means every routed request pays a
	// GetSecretValue call (two, for the split config/password refs) and gains a
	// per-request dependency on secret-backend availability with no offsetting
	// benefit until the DSN-aware client cache lands; with file: refs the cost and
	// availability risk are negligible. Prefer file: refs until that follow-up.
	DSNFrom  *StaticDSNFromConfig `yaml:"dsn_from,omitempty"`
	Metadata map[string]string    `yaml:"metadata,omitempty"`
	// SchemaOverrides maps a requested (canonical) MySQL namespace to the
	// physical schema name on this target, for targets whose physical schema
	// names embed environment or region (e.g. bikeshare → bikeshare_eu_qa).
	// The target DSN stays namespace-free; the mapped physical name is injected
	// per operation instead of the requested namespace. When non-empty it is a
	// strict allowlist: a requested namespace without a mapping fails rather
	// than falling back to the canonical name, so a misrouted request cannot
	// land in the wrong physical schema. MySQL only; currently limited to a
	// single mapping per target.
	SchemaOverrides map[string]string `yaml:"schema_overrides,omitempty"`
}

// StaticDSNFromConfig assembles a DSN for a static target from secret
// references, resolved fresh on every request. For MySQL it deliberately never
// reads a database name: the per-operation request supplies the schema, so the
// assembled DSN carries none (a DSN with a database name is rejected). For
// PostgreSQL the connection is made to one database, so the config document
// must carry the database name and the assembled DSN includes it; the request
// namespace selects a schema within that database.
type StaticDSNFromConfig struct {
	// ConfigRef is a secret reference to a JSON document holding the target's
	// connection metadata (host and port, plus the database name for
	// PostgreSQL). Like PasswordRef it may contain a "{target}" placeholder,
	// replaced with the request target for per-target secret naming, and is
	// resolved fresh on every request.
	ConfigRef string `yaml:"config_ref"`
	// ConfigPaths selects which keys in ConfigRef hold the connection fields,
	// defaulting to "host", "port", and (PostgreSQL only) "dbname".
	ConfigPaths StaticDSNFromPaths `yaml:"config_paths,omitempty"`
	// Username is the database user included in the assembled DSN.
	Username string `yaml:"username"`
	// PasswordRef is a secret reference to the database user's password. It may
	// contain a "{target}" placeholder, replaced with the request target for
	// per-target secret naming, and is resolved fresh on every request.
	PasswordRef string `yaml:"password_ref"`
	// Params are appended as DSN query parameters. For MySQL they are freeform
	// (for example TLS settings); for PostgreSQL only "sslmode: verify-full" is
	// accepted — TLS trust material is configured via CARef, never via raw
	// libpq parameters.
	Params map[string]string `yaml:"params,omitempty"`
	// CARef (PostgreSQL only) selects the CA bundle the data plane verifies the
	// server against: "embedded:rds-global" or "file:<absolute-path>". Empty
	// defaults to the embedded RDS bundle when the resolved host is an RDS
	// endpoint and fails otherwise — a verified CA is required.
	CARef string `yaml:"ca_ref,omitempty"`
}

// StaticDSNFromPaths selects the connection-field keys in a
// StaticDSNFromConfig's config document. Keys are looked up at the top level of
// the document.
type StaticDSNFromPaths struct {
	Host string `yaml:"host,omitempty"`
	Port string `yaml:"port,omitempty"`
	// DBName is the key holding the PostgreSQL database name, defaulting to
	// "dbname". PostgreSQL only: MySQL targets stay namespace-free.
	DBName string `yaml:"dbname,omitempty"`
}

// StaticResolver resolves targets from static configuration.
type StaticResolver struct {
	targets map[string]staticTargetEntry
}

// staticTargetEntry is one prepared static target. A dsn entry resolves once at
// construction and caches its DSN; a dsn_from entry assembles its DSN fresh on
// every request.
type staticTargetEntry struct {
	target       string
	databaseType string
	metadata     map[string]string
	// schemaOverrides is the validated canonical→physical MySQL schema
	// allowlist for this target; empty means the requested namespace is the
	// physical schema.
	schemaOverrides map[string]string
	// dsn is set (non-nil) for entries resolved once at construction.
	dsn *string
	// dsnFrom is set for entries assembled fresh on every request.
	dsnFrom *StaticDSNFromConfig
}

var _ Resolver = (*StaticResolver)(nil)

// NewStaticResolver creates a static target resolver.
func NewStaticResolver(config StaticConfig) (*StaticResolver, error) {
	if len(config.Targets) == 0 {
		return nil, fmt.Errorf("static target resolver requires at least one target")
	}
	targets := make(map[string]staticTargetEntry, len(config.Targets))
	for target, entry := range config.Targets {
		if target == "" {
			return nil, fmt.Errorf("static target resolver contains an empty target key")
		}
		prepared, err := newStaticTargetEntry(target, entry)
		if err != nil {
			return nil, err
		}
		targets[target] = *prepared
	}
	return &StaticResolver{targets: targets}, nil
}

// ResolveTarget resolves one target from static configuration.
func (r *StaticResolver) ResolveTarget(ctx context.Context, req Request) (*Target, error) {
	if r == nil {
		return nil, fmt.Errorf("static target resolver is nil")
	}
	if req.Target == "" {
		return nil, fmt.Errorf("target is required")
	}
	entry, ok := r.targets[req.Target]
	if !ok {
		return nil, fmt.Errorf("target %q is not configured", req.Target)
	}
	if strings.TrimSpace(req.DatabaseType) != "" {
		requestedType := canonicalDatabaseType(req.DatabaseType)
		if requestedType != entry.databaseType {
			return nil, fmt.Errorf("target %q is configured for database type %q, not %q", req.Target, entry.databaseType, requestedType)
		}
	}
	dsn, assembled, err := entry.resolveConnection(ctx, req)
	if err != nil {
		return nil, err
	}
	// Configured metadata supplies extra engine fields but must not override
	// the fields the assembler resolved (for example the PostgreSQL CA
	// reference), so it is written first and the assembled fields last.
	metadata := maps.Clone(entry.metadata)
	if len(assembled) > 0 {
		if metadata == nil {
			metadata = make(map[string]string, len(assembled))
		}
		maps.Copy(metadata, assembled)
	}
	return &Target{
		Target:          entry.target,
		DatabaseType:    entry.databaseType,
		DSN:             dsn,
		Metadata:        metadata,
		SchemaOverrides: maps.Clone(entry.schemaOverrides),
	}, nil
}

// newStaticTargetEntry prepares one static target: it validates the entry, and
// for a dsn entry resolves and caches the DSN now, while a dsn_from entry only
// validates its shape (its DSN is assembled per request).
func newStaticTargetEntry(target string, entry StaticTarget) (*staticTargetEntry, error) {
	databaseType := canonicalDatabaseType(entry.DatabaseType)
	if databaseType == "" {
		return nil, fmt.Errorf("target %q missing type", target)
	}
	hasDSN := entry.DSN != ""
	hasDSNFrom := entry.DSNFrom != nil
	switch {
	case hasDSN && hasDSNFrom:
		return nil, fmt.Errorf("target %q cannot configure both dsn and dsn_from", target)
	case !hasDSN && !hasDSNFrom:
		return nil, fmt.Errorf("target %q missing dsn or dsn_from", target)
	}
	if err := ValidateSchemaOverrides(databaseType, entry.SchemaOverrides); err != nil {
		return nil, fmt.Errorf("target %q: %w", target, err)
	}
	prepared := &staticTargetEntry{
		target:          target,
		databaseType:    databaseType,
		metadata:        maps.Clone(entry.Metadata),
		schemaOverrides: maps.Clone(entry.SchemaOverrides),
	}
	if hasDSNFrom {
		if databaseType != "mysql" && databaseType != "postgres" {
			return nil, fmt.Errorf("target %q dsn_from is only supported for mysql and postgres, not %q", target, databaseType)
		}
		if err := entry.DSNFrom.validate(target, databaseType); err != nil {
			return nil, err
		}
		prepared.dsnFrom = entry.DSNFrom
		return prepared, nil
	}
	dsn, err := secrets.Resolve(entry.DSN, "")
	if err != nil {
		return nil, fmt.Errorf("resolve DSN for target %q: %w", target, err)
	}
	if dsn == "" {
		return nil, fmt.Errorf("target %q resolved an empty DSN", target)
	}
	if err := validateResolvedStaticTargetDSN(target, databaseType, dsn); err != nil {
		return nil, err
	}
	prepared.dsn = &dsn
	return prepared, nil
}

// resolveConnection returns the entry's DSN and any assembled engine metadata:
// the cached DSN for a dsn entry, or a freshly assembled DSN (plus, for
// PostgreSQL, the resolved CA reference) for a dsn_from entry.
func (e staticTargetEntry) resolveConnection(ctx context.Context, req Request) (string, map[string]string, error) {
	if e.dsn != nil {
		return *e.dsn, nil, nil
	}
	dsn, assembled, err := e.dsnFrom.assemble(ctx, req, e.databaseType)
	if err != nil {
		return "", nil, err
	}
	if err := validateResolvedStaticTargetDSN(e.target, e.databaseType, dsn); err != nil {
		return "", nil, err
	}
	return dsn, assembled, nil
}

func canonicalDatabaseType(databaseType string) string {
	return strings.ToLower(strings.TrimSpace(databaseType))
}

// ValidateSchemaOverrides checks a canonical→physical schema mapping. The
// mapping is MySQL-only (it selects a MySQL schema; for Vitess the namespace
// is a keyspace, a different concept) and is currently limited to a single
// entry: SchemaBot's MySQL operations address one schema per target, and a
// wider map would silently permit multi-schema routing this feature does not
// support yet. It is exported so every consumer of a resolved target (not just
// static inventory) enforces the same contract; a custom resolver cannot hand
// an invalid mapping past it.
func ValidateSchemaOverrides(databaseType string, overrides map[string]string) error {
	if len(overrides) == 0 {
		return nil
	}
	if databaseType != "mysql" {
		return fmt.Errorf("schema_overrides is only supported for mysql, not %q", databaseType)
	}
	if len(overrides) > 1 {
		return fmt.Errorf("schema_overrides supports exactly one mapping, got %d", len(overrides))
	}
	for canonical, physical := range overrides {
		if err := validateSchemaIdentifier(canonical); err != nil {
			return fmt.Errorf("schema_overrides key %q: %w", canonical, err)
		}
		if err := validateSchemaIdentifier(physical); err != nil {
			return fmt.Errorf("schema_overrides value %q for key %q: %w", physical, canonical, err)
		}
	}
	return nil
}

// validateSchemaIdentifier requires an unquoted-identifier-safe MySQL schema
// name. Mapped names are injected into DSNs and SQL, so restrict them to the
// character set that never needs quoting rather than supporting MySQL's full
// quoted-identifier grammar.
func validateSchemaIdentifier(name string) error {
	if name == "" {
		return fmt.Errorf("schema name must not be empty")
	}
	if len(name) > 64 {
		return fmt.Errorf("schema name exceeds MySQL's 64-character identifier limit")
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '$':
		default:
			return fmt.Errorf("schema name contains unsupported character %q; only [a-zA-Z0-9_$] is allowed", r)
		}
	}
	return nil
}

func validateResolvedStaticTargetDSN(target, databaseType, dsn string) error {
	if databaseType != "mysql" {
		return nil
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return fmt.Errorf("parse MySQL DSN for target %q: %w", target, err)
	}
	if cfg.DBName != "" {
		return fmt.Errorf("target %q MySQL DSN must not include a database name; the request supplies the namespace", target)
	}
	return nil
}

// validate checks that a dsn_from entry has the fields required to assemble a
// DSN, including the engine-specific ones: the PostgreSQL-only fields are
// rejected on other engines rather than silently ignored, and the PostgreSQL
// TLS configuration is checked here so a misconfiguration surfaces at
// construction, not on the first routed request. The assembler enforces the
// same TLS rules again per request; that copy is the only gate for inventory
// sources that construct the assembler directly, so neither check is
// redundant. The password is not read here — it is resolved per request so a
// rotated credential is picked up without a restart.
func (c *StaticDSNFromConfig) validate(target, databaseType string) error {
	if c.ConfigRef == "" {
		return fmt.Errorf("target %q dsn_from missing config_ref", target)
	}
	if c.Username == "" {
		return fmt.Errorf("target %q dsn_from missing username", target)
	}
	if c.PasswordRef == "" {
		return fmt.Errorf("target %q dsn_from missing password_ref", target)
	}
	if databaseType != "postgres" {
		if c.CARef != "" {
			return fmt.Errorf("target %q dsn_from ca_ref is only supported for postgres, not %q", target, databaseType)
		}
		if c.ConfigPaths.DBName != "" {
			return fmt.Errorf("target %q dsn_from config_paths.dbname is only supported for postgres, not %q", target, databaseType)
		}
		return nil
	}
	if err := validatePostgresDSNParams(c.Params); err != nil {
		return fmt.Errorf("target %q dsn_from: %w", target, err)
	}
	if c.CARef != "" {
		if err := validatePostgresCARef(c.CARef); err != nil {
			return fmt.Errorf("target %q dsn_from: %w", target, err)
		}
	}
	return nil
}

// assemble builds the entry's DSN from the config document and the password
// reference, both resolved fresh, routing to the engine's assembler: a
// namespace-free MySQL DSN, or a PostgreSQL libpq URL carrying the database
// name plus the resolved CA reference as engine metadata. It reuses the shared
// assemblers and the fail-closed secret-ref credential resolver so a rotated
// password or empty secret is handled identically to the Etre data-plane path.
func (c *StaticDSNFromConfig) assemble(ctx context.Context, req Request, databaseType string) (string, map[string]string, error) {
	document, err := c.resolveConfigDocument(ctx, req.Target)
	if err != nil {
		return "", nil, err
	}
	if databaseType == "postgres" {
		return c.assemblePostgres(ctx, document, req)
	}
	// The database name in the document, if any, is ignored: static MySQL
	// target DSNs are namespace-free, the request supplies the schema.
	host, port, err := c.endpoint(document, req.Target, "3306")
	if err != nil {
		return "", nil, err
	}
	creds, err := c.resolveCredentials(ctx, req)
	if err != nil {
		return "", nil, err
	}
	assembler := MySQLConnectionAssembler{Type: databaseType, DefaultPort: port, Params: c.Params}
	dsn, _, err := assembler.Assemble(host, nil, creds)
	if err != nil {
		return "", nil, fmt.Errorf("assemble DSN for target %q: %w", req.Target, err)
	}
	return dsn, nil, nil
}

// assemblePostgres builds a PostgreSQL libpq URL from the config document's
// host, port, and database name. The database name is required: a PostgreSQL
// connection is made to one database, and assembling a DSN without one would
// fail closed only at dial time with a confusing server-side error.
func (c *StaticDSNFromConfig) assemblePostgres(ctx context.Context, document map[string]any, req Request) (string, map[string]string, error) {
	target := req.Target
	host, port, err := c.endpoint(document, target, "5432")
	if err != nil {
		return "", nil, err
	}
	dbnameKey := c.ConfigPaths.DBName
	if dbnameKey == "" {
		dbnameKey = PostgresDBNameAttribute
	}
	dbname, err := requiredStringField(document, dbnameKey)
	if err != nil {
		return "", nil, fmt.Errorf("read dbname for target %q: %w", target, err)
	}
	creds, err := c.resolveCredentials(ctx, req)
	if err != nil {
		return "", nil, err
	}
	assembler := PostgresConnectionAssembler{DefaultPort: port, Params: c.Params, CARef: c.CARef}
	dsn, metadata, err := assembler.Assemble(host, map[string]string{PostgresDBNameAttribute: dbname}, creds)
	if err != nil {
		return "", nil, fmt.Errorf("assemble DSN for target %q: %w", target, err)
	}
	return dsn, metadata, nil
}

// resolveCredentials resolves the entry's username and per-request password.
// It runs after the config document's connection fields are read so a
// malformed document fails before it costs a password secret read, and with
// the endpoint error rather than a later credential one.
func (c *StaticDSNFromConfig) resolveCredentials(ctx context.Context, req Request) (*Credentials, error) {
	return SecretRefCredentialResolver{Username: c.Username, PasswordRef: c.PasswordRef}.ResolveCredentials(ctx, req, nil)
}

// resolveConfigDocument resolves the config reference and parses it as a JSON
// document.
func (c *StaticDSNFromConfig) resolveConfigDocument(ctx context.Context, target string) (map[string]any, error) {
	ref := strings.ReplaceAll(c.ConfigRef, "{target}", target)
	document, err := secrets.ResolveContext(ctx, ref, "")
	if err != nil {
		return nil, fmt.Errorf("resolve config reference for target %q: %w", target, err)
	}
	if document == "" {
		return nil, fmt.Errorf("config reference for target %q resolved an empty value", target)
	}
	var config map[string]any
	if err := json.Unmarshal([]byte(document), &config); err != nil {
		return nil, fmt.Errorf("parse config for target %q: %w", target, err)
	}
	return config, nil
}

// endpoint reads the target host and port from the config document. The port
// defaults to the engine's default when absent.
func (c *StaticDSNFromConfig) endpoint(document map[string]any, target, defaultPort string) (host, port string, err error) {
	hostKey := c.ConfigPaths.Host
	if hostKey == "" {
		hostKey = "host"
	}
	portKey := c.ConfigPaths.Port
	if portKey == "" {
		portKey = "port"
	}
	host, err = requiredStringField(document, hostKey)
	if err != nil {
		return "", "", fmt.Errorf("read host for target %q: %w", target, err)
	}
	port, err = optionalNumericField(document, portKey)
	if err != nil {
		return "", "", fmt.Errorf("read port for target %q: %w", target, err)
	}
	if port == "" {
		port = defaultPort
	}
	return host, port, nil
}

// requiredStringField reads a string field from the config document, trims
// surrounding whitespace, and requires the result to be non-empty.
func requiredStringField(document map[string]any, key string) (string, error) {
	value, ok := document[key]
	if !ok {
		return "", fmt.Errorf("missing key %q", key)
	}
	text, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("key %q must be a string", key)
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return "", fmt.Errorf("key %q is empty", key)
	}
	return text, nil
}

// optionalNumericField reads an optional port field, accepting a string or a
// JSON number. A missing or blank field returns an empty string (the caller
// applies the default). A value that is present must be a base-10 integer in the
// valid TCP port range; anything else fails closed with a clear config error
// rather than producing a confusing downstream failure (or host:0).
func optionalNumericField(document map[string]any, key string) (string, error) {
	value, ok := document[key]
	if !ok {
		return "", nil
	}
	switch v := value.(type) {
	case string:
		text := strings.TrimSpace(v)
		if text == "" {
			return "", nil
		}
		return normalizePort(key, text)
	case float64:
		if v != float64(int64(v)) {
			return "", fmt.Errorf("key %q must be an integer", key)
		}
		return normalizePort(key, strconv.FormatInt(int64(v), 10))
	default:
		return "", fmt.Errorf("key %q must be a string or number", key)
	}
}

// normalizePort validates that text is a base-10 integer in the valid TCP port
// range and returns its canonical form.
func normalizePort(key, text string) (string, error) {
	n, err := strconv.Atoi(text)
	if err != nil {
		return "", fmt.Errorf("key %q must be an integer port: %w", key, err)
	}
	if n < 1 || n > 65535 {
		return "", fmt.Errorf("key %q must be a port between 1 and 65535, got %d", key, n)
	}
	return strconv.Itoa(n), nil
}
