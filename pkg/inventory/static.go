package inventory

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"

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
	// when the resolver is constructed, and cached. Mutually exclusive with
	// DSNFrom.
	DSN string `yaml:"dsn,omitempty"`
	// DSNFrom assembles a namespace-free MySQL DSN from separate config and
	// password secret references. Unlike DSN it is resolved fresh on every request
	// so a rotated target credential (for example one re-synced by the External
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
}

// StaticDSNFromConfig assembles a namespace-free MySQL DSN for a static target
// from secret references, resolved fresh on every request. It deliberately never
// reads a database name: the per-operation request supplies the schema, so the
// assembled DSN carries none (a DSN with a database name is rejected).
type StaticDSNFromConfig struct {
	// ConfigRef is a secret reference to a JSON document holding the target's
	// connection metadata (host and port). Like PasswordRef it may contain a
	// "{target}" placeholder, replaced with the request target for per-target
	// secret naming, and is resolved fresh on every request.
	ConfigRef string `yaml:"config_ref"`
	// ConfigPaths selects which keys in ConfigRef hold the host and port,
	// defaulting to "host" and "port".
	ConfigPaths StaticDSNFromPaths `yaml:"config_paths,omitempty"`
	// Username is the database user included in the assembled DSN.
	Username string `yaml:"username"`
	// PasswordRef is a secret reference to the database user's password. It may
	// contain a "{target}" placeholder, replaced with the request target for
	// per-target secret naming, and is resolved fresh on every request.
	PasswordRef string `yaml:"password_ref"`
	// Params are appended as MySQL DSN query parameters (for example TLS
	// settings).
	Params map[string]string `yaml:"params,omitempty"`
}

// StaticDSNFromPaths selects the host and port keys in a StaticDSNFromConfig's
// config document. Keys are looked up at the top level of the document.
type StaticDSNFromPaths struct {
	Host string `yaml:"host,omitempty"`
	Port string `yaml:"port,omitempty"`
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
	dsn, err := entry.resolveDSN(ctx, req)
	if err != nil {
		return nil, err
	}
	return &Target{
		Target:       entry.target,
		DatabaseType: entry.databaseType,
		DSN:          dsn,
		Metadata:     maps.Clone(entry.metadata),
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
	prepared := &staticTargetEntry{
		target:       target,
		databaseType: databaseType,
		metadata:     maps.Clone(entry.Metadata),
	}
	if hasDSNFrom {
		if databaseType != "mysql" {
			return nil, fmt.Errorf("target %q dsn_from is only supported for mysql, not %q", target, databaseType)
		}
		if err := entry.DSNFrom.validate(target); err != nil {
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

// resolveDSN returns the entry's DSN: the cached value for a dsn entry, or a
// freshly assembled namespace-free DSN for a dsn_from entry.
func (e staticTargetEntry) resolveDSN(ctx context.Context, req Request) (string, error) {
	if e.dsn != nil {
		return *e.dsn, nil
	}
	dsn, err := e.dsnFrom.assemble(ctx, req, e.databaseType)
	if err != nil {
		return "", err
	}
	if err := validateResolvedStaticTargetDSN(e.target, e.databaseType, dsn); err != nil {
		return "", err
	}
	return dsn, nil
}

func canonicalDatabaseType(databaseType string) string {
	return strings.ToLower(strings.TrimSpace(databaseType))
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
// DSN. The password is not read here — it is resolved per request so a rotated
// credential is picked up without a restart.
func (c *StaticDSNFromConfig) validate(target string) error {
	if c.ConfigRef == "" {
		return fmt.Errorf("target %q dsn_from missing config_ref", target)
	}
	if c.Username == "" {
		return fmt.Errorf("target %q dsn_from missing username", target)
	}
	if c.PasswordRef == "" {
		return fmt.Errorf("target %q dsn_from missing password_ref", target)
	}
	return nil
}

// assemble builds a namespace-free MySQL DSN from the config document (host and
// port) and the password reference, both resolved fresh. It reuses the shared
// MySQL assembler and the fail-closed secret-ref credential resolver so a rotated
// password or empty secret is handled identically to the Etre data-plane path.
func (c *StaticDSNFromConfig) assemble(ctx context.Context, req Request, databaseType string) (string, error) {
	host, port, err := c.resolveEndpoint(ctx, req.Target)
	if err != nil {
		return "", err
	}
	creds, err := SecretRefCredentialResolver{Username: c.Username, PasswordRef: c.PasswordRef}.ResolveCredentials(ctx, req, nil)
	if err != nil {
		return "", err
	}
	assembler := MySQLConnectionAssembler{Type: databaseType, DefaultPort: port, Params: c.Params}
	dsn, _, err := assembler.Assemble(host, nil, creds)
	if err != nil {
		return "", fmt.Errorf("assemble DSN for target %q: %w", req.Target, err)
	}
	return dsn, nil
}

// resolveEndpoint resolves the config document and reads the target host and
// port from it. The port defaults to 3306 when absent; the database name in the
// document, if any, is ignored (static target DSNs are namespace-free).
func (c *StaticDSNFromConfig) resolveEndpoint(ctx context.Context, target string) (host, port string, err error) {
	ref := strings.ReplaceAll(c.ConfigRef, "{target}", target)
	document, err := secrets.ResolveContext(ctx, ref, "")
	if err != nil {
		return "", "", fmt.Errorf("resolve config reference for target %q: %w", target, err)
	}
	if document == "" {
		return "", "", fmt.Errorf("config reference for target %q resolved an empty value", target)
	}
	var config map[string]any
	if err := json.Unmarshal([]byte(document), &config); err != nil {
		return "", "", fmt.Errorf("parse config for target %q: %w", target, err)
	}
	hostKey := c.ConfigPaths.Host
	if hostKey == "" {
		hostKey = "host"
	}
	portKey := c.ConfigPaths.Port
	if portKey == "" {
		portKey = "port"
	}
	host, err = requiredStringField(config, hostKey)
	if err != nil {
		return "", "", fmt.Errorf("read host for target %q: %w", target, err)
	}
	port, err = optionalNumericField(config, portKey)
	if err != nil {
		return "", "", fmt.Errorf("read port for target %q: %w", target, err)
	}
	if port == "" {
		port = "3306"
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
