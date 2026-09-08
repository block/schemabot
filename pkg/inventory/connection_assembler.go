package inventory

import (
	"encoding/json"
	"fmt"
	"maps"
	"net"
	"net/url"
	"strings"

	"github.com/block/mysql"
	"github.com/block/spirit/pkg/dbconn"
)

// ConnectionAssembler turns a resolved endpoint and credentials into the
// connection fields of a Target for a specific database engine.
//
// It is the engine axis of resolution: written once per engine and reused
// across every inventory source, so endpoint resolution (which source) and
// credential resolution (which secret backend) stay engine-agnostic. Adding an
// engine means adding an assembler, not a new per-source resolver.
type ConnectionAssembler interface {
	// DatabaseType is the engine this assembler targets.
	DatabaseType() string
	// Assemble builds the connection fields of a Target — a DSN and/or
	// engine-specific Metadata — from a resolved endpoint host, the endpoint's
	// attributes, and credentials.
	Assemble(host string, attrs map[string]string, creds *Credentials) (dsn string, metadata map[string]string, err error)
}

// MySQLConnectionAssembler assembles a namespace-free MySQL DSN. The schema is
// injected per operation by the data plane, so the DSN carries no database name.
type MySQLConnectionAssembler struct {
	// Type is the engine type reported for assembled targets, defaulting to
	// "mysql". An engine that connects over the MySQL protocol but routes to a
	// distinct data-plane engine (for example "strata") sets this so the
	// resolver's request-type guard and the resolved target's DatabaseType reflect
	// the real engine rather than plain MySQL.
	Type string
	// DefaultPort is appended to the host when it has no port.
	DefaultPort string
	// Params are extra MySQL DSN parameters (for example TLS settings).
	Params map[string]string
	// Metadata is attached to every assembled target for engine-specific
	// configuration the data plane reads.
	Metadata map[string]string
}

var _ ConnectionAssembler = MySQLConnectionAssembler{}

// DatabaseType returns the configured engine type, defaulting to "mysql".
func (a MySQLConnectionAssembler) DatabaseType() string {
	if a.Type == "" {
		return "mysql"
	}
	return a.Type
}

// Assemble builds a namespace-free MySQL DSN from the host and credentials. The
// endpoint attributes are unused for MySQL.
func (a MySQLConnectionAssembler) Assemble(host string, _ map[string]string, creds *Credentials) (string, map[string]string, error) {
	if host == "" {
		return "", nil, fmt.Errorf("mysql connection requires a host")
	}
	if creds == nil {
		return "", nil, fmt.Errorf("mysql connection requires credentials")
	}
	if a.DefaultPort != "" {
		host = hostWithDefaultPort(host, a.DefaultPort)
	}
	cfg := mysql.NewConfig()
	cfg.User = creds.Username
	cfg.Passwd = creds.Password
	cfg.Net = "tcp"
	cfg.Addr = host
	if len(a.Params) > 0 {
		cfg.Params = maps.Clone(a.Params)
	}
	return cfg.FormatDSN(), maps.Clone(a.Metadata), nil
}

// hostWithDefaultPort returns host with defaultPort appended when host carries
// no port, and host unchanged when it already specifies one. It is robust to
// every form a resolved endpoint may take:
//
//   - "host" / "1.2.3.4"        -> "host:port"        (bare host gains the port)
//   - "host:3306"               -> "host:3306"        (existing port preserved)
//   - "host:"                   -> "host:port"        (empty port filled in)
//   - "2001:db8::1"             -> "[2001:db8::1]:port" (bare IPv6 bracketed)
//   - "[2001:db8::1]"           -> "[2001:db8::1]:port" (bracketed IPv6 gains port)
//   - "[2001:db8::1]:3306"      -> "[2001:db8::1]:3306" (existing port preserved)
//
// net.SplitHostPort succeeds with an empty port for a trailing colon, and a
// bracketed IPv6 literal with no port already carries the brackets that
// net.JoinHostPort would add again — so the raw host must be unwrapped before
// rejoining to avoid a double-bracketed, undialable address.
func hostWithDefaultPort(host, defaultPort string) string {
	if h, port, err := net.SplitHostPort(host); err == nil {
		if port != "" {
			return host
		}
		return net.JoinHostPort(h, defaultPort)
	}
	bare := strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
	return net.JoinHostPort(bare, defaultPort)
}

// PostgresDBNameAttribute is the endpoint attribute the PostgreSQL assembler
// reads the database name from. Unlike MySQL — where the schema is injected per
// operation and the DSN stays namespace-free — a PostgreSQL connection is made
// to one database, so the DSN must carry it; the request namespace selects a
// schema within that database.
const PostgresDBNameAttribute = "dbname"

// MetadataPostgresCARef is the metadata key carrying the CA reference the
// PostgreSQL data plane resolves into verified TLS trust. It is the only
// engine metadata the PostgreSQL assembler emits.
const MetadataPostgresCARef = "postgres_ca_ref"

// PostgresCARefEmbeddedRDSGlobal selects pg-sprite's reviewed, embedded AWS RDS
// global CA bundle. It is the default when the resolved host is an RDS
// endpoint, so an omitted ca_ref stays on verified TLS and never falls back to
// the ambient trust store.
const PostgresCARefEmbeddedRDSGlobal = "embedded:rds-global"

// postgresCARefFilePrefix prefixes a CA reference naming a read-only mounted
// PEM bundle by absolute path, for private proxies and test endpoints whose
// chain is not in the embedded RDS bundle.
const postgresCARefFilePrefix = "file:"

// postgresSSLModeVerifyFull is the only accepted sslmode: encrypt the session,
// validate the certificate chain, and verify the endpoint hostname. Weaker
// modes (require, prefer, verify-ca, disable) prove at most encryption, not
// server identity, so they are rejected rather than silently accepted.
const postgresSSLModeVerifyFull = "verify-full"

// PostgresConnectionAssembler assembles a PostgreSQL Target: one libpq URL
// carrying the database name, built with net/url so every component is escaped
// — never by string concatenation — plus the CA reference in Metadata. A
// database name containing "/" (the one character URL path encoding leaves
// unescaped) is rejected rather than escaped.
//
// Transport is fail-closed: the DSN always carries sslmode=verify-full (the
// only allowlisted value), and the CA reference in Metadata is always explicit
// — the embedded RDS bundle by default for RDS endpoints, a mounted file by
// configuration, and an error for a non-RDS endpoint with no configured CA.
// The data plane does not read the reference yet: the connection layer
// verifies RDS endpoints against its own embedded RDS bundle, while a file:
// reference is recorded but not yet enforced — verification of such an
// endpoint falls back to the ambient trust store until the engine adapter
// consumes the reference in a follow-up.
type PostgresConnectionAssembler struct {
	// DefaultPort is appended to the host when it has no port.
	DefaultPort string
	// Params are extra libpq URL parameters. Only "sslmode" is accepted, and
	// only with the value "verify-full" — the mode every assembled DSN carries
	// regardless, so the field is validate-only; TLS trust material is
	// configured via CARef, never via raw libpq parameters.
	Params map[string]string
	// CARef selects the CA bundle the data plane verifies the server against:
	// PostgresCARefEmbeddedRDSGlobal or "file:<absolute-path>". Empty defaults
	// to the embedded RDS bundle when the host is an RDS endpoint and fails
	// otherwise.
	CARef string
	// Metadata is attached to every assembled target for engine-specific
	// configuration the data plane reads, merged before the authoritative
	// CA reference.
	Metadata map[string]string
}

var _ ConnectionAssembler = PostgresConnectionAssembler{}

// DatabaseType returns the PostgreSQL engine type.
func (PostgresConnectionAssembler) DatabaseType() string { return "postgres" }

// Assemble builds a PostgreSQL libpq URL from the host, the "dbname" endpoint
// attribute, and credentials, and emits the resolved CA reference in Metadata.
func (a PostgresConnectionAssembler) Assemble(host string, attrs map[string]string, creds *Credentials) (string, map[string]string, error) {
	// Hosts are DNS names or IP literals, so trimming stray whitespace and
	// lowercasing preserve identity. Normalizing here keeps the emitted DSN,
	// the CA-reference decision, and the data plane's own case-sensitive RDS
	// check on the DSN host all in agreement.
	host = strings.ToLower(strings.TrimSpace(host))
	if host == "" {
		return "", nil, fmt.Errorf("postgres connection requires a host")
	}
	if creds == nil {
		return "", nil, fmt.Errorf("postgres connection requires credentials")
	}
	if creds.Username == "" {
		return "", nil, fmt.Errorf("postgres connection requires a username")
	}
	// A comma would smuggle a multi-host libpq DSN through single-host
	// validation: pgx dials the hosts in order while the RDS check below
	// matches the end of the whole string, so the TLS policy could be decided
	// by a host that is dialed last or never.
	if strings.Contains(host, ",") {
		return "", nil, fmt.Errorf("postgres connection requires a single host, got %q", host)
	}
	// Characters that are structural in a URL authority — or embedded
	// whitespace — cannot appear in a real host, and would otherwise surface
	// downstream as a misleading CA or DSN parse error instead of naming the
	// malformed host.
	if strings.ContainsAny(host, " \t\r\n@/?#") {
		return "", nil, fmt.Errorf("postgres host contains invalid characters, got %q", host)
	}
	dbname := strings.TrimSpace(attrs[PostgresDBNameAttribute])
	if dbname == "" {
		return "", nil, fmt.Errorf("postgres connection requires the %q endpoint attribute", PostgresDBNameAttribute)
	}
	if strings.Contains(dbname, "/") {
		return "", nil, fmt.Errorf("postgres database name must not contain \"/\", got %q", dbname)
	}
	// Params are validated on every call, not only at config load: Assemble is
	// the only gate for inventory sources that construct the assembler
	// directly.
	if err := validatePostgresDSNParams(a.Params); err != nil {
		return "", nil, err
	}
	if a.DefaultPort != "" {
		host = hostWithDefaultPort(host, a.DefaultPort)
	}
	caRef, err := a.resolveCARef(host)
	if err != nil {
		return "", nil, err
	}
	// The allowlist admits exactly the default, so the query is fixed; Params
	// exist to be validated, never to vary the assembled DSN.
	query := url.Values{"sslmode": {postgresSSLModeVerifyFull}}
	u := url.URL{
		Scheme:   "postgresql",
		User:     url.UserPassword(creds.Username, creds.Password),
		Host:     host,
		Path:     "/" + dbname,
		RawQuery: query.Encode(),
	}
	metadata := maps.Clone(a.Metadata)
	if metadata == nil {
		metadata = make(map[string]string, 1)
	}
	metadata[MetadataPostgresCARef] = caRef
	return u.String(), metadata, nil
}

// resolveCARef returns the CA reference for the resolved host: the configured
// one when set, the embedded RDS bundle for an RDS endpoint, and an error
// otherwise — a verified CA is required, and the ambient trust store is never
// an implicit fallback.
func (a PostgresConnectionAssembler) resolveCARef(host string) (string, error) {
	if a.CARef != "" {
		if err := validatePostgresCARef(a.CARef); err != nil {
			return "", err
		}
		return a.CARef, nil
	}
	// Assemble normalized the host to lowercase, so this case-sensitive
	// suffix check also covers uppercase-configured RDS endpoints.
	if dbconn.IsRDSHost(host) {
		return PostgresCARefEmbeddedRDSGlobal, nil
	}
	return "", fmt.Errorf("a verified CA is required: host %q is not an RDS endpoint and no ca_ref is configured", host)
}

// validatePostgresCARef checks a CA reference's form: the embedded RDS bundle
// selector, or a file reference with an absolute path.
func validatePostgresCARef(caRef string) error {
	if caRef == PostgresCARefEmbeddedRDSGlobal {
		return nil
	}
	if path, ok := strings.CutPrefix(caRef, postgresCARefFilePrefix); ok {
		if !strings.HasPrefix(path, "/") {
			return fmt.Errorf("postgres ca_ref file path must be absolute, got %q", caRef)
		}
		return nil
	}
	return fmt.Errorf("postgres ca_ref must be %q or \"file:<absolute-path>\", got %q", PostgresCARefEmbeddedRDSGlobal, caRef)
}

// validatePostgresDSNParams enforces the closed parameter allowlist for
// assembled PostgreSQL DSNs: only sslmode, and only verify-full. Arbitrary
// libpq parameters could weaken transport security (sslmode downgrades) or
// smuggle TLS material outside CARef (sslrootcert, sslcert), so unknown keys
// fail closed rather than pass through.
func validatePostgresDSNParams(params map[string]string) error {
	for key, value := range params {
		if key != "sslmode" {
			return fmt.Errorf("postgres connection params support only \"sslmode\", got %q", key)
		}
		if value != postgresSSLModeVerifyFull {
			return fmt.Errorf("postgres sslmode must be %q, got %q", postgresSSLModeVerifyFull, value)
		}
	}
	return nil
}

// Metadata keys the Vitess (PlanetScale) engine reads from a resolved Target.
const (
	// MetadataOrganization is the PlanetScale organization that owns the database.
	MetadataOrganization = "organization"
	// MetadataDatabase is the PlanetScale database name. It can differ from the
	// SchemaBot database identifier the target was requested under: the
	// identifier is a routing and display key, while this name is what every
	// PlanetScale API call must address.
	MetadataDatabase = "database"
	// MetadataTokenName is the PlanetScale service token id. sadscan:disable kingfisher.planetscale.2
	MetadataTokenName = "token_name"
	// MetadataTokenValue is the PlanetScale service token secret.
	MetadataTokenValue = "token_value"
	// MetadataAPIURL is the PlanetScale-compatible API base URL.
	MetadataAPIURL = "api_url"
)

// DefaultPlanetScaleAPIURL is the public PlanetScale API endpoint, used when the
// assembler is not configured with an override (for example a LocalScale URL in
// tests).
const DefaultPlanetScaleAPIURL = "https://api.planetscale.com"

// DefaultDatabaseAttribute is the endpoint attribute the Vitess assembler reads
// the PlanetScale database name from when none is configured.
const DefaultDatabaseAttribute = "name"

// VitessConnectionAssembler assembles a Target for Vitess via PlanetScale.
// Vitess applies DDL through the PlanetScale API (organization + database name
// + service token, carried in Metadata), so those fields are always populated:
// the organization and database name come from the resolved endpoint's
// attributes, the service token from credentials, and the API URL from
// deployment configuration.
//
// When the resolved endpoint also exposes a vtgate host and the credentials carry
// a MySQL username, the assembler additionally returns a namespace-free vtgate
// DSN so the engine can read per-shard progress via SHOW VITESS_MIGRATIONS.
// Without a host or username the DSN is empty and progress degrades to the
// deploy-request state only.
type VitessConnectionAssembler struct {
	// OrganizationAttribute is the endpoint attribute holding the PlanetScale
	// organization. Defaults to "organization" when empty.
	OrganizationAttribute string
	// DatabaseAttribute is the endpoint attribute holding the PlanetScale
	// database name. Defaults to "name" when empty. The resolved name rides in
	// Metadata so the SchemaBot database identifier stays an arbitrary routing
	// key while PlanetScale API calls address the database by its own name.
	DatabaseAttribute string
	// APIURL is the PlanetScale-compatible API base URL. A per-target override in
	// the credential secret (api_url) takes precedence; this is the deployment
	// default, itself falling back to DefaultPlanetScaleAPIURL when empty.
	APIURL string
	// DefaultPort is appended to the vtgate host when it carries no port. Empty
	// means the resolved endpoint must already include one.
	DefaultPort string
	// Params are extra MySQL DSN parameters for the vtgate connection (for
	// example TLS settings).
	Params map[string]string
	// Metadata is attached to every assembled target for engine-specific
	// configuration the data plane reads, merged after the resolved fields.
	Metadata map[string]string
}

var _ ConnectionAssembler = VitessConnectionAssembler{}

// DatabaseType returns the Vitess engine type.
func (VitessConnectionAssembler) DatabaseType() string { return "vitess" }

// Assemble builds a Vitess Target. The PlanetScale API fields (organization,
// database name, service token, API URL) always populate Metadata. When a vtgate host and a
// MySQL username are available, a namespace-free vtgate DSN is also returned for
// SHOW VITESS_MIGRATIONS progress; otherwise the DSN is empty (progress falls
// back to the deploy-request state).
func (a VitessConnectionAssembler) Assemble(host string, attrs map[string]string, creds *Credentials) (string, map[string]string, error) {
	if creds == nil {
		return "", nil, fmt.Errorf("vitess connection requires credentials")
	}
	orgAttr := a.OrganizationAttribute
	if orgAttr == "" {
		orgAttr = MetadataOrganization
	}
	organization := attrs[orgAttr]
	if organization == "" {
		return "", nil, fmt.Errorf("vitess connection requires the %q endpoint attribute", orgAttr)
	}
	dbAttr := a.DatabaseAttribute
	if dbAttr == "" {
		dbAttr = DefaultDatabaseAttribute
	}
	database := attrs[dbAttr]
	if database == "" {
		return "", nil, fmt.Errorf("vitess connection requires the %q endpoint attribute", dbAttr)
	}
	tokenName := creds.Metadata[MetadataTokenName]
	tokenValue := creds.Metadata[MetadataTokenValue]
	if tokenName == "" || tokenValue == "" {
		return "", nil, fmt.Errorf("vitess connection requires %q and %q credentials", MetadataTokenName, MetadataTokenValue)
	}
	// A per-target API URL from the secret wins; otherwise the deployment default,
	// then the public PlanetScale endpoint.
	apiURL := creds.Metadata[MetadataAPIURL]
	if apiURL == "" {
		apiURL = a.APIURL
	}
	if apiURL == "" {
		apiURL = DefaultPlanetScaleAPIURL
	}
	// Configured Metadata supplies extra engine fields (for example main_branch)
	// but must not override the resolved connection fields, so it is written
	// first and the authoritative fields last.
	metadata := maps.Clone(a.Metadata)
	if metadata == nil {
		metadata = make(map[string]string, 4)
	}
	metadata[MetadataOrganization] = organization
	metadata[MetadataDatabase] = database
	metadata[MetadataTokenName] = tokenName
	metadata[MetadataTokenValue] = tokenValue
	metadata[MetadataAPIURL] = apiURL
	return a.vtgateDSN(host, creds), metadata, nil
}

// vtgateDSN builds the namespace-free MySQL DSN the engine uses to read per-shard
// progress via SHOW VITESS_MIGRATIONS at the vtgate. It returns "" — not an error
// — when the endpoint exposes no vtgate host or the credentials carry no MySQL
// username: that target is simply API-only and reports deploy-request-level
// progress. The schema is injected per operation, so the DSN carries no database.
func (a VitessConnectionAssembler) vtgateDSN(host string, creds *Credentials) string {
	if host == "" || creds.Username == "" {
		return ""
	}
	// Append the default port only when the host has none (net.SplitHostPort
	// errors when no port is present, including for bare IPv6).
	if a.DefaultPort != "" {
		if _, _, err := net.SplitHostPort(host); err != nil {
			host = net.JoinHostPort(host, a.DefaultPort)
		}
	}
	cfg := mysql.NewConfig()
	cfg.User = creds.Username
	cfg.Passwd = creds.Password
	cfg.Net = "tcp"
	cfg.Addr = host
	if len(a.Params) > 0 {
		cfg.Params = maps.Clone(a.Params)
	}
	return cfg.FormatDSN()
}

// DecodePlanetScaleSecret decodes a PlanetScale credential secret into a Vitess
// Target's credentials. The secret is JSON carrying a service token in
// "name=value" form (for the PlanetScale API), an optional API URL override, and
// optional read-only vtgate MySQL credentials used for SHOW VITESS_MIGRATIONS
// progress:
//
//	{"token": "<id>=<value>", "api_url": "https://...",
//	 "vtgate_username": "...", "vtgate_password": "..."}
//
// The token populates engine Metadata; the vtgate username/password populate the
// MySQL Username/Password the assembler builds the vtgate DSN from. When the
// vtgate fields are absent the target is API-only and progress falls back to the
// deploy-request state. The organization is not part of the secret — it comes
// from the inventory entity. As a SecretDecoder it plugs into any credential
// backend that fetches the raw secret (a reference, or an assumed-role Secrets
// Manager read).
func DecodePlanetScaleSecret(raw string) (*Credentials, error) {
	var secret struct {
		Token          string `json:"token"`
		APIURL         string `json:"api_url"`
		VtgateUsername string `json:"vtgate_username"`
		VtgatePassword string `json:"vtgate_password"`
	}
	if err := json.Unmarshal([]byte(raw), &secret); err != nil {
		return nil, fmt.Errorf("parse planetscale secret as JSON: %w", err)
	}
	tokenName, tokenValue, ok := strings.Cut(secret.Token, "=")
	if !ok || tokenName == "" || tokenValue == "" {
		return nil, fmt.Errorf(`planetscale secret "token" must be in "name=value" form`)
	}
	metadata := map[string]string{
		MetadataTokenName:  tokenName,
		MetadataTokenValue: tokenValue,
	}
	if secret.APIURL != "" {
		metadata[MetadataAPIURL] = secret.APIURL
	}
	return &Credentials{
		Username: secret.VtgateUsername,
		Password: secret.VtgatePassword,
		Metadata: metadata,
	}, nil
}
