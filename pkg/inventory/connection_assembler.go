package inventory

import (
	"encoding/json"
	"fmt"
	"maps"
	"net"
	"strings"

	"github.com/go-sql-driver/mysql"
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

// Metadata keys the Vitess (PlanetScale) engine reads from a resolved Target.
const (
	// MetadataOrganization is the PlanetScale organization that owns the database.
	MetadataOrganization = "organization"
	// MetadataTokenName is the PlanetScale service token id.
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

// VitessConnectionAssembler assembles a Target for Vitess via PlanetScale.
// Vitess applies DDL through the PlanetScale API (organization + service token,
// carried in Metadata), so those fields are always populated: the organization
// comes from the resolved endpoint's attributes, the service token from
// credentials, and the API URL from deployment configuration.
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
// service token, API URL) always populate Metadata. When a vtgate host and a
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
