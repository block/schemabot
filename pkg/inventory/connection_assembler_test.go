package inventory

import (
	"net/url"
	"testing"

	"github.com/block/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMySQLConnectionAssemblerBuildsNamespaceFreeDSN(t *testing.T) {
	a := MySQLConnectionAssembler{DefaultPort: "3306", Metadata: map[string]string{"pending_drops": "false"}}

	dsn, meta, err := a.Assemble("orders.example", nil, &Credentials{Username: "ddl", Password: "secret"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"pending_drops": "false"}, meta)

	cfg, err := mysql.ParseDSN(dsn)
	require.NoError(t, err)
	assert.Equal(t, "ddl", cfg.User)
	assert.Equal(t, "secret", cfg.Passwd)
	assert.Equal(t, "tcp", cfg.Net)
	assert.Equal(t, "orders.example:3306", cfg.Addr)
	assert.Equal(t, "", cfg.DBName, "MySQL DSN must be namespace-free; the request supplies the schema")
}

func TestMySQLConnectionAssemblerKeepsExistingPort(t *testing.T) {
	a := MySQLConnectionAssembler{DefaultPort: "3306"}

	dsn, _, err := a.Assemble("orders.example:3307", nil, &Credentials{Username: "ddl", Password: "secret"})
	require.NoError(t, err)
	cfg, err := mysql.ParseDSN(dsn)
	require.NoError(t, err)
	assert.Equal(t, "orders.example:3307", cfg.Addr)
}

func TestMySQLConnectionAssemblerPassesParams(t *testing.T) {
	a := MySQLConnectionAssembler{Params: map[string]string{"foo": "bar"}}

	dsn, _, err := a.Assemble("orders.example:3306", nil, &Credentials{Username: "ddl", Password: "secret"})
	require.NoError(t, err)
	cfg, err := mysql.ParseDSN(dsn)
	require.NoError(t, err)
	assert.Equal(t, "bar", cfg.Params["foo"])
}

// A bare IPv6 host has colons but no port; the default port must be appended
// with brackets so the resulting MySQL address is valid.
func TestMySQLConnectionAssemblerBracketsIPv6WithDefaultPort(t *testing.T) {
	a := MySQLConnectionAssembler{DefaultPort: "3306"}

	dsn, _, err := a.Assemble("2001:db8::1", nil, &Credentials{Username: "ddl", Password: "secret"})
	require.NoError(t, err)
	cfg, err := mysql.ParseDSN(dsn)
	require.NoError(t, err)
	assert.Equal(t, "[2001:db8::1]:3306", cfg.Addr)
}

func TestMySQLConnectionAssemblerKeepsBracketedIPv6Port(t *testing.T) {
	a := MySQLConnectionAssembler{DefaultPort: "3306"}

	dsn, _, err := a.Assemble("[2001:db8::1]:3307", nil, &Credentials{Username: "ddl", Password: "secret"})
	require.NoError(t, err)
	cfg, err := mysql.ParseDSN(dsn)
	require.NoError(t, err)
	assert.Equal(t, "[2001:db8::1]:3307", cfg.Addr)
}

// A bracketed IPv6 literal with no port already carries the brackets; the
// default port must be appended without bracketing a second time.
func TestMySQLConnectionAssemblerAppendsPortToBracketedIPv6WithoutPort(t *testing.T) {
	a := MySQLConnectionAssembler{DefaultPort: "3306"}

	dsn, _, err := a.Assemble("[2001:db8::1]", nil, &Credentials{Username: "ddl", Password: "secret"})
	require.NoError(t, err)
	cfg, err := mysql.ParseDSN(dsn)
	require.NoError(t, err)
	assert.Equal(t, "[2001:db8::1]:3306", cfg.Addr)
}

// A host with a trailing colon parses with an empty port; the default port
// fills it in rather than leaving an undialable address.
func TestMySQLConnectionAssemblerFillsEmptyPort(t *testing.T) {
	a := MySQLConnectionAssembler{DefaultPort: "3306"}

	dsn, _, err := a.Assemble("orders.example:", nil, &Credentials{Username: "ddl", Password: "secret"})
	require.NoError(t, err)
	cfg, err := mysql.ParseDSN(dsn)
	require.NoError(t, err)
	assert.Equal(t, "orders.example:3306", cfg.Addr)
}

func TestMySQLConnectionAssemblerRequiresHost(t *testing.T) {
	_, _, err := MySQLConnectionAssembler{}.Assemble("", nil, &Credentials{Username: "ddl", Password: "secret"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "host")
}

func TestMySQLConnectionAssemblerRequiresCredentials(t *testing.T) {
	_, _, err := MySQLConnectionAssembler{}.Assemble("orders.example:3306", nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "credentials")
}

func TestMySQLConnectionAssemblerDatabaseType(t *testing.T) {
	assert.Equal(t, "mysql", MySQLConnectionAssembler{}.DatabaseType())
}

// Vitess connects through the PlanetScale API, so the assembler emits a
// metadata-only target: organization and database name from the endpoint
// attributes, the service token from credentials, and the API URL from
// configuration. No DSN.
func TestVitessConnectionAssemblerBuildsMetadataTarget(t *testing.T) {
	a := VitessConnectionAssembler{APIURL: "https://localscale.test"}

	dsn, meta, err := a.Assemble(
		"",
		map[string]string{MetadataOrganization: "acme", DefaultDatabaseAttribute: "acme_main"},
		&Credentials{Metadata: map[string]string{
			MetadataTokenName:  "tok-id",
			MetadataTokenValue: "tok-secret",
		}},
	)
	require.NoError(t, err)
	assert.Empty(t, dsn, "Vitess targets carry connection details in metadata, not a DSN")
	assert.Equal(t, map[string]string{
		MetadataOrganization: "acme",
		MetadataDatabase:     "acme_main",
		MetadataTokenName:    "tok-id",
		MetadataTokenValue:   "tok-secret",
		MetadataAPIURL:       "https://localscale.test",
	}, meta)
}

// A vtgate DSN is only produced when the credentials carry a MySQL username. A
// token-only credential (PlanetScale API access, no MySQL user) yields the API
// metadata but no DSN, so the apply runs API-only with deploy-request-level
// progress — even when a host is present.
func TestVitessConnectionAssemblerNoVtgateDSNWithoutMySQLUsername(t *testing.T) {
	a := VitessConnectionAssembler{}

	dsn, meta, err := a.Assemble(
		"vtgate.example:3306",
		map[string]string{MetadataOrganization: "acme", DefaultDatabaseAttribute: "acme_main"},
		&Credentials{Metadata: map[string]string{MetadataTokenName: "id", MetadataTokenValue: "secret"}},
	)
	require.NoError(t, err)
	assert.Empty(t, dsn, "no MySQL username → no vtgate DSN even with a host")
	assert.Equal(t, DefaultPlanetScaleAPIURL, meta[MetadataAPIURL])
	assert.Equal(t, "acme", meta[MetadataOrganization])
}

// When the endpoint exposes a vtgate host and the credentials carry a MySQL
// username/password, the assembler returns a namespace-free vtgate DSN for SHOW
// VITESS_MIGRATIONS progress, alongside the PlanetScale API metadata.
func TestVitessConnectionAssemblerBuildsVtgateDSN(t *testing.T) {
	a := VitessConnectionAssembler{APIURL: "https://localscale.test", DefaultPort: "3306"}

	dsn, meta, err := a.Assemble(
		"vtgate.example",
		map[string]string{MetadataOrganization: "acme", DefaultDatabaseAttribute: "acme_main"},
		&Credentials{
			Username: "ddl-user",
			Password: "ddl-pass",
			Metadata: map[string]string{MetadataTokenName: "tok-id", MetadataTokenValue: "tok-secret"},
		},
	)
	require.NoError(t, err)
	// PlanetScale API metadata is still populated.
	assert.Equal(t, "acme", meta[MetadataOrganization])
	assert.Equal(t, "tok-secret", meta[MetadataTokenValue])
	// And a namespace-free vtgate DSN is produced for progress queries.
	cfg, err := mysql.ParseDSN(dsn)
	require.NoError(t, err)
	assert.Equal(t, "ddl-user", cfg.User)
	assert.Equal(t, "ddl-pass", cfg.Passwd)
	assert.Equal(t, "tcp", cfg.Net)
	assert.Equal(t, "vtgate.example:3306", cfg.Addr)
	assert.Equal(t, "", cfg.DBName, "vtgate DSN is namespace-free; the schema is supplied per operation")
}

// A custom organization attribute lets the resolver surface the organization
// under a label other than the default.
func TestVitessConnectionAssemblerCustomOrganizationAttribute(t *testing.T) {
	a := VitessConnectionAssembler{OrganizationAttribute: "ps_org"}

	_, meta, err := a.Assemble(
		"",
		map[string]string{"ps_org": "acme", DefaultDatabaseAttribute: "acme_main"},
		&Credentials{Metadata: map[string]string{MetadataTokenName: "id", MetadataTokenValue: "secret"}},
	)
	require.NoError(t, err)
	assert.Equal(t, "acme", meta[MetadataOrganization])
}

// A custom database attribute lets the resolver surface the PlanetScale
// database name under a label other than the default.
func TestVitessConnectionAssemblerCustomDatabaseAttribute(t *testing.T) {
	a := VitessConnectionAssembler{DatabaseAttribute: "ps_database"}

	_, meta, err := a.Assemble(
		"",
		map[string]string{MetadataOrganization: "acme", "ps_database": "acme_main"},
		&Credentials{Metadata: map[string]string{MetadataTokenName: "id", MetadataTokenValue: "secret"}},
	)
	require.NoError(t, err)
	assert.Equal(t, "acme_main", meta[MetadataDatabase])
}

// The PlanetScale database name attribute is required: without it every API
// call would fall back to addressing the database by its registered
// identifier, which is an arbitrary routing key rather than a PlanetScale name. sadscan:disable kingfisher.planetscale.2
func TestVitessConnectionAssemblerRequiresDatabase(t *testing.T) {
	_, _, err := VitessConnectionAssembler{}.Assemble(
		"",
		map[string]string{MetadataOrganization: "acme"},
		&Credentials{Metadata: map[string]string{MetadataTokenName: "id", MetadataTokenValue: "secret"}},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), DefaultDatabaseAttribute)
}

// Configured Metadata is merged after the resolved fields so deployments can
// attach extra engine configuration.
func TestVitessConnectionAssemblerMergesConfiguredMetadata(t *testing.T) {
	a := VitessConnectionAssembler{Metadata: map[string]string{"main_branch": "main"}}

	_, meta, err := a.Assemble(
		"",
		map[string]string{MetadataOrganization: "acme", DefaultDatabaseAttribute: "acme_main"},
		&Credentials{Metadata: map[string]string{MetadataTokenName: "id", MetadataTokenValue: "secret"}},
	)
	require.NoError(t, err)
	assert.Equal(t, "main", meta["main_branch"])
}

func TestVitessConnectionAssemblerRequiresCredentials(t *testing.T) {
	_, _, err := VitessConnectionAssembler{}.Assemble("", map[string]string{MetadataOrganization: "acme"}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "credentials")
}

func TestVitessConnectionAssemblerRequiresOrganization(t *testing.T) {
	_, _, err := VitessConnectionAssembler{}.Assemble(
		"",
		nil,
		&Credentials{Metadata: map[string]string{MetadataTokenName: "id", MetadataTokenValue: "secret"}},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "organization")
}

func TestVitessConnectionAssemblerRequiresToken(t *testing.T) {
	_, _, err := VitessConnectionAssembler{}.Assemble(
		"",
		map[string]string{MetadataOrganization: "acme", DefaultDatabaseAttribute: "acme_main"},
		&Credentials{Metadata: map[string]string{MetadataTokenName: "id"}},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), MetadataTokenValue)
}

func TestVitessConnectionAssemblerDatabaseType(t *testing.T) {
	assert.Equal(t, "vitess", VitessConnectionAssembler{}.DatabaseType())
}

// A per-target API URL carried in the credential metadata overrides the
// assembler's configured default.
func TestVitessConnectionAssemblerSecretAPIURLOverridesConfig(t *testing.T) {
	a := VitessConnectionAssembler{APIURL: "https://configured.example"}

	_, meta, err := a.Assemble(
		"",
		map[string]string{MetadataOrganization: "acme", DefaultDatabaseAttribute: "acme_main"},
		&Credentials{Metadata: map[string]string{
			MetadataTokenName:  "id",
			MetadataTokenValue: "secret",
			MetadataAPIURL:     "https://from-secret.example",
		}},
	)
	require.NoError(t, err)
	assert.Equal(t, "https://from-secret.example", meta[MetadataAPIURL])
}

// The PlanetScale secret decoder splits the "name=value" token and surfaces the
// optional API URL, leaving organization to the inventory entity.
func TestDecodePlanetScaleSecret(t *testing.T) {
	creds, err := DecodePlanetScaleSecret(`{"token":"tok-id=tok-secret","api_url":"https://localscale.test"}`)
	require.NoError(t, err)
	assert.Empty(t, creds.Username)
	assert.Empty(t, creds.Password)
	assert.Equal(t, "tok-id", creds.Metadata[MetadataTokenName])
	assert.Equal(t, "tok-secret", creds.Metadata[MetadataTokenValue])
	assert.Equal(t, "https://localscale.test", creds.Metadata[MetadataAPIURL])
	assert.NotContains(t, creds.Metadata, MetadataOrganization, "organization comes from the entity, not the secret")
}

func TestDecodePlanetScaleSecretOptionalAPIURL(t *testing.T) {
	creds, err := DecodePlanetScaleSecret(`{"token":"tok-id=tok-secret"}`)
	require.NoError(t, err)
	assert.Equal(t, "tok-id", creds.Metadata[MetadataTokenName])
	assert.NotContains(t, creds.Metadata, MetadataAPIURL, "api_url is optional in the secret")
}

// A secret carrying read-only vtgate credentials populates the MySQL
// Username/Password the assembler builds the vtgate DSN from, alongside the API
// token metadata. The vtgate fields are optional — a token-only secret leaves
// them empty (covered by TestDecodePlanetScaleSecret).
func TestDecodePlanetScaleSecretVtgateCredentials(t *testing.T) {
	creds, err := DecodePlanetScaleSecret(
		`{"token":"tok-id=tok-secret","vtgate_username":"vt-user","vtgate_password":"vt-pass"}`)
	require.NoError(t, err)
	assert.Equal(t, "vt-user", creds.Username)
	assert.Equal(t, "vt-pass", creds.Password)
	assert.Equal(t, "tok-id", creds.Metadata[MetadataTokenName])
}

func TestDecodePlanetScaleSecretRejectsBadInput(t *testing.T) {
	_, err := DecodePlanetScaleSecret("not-json")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "JSON")

	_, err = DecodePlanetScaleSecret(`{"token":"missing-separator"}`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "name=value")
}

// The PostgreSQL assembler emits one libpq URL carrying the database name —
// unlike MySQL, where the request supplies the schema per operation — with
// verify-full TLS and the resolved CA reference in metadata.
func TestPostgresConnectionAssemblerBuildsLibpqURL(t *testing.T) {
	configured := map[string]string{"extra": "field"}
	a := PostgresConnectionAssembler{DefaultPort: "5432", Metadata: configured}

	dsn, meta, err := a.Assemble(
		"orders.cluster-abc.us-east-1.rds.amazonaws.com",
		map[string]string{PostgresDBNameAttribute: "orders"},
		&Credentials{Username: "pgsprite_engine", Password: "s3cret"},
	)
	require.NoError(t, err)
	assert.Equal(t, "postgresql://pgsprite_engine:s3cret@orders.cluster-abc.us-east-1.rds.amazonaws.com:5432/orders?sslmode=verify-full", dsn) // sadscan:disable np.postgres.1
	assert.Equal(t, map[string]string{
		"extra":               "field",
		MetadataPostgresCARef: PostgresCARefEmbeddedRDSGlobal,
	}, meta)
	// The assembler is a shared value type: the caller's map must not gain the
	// resolved CA reference.
	assert.Equal(t, map[string]string{"extra": "field"}, configured)
}

// DNS names are case-insensitive: an uppercase RDS endpoint resolves the same
// embedded-bundle default instead of failing as a non-RDS host, and the
// assembled DSN carries the lowercased host so the data plane's own
// case-sensitive RDS check agrees with the CA decision made here.
func TestPostgresConnectionAssemblerRDSHostCaseInsensitive(t *testing.T) {
	a := PostgresConnectionAssembler{DefaultPort: "5432"}

	dsn, meta, err := a.Assemble(
		"ORDERS.CLUSTER-ABC.US-EAST-1.RDS.AMAZONAWS.COM",
		map[string]string{PostgresDBNameAttribute: "orders"},
		&Credentials{Username: "ddl", Password: "secret"},
	)
	require.NoError(t, err)
	assert.Equal(t, PostgresCARefEmbeddedRDSGlobal, meta[MetadataPostgresCARef])
	assert.Contains(t, dsn, "@orders.cluster-abc.us-east-1.rds.amazonaws.com:5432/")
}

// Stray surrounding whitespace on a host is a config typo, not a different
// host: it is trimmed rather than surfacing later as a misleading CA or DSN
// parse error. Embedded structural characters, by contrast, can never be part
// of a real host and are rejected with an error that names the malformed host.
func TestPostgresConnectionAssemblerNormalizesAndRejectsMalformedHost(t *testing.T) {
	a := PostgresConnectionAssembler{DefaultPort: "5432"}
	creds := &Credentials{Username: "ddl", Password: "secret"}
	dbname := map[string]string{PostgresDBNameAttribute: "orders"}

	_, meta, err := a.Assemble("  db.example.rds.amazonaws.com \n", dbname, creds)
	require.NoError(t, err)
	assert.Equal(t, PostgresCARefEmbeddedRDSGlobal, meta[MetadataPostgresCARef])

	for _, host := range []string{
		"evil.example@db.example.rds.amazonaws.com",
		"db.example .rds.amazonaws.com",
		"db.example.rds.amazonaws.com/extra",
		"db.example.rds.amazonaws.com?sslmode=disable",
		"db.example.rds.amazonaws.com#frag",
	} {
		_, _, err := a.Assemble(host, dbname, creds)
		require.Error(t, err, "host %q must be rejected", host)
		assert.Contains(t, err.Error(), "invalid characters")
	}
}

// A comma in the host would smuggle a multi-host libpq DSN through single-host
// validation: pgx dials the hosts in order while the RDS check matches the end
// of the whole string, so the TLS policy could be decided by a host that is
// never dialed. Rejected regardless of ordering.
func TestPostgresConnectionAssemblerRejectsMultiHost(t *testing.T) {
	a := PostgresConnectionAssembler{DefaultPort: "5432"}
	creds := &Credentials{Username: "ddl", Password: "secret"}
	dbname := map[string]string{PostgresDBNameAttribute: "orders"}

	for _, host := range []string{
		"attacker.example,db.example.rds.amazonaws.com",
		"db.example.rds.amazonaws.com,attacker.example",
	} {
		_, _, err := a.Assemble(host, dbname, creds)
		require.Error(t, err, "host %q must be rejected", host)
		assert.Contains(t, err.Error(), "single host")
	}
}

// A database name containing "/" is the one character net/url would pass
// through unescaped — it would add path segments instead of naming a database
// — so it is rejected, as is a whitespace-only name.
func TestPostgresConnectionAssemblerRejectsMalformedDBName(t *testing.T) {
	a := PostgresConnectionAssembler{DefaultPort: "5432"}
	creds := &Credentials{Username: "ddl", Password: "secret"}

	_, _, err := a.Assemble(
		"db.example.rds.amazonaws.com",
		map[string]string{PostgresDBNameAttribute: "orders/evil"},
		creds,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `must not contain "/"`)

	_, _, err = a.Assemble(
		"db.example.rds.amazonaws.com",
		map[string]string{PostgresDBNameAttribute: "   "},
		creds,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `requires the "dbname" endpoint attribute`)
}

// Credentials and database names are escaped through net/url, so characters
// that are structural in a URL cannot corrupt the DSN or leak into the wrong
// component.
func TestPostgresConnectionAssemblerEscapesComponents(t *testing.T) {
	a := PostgresConnectionAssembler{DefaultPort: "5432"}

	dsn, _, err := a.Assemble(
		"db.example.rds.amazonaws.com",
		map[string]string{PostgresDBNameAttribute: "orders db"},
		&Credentials{Username: "user@corp", Password: "p@ss/word:1?&"},
	)
	require.NoError(t, err)

	u, err := url.Parse(dsn)
	require.NoError(t, err)
	assert.Equal(t, "user@corp", u.User.Username())
	password, set := u.User.Password()
	require.True(t, set)
	assert.Equal(t, "p@ss/word:1?&", password)
	assert.Equal(t, "/orders db", u.Path)
	assert.Equal(t, "verify-full", u.Query().Get("sslmode"))
}

func TestPostgresConnectionAssemblerKeepsExistingPort(t *testing.T) {
	a := PostgresConnectionAssembler{DefaultPort: "5432"}

	dsn, _, err := a.Assemble(
		"db.example.rds.amazonaws.com:6432",
		map[string]string{PostgresDBNameAttribute: "orders"},
		&Credentials{Username: "ddl", Password: "secret"},
	)
	require.NoError(t, err)
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	assert.Equal(t, "db.example.rds.amazonaws.com:6432", u.Host)
}

// sslmode is a closed allowlist: verify-full passes through, anything weaker is
// rejected rather than silently downgrading transport security.
func TestPostgresConnectionAssemblerSSLModeAllowlist(t *testing.T) {
	verified := PostgresConnectionAssembler{Params: map[string]string{"sslmode": "verify-full"}}
	dsn, _, err := verified.Assemble(
		"db.example.rds.amazonaws.com:5432",
		map[string]string{PostgresDBNameAttribute: "orders"},
		&Credentials{Username: "ddl", Password: "secret"},
	)
	require.NoError(t, err)
	assert.Contains(t, dsn, "sslmode=verify-full")

	for _, mode := range []string{"require", "prefer", "verify-ca", "disable"} {
		weak := PostgresConnectionAssembler{Params: map[string]string{"sslmode": mode}}
		_, _, err := weak.Assemble(
			"db.example.rds.amazonaws.com:5432",
			map[string]string{PostgresDBNameAttribute: "orders"},
			&Credentials{Username: "ddl", Password: "secret"},
		)
		require.Error(t, err, "sslmode %q must be rejected", mode)
		assert.Contains(t, err.Error(), "verify-full")
	}
}

// Params other than sslmode are rejected: arbitrary libpq parameters could
// weaken transport security or smuggle TLS material outside the CA reference.
func TestPostgresConnectionAssemblerRejectsUnknownParams(t *testing.T) {
	a := PostgresConnectionAssembler{Params: map[string]string{"sslrootcert": "/tmp/ca.pem"}}

	_, _, err := a.Assemble(
		"db.example.rds.amazonaws.com:5432",
		map[string]string{PostgresDBNameAttribute: "orders"},
		&Credentials{Username: "ddl", Password: "secret"},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `support only "sslmode"`)
}

// A file: CA reference names a mounted PEM bundle by absolute path; relative
// paths and unknown forms are rejected.
func TestPostgresConnectionAssemblerCARefForms(t *testing.T) {
	fileRef := PostgresConnectionAssembler{CARef: "file:/etc/ssl/private-ca.pem"}
	_, meta, err := fileRef.Assemble(
		"proxy.internal.example:5432",
		map[string]string{PostgresDBNameAttribute: "orders"},
		&Credentials{Username: "ddl", Password: "secret"},
	)
	require.NoError(t, err)
	assert.Equal(t, "file:/etc/ssl/private-ca.pem", meta[MetadataPostgresCARef])

	relative := PostgresConnectionAssembler{CARef: "file:relative/ca.pem"}
	_, _, err = relative.Assemble(
		"proxy.internal.example:5432",
		map[string]string{PostgresDBNameAttribute: "orders"},
		&Credentials{Username: "ddl", Password: "secret"},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be absolute")

	unknown := PostgresConnectionAssembler{CARef: "system"}
	_, _, err = unknown.Assemble(
		"proxy.internal.example:5432",
		map[string]string{PostgresDBNameAttribute: "orders"},
		&Credentials{Username: "ddl", Password: "secret"},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "embedded:rds-global")
}

// A non-RDS host with no configured CA reference fails closed: the ambient
// trust store is never an implicit fallback.
func TestPostgresConnectionAssemblerRequiresCAForNonRDSHost(t *testing.T) {
	a := PostgresConnectionAssembler{DefaultPort: "5432"}

	_, _, err := a.Assemble(
		"proxy.internal.example",
		map[string]string{PostgresDBNameAttribute: "orders"},
		&Credentials{Username: "ddl", Password: "secret"},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "verified CA is required")
}

func TestPostgresConnectionAssemblerRequiredInputs(t *testing.T) {
	a := PostgresConnectionAssembler{DefaultPort: "5432"}
	creds := &Credentials{Username: "ddl", Password: "secret"}
	dbname := map[string]string{PostgresDBNameAttribute: "orders"}

	_, _, err := a.Assemble("", dbname, creds)
	assert.ErrorContains(t, err, "requires a host")

	_, _, err = a.Assemble("db.example.rds.amazonaws.com", dbname, nil)
	assert.ErrorContains(t, err, "requires credentials")

	_, _, err = a.Assemble("db.example.rds.amazonaws.com", dbname, &Credentials{Password: "secret"})
	assert.ErrorContains(t, err, "requires a username")

	_, _, err = a.Assemble("db.example.rds.amazonaws.com", nil, creds)
	assert.ErrorContains(t, err, `requires the "dbname" endpoint attribute`)
}
