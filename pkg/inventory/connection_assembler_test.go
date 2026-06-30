package inventory

import (
	"testing"

	"github.com/go-sql-driver/mysql"
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
// metadata-only target: organization from the endpoint attributes, the service
// token from credentials, and the API URL from configuration. No DSN.
func TestVitessConnectionAssemblerBuildsMetadataTarget(t *testing.T) {
	a := VitessConnectionAssembler{APIURL: "https://localscale.test"}

	dsn, meta, err := a.Assemble(
		"",
		map[string]string{MetadataOrganization: "acme"},
		&Credentials{Metadata: map[string]string{
			MetadataTokenName:  "tok-id",
			MetadataTokenValue: "tok-secret",
		}},
	)
	require.NoError(t, err)
	assert.Empty(t, dsn, "Vitess targets carry connection details in metadata, not a DSN")
	assert.Equal(t, map[string]string{
		MetadataOrganization: "acme",
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
		map[string]string{MetadataOrganization: "acme"},
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
		map[string]string{MetadataOrganization: "acme"},
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
		map[string]string{"ps_org": "acme"},
		&Credentials{Metadata: map[string]string{MetadataTokenName: "id", MetadataTokenValue: "secret"}},
	)
	require.NoError(t, err)
	assert.Equal(t, "acme", meta[MetadataOrganization])
}

// Configured Metadata is merged after the resolved fields so deployments can
// attach extra engine configuration.
func TestVitessConnectionAssemblerMergesConfiguredMetadata(t *testing.T) {
	a := VitessConnectionAssembler{Metadata: map[string]string{"main_branch": "main"}}

	_, meta, err := a.Assemble(
		"",
		map[string]string{MetadataOrganization: "acme"},
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
		map[string]string{MetadataOrganization: "acme"},
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
		map[string]string{MetadataOrganization: "acme"},
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
