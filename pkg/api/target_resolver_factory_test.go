package api

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/inventory"
	"github.com/block/schemabot/pkg/storage"
)

// A data plane that serves more than one engine configures one etre block per
// database type; they compose into a per-type router that selects the engine
// from each request's database type.
func TestBuildTargetResolverComposesPerTypeResolvers(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	cfg := TargetResolverConfig{
		Etre: []EtreConfig{
			{
				Addr: "https://etre.example", DatabaseType: storage.DatabaseTypeMySQL,
				EntityType: "cluster", TargetLabel: "dsid",
				MySQL:       EtreMySQLConfig{HostField: "writer_endpoint"},
				Credentials: EtreCredentialsConfig{Username: "spirit", PasswordRef: "env:DDL_PASSWORD"},
			},
			{
				Addr: "https://etre.example", DatabaseType: storage.DatabaseTypeVitess,
				EntityType: "keyspace", TargetLabel: "dsid",
				Vitess:      EtreVitessConfig{OrganizationAttribute: "organization"},
				Credentials: EtreCredentialsConfig{PasswordRef: "env:PS_TOKEN"},
			},
		},
	}

	resolver, err := cfg.BuildResolver(t.Context(), logger)
	require.NoError(t, err)
	_, ok := resolver.(*inventory.TypeRoutingResolver)
	assert.True(t, ok, "expected a TypeRoutingResolver for multiple per-type etre blocks, got %T", resolver)
}

// With more than one resolver, each must declare a database_type so the router
// can select among them; an omitted type is a startup configuration error.
func TestBuildEtreResolversRejectsMissingDatabaseTypeWhenMultiple(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	etres := []EtreConfig{
		{
			Addr: "https://etre.example", DatabaseType: storage.DatabaseTypeMySQL,
			EntityType: "cluster", TargetLabel: "dsid",
			MySQL:       EtreMySQLConfig{HostField: "writer_endpoint"},
			Credentials: EtreCredentialsConfig{Username: "spirit", PasswordRef: "env:DDL_PASSWORD"},
		},
		{
			Addr:       "https://etre.example", // no database_type
			EntityType: "cluster", TargetLabel: "dsid",
			MySQL:       EtreMySQLConfig{HostField: "writer_endpoint"},
			Credentials: EtreCredentialsConfig{Username: "spirit", PasswordRef: "env:DDL_PASSWORD"},
		},
	}

	_, err := buildEtreResolvers(t.Context(), etres, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "database_type is required")
}

// Two resolvers claiming the same database type is ambiguous and fails closed at
// startup rather than letting map iteration order decide which one routes.
func TestBuildEtreResolversRejectsDuplicateDatabaseType(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	block := EtreConfig{
		Addr: "https://etre.example", DatabaseType: storage.DatabaseTypeMySQL,
		EntityType: "cluster", TargetLabel: "dsid",
		MySQL:       EtreMySQLConfig{HostField: "writer_endpoint"},
		Credentials: EtreCredentialsConfig{Username: "spirit", PasswordRef: "env:DDL_PASSWORD"},
	}

	_, err := buildEtreResolvers(t.Context(), []EtreConfig{block, block}, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), storage.DatabaseTypeMySQL)
	assert.Contains(t, err.Error(), "more than one resolver")
}

// An unknown credentials.type fails closed at startup.
func TestBuildEtreResolverRejectsUnknownCredentialType(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	cfg := EtreConfig{
		Addr: "https://etre.example", DatabaseType: storage.DatabaseTypeMySQL, EntityType: "cluster", TargetLabel: "dsid",
		MySQL:       EtreMySQLConfig{HostField: "writer_endpoint"},
		Credentials: EtreCredentialsConfig{Type: "vault"},
	}

	_, err := buildEtreResolver(t.Context(), cfg, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "vault")
}

// The awssm backend validates its required fields with config-path context at
// startup, before any AWS work, so a misconfiguration fails fast.
func TestBuildCredentialResolverAWSSMRequiresFields(t *testing.T) {
	base := EtreCredentialsConfig{
		Type:       "awssm",
		Region:     "us-east-1",
		RoleARN:    "arn:aws:iam::{account}:role/tern-assumed",
		SecretName: "{target}_ddl_password",
	}

	_, err := buildCredentialResolver(t.Context(), base, nil)
	require.NoError(t, err)

	// role_arn is optional: without it the backend reads from the caller's own
	// account, so the resolver still builds.
	ownAccount := base
	ownAccount.RoleARN = ""
	_, err = buildCredentialResolver(t.Context(), ownAccount, nil)
	require.NoError(t, err)

	cases := map[string]func(*EtreCredentialsConfig){
		"region":      func(c *EtreCredentialsConfig) { c.Region = "" },
		"secret_name": func(c *EtreCredentialsConfig) { c.SecretName = "" },
	}
	for field, mutate := range cases {
		cfg := base
		mutate(&cfg)
		_, err := buildCredentialResolver(t.Context(), cfg, nil)
		require.Error(t, err, field)
		assert.Contains(t, err.Error(), field)
	}

	// A username template (plain-password mode) combined with a token-decoding
	// engine fails fast with a config-path error, before any AWS config loading.
	withUsername := base
	withUsername.Username = "{app}_ddl"
	_, err = buildCredentialResolver(t.Context(), withUsername, inventory.DecodePlanetScaleSecret)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "username")
}

// The assume-role backend's account attribute is surfaced to the resolver even
// when not listed in attribute_fields, so credential resolution can read it.
func TestCredentialAttributeFields(t *testing.T) {
	// Assume-role mode (role_arn set) resolves the target account from an
	// attribute, defaulting to aws_account_id.
	assumeRole := EtreConfig{
		AttributeFields: []string{"region"},
		Credentials:     EtreCredentialsConfig{Type: "awssm", RoleARN: "arn:aws:iam::{account}:role/ddl", SecretName: "secret"},
	}
	assert.Equal(t, []string{"region", "aws_account_id"}, resolverAttributeFields(assumeRole))

	// A custom account attribute is surfaced instead of the default.
	custom := EtreConfig{
		Credentials: EtreCredentialsConfig{Type: "awssm", RoleARN: "arn:aws:iam::{account}:role/ddl", AccountAttribute: "account", SecretName: "secret"},
	}
	assert.Equal(t, []string{"account"}, resolverAttributeFields(custom))

	// Own-account mode (no role_arn) needs no account attribute.
	ownAccount := EtreConfig{
		AttributeFields: []string{"region"},
		Credentials:     EtreCredentialsConfig{Type: "awssm", SecretName: "secret"},
	}
	assert.Equal(t, []string{"region"}, resolverAttributeFields(ownAccount))

	// A secret_name templated over entity attributes surfaces those attributes so
	// the resolver fetches them for the credential backend.
	templated := EtreConfig{
		Credentials: EtreCredentialsConfig{Type: "awssm", SecretName: "{cluster}_schemabot_password"},
	}
	assert.Equal(t, []string{"cluster"}, resolverAttributeFields(templated))

	secretRef := EtreConfig{
		AttributeFields: []string{"region"},
		Credentials:     EtreCredentialsConfig{Type: "secret_ref"},
	}
	assert.Equal(t, []string{"region"}, resolverAttributeFields(secretRef))
}

// A Vitess Etre resolver assembles PlanetScale API metadata, so it needs no
// host_field and no credential username — the token secret carries the
// credential. Startup wiring accepts this shape.
func TestBuildEtreResolverVitess(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	cfg := EtreConfig{
		Addr:         "https://etre.example",
		DatabaseType: storage.DatabaseTypeVitess,
		EntityType:   "planetscale_database",
		TargetLabel:  "dsid",
		EnvLabel:     "env",
		Vitess:       EtreVitessConfig{APIURL: "https://api.planetscale.test"},
		Credentials:  EtreCredentialsConfig{Type: "secret_ref", PasswordRef: `{"token":"id=value"}`},
	}

	resolver, err := buildEtreResolver(t.Context(), cfg, logger)
	require.NoError(t, err)
	require.NotNil(t, resolver)

	// A Vitess resolver still requires the password ref that carries the token.
	noToken := cfg
	noToken.Credentials.PasswordRef = ""
	_, err = buildEtreResolver(t.Context(), noToken, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "password_ref")
}

// Strata is Aurora-backed and reached over the MySQL protocol, so it assembles
// its connection from the MySQL block exactly as the MySQL engine does. The same
// host_field validation applies, failing closed at startup when it is missing.
func TestBuildEtreResolverStrata(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	cfg := EtreConfig{
		Addr: "https://etre.example", DatabaseType: storage.DatabaseTypeStrata,
		EntityType: "aurora_cluster", TargetLabel: "dsid", EnvLabel: "env",
		MySQL:       EtreMySQLConfig{HostField: "writer_endpoint"},
		Credentials: EtreCredentialsConfig{Username: "ddl", PasswordRef: "env:DDL_PASSWORD"},
	}

	resolver, err := buildEtreResolver(t.Context(), cfg, logger)
	require.NoError(t, err)
	require.NotNil(t, resolver)

	// The assembler must report the Strata engine type — not plain MySQL — or the
	// resolver's request-type guard would reject "strata" requests and stamp the
	// resolved target as MySQL, routing it to the wrong data-plane engine.
	assembler, _, err := etreAssembler(cfg)
	require.NoError(t, err)
	assert.Equal(t, storage.DatabaseTypeStrata, assembler.DatabaseType())

	noHost := cfg
	noHost.MySQL.HostField = ""
	_, err = buildEtreResolver(t.Context(), noHost, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "host_field")
	assert.Contains(t, err.Error(), storage.DatabaseTypeStrata)
}

// An unsupported database_type fails closed at startup rather than silently
// resolving as MySQL, so adding an engine (postgres) is a deliberate change at
// the assembler-selection site.
func TestBuildEtreResolverRejectsUnsupportedEngine(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	cfg := EtreConfig{
		Addr:         "https://etre.example",
		DatabaseType: "postgres",
		EntityType:   "pg_cluster",
		TargetLabel:  "dsid",
		Credentials:  EtreCredentialsConfig{Type: "secret_ref", Username: "ddl", PasswordRef: "env:PW"},
	}

	_, err := buildEtreResolver(t.Context(), cfg, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "postgres")
	assert.Contains(t, err.Error(), "not supported")
}

// The Vitess organization attribute is surfaced to the resolver so the assembler
// can read it, even when not listed in attribute_fields.
func TestResolverAttributeFieldsIncludesOrganization(t *testing.T) {
	defaultOrg := EtreConfig{DatabaseType: storage.DatabaseTypeVitess}
	assert.Equal(t, []string{"organization"}, resolverAttributeFields(defaultOrg))

	customOrg := EtreConfig{DatabaseType: storage.DatabaseTypeVitess, Vitess: EtreVitessConfig{OrganizationAttribute: "ps_org"}}
	assert.Equal(t, []string{"ps_org"}, resolverAttributeFields(customOrg))
}

// The Etre resolver's lazily-validated fields are checked at startup so a
// misconfiguration fails fast instead of on the first request.
func TestBuildEtreResolverValidatesConfig(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	base := EtreConfig{
		Addr: "https://etre.example", DatabaseType: storage.DatabaseTypeMySQL, EntityType: "cluster", TargetLabel: "dsid",
		MySQL: EtreMySQLConfig{HostField: "writer_endpoint"}, Credentials: EtreCredentialsConfig{Username: "spirit", PasswordRef: "env:DDL_PASSWORD"},
	}

	_, err := buildEtreResolver(t.Context(), base, logger)
	require.NoError(t, err)

	noHost := base
	noHost.MySQL.HostField = ""
	_, err = buildEtreResolver(t.Context(), noHost, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "host_field")

	noPassword := base
	noPassword.Credentials.PasswordRef = ""
	_, err = buildEtreResolver(t.Context(), noPassword, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "password_ref")

	noUsername := base
	noUsername.Credentials.Username = ""
	_, err = buildEtreResolver(t.Context(), noUsername, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "username")

	// A secret ref that resolves to empty (e.g. an unset env var) fails closed
	// with config context rather than a generic downstream error.
	emptyAddr := base
	emptyAddr.Addr = "env:UNSET_ETRE_ADDR"
	_, err = buildEtreResolver(t.Context(), emptyAddr, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "addr resolved to an empty value")
}

// BuildResolver fails closed on an ambiguous config: configuring both the Etre
// and static backends is rejected rather than silently preferring one, so an
// embedder gets the same guarantee the server enforces.
func TestBuildResolverRejectsBothEtreAndStatic(t *testing.T) {
	cfg := TargetResolverConfig{
		Targets: map[string]inventory.StaticTarget{"db": {}},
		Etre: []EtreConfig{{
			Addr: "https://etre.example", DatabaseType: storage.DatabaseTypeMySQL,
			EntityType: "cluster", TargetLabel: "dsid",
			MySQL:       EtreMySQLConfig{HostField: "writer_endpoint"},
			Credentials: EtreCredentialsConfig{Username: "spirit", PasswordRef: "env:DDL_PASSWORD"},
		}},
	}
	_, err := cfg.BuildResolver(t.Context(), slog.New(slog.DiscardHandler))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "both etre and static targets")
}

// BuildResolver fails closed on an empty config: configuring neither backend is
// an error rather than a degenerate empty resolver.
func TestBuildResolverRejectsNeitherConfigured(t *testing.T) {
	_, err := TargetResolverConfig{}.BuildResolver(t.Context(), slog.New(slog.DiscardHandler))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "neither etre nor static")
}
