package commands

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/inventory"
	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/mysqlstore"
	"github.com/block/schemabot/pkg/tern"
)

// The data-plane gRPC server routes by opaque target when a target resolver is
// configured, so an operator running serve --grpc against a target inventory
// connects per request rather than binding to one database at startup.
func TestBuildGRPCTernClientRoutesWhenTargetResolverConfigured(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	config := &api.ServerConfig{
		TargetResolver: api.TargetResolverConfig{
			Targets: map[string]inventory.StaticTarget{
				"dsid-orders-prod": {
					DatabaseType: storage.DatabaseTypeMySQL,
					DSN:          "root@tcp(localhost:3306)/",
				},
			},
		},
	}

	client, err := buildGRPCTernClient(t.Context(), config, mysqlstore.New(nil), logger, "production")
	require.NoError(t, err)
	require.NotNil(t, client)
	_, ok := client.(*tern.TargetRouter)
	assert.True(t, ok, "expected a TargetRouter when target_resolver is configured")
}

// When a target_resolver.etre block is configured, the data plane routes
// through the Etre-backed resolver, resolving each target against Etre per
// request rather than binding to one database.
func TestBuildGRPCTernClientRoutesViaEtreResolver(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	config := &api.ServerConfig{
		TargetResolver: api.TargetResolverConfig{
			Etre: api.EtreConfig{
				Addr:         "https://etre.example",
				DatabaseType: storage.DatabaseTypeMySQL,
				EntityType:   "cluster",
				TargetLabel:  "dsid",
				MySQL:        api.EtreMySQLConfig{HostField: "writer_endpoint"},
				Credentials:  api.EtreCredentialsConfig{Username: "spirit", PasswordRef: "env:DDL_PASSWORD"},
			},
		},
	}

	client, err := buildGRPCTernClient(t.Context(), config, mysqlstore.New(nil), logger, "")
	require.NoError(t, err)
	_, ok := client.(*tern.TargetRouter)
	assert.True(t, ok, "expected a TargetRouter when target_resolver.etre is configured")
}

// The credential backend is selectable: with credentials.type=awssm the data
// plane uses the assume-role Secrets Manager resolver instead of a secret ref.
func TestBuildGRPCTernClientRoutesViaEtreWithAWSSMCredentials(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	config := &api.ServerConfig{
		TargetResolver: api.TargetResolverConfig{
			Etre: api.EtreConfig{
				Addr:         "https://etre.example",
				DatabaseType: storage.DatabaseTypeMySQL,
				EntityType:   "cluster",
				TargetLabel:  "dsid",
				MySQL:        api.EtreMySQLConfig{HostField: "writer_endpoint"},
				Credentials: api.EtreCredentialsConfig{
					Type:       "awssm",
					Region:     "us-west-2",
					RoleARN:    "arn:aws:iam::{account}:role/tern-assumed",
					SecretName: "schemabot/{target}/ddl",
				},
			},
		},
	}

	client, err := buildGRPCTernClient(t.Context(), config, mysqlstore.New(nil), logger, "")
	require.NoError(t, err)
	_, ok := client.(*tern.TargetRouter)
	assert.True(t, ok, "expected a TargetRouter with awssm credentials")
}

// An unknown credentials.type fails closed at startup.
func TestBuildEtreResolverRejectsUnknownCredentialType(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	cfg := api.EtreConfig{
		Addr: "https://etre.example", DatabaseType: storage.DatabaseTypeMySQL, EntityType: "cluster", TargetLabel: "dsid",
		MySQL:       api.EtreMySQLConfig{HostField: "writer_endpoint"},
		Credentials: api.EtreCredentialsConfig{Type: "vault"},
	}

	_, err := buildEtreResolver(t.Context(), cfg, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "vault")
}

// The awssm backend validates its required fields with config-path context at
// startup, before any AWS work, so a misconfiguration fails fast.
func TestBuildCredentialResolverAWSSMRequiresFields(t *testing.T) {
	base := api.EtreCredentialsConfig{
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

	cases := map[string]func(*api.EtreCredentialsConfig){
		"region":      func(c *api.EtreCredentialsConfig) { c.Region = "" },
		"secret_name": func(c *api.EtreCredentialsConfig) { c.SecretName = "" },
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
	assumeRole := api.EtreConfig{
		AttributeFields: []string{"region"},
		Credentials:     api.EtreCredentialsConfig{Type: "awssm", RoleARN: "arn:aws:iam::{account}:role/ddl", SecretName: "secret"},
	}
	assert.Equal(t, []string{"region", "aws_account_id"}, resolverAttributeFields(assumeRole))

	// A custom account attribute is surfaced instead of the default.
	custom := api.EtreConfig{
		Credentials: api.EtreCredentialsConfig{Type: "awssm", RoleARN: "arn:aws:iam::{account}:role/ddl", AccountAttribute: "account", SecretName: "secret"},
	}
	assert.Equal(t, []string{"account"}, resolverAttributeFields(custom))

	// Own-account mode (no role_arn) needs no account attribute.
	ownAccount := api.EtreConfig{
		AttributeFields: []string{"region"},
		Credentials:     api.EtreCredentialsConfig{Type: "awssm", SecretName: "secret"},
	}
	assert.Equal(t, []string{"region"}, resolverAttributeFields(ownAccount))

	// A secret_name templated over entity attributes surfaces those attributes so
	// the resolver fetches them for the credential backend.
	templated := api.EtreConfig{
		Credentials: api.EtreCredentialsConfig{Type: "awssm", SecretName: "{cluster}_schemabot_password"},
	}
	assert.Equal(t, []string{"cluster"}, resolverAttributeFields(templated))

	secretRef := api.EtreConfig{
		AttributeFields: []string{"region"},
		Credentials:     api.EtreCredentialsConfig{Type: "secret_ref"},
	}
	assert.Equal(t, []string{"region"}, resolverAttributeFields(secretRef))
}

// A Vitess Etre resolver assembles PlanetScale API metadata, so it needs no
// host_field and no credential username — the token secret carries the
// credential. Startup wiring accepts this shape.
func TestBuildEtreResolverVitess(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	cfg := api.EtreConfig{
		Addr:         "https://etre.example",
		DatabaseType: storage.DatabaseTypeVitess,
		EntityType:   "planetscale_database",
		TargetLabel:  "dsid",
		EnvLabel:     "env",
		Vitess:       api.EtreVitessConfig{APIURL: "https://api.planetscale.test"},
		Credentials:  api.EtreCredentialsConfig{Type: "secret_ref", PasswordRef: `{"token":"id=value"}`},
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

// An unsupported database_type fails closed at startup rather than silently
// resolving as MySQL, so adding an engine (postgres, strata) is a deliberate
// change at the assembler-selection site.
func TestBuildEtreResolverRejectsUnsupportedEngine(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	cfg := api.EtreConfig{
		Addr:         "https://etre.example",
		DatabaseType: "postgres",
		EntityType:   "pg_cluster",
		TargetLabel:  "dsid",
		Credentials:  api.EtreCredentialsConfig{Type: "secret_ref", Username: "ddl", PasswordRef: "env:PW"},
	}

	_, err := buildEtreResolver(t.Context(), cfg, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "postgres")
	assert.Contains(t, err.Error(), "not supported")
}

// The Vitess organization attribute is surfaced to the resolver so the assembler
// can read it, even when not listed in attribute_fields.
func TestResolverAttributeFieldsIncludesOrganization(t *testing.T) {
	defaultOrg := api.EtreConfig{DatabaseType: storage.DatabaseTypeVitess}
	assert.Equal(t, []string{"organization"}, resolverAttributeFields(defaultOrg))

	customOrg := api.EtreConfig{DatabaseType: storage.DatabaseTypeVitess, Vitess: api.EtreVitessConfig{OrganizationAttribute: "ps_org"}}
	assert.Equal(t, []string{"ps_org"}, resolverAttributeFields(customOrg))
}

// Configuring both the Etre resolver and static targets is ambiguous until
// per-target overrides exist, so startup fails closed rather than silently
// picking one.
func TestBuildGRPCTernClientErrorsWhenEtreAndStaticBothConfigured(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	config := &api.ServerConfig{
		TargetResolver: api.TargetResolverConfig{
			Targets: map[string]inventory.StaticTarget{
				"dsid-orders-prod": {DatabaseType: storage.DatabaseTypeMySQL, DSN: "root@tcp(localhost:3306)/"},
			},
			Etre: api.EtreConfig{
				Addr: "https://etre.example", EntityType: "cluster", TargetLabel: "dsid",
				MySQL: api.EtreMySQLConfig{HostField: "writer_endpoint"}, Credentials: api.EtreCredentialsConfig{Username: "spirit", PasswordRef: "env:DDL_PASSWORD"},
			},
		},
	}

	_, err := buildGRPCTernClient(t.Context(), config, mysqlstore.New(nil), logger, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "both etre and static")
}

// The Etre resolver's lazily-validated fields are checked at startup so a
// misconfiguration fails fast instead of on the first request.
func TestBuildEtreResolverValidatesConfig(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	base := api.EtreConfig{
		Addr: "https://etre.example", DatabaseType: storage.DatabaseTypeMySQL, EntityType: "cluster", TargetLabel: "dsid",
		MySQL: api.EtreMySQLConfig{HostField: "writer_endpoint"}, Credentials: api.EtreCredentialsConfig{Username: "spirit", PasswordRef: "env:DDL_PASSWORD"},
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

// Without a target resolver the data plane falls back to a single LocalClient
// bound to the first database configured for the environment, preserving the
// pre-router single-database serving mode.
func TestBuildGRPCTernClientFallsBackToSingleDatabase(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	config := &api.ServerConfig{
		Databases: map[string]api.DatabaseConfig{
			"orders": {
				Type: storage.DatabaseTypeMySQL,
				Environments: map[string]api.EnvironmentConfig{
					"production": {DSN: "root@tcp(localhost:3306)/orders"},
				},
			},
		},
	}

	client, err := buildGRPCTernClient(t.Context(), config, mysqlstore.New(nil), logger, "production")
	require.NoError(t, err)
	require.NotNil(t, client)
	_, ok := client.(*tern.LocalClient)
	assert.True(t, ok, "expected a LocalClient when only databases are configured")
}

// In target-routing mode each request carries its own environment, so the
// server-level TERN_ENVIRONMENT is not required to start.
func TestBuildGRPCTernClientRoutesWithoutEnvironment(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	config := &api.ServerConfig{
		TargetResolver: api.TargetResolverConfig{
			Targets: map[string]inventory.StaticTarget{
				"dsid-orders-prod": {
					DatabaseType: storage.DatabaseTypeMySQL,
					DSN:          "root@tcp(localhost:3306)/",
				},
			},
		},
	}

	client, err := buildGRPCTernClient(t.Context(), config, mysqlstore.New(nil), logger, "")
	require.NoError(t, err)
	_, ok := client.(*tern.TargetRouter)
	assert.True(t, ok, "resolver mode should not require an environment")
}

// The single-database fallback requires an environment to select against.
func TestBuildGRPCTernClientErrorsWhenEnvMissingInFallback(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	config := &api.ServerConfig{
		Databases: map[string]api.DatabaseConfig{
			"orders": {
				Type:         storage.DatabaseTypeMySQL,
				Environments: map[string]api.EnvironmentConfig{"production": {DSN: "root@tcp(localhost:3306)/orders"}},
			},
		},
	}

	_, err := buildGRPCTernClient(t.Context(), config, mysqlstore.New(nil), logger, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TERN_ENVIRONMENT")
}

// A single LocalClient serves exactly one database, so a config where multiple
// databases have a local DSN for the environment is ambiguous and fails closed
// rather than binding to a nondeterministic one.
func TestBuildGRPCTernClientErrorsOnAmbiguousFallback(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	config := &api.ServerConfig{
		Databases: map[string]api.DatabaseConfig{
			"orders": {
				Type:         storage.DatabaseTypeMySQL,
				Environments: map[string]api.EnvironmentConfig{"production": {DSN: "root@tcp(localhost:3306)/orders"}},
			},
			"payments": {
				Type:         storage.DatabaseTypeMySQL,
				Environments: map[string]api.EnvironmentConfig{"production": {DSN: "root@tcp(localhost:3306)/payments"}},
			},
		},
	}

	_, err := buildGRPCTernClient(t.Context(), config, mysqlstore.New(nil), logger, "production")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "orders")
	assert.Contains(t, err.Error(), "payments")
}

// With neither a target resolver nor a database for the environment, startup
// fails closed rather than serving a gRPC endpoint that can resolve nothing.
func TestBuildGRPCTernClientErrorsWhenNothingConfigured(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)

	_, err := buildGRPCTernClient(t.Context(), &api.ServerConfig{}, mysqlstore.New(nil), logger, "production")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "production")
}
