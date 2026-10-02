package etre

import (
	"context"
	"testing"

	"github.com/block/mysql"
	"github.com/square/etre"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/inventory"
)

func newEtreResolverForTest(t *testing.T, gotQuery *string, entities []etre.Entity, cfg EtreResolverConfig) *EtreResolver {
	t.Helper()
	cfg.Client = newClient(mockClient(gotQuery, entities, nil), "cluster", nil)
	if cfg.Credentials == nil {
		cfg.Credentials = inventory.SecretRefCredentialResolver{Username: "ddl", PasswordRef: "pw"}
	}
	if cfg.Assembler == nil {
		cfg.Assembler = inventory.MySQLConnectionAssembler{DefaultPort: "3306"}
	}
	r, err := NewEtreResolver(cfg)
	require.NoError(t, err)
	return r
}

// capturingCredResolver records what the resolver passes to credential
// resolution so tests can assert the surfaced attributes.
type capturingCredResolver struct {
	gotTarget string
	gotAttrs  map[string]string
	creds     *inventory.Credentials
}

func (c *capturingCredResolver) ResolveCredentials(_ context.Context, req inventory.Request, attrs map[string]string) (*inventory.Credentials, error) {
	c.gotTarget = req.Target
	c.gotAttrs = attrs
	return c.creds, nil
}

// fakeAssembler records what the resolver hands the assembler and returns a
// fixed connection, so the resolver's delegation can be asserted in isolation.
type fakeAssembler struct {
	gotHost  string
	gotAttrs map[string]string
	gotCreds *inventory.Credentials
}

func (f *fakeAssembler) DatabaseType() string { return "fake" }

func (f *fakeAssembler) Assemble(host string, attrs map[string]string, creds *inventory.Credentials) (string, map[string]string, error) {
	f.gotHost = host
	f.gotAttrs = attrs
	f.gotCreds = creds
	return "assembled-dsn", map[string]string{"k": "v"}, nil
}

// End to end with the real MySQL assembler: the resolver builds the Etre
// selector, looks up the cluster for its endpoint, and produces a namespace-free
// MySQL DSN from the host and the (separately resolved) credentials.
func TestEtreResolverResolvesMySQLTarget(t *testing.T) {
	var gotQuery string
	entity := etre.Entity{"writer_endpoint": "orders.example"}
	r := newEtreResolverForTest(t, &gotQuery, []etre.Entity{entity}, EtreResolverConfig{
		TargetLabel: "dsid",
		Labels:      map[string]string{"aws_region": "r1"},
		EnvLabel:    "env",
		HostField:   "writer_endpoint",
		Credentials: inventory.SecretRefCredentialResolver{Username: "ddl", PasswordRef: "ddl-secret"},
		Assembler:   inventory.MySQLConnectionAssembler{DefaultPort: "3306", Metadata: map[string]string{"pending_drops": "false"}},
	})

	target, err := r.ResolveTarget(t.Context(), inventory.Request{Target: "orders-dsid", DatabaseType: "mysql", Environment: "production"})
	require.NoError(t, err)

	assert.Equal(t, "aws_region=r1,dsid=orders-dsid,env=production", gotQuery)
	assert.Equal(t, "orders-dsid", target.Target)
	assert.Equal(t, "mysql", target.DatabaseType)
	assert.Equal(t, map[string]string{"pending_drops": "false"}, target.Metadata)

	cfg, err := mysql.ParseDSN(target.DSN)
	require.NoError(t, err)
	assert.Equal(t, "ddl", cfg.User)
	assert.Equal(t, "ddl-secret", cfg.Passwd)
	assert.Equal(t, "orders.example:3306", cfg.Addr)
	assert.Equal(t, "", cfg.DBName, "MySQL DSN must be namespace-free; the request supplies the schema")
}

// End to end with the real Vitess assembler: the resolver looks up the endpoint
// (vtgate host) and the credentials (read-only vtgate user/pass + API token), and
// the assembler produces a namespace-free vtgate DSN for SHOW VITESS_MIGRATIONS
// progress alongside the PlanetScale API metadata.
func TestEtreResolverResolvesVitessTarget(t *testing.T) {
	entity := etre.Entity{"endpoint": "vtgate.example", "organization": "acme", "name": "boardgames_main"}
	creds := &capturingCredResolver{creds: &inventory.Credentials{
		Username: "vt-user",
		Password: "vt-pass",
		Metadata: map[string]string{
			inventory.MetadataTokenName:  "tok-id",
			inventory.MetadataTokenValue: "tok-secret",
		},
	}}
	r := newEtreResolverForTest(t, nil, []etre.Entity{entity}, EtreResolverConfig{
		TargetLabel:     "dsid",
		HostField:       "endpoint",
		AttributeFields: []string{"organization", "name"},
		Credentials:     creds,
		Assembler:       inventory.VitessConnectionAssembler{DefaultPort: "3306"},
	})

	target, err := r.ResolveTarget(t.Context(), inventory.Request{Target: "boardgames-dsid", DatabaseType: "vitess"})
	require.NoError(t, err)

	assert.Equal(t, "vitess", target.DatabaseType)
	assert.Equal(t, "acme", target.Metadata[inventory.MetadataOrganization])
	assert.Equal(t, "boardgames_main", target.Metadata[inventory.MetadataDatabase])
	assert.Equal(t, "tok-secret", target.Metadata[inventory.MetadataTokenValue])

	cfg, err := mysql.ParseDSN(target.DSN)
	require.NoError(t, err)
	assert.Equal(t, "vt-user", cfg.User)
	assert.Equal(t, "vt-pass", cfg.Passwd)
	assert.Equal(t, "vtgate.example:3306", cfg.Addr)
	assert.Equal(t, "", cfg.DBName, "vtgate DSN is namespace-free; the schema is supplied per operation")
}

// The resolver surfaces the endpoint host and configured attributes to both the
// credential resolver and the assembler, and returns the assembler's output.
func TestEtreResolverDelegatesToAssembler(t *testing.T) {
	entity := etre.Entity{"writer_endpoint": "orders.example:3306", "aws_account_id": "123456789012", "name": "orders-001"}
	asm := &fakeAssembler{}
	creds := &capturingCredResolver{creds: &inventory.Credentials{Username: "spirit", Password: "pw"}}
	r := newEtreResolverForTest(t, nil, []etre.Entity{entity}, EtreResolverConfig{
		TargetLabel:     "dsid",
		HostField:       "writer_endpoint",
		AttributeFields: []string{"aws_account_id", "name"},
		Credentials:     creds,
		Assembler:       asm,
	})

	target, err := r.ResolveTarget(t.Context(), inventory.Request{Target: "orders-dsid"})
	require.NoError(t, err)

	wantAttrs := map[string]string{"aws_account_id": "123456789012", "name": "orders-001"}
	assert.Equal(t, "orders-dsid", creds.gotTarget)
	assert.Equal(t, wantAttrs, creds.gotAttrs)
	assert.Equal(t, "orders.example:3306", asm.gotHost)
	assert.Equal(t, wantAttrs, asm.gotAttrs)
	assert.Equal(t, "spirit", asm.gotCreds.Username)

	assert.Equal(t, "fake", target.DatabaseType)
	assert.Equal(t, "assembled-dsn", target.DSN)
	assert.Equal(t, map[string]string{"k": "v"}, target.Metadata)
}

func TestEtreResolverOmitsEnvLabelWhenUnset(t *testing.T) {
	var gotQuery string
	entity := etre.Entity{"writer_endpoint": "orders.example:3306"}
	r := newEtreResolverForTest(t, &gotQuery, []etre.Entity{entity}, EtreResolverConfig{
		TargetLabel: "dsid",
		HostField:   "writer_endpoint",
	})

	_, err := r.ResolveTarget(t.Context(), inventory.Request{Target: "orders-dsid", Environment: "production"})
	require.NoError(t, err)
	assert.Equal(t, "dsid=orders-dsid", gotQuery)
}

// A not-found lookup fails closed and surfaces the sentinel so a multi-type
// resolver could fall back.
func TestEtreResolverFailsClosedOnNotFound(t *testing.T) {
	r := newEtreResolverForTest(t, nil, nil, EtreResolverConfig{
		TargetLabel: "dsid",
		HostField:   "writer_endpoint",
	})

	_, err := r.ResolveTarget(t.Context(), inventory.Request{Target: "missing"})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNotFound)
}

// A request whose type does not match the engine the resolver serves fails
// closed rather than returning a connection for the wrong engine.
func TestEtreResolverRejectsDatabaseTypeMismatch(t *testing.T) {
	entity := etre.Entity{"writer_endpoint": "orders.example:3306"}
	r := newEtreResolverForTest(t, nil, []etre.Entity{entity}, EtreResolverConfig{
		TargetLabel: "dsid",
		HostField:   "writer_endpoint",
	})

	_, err := r.ResolveTarget(t.Context(), inventory.Request{Target: "orders-dsid", DatabaseType: "vitess"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "vitess")
	assert.Contains(t, err.Error(), "mysql")
}

// With an env label configured, a request without an environment fails closed
// rather than running an environment-unscoped lookup.
func TestEtreResolverRequiresEnvironmentWhenEnvLabelSet(t *testing.T) {
	entity := etre.Entity{"writer_endpoint": "orders.example:3306"}
	r := newEtreResolverForTest(t, nil, []etre.Entity{entity}, EtreResolverConfig{
		TargetLabel: "dsid",
		EnvLabel:    "env",
		HostField:   "writer_endpoint",
	})

	_, err := r.ResolveTarget(t.Context(), inventory.Request{Target: "orders-dsid"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "environment")
}

// A fixed label that collides with the target or env label is a configuration
// error, since it could silently override the predicate that selects the entity.
func TestNewEtreResolverRejectsCollidingLabels(t *testing.T) {
	client := newClient(mockClient(nil, nil, nil), "cluster", nil)
	base := EtreResolverConfig{
		Client:      client,
		TargetLabel: "dsid",
		EnvLabel:    "env",
		HostField:   "writer_endpoint",
		Credentials: inventory.SecretRefCredentialResolver{Username: "ddl", PasswordRef: "pw"},
		Assembler:   inventory.MySQLConnectionAssembler{},
	}

	targetCollision := base
	targetCollision.Labels = map[string]string{"dsid": "x"}
	_, err := NewEtreResolver(targetCollision)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "target label")

	envCollision := base
	envCollision.Labels = map[string]string{"env": "production"}
	_, err = NewEtreResolver(envCollision)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "env label")
}

func TestEtreResolverRequiresTarget(t *testing.T) {
	r := newEtreResolverForTest(t, nil, nil, EtreResolverConfig{
		TargetLabel: "dsid",
		HostField:   "writer_endpoint",
	})

	_, err := r.ResolveTarget(t.Context(), inventory.Request{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "target is required")
}

func TestNewEtreResolverValidatesConfig(t *testing.T) {
	client := newClient(mockClient(nil, nil, nil), "cluster", nil)
	base := EtreResolverConfig{
		Client:      client,
		TargetLabel: "dsid",
		HostField:   "writer_endpoint",
		Credentials: inventory.SecretRefCredentialResolver{Username: "ddl", PasswordRef: "pw"},
		Assembler:   inventory.MySQLConnectionAssembler{},
	}

	_, err := NewEtreResolver(base)
	require.NoError(t, err)

	cases := map[string]func(*EtreResolverConfig){
		"etre client is required":          func(c *EtreResolverConfig) { c.Client = nil },
		"target label is required":         func(c *EtreResolverConfig) { c.TargetLabel = "" },
		"credential resolver is required":  func(c *EtreResolverConfig) { c.Credentials = nil },
		"connection assembler is required": func(c *EtreResolverConfig) { c.Assembler = nil },
	}
	for wantErr, mutate := range cases {
		cfg := base
		mutate(&cfg)
		_, err := NewEtreResolver(cfg)
		require.Error(t, err, wantErr)
		assert.Contains(t, err.Error(), wantErr)
	}
}

// A sharded database holds the same schema under a different name on each
// shard. Each shard's target carries its own mapping, so the shard's resolved
// connection names the shard's physical schema while every request still uses
// the canonical namespace. A target with no mapping keeps the default, where
// the requested namespace is the physical schema.
func TestEtreResolverAttachesEachTargetsSchemaOverrides(t *testing.T) {
	entity := etre.Entity{"writer_endpoint": "consentsys.example"}
	overrides := map[string]map[string]string{
		"consentsys-002": {"consentsys": "consentsys_0_production"},
		"consentsys-003": {"consentsys": "consentsys_1_production"},
	}
	r := newEtreResolverForTest(t, nil, []etre.Entity{entity}, EtreResolverConfig{
		TargetLabel:     "dsid",
		HostField:       "writer_endpoint",
		SchemaOverrides: overrides,
	})
	// The resolver owns a copy, so a later edit to the caller's config cannot
	// change where a target's namespace lands.
	overrides["consentsys-002"]["consentsys"] = "consentsys_9_production"

	shard0, err := r.ResolveTarget(t.Context(), inventory.Request{Target: "consentsys-002", DatabaseType: "mysql"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"consentsys": "consentsys_0_production"}, shard0.SchemaOverrides)

	shard1, err := r.ResolveTarget(t.Context(), inventory.Request{Target: "consentsys-003", DatabaseType: "mysql"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"consentsys": "consentsys_1_production"}, shard1.SchemaOverrides)

	unmapped, err := r.ResolveTarget(t.Context(), inventory.Request{Target: "orders-001", DatabaseType: "mysql"})
	require.NoError(t, err)
	assert.Empty(t, unmapped.SchemaOverrides)

	cfg, err := mysql.ParseDSN(shard0.DSN)
	require.NoError(t, err)
	assert.Equal(t, "", cfg.DBName, "the mapping is injected per operation; the DSN stays namespace-free")

	// Each resolution hands out its own map, so a consumer cannot rewrite the
	// mapping the next request for the same target receives.
	shard0.SchemaOverrides["consentsys"] = "consentsys_9_production"
	again, err := r.ResolveTarget(t.Context(), inventory.Request{Target: "consentsys-002", DatabaseType: "mysql"})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"consentsys": "consentsys_0_production"}, again.SchemaOverrides)
}

// A malformed mapping fails at construction, so the data plane refuses to
// start rather than failing the first request for that target.
func TestNewEtreResolverRejectsInvalidSchemaOverrides(t *testing.T) {
	client := newClient(mockClient(nil, nil, nil), "cluster", nil)
	base := EtreResolverConfig{
		Client:      client,
		TargetLabel: "dsid",
		HostField:   "writer_endpoint",
		Credentials: inventory.SecretRefCredentialResolver{Username: "ddl", PasswordRef: "pw"},
		Assembler:   inventory.MySQLConnectionAssembler{},
	}

	cases := []struct {
		name      string
		assembler inventory.ConnectionAssembler
		overrides map[string]map[string]string
		wantErr   string
	}{
		{
			name:      "empty target",
			overrides: map[string]map[string]string{" ": {"consentsys": "consentsys_0_production"}},
			wantErr:   "target must not be empty",
		},
		{
			name:      "empty mapping",
			overrides: map[string]map[string]string{"consentsys-002": {}},
			wantErr:   `schema overrides for target "consentsys-002": at least one mapping is required`,
		},
		{
			name:      "two mappings",
			overrides: map[string]map[string]string{"consentsys-002": {"consentsys": "consentsys_0_production", "audit": "audit_0"}},
			wantErr:   "exactly one mapping",
		},
		{
			name:      "unsafe identifier",
			overrides: map[string]map[string]string{"consentsys-002": {"consentsys": "consentsys-0"}},
			wantErr:   `schema_overrides value "consentsys-0"`,
		},
		{
			name:      "engine without schema overrides",
			assembler: inventory.MySQLConnectionAssembler{Type: "strata"},
			overrides: map[string]map[string]string{"consentsys-002": {"consentsys": "consentsys_0_production"}},
			wantErr:   `only supported for mysql and postgres, not "strata"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			if tc.assembler != nil {
				cfg.Assembler = tc.assembler
			}
			cfg.SchemaOverrides = tc.overrides
			_, err := NewEtreResolver(cfg)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}
