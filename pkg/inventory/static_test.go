package inventory

import (
	"os"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStaticResolverResolveTarget(t *testing.T) {
	t.Setenv("TARGET_DSN", "user:pass@tcp(db.example:3306)/")
	resolver, err := NewStaticResolver(StaticConfig{Targets: map[string]StaticTarget{
		"dsid-orders-prod": {
			DatabaseType: "MySQL",
			DSN:          "env:TARGET_DSN",
			Metadata:     map[string]string{"pending_drops": "false"},
		},
	}})
	require.NoError(t, err)

	got, err := resolver.ResolveTarget(t.Context(), Request{
		Target:       "dsid-orders-prod",
		DatabaseType: "MYSQL",
		Environment:  "production",
	})

	require.NoError(t, err)
	assert.Equal(t, "dsid-orders-prod", got.Target)
	assert.Equal(t, "mysql", got.DatabaseType)
	assert.Equal(t, "user:pass@tcp(db.example:3306)/", got.DSN)
	assert.Equal(t, map[string]string{"pending_drops": "false"}, got.Metadata)
}

func TestStaticResolverResolvesDSNAtConstruction(t *testing.T) {
	t.Setenv("TARGET_DSN", "user:pass@tcp(db.example:3306)/")
	resolver, err := NewStaticResolver(StaticConfig{Targets: map[string]StaticTarget{
		"target-1": {
			DatabaseType: "mysql",
			DSN:          "env:TARGET_DSN",
		},
	}})
	require.NoError(t, err)
	t.Setenv("TARGET_DSN", "other:pass@tcp(db.example:3306)/")

	got, err := resolver.ResolveTarget(t.Context(), Request{Target: "target-1"})

	require.NoError(t, err)
	assert.Equal(t, "user:pass@tcp(db.example:3306)/", got.DSN)
}

func TestStaticResolverResolveTargetClonesMetadata(t *testing.T) {
	resolver, err := NewStaticResolver(StaticConfig{Targets: map[string]StaticTarget{
		"target-1": {
			DatabaseType: "mysql",
			DSN:          "root@tcp(localhost:3306)/",
			Metadata:     map[string]string{"source": "static"},
		},
	}})
	require.NoError(t, err)

	first, err := resolver.ResolveTarget(t.Context(), Request{Target: "target-1"})
	require.NoError(t, err)
	first.Metadata["source"] = "mutated"

	second, err := resolver.ResolveTarget(t.Context(), Request{Target: "target-1"})
	require.NoError(t, err)
	assert.Equal(t, "static", second.Metadata["source"])
}

func TestStaticResolverResolveTargetFromFile(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "dsn-*")
	require.NoError(t, err)
	_, err = file.WriteString("root@tcp(localhost:3306)/\n")
	require.NoError(t, err)
	require.NoError(t, file.Close())

	resolver, err := NewStaticResolver(StaticConfig{Targets: map[string]StaticTarget{
		"target-1": {
			DatabaseType: "mysql",
			DSN:          "file:" + file.Name(),
		},
	}})
	require.NoError(t, err)

	got, err := resolver.ResolveTarget(t.Context(), Request{Target: "target-1"})

	require.NoError(t, err)
	assert.Equal(t, "root@tcp(localhost:3306)/", got.DSN)
}

func TestStaticResolverResolveTargetValidatesRequest(t *testing.T) {
	resolver, err := NewStaticResolver(StaticConfig{Targets: map[string]StaticTarget{
		"target-1": {
			DatabaseType: "mysql",
			DSN:          "root@tcp(localhost:3306)/",
		},
	}})
	require.NoError(t, err)

	_, err = resolver.ResolveTarget(t.Context(), Request{})
	assert.ErrorContains(t, err, "target is required")

	_, err = resolver.ResolveTarget(t.Context(), Request{Target: "missing"})
	assert.ErrorContains(t, err, "target \"missing\" is not configured")

	_, err = resolver.ResolveTarget(t.Context(), Request{Target: "target-1", DatabaseType: "vitess"})
	assert.ErrorContains(t, err, "target \"target-1\" is configured for database type \"mysql\", not \"vitess\"")
}

func TestStaticResolverResolveTargetRejectsMySQLDSNWithDatabase(t *testing.T) {
	_, err := NewStaticResolver(StaticConfig{Targets: map[string]StaticTarget{
		"target-1": {
			DatabaseType: "mysql",
			DSN:          "root@tcp(localhost:3306)/appdb",
		},
	}})

	assert.ErrorContains(t, err, "MySQL DSN must not include a database name")
}

func TestStaticResolverResolveTargetDSNFrom(t *testing.T) {
	t.Setenv("TARGET_CONFIG", `{"host":"db.example","port":3306,"database":"appdb"}`)
	t.Setenv("TARGET_PASSWORD", "s3cret")
	resolver, err := NewStaticResolver(StaticConfig{Targets: map[string]StaticTarget{
		"apse2-prod": {
			DatabaseType: "mysql",
			DSNFrom: &StaticDSNFromConfig{
				ConfigRef:   "env:TARGET_CONFIG",
				Username:    "m_spirit",
				PasswordRef: "env:TARGET_PASSWORD",
			},
			Metadata: map[string]string{"pending_drops": "false"},
		},
	}})
	require.NoError(t, err)

	got, err := resolver.ResolveTarget(t.Context(), Request{Target: "apse2-prod", DatabaseType: "MYSQL"})
	require.NoError(t, err)

	assert.Equal(t, "apse2-prod", got.Target)
	assert.Equal(t, "mysql", got.DatabaseType)
	assert.Equal(t, map[string]string{"pending_drops": "false"}, got.Metadata)

	cfg, err := mysql.ParseDSN(got.DSN)
	require.NoError(t, err)
	assert.Equal(t, "db.example:3306", cfg.Addr)
	assert.Equal(t, "m_spirit", cfg.User)
	assert.Equal(t, "s3cret", cfg.Passwd)
	assert.Empty(t, cfg.DBName, "static target DSN must be namespace-free")
}

func TestStaticResolverDSNFromDefaultsPort(t *testing.T) {
	t.Setenv("TARGET_CONFIG", `{"host":"db.example"}`)
	t.Setenv("TARGET_PASSWORD", "s3cret")
	resolver, err := NewStaticResolver(StaticConfig{Targets: map[string]StaticTarget{
		"target-1": {
			DatabaseType: "mysql",
			DSNFrom: &StaticDSNFromConfig{
				ConfigRef:   "env:TARGET_CONFIG",
				Username:    "m_spirit",
				PasswordRef: "env:TARGET_PASSWORD",
			},
		},
	}})
	require.NoError(t, err)

	got, err := resolver.ResolveTarget(t.Context(), Request{Target: "target-1"})
	require.NoError(t, err)

	cfg, err := mysql.ParseDSN(got.DSN)
	require.NoError(t, err)
	assert.Equal(t, "db.example:3306", cfg.Addr)
}

// A dsn_from target resolves its credentials fresh on every request, so a
// rotated password is picked up without rebuilding the resolver.
func TestStaticResolverDSNFromResolvesPerRequest(t *testing.T) {
	t.Setenv("TARGET_CONFIG", `{"host":"db.example","port":3306}`)
	t.Setenv("TARGET_PASSWORD", "old-pass")
	resolver, err := NewStaticResolver(StaticConfig{Targets: map[string]StaticTarget{
		"target-1": {
			DatabaseType: "mysql",
			DSNFrom: &StaticDSNFromConfig{
				ConfigRef:   "env:TARGET_CONFIG",
				Username:    "m_spirit",
				PasswordRef: "env:TARGET_PASSWORD",
			},
		},
	}})
	require.NoError(t, err)

	first, err := resolver.ResolveTarget(t.Context(), Request{Target: "target-1"})
	require.NoError(t, err)
	firstCfg, err := mysql.ParseDSN(first.DSN)
	require.NoError(t, err)
	assert.Equal(t, "old-pass", firstCfg.Passwd)

	t.Setenv("TARGET_PASSWORD", "new-pass")
	second, err := resolver.ResolveTarget(t.Context(), Request{Target: "target-1"})
	require.NoError(t, err)
	secondCfg, err := mysql.ParseDSN(second.DSN)
	require.NoError(t, err)
	assert.Equal(t, "new-pass", secondCfg.Passwd)
}

// Both config_ref and password_ref support the "{target}" placeholder, so a
// single target entry can name its per-target secrets by the request target.
func TestStaticResolverDSNFromSubstitutesTargetPlaceholder(t *testing.T) {
	t.Setenv("CONFIG_apse2-prod", `{"host":"db.example","port":3306}`)
	t.Setenv("PASSWORD_apse2-prod", "s3cret")
	resolver, err := NewStaticResolver(StaticConfig{Targets: map[string]StaticTarget{
		"apse2-prod": {
			DatabaseType: "mysql",
			DSNFrom: &StaticDSNFromConfig{
				ConfigRef:   "env:CONFIG_{target}",
				Username:    "m_spirit",
				PasswordRef: "env:PASSWORD_{target}",
			},
		},
	}})
	require.NoError(t, err)

	got, err := resolver.ResolveTarget(t.Context(), Request{Target: "apse2-prod"})
	require.NoError(t, err)

	cfg, err := mysql.ParseDSN(got.DSN)
	require.NoError(t, err)
	assert.Equal(t, "db.example:3306", cfg.Addr)
	assert.Equal(t, "s3cret", cfg.Passwd)
}

// A host value with surrounding whitespace is trimmed so it can't produce a DSN
// that only fails at dial time.
func TestStaticResolverDSNFromTrimsHost(t *testing.T) {
	t.Setenv("TARGET_CONFIG", `{"host":" db.example ","port":3306}`)
	t.Setenv("TARGET_PASSWORD", "s3cret")
	resolver, err := NewStaticResolver(StaticConfig{Targets: map[string]StaticTarget{
		"target-1": {
			DatabaseType: "mysql",
			DSNFrom: &StaticDSNFromConfig{
				ConfigRef:   "env:TARGET_CONFIG",
				Username:    "m_spirit",
				PasswordRef: "env:TARGET_PASSWORD",
			},
		},
	}})
	require.NoError(t, err)

	got, err := resolver.ResolveTarget(t.Context(), Request{Target: "target-1"})
	require.NoError(t, err)

	cfg, err := mysql.ParseDSN(got.DSN)
	require.NoError(t, err)
	assert.Equal(t, "db.example:3306", cfg.Addr)
}

func TestNewStaticResolverValidatesDSNFromConfig(t *testing.T) {
	tests := []struct {
		name    string
		target  StaticTarget
		wantErr string
	}{
		{
			name: "both dsn and dsn_from",
			target: StaticTarget{
				DatabaseType: "mysql",
				DSN:          "root@tcp(localhost:3306)/",
				DSNFrom:      &StaticDSNFromConfig{ConfigRef: "env:X", Username: "u", PasswordRef: "env:P"},
			},
			wantErr: "cannot configure both dsn and dsn_from",
		},
		{
			name:    "dsn_from missing config_ref",
			target:  StaticTarget{DatabaseType: "mysql", DSNFrom: &StaticDSNFromConfig{Username: "u", PasswordRef: "env:P"}},
			wantErr: "dsn_from missing config_ref",
		},
		{
			name:    "dsn_from missing username",
			target:  StaticTarget{DatabaseType: "mysql", DSNFrom: &StaticDSNFromConfig{ConfigRef: "env:X", PasswordRef: "env:P"}},
			wantErr: "dsn_from missing username",
		},
		{
			name:    "dsn_from missing password_ref",
			target:  StaticTarget{DatabaseType: "mysql", DSNFrom: &StaticDSNFromConfig{ConfigRef: "env:X", Username: "u"}},
			wantErr: "dsn_from missing password_ref",
		},
		{
			name:    "dsn_from non-mysql type",
			target:  StaticTarget{DatabaseType: "vitess", DSNFrom: &StaticDSNFromConfig{ConfigRef: "env:X", Username: "u", PasswordRef: "env:P"}},
			wantErr: "dsn_from is only supported for mysql",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewStaticResolver(StaticConfig{Targets: map[string]StaticTarget{"target-1": tt.target}})
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestStaticResolverDSNFromFailsClosed(t *testing.T) {
	newResolver := func(t *testing.T) *StaticResolver {
		t.Helper()
		resolver, err := NewStaticResolver(StaticConfig{Targets: map[string]StaticTarget{
			"target-1": {
				DatabaseType: "mysql",
				DSNFrom: &StaticDSNFromConfig{
					ConfigRef:   "env:TARGET_CONFIG",
					Username:    "m_spirit",
					PasswordRef: "env:TARGET_PASSWORD",
				},
			},
		}})
		require.NoError(t, err)
		return resolver
	}

	t.Run("empty password", func(t *testing.T) {
		t.Setenv("TARGET_CONFIG", `{"host":"db.example","port":3306}`)
		t.Setenv("TARGET_PASSWORD", "")
		_, err := newResolver(t).ResolveTarget(t.Context(), Request{Target: "target-1"})
		assert.ErrorContains(t, err, "resolved to an empty value")
	})

	t.Run("missing host", func(t *testing.T) {
		t.Setenv("TARGET_CONFIG", `{"port":3306}`)
		t.Setenv("TARGET_PASSWORD", "s3cret")
		_, err := newResolver(t).ResolveTarget(t.Context(), Request{Target: "target-1"})
		assert.ErrorContains(t, err, `missing key "host"`)
	})

	t.Run("empty config document", func(t *testing.T) {
		t.Setenv("TARGET_CONFIG", "")
		t.Setenv("TARGET_PASSWORD", "s3cret")
		_, err := newResolver(t).ResolveTarget(t.Context(), Request{Target: "target-1"})
		assert.ErrorContains(t, err, "resolved an empty value")
	})

	t.Run("non-integer port", func(t *testing.T) {
		t.Setenv("TARGET_CONFIG", `{"host":"db.example","port":true}`)
		t.Setenv("TARGET_PASSWORD", "s3cret")
		_, err := newResolver(t).ResolveTarget(t.Context(), Request{Target: "target-1"})
		assert.ErrorContains(t, err, "must be a string or number")
	})

	t.Run("non-numeric string port", func(t *testing.T) {
		t.Setenv("TARGET_CONFIG", `{"host":"db.example","port":"not-a-port"}`)
		t.Setenv("TARGET_PASSWORD", "s3cret")
		_, err := newResolver(t).ResolveTarget(t.Context(), Request{Target: "target-1"})
		assert.ErrorContains(t, err, "must be an integer port")
	})

	t.Run("out-of-range port", func(t *testing.T) {
		t.Setenv("TARGET_CONFIG", `{"host":"db.example","port":70000}`)
		t.Setenv("TARGET_PASSWORD", "s3cret")
		_, err := newResolver(t).ResolveTarget(t.Context(), Request{Target: "target-1"})
		assert.ErrorContains(t, err, "between 1 and 65535")
	})

	t.Run("whitespace string port is trimmed", func(t *testing.T) {
		t.Setenv("TARGET_CONFIG", `{"host":"db.example","port":" 3307 "}`)
		t.Setenv("TARGET_PASSWORD", "s3cret")
		got, err := newResolver(t).ResolveTarget(t.Context(), Request{Target: "target-1"})
		require.NoError(t, err)
		cfg, err := mysql.ParseDSN(got.DSN)
		require.NoError(t, err)
		assert.Equal(t, "db.example:3307", cfg.Addr)
	})
}

func TestNewStaticResolverValidatesConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  StaticConfig
		wantErr string
	}{
		{
			name:    "empty targets",
			config:  StaticConfig{},
			wantErr: "static target resolver requires at least one target",
		},
		{
			name:    "empty target key",
			config:  StaticConfig{Targets: map[string]StaticTarget{"": {DatabaseType: "mysql", DSN: "dsn"}}},
			wantErr: "static target resolver contains an empty target key",
		},
		{
			name:    "missing type",
			config:  StaticConfig{Targets: map[string]StaticTarget{"target-1": {DSN: "dsn"}}},
			wantErr: "target \"target-1\" missing type",
		},
		{
			name:    "missing dsn",
			config:  StaticConfig{Targets: map[string]StaticTarget{"target-1": {DatabaseType: "mysql"}}},
			wantErr: "target \"target-1\" missing dsn or dsn_from",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewStaticResolver(tt.config)
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}
