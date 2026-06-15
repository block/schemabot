package inventory

import (
	"os"
	"testing"

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
			wantErr: "target \"target-1\" missing dsn",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewStaticResolver(tt.config)
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}
