package api

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateDatabaseApp(t *testing.T) {
	valid := []string{
		"billing",
		"billing-service",
		"app2",
		"a",
		"7tenants",
		strings.Repeat("a", maxAppNameChars),
	}
	for _, app := range valid {
		t.Run("valid "+app, func(t *testing.T) {
			assert.NoError(t, validateDatabaseApp("db1", DatabaseConfig{App: app}))
		})
	}

	t.Run("empty is allowed", func(t *testing.T) {
		assert.NoError(t, validateDatabaseApp("db1", DatabaseConfig{}))
	})

	invalid := []struct {
		name string
		app  string
		want string
	}{
		{name: "uppercase", app: "Billing", want: "lowercase alphanumeric"},
		{name: "underscore", app: "billing_service", want: "lowercase alphanumeric"},
		{name: "space", app: "billing service", want: "lowercase alphanumeric"},
		{name: "unicode", app: "billíng", want: "lowercase alphanumeric"},
		{name: "leading hyphen", app: "-billing", want: "start and end with a letter or digit"},
		{name: "trailing hyphen", app: "billing-", want: "start and end with a letter or digit"},
		{name: "consecutive hyphens", app: "billing--service", want: "consecutive hyphens"},
		{name: "too long", app: strings.Repeat("a", maxAppNameChars+1), want: "exceeds"},
		{name: "multibyte over the length cap reports the charset violation", app: strings.Repeat("í", maxAppNameChars+1), want: "lowercase alphanumeric"},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			err := validateDatabaseApp("db1", DatabaseConfig{App: tc.app})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "databases.db1.app")
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

// TestServerConfig_Validate_App verifies the app field is validated as part of
// full config validation, so a malformed app identifier fails config load
// instead of surfacing as an unknown app at command time.
func TestServerConfig_Validate_App(t *testing.T) {
	cfg := ServerConfig{
		Databases: map[string]DatabaseConfig{
			"tenants-shard-01": {
				Type: "mysql",
				App:  "Tenants",
				Environments: map[string]EnvironmentConfig{
					"staging": {DSN: "root@tcp(localhost)/tenants_01"},
				},
			},
		},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "databases.tenants-shard-01.app")

	cfg.Databases["tenants-shard-01"] = DatabaseConfig{
		Type: "mysql",
		App:  "tenants",
		Environments: map[string]EnvironmentConfig{
			"staging": {DSN: "root@tcp(localhost)/tenants_01"},
		},
	}
	require.NoError(t, cfg.Validate())
}

func TestServerConfig_DatabasesForApp(t *testing.T) {
	cfg := ServerConfig{
		Databases: map[string]DatabaseConfig{
			"tenants-shard-02": {Type: "mysql", App: "tenants"},
			"tenants-shard-01": {Type: "mysql", App: "tenants"},
			"tenants-shard-03": {Type: "mysql", App: "tenants"},
			"ledger":           {Type: "mysql", App: "billing"},
			"standalone":       {Type: "mysql"},
		},
	}

	t.Run("returns every match sorted by name", func(t *testing.T) {
		got, err := cfg.DatabasesForApp("tenants")
		require.NoError(t, err)
		assert.Equal(t, []string{"tenants-shard-01", "tenants-shard-02", "tenants-shard-03"}, got)
	})

	t.Run("single match", func(t *testing.T) {
		got, err := cfg.DatabasesForApp("billing")
		require.NoError(t, err)
		assert.Equal(t, []string{"ledger"}, got)
	})

	t.Run("unknown app fails closed", func(t *testing.T) {
		_, err := cfg.DatabasesForApp("unknown-app")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `app "unknown-app" has no configured databases`)
	})

	t.Run("empty app never matches untagged databases", func(t *testing.T) {
		_, err := cfg.DatabasesForApp("")
		require.Error(t, err)
	})

	t.Run("nil config fails closed", func(t *testing.T) {
		var nilConfig *ServerConfig
		_, err := nilConfig.DatabasesForApp("tenants")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "server config not loaded")
	})
}
