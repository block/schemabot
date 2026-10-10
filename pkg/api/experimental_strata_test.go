package api

import (
	"strings"
	"testing"

	"github.com/block/schemabot/pkg/inventory"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestExperimentalStrataRegistrations(t *testing.T) {
	cases := map[string]*ServerConfig{
		"database": {Databases: map[string]DatabaseConfig{"example": {Type: storage.DatabaseTypeStrata}}},
		"target":   {TargetResolver: TargetResolverConfig{Targets: map[string]inventory.StaticTarget{"example": {DatabaseType: storage.DatabaseTypeStrata}}}},
		"resolver": {TargetResolver: TargetResolverConfig{Etre: []EtreConfig{{DatabaseType: storage.DatabaseTypeStrata}}}},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			require.ErrorContains(t, cfg.Validate(), "experimental-strata-enabled: true")
			cfg.ExperimentalStrataEnabled = true
			require.NoError(t, cfg.ValidateExperimentalStrata())
		})
	}
	require.NoError(t, (&ServerConfig{Databases: map[string]DatabaseConfig{"example": {Type: storage.DatabaseTypeMySQL}}}).ValidateExperimentalStrata())
}

func TestExperimentalStrataServerSetting(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		raw := "experimental-strata-enabled: false\n"
		if enabled {
			raw = "experimental-strata-enabled: true\n"
		}
		cfg := ServerConfig{ExperimentalStrataEnabled: !enabled}
		require.NoError(t, yaml.Unmarshal([]byte(raw), &cfg))
		require.Equal(t, enabled, cfg.ExperimentalStrataEnabled)
	}
}

func TestExperimentalStrataParseServerConfig(t *testing.T) {
	const config = `databases:
  example:
    type: strata
    environments:
      staging:
        dsn: "user:password@tcp(localhost:3306)/example"
`
	_, err := ParseServerConfig([]byte(config))
	require.ErrorContains(t, err, "experimental-strata-enabled: true")
	cfg, err := ParseServerConfig([]byte("experimental-strata-enabled: true\n" + config))
	require.NoError(t, err)
	require.True(t, cfg.ExperimentalStrataEnabled)
}

// Canonical spelling variants must require opt-in before any resolver runs.
func TestExperimentalStrataCanonicalTypes(t *testing.T) {
	for _, spelling := range []string{"strata", "Strata", "STRATA", " strata "} {
		for _, kind := range []string{"database", "target", "resolver"} {
			t.Run(kind+"/"+spelling, func(t *testing.T) {
				cfg := &ServerConfig{}
				switch kind {
				case "database":
					cfg.Databases = map[string]DatabaseConfig{"example": {Type: spelling}}
				case "target":
					cfg.TargetResolver.Targets = map[string]inventory.StaticTarget{"example": {DatabaseType: spelling}}
				case "resolver":
					cfg.TargetResolver.Etre = []EtreConfig{{DatabaseType: spelling}}
				}
				require.ErrorContains(t, cfg.ValidateExperimentalStrata(), "experimental-strata-enabled: true")
				cfg.ExperimentalStrataEnabled = true
				require.NoError(t, cfg.ValidateExperimentalStrata())
			})
		}
	}
}

// Report the opt-in requirement before unrelated configuration errors.
func TestExperimentalStrataValidationPrecedence(t *testing.T) {
	cfg := &ServerConfig{EnvironmentOrder: []string{"invalid environment"}, Databases: map[string]DatabaseConfig{"example": {Type: "strata"}}}
	require.ErrorContains(t, cfg.Validate(), "experimental-strata-enabled: true")
	cfg.ExperimentalStrataEnabled = true
	require.ErrorContains(t, cfg.Validate(), "environment")
}

// Invalid registrations list only the types available under the server setting.
func TestExperimentalStrataInvalidTypeGuidance(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		cfg := &ServerConfig{ExperimentalStrataEnabled: enabled, Databases: map[string]DatabaseConfig{"example": {Type: "sqlite"}}}
		err := cfg.Validate()
		require.ErrorContains(t, err, "mysql, postgres")
		require.Equal(t, enabled, strings.Contains(err.Error(), "strata (experimental)"))
	}
}

// Resolver errors identify the entry that needs the server opt-in.
func TestExperimentalStrataResolverErrorLocation(t *testing.T) {
	cfg := &ServerConfig{TargetResolver: TargetResolverConfig{Etre: []EtreConfig{
		{DatabaseType: "mysql"}, {DatabaseType: "strata"},
	}}}
	require.EqualError(t, cfg.ValidateExperimentalStrata(), "target_resolver.etre[1]: Strata is experimental; set experimental-strata-enabled: true in the server configuration to enable it")
}
