package api

import (
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
			require.False(t, cfg.ExperimentalStrataEnabled)
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
		var cfg ServerConfig
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
