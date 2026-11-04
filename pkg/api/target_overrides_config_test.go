package api

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestTargetOverridesStrictDecoding(t *testing.T) {
	const config = `target_resolver:
  overrides:
    - environment: production
      target: cluster-002
      type: mysql
      schema_overrides:
        orders: orders_sandbox
`
	for _, text := range []string{strings.Replace(config, "      type:", "      typo:", 1), strings.Replace(config, "      schema_overrides:", "      schema_override:", 1)} {
		var cfg ServerConfig
		decoder := yaml.NewDecoder(strings.NewReader(text))
		decoder.KnownFields(true)
		require.Error(t, decoder.Decode(&cfg))
	}
	var cfg ServerConfig
	decoder := yaml.NewDecoder(strings.NewReader(config))
	decoder.KnownFields(true)
	require.NoError(t, decoder.Decode(&cfg))
	require.ErrorContains(t, cfg.Validate(), "requires an etre or static backend")
}
