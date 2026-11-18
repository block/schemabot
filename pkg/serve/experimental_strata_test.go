package serve

import (
	"testing"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestBuildRejectsExperimentalStrataBeforeStartup(t *testing.T) {
	cfg := &api.ServerConfig{Databases: map[string]api.DatabaseConfig{
		"example": {Type: storage.DatabaseTypeStrata},
	}}
	server, err := Build(t.Context(), cfg)
	require.Nil(t, server)
	require.ErrorContains(t, err, "experimental-strata-enabled: true")
}
