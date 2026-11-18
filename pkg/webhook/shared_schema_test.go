package webhook

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ghclient "github.com/block/schemabot/pkg/github"
)

// A peer is skipped as configured elsewhere only when the search proved its
// config is not in this repository. A miss that skipped configs it could not
// read, or any other failure, fails the environment closed.
func TestPeerConfigOutsideRepositoryOnlyForAConclusiveMiss(t *testing.T) {
	for name, tc := range map[string]struct {
		err     error
		outside bool
	}{
		"no config in the repository": {err: fmt.Errorf("find config: %w", ghclient.ErrNoConfig), outside: true},
		"every config read":           {err: &ghclient.DatabaseNotFoundError{DatabaseName: "shop"}, outside: true},
		"a config could not be read":  {err: &ghclient.DatabaseNotFoundError{DatabaseName: "shop", UnreadableConfigs: []string{"schema/shop/schemabot.yaml"}}},
		"invalid config":              {err: fmt.Errorf("%w at schema/shop/schemabot.yaml", ghclient.ErrInvalidConfig)},
		"github unavailable":          {err: errors.New("github unavailable")},
		"no error":                    {},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.outside, peerConfigOutsideRepository(tc.err))
		})
	}
}

// Planning several environments looks each peer's config up once. A failed
// lookup is not remembered, so the next environment reads it again.
func TestSharedSchemaPeerConfigsLooksEachPeerUpOnce(t *testing.T) {
	peers := newSharedSchemaPeerConfigs()
	lookups := map[string]int{}
	lookup := func(peer string, err error) func() (sharedSchemaPeerConfig, error) {
		return func() (sharedSchemaPeerConfig, error) {
			lookups[peer]++
			if err != nil {
				return sharedSchemaPeerConfig{}, err
			}
			return sharedSchemaPeerConfig{config: &ghclient.SchemabotConfig{Database: peer}, configDir: "schema/" + peer}, nil
		}
	}

	for range 2 {
		found, err := peers.find("shop", lookup("shop", nil))
		require.NoError(t, err)
		assert.Equal(t, "schema/shop", found.configDir)
		_, err = peers.find("ledger", lookup("ledger", errors.New("github unavailable")))
		require.Error(t, err)
	}
	assert.Equal(t, map[string]int{"shop": 1, "ledger": 2}, lookups)

	var none *sharedSchemaPeerConfigs
	for range 2 {
		_, err := none.find("shop", lookup("shop", nil))
		require.NoError(t, err)
	}
	assert.Equal(t, 3, lookups["shop"], "without a cache every call looks the peer up")
}

// An apply's base freshness check compares the database's own schema paths and
// then each shared schema peer's inputs, each path once. A stale rejection
// names the database's own schema when that changed, since its plan would
// revert it, and otherwise the peer whose files changed.
func TestBaseFreshnessPathsNameThePeerWhoseFilesChanged(t *testing.T) {
	schema := &ghclient.SchemaRequestResult{Database: "shop_sharded", SchemaPath: "schema/shop_sharded"}
	peers := []sharedSchemaPeerInputs{
		{Database: "shop", Paths: []string{"schema/shop", "schema/shop/schemabot.yaml"}},
		{Database: "shop_archive", Paths: []string{"schema/shop", "schema/shop_sharded", "schema/shop_archive"}},
	}

	paths, peerOfPath := baseFreshnessPaths(schema, peers)

	assert.Equal(t, []string{"schema/shop_sharded", "schema/shop", "schema/shop/schemabot.yaml", "schema/shop_archive"}, paths)
	assert.Equal(t, map[string]string{
		"schema/shop":                "shop",
		"schema/shop/schemabot.yaml": "shop",
		"schema/shop_archive":        "shop_archive",
	}, peerOfPath)

	path, peer := stalePathToShow(schema, []string{"schema/shop/schemabot.yaml", "schema/shop_sharded"}, peerOfPath)
	assert.Equal(t, "schema/shop_sharded", path)
	assert.Empty(t, peer)

	path, peer = stalePathToShow(schema, []string{"schema/shop/schemabot.yaml"}, peerOfPath)
	assert.Equal(t, "schema/shop/schemabot.yaml", path)
	assert.Equal(t, "shop", peer)
}
