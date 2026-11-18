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
