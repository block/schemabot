//go:build integration

package spirit

import (
	"testing"

	drivermysql "github.com/block/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/targetauth"
)

// A MySQL target that rejects the configured credentials or names a database
// that does not exist fails the schema fetch with an error that still carries
// its authentication classification after the engine wraps it, so a caller
// can tell a credential problem apart from a transient connection failure
// without parsing the driver's message.
func TestEngine_FetchCurrentSchema_ClassifiesTargetAuthFailure(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(cfg *drivermysql.Config)
		want   targetauth.Classification
	}{
		{
			name:   "wrong password",
			mutate: func(cfg *drivermysql.Config) { cfg.Passwd = "wrong" },
			want:   targetauth.AuthInvalidCredentials,
		},
		{
			name:   "missing database",
			mutate: func(cfg *drivermysql.Config) { cfg.DBName = "target_auth_missing_db" },
			want:   targetauth.AuthNoDatabase,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := drivermysql.ParseDSN(sharedDSN)
			require.NoError(t, err)
			tc.mutate(cfg)

			_, err = New(Config{}).fetchCurrentSchema(t.Context(), cfg.FormatDSN(), cfg.DBName)
			require.Error(t, err)

			classification, ok := targetauth.ClassificationOf(err)
			assert.True(t, ok, "error should carry a target auth classification: %v", err)
			assert.Equal(t, tc.want, classification)
		})
	}
}
