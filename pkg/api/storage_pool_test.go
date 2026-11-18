package api

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStoragePoolConfig_Defaults verifies that an unset pool config resolves to
// the values that were previously hardcoded in the server build path, so
// omitting storage.pool preserves the prior behavior exactly.
func TestStoragePoolConfig_Defaults(t *testing.T) {
	var p StoragePoolConfig

	assert.Equal(t, DefaultStorageMaxIdleConns, p.MaxIdleConnsOrDefault())
	assert.Equal(t, DefaultStorageConnMaxLifetime, p.ConnMaxLifetimeOrDefault())
	assert.Equal(t, DefaultStorageConnMaxIdleTime, p.ConnMaxIdleTimeOrDefault())
	// An unset connect timeout means "leave the driver default".
	assert.Equal(t, time.Duration(0), p.ConnectTimeoutOrZero())
	// MaxOpenConns is applied by the caller only when > 0; unset stays 0.
	assert.Equal(t, 0, p.MaxOpenConns)
}

// TestStoragePoolConfig_Overrides verifies that configured values win over the
// defaults.
func TestStoragePoolConfig_Overrides(t *testing.T) {
	p := StoragePoolConfig{
		MaxIdleConns:    25,
		MaxOpenConns:    50,
		ConnMaxLifetime: "10m",
		ConnMaxIdleTime: "90s",
		ConnectTimeout:  "3s",
	}

	assert.Equal(t, 25, p.MaxIdleConnsOrDefault())
	assert.Equal(t, 10*time.Minute, p.ConnMaxLifetimeOrDefault())
	assert.Equal(t, 90*time.Second, p.ConnMaxIdleTimeOrDefault())
	assert.Equal(t, 3*time.Second, p.ConnectTimeoutOrZero())
}

// TestStoragePoolConfig_Validate exercises the config-load guardrails.
func TestStoragePoolConfig_Validate(t *testing.T) {
	tests := []struct {
		name       string
		pool       StoragePoolConfig
		wantErrSub string
	}{
		{
			name: "empty is valid",
			pool: StoragePoolConfig{},
		},
		{
			name: "fully specified is valid",
			pool: StoragePoolConfig{
				MaxIdleConns:    10,
				MaxOpenConns:    20,
				ConnMaxLifetime: "5m",
				ConnMaxIdleTime: "3m",
				ConnectTimeout:  "5s",
			},
		},
		{
			name:       "negative max_idle_conns rejected",
			pool:       StoragePoolConfig{MaxIdleConns: -1},
			wantErrSub: "max_idle_conns",
		},
		{
			name:       "negative max_open_conns rejected",
			pool:       StoragePoolConfig{MaxOpenConns: -1},
			wantErrSub: "max_open_conns",
		},
		{
			name:       "idle exceeding open rejected",
			pool:       StoragePoolConfig{MaxIdleConns: 30, MaxOpenConns: 10},
			wantErrSub: "must not exceed",
		},
		{
			name:       "unparseable lifetime rejected",
			pool:       StoragePoolConfig{ConnMaxLifetime: "nope"},
			wantErrSub: "conn_max_lifetime",
		},
		{
			name:       "non-positive idle time rejected",
			pool:       StoragePoolConfig{ConnMaxIdleTime: "0s"},
			wantErrSub: "must be positive",
		},
		{
			name:       "unparseable connect timeout rejected",
			pool:       StoragePoolConfig{ConnectTimeout: "5"},
			wantErrSub: "connect_timeout",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.pool.Validate("storage")
			if tt.wantErrSub == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErrSub)
		})
	}
}

// TestStorageConfig_ValidatePropagatesPoolErrors ensures a bad pool config is
// surfaced through the storage config's validation entry point (so it fails at
// config load rather than silently at runtime).
func TestStorageConfig_ValidatePropagatesPoolErrors(t *testing.T) {
	c := StorageConfig{
		DSN:  "root:secret@tcp(localhost:3306)/app",
		Pool: StoragePoolConfig{ConnMaxLifetime: "bogus"},
	}
	err := c.validateLocalDSNConfig("storage")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "conn_max_lifetime")
}
