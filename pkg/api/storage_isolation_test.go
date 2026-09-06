package api

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStorageIsolationAcrossDatabaseFamilies(t *testing.T) {
	for _, tc := range []struct {
		name, dialect, storageDSN, engine, targetDSN string
		shared                                       bool
	}{
		{"postgres URL", "postgres", "postgres://user@localhost/state", "postgres", "postgres://user@localhost/app", false},
		{"postgres keyword", "postgres", "host=localhost user=user dbname=state", "postgres", "host=alias user=user dbname=state", true},
		{"postgres aliases", "postgres", "postgres://user@localhost/state", "postgres", "postgres://user@alias/state", true},
		{"independent families", "postgres", "postgres://user@localhost/app", "mysql", "root@tcp(localhost:3306)/app", false},
		{"MySQL family", "mysql", "root@tcp(localhost:3306)/state", "vitess", "root@tcp(alias:3306)/state", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := ServerConfig{Storage: StorageConfig{Dialect: tc.dialect, DSN: tc.storageDSN}, Databases: map[string]DatabaseConfig{
				"app": {Type: tc.engine, Environments: map[string]EnvironmentConfig{"development": {DSN: tc.targetDSN}}},
			}}
			err := cfg.ValidateStorageIsolation()
			if tc.shared {
				require.ErrorContains(t, err, "different name")
			} else {
				require.NoError(t, err)
			}
		})
	}
}
