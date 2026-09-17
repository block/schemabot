package api

import (
	"testing"

	"github.com/block/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestStorageDatabasePreservesConnectionAndRotation(t *testing.T) {
	for _, tt := range []struct{ engine, dsn string }{
		{"mysql", "user:secret@tcp(localhost:3306)/app?parseTime=true&timeout=5s"},
		{"postgres", "postgres://user:secret@localhost/app?sslmode=disable&application_name=wizard"},
		{"postgres", "host=localhost user=user password=secret dbname=app sslmode=disable application_name=wizard"},
		{"postgres", "postgres://user:secret@localhost/app?dbname=other&sslmode=disable&application_name=wizard"},
	} {
		t.Run(tt.dsn, func(t *testing.T) {
			t.Setenv("INTEGRATED_TEST_DSN", tt.dsn)
			c := ServerConfig{Storage: StorageConfig{Dialect: tt.engine, DSN: "env:INTEGRATED_TEST_DSN", Database: "schemabot"}}
			got, err := c.StorageDSN()
			require.NoError(t, err)
			if tt.engine == "mysql" {
				cfg, err := mysql.ParseDSN(got)
				require.NoError(t, err)
				require.Equal(t, "schemabot", cfg.DBName)
				require.Equal(t, "secret", cfg.Passwd)
				require.True(t, cfg.ParseTime)
			} else {
				cfg, err := pgx.ParseConfig(got)
				require.NoError(t, err)
				require.Equal(t, "schemabot", cfg.Database)
				require.Equal(t, "secret", cfg.Password)
				require.Equal(t, "wizard", cfg.RuntimeParams["application_name"])
			}
			require.Equal(t, "env:INTEGRATED_TEST_DSN", c.Storage.DSN)
			t.Setenv("INTEGRATED_TEST_DSN", "invalid://connection")
			_, err = c.StorageDSN()
			require.Error(t, err)
		})
	}
}
