package localsetup

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/api"
	"github.com/block/schemabot/pkg/localruntime"
)

func registration(t *testing.T, engine string) Registration {
	t.Helper()
	target, storage := "user:private@tcp(127.0.0.1:3306)/app", "user:private@tcp(127.0.0.1:3306)/state"
	if engine == "postgres" {
		target, storage = "postgres://user:private@127.0.0.1/app", "postgres://user:private@127.0.0.1/state"
	}
	t.Setenv("SETUP_TARGET", target)
	t.Setenv("SETUP_STORAGE", storage)
	return Registration{Database: "app", Environment: "development", Engine: engine,
		Connection: api.EnvironmentConfig{DSN: "env:SETUP_TARGET"},
		Storage:    api.StorageConfig{Dialect: engine, DSN: "env:SETUP_STORAGE"}}
}

func TestRegisterPreservesReferencesAndRegistrations(t *testing.T) {
	for _, engine := range []string{"mysql", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			m := localruntime.Manager{Dir: filepath.Join(t.TempDir(), "shared")}
			r := registration(t, engine)
			changed, err := Register(m, r)
			require.NoError(t, err)
			require.True(t, changed)
			path := filepath.Join(m.Dir, "runtime.yaml")
			first, err := localruntime.ReadPrivate(path)
			require.NoError(t, err)
			require.NotContains(t, string(first), "user:private")
			require.Contains(t, string(first), "env:SETUP_TARGET")
			changed, err = Register(m, r)
			require.NoError(t, err)
			require.False(t, changed)
			again, err := localruntime.ReadPrivate(path)
			require.NoError(t, err)
			require.Equal(t, first, again)
			r.Database = "billing"
			changed, err = Register(m, r)
			require.NoError(t, err)
			require.True(t, changed)
			data, err := localruntime.ReadPrivate(path)
			require.NoError(t, err)
			cfg, err := api.ParseServerConfig(data)
			require.NoError(t, err)
			require.Len(t, cfg.Databases, 2)
			require.Equal(t, engine, cfg.Databases["app"].Type)
			require.Equal(t, "env:SETUP_TARGET", cfg.Databases["billing"].Environments["development"].DSN)
		})
	}
}

func TestRegisterRefusesConflictingChanges(t *testing.T) {
	m := localruntime.Manager{Dir: filepath.Join(t.TempDir(), "shared")}
	r := registration(t, "mysql")
	_, err := Register(m, r)
	require.NoError(t, err)
	before, err := localruntime.ReadPrivate(filepath.Join(m.Dir, "runtime.yaml"))
	require.NoError(t, err)
	conflict := r
	conflict.Connection.DSN = "user@tcp(127.0.0.1:3306)/other"
	_, err = Register(m, conflict)
	require.ErrorContains(t, err, "different registration")
	conflict = r
	conflict.Storage.DSN = "user@tcp(127.0.0.1:3306)/other_state"
	_, err = Register(m, conflict)
	require.ErrorContains(t, err, "cannot replace durable state")
	after, err := localruntime.ReadPrivate(filepath.Join(m.Dir, "runtime.yaml"))
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestRegisterRejectsUnsafeStorageBeforeWriting(t *testing.T) {
	m := localruntime.Manager{Dir: filepath.Join(t.TempDir(), "shared")}
	r := registration(t, "mysql")
	r.Storage.DSN = r.Connection.DSN
	_, err := Register(m, r)
	require.Error(t, err)
	_, err = os.Stat(m.Dir)
	require.True(t, os.IsNotExist(err))
}

func TestRegisterPreservesExistingStoragePool(t *testing.T) {
	m := localruntime.Manager{Dir: filepath.Join(t.TempDir(), "shared")}
	r := registration(t, "mysql")
	r.Storage.Pool.MaxOpenConns = 17
	_, err := Register(m, r)
	require.NoError(t, err)
	r.Database = "billing"
	r.Storage.Pool = api.StoragePoolConfig{}
	changed, err := Register(m, r)
	require.NoError(t, err)
	require.True(t, changed)
	data, err := localruntime.ReadPrivate(filepath.Join(m.Dir, "runtime.yaml"))
	require.NoError(t, err)
	cfg, err := api.ParseServerConfig(data)
	require.NoError(t, err)
	require.Equal(t, 17, cfg.Storage.Pool.MaxOpenConns)
	require.Len(t, cfg.Databases, 2)
}
