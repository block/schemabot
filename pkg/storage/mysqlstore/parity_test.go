//go:build integration

package mysqlstore

import (
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/storagetest"
)

// mysqlHarness adapts the package's MySQL testcontainer fixture to the
// cross-dialect storage parity suite.
type mysqlHarness struct{}

func (mysqlHarness) NewStorage(t *testing.T) storage.Storage {
	clearTables(t)
	return New(testDB)
}

func (mysqlHarness) NewUnreachableStorage(t *testing.T) storage.Storage {
	db, err := sql.Open("mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	return New(db)
}

func TestStorageParity_Settings(t *testing.T) {
	storagetest.TestSettings(t, mysqlHarness{})
}

func TestStorageParity_ApplyLogs(t *testing.T) {
	storagetest.TestApplyLogs(t, mysqlHarness{})
}
