//go:build integration

package sqlstore

import (
	"database/sql"
	"testing"

	_ "github.com/block/mysql"
	"github.com/stretchr/testify/require"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/storagetest"
)

// mysqlHarness adapts the package's MySQL testcontainer fixture to the
// cross-dialect storage parity suite.
type mysqlHarness struct{}

func (mysqlHarness) NewStorage(t *testing.T) storage.Storage {
	t.Helper()
	clearTables(t)
	return NewMySQL(testDB)
}

func (mysqlHarness) NewUnreachableStorage(t *testing.T) storage.Storage {
	t.Helper()
	db, err := sql.Open("block-mysql", testDSN)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	return NewMySQL(db)
}

func TestStorageParity(t *testing.T) {
	storagetest.Run(t, mysqlHarness{})
}
