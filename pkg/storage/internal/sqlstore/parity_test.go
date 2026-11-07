//go:build integration

package sqlstore

import (
	"database/sql"
	"testing"
	"time"

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

func (mysqlHarness) AgeApplyOperation(t *testing.T, store storage.Storage, operationID int64, age time.Duration) {
	t.Helper()
	ageParityApplyOperation(t, store, operationID, age)
}

// ageParityApplyOperation backdates an operation's heartbeat on a Storage the
// parity harnesses returned, on either dialect.
func ageParityApplyOperation(t *testing.T, store storage.Storage, operationID int64, age time.Duration) {
	t.Helper()
	sqlStore, ok := store.(*Storage)
	require.True(t, ok, "parity harness storage is %T, not *Storage", store)
	backdateOperationLease(t, t.Context(), sqlStore, operationID, age)
}

func TestStorageParity(t *testing.T) {
	storagetest.Run(t, mysqlHarness{})
}
