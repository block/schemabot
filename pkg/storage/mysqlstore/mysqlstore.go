// Package mysqlstore provides the MySQL-backed storage.Storage implementation.
// The store logic lives in the shared internal sqlstore core; this package is
// the public constructor that assembles it with MySQL dependencies.
package mysqlstore

import (
	"database/sql"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/internal/sqlstore"
)

// Storage is the MySQL-backed storage implementation.
type Storage = sqlstore.Storage

var _ storage.Storage = (*Storage)(nil)

// New creates a new MySQL storage instance.
func New(db *sql.DB, opts ...storage.Option) *Storage {
	return sqlstore.NewMySQL(db, opts...)
}
