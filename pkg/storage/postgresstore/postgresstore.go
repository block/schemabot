// Package postgresstore provides the PostgreSQL-backed storage.Storage
// implementation. The store logic lives in the shared internal sqlstore core;
// this package is the public constructor that assembles it with PostgreSQL
// dependencies.
package postgresstore

import (
	"database/sql"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/schemabot/pkg/storage/internal/sqlstore"
)

// Storage is the PostgreSQL-backed storage implementation.
type Storage = sqlstore.Storage

var _ storage.Storage = (*Storage)(nil)

// New creates a new PostgreSQL storage instance.
func New(db *sql.DB, opts ...storage.Option) *Storage {
	return sqlstore.NewPostgres(db, opts...)
}
