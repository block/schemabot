// Package mysql implements the storage interface using MySQL.
package mysqlstore

import (
	"context"
	"database/sql"

	"github.com/block/schemabot/pkg/storage"
)

// Storage implements the storage.Storage interface using MySQL.
type Storage struct {
	db              *sql.DB
	locks           *lockStore
	plans           *planStore
	applies         *applyStore
	tasks           *taskStore
	applyLogs       *applyLogStore
	applyComments   *applyCommentStore
	checks          *checkStore
	settings        *settingsStore
	vitessApplyData *vitessApplyDataStore
}

// New creates a new MySQL storage instance.
func New(db *sql.DB) *Storage {
	return &Storage{
		db:              db,
		locks:           &lockStore{db: db},
		plans:           &planStore{db: db},
		applies:         &applyStore{db: db},
		tasks:           &taskStore{db: db},
		applyLogs:       &applyLogStore{db: db},
		applyComments:   &applyCommentStore{db: db},
		checks:          &checkStore{db: db},
		settings:        &settingsStore{db: db},
		vitessApplyData: &vitessApplyDataStore{db: db},
	}
}

// Locks returns the lock store.
func (s *Storage) Locks() storage.LockStore {
	return s.locks
}

// Plans returns the plan store.
func (s *Storage) Plans() storage.PlanStore {
	return s.plans
}

// Applies returns the apply store.
func (s *Storage) Applies() storage.ApplyStore {
	return s.applies
}

// Tasks returns the task store.
func (s *Storage) Tasks() storage.TaskStore {
	return s.tasks
}

// ApplyLogs returns the apply logs store.
func (s *Storage) ApplyLogs() storage.ApplyLogStore {
	return s.applyLogs
}

// ApplyComments returns the apply comment store.
func (s *Storage) ApplyComments() storage.ApplyCommentStore {
	return s.applyComments
}

// Checks returns the check store.
func (s *Storage) Checks() storage.CheckStore {
	return s.checks
}

// Settings returns the settings store.
func (s *Storage) Settings() storage.SettingsStore {
	return s.settings
}

// VitessApplyData returns the Vitess apply data store.
func (s *Storage) VitessApplyData() storage.VitessApplyDataStore {
	return s.vitessApplyData
}

// Ping verifies the database connection is alive.
func (s *Storage) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// Close closes the database connection.
func (s *Storage) Close() error {
	return s.db.Close()
}
