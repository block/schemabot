// Package mysql implements the storage interface using MySQL.
package mysqlstore

import (
	"context"
	"database/sql"

	"github.com/block/schemabot/pkg/namedlock"
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
	controlRequests *controlRequestStore
	applyComments   *applyCommentStore
	planComments    *planCommentStore
	applyOperations *applyOperationStore
	checks          *checkStore
	settings        *settingsStore
	webhookEvents   *webhookEventStore
	checkRefreshes  *checkRefreshRequestStore
}

// New creates a new MySQL storage instance.
func New(db *sql.DB) *Storage {
	return &Storage{
		db:              db,
		locks:           &lockStore{db: db},
		plans:           &planStore{db: db, identity: MySQLDialect{}},
		applies:         &applyStore{db: db, dialect: MySQLDialect{}, identity: MySQLDialect{}, locker: namedlock.MySQL{}},
		tasks:           &taskStore{db: db, identity: MySQLDialect{}},
		applyLogs:       &applyLogStore{db: db, identity: MySQLDialect{}},
		controlRequests: &controlRequestStore{db: db, identity: MySQLDialect{}},
		applyComments:   &applyCommentStore{db: db, dialect: MySQLDialect{}},
		planComments:    &planCommentStore{db: db, identity: MySQLDialect{}},
		applyOperations: &applyOperationStore{db: db, dialect: MySQLDialect{}, identity: MySQLDialect{}, locker: namedlock.MySQL{}},
		checks:          &checkStore{db: db, dialect: MySQLDialect{}},
		settings:        &settingsStore{db: db, dialect: MySQLDialect{}},
		webhookEvents:   &webhookEventStore{db: db, dialect: MySQLDialect{}, identity: MySQLDialect{}},
		checkRefreshes:  &checkRefreshRequestStore{db: db, dialect: MySQLDialect{}, identity: MySQLDialect{}},
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

// ControlRequests returns the control request store.
func (s *Storage) ControlRequests() storage.ControlRequestStore {
	return s.controlRequests
}

// ApplyComments returns the apply comment store.
func (s *Storage) ApplyComments() storage.ApplyCommentStore {
	return s.applyComments
}

// PlanComments returns the plan comment store.
func (s *Storage) PlanComments() storage.PlanCommentStore {
	return s.planComments
}

// ApplyOperations returns the apply-operations store.
func (s *Storage) ApplyOperations() storage.ApplyOperationStore {
	return s.applyOperations
}

// Checks returns the check store.
func (s *Storage) Checks() storage.CheckStore {
	return s.checks
}

// Settings returns the settings store.
func (s *Storage) Settings() storage.SettingsStore {
	return s.settings
}

// WebhookEvents returns the durable webhook event inbox store.
func (s *Storage) WebhookEvents() storage.WebhookEventStore {
	return s.webhookEvents
}

// CheckRefreshRequests returns the durable check refresh request store.
func (s *Storage) CheckRefreshRequests() storage.CheckRefreshRequestStore {
	return s.checkRefreshes
}

// Ping verifies the database connection is alive.
func (s *Storage) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// Close closes the database connection.
func (s *Storage) Close() error {
	return s.db.Close()
}
