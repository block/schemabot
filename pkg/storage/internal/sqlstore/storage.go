// Package sqlstore implements the storage interface over database/sql. It is
// the shared dialect-parameterized core behind the public per-dialect
// constructors (mysqlstore, and in the future postgresstore); it currently
// emits MySQL-style SQL and is wired with MySQL dependencies.
package sqlstore

import (
	"context"
	"database/sql"

	"github.com/block/schemabot/pkg/namedlock"
	"github.com/block/schemabot/pkg/storage"
)

// Storage implements the storage.Storage interface using MySQL.
type Storage struct {
	db              *rebindDB
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
}

var _ storage.Storage = (*Storage)(nil)

// New creates a MySQL storage instance.
func New(db *sql.DB) *Storage {
	dialect := MySQLDialect{}
	return NewWithDependencies(db, dialect, dialect, dialect, namedlock.MySQL{}, NewMySQLErrorClassifier())
}

// NewWithDependencies creates a storage instance with database-specific dependencies.
func NewWithDependencies(db *sql.DB, binder binder, dialect Dialect, identity identityInserter, locker namedlock.Locker, classifier ErrorClassifier) *Storage {
	rdb := newRebindDB(db, binder)
	return &Storage{
		db:              rdb,
		locks:           &lockStore{db: rdb, classifier: classifier},
		plans:           &planStore{db: rdb, identity: identity, classifier: classifier},
		applies:         &applyStore{db: rdb, dialect: dialect, identity: identity, locker: locker, classifier: classifier},
		tasks:           &taskStore{db: rdb, identity: MySQLDialect{}},
		applyLogs:       &applyLogStore{db: rdb, identity: MySQLDialect{}},
		controlRequests: &controlRequestStore{db: rdb, identity: identity, classifier: classifier},
		applyComments:   &applyCommentStore{db: rdb, dialect: MySQLDialect{}},
		planComments:    &planCommentStore{db: rdb, identity: MySQLDialect{}},
		applyOperations: &applyOperationStore{db: rdb, dialect: dialect, identity: identity, locker: locker, classifier: classifier},
		checks:          &checkStore{db: rdb, dialect: dialect, classifier: classifier},
		settings:        &settingsStore{db: rdb, dialect: MySQLDialect{}},
		webhookEvents:   &webhookEventStore{db: rdb, dialect: dialect, identity: identity, classifier: classifier},
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

// Ping verifies the database connection is alive.
func (s *Storage) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// Close closes the database connection.
func (s *Storage) Close() error {
	return s.db.Close()
}
