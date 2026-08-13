// Package sqlstore implements the storage interface over database/sql. It is
// the shared dialect-parameterized core behind the public per-dialect
// constructors (mysqlstore and postgresstore); every store routes
// family-varying SQL syntax, placeholder binding, and identity retrieval
// through the injected dependencies.
package sqlstore

import (
	"context"
	"database/sql"

	"github.com/block/schemabot/pkg/namedlock"
	"github.com/block/schemabot/pkg/storage"
)

// Storage implements the storage.Storage interface over database/sql.
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

// NewMySQL creates a MySQL storage instance.
func NewMySQL(db *sql.DB) *Storage {
	dialect := MySQLDialect{}
	return NewWithDependencies(Dependencies{
		DB:         db,
		Binder:     dialect,
		Dialect:    dialect,
		Identity:   dialect,
		Locker:     namedlock.MySQL{},
		Classifier: NewMySQLErrorClassifier(),
	})
}

// NewPostgres creates a PostgreSQL storage instance.
func NewPostgres(db *sql.DB) *Storage {
	dialect := PostgresDialect{}
	return NewWithDependencies(Dependencies{
		DB:         db,
		Binder:     dialect,
		Dialect:    dialect,
		Identity:   dialect,
		Locker:     namedlock.Postgres{},
		Classifier: NewPostgresErrorClassifier(),
	})
}

// Dependencies contains the database-specific behavior used by Storage.
type Dependencies struct {
	DB         *sql.DB
	Binder     binder
	Dialect    Dialect
	Identity   identityInserter
	Locker     namedlock.Locker
	Classifier ErrorClassifier
}

// NewWithDependencies creates a storage instance whose database-specific
// behavior comes entirely from deps; no store hardwires a dialect.
func NewWithDependencies(deps Dependencies) *Storage {
	rdb := newRebindDB(deps.DB, deps.Binder)
	return &Storage{
		db:              rdb,
		locks:           &lockStore{db: rdb, classifier: deps.Classifier},
		plans:           &planStore{db: rdb, identity: deps.Identity, classifier: deps.Classifier},
		applies:         &applyStore{db: rdb, dialect: deps.Dialect, identity: deps.Identity, locker: deps.Locker, classifier: deps.Classifier},
		tasks:           &taskStore{db: rdb, identity: deps.Identity},
		applyLogs:       &applyLogStore{db: rdb, identity: deps.Identity},
		controlRequests: &controlRequestStore{db: rdb, identity: deps.Identity, classifier: deps.Classifier, dialect: deps.Dialect},
		applyComments:   &applyCommentStore{db: rdb, dialect: deps.Dialect},
		planComments:    &planCommentStore{db: rdb, identity: deps.Identity},
		applyOperations: &applyOperationStore{db: rdb, dialect: deps.Dialect, identity: deps.Identity, locker: deps.Locker, classifier: deps.Classifier},
		checks:          &checkStore{db: rdb, dialect: deps.Dialect, classifier: deps.Classifier},
		settings:        &settingsStore{db: rdb, dialect: deps.Dialect},
		webhookEvents:   &webhookEventStore{db: rdb, dialect: deps.Dialect, identity: deps.Identity, classifier: deps.Classifier},
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
