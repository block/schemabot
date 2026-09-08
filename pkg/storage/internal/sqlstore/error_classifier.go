package sqlstore

import (
	"errors"
	"strings"

	"github.com/block/schemabot/pkg/mysqlerr"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	// InnoDB rolls back deadlock victims and failed lock-wait statements, so
	// callers can safely retry these transient conflicts.
	mysqlErrDeadlock        = 1213
	mysqlErrLockWaitTimeout = 1205
	mysqlErrDuplicateKey    = 1062

	// PostgreSQL rolls back the failed transaction for deadlocks and
	// serialization failures. Lock-not-available reports an unacquired lock.
	postgresErrDeadlock             = "40P01"
	postgresErrSerializationFailure = "40001"
	postgresErrLockNotAvailable     = "55P03"
	postgresErrUniqueViolation      = "23505"
)

// ErrorClassifier identifies database errors that affect shared storage flow.
type ErrorClassifier interface {
	IsRetryableConflict(error) bool
	IsDuplicateKey(error) bool
}

type mysqlErrorClassifier struct{}

// NewMySQLErrorClassifier returns error classification for MySQL storage.
func NewMySQLErrorClassifier() ErrorClassifier {
	return mysqlErrorClassifier{}
}

// The codes are read through mysqlerr.Number rather than by asserting a
// driver's error type. Asserting a type is what silently broke here before: the
// credential-reloading storage pool went through a hot-swap DSN driver that
// embedded upstream go-sql-driver, so it returned a *mysql.MySQLError of a type
// no errors.As against block/mysql's could match — and the failure mode was
// every deadlock from that pool classified as non-retryable, not an error
// anyone would see. See mysqlerr.Number.
func (mysqlErrorClassifier) IsRetryableConflict(err error) bool {
	return mysqlerr.Is(err, mysqlErrDeadlock, mysqlErrLockWaitTimeout)
}

func (mysqlErrorClassifier) IsDuplicateKey(err error) bool {
	if mysqlerr.Is(err, mysqlErrDuplicateKey) {
		return true
	}
	// Defend against driver errors flattened to strings with %v in a call path.
	return err != nil && strings.Contains(err.Error(), "Duplicate entry")
}

type postgresErrorClassifier struct{}

// NewPostgresErrorClassifier returns error classification for PostgreSQL storage.
func NewPostgresErrorClassifier() ErrorClassifier {
	return postgresErrorClassifier{}
}

func (postgresErrorClassifier) IsRetryableConflict(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == postgresErrDeadlock ||
		pgErr.Code == postgresErrSerializationFailure ||
		pgErr.Code == postgresErrLockNotAvailable
}

func (postgresErrorClassifier) IsDuplicateKey(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == postgresErrUniqueViolation {
		return true
	}
	// Defend against driver errors flattened to strings with %v in a call path.
	return err != nil && strings.Contains(err.Error(), "duplicate key value violates unique constraint")
}
