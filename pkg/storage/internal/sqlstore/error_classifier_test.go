package sqlstore

import (
	"errors"
	"fmt"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
)

func TestMySQLErrorClassifier(t *testing.T) {
	classifier := NewMySQLErrorClassifier()

	assert.True(t, classifier.IsRetryableConflict(&gomysql.MySQLError{Number: mysqlErrDeadlock}))
	assert.True(t, classifier.IsRetryableConflict(&gomysql.MySQLError{Number: mysqlErrLockWaitTimeout}))
	assert.True(t, classifier.IsRetryableConflict(fmt.Errorf("write row: %w", &gomysql.MySQLError{Number: mysqlErrDeadlock})))
	assert.False(t, classifier.IsRetryableConflict(&gomysql.MySQLError{Number: mysqlErrDuplicateKey}))
	assert.False(t, classifier.IsRetryableConflict(errors.New("write failed")))

	assert.True(t, classifier.IsDuplicateKey(&gomysql.MySQLError{Number: mysqlErrDuplicateKey}))
	assert.True(t, classifier.IsDuplicateKey(fmt.Errorf("write row: %w", &gomysql.MySQLError{Number: mysqlErrDuplicateKey})))
	assert.True(t, classifier.IsDuplicateKey(errors.New("Duplicate entry 'value' for key 'key'")))
	assert.False(t, classifier.IsDuplicateKey(&gomysql.MySQLError{Number: mysqlErrDeadlock}))
	assert.False(t, classifier.IsDuplicateKey(nil))
}

func TestPostgresErrorClassifier(t *testing.T) {
	classifier := NewPostgresErrorClassifier()

	for _, code := range []string{"40P01", "40001", "55P03"} {
		t.Run("retryable "+code, func(t *testing.T) {
			assert.True(t, classifier.IsRetryableConflict(&pgconn.PgError{Code: code}))
			assert.True(t, classifier.IsRetryableConflict(fmt.Errorf("write row: %w", &pgconn.PgError{Code: code})))
		})
	}
	assert.False(t, classifier.IsRetryableConflict(&pgconn.PgError{Code: "23505"}))
	assert.False(t, classifier.IsRetryableConflict(errors.New("write failed")))

	assert.True(t, classifier.IsDuplicateKey(&pgconn.PgError{Code: "23505"}))
	assert.True(t, classifier.IsDuplicateKey(fmt.Errorf("write row: %w", &pgconn.PgError{Code: "23505"})))
	assert.False(t, classifier.IsDuplicateKey(&pgconn.PgError{Code: "40P01"}))
	assert.False(t, classifier.IsDuplicateKey(nil))
}
