package sqlstore

import (
	"errors"
	"fmt"
	"testing"

	gomysql "github.com/block/mysql"
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
	assert.False(t, classifier.IsDuplicateKey(errors.New("write failed")))
	assert.False(t, classifier.IsDuplicateKey(nil))

	for _, number := range []uint16{mysqlErrWarnDataOutOfRange, mysqlErrDataTooLong, mysqlErrTruncatedWrongValue, mysqlErrIncorrectValue} {
		assert.True(t, classifier.IsValueRejected(fmt.Errorf("update task: %w", &gomysql.MySQLError{Number: number})), "error %d", number)
	}
	assert.False(t, classifier.IsValueRejected(&gomysql.MySQLError{Number: mysqlErrDeadlock}))
	assert.False(t, classifier.IsValueRejected(errors.New("write failed")))
	assert.False(t, classifier.IsValueRejected(nil))
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
	assert.True(t, classifier.IsDuplicateKey(errors.New("write row: duplicate key value violates unique constraint \"widgets_name_key\"")))
	assert.False(t, classifier.IsDuplicateKey(&pgconn.PgError{Code: "40P01"}))
	assert.False(t, classifier.IsDuplicateKey(errors.New("write failed")))
	assert.False(t, classifier.IsDuplicateKey(nil))

	for _, code := range []string{"22003", "22001", "22P02"} {
		assert.True(t, classifier.IsValueRejected(fmt.Errorf("update task: %w", &pgconn.PgError{Code: code})), "code %s", code)
	}
	assert.False(t, classifier.IsValueRejected(&pgconn.PgError{Code: "23505"}))
	assert.False(t, classifier.IsValueRejected(errors.New("write failed")))
	assert.False(t, classifier.IsValueRejected(nil))
}
