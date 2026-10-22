package localsetup

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"

	"github.com/block/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	ps "github.com/planetscale/planetscale-go/planetscale"
)

// Preserve the cause for diagnostic callers while displaying only safe error
// categories and server codes, never driver text that may contain credentials.
type setupConnectionError struct {
	message string
	cause   error
}

func (e *setupConnectionError) Error() string {
	return e.message + ": " + connectionFailureReason(e.cause)
}
func (e *setupConnectionError) Unwrap() error { return e.cause }
func connectionFailureReason(err error) string {
	if errors.Is(err, context.Canceled) {
		return "connection check cancelled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "connection check timed out"
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return "connection refused"
	}
	var dns *net.DNSError
	if errors.As(err, &dns) {
		return "database hostname could not be resolved"
	}
	var psError *ps.Error
	if errors.As(err, &psError) {
		return fmt.Sprintf("PlanetScale API error %s", psError.Code)
	}
	var mysqlError *mysql.MySQLError
	if errors.As(err, &mysqlError) {
		return fmt.Sprintf("database error %d", mysqlError.Number)
	}
	var pgError *pgconn.PgError
	if errors.As(err, &pgError) {
		// SQLSTATE contains five ASCII letters or digits.
		if len(pgError.Code) == 5 {
			for _, r := range pgError.Code {
				if (r < '0' || r > '9') && (r < 'A' || r > 'Z') {
					return "database rejected the connection"
				}
			}
			return "database error " + pgError.Code
		}
	}
	return "connection could not be verified"
}
