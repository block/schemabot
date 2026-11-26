package localsetup

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"syscall"

	"github.com/block/mysql"
	"github.com/jackc/pgx/v5/pgconn"
)

// Preserve the cause for diagnostic callers while displaying only safe error
// categories and server codes, never driver text that may contain credentials.
type setupConnectionError struct {
	message string
	cause   error
}

// Error names the cause and its fix in one line when the cause is one
// SchemaBot can identify, and falls back to the caller's generic line, with
// the error category or server code, for any other cause.
func (e *setupConnectionError) Error() string {
	if failure, ok := identifiedConnectionFailure(e.cause); ok {
		return failure
	}
	return e.message + ": " + connectionFailureReason(e.cause)
}
func (e *setupConnectionError) Unwrap() error { return e.cause }

// PostgreSQL SQLSTATE codes for the connection failures a first setup most
// often meets.
const (
	pgInvalidPassword          = "28P01"
	pgInvalidAuthorizationSpec = "28000"
	pgInvalidCatalogName       = "3D000"
)

// MySQL server error numbers for the same failures.
const (
	mysqlAccessDenied    = 1045
	mysqlUnknownDatabase = 1049
)

// identifiedConnectionFailure returns one line naming a connection failure's
// cause and what to change, for the causes SchemaBot can identify. The user
// and database it names come from the parsed connection settings, never from
// server or driver text.
func identifiedConnectionFailure(err error) (string, bool) {
	// A host name can resolve to several addresses, and pgx joins the error
	// from each one. What a server answered is checked before what the
	// network did, so a refusal from one address never hides the server's
	// answer on another.
	if errors.Is(err, context.Canceled) {
		return "the connection check was cancelled", true
	}
	var pgError *pgconn.PgError
	if errors.As(err, &pgError) {
		return identifiedPostgresFailure(err, pgError.Code)
	}
	var mysqlError *mysql.MySQLError
	if errors.As(err, &mysqlError) {
		switch mysqlError.Number {
		case mysqlAccessDenied:
			return "the server denied access for that user and password; check them in the connection string", true
		case mysqlUnknownDatabase:
			return "that database does not exist on the server; check the database name in the connection string", true
		}
		return "", false
	}
	if requiresTLS(err) && serverRefusedTLS(err) {
		return "the server does not offer TLS, which the connection's sslmode requires; connect to an endpoint that offers TLS, or change sslmode in the connection string", true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "the connection check timed out; check the host and port, and that your network can reach the server", true
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return "the server refused the connection; check that the database is running and that the host and port are right", true
	}
	var dns *net.DNSError
	if errors.As(err, &dns) {
		return "the database hostname could not be resolved; check the host in the connection string and your network connection", true
	}
	return "", false
}

// identifiedPostgresFailure names a PostgreSQL server refusal by its SQLSTATE.
func identifiedPostgresFailure(err error, code string) (string, bool) {
	user, database := "that user", "that database"
	var connectError *pgconn.ConnectError
	if errors.As(err, &connectError) && connectError.Config != nil {
		user = quotedSetting("user", connectError.Config.User, user)
		database = quotedSetting("database", connectError.Config.Database, database)
	}
	switch code {
	case pgInvalidPassword:
		// PostgreSQL answers a role that does not exist with this code too,
		// so the fix names both the user and the password.
		return fmt.Sprintf("the server rejected the password for %s; check the user name and password in the connection string", user), true
	case pgInvalidAuthorizationSpec:
		return fmt.Sprintf("the server does not accept %s from this client; check the user name, and that the server's client authentication rules allow it", user), true
	case pgInvalidCatalogName:
		return fmt.Sprintf("%s does not exist on the server; check the database name in the connection string", database), true
	}
	return "", false
}

// quotedSetting names a connection setting such as `user "postgres"`, quoted
// so a name cannot inject terminal controls, or returns fallback when the
// setting is empty.
func quotedSetting(kind, value, fallback string) string {
	if value == "" {
		return fallback
	}
	return fmt.Sprintf("%s %q", kind, value)
}

// pgxRefusedTLS is the text of the error pgx returns when the server answers
// its TLS request with a refusal. pgx exposes no typed error or sentinel for
// it, so serverRefusedTLS matches this exact text on the innermost error, and
// TestCheckConnectionNamesEachPostgresFailure pins it against a real server.
const pgxRefusedTLS = "server refused TLS connection"

// requiresTLS reports whether the connection settings pgx attached to its
// connect error allow no attempt without TLS. With sslmode=prefer or allow,
// pgx tries the server both ways, so a TLS refusal on one attempt is not why
// the connection failed, and the attempt without TLS holds the cause. Without
// the settings, nothing says TLS was required.
func requiresTLS(err error) bool {
	var connectError *pgconn.ConnectError
	if !errors.As(err, &connectError) || connectError.Config == nil {
		return false
	}
	if connectError.Config.TLSConfig == nil {
		return false
	}
	return !slices.ContainsFunc(connectError.Config.Fallbacks, func(fallback *pgconn.FallbackConfig) bool {
		return fallback.TLSConfig == nil
	})
}

// serverRefusedTLS reports whether a connection failed because the server
// does not offer TLS, on any of the addresses tried.
func serverRefusedTLS(err error) bool {
	if err == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		return slices.ContainsFunc(joined.Unwrap(), serverRefusedTLS)
	}
	if next := errors.Unwrap(err); next != nil {
		return serverRefusedTLS(next)
	}
	return err.Error() == pgxRefusedTLS
}

// connectionFailureReason names an unidentified failure by its category or
// server code, for the generic line.
func connectionFailureReason(err error) string {
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
