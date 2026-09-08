package mysqlconn

import (
	"database/sql/driver"

	"github.com/block/mysql"
	"github.com/block/schemabot/pkg/connreload"
	"github.com/block/schemabot/pkg/mysqlerr"
)

// erAccessDenied is MySQL's ER_ACCESS_DENIED_ERROR, returned when the server
// rejects the credentials a connection presented. It is the signature of a
// password rotated out from under a running pod, and it is the only code that
// triggers a credential reload.
//
// ER_ACCESS_DENIED_NO_PASSWORD_ERROR (1698) is deliberately not included. It
// means the account authenticates by something other than the password sent —
// auth_socket, or an account with no password at all — which is a grant shape
// no rotation of the secret changes, so a reload could only re-resolve the same
// credential and surface the same refusal a cooldown window later.
const erAccessDenied = 1045

// newConnector builds the connector that dials physical connections for a
// config. It is a seam so tests can exercise the reload path without a server.
var newConnector = mysql.NewConnector

// resolveConnector normalizes a raw DSN the way every SchemaBot-managed MySQL
// connection is normalized (see ConnectionDSN), applies the caller's options,
// and builds the connector that dials it. It is the Resolve half of the
// reloadable pool: it runs once at open and once per reload, never per dial, so
// a reloaded DSN the driver refuses is rejected when it is published rather
// than on every dial that follows.
func resolveConnector(dsn string, opts ...Option) (driver.Connector, error) {
	connectionDSN, err := ConnectionDSN(dsn, opts...)
	if err != nil {
		return nil, err
	}
	cfg, err := mysql.ParseDSN(connectionDSN)
	if err != nil {
		return nil, err
	}
	return newConnector(cfg)
}

// isAccessDenied reports whether err is the server rejecting the connection's
// credentials. The code is read through mysqlerr.Number rather than by
// asserting a driver error type, so a dial error wrapped on the way up — which
// database/sql and the pool above it both do — is still recognized.
func isAccessDenied(err error) bool {
	return mysqlerr.Is(err, erAccessDenied)
}

// reloadConfig describes the MySQL storage pool to pkg/connreload. Everything
// about *when* to reload — one reload per generation of credentials however
// many dials failed against it, a cooldown so a secrets-backend outage is not
// amplified into one resolve per refused dial, a reload detached from the dial
// that elected it — lives there and is shared with the PostgreSQL pool. What is
// MySQL's, and all that is MySQL's, is the two functions below.
func reloadConfig(reload func() (string, error), opts []Option) connreload.Config {
	return connreload.Config{
		Resolve: func(dsn string) (driver.Connector, error) { return resolveConnector(dsn, opts...) },
		Refused: isAccessDenied,
		Reload:  reload,
		Driver:  mysql.MySQLDriver{},
		Name:    "mysql-storage",
	}
}
