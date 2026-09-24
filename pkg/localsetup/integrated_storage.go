package localsetup

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/block/mysql"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/block/schemabot/pkg/localruntime"
	"github.com/block/schemabot/pkg/mysqlconn"
	"github.com/block/schemabot/pkg/postgresconn"
	"github.com/block/spirit/pkg/utils"
)

// PrepareIntegratedStorage creates only the dedicated state database. Existing
// application schemas are never modified. Register revalidates before publishing.
func PrepareIntegratedStorage(ctx context.Context, manager localruntime.Manager, r Registration) error {
	if r.Storage.Database != "schemabot" || r.Storage.DSN != r.Connection.DSN {
		return fmt.Errorf("integrated storage requires a separate schemabot database using the application connection")
	}
	data, err := localruntime.ReadPrivate(filepath.Join(manager.Dir, "runtime.yaml"))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if _, err := registrationConfig(data, r); err != nil {
		return err
	}
	// A previously registered runtime owns its state. Never bootstrap it again:
	// startup will verify connectivity and initialize metadata as usual.
	if data != nil {
		return nil
	}
	dsn, err := r.Connection.ResolveDSN()
	if err != nil {
		return fmt.Errorf("resolve application connection")
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	var db *sql.DB
	switch r.Engine {
	case "mysql":
		db, err = mysqlconn.Open(dsn)
	case "postgres":
		db, err = postgresconn.Open(dsn)
	default:
		return fmt.Errorf("integrated setup is not available for this engine")
	}
	if err != nil {
		return fmt.Errorf("could not open the application connection")
	}
	defer utils.CloseAndLog(db)
	if err := db.PingContext(ctx); err != nil {
		return &setupConnectionError{message: "could not connect to your application server; check the connection and try again", cause: err}
	}
	// Deliberately omit IF NOT EXISTS: an existing database may belong to someone
	// else. Users can explicitly connect to it through standalone setup instead.
	if _, err := db.ExecContext(ctx, "CREATE DATABASE schemabot"); err != nil {
		return integratedStorageCreationError(err)
	}
	return nil
}

func integratedStorageCreationError(err error) error {
	message := "could not create the schemabot database"
	var my *mysql.MySQLError
	var pg *pgconn.PgError
	exists, denied := false, false
	if errors.As(err, &my) {
		exists = my.Number == 1007
		denied = my.Number == 1044 || my.Number == 1045 || my.Number == 1142
	} else if errors.As(err, &pg) {
		exists = pg.Code == "42P04"
		denied = pg.Code == "42501"
	}
	if exists {
		message = "the schemabot database already exists, possibly from an earlier setup attempt; if it is yours, choose standalone and connect to it to continue. Existing databases are never adopted automatically"
	} else if denied {
		message = "this user cannot create the schemabot database; ask for a separate state database, then choose standalone to connect to it"
	}
	return &setupConnectionError{message: message, cause: err}
}
