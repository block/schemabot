package serve

import (
	"fmt"
	"net"
	"strconv"

	"github.com/block/mysql"
	"github.com/jackc/pgx/v5"

	"github.com/block/schemabot/pkg/schema"
)

// storageTarget is the database a DSN names, with the credentials left out: the
// server and the database on it, and nothing that rotation changes.
//
// It exists so a re-resolved DSN can be checked against the one this instance
// booted on. The storage schema surface reads and converges *this instance's
// own* storage (AV-9), and the DSN is resolved from a config file, an
// environment variable, or a mounted secret — all of which can be rewritten
// under a running pod. A password rewritten there is a rotation and must keep
// working; a host or a database name rewritten there names a different
// database, and answering for that one would make every report a lie about the
// instance that produced it.
type storageTarget struct {
	address  string
	database string
}

func (t storageTarget) String() string {
	return t.address + "/" + t.database
}

// storageTargetFor parses the target out of a DSN for the storage dialect. The
// two families are parsed by their own drivers rather than by a shared string
// rule, because neither DSN grammar is the other's: a MySQL DSN carries the
// address in its own form and a PostgreSQL connection string accepts both a URL
// and a keyword/value spelling.
func storageTargetFor(dialect schema.Dialect, dsn string) (storageTarget, error) {
	switch dialect {
	case schema.DialectMySQL:
		cfg, err := mysql.ParseDSN(dsn)
		if err != nil {
			return storageTarget{}, fmt.Errorf("parse MySQL storage DSN: %w", err)
		}
		return storageTarget{address: cfg.Addr, database: cfg.DBName}, nil
	case schema.DialectPostgres:
		cfg, err := pgx.ParseConfig(dsn)
		if err != nil {
			return storageTarget{}, fmt.Errorf("parse PostgreSQL storage DSN: %w", err)
		}
		return storageTarget{
			address:  net.JoinHostPort(cfg.Host, strconv.Itoa(int(cfg.Port))),
			database: cfg.Database,
		}, nil
	default:
		return storageTarget{}, fmt.Errorf("no storage DSN parser for storage dialect %q (supported: %q, %q)", dialect, schema.DialectMySQL, schema.DialectPostgres)
	}
}
