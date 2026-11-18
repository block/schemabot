package serve

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/block/mysql"
	"github.com/jackc/pgx/v5"

	"github.com/block/schemabot/pkg/postgresconn"
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

// foldedAddress is an address compared the way a host is resolved: case
// insensitively. Hostnames are, so the same database reached through a
// differently-cased spelling is the same target — and a secret rewritten with
// an RDS endpoint in another case is a rewrite of the spelling, not a move.
// Comparing the two byte-for-byte would refuse the reconnect, keep the pool on
// credentials that no longer authenticate, and tell every plan and apply to
// restart for the database they are already on.
//
// Only the host folds. A database name is case sensitive on both engines this
// serves, so folding it would accept a different database as the same one.
func foldedAddress(address string) string {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		// No port to separate: fold what there is, which is the host.
		return strings.ToLower(address)
	}
	return net.JoinHostPort(strings.ToLower(host), port)
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
		return storageTarget{address: foldedAddress(cfg.Addr), database: cfg.DBName}, nil
	case schema.DialectPostgres:
		// Through ConnectionDSN rather than straight to pgx: a failure here is
		// wrapped by callers that log it on a server, and a *url.Error in pgx's
		// chain reproduces the whole connection string — password included — in
		// its message. ConnectionDSN already refuses one without echoing it,
		// and normalizing first changes nothing this reads, since transport
		// settings are not the address or the database name.
		normalized, err := postgresconn.ConnectionDSN(dsn)
		if err != nil {
			return storageTarget{}, fmt.Errorf("read PostgreSQL storage DSN: %w", err)
		}
		cfg, err := pgx.ParseConfig(normalized)
		if err != nil {
			return storageTarget{}, fmt.Errorf("parse PostgreSQL storage DSN: %w", err)
		}
		return storageTarget{
			address:  foldedAddress(net.JoinHostPort(cfg.Host, strconv.Itoa(int(cfg.Port)))),
			database: cfg.Database,
		}, nil
	default:
		return storageTarget{}, fmt.Errorf("no storage DSN parser for storage dialect %q (supported: %q, %q)", dialect, schema.DialectMySQL, schema.DialectPostgres)
	}
}
