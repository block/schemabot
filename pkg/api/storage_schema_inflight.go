package api

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/mysqlconn"
	"github.com/block/schemabot/pkg/namedlock"
	"github.com/block/schemabot/pkg/postgresconn"
	"github.com/block/schemabot/pkg/schema"
)

// A convergence is invisible in a diff. Its DDL runs against a shadow table
// that is not the real table yet, so a plan taken while one is copying reports
// exactly what a plan taken against an idle database reports: the statement is
// still outstanding. The two situations call for opposite decisions — wait, or
// converge — and nothing in the catalog separates them.
//
// The advisory lock does. A convergence holds it from before its diff until
// after its last statement, on both dialects, so "held" is the one signal that
// says work is happening rather than merely pending. It is what an operator
// who lost the session their convergence was running in has to read, and it is
// what keeps a second operator from starting an apply that would do nothing
// but wait out the first one's whole budget on a lock, silently.

// storageConvergenceInFlight reports whether a convergence currently holds the
// storage bootstrap lock on the database the report describes, which is the
// database at dsn.
//
// It takes nothing and waits for nothing: the probe reads the lock's state and
// returns, so it is safe to run against a database being converged right now,
// which is the only time it has anything to say.
//
// A false is not a claim that the database is idle — it is the absence of
// evidence that it is not. The answer describes the instant it was read, a
// holder can appear immediately afterwards, and a probe that could not run at
// all also reports false. Callers should surface a true and stay quiet on a
// false rather than render the negative as a finding.
func storageConvergenceInFlight(ctx context.Context, dsn string, report *StorageSchemaReport, logger *slog.Logger) bool {
	held, err := probeStorageConvergenceLock(ctx, dsn, report.Dialect)
	if err != nil {
		// Never fatal to the caller: this decorates a diff that has already
		// been computed, and a report an operator can act on beats a refusal
		// over a probe. Logged rather than dropped, because a probe failing on
		// a database whose diff succeeded is itself surprising.
		//
		// Named by the database the report is about, not by the DSN: an
		// operator reading this has to know which storage database went
		// unread, and the failure is one an instance reaches for its own
		// storage as readily as for a data plane's.
		logger.Warn("could not determine whether a storage convergence is in flight; reporting none",
			append(report.logAttrs(), "error", err)...)
		return false
	}
	return held
}

// probeStorageConvergenceLock opens a connection of the dialect's own kind,
// reads the lock, and closes it. The connection is deliberately short-lived
// and separate from any the caller holds: asking whether a lock is held on a
// session that might itself hold it answers a different question.
func probeStorageConvergenceLock(ctx context.Context, dsn string, dialect schema.Dialect) (bool, error) {
	var db *sql.DB
	var locker namedlock.Locker
	var err error
	switch dialect {
	case schema.DialectMySQL:
		db, err = mysqlconn.Open(dsn)
		locker = namedlock.MySQL{}
	case schema.DialectPostgres:
		db, err = postgresconn.Open(dsn)
		locker = namedlock.Postgres{}
	default:
		return false, fmt.Errorf("no storage bootstrap locker for dialect %q", dialect)
	}
	if err != nil {
		return false, fmt.Errorf("open storage database: %w", err)
	}
	defer utils.CloseAndLog(db)

	conn, err := db.Conn(ctx)
	if err != nil {
		return false, fmt.Errorf("connect to storage database: %w", err)
	}
	defer utils.CloseAndLog(conn)

	held, err := locker.HeldByAnySession(ctx, conn, ensureSchemaLockName)
	if err != nil {
		return false, fmt.Errorf("read storage bootstrap lock: %w", err)
	}
	return held, nil
}
