// stopped_statements.go tracks statements a stopped run could not confirm had
// ended on the server, and keeps the engine from starting another schema change
// on that database until each one is gone.
package spirit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"github.com/block/spirit/pkg/dbconn"
	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/mysqlconn"
)

// unconfirmedStatement is a statement a stopped run issued whose end on the
// server could not be confirmed, so it may still take effect.
type unconfirmedStatement struct {
	// host and username are the target and user the run connected with. An
	// empty host matches every target.
	host     string
	username string
	database string
	// connID is the statement's session on the server. Zero means the session
	// is unknown, and the statement is treated as running until the engine
	// restarts.
	connID int
}

// matches reports whether the statement could still change the database a run
// on host is about to start on.
func (s unconfirmedStatement) matches(host, database string) bool {
	return (s.host == "" || s.host == host) && s.database == database
}

// noteStoppedStatement records that a stopped run's statement may still be
// running on the server when its error says the end could not be confirmed.
func (e *Engine) noteStoppedStatement(logger *slog.Logger, database string, err error) {
	if !errors.Is(err, dbconn.ErrCancelledStatementMayStillRun) {
		return
	}
	stmt := unconfirmedStatement{database: database}
	var cancelled *dbconn.CancelledStatementError
	if errors.As(err, &cancelled) {
		stmt.connID = cancelled.ConnID
	}
	e.mu.Lock()
	if rm := e.runningSchemaChange; rm != nil {
		stmt.host = rm.host
		stmt.username = rm.username
	}
	e.unconfirmedStatements = append(e.unconfirmedStatements, stmt)
	e.mu.Unlock()
	logger.Error("schema change stopped, but a statement it issued could not be confirmed to have ended on the server; the engine will not start another schema change on this database until the statement's session is gone",
		"database", database,
		"session", stmt.connID,
		"error", err,
	)
}

// CancelledStatementMayStillRun reports whether a run of this engine was
// stopped while a statement it issued could not be confirmed to have ended on
// the server, and no later start on that database has since confirmed the
// statement's session is gone. A stopped run kills the session of every
// statement it had in flight and waits for it to exit, so once this engine has
// drained and this reports false, nothing this engine issued is still running
// on the target.
//
// The record is held in memory by this Engine. It does not cover statements
// issued by another Engine, including one in an earlier process.
func (e *Engine) CancelledStatementMayStillRun() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.unconfirmedStatements) > 0
}

// stoppedStatementsEnded reports whether every unconfirmed statement on this
// database has left the server, forgetting each one that has. When one is
// still running, or the check itself fails, it records the run as failed and
// returns false, so the caller must not start the schema change. A statement
// still running is reported as the target being held, so the drive waits and
// starts the work again rather than failing the schema change.
func (e *Engine) stoppedStatementsEnded(ctx context.Context, host, username, password, database string) bool {
	e.mu.Lock()
	var pending []unconfirmedStatement
	for _, stmt := range e.unconfirmedStatements {
		if stmt.matches(host, database) {
			pending = append(pending, stmt)
		}
	}
	e.mu.Unlock()
	if len(pending) == 0 {
		return true
	}

	logger := e.changeLogger()
	ended, err := e.endedStatements(ctx, host, username, password, database, pending)
	e.forgetStatements(ended)
	for _, stmt := range ended {
		logger.Info("statement from a stopped schema change has left the server",
			"database", database,
			"session", stmt.connID,
		)
	}
	if err != nil {
		logger.Error("could not check whether a statement from a stopped schema change is still running; the schema change will not start",
			"database", database,
			"error", err,
		)
		e.setSchemaChangeFailed(fmt.Errorf("check statements from a stopped schema change on database %s: %w", database, err))
		return false
	}
	if len(ended) == len(pending) {
		return true
	}

	running := pending[len(ended)]
	logger.Warn("a statement from a stopped schema change is still running on the server; the schema change will start once its session is gone",
		"database", database,
		"session", running.connID,
	)
	e.setSchemaChangeFailed(fmt.Errorf("%w: %w", engine.ErrTargetHeld, engine.OperatorErrorf(
		fmt.Errorf("session %d", running.connID),
		"a statement from a stopped schema change on database %s is still running on the server; wait for it to finish or kill its session", database)))
	return false
}

// endedStatements returns the leading statements of pending whose sessions are
// gone from the server, stopping at the first one that may still be running.
func (e *Engine) endedStatements(ctx context.Context, host, username, password, database string, pending []unconfirmedStatement) ([]unconfirmedStatement, error) {
	db, err := mysqlconn.Open(targetDSN(host, username, password, database))
	if err != nil {
		return nil, fmt.Errorf("open database %s: %w", database, err)
	}
	defer utils.CloseAndLog(db)
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping database %s: %w", database, err)
	}

	var ended []unconfirmedStatement
	for _, stmt := range pending {
		// A user always sees its own sessions in the processlist, but not
		// another user's without the PROCESS privilege, so an absent session
		// proves nothing when this run connects as someone else.
		if stmt.connID == 0 || stmt.username != username {
			return ended, nil
		}
		var sessions int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.processlist WHERE id = ?", stmt.connID).Scan(&sessions); err != nil {
			return ended, fmt.Errorf("look up session %d: %w", stmt.connID, err)
		}
		if sessions > 0 {
			return ended, nil
		}
		ended = append(ended, stmt)
	}
	return ended, nil
}

// forgetStatements drops statements confirmed to have left the server.
func (e *Engine) forgetStatements(ended []unconfirmedStatement) {
	if len(ended) == 0 {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	kept := e.unconfirmedStatements[:0]
	for _, stmt := range e.unconfirmedStatements {
		if !containsStatement(ended, stmt) {
			kept = append(kept, stmt)
		}
	}
	e.unconfirmedStatements = kept
}

func containsStatement(stmts []unconfirmedStatement, target unconfirmedStatement) bool {
	return slices.Contains(stmts, target)
}
