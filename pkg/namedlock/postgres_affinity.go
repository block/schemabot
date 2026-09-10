package namedlock

import (
	"context"
	"crypto/rand"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// ErrNoSessionAffinity reports that a PostgreSQL connection pool was proven not
// to give each of its connections one stable server session. Session-scoped
// advisory locks are the whole of SchemaBot's cross-instance exclusion on
// PostgreSQL, and without that binding they grant without excluding anyone: the
// lock lands on a backend the client stops mapping to, so a second caller can
// take the same lock while the first believes it holds it.
//
// The usual cause is a transaction-mode connection pooler in front of the
// database, which rebinds a backend per transaction.
var ErrNoSessionAffinity = errors.New("connection pool does not bind a connection to one PostgreSQL session")

// sessionAffinityProbeTimeout bounds the whole probe so a pooler that stalls
// the second connection cannot wedge the caller's startup. The probe issues a
// handful of trivial statements, so anything near this bound is already a
// failure.
const sessionAffinityProbeTimeout = 15 * time.Second

// sessionAffinityProbePrefix names the probe's advisory locks in pg_locks, so
// an operator reading the catalog while a probe runs can tell what took the
// key.
const sessionAffinityProbePrefix = "schemabot_session_affinity_probe_"

// VerifySessionAffinity proves on db the property every session-scoped advisory
// lock rests on: that a connection handed out by this pool keeps one server
// session, so a lock taken on it is a lock other connections cannot take. It
// returns an error wrapping ErrNoSessionAffinity when it proves the property
// does not hold, so callers can refuse to run rather than proceed without the
// exclusion they think they have.
//
// The probe takes a lock on one connection and then makes a second connection
// hold a transaction open across the check. A transaction-mode pooler pins a
// backend for a transaction's duration, so the second connection takes the
// backend the first one's single-statement acquire just released, and the
// first connection's next statement lands somewhere else. That turns a rebind
// that would otherwise depend on load into one this probe can observe, and it
// gives three independent readings of the same failure:
//
//   - the second connection takes a lock the first one still holds, which is
//     the lost exclusion itself rather than a proxy for it;
//   - the first connection no longer appears in pg_locks as the session
//     holding its own lock;
//   - the first connection cannot release what it took.
//
// Against a direct connection, and against a pooler that hands out a session
// per client connection, all three readings are the healthy one — the probe
// has no false positive to trade off, because each reading is a fact about
// this pool rather than a guess about what sits behind it.
//
// It is one-sided: an idle transaction-mode pooler with spare backends can
// still answer every reading the healthy way, so a clean probe is evidence and
// not proof. Callers should treat it as a startup guard against the
// configuration an operator lands on by following a hosted platform's default
// connection string, not as a substitute for pointing storage at a direct
// endpoint.
//
// The probe key is freshly random on every call, so concurrent probes never
// contend and a lock left behind on an unreachable backend can never block
// anything later.
func (p Postgres) VerifySessionAffinity(ctx context.Context, db *sql.DB) error {
	ctx, cancel := context.WithTimeout(ctx, sessionAffinityProbeTimeout)
	defer cancel()

	name, err := sessionAffinityProbeName()
	if err != nil {
		return err
	}
	key := advisoryLockKey(name)

	holder, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("verify session affinity: open probe connection: %w", err)
	}
	// Until the release below succeeds, this connection's session may still
	// carry the probe lock, so it is discarded rather than returned to the
	// pool for reuse.
	holderClean := false
	defer func() { closeProbeConn(ctx, holder, holderClean) }()

	// A zero wait runs pg_try_advisory_lock as a bare statement, which is its
	// own implicit transaction — the same shape as the real acquires this
	// probe is vouching for.
	acquired, err := p.Acquire(ctx, holder, name, 0)
	if err != nil {
		return fmt.Errorf("verify session affinity: %w", err)
	}
	if !acquired {
		// Nothing else can be holding a freshly random key, so a refusal means
		// the acquire did not land where it reported.
		return fmt.Errorf("%w: a freshly keyed probe lock was reported as already held", ErrNoSessionAffinity)
	}

	reentered, stillHeld, err := probeAcrossPinnedBackend(ctx, db, holder, key)
	if err != nil {
		return fmt.Errorf("verify session affinity: %w", err)
	}
	if reentered {
		return fmt.Errorf("%w: a second connection took an advisory lock the first connection holds", ErrNoSessionAffinity)
	}
	if !stillHeld {
		return fmt.Errorf("%w: the connection that took an advisory lock is no longer the session holding it", ErrNoSessionAffinity)
	}

	released, err := p.Release(ctx, holder, name)
	if err != nil {
		return fmt.Errorf("verify session affinity: %w", err)
	}
	if !released {
		return fmt.Errorf("%w: the connection that took an advisory lock could not release it", ErrNoSessionAffinity)
	}
	holderClean = true
	return nil
}

// probeAcrossPinnedBackend holds a transaction open on a second pool
// connection and, from inside it, reports whether that connection can take the
// advisory lock holder already holds. While the transaction is open it reports
// whether holder still appears as the session holding that lock.
//
// The open transaction is the point: it denies holder the backend it acquired
// on, so holder's read runs wherever the pool sends it next.
func probeAcrossPinnedBackend(ctx context.Context, db *sql.DB, holder *sql.Conn, key int64) (reentered, stillHeld bool, err error) {
	pinned, err := db.Conn(ctx)
	if err != nil {
		return false, false, fmt.Errorf("open second probe connection: %w", err)
	}
	pinnedClean := false
	defer func() { closeProbeConn(ctx, pinned, pinnedClean) }()

	tx, err := pinned.BeginTx(ctx, nil)
	if err != nil {
		return false, false, fmt.Errorf("begin probe transaction: %w", err)
	}

	if err := tx.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&reentered); err != nil {
		return false, false, rollbackAfter(tx, fmt.Errorf("probe advisory lock from a second connection: %w", err))
	}
	if reentered {
		// Undo the re-entry so the lock count on the shared backend goes back
		// to what the holder took, leaving the holder's own release meaningful.
		if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", key); err != nil {
			return false, false, rollbackAfter(tx, fmt.Errorf("undo probe advisory lock re-entry: %w", err))
		}
	}

	stillHeld, err = sessionHoldsAdvisoryLock(ctx, holder, key)
	if err != nil {
		return false, false, rollbackAfter(tx, err)
	}

	if err := tx.Rollback(); err != nil {
		return false, false, fmt.Errorf("roll back probe transaction: %w", err)
	}
	// A connection that reached another connection's lock is on a session this
	// one cannot account for, undone re-entry or not, so it is discarded rather
	// than handed back to the pool.
	pinnedClean = !reentered
	return reentered, stillHeld, nil
}

// sessionHoldsAdvisoryLock reports whether conn's current backend is the one
// holding the advisory lock for key. pg_locks reports a single-argument
// advisory key as objsubid 1 with the key's high and low halves in classid and
// objid, both read as unsigned OIDs.
func sessionHoldsAdvisoryLock(ctx context.Context, conn *sql.Conn, key int64) (bool, error) {
	classID, objID := advisoryLockCatalogKey(key)
	var held bool
	const query = `SELECT EXISTS (
		SELECT 1 FROM pg_locks
		WHERE locktype = 'advisory'
		  AND granted
		  AND objsubid = 1
		  AND classid::bigint = $1
		  AND objid::bigint = $2
		  AND pid = pg_backend_pid()
	)`
	if err := conn.QueryRowContext(ctx, query, classID, objID).Scan(&held); err != nil {
		return false, fmt.Errorf("read advisory lock holder from pg_locks: %w", err)
	}
	return held, nil
}

// advisoryLockCatalogKey splits an advisory lock key into the (classid, objid)
// pair pg_locks reports for it: the high and low 32 bits, each widened back
// out of the unsigned OID the catalog stores.
func advisoryLockCatalogKey(key int64) (classID, objID int64) {
	return int64(uint32(uint64(key) >> 32)), int64(uint32(uint64(key)))
}

// sessionAffinityProbeName returns a lock name no other caller can be using.
func sessionAffinityProbeName() (string, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", fmt.Errorf("generate session affinity probe key: %w", err)
	}
	return sessionAffinityProbePrefix + hex.EncodeToString(nonce[:]), nil
}

// closeProbeConn returns a probe connection to the pool when its session is
// known to carry nothing, and otherwise discards it so the pool destroys it
// instead of handing out a session whose advisory-lock state is unknown. A
// discard that fails is reported rather than dropped, though it costs the
// caller nothing: the only thing a surviving session can still hold is the
// probe's random key, which nothing will ever ask for again.
//
// Marking a connection bad closes it, so only the clean path closes it here.
func closeProbeConn(ctx context.Context, conn *sql.Conn, clean bool) {
	if !clean {
		if err := discardConn(conn); err != nil {
			slog.WarnContext(ctx, "failed to discard a session affinity probe connection; it may return to the pool holding the probe lock",
				"error", err)
		}
		return
	}
	if err := conn.Close(); err != nil {
		slog.WarnContext(ctx, "failed to close a session affinity probe connection", "error", err)
	}
}

// discardConn marks conn bad so the pool destroys it rather than reusing a
// session whose advisory-lock state is uncertain; ending the session server
// side drops whatever lock is still on it.
func discardConn(conn *sql.Conn) error {
	if err := conn.Raw(func(any) error { return driver.ErrBadConn }); err != nil && !errors.Is(err, driver.ErrBadConn) {
		return err
	}
	return nil
}
