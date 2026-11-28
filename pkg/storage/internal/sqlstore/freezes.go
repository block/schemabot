// freezes.go implements FreezeStore, the record of environment freezes.
package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
)

// freezeColumns lists all columns for SELECT queries.
const freezeColumns = `id, freeze_identifier, environment, scope_deployments, scope_databases, kind, running_policy,
	starts_at, ends_at, reason, created_by, source, external_id, lifted_by, lift_reason, lifted_at, created_at`

// freezeStore implements storage.FreezeStore. A freeze is in effect while it
// has started, has not reached its end time, and has not been lifted; the
// queries below compare those columns against the caller's time, so a
// planned freeze needs no timer to start or end.
type freezeStore struct {
	db         *rebindDB
	dialect    Dialect
	identity   identityInserter
	classifier ErrorClassifier
}

// Create records a freeze and returns it as stored.
func (s *freezeStore) Create(ctx context.Context, freeze *storage.EnvironmentFreeze) (*storage.EnvironmentFreeze, error) {
	if err := freeze.Validate(); err != nil {
		return nil, fmt.Errorf("create freeze: %w", err)
	}
	deployments, err := freezeScopeValue(freeze.Deployments)
	if err != nil {
		return nil, fmt.Errorf("encode deployments of freeze %s: %w", freeze.Identifier, err)
	}
	databases, err := freezeScopeValue(freeze.Databases)
	if err != nil {
		return nil, fmt.Errorf("encode databases of freeze %s: %w", freeze.Identifier, err)
	}
	environment := storage.CanonicalKey(freeze.Environment)
	// The freeze and the epoch bump commit together. The bump waits for any
	// admission write that holds the epoch row, so a change that checked
	// before this freeze is recorded before the freeze is acknowledged, and
	// one that checks after it sees the new epoch.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin create freeze %s of %s: %w", freeze.Identifier, environment, err)
	}
	defer rollbackTx(ctx, tx, "create freeze")
	id, err := s.identity.InsertID(ctx, tx, `
		INSERT INTO environment_freezes (
			freeze_identifier, environment, scope_deployments, scope_databases, kind, running_policy,
			starts_at, ends_at, reason, created_by, source, external_id
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, freeze.Identifier, environment, deployments, databases, freeze.Kind, freeze.Running,
		freezeTime(freeze.StartsAt), nullTimePtr(freezeTimePtr(freeze.EndsAt)), freeze.Reason, freeze.CreatedBy,
		freeze.Source, nullString(freeze.ExternalID))
	if err != nil {
		if s.classifier.IsDuplicateKey(err) {
			return nil, fmt.Errorf("create freeze %s of %s: %w", freeze.Identifier, environment, storage.ErrFreezeExists)
		}
		return nil, fmt.Errorf("create freeze %s of %s: %w", freeze.Identifier, environment, err)
	}
	upsert := s.dialect.UpsertClause([]string{"environment"}, []UpsertAssignment{
		{Column: "epoch", Expr: "freeze_epochs.epoch + 1"},
		{Column: "updated_at", Expr: s.dialect.CurrentTimestamp(TimestampPrecisionDefault)},
	})
	if _, err := tx.ExecContext(ctx, `INSERT INTO freeze_epochs (environment, epoch) VALUES (?, 1) `+upsert, environment); err != nil {
		return nil, fmt.Errorf("bump freeze epoch of %s for freeze %s: %w", environment, freeze.Identifier, err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit freeze %s of %s: %w", freeze.Identifier, environment, err)
	}
	created, err := s.getByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("read freeze %s after insert: %w", freeze.Identifier, err)
	}
	return created, nil
}

// Get returns the freeze with this identifier.
func (s *freezeStore) Get(ctx context.Context, identifier string) (*storage.EnvironmentFreeze, error) {
	freeze, err := scanFreeze(s.db.QueryRowContext(ctx, `
		SELECT `+freezeColumns+`
		FROM environment_freezes
		WHERE freeze_identifier = ?
	`, identifier))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, storage.ErrFreezeNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("read freeze %s: %w", identifier, err)
	}
	return freeze, nil
}

// Lift ends a freeze early. The update only matches a freeze that is neither
// lifted nor past its end time, so two concurrent lifts end it once; when it
// matches nothing, the row is read back to say why. The lift time is the
// database's clock, the same clock ListInEffect reads.
func (s *freezeStore) Lift(ctx context.Context, identifier, liftedBy, reason string) (*storage.EnvironmentFreeze, error) {
	if strings.TrimSpace(liftedBy) == "" {
		return nil, fmt.Errorf("lift freeze %s: the caller who lifts it is required", identifier)
	}
	now := s.dialect.CurrentTimestamp(TimestampPrecisionMicrosecond)
	result, err := s.db.ExecContext(ctx, `
		UPDATE environment_freezes
		SET lifted_by = ?, lift_reason = ?, lifted_at = `+now+`, updated_at = `+now+`
		WHERE freeze_identifier = ? AND lifted_at IS NULL AND (ends_at IS NULL OR ends_at > `+now+`)
	`, liftedBy, nullString(reason), identifier)
	if err != nil {
		return nil, fmt.Errorf("lift freeze %s: %w", identifier, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("lift freeze %s: read rows affected: %w", identifier, err)
	}
	freeze, err := s.Get(ctx, identifier)
	if err != nil {
		return nil, fmt.Errorf("read freeze %s after lifting it: %w", identifier, err)
	}
	if affected > 0 {
		return freeze, nil
	}
	if freeze.LiftedAt != nil {
		return nil, fmt.Errorf("lift freeze %s: lifted by %s at %s: %w",
			identifier, freeze.LiftedBy, freeze.LiftedAt.Format(time.RFC3339), storage.ErrFreezeAlreadyLifted)
	}
	return nil, fmt.Errorf("lift freeze %s: %w", identifier, storage.ErrFreezeEnded)
}

// ListInEffect returns the freezes in effect now, by the database's clock,
// oldest start first.
func (s *freezeStore) ListInEffect(ctx context.Context, environment string) ([]*storage.EnvironmentFreeze, error) {
	now := s.dialect.CurrentTimestamp(TimestampPrecisionMicrosecond)
	where, args := freezeEnvironmentFilter(environment)
	freezes, err := s.list(ctx, `
		SELECT `+freezeColumns+`
		FROM environment_freezes
		WHERE `+where+`lifted_at IS NULL AND starts_at <= `+now+` AND (ends_at IS NULL OR ends_at > `+now+`)
		ORDER BY starts_at, id
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("list freezes in effect (environment %q): %w", environment, err)
	}
	return freezes, nil
}

// List reads the current and recent freezes in one read-only, repeatable-read
// transaction, so both lists see the same snapshot. The database clock is read
// once inside it and bound into the current query, and returned for the
// caller to label freezes by.
func (s *freezeStore) List(ctx context.Context, environment string, recent int) (*storage.FreezeListing, error) {
	if recent <= 0 {
		return nil, fmt.Errorf("list freezes (environment %q): recent %d must be positive", environment, recent)
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("list freezes (environment %q): begin: %w", environment, err)
	}
	defer func() { _ = tx.Rollback() }()

	now, err := readFreezeClock(ctx, tx, s.dialect)
	if err != nil {
		return nil, fmt.Errorf("list freezes (environment %q): %w", environment, err)
	}
	where, args := freezeEnvironmentFilter(environment)
	current, err := listFreezes(ctx, tx, `
		SELECT `+freezeColumns+`
		FROM environment_freezes
		WHERE `+where+`lifted_at IS NULL AND (ends_at IS NULL OR ends_at > ?)
		ORDER BY starts_at, id
	`, append(slices.Clone(args), now)...)
	if err != nil {
		return nil, fmt.Errorf("list current freezes (environment %q): %w", environment, err)
	}
	recentFreezes, err := listFreezes(ctx, tx, `
		SELECT `+freezeColumns+`
		FROM environment_freezes
		WHERE `+where+`(lifted_at IS NOT NULL OR ends_at <= ?)
		ORDER BY starts_at DESC, id DESC
		LIMIT ?
	`, append(slices.Clone(args), now, recent)...)
	if err != nil {
		return nil, fmt.Errorf("list recent freezes (environment %q): %w", environment, err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("list freezes (environment %q): commit: %w", environment, err)
	}
	return &storage.FreezeListing{Now: now, Current: current, Recent: recentFreezes}, nil
}

// Now returns the database's clock.
func (s *freezeStore) Now(ctx context.Context) (time.Time, error) {
	return readFreezeClock(ctx, s.db, s.dialect)
}

// Epoch returns the environment's freeze epoch. An environment with no row
// is first recorded at epoch 0, outside any transaction, so the admission
// write that checks the guard finds a row to lock. Once the row exists, a
// read is a single SELECT.
func (s *freezeStore) Epoch(ctx context.Context, environment string) (int64, error) {
	environment = storage.CanonicalKey(environment)
	epoch, err := s.readEpoch(ctx, environment)
	if !errors.Is(err, sql.ErrNoRows) {
		return epoch, err
	}
	insertIfAbsent := s.dialect.InsertIfAbsent([]string{"environment"})
	if _, err := s.db.ExecContext(ctx, `
		INSERT`+insertIfAbsent.Modifier+` INTO freeze_epochs (environment, epoch)
		VALUES (?, 0)
		`+insertIfAbsent.Suffix+`
	`, environment); err != nil {
		return 0, fmt.Errorf("record freeze epoch of %s: %w", environment, err)
	}
	return s.readEpoch(ctx, environment)
}

// readEpoch reads the environment's freeze epoch. It returns sql.ErrNoRows,
// wrapped, when the environment has no row.
func (s *freezeStore) readEpoch(ctx context.Context, environment string) (int64, error) {
	var epoch int64
	if err := s.db.QueryRowContext(ctx, `SELECT epoch FROM freeze_epochs WHERE environment = ?`, environment).Scan(&epoch); err != nil {
		return 0, fmt.Errorf("read freeze epoch of %s: %w", environment, err)
	}
	return epoch, nil
}

// checkFreezeEpochGuard refuses an admission write with
// storage.ErrFreezeEpochChanged when ctx carries a FreezeEpochGuard whose
// epoch is no longer current. It share-locks the epoch row in tx, so a freeze
// being created in the environment waits until tx ends. A missing row means
// the guard was not read through Epoch, and is refused the same way. Without
// a guard there is nothing to check: the caller ran no change gate.
func checkFreezeEpochGuard(ctx context.Context, tx queryExecer) error {
	guard, ok := storage.FreezeEpochGuardFrom(ctx)
	if !ok {
		return nil
	}
	var epoch int64
	err := tx.QueryRowContext(ctx, `SELECT epoch FROM freeze_epochs WHERE environment = ? FOR SHARE`, guard.Environment).Scan(&epoch)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("check freeze epoch of %s: no epoch recorded: %w", guard.Environment, storage.ErrFreezeEpochChanged)
	}
	if err != nil {
		return fmt.Errorf("check freeze epoch of %s: %w: %w", guard.Environment, storage.ErrFreezeEpochUnreadable, err)
	}
	if epoch != guard.Epoch {
		return fmt.Errorf("check freeze epoch of %s: checked at %d, now %d: %w", guard.Environment, guard.Epoch, epoch, storage.ErrFreezeEpochChanged)
	}
	return nil
}

// readFreezeClock reads the database clock through q.
func readFreezeClock(ctx context.Context, q interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}, dialect Dialect) (time.Time, error) {
	var now sql.NullTime
	if err := q.QueryRowContext(ctx, `SELECT `+dialect.CurrentTimestamp(TimestampPrecisionMicrosecond)).Scan(&now); err != nil {
		return time.Time{}, fmt.Errorf("read database clock: %w", err)
	}
	if !now.Valid {
		return time.Time{}, errors.New("read database clock: got NULL")
	}
	return freezeTime(now.Time), nil
}

func (s *freezeStore) getByID(ctx context.Context, id int64) (*storage.EnvironmentFreeze, error) {
	return scanFreeze(s.db.QueryRowContext(ctx, `
		SELECT `+freezeColumns+`
		FROM environment_freezes
		WHERE id = ?
	`, id))
}

func (s *freezeStore) list(ctx context.Context, query string, args ...any) ([]*storage.EnvironmentFreeze, error) {
	return listFreezes(ctx, s.db, query, args...)
}

func listFreezes(ctx context.Context, q queryer, query string, args ...any) ([]*storage.EnvironmentFreeze, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer utils.CloseAndLog(rows)

	var freezes []*storage.EnvironmentFreeze
	for rows.Next() {
		freeze, err := scanFreeze(rows)
		if err != nil {
			return nil, err
		}
		freezes = append(freezes, freeze)
	}
	return freezes, rows.Err()
}

// freezeEnvironmentFilter returns the WHERE prefix and arguments that narrow
// a query to one environment, or nothing for every environment. A non-empty
// prefix ends in "AND " so the caller can append its own conditions.
func freezeEnvironmentFilter(environment string) (string, []any) {
	environment = storage.CanonicalKey(environment)
	if environment == "" {
		return "", nil
	}
	return "environment = ? AND ", []any{environment}
}

// freezeScopeValue renders a scope column value: the canonical entries as a
// JSON array, where an empty array means the whole environment. Validate
// already refused blank entries, so none is dropped here.
func freezeScopeValue(scope []string) (string, error) {
	canonical := make([]string, 0, len(scope))
	for _, entry := range scope {
		canonical = append(canonical, storage.CanonicalKey(entry))
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

// freezeTime normalizes a freeze time for the datetime(6) and timestamp
// columns: UTC, since not every SQL driver converts a zoned time.Time before
// binding it, and microseconds, the precision both columns keep.
func freezeTime(t time.Time) time.Time {
	return t.UTC().Truncate(time.Microsecond)
}

func freezeTimePtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	normalized := freezeTime(*t)
	return &normalized
}

func scanFreeze(row interface{ Scan(dest ...any) error }) (*storage.EnvironmentFreeze, error) {
	var (
		freeze      storage.EnvironmentFreeze
		deployments []byte
		databases   []byte
		endsAt      sql.NullTime
		externalID  sql.NullString
		liftedBy    sql.NullString
		liftReason  sql.NullString
		liftedAt    sql.NullTime
	)
	if err := row.Scan(&freeze.ID, &freeze.Identifier, &freeze.Environment, &deployments, &databases,
		&freeze.Kind, &freeze.Running, &freeze.StartsAt, &endsAt, &freeze.Reason, &freeze.CreatedBy,
		&freeze.Source, &externalID, &liftedBy, &liftReason, &liftedAt, &freeze.CreatedAt); err != nil {
		return nil, err
	}
	if err := unmarshalFreezeScope(deployments, &freeze.Deployments); err != nil {
		return nil, fmt.Errorf("decode deployments of freeze %s: %w", freeze.Identifier, err)
	}
	if err := unmarshalFreezeScope(databases, &freeze.Databases); err != nil {
		return nil, fmt.Errorf("decode databases of freeze %s: %w", freeze.Identifier, err)
	}
	freeze.StartsAt = freeze.StartsAt.UTC()
	if endsAt.Valid {
		at := endsAt.Time.UTC()
		freeze.EndsAt = &at
	}
	freeze.ExternalID = externalID.String
	freeze.LiftedBy = liftedBy.String
	freeze.LiftReason = liftReason.String
	if liftedAt.Valid {
		at := liftedAt.Time.UTC()
		freeze.LiftedAt = &at
	}
	return &freeze, nil
}

// unmarshalFreezeScope decodes a scope column, leaving an empty scope nil so
// a freeze of the whole environment reads back the same however it was set.
func unmarshalFreezeScope(raw []byte, scope *[]string) error {
	var entries []string
	if err := json.Unmarshal(raw, &entries); err != nil {
		return err
	}
	if len(entries) > 0 {
		*scope = entries
	}
	return nil
}
