// freezes.go implements FreezeStore, the record of environment freezes.
package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
)

// freezeColumns lists all columns for SELECT queries.
const freezeColumns = `id, freeze_identifier, environment, scope_deployments, scope_databases, kind, override_rule,
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
	id, err := s.identity.InsertID(ctx, s.db, `
		INSERT INTO environment_freezes (
			freeze_identifier, environment, scope_deployments, scope_databases, kind, override_rule,
			starts_at, ends_at, reason, created_by, source, external_id
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, freeze.Identifier, environment, deployments, databases, freeze.Kind, freeze.Override,
		freezeTime(freeze.StartsAt), nullTimePtr(freezeTimePtr(freeze.EndsAt)), freeze.Reason, freeze.CreatedBy,
		freeze.Source, nullString(freeze.ExternalID))
	if err != nil {
		if s.classifier.IsDuplicateKey(err) {
			return nil, fmt.Errorf("create freeze %s of %s: %w", freeze.Identifier, environment, storage.ErrFreezeExists)
		}
		return nil, fmt.Errorf("create freeze %s of %s: %w", freeze.Identifier, environment, err)
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
// matches nothing, the row is read back to say why.
func (s *freezeStore) Lift(ctx context.Context, identifier, liftedBy, reason string, at time.Time) (*storage.EnvironmentFreeze, error) {
	if strings.TrimSpace(liftedBy) == "" {
		return nil, fmt.Errorf("lift freeze %s: the caller who lifts it is required", identifier)
	}
	at = freezeTime(at)
	now := s.dialect.CurrentTimestamp(TimestampPrecisionDefault)
	result, err := s.db.ExecContext(ctx, `
		UPDATE environment_freezes
		SET lifted_by = ?, lift_reason = ?, lifted_at = ?, updated_at = `+now+`
		WHERE freeze_identifier = ? AND lifted_at IS NULL AND (ends_at IS NULL OR ends_at > ?)
	`, liftedBy, nullString(reason), at, identifier, at)
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

// ListInEffect returns the freezes in effect at at, oldest start first.
func (s *freezeStore) ListInEffect(ctx context.Context, environment string, at time.Time) ([]*storage.EnvironmentFreeze, error) {
	at = freezeTime(at)
	where, args := freezeEnvironmentFilter(environment)
	freezes, err := s.list(ctx, `
		SELECT `+freezeColumns+`
		FROM environment_freezes
		WHERE `+where+`lifted_at IS NULL AND starts_at <= ? AND (ends_at IS NULL OR ends_at > ?)
		ORDER BY starts_at, id
	`, append(args, at, at)...)
	if err != nil {
		return nil, fmt.Errorf("list freezes in effect (environment %q): %w", environment, err)
	}
	return freezes, nil
}

func (s *freezeStore) getByID(ctx context.Context, id int64) (*storage.EnvironmentFreeze, error) {
	return scanFreeze(s.db.QueryRowContext(ctx, `
		SELECT `+freezeColumns+`
		FROM environment_freezes
		WHERE id = ?
	`, id))
}

func (s *freezeStore) list(ctx context.Context, query string, args ...any) ([]*storage.EnvironmentFreeze, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
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
// JSON array, where an empty array means the whole environment.
func freezeScopeValue(scope []string) (string, error) {
	canonical := make([]string, 0, len(scope))
	for _, entry := range scope {
		if key := storage.CanonicalKey(entry); key != "" {
			canonical = append(canonical, key)
		}
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
		&freeze.Kind, &freeze.Override, &freeze.StartsAt, &endsAt, &freeze.Reason, &freeze.CreatedBy,
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
