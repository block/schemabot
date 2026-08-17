package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/block/spirit/pkg/utils"

	"github.com/block/schemabot/pkg/storage"
)

const pendingDropColumns = `id, target, environment, database_name, original_table,
	quarantined_name, quarantined_at, run_id, engine, state, arrival_target,
	metadata, created_at, updated_at`

// pendingDropConflictColumns is the unique key that identifies one quarantined
// table within a deployment's ledger. A target cannot hold two tables under the
// same quarantined name, so a conflict always means the row is already recorded.
var pendingDropConflictColumns = []string{"target", "environment", "quarantined_name"}

type pendingDropStore struct {
	db      *rebindDB
	dialect Dialect
}

func (s *pendingDropStore) Record(ctx context.Context, drops []*storage.PendingDrop) error {
	if len(drops) == 0 {
		return nil
	}

	syntax := s.dialect.InsertIfAbsent(pendingDropConflictColumns)
	query := `INSERT` + syntax.Modifier + ` INTO pending_drops (
		target, environment, database_name, original_table,
		quarantined_name, quarantined_at, run_id, engine, state, arrival_target, metadata
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)` + syntax.Suffix

	// One statement per row rather than a multi-row VALUES list: the rows in a
	// single call are the tables of one RENAME, so the count is small, and
	// per-row statements keep an insert-if-absent conflict on one table from
	// deciding the outcome of its siblings.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin pending drops ledger transaction for %d table(s): %w", len(drops), err)
	}
	defer rollbackTx(ctx, tx, "record pending drops")

	for _, drop := range drops {
		canonicalizePendingDrop(drop)
		state := drop.State
		if state == "" {
			state = storage.PendingDropQuarantined
		}
		_, err := tx.ExecContext(ctx, query,
			drop.Target, drop.Environment, drop.DatabaseName, drop.OriginalTable,
			drop.QuarantinedName, drop.QuarantinedAt.UTC(), drop.RunID, drop.Engine,
			state, drop.ArrivalTarget, nullJSON(drop.Metadata),
		)
		if err != nil {
			return fmt.Errorf("record pending drop %s.%s as `%s` on target %s/%s: %w",
				drop.DatabaseName, drop.OriginalTable, drop.QuarantinedName,
				drop.Target, drop.Environment, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit pending drops ledger transaction for %d table(s): %w", len(drops), err)
	}
	return nil
}

func (s *pendingDropStore) LatestForTable(ctx context.Context, target, environment, databaseName, originalTable string) (*storage.PendingDrop, error) {
	environment = storage.CanonicalKey(environment)
	databaseName = storage.CanonicalKey(databaseName)
	row := s.db.QueryRowContext(ctx, `
		SELECT `+pendingDropColumns+`
		FROM pending_drops
		WHERE target = ? AND environment = ? AND database_name = ? AND original_table = ?
		ORDER BY quarantined_at DESC, id DESC
		LIMIT 1
	`, target, environment, databaseName, originalTable)
	drop, err := scanPendingDrop(row)
	if err != nil {
		return nil, fmt.Errorf("get latest pending drop for %s.%s on target %s/%s: %w",
			databaseName, originalTable, target, environment, err)
	}
	return drop, nil
}

func (s *pendingDropStore) ListExpired(ctx context.Context, cutoff time.Time, limit int) ([]*storage.PendingDrop, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+pendingDropColumns+`
		FROM pending_drops
		WHERE state = ? AND quarantined_at <= ?
		ORDER BY quarantined_at ASC, id ASC
		LIMIT ?
	`, storage.PendingDropQuarantined, cutoff.UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("list pending drops expired before %s: %w", cutoff.UTC().Format(time.RFC3339), err)
	}
	drops, err := scanPendingDrops(rows)
	if err != nil {
		return nil, fmt.Errorf("list pending drops expired before %s: %w", cutoff.UTC().Format(time.RFC3339), err)
	}
	return drops, nil
}

func (s *pendingDropStore) ListQuarantined(ctx context.Context, target, environment string) ([]*storage.PendingDrop, error) {
	environment = storage.CanonicalKey(environment)
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+pendingDropColumns+`
		FROM pending_drops
		WHERE target = ? AND environment = ? AND state = ?
		ORDER BY quarantined_at ASC, id ASC
	`, target, environment, storage.PendingDropQuarantined)
	if err != nil {
		return nil, fmt.Errorf("list quarantined pending drops on target %s/%s: %w", target, environment, err)
	}
	drops, err := scanPendingDrops(rows)
	if err != nil {
		return nil, fmt.Errorf("list quarantined pending drops on target %s/%s: %w", target, environment, err)
	}
	return drops, nil
}

func (s *pendingDropStore) SetState(ctx context.Context, ids []int64, state storage.PendingDropState) error {
	if len(ids) == 0 {
		return nil
	}
	args := append([]any{string(state)}, int64Args(ids)...)
	_, err := s.db.ExecContext(ctx, `
		UPDATE pending_drops
		SET state = ?, updated_at = `+s.dialect.CurrentTimestamp(TimestampPrecisionDefault)+`
		WHERE id IN (`+placeholders(len(ids))+`)
	`, args...)
	if err != nil {
		return fmt.Errorf("set %d pending drop row(s) to state %s: %w", len(ids), state, err)
	}
	return nil
}

func (s *pendingDropStore) Prune(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	// Terminal rows only: a quarantined row is still the proof an interrupted
	// DROP phase converges on, and deleting it would turn a completed change
	// into a fail-closed error on re-run.
	//
	// The bounded victim set is selected through a derived table rather than a
	// correlated subquery because MySQL refuses to read the delete's own target
	// table directly, and it is selected at all so one pass cannot lock an
	// unbounded number of rows.
	result, err := s.db.ExecContext(ctx, `
		DELETE FROM pending_drops
		WHERE id IN (
			SELECT id FROM (
				SELECT id FROM pending_drops
				WHERE state <> ? AND updated_at <= ?
				ORDER BY updated_at ASC, id ASC
				LIMIT ?
			) victims
		)
	`, storage.PendingDropQuarantined, cutoff.UTC(), limit)
	if err != nil {
		return 0, fmt.Errorf("prune terminal pending drop rows older than %s: %w", cutoff.UTC().Format(time.RFC3339), err)
	}
	pruned, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count pruned terminal pending drop rows older than %s: %w", cutoff.UTC().Format(time.RFC3339), err)
	}
	return pruned, nil
}

// canonicalizePendingDrop folds the row-identity strings a lookup compares
// byte-wise on PostgreSQL. The quarantined and original table names are left
// as given: they are real MySQL identifiers that must match the target
// server's own spelling, not SchemaBot row-identity keys.
func canonicalizePendingDrop(drop *storage.PendingDrop) {
	drop.Environment = storage.CanonicalKey(drop.Environment)
	drop.DatabaseName = storage.CanonicalKey(drop.DatabaseName)
}

func scanPendingDrops(rows *sql.Rows) ([]*storage.PendingDrop, error) {
	defer utils.CloseAndLog(rows)

	var drops []*storage.PendingDrop
	for rows.Next() {
		drop, err := scanPendingDrop(rows)
		if err != nil {
			return nil, err
		}
		drops = append(drops, drop)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending drop rows: %w", err)
	}
	return drops, nil
}

func scanPendingDrop(s scanner) (*storage.PendingDrop, error) {
	var drop storage.PendingDrop
	err := s.Scan(
		&drop.ID, &drop.Target, &drop.Environment, &drop.DatabaseName, &drop.OriginalTable,
		&drop.QuarantinedName, &drop.QuarantinedAt, &drop.RunID, &drop.Engine, &drop.State,
		&drop.ArrivalTarget, &drop.Metadata, &drop.CreatedAt, &drop.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &drop, nil
}
