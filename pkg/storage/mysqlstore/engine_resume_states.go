package mysqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/block/schemabot/pkg/storage"
)

type engineResumeStateStore struct {
	db *sql.DB
}

func (s *engineResumeStateStore) Save(ctx context.Context, resumeState *storage.EngineResumeState) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO engine_resume_states (apply_id, engine, migration_context, metadata)
		VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			engine = VALUES(engine),
			migration_context = VALUES(migration_context),
			metadata = VALUES(metadata)`,
		resumeState.ApplyID, resumeState.Engine, resumeState.MigrationContext, resumeState.Metadata,
	)
	if err != nil {
		return fmt.Errorf("save engine resume state for apply %d engine %s: %w", resumeState.ApplyID, resumeState.Engine, err)
	}
	return nil
}

func (s *engineResumeStateStore) GetByApplyID(ctx context.Context, applyID int64) (*storage.EngineResumeState, error) {
	var resumeState storage.EngineResumeState
	var migrationContext sql.NullString
	err := s.db.QueryRowContext(ctx, `
		SELECT id, apply_id, engine, migration_context, metadata, created_at, updated_at
		FROM engine_resume_states WHERE apply_id = ?`, applyID,
	).Scan(
		&resumeState.ID,
		&resumeState.ApplyID,
		&resumeState.Engine,
		&migrationContext,
		&resumeState.Metadata,
		&resumeState.CreatedAt,
		&resumeState.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, storage.ErrEngineResumeStateNotFound
		}
		return nil, fmt.Errorf("get engine resume state for apply %d: %w", applyID, err)
	}
	resumeState.MigrationContext = migrationContext.String
	return &resumeState, nil
}
