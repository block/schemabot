package mysqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/block/schemabot/pkg/storage"
)

type vitessApplyDataStore struct {
	db *sql.DB
}

func (s *vitessApplyDataStore) Save(ctx context.Context, data *storage.VitessApplyData) error {
	existingContexts, err := json.Marshal(data.ExistingMigrationCtxs)
	if err != nil {
		return fmt.Errorf("marshal existing migration contexts for apply %d: %w", data.ApplyID, err)
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO vitess_apply_data (apply_id, branch_name, deploy_request_id, migration_context, existing_migration_contexts, deploy_request_url, is_instant, deferred_deploy, revert_skipped_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			branch_name = VALUES(branch_name),
			deploy_request_id = VALUES(deploy_request_id),
			migration_context = VALUES(migration_context),
			existing_migration_contexts = VALUES(existing_migration_contexts),
			deploy_request_url = VALUES(deploy_request_url),
			is_instant = VALUES(is_instant),
			deferred_deploy = VALUES(deferred_deploy),
			revert_skipped_at = VALUES(revert_skipped_at)`,
		data.ApplyID, data.BranchName, data.DeployRequestID, data.MigrationContext, string(existingContexts), data.DeployRequestURL, data.IsInstant, data.DeferredDeploy, data.RevertSkippedAt,
	)
	if err != nil {
		return fmt.Errorf("save vitess apply data for apply %d: %w", data.ApplyID, err)
	}
	return nil
}

func (s *vitessApplyDataStore) GetByApplyID(ctx context.Context, applyID int64) (*storage.VitessApplyData, error) {
	var data storage.VitessApplyData
	var existingContexts sql.NullString
	err := s.db.QueryRowContext(ctx, `
		SELECT apply_id, branch_name, deploy_request_id, migration_context, existing_migration_contexts, deploy_request_url, is_instant, deferred_deploy, revert_skipped_at
		FROM vitess_apply_data WHERE apply_id = ?`, applyID,
	).Scan(&data.ApplyID, &data.BranchName, &data.DeployRequestID, &data.MigrationContext, &existingContexts, &data.DeployRequestURL, &data.IsInstant, &data.DeferredDeploy, &data.RevertSkippedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, storage.ErrVitessApplyDataNotFound
		}
		return nil, fmt.Errorf("get vitess apply data for apply %d: %w", applyID, err)
	}
	if existingContexts.Valid && existingContexts.String != "" {
		if err := json.Unmarshal([]byte(existingContexts.String), &data.ExistingMigrationCtxs); err != nil {
			return nil, fmt.Errorf("unmarshal existing migration contexts for apply %d: %w", applyID, err)
		}
	}
	return &data, nil
}
