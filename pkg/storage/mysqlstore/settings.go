// settings.go implements SettingsStore for admin-level runtime configuration.
package mysqlstore

import (
	"context"
	"database/sql"
	"errors"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
)

// settingColumns lists all columns for SELECT queries.
const settingColumns = `id, setting_key, setting_value, created_at, updated_at`

// settingsStore implements storage.SettingsStore using MySQL.
type settingsStore struct {
	db *sql.DB
}

// Get returns a setting by key, or nil if not found.
func (s *settingsStore) Get(ctx context.Context, key string) (*storage.Setting, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT `+settingColumns+`
		FROM settings
		WHERE setting_key = ?
	`, key)

	var setting storage.Setting
	err := row.Scan(&setting.ID, &setting.Key, &setting.Value, &setting.CreatedAt, &setting.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &setting, nil
}

// Set saves a setting. Creates if not exists, updates if exists.
func (s *settingsStore) Set(ctx context.Context, key, value string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO settings (setting_key, setting_value)
		VALUES (?, ?)
		ON DUPLICATE KEY UPDATE
			setting_value = VALUES(setting_value)
	`, key, value)
	return err
}

// List returns all settings.
func (s *settingsStore) List(ctx context.Context) ([]*storage.Setting, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+settingColumns+`
		FROM settings
		ORDER BY setting_key
	`)
	if err != nil {
		return nil, err
	}
	defer utils.CloseAndLog(rows)

	var settings []*storage.Setting
	for rows.Next() {
		var setting storage.Setting
		err := rows.Scan(&setting.ID, &setting.Key, &setting.Value, &setting.CreatedAt, &setting.UpdatedAt)
		if err != nil {
			return nil, err
		}
		settings = append(settings, &setting)
	}
	return settings, rows.Err()
}

// Delete removes a setting by key.
func (s *settingsStore) Delete(ctx context.Context, key string) error {
	result, err := s.db.ExecContext(ctx, `
		DELETE FROM settings WHERE setting_key = ?
	`, key)
	if err != nil {
		return err
	}

	return checkRowsAffected(result, storage.ErrSettingNotFound)
}
