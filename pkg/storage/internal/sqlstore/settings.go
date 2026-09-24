// settings.go implements SettingsStore for admin-level runtime configuration.
package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
)

// settingColumns lists all columns for SELECT queries.
const settingColumns = `id, setting_key, setting_value, created_at, updated_at`

// settingsStore implements storage.SettingsStore.
type settingsStore struct {
	db         *rebindDB
	dialect    Dialect
	classifier ErrorClassifier
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

// Set saves a setting. Creates if not exists, updates if exists. The conflict
// path stamps updated_at explicitly: the column reports when the setting was
// last written, and not every dialect renews it automatically on update.
func (s *settingsStore) Set(ctx context.Context, key, value string) error {
	upsert := s.dialect.UpsertClause(
		[]string{"setting_key"},
		[]UpsertAssignment{
			{Column: "setting_value"},
			{Column: "updated_at", Expr: s.dialect.CurrentTimestamp(TimestampPrecisionDefault)},
		},
	)
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO settings (setting_key, setting_value)
		VALUES (?, ?)
		`+upsert, key, value)
	return err
}

// CompareAndSet writes value only while the stored setting still matches
// previous. With no previous setting the UNIQUE key on setting_key arbitrates:
// the INSERT loser sees a duplicate-key error and reports a lost race. With a
// previous setting the UPDATE is predicated on the stored value, compared
// under the binary collation so two encodings that differ only in case or
// accent are not mistaken for the same value.
func (s *settingsStore) CompareAndSet(ctx context.Context, key string, previous *storage.Setting, value string) (bool, error) {
	if previous == nil {
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO settings (setting_key, setting_value)
			VALUES (?, ?)
		`, key, value)
		if err == nil {
			return true, nil
		}
		if s.classifier.IsDuplicateKey(err) {
			return false, nil
		}
		return false, fmt.Errorf("insert setting %s: %w", key, err)
	}

	result, err := s.db.ExecContext(ctx, `
		UPDATE settings
		SET setting_value = ?, updated_at = `+s.dialect.CurrentTimestamp(TimestampPrecisionDefault)+`
		WHERE setting_key = ? AND `+s.dialect.BinaryEquals("setting_value")+`
	`, value, key, previous.Value)
	if err != nil {
		return false, fmt.Errorf("compare-and-set setting %s: %w", key, err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read rows affected by compare-and-set of setting %s: %w", key, err)
	}
	if rowsAffected > 0 {
		return true, nil
	}
	// RowsAffected==0 is ambiguous: under MySQL's default changed-rows
	// semantics a matched row reports zero affected rows when the stored value
	// already equals the new one. Re-read to tell "already at value" from
	// "another writer moved it".
	current, err := s.Get(ctx, key)
	if err != nil {
		return false, fmt.Errorf("re-read setting %s after compare-and-set matched no row: %w", key, err)
	}
	if current == nil {
		return false, nil
	}
	return current.Value == value, nil
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
