package mysqlstore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/block/schemabot/pkg/storage"
	"github.com/block/spirit/pkg/utils"
)

type vitessTaskStore struct {
	db *sql.DB
}

func (s *vitessTaskStore) Create(ctx context.Context, task *storage.VitessTask) error {
	result, err := s.db.ExecContext(ctx,
		`INSERT INTO vitess_tasks (apply_id, keyspace, task_type, state, created_at, updated_at)
		 VALUES (?, ?, ?, ?, NOW(), NOW())`,
		task.ApplyID, task.Keyspace, task.TaskType, task.State,
	)
	if err != nil {
		return fmt.Errorf("insert vitess_task: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("get vitess_task id: %w", err)
	}
	task.ID = id
	return nil
}

func (s *vitessTaskStore) GetByApplyID(ctx context.Context, applyID int64) ([]*storage.VitessTask, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, apply_id, keyspace, task_type, state, created_at, updated_at
		 FROM vitess_tasks WHERE apply_id = ? ORDER BY id`,
		applyID,
	)
	if err != nil {
		return nil, fmt.Errorf("query vitess_tasks: %w", err)
	}
	defer utils.CloseAndLog(rows)

	var tasks []*storage.VitessTask
	for rows.Next() {
		t := &storage.VitessTask{}
		if err := rows.Scan(&t.ID, &t.ApplyID, &t.Keyspace, &t.TaskType, &t.State, &t.CreatedAt, &t.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan vitess_task: %w", err)
		}
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}

func (s *vitessTaskStore) UpdateState(ctx context.Context, id int64, state string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE vitess_tasks SET state = ?, updated_at = NOW() WHERE id = ?`,
		state, id,
	)
	if err != nil {
		return fmt.Errorf("update vitess_task state: %w", err)
	}
	return nil
}

// Ensure vitessTaskStore implements VitessTaskStore at compile time.
var _ storage.VitessTaskStore = (*vitessTaskStore)(nil)
