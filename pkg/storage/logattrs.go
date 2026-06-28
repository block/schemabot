package storage

// LogAttrs returns the canonical slog key/value attributes for triaging this
// apply from logs alone: which apply, on which database and environment, and in
// what state. Append call-specific attributes after it, e.g.:
//
//	logger.Error("failed to drive apply", append(apply.LogAttrs(), "error", err)...)
//
// A nil receiver returns nil so callers on not-found paths can log safely.
func (a *Apply) LogAttrs() []any {
	if a == nil {
		return nil
	}
	return []any{
		"apply_id", a.ApplyIdentifier,
		"database", a.Database,
		"database_type", a.DatabaseType,
		"environment", a.Environment,
		"deployment", a.Deployment,
		"state", a.State,
	}
}

// LogAttrs returns the canonical triage attributes for an apply_operation. The
// parent apply's database and environment require a separate load, so when only
// the operation is in scope these identifiers (plus deployment and kind) are what
// pin down the stuck operation; once the parent apply is loaded, prefer
// Apply.LogAttrs for the database/environment context. A nil receiver returns nil.
func (op *ApplyOperation) LogAttrs() []any {
	if op == nil {
		return nil
	}
	return []any{
		"apply_operation_id", op.ID,
		"apply_db_id", op.ApplyID,
		"deployment", op.Deployment,
		"operation_kind", op.OperationKind,
		"state", op.State,
	}
}

// LogAttrs returns the canonical triage attributes for a task: which task, on
// which database and environment, which table, and in what state. A nil receiver
// returns nil.
func (t *Task) LogAttrs() []any {
	if t == nil {
		return nil
	}
	return []any{
		"task_id", t.TaskIdentifier,
		"apply_db_id", t.ApplyID,
		"database", t.Database,
		"database_type", t.DatabaseType,
		"environment", t.Environment,
		"table", t.TableName,
		"state", t.State,
	}
}
