package storage

// LogAttrs returns the canonical slog key/value attributes for triaging this
// apply from logs alone: which apply, on which database and environment, and in
// what state. Append call-specific attributes after it, e.g.:
//
//	logger.Error("failed to drive apply", append(apply.LogAttrs(), "error", err)...)
//
// A nil receiver returns nil so callers on not-found paths can log safely.
//
// apply_id is the searchable string identifier; the internal numeric row ID is
// deliberately not logged so it can't be confused with the user-facing id.
// repo, pr, external_id (the remote data plane's identifier for this apply), and
// caller (who initiated the apply) are included only when set, so operators can
// tie the apply to its PR, correlate to the data plane, and see who triggered it.
func (a *Apply) LogAttrs() []any {
	if a == nil {
		return nil
	}
	attrs := []any{
		"apply_id", a.ApplyIdentifier,
		"database", a.Database,
		"database_type", a.DatabaseType,
		"environment", a.Environment,
		"deployment", a.Deployment,
		"state", a.State,
	}
	if a.Repository != "" {
		attrs = append(attrs, "repo", a.Repository)
	}
	if a.PullRequest > 0 {
		attrs = append(attrs, "pr", a.PullRequest)
	}
	if a.ExternalID != "" {
		attrs = append(attrs, "external_id", a.ExternalID)
	}
	if a.Caller != "" {
		attrs = append(attrs, "caller", a.Caller)
	}
	return attrs
}

// LogAttrs returns the canonical triage attributes for an apply_operation. The
// parent apply's database and environment require a separate load, so when only
// the operation is in scope these identifiers (plus deployment and kind) are what
// pin down the stuck operation; once the parent apply is loaded, prefer
// Apply.LogAttrs for the database/environment context. A nil receiver returns nil.
//
// deployment here is the operation's own (authoritative routing) deployment.
// external_id (the remote data plane's apply identifier for this operation) and
// external_operation_id (the remote data plane's operation handle) are included
// only when set, so operators can correlate a stuck operation to the data plane.
func (op *ApplyOperation) LogAttrs() []any {
	if op == nil {
		return nil
	}
	attrs := []any{
		"apply_operation_id", op.ID,
		"deployment", op.Deployment,
		"operation_kind", op.OperationKind,
		"state", op.State,
	}
	if op.ExternalID != "" {
		attrs = append(attrs, "external_id", op.ExternalID)
	}
	if op.ExternalOperationID != "" {
		attrs = append(attrs, "external_operation_id", op.ExternalOperationID)
	}
	return attrs
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
		"database", t.Database,
		"database_type", t.DatabaseType,
		"environment", t.Environment,
		"table", t.TableName,
		"state", t.State,
	}
}
