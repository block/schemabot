// Package apitypes defines the shared HTTP request and response types for SchemaBot's API.
// These types are used by both the server (pkg/api) and the CLI client (pkg/cmd/client).
// This package has zero dependencies — import it freely from any package.
package apitypes

// =============================================================================
// Error Codes
// =============================================================================

// Machine-readable error codes returned in API responses. Clients should
// match on these constants rather than parsing error_message strings.
const (
	// ErrCodeStateSyncFailed indicates the operation succeeded on the backend
	// but local state synchronization failed. Status and progress endpoints
	// may show stale state until the next recovery cycle.
	ErrCodeStateSyncFailed = "state_sync_failed"

	// ErrCodeEngineError indicates the schema change engine (Spirit, PlanetScale)
	// encountered a failure during execution.
	ErrCodeEngineError = "engine_error"
)

// =============================================================================
// Request Types
// =============================================================================

// SchemaFiles contains the schema files for a namespace (schema name for MySQL,
// keyspace for Vitess). This is a lightweight equivalent of ternv1.SchemaFiles
// that avoids pulling in proto dependencies.
type SchemaFiles struct {
	Files map[string]string `json:"files,omitempty"`
}

// PlanRequest is the HTTP request body for POST /api/plan.
type PlanRequest struct {
	Database    string                  `json:"database"`
	Deployment  string                  `json:"deployment,omitempty"`
	Environment string                  `json:"environment"`
	Type        string                  `json:"type"`
	SchemaFiles map[string]*SchemaFiles `json:"schema_files"`
	Repository  string                  `json:"repository,omitempty"`
	PullRequest *int32                  `json:"pull_request,omitempty"`
	Target      string                  `json:"target,omitempty"`
}

// ApplyRequest is the HTTP request body for POST /api/apply.
type ApplyRequest struct {
	PlanID      string            `json:"plan_id"`
	Database    string            `json:"database,omitempty"`
	Deployment  string            `json:"deployment,omitempty"`
	Environment string            `json:"environment"`
	Caller      string            `json:"caller,omitempty"`
	Options     map[string]string `json:"options,omitempty"`
	Target      string            `json:"target,omitempty"`
}

// ControlRequest is the HTTP request body for control operations
// (stop, start, cutover, revert, skip-revert).
type ControlRequest struct {
	Database    string `json:"database"`
	Deployment  string `json:"deployment,omitempty"`
	Environment string `json:"environment"`
	ApplyID     string `json:"apply_id,omitempty"`
}

// VolumeRequest is the HTTP request body for POST /api/volume.
type VolumeRequest struct {
	ApplyID     string `json:"apply_id,omitempty"`
	Database    string `json:"database"`
	Deployment  string `json:"deployment,omitempty"`
	Environment string `json:"environment"`
	Volume      int32  `json:"volume"`
}

// =============================================================================
// Response Types
// =============================================================================

// PlanResponse is the HTTP response for POST /api/plan.
type PlanResponse struct {
	PlanID       string                   `json:"plan_id"`
	Database     string                   `json:"database,omitempty"`
	DatabaseType string                   `json:"database_type,omitempty"`
	Environment  string                   `json:"environment,omitempty"`
	Engine       string                   `json:"engine"`
	Changes      []*SchemaChangeResponse  `json:"changes"`
	LintResults  []*LintViolationResponse `json:"lint_violations"`
	Errors       []string                 `json:"errors"`
}

// HasErrors returns true if any lint result has error severity.
func (r *PlanResponse) HasErrors() bool {
	for _, w := range r.LintResults {
		if w.Severity == "error" {
			return true
		}
	}
	return false
}

// FlatTables returns a flat list of all table changes across all namespaces.
func (r *PlanResponse) FlatTables() []*TableChangeResponse {
	var tables []*TableChangeResponse
	for _, sc := range r.Changes {
		tables = append(tables, sc.TableChanges...)
	}
	return tables
}

// UnsafeChange represents a destructive schema change extracted from a plan.
type UnsafeChange struct {
	Table      string
	Reason     string
	ChangeType string
}

// UnsafeChanges extracts all table changes marked as unsafe (DROP TABLE, DROP COLUMN, etc.).
func (r *PlanResponse) UnsafeChanges() []UnsafeChange {
	var changes []UnsafeChange
	for _, tbl := range r.FlatTables() {
		if !tbl.IsUnsafe {
			continue
		}
		changes = append(changes, UnsafeChange{
			Table:      tbl.TableName,
			Reason:     tbl.UnsafeReason,
			ChangeType: tbl.ChangeType,
		})
	}
	return changes
}

// LintViolation represents a lint warning extracted from a plan response.
type LintViolation struct {
	Message string
	Table   string
	Linter  string
}

// LintViolations returns non-error lint results (warning and info severity).
// Error-severity results represent unsafe/blocking changes and are shown
// separately via UnsafeChanges().
func (r *PlanResponse) LintViolations() []LintViolation {
	var warnings []LintViolation
	for _, w := range r.LintResults {
		if w.Severity == "error" {
			continue
		}
		warnings = append(warnings, LintViolation{
			Message: w.Message,
			Table:   w.Table,
			Linter:  w.Linter,
		})
	}
	return warnings
}

// LintErrors returns error-severity lint results (unsafe/blocking changes).
func (r *PlanResponse) LintErrors() []LintViolation {
	var warnings []LintViolation
	for _, w := range r.LintResults {
		if w.Severity != "error" {
			continue
		}
		warnings = append(warnings, LintViolation{
			Message: w.Message,
			Table:   w.Table,
			Linter:  w.Linter,
		})
	}
	return warnings
}

// SchemaChangeResponse groups changes for a single namespace.
type SchemaChangeResponse struct {
	Namespace    string                 `json:"namespace"`
	TableChanges []*TableChangeResponse `json:"table_changes,omitempty"`
	Metadata     map[string]string      `json:"metadata,omitempty"` // Engine-specific data (e.g., "vschema" → diff)
}

// TableChangeResponse represents a DDL change in the HTTP response.
type TableChangeResponse struct {
	TableName    string `json:"table_name"`
	Namespace    string `json:"namespace,omitempty"`
	DDL          string `json:"ddl"`
	ChangeType   string `json:"change_type"`
	IsUnsafe     bool   `json:"is_unsafe,omitempty"`
	UnsafeReason string `json:"unsafe_reason,omitempty"`
}

// GetTableName implements ddl.TableWithName for filtering Spirit internal tables.
func (t *TableChangeResponse) GetTableName() string { return t.TableName }

// LintViolationResponse represents a lint warning in the HTTP response.
type LintViolationResponse struct {
	Message  string `json:"message"`
	Table    string `json:"table,omitempty"`
	Column   string `json:"column,omitempty"`
	Linter   string `json:"linter,omitempty"`
	Severity string `json:"severity,omitempty"` // "error", "warning", or "info"
	FixType  string `json:"fix_type,omitempty"`
}

// ApplyResponse is the HTTP response for POST /api/apply.
type ApplyResponse struct {
	Accepted     bool   `json:"accepted"`
	ApplyID      string `json:"apply_id,omitempty"`
	ErrorCode    string `json:"error_code,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
}

// ControlResponse is the HTTP response for simple control operations
// (cutover, revert, skip-revert) that return accepted + optional error.
type ControlResponse struct {
	Accepted     bool   `json:"accepted"`
	ErrorMessage string `json:"error_message,omitempty"`
}

// StopResponse is the HTTP response for POST /api/stop.
type StopResponse struct {
	Accepted     bool   `json:"accepted"`
	ErrorMessage string `json:"error_message,omitempty"`
	StoppedCount int64  `json:"stopped_count"`
	SkippedCount int64  `json:"skipped_count"`
}

// StartResponse is the HTTP response for POST /api/start.
type StartResponse struct {
	Accepted     bool   `json:"accepted"`
	ErrorCode    string `json:"error_code,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
	SkippedCount int64  `json:"skipped_count"`
	StartedCount int64  `json:"started_count"`
}

// VolumeResponse is the HTTP response for POST /api/volume.
type VolumeResponse struct {
	Accepted       bool   `json:"accepted"`
	ErrorMessage   string `json:"error_message,omitempty"`
	PreviousVolume int32  `json:"previous_volume"`
	NewVolume      int32  `json:"new_volume"`
}

// ProgressResponse is the HTTP response for GET /api/progress/{database}.
type ProgressResponse struct {
	State        string                   `json:"state"`
	Engine       string                   `json:"engine"`
	ApplyID      string                   `json:"apply_id,omitempty"`
	Database     string                   `json:"database,omitempty"`     // Included in apply-id lookups
	Environment  string                   `json:"environment,omitempty"`  // Included in apply-id lookups
	Caller       string                   `json:"caller,omitempty"`       // Included in apply-id lookups
	PullRequest  string                   `json:"pull_request,omitempty"` // PR URL (blank for CLI context)
	StartedAt    string                   `json:"started_at,omitempty"`
	CompletedAt  string                   `json:"completed_at,omitempty"`
	Tables       []*TableProgressResponse `json:"tables,omitempty"`
	ErrorCode    string                   `json:"error_code,omitempty"`
	ErrorMessage string                   `json:"error_message,omitempty"`
	Summary      string                   `json:"summary,omitempty"` // Combined status with ETA
	Volume       int32                    `json:"volume,omitempty"`  // Current volume setting (1-11)
}

// TableProgressResponse represents progress for a single table.
type TableProgressResponse struct {
	TableName       string `json:"table_name"`
	DDL             string `json:"ddl"`
	Status          string `json:"status"`
	RowsCopied      int64  `json:"rows_copied"`
	RowsTotal       int64  `json:"rows_total"`
	PercentComplete int32  `json:"percent_complete"`
	ETASeconds      int64  `json:"eta_seconds,omitempty"`
	IsInstant       bool   `json:"is_instant,omitempty"`
	ProgressDetail  string `json:"progress_detail,omitempty"` // e.g., "12.5% copyRows ETA 1h 30m"
	TaskID          string `json:"task_id,omitempty"`
}

// GetTableName implements ddl.TableWithName for filtering Spirit internal tables.
func (t *TableProgressResponse) GetTableName() string { return t.TableName }

// ApplyHistoryResponse represents a single apply in the history.
type ApplyHistoryResponse struct {
	ApplyID     string `json:"apply_id"`
	Caller      string `json:"caller"`
	CompletedAt string `json:"completed_at,omitempty"`
	Engine      string `json:"engine"`
	Environment string `json:"environment"`
	Error       string `json:"error,omitempty"`
	ErrorCode   string `json:"error_code,omitempty"`
	StartedAt   string `json:"started_at,omitempty"`
	State       string `json:"state"`
}

// DatabaseHistoryResponse is the response for GET /api/history/{database}.
type DatabaseHistoryResponse struct {
	Database string                  `json:"database"`
	Applies  []*ApplyHistoryResponse `json:"applies"`
}

// ActiveApplyResponse represents a schema change in the status list.
type ActiveApplyResponse struct {
	ApplyID     string `json:"apply_id"`
	Database    string `json:"database"`
	Environment string `json:"environment"`
	State       string `json:"state"`
	Engine      string `json:"engine"`
	Caller      string `json:"caller"`
	StartedAt   string `json:"started_at,omitempty"`
	CompletedAt string `json:"completed_at,omitempty"`
	UpdatedAt   string `json:"updated_at"`
	Volume      int    `json:"volume,omitempty"`
}

// StatusResponse is the HTTP response for GET /api/status.
type StatusResponse struct {
	ActiveCount int                    `json:"active_count"`
	Applies     []*ActiveApplyResponse `json:"applies"`
}
