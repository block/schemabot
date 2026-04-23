// Package apitypes defines the shared HTTP request and response types for SchemaBot's API.
// These types are used by both the server (pkg/api) and the CLI client (pkg/cmd/client).
// This package has zero dependencies — import it freely from any package.
package apitypes

// =============================================================================
// Error Codes
// =============================================================================

// Error codes returned in API responses. Clients should match on these
// constants rather than parsing error_message strings or HTTP status codes.
// Use IsRetryableErrorCode to determine whether a given code is retryable.
const (
	ErrCodeInvalidRequest     = "invalid_request"      // Malformed request (missing params, bad values)
	ErrCodeNotFound           = "not_found"            // Resource doesn't exist (unknown apply ID, database)
	ErrCodeDeploymentNotFound = "deployment_not_found" // No tern deployment configured for database/environment
	ErrCodeEngineError        = "engine_error"         // Schema change engine failure during execution
	ErrCodeStorageError       = "storage_error"        // Storage backend (MySQL) read/write failure
	ErrCodeEngineUnavailable  = "engine_unavailable"   // Schema change engine (Tern) unreachable or RPC error
	ErrCodeStateSyncFailed    = "state_sync_failed"    // Operation succeeded but local state sync failed
)

var retryableErrorCodes = map[string]bool{
	ErrCodeStorageError:      true,
	ErrCodeEngineUnavailable: true,
	ErrCodeStateSyncFailed:   true,
}

// IsRetryableErrorCode reports whether the given API error code represents a
// transient failure that clients should retry with backoff.
func IsRetryableErrorCode(code string) bool {
	return retryableErrorCodes[code]
}

// ErrorResponse is the standard error response body for non-200 HTTP responses.
// All error endpoints return this shape.
type ErrorResponse struct {
	Error     string `json:"error"`
	ErrorCode string `json:"error_code"`
}

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

// UnsafeChange represents a table change that is potentially destructive.
type UnsafeChange struct {
	Table      string
	Reason     string
	DDL        string
	ChangeType string
}

// UnsafeChanges returns all table changes marked as unsafe across all namespaces.
func (r *PlanResponse) UnsafeChanges() []UnsafeChange {
	var result []UnsafeChange
	for _, sc := range r.Changes {
		for _, t := range sc.TableChanges {
			if t.IsUnsafe {
				result = append(result, UnsafeChange{
					Table:      t.TableName,
					Reason:     t.UnsafeReason,
					DDL:        t.DDL,
					ChangeType: t.ChangeType,
				})
			}
		}
	}
	return result
}

// LintWarnings returns lint results with warning severity.
func (r *PlanResponse) LintWarnings() []LintViolationResponse {
	var result []LintViolationResponse
	for _, w := range r.LintResults {
		if w.Severity == "warning" {
			result = append(result, *w)
		}
	}
	return result
}

// LintInfos returns lint results with info severity.
func (r *PlanResponse) LintInfos() []LintViolationResponse {
	var result []LintViolationResponse
	for _, w := range r.LintResults {
		if w.Severity == "info" {
			result = append(result, *w)
		}
	}
	return result
}

// LintNonErrors returns lint results that don't block the apply (warning + info).
func (r *PlanResponse) LintNonErrors() []LintViolationResponse {
	return append(r.LintWarnings(), r.LintInfos()...)
}

// LintErrors returns lint results with error severity.
func (r *PlanResponse) LintErrors() []LintViolationResponse {
	var result []LintViolationResponse
	for _, w := range r.LintResults {
		if w.Severity == "error" {
			result = append(result, *w)
		}
	}
	return result
}

// FlatTables returns a flat list of all table changes across all namespaces.
func (r *PlanResponse) FlatTables() []*TableChangeResponse {
	var tables []*TableChangeResponse
	for _, sc := range r.Changes {
		tables = append(tables, sc.TableChanges...)
	}
	return tables
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

// LintViolationResponse represents a lint violation in the HTTP response.
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
	Summary      string                   `json:"summary,omitempty"`  // Combined status with ETA
	Volume       int32                    `json:"volume,omitempty"`   // Current volume setting (1-11)
	Options      map[string]string        `json:"options,omitempty"`  // Apply options (defer_cutover, skip_revert, etc.)
	Metadata     map[string]string        `json:"metadata,omitempty"` // Engine-specific data
}

// TableProgressResponse represents progress for a single table.
type TableProgressResponse struct {
	TableName       string                   `json:"table_name"`
	DDL             string                   `json:"ddl"`
	Keyspace        string                   `json:"keyspace,omitempty"`
	Status          string                   `json:"status"`
	RowsCopied      int64                    `json:"rows_copied"`
	RowsTotal       int64                    `json:"rows_total"`
	PercentComplete int32                    `json:"percent_complete"`
	ETASeconds      int64                    `json:"eta_seconds,omitempty"`
	IsInstant       bool                     `json:"is_instant,omitempty"`
	ProgressDetail  string                   `json:"progress_detail,omitempty"`
	TaskID          string                   `json:"task_id,omitempty"`
	StartedAt       string                   `json:"started_at,omitempty"`
	CompletedAt     string                   `json:"completed_at,omitempty"`
	Shards          []*ShardProgressResponse `json:"shards,omitempty"`
}

// ShardProgressResponse contains per-shard progress for Vitess schema changes.
type ShardProgressResponse struct {
	Shard           string `json:"shard"`
	Status          string `json:"status"`
	RowsCopied      int64  `json:"rows_copied"`
	RowsTotal       int64  `json:"rows_total"`
	ETASeconds      int64  `json:"eta_seconds,omitempty"`
	PercentComplete int32  `json:"percent_complete"`
	CutoverAttempts int32  `json:"cutover_attempts,omitempty"`
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
