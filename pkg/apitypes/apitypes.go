// Package apitypes defines the shared HTTP request and response types for SchemaBot's API.
// These types are used by both the server (pkg/api) and the CLI client (pkg/cmd/client).
// This package has zero dependencies — import it freely from any package.
package apitypes

import "strings"

// =============================================================================
// Error Codes
// =============================================================================

// Error codes returned in API responses. Clients should match on these
// constants rather than parsing error_message strings or HTTP status codes.
// Use IsRetryableErrorCode to determine whether a given code is retryable.
const (
	ErrCodeInvalidRequest       = "invalid_request"        // Malformed request (missing params, bad values)
	ErrCodeNotFound             = "not_found"              // Resource doesn't exist (unknown apply ID, database)
	ErrCodeDeploymentNotFound   = "deployment_not_found"   // No tern deployment configured for database/environment
	ErrCodeEngineError          = "engine_error"           // Schema change engine failure during execution
	ErrCodeEngineErrorRetryable = "engine_error_retryable" // Schema change engine failure that may recover on retry
	ErrCodeStorageError         = "storage_error"          // Storage backend (MySQL) read/write failure
	ErrCodeEngineUnavailable    = "engine_unavailable"     // Schema change engine (Tern) unreachable or RPC error
	ErrCodeStateSyncFailed      = "state_sync_failed"      // Operation succeeded but local state sync failed
	ErrCodeActiveApplyExists    = "active_apply_exists"    // Another active apply already exists for the target
	ErrCodeSourcePolicyDenied   = "source_policy_denied"   // Source repo/path is not authorized for the database
)

var retryableErrorCodes = map[string]bool{
	ErrCodeEngineErrorRetryable: true,
	ErrCodeStorageError:         true,
	ErrCodeEngineUnavailable:    true,
	ErrCodeStateSyncFailed:      true,
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

// PulledNamespace contains live schema content for a namespace (schema name for
// MySQL, keyspace for Vitess). It intentionally describes database objects, not
// repository filenames; clients decide how to materialize tables and artifacts.
type PulledNamespace struct {
	Tables           map[string]string        `json:"tables,omitempty"`
	Artifacts        map[string]string        `json:"artifacts,omitempty"`
	NamespaceCatalog *NamespaceCatalog        `json:"namespace_catalog,omitempty"`
	TableCatalog     map[string]*TableCatalog `json:"table_catalog,omitempty"`
}

// NamespaceCatalog contains structured metadata for a pulled namespace.
type NamespaceCatalog struct {
	Name       string `json:"name"`
	Engine     string `json:"engine"`
	TableCount int32  `json:"table_count"`
}

// TableCatalog contains structured metadata for a pulled table or view.
type TableCatalog struct {
	Name    string           `json:"name"`
	Kind    string           `json:"kind"`
	Comment string           `json:"comment,omitempty"`
	Columns []*ColumnCatalog `json:"columns,omitempty"`
	Indexes []*IndexCatalog  `json:"indexes,omitempty"`
	// EstimatedRowCount and DataSizeBytes are engine-maintained estimates
	// (from information_schema for MySQL) and may be stale until statistics
	// are refreshed; they are not exact counts.
	EstimatedRowCount int64                `json:"estimated_row_count,omitempty"`
	DataSizeBytes     int64                `json:"data_size_bytes,omitempty"`
	ForeignKeys       []*ForeignKeyCatalog `json:"foreign_keys,omitempty"`
}

// ColumnCatalog contains structured metadata for a pulled table column.
type ColumnCatalog struct {
	Name          string `json:"name"`
	Type          string `json:"type"`
	Nullable      bool   `json:"nullable"`
	DefaultValue  string `json:"default_value,omitempty"`
	Comment       string `json:"comment,omitempty"`
	AutoIncrement bool   `json:"auto_increment,omitempty"`
	Generated     bool   `json:"generated,omitempty"`
}

// IndexCatalog contains structured metadata for a pulled table index.
type IndexCatalog struct {
	Name    string   `json:"name"`
	Primary bool     `json:"primary,omitempty"`
	Unique  bool     `json:"unique,omitempty"`
	Parts   []string `json:"parts,omitempty"`
}

// ForeignKeyCatalog contains structured metadata for a foreign-key constraint.
type ForeignKeyCatalog struct {
	Name              string   `json:"name"`
	Columns           []string `json:"columns,omitempty"`
	ReferencedTable   string   `json:"referenced_table"`
	ReferencedColumns []string `json:"referenced_columns,omitempty"`
	OnUpdate          string   `json:"on_update,omitempty"`
	OnDelete          string   `json:"on_delete,omitempty"`
}

// PullSchemaRequest is the HTTP request body for POST /api/pull.
type PullSchemaRequest struct {
	Database      string   `json:"database"`
	Environment   string   `json:"environment"`
	Type          string   `json:"type"`
	Namespaces    []string `json:"namespaces,omitempty"`
	CatalogDetail string   `json:"catalog_detail,omitempty"`
}

// PullSchemaResponse is the HTTP response body for POST /api/pull.
type PullSchemaResponse struct {
	Database    string                      `json:"database"`
	Type        string                      `json:"type"`
	Environment string                      `json:"environment"`
	Namespaces  map[string]*PulledNamespace `json:"namespaces"`
	TableCount  int32                       `json:"table_count"`
}

// DatabaseListResponse is the HTTP response body for GET /api/databases.
type DatabaseListResponse struct {
	Databases []*DatabaseResponse `json:"databases"`
}

// DatabaseResponse describes one server-side database without
// exposing connection strings, opaque execution targets, or endpoint addresses.
type DatabaseResponse struct {
	Database     string                         `json:"database"`
	Type         string                         `json:"type"`
	Environments []*DatabaseEnvironmentResponse `json:"environments"`
}

// DatabaseEnvironmentResponse describes one configured database environment
// without exposing connection strings, opaque execution targets, or endpoints.
type DatabaseEnvironmentResponse struct {
	Environment string   `json:"environment"`
	Deployments []string `json:"deployments,omitempty"`
}

// PlanRequest is the HTTP request body for POST /api/plan.
type PlanRequest struct {
	Database    string                  `json:"database"`
	Environment string                  `json:"environment"`
	Type        string                  `json:"type"`
	SchemaFiles map[string]*SchemaFiles `json:"schema_files"`
	Repository  string                  `json:"repository,omitempty"`
	PullRequest *int32                  `json:"pull_request,omitempty"`
	// HeadSHA is the PR HEAD SHA at the time the schema files were discovered.
	// Persisted on the plan record and used at apply-confirm time to detect the
	// cross-delivery race where HEAD advances between plan and confirm.
	// Optional — absent for non-webhook callers (e.g. CLI plan invocations without a PR).
	HeadSHA *string `json:"head_sha,omitempty"`
}

// ApplyRequest is the HTTP request body for POST /api/apply.
type ApplyRequest struct {
	PlanID      string            `json:"plan_id"`
	Environment string            `json:"environment"`
	Caller      string            `json:"caller,omitempty"`
	Options     map[string]string `json:"options,omitempty"`
}

// ControlRequest is the HTTP request body for control operations
// (stop, start, cutover, revert, skip-revert).
type ControlRequest struct {
	Environment string `json:"environment"`
	ApplyID     string `json:"apply_id"`
	Caller      string `json:"caller,omitempty"`
}

// VolumeRequest is the HTTP request body for POST /api/volume.
type VolumeRequest struct {
	ApplyID     string `json:"apply_id"`
	Environment string `json:"environment"`
	Volume      int32  `json:"volume"`
}

// =============================================================================
// Response Types
// =============================================================================

// PlanResponse is the HTTP response for POST /api/plan.
type PlanResponse struct {
	PlanID       string `json:"plan_id"`
	Database     string `json:"database,omitempty"`
	DatabaseType string `json:"database_type,omitempty"`
	Environment  string `json:"environment,omitempty"`
	// Deployment is the primary deployment this plan was created against
	// (rollout index 0 at plan time). The review-time drift rollup carries it
	// forward so it can verify the plan's baseline still maps to the primary at
	// rollup time, rather than trusting that current config re-resolves the same
	// primary.
	Deployment  string                   `json:"deployment,omitempty"`
	Engine      string                   `json:"engine"`
	Changes     []*SchemaChangeResponse  `json:"changes"`
	LintResults []*LintViolationResponse `json:"lint_violations"`
	Errors      []string                 `json:"errors"`
	// Shards carries the per-shard plan for a sharded engine: each changing shard
	// and the changes it needs. The namespace-level Changes above collapse a
	// keyspace to one entry, so a keyspace whose shards diverge is represented
	// faithfully only here. Empty for non-sharded plans.
	Shards []*ShardPlanResponse `json:"shards,omitempty"`
}

// ShardPlanResponse is one changing shard's plan: the keyspace it belongs to and
// the table changes that shard needs.
type ShardPlanResponse struct {
	Namespace string                 `json:"namespace,omitempty"`
	Shard     string                 `json:"shard"`
	Changes   []*TableChangeResponse `json:"changes,omitempty"`
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

// UnsafeChanges returns all table changes marked as unsafe across all
// namespaces. DROP table changes are treated as unsafe even when an engine omits
// IsUnsafe, so destructive table deletion fails closed.
func (r *PlanResponse) UnsafeChanges() []UnsafeChange {
	if r == nil {
		return nil
	}
	var result []UnsafeChange
	for _, sc := range r.Changes {
		if sc == nil {
			continue
		}
		for _, t := range sc.TableChanges {
			if unsafeChange, ok := t.UnsafeChange(); ok {
				result = append(result, unsafeChange)
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

// UnsafeChange returns the unsafe-change view for table changes that require
// explicit operator opt-in. Engines should mark unsafe table changes directly;
// the drop fallback keeps table deletion fail-closed if an engine omits that
// metadata.
func (t *TableChangeResponse) UnsafeChange() (UnsafeChange, bool) {
	if t == nil {
		return UnsafeChange{}, false
	}
	if !t.IsUnsafe && !strings.EqualFold(t.ChangeType, "drop") {
		return UnsafeChange{}, false
	}
	reason := t.UnsafeReason
	if reason == "" && strings.EqualFold(t.ChangeType, "drop") {
		reason = "DROP TABLE removes all data"
	}
	return UnsafeChange{
		Table:      t.TableName,
		Reason:     reason,
		DDL:        t.DDL,
		ChangeType: t.ChangeType,
	}, true
}

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
	Status       string `json:"status,omitempty"`
}

const (
	ControlStatusAlreadyInProgress = "already_in_progress"
	ControlStatusAlreadyRequested  = "already_requested"
)

// StopResponse is the HTTP response for POST /api/stop.
type StopResponse struct {
	Accepted     bool   `json:"accepted"`
	ErrorMessage string `json:"error_message,omitempty"`
	StoppedCount int64  `json:"stopped_count"`
	SkippedCount int64  `json:"skipped_count"`
	Status       string `json:"status,omitempty"`
}

// CancelResponse is the HTTP response for POST /api/cancel.
type CancelResponse struct {
	Accepted       bool   `json:"accepted"`
	ErrorMessage   string `json:"error_message,omitempty"`
	CancelledCount int64  `json:"cancelled_count"`
	SkippedCount   int64  `json:"skipped_count"`
	Status         string `json:"status,omitempty"`
}

// StartResponse is the HTTP response for POST /api/start.
type StartResponse struct {
	Accepted     bool   `json:"accepted"`
	ErrorCode    string `json:"error_code,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
	SkippedCount int64  `json:"skipped_count"`
	Status       string `json:"status,omitempty"`
	StartedCount int64  `json:"started_count"`
}

// ReleaseResponse is the HTTP response for POST /api/release.
type ReleaseResponse struct {
	Accepted     bool   `json:"accepted"`
	ErrorMessage string `json:"error_message,omitempty"`
	Status       string `json:"status,omitempty"`
}

// VolumeResponse is the HTTP response for POST /api/volume.
type VolumeResponse struct {
	Accepted       bool   `json:"accepted"`
	ErrorMessage   string `json:"error_message,omitempty"`
	PreviousVolume int32  `json:"previous_volume"`
	NewVolume      int32  `json:"new_volume"`
}

// ProgressResponse is the HTTP response for GET /api/progress/apply/{apply_id}.
type ProgressResponse struct {
	State       string `json:"state"`
	Engine      string `json:"engine"`
	ApplyID     string `json:"apply_id,omitempty"`
	Database    string `json:"database,omitempty"`     // Included in apply-id lookups
	Environment string `json:"environment,omitempty"`  // Included in apply-id lookups
	Caller      string `json:"caller,omitempty"`       // Included in apply-id lookups
	PullRequest string `json:"pull_request,omitempty"` // PR URL (blank for CLI context)
	StartedAt   string `json:"started_at,omitempty"`
	CompletedAt string `json:"completed_at,omitempty"`
	// Operations carries per-deployment operation rows for multi-deployment applies.
	// Empty for single-deployment applies.
	Operations   []*ProgressOperationResponse `json:"operations,omitempty"`
	Tables       []*TableProgressResponse     `json:"tables,omitempty"`
	ErrorCode    string                       `json:"error_code,omitempty"`
	ErrorMessage string                       `json:"error_message,omitempty"`
	Summary      string                       `json:"summary,omitempty"`  // Combined status with ETA
	Volume       int32                        `json:"volume,omitempty"`   // Current volume setting (1-11)
	Options      map[string]string            `json:"options,omitempty"`  // Apply options (defer_cutover, skip_revert, etc.)
	Metadata     map[string]string            `json:"metadata,omitempty"` // Engine-specific data
	// Released is true when an operator has released a paused rollout open via a
	// release control request, so a deployment that failed under
	// on_failure=pause no longer holds later deployments — the rollout proceeds
	// like continue. Apply-level: it applies to every operation of the apply.
	Released bool `json:"released,omitempty"`
}

// ProgressOperationResponse represents progress for one deployment operation.
type ProgressOperationResponse struct {
	Deployment string `json:"deployment"`
	// ExternalID is the remote data plane's stable apply identifier.
	ExternalID string `json:"external_id,omitempty"`
	// ExternalOperationID is the remote data plane's numeric operation row ID.
	ExternalOperationID string `json:"external_operation_id,omitempty"`
	OperationKind       string `json:"operation_kind,omitempty"`
	Target              string `json:"target,omitempty"`
	State               string `json:"state"`
	// CutoverPolicy is the rollout boundary policy for this deployment operation.
	CutoverPolicy string `json:"cutover_policy,omitempty"`
	// OnFailure is the rollout failure policy for this deployment operation.
	OnFailure    string `json:"on_failure,omitempty"`
	ErrorCode    string `json:"error_code,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
	StartedAt    string `json:"started_at,omitempty"`
	CompletedAt  string `json:"completed_at,omitempty"`
}

// TableProgressResponse represents progress for a single table.
type TableProgressResponse struct {
	TableName string `json:"table_name"`
	DDL       string `json:"ddl"`
	// Deployment attributes this table/task to a deployment in a multi-deployment apply.
	// Empty for single-deployment applies.
	Deployment      string `json:"deployment,omitempty"`
	Keyspace        string `json:"keyspace,omitempty"`
	ChangeType      string `json:"change_type,omitempty"` // create, alter, drop
	Status          string `json:"status"`
	RowsCopied      int64  `json:"rows_copied"`
	RowsTotal       int64  `json:"rows_total"`
	PercentComplete int32  `json:"percent_complete"`
	ETASeconds      int64  `json:"eta_seconds,omitempty"`
	// Checksum phase progress: rows verified so far and total to verify.
	// Non-zero only while the table is checksumming (verifying copied data).
	ChecksumRowsChecked int64                    `json:"checksum_rows_checked,omitempty"`
	ChecksumRowsTotal   int64                    `json:"checksum_rows_total,omitempty"`
	IsInstant           bool                     `json:"is_instant,omitempty"`
	ProgressDetail      string                   `json:"progress_detail,omitempty"`
	TaskID              string                   `json:"task_id,omitempty"`
	StartedAt           string                   `json:"started_at,omitempty"`
	CompletedAt         string                   `json:"completed_at,omitempty"`
	Shards              []*ShardProgressResponse `json:"shards,omitempty"`
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
	ApplyID string `json:"apply_id"`
	// ExternalID is the remote data plane's stable apply identifier.
	ExternalID string `json:"external_id,omitempty"`
	// ExternalOperationID is the remote data plane's numeric operation row ID.
	ExternalOperationID string `json:"external_operation_id,omitempty"`
	Database            string `json:"database"`
	Environment         string `json:"environment"`
	Deployment          string `json:"deployment,omitempty"`
	State               string `json:"state"`
	Engine              string `json:"engine"`
	Caller              string `json:"caller"`
	ErrorMessage        string `json:"error_message,omitempty"`
	StartedAt           string `json:"started_at,omitempty"`
	CompletedAt         string `json:"completed_at,omitempty"`
	UpdatedAt           string `json:"updated_at"`
	Volume              int    `json:"volume,omitempty"`
}

// StatusResponse is the HTTP response for GET /api/status.
type StatusResponse struct {
	ActiveCount  int                    `json:"active_count"`
	Limit        int                    `json:"limit,omitempty"`
	MaxLimit     int                    `json:"max_limit,omitempty"`
	HasMore      bool                   `json:"has_more,omitempty"`
	FailuresOnly bool                   `json:"failures_only,omitempty"`
	Applies      []*ActiveApplyResponse `json:"applies"`
}
