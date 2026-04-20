// Package client provides HTTP client utilities for the SchemaBot CLI.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/schema"
)

// ResolveEndpoint returns the endpoint to use, checking in order:
// 1. Explicit flag value
// 2. SCHEMABOT_ENDPOINT environment variable
// 3. Config file value (if provided)
func ResolveEndpoint(flag string, configEndpoint ...string) string {
	if flag != "" {
		return strings.TrimSuffix(flag, "/")
	}
	if env := os.Getenv("SCHEMABOT_ENDPOINT"); env != "" {
		return strings.TrimSuffix(env, "/")
	}
	if len(configEndpoint) > 0 && configEndpoint[0] != "" {
		return strings.TrimSuffix(configEndpoint[0], "/")
	}
	return ""
}

// GetEnvironments fetches the list of environments for a database from the API.
func GetEnvironments(endpoint, database, deployment string) ([]string, error) {
	var result struct {
		Environments []string `json:"environments"`
	}
	path := fmt.Sprintf("/api/databases/%s/environments", url.PathEscape(database))
	if deployment != "" {
		path += "?" + url.Values{"deployment": {deployment}}.Encode()
	}
	if err := doGetInto(endpoint, path, &result); err != nil {
		return nil, err
	}
	return result.Environments, nil
}

// PlanOptions holds optional parameters for CallPlanAPI.
type PlanOptions struct {
	Target     string
	Deployment string
}

// CallPlanAPI calls the plan API by reading .sql files from schemaDir.
// Files are grouped by namespace: subdirectories become namespace keys,
// flat files use "default".
func CallPlanAPI(endpoint, database, dbType, environment, schemaDir, repo string, pr int, opts ...PlanOptions) (*apitypes.PlanResponse, error) {
	schemaFiles, err := ReadSchemaFiles(schemaDir)
	if err != nil {
		return nil, fmt.Errorf("read schema files: %w", err)
	}
	if len(schemaFiles) == 0 {
		return nil, fmt.Errorf("no .sql files found in %s", schemaDir)
	}
	return CallPlanAPIWithFiles(endpoint, database, dbType, environment, schemaFiles, repo, pr, opts...)
}

// CallPlanAPIWithFiles calls the plan API with pre-loaded, namespace-grouped schema files.
func CallPlanAPIWithFiles(endpoint, database, dbType, environment string, schemaFiles map[string]*apitypes.SchemaFiles, repo string, pr int, opts ...PlanOptions) (*apitypes.PlanResponse, error) {
	req := apitypes.PlanRequest{
		Database:    database,
		Type:        dbType,
		Environment: environment,
		SchemaFiles: schemaFiles,
		Repository:  repo,
	}
	if pr != 0 {
		prVal := int32(pr)
		req.PullRequest = &prVal
	}
	if len(opts) > 0 {
		if opts[0].Target != "" {
			req.Target = opts[0].Target
		}
		if opts[0].Deployment != "" {
			req.Deployment = opts[0].Deployment
		}
	}
	var result apitypes.PlanResponse
	if err := doPostInto(endpoint, "/api/plan", req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// CallRollbackPlanAPI calls the rollback API to generate a plan that reverts
// the specified apply. The response includes database/environment metadata.
func CallRollbackPlanAPI(endpoint, applyID string) (*apitypes.PlanResponse, error) {
	body := map[string]any{
		"apply_id": applyID,
	}
	var result apitypes.PlanResponse
	if err := doPostInto(endpoint, "/api/rollback/plan", body, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// CallApplyAPI calls the apply API and returns the typed result.
func CallApplyAPI(endpoint, planID, database, environment, caller string, options map[string]string, opts ...PlanOptions) (*apitypes.ApplyResponse, error) {
	req := apitypes.ApplyRequest{
		PlanID:      planID,
		Database:    database,
		Environment: environment,
		Caller:      caller,
		Options:     options,
	}
	if len(opts) > 0 {
		if opts[0].Target != "" {
			req.Target = opts[0].Target
		}
		if opts[0].Deployment != "" {
			req.Deployment = opts[0].Deployment
		}
	}
	var result apitypes.ApplyResponse
	if err := doPostInto(endpoint, "/api/apply", req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// CallCutoverAPI calls the cutover API and returns the typed result.
func CallCutoverAPI(endpoint, database, environment, applyID string) (*apitypes.ControlResponse, error) {
	req := apitypes.ControlRequest{Database: database, Environment: environment, ApplyID: applyID}
	var result apitypes.ControlResponse
	if err := doPostInto(endpoint, "/api/cutover", req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetProgress fetches progress for a schema change by apply ID.
func GetProgress(endpoint, applyID string) (*apitypes.ProgressResponse, error) {
	return GetProgressCtx(context.Background(), endpoint, applyID)
}

// GetProgressCtx is like GetProgress but accepts a context for timeout/cancellation control.
func GetProgressCtx(ctx context.Context, endpoint, applyID string) (*apitypes.ProgressResponse, error) {
	var result apitypes.ProgressResponse
	if err := doGetIntoCtx(ctx, endpoint, fmt.Sprintf("/api/progress/apply/%s", applyID), &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetDatabaseHistory fetches the apply history for a database.
func GetDatabaseHistory(endpoint, database, environment string) (*apitypes.DatabaseHistoryResponse, error) {
	path := fmt.Sprintf("/api/history/%s", database)
	if environment != "" {
		path += "?environment=" + environment
	}
	var result apitypes.DatabaseHistoryResponse
	if err := doGetInto(endpoint, path, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// CheckActiveSchemaChange checks if there's an active schema change for the database.
// Returns nil if no active schema change.
type ActiveSchemaChange struct {
	State   string
	ApplyID string
}

func CheckActiveSchemaChange(endpoint, database, environment string) (*ActiveSchemaChange, error) {
	var result apitypes.ProgressResponse
	path := fmt.Sprintf("/api/progress/%s?environment=%s", url.PathEscape(database), url.QueryEscape(environment))
	if err := doGetInto(endpoint, path, &result); err != nil {
		if IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	if result.State == "" || strings.EqualFold(result.State, "no_active_change") {
		return nil, nil
	}
	return &ActiveSchemaChange{State: result.State, ApplyID: result.ApplyID}, nil
}

// ReadSchemaFiles reads .sql files from a directory and groups them by namespace.
// Subdirectories become namespace keys; flat files use the directory name as the
// namespace (the MySQL database name). Only one level of subdirectories is
// supported (matching the webhook path behavior).
func ReadSchemaFiles(dir string) (map[string]*apitypes.SchemaFiles, error) {
	// Collect all files as relativePath → content
	rawFiles := make(map[string]string)

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			// Read schema files inside the subdirectory
			subEntries, err := os.ReadDir(filepath.Join(dir, entry.Name()))
			if err != nil {
				return nil, fmt.Errorf("read subdirectory %s: %w", entry.Name(), err)
			}
			for _, sub := range subEntries {
				if sub.IsDir() {
					continue
				}
				if !isSchemaFile(sub.Name()) {
					continue
				}
				// Use path.Join (forward slashes) for map keys so
				// GroupFilesByNamespace can parse them consistently.
				relPath := path.Join(entry.Name(), sub.Name())
				content, err := os.ReadFile(filepath.Join(dir, entry.Name(), sub.Name()))
				if err != nil {
					return nil, fmt.Errorf("read %s: %w", relPath, err)
				}
				rawFiles[relPath] = string(content)
			}
			continue
		}
		if !isSchemaFile(entry.Name()) {
			continue
		}
		content, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", entry.Name(), err)
		}
		rawFiles[entry.Name()] = string(content)
	}

	// Group by namespace using the shared helper.
	// For flat files, the directory name is the database name.
	grouped, err := schema.GroupFilesByNamespace(rawFiles, filepath.Base(dir))
	if err != nil {
		return nil, err
	}

	// Convert schema.SchemaFiles → apitypes.SchemaFiles
	result := make(map[string]*apitypes.SchemaFiles, len(grouped))
	for ns, nsFiles := range grouped {
		result[ns] = &apitypes.SchemaFiles{Files: nsFiles.Files}
	}
	return result, nil
}

func isSchemaFile(name string) bool {
	return strings.HasSuffix(name, ".sql") || name == "vschema.json"
}

// CallStopAPI calls the stop API and returns the typed result.
func CallStopAPI(endpoint, database, environment, applyID string) (*apitypes.StopResponse, error) {
	req := apitypes.ControlRequest{Database: database, Environment: environment, ApplyID: applyID}
	var result apitypes.StopResponse
	if err := doPostInto(endpoint, "/api/stop", req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// CallStartAPI calls the start API and returns the typed result.
func CallStartAPI(endpoint, database, environment, applyID string) (*apitypes.StartResponse, error) {
	req := apitypes.ControlRequest{Database: database, Environment: environment, ApplyID: applyID}
	var result apitypes.StartResponse
	if err := doPostInto(endpoint, "/api/start", req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// CallVolumeAPI calls the volume API and returns the typed result.
func CallVolumeAPI(endpoint, database, environment, applyID string, volume int) (*apitypes.VolumeResponse, error) {
	req := apitypes.VolumeRequest{Database: database, Environment: environment, Volume: int32(volume), ApplyID: applyID}
	var result apitypes.VolumeResponse
	if err := doPostInto(endpoint, "/api/volume", req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// CallRevertAPI calls the revert API and returns the typed result.
func CallRevertAPI(endpoint, database, environment string) (*apitypes.ControlResponse, error) {
	req := apitypes.ControlRequest{Database: database, Environment: environment}
	var result apitypes.ControlResponse
	if err := doPostInto(endpoint, "/api/revert", req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// CallSkipRevertAPI calls the skip-revert API and returns the typed result.
func CallSkipRevertAPI(endpoint, database, environment string) (*apitypes.ControlResponse, error) {
	req := apitypes.ControlRequest{Database: database, Environment: environment}
	var result apitypes.ControlResponse
	if err := doPostInto(endpoint, "/api/skip-revert", req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ExitWithJSON outputs a JSON error response and exits with code 1.
// Returns nil so callers can use: return ExitWithJSON(...)
func ExitWithJSON(code, message string) error {
	resp := map[string]any{
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(resp)
	os.Exit(1)
	return nil // unreachable, but satisfies return type
}

// LockInfo represents lock information returned from the API.
type LockInfo struct {
	Database     string    `json:"database"`
	DatabaseType string    `json:"database_type"`
	Owner        string    `json:"owner"`
	Repository   string    `json:"repository"`
	PullRequest  int       `json:"pull_request"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// GenerateCLIOwner generates an owner identifier for CLI-based locks.
// Format: "cli:username@hostname"
func GenerateCLIOwner() string {
	username := "unknown"
	if u, err := user.Current(); err == nil {
		username = u.Username
	}

	hostname := "unknown"
	if h, err := os.Hostname(); err == nil {
		hostname = h
	}

	return fmt.Sprintf("cli:%s@%s", username, hostname)
}

// AcquireLock attempts to acquire a lock on a database.
// Returns the lock info on success, or an error with ErrLockHeld if already locked.
func AcquireLock(endpoint, database, dbType, owner, repo string, pr int) (*LockInfo, error) {
	reqBody := map[string]any{
		"database":      database,
		"database_type": dbType,
		"owner":         owner,
	}
	if repo != "" {
		reqBody["repository"] = repo
	}
	if pr != 0 {
		reqBody["pull_request"] = pr
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal request body: %w", err)
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, endpoint+"/api/locks/acquire", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, FormatConnectionError(endpoint, err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	// Check for lock conflict (HTTP 409)
	if resp.StatusCode == http.StatusConflict {
		var result struct {
			Error       string    `json:"error"`
			CurrentLock *LockInfo `json:"current_lock"`
		}
		if err := json.Unmarshal(respBody, &result); err == nil && result.CurrentLock != nil {
			return result.CurrentLock, ErrLockHeld
		}
		return nil, ErrLockHeld
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("acquire lock failed: %s", FormatAPIError(resp.StatusCode, respBody))
	}

	var result struct {
		Lock *LockInfo `json:"lock"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	return result.Lock, nil
}

// ReleaseLock releases a lock. Returns ErrLockNotOwned if not the owner.
func ReleaseLock(endpoint, database, dbType, owner string) error {
	err := doSendBody(endpoint, http.MethodDelete, "/api/locks", map[string]any{
		"database":      database,
		"database_type": dbType,
		"owner":         owner,
	})
	if err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) {
			if apiErr.Status == http.StatusForbidden {
				return ErrLockNotOwned
			}
			if apiErr.Status == http.StatusNotFound {
				return ErrLockNotFound
			}
		}
	}
	return err
}

// ForceReleaseLock releases a lock regardless of owner (admin override).
func ForceReleaseLock(endpoint, database, dbType string) error {
	err := doSendBody(endpoint, http.MethodDelete, "/api/locks", map[string]any{
		"database":      database,
		"database_type": dbType,
		"force":         true,
	})
	if IsNotFound(err) {
		return ErrLockNotFound
	}
	return err
}

// GetLock retrieves lock information for a database.
// Returns nil if no lock exists.
func GetLock(endpoint, database, dbType string) (*LockInfo, error) {
	var result struct {
		Lock *LockInfo `json:"lock"`
	}
	err := doGetInto(endpoint, fmt.Sprintf("/api/locks/%s/%s", database, dbType), &result)
	if IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return result.Lock, nil
}

// ListLocks retrieves all active locks.
func ListLocks(endpoint string) ([]*LockInfo, error) {
	var result struct {
		Locks []*LockInfo `json:"locks"`
	}
	if err := doGetInto(endpoint, "/api/locks", &result); err != nil {
		return nil, err
	}
	return result.Locks, nil
}

// Lock error sentinels
var (
	ErrLockHeld     = fmt.Errorf("lock is already held by another owner")
	ErrLockNotOwned = fmt.Errorf("lock is not owned by you")
	ErrLockNotFound = fmt.Errorf("lock not found")
)

// GetStatus fetches all active schema changes.
func GetStatus(endpoint string) (*apitypes.StatusResponse, error) {
	var result apitypes.StatusResponse
	if err := doGetInto(endpoint, "/api/status", &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// LogEntry represents a single log entry from the apply logs.
type LogEntry struct {
	ID        int64     `json:"id"`
	ApplyID   string    `json:"apply_id"`
	TaskID    *int64    `json:"task_id,omitempty"`
	Level     string    `json:"level"`
	EventType string    `json:"event_type"`
	Message   string    `json:"message"`
	OldState  string    `json:"old_state,omitempty"`
	NewState  string    `json:"new_state,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// GetLogs retrieves apply logs for a database.
// If applyID is provided, it fetches logs for that specific apply.
// Otherwise, it fetches logs for the most recent apply in the environment.
func GetLogs(endpoint, database, environment, applyID string, limit int) ([]*LogEntry, error) {
	if database == "" && applyID == "" {
		return nil, fmt.Errorf("database or apply_id is required")
	}

	var path string
	if database == "" && applyID != "" {
		path = fmt.Sprintf("/api/logs?apply_id=%s", applyID)
	} else {
		path = fmt.Sprintf("/api/logs/%s?", database)
		if applyID != "" {
			path += fmt.Sprintf("apply_id=%s", applyID)
		} else if environment != "" {
			path += fmt.Sprintf("environment=%s", environment)
		}
	}
	if limit > 0 {
		path += fmt.Sprintf("&limit=%d", limit)
	}

	var result struct {
		Logs []*LogEntry `json:"logs"`
	}
	if err := doGetInto(endpoint, path, &result); err != nil {
		return nil, err
	}
	return result.Logs, nil
}

// Setting represents a key-value setting.
type Setting struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// ListSettings retrieves all settings.
func ListSettings(endpoint string) ([]*Setting, error) {
	var result struct {
		Settings []*Setting `json:"settings"`
	}
	if err := doGetInto(endpoint, "/api/settings", &result); err != nil {
		return nil, err
	}
	return result.Settings, nil
}

// GetSetting retrieves the value of a specific setting.
func GetSetting(endpoint, key string) (string, error) {
	var result struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	err := doGetInto(endpoint, fmt.Sprintf("/api/settings/%s", key), &result)
	if IsNotFound(err) {
		return "", nil // Setting not found, return empty
	}
	if err != nil {
		return "", err
	}
	return result.Value, nil
}

// SetSetting sets the value of a setting.
func SetSetting(endpoint, key, value string) error {
	return doSendBody(endpoint, http.MethodPost, "/api/settings", map[string]string{
		"key":   key,
		"value": value,
	})
}
