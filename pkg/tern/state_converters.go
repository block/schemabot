package tern

import (
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/block/spirit/pkg/statement"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

func taskStates(tasks []*storage.Task) []string {
	states := make([]string, 0, len(tasks))
	for _, task := range tasks {
		states = append(states, task.State)
	}
	return states
}

// engineStateToStorage converts engine State to a canonical task state string.
func engineStateToStorage(es engine.State) string {
	switch es {
	case engine.StatePending:
		return state.Task.Pending
	case engine.StateRunning:
		return state.Task.Running
	case engine.StateWaitingForDeploy:
		return state.Task.WaitingForDeploy
	case engine.StateWaitingForCutover:
		return state.Task.WaitingForCutover
	case engine.StateCuttingOver:
		return state.Task.CuttingOver
	case engine.StateRevertWindow:
		return state.Task.RevertWindow
	case engine.StateReverting:
		return state.Task.Reverting
	case engine.StateCompleted:
		return state.Task.Completed
	case engine.StateFailed:
		return state.Task.Failed
	case engine.StateStopped:
		return state.Task.Stopped
	case engine.StateCancelled:
		return state.Task.Cancelled
	case engine.StateReverted:
		return state.Task.Reverted
	default:
		// Unknown engine states represent in-flight work until proven otherwise.
		// Keep them visible and blocking, and add an explicit mapping once known.
		return state.Task.Running
	}
}

// taskStateFromProgressResult converts an engine progress result to the task
// state Tern should persist. Engines use Retryable to opt a failed result into
// operator recovery instead of permanent failure.
func taskStateFromProgressResult(result *engine.ProgressResult) string {
	if result == nil {
		return state.Task.Pending
	}
	if result.State == engine.StateFailed && result.Retryable {
		return state.Task.FailedRetryable
	}
	return engineStateToStorage(result.State)
}

func progressFailureMessage(result *engine.ProgressResult) string {
	if result == nil {
		return ""
	}
	if result.ErrorMessage != "" {
		return result.ErrorMessage
	}
	return result.Message
}

// storageStateToProto converts a task state string to proto State enum.
func storageStateToProto(ts string) ternv1.State {
	ts = state.NormalizeState(ts)
	switch ts {
	case state.Task.Pending:
		return ternv1.State_STATE_PENDING
	case state.Task.Running:
		return ternv1.State_STATE_RUNNING
	case state.Task.WaitingForDeploy:
		return ternv1.State_STATE_WAITING_FOR_DEPLOY
	case state.Task.WaitingForCutover:
		return ternv1.State_STATE_WAITING_FOR_CUTOVER
	case state.Task.Recovering, state.Apply.Recovering:
		return ternv1.State_STATE_RECOVERING
	case state.Task.CuttingOver:
		return ternv1.State_STATE_CUTTING_OVER
	case state.Task.RevertWindow:
		return ternv1.State_STATE_REVERT_WINDOW
	case state.Task.Reverting, state.Apply.Reverting:
		return ternv1.State_STATE_REVERTING
	case state.Task.Completed:
		return ternv1.State_STATE_COMPLETED
	case state.Task.Failed:
		return ternv1.State_STATE_FAILED
	case state.Task.FailedRetryable, state.Apply.FailedRetryable:
		return ternv1.State_STATE_FAILED
	case state.Task.Stopped:
		return ternv1.State_STATE_STOPPED
	case state.Task.Cancelled:
		return ternv1.State_STATE_CANCELLED
	case state.Task.Reverted:
		return ternv1.State_STATE_REVERTED
	case state.Apply.PreparingBranch:
		return ternv1.State_STATE_PREPARING_BRANCH
	case state.Apply.ApplyingBranchChanges:
		return ternv1.State_STATE_APPLYING_BRANCH_CHANGES
	case state.Apply.CreatingDeployRequest:
		return ternv1.State_STATE_CREATING_DEPLOY_REQUEST
	case state.Apply.ValidatingBranch:
		return ternv1.State_STATE_VALIDATING_BRANCH
	case state.Apply.ValidatingDeployRequest:
		return ternv1.State_STATE_VALIDATING_DEPLOY_REQUEST
	default:
		// Unknown task state — return PENDING as a safe default so clients
		// continue polling rather than assuming no change is active.
		return ternv1.State_STATE_PENDING
	}
}

// changeTypeToProto converts a Spirit StatementType to the proto ChangeType enum.
func changeTypeToProto(op statement.StatementType) ternv1.ChangeType {
	switch op {
	case statement.StatementCreateTable:
		return ternv1.ChangeType_CHANGE_TYPE_CREATE
	case statement.StatementAlterTable:
		return ternv1.ChangeType_CHANGE_TYPE_ALTER
	case statement.StatementDropTable:
		return ternv1.ChangeType_CHANGE_TYPE_DROP
	default:
		return ternv1.ChangeType_CHANGE_TYPE_OTHER
	}
}

// ddlActionToProtoChangeType converts a task's DDLAction string to a proto ChangeType.
// Handles vschema_update which doesn't come from Spirit's statement parser.
func ddlActionToProtoChangeType(action string) ternv1.ChangeType {
	switch action {
	case "vschema_update":
		return ternv1.ChangeType_CHANGE_TYPE_VSCHEMA
	default:
		return changeTypeToProto(ddl.OpToStatementType(action))
	}
}

// protoChangeTypeToDDLAction converts a proto ChangeType back to the lowercase
// DDLAction string used in storage. It is the inverse of
// ddlActionToProtoChangeType and is used to rebuild a plan's table changes from
// a dispatch request on a deployment that did not plan locally.
func protoChangeTypeToDDLAction(ct ternv1.ChangeType) string {
	switch ct {
	case ternv1.ChangeType_CHANGE_TYPE_VSCHEMA:
		return "vschema_update"
	case ternv1.ChangeType_CHANGE_TYPE_CREATE:
		return ddl.StatementTypeToOp(statement.StatementCreateTable)
	case ternv1.ChangeType_CHANGE_TYPE_ALTER:
		return ddl.StatementTypeToOp(statement.StatementAlterTable)
	case ternv1.ChangeType_CHANGE_TYPE_DROP:
		return ddl.StatementTypeToOp(statement.StatementDropTable)
	default:
		return "unknown"
	}
}

// filterTasksByApply returns only tasks belonging to the specified apply, sorted by ID (execution order).
func filterTasksByApply(tasks []*storage.Task, applyID int64) []*storage.Task {
	var filtered []*storage.Task
	for _, t := range tasks {
		if t.ApplyID == applyID {
			filtered = append(filtered, t)
		}
	}
	// Sort by ID to maintain execution order (tasks are created in the order they will run)
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].ID < filtered[j].ID
	})
	return filtered
}

// ProtoStateToStorage converts proto State to storage apply state string.
// Returns "" for STATE_NO_ACTIVE_CHANGE so callers can distinguish "no state" from "pending".
func ProtoStateToStorage(ps ternv1.State) string {
	switch ps {
	case ternv1.State_STATE_NO_ACTIVE_CHANGE:
		return ""
	case ternv1.State_STATE_PENDING:
		return state.Apply.Pending
	case ternv1.State_STATE_RUNNING:
		return state.Apply.Running
	case ternv1.State_STATE_WAITING_FOR_DEPLOY:
		return state.Apply.WaitingForDeploy
	case ternv1.State_STATE_WAITING_FOR_CUTOVER:
		return state.Apply.WaitingForCutover
	case ternv1.State_STATE_RECOVERING:
		return state.Apply.Recovering
	case ternv1.State_STATE_CUTTING_OVER:
		return state.Apply.CuttingOver
	case ternv1.State_STATE_REVERT_WINDOW:
		return state.Apply.RevertWindow
	case ternv1.State_STATE_REVERTING:
		return state.Apply.Reverting
	case ternv1.State_STATE_COMPLETED:
		return state.Apply.Completed
	case ternv1.State_STATE_FAILED:
		return state.Apply.Failed
	case ternv1.State_STATE_STOPPED:
		return state.Apply.Stopped
	case ternv1.State_STATE_CANCELLED:
		return state.Apply.Cancelled
	case ternv1.State_STATE_REVERTED:
		return state.Apply.Reverted
	case ternv1.State_STATE_PREPARING_BRANCH:
		return state.Apply.PreparingBranch
	case ternv1.State_STATE_APPLYING_BRANCH_CHANGES:
		return state.Apply.ApplyingBranchChanges
	case ternv1.State_STATE_CREATING_DEPLOY_REQUEST:
		return state.Apply.CreatingDeployRequest
	case ternv1.State_STATE_VALIDATING_BRANCH:
		return state.Apply.ValidatingBranch
	case ternv1.State_STATE_VALIDATING_DEPLOY_REQUEST:
		return state.Apply.ValidatingDeployRequest
	default:
		return ""
	}
}

// isTerminalProtoState returns true if the proto state is terminal.
func isTerminalProtoState(ps ternv1.State) bool {
	switch ps {
	case ternv1.State_STATE_COMPLETED, ternv1.State_STATE_FAILED,
		ternv1.State_STATE_STOPPED, ternv1.State_STATE_CANCELLED,
		ternv1.State_STATE_REVERTED:
		return true
	default:
		return false
	}
}

// protoToSchemaFiles converts proto SchemaFiles to the engine's schema.SchemaFiles,
// copying the unified files map for each namespace. A nil namespace value yields an
// empty Files map (GetFiles is nil-safe).
func protoToSchemaFiles(sf map[string]*ternv1.SchemaFiles) schema.SchemaFiles {
	result := make(schema.SchemaFiles, len(sf))
	for ns, ksFiles := range sf {
		// A nil namespace value yields an empty Files map; GetFiles is nil-safe.
		nsFiles := ksFiles.GetFiles()
		files := make(map[string]string, len(nsFiles))
		maps.Copy(files, nsFiles)
		result[ns] = &schema.Namespace{Files: files}
	}
	return result
}

// schemaFilesToProto converts the engine's schema.SchemaFiles to proto
// SchemaFiles. Remote deployments that re-plan per shard (e.g. sharded engines)
// need the full declarative input, including vschema.json, at apply time.
func schemaFilesToProto(sf schema.SchemaFiles) map[string]*ternv1.SchemaFiles {
	if len(sf) == 0 {
		return nil
	}
	result := make(map[string]*ternv1.SchemaFiles, len(sf))
	for ns, namespace := range sf {
		if namespace == nil {
			continue
		}
		files := make(map[string]string, len(namespace.Files))
		maps.Copy(files, namespace.Files)
		result[ns] = &ternv1.SchemaFiles{Files: files}
	}
	return result
}

// psMetadataForStorage is a subset of the PlanetScale engine's metadata
// used for storing deploy request tracking data.
type psMetadataForStorage struct {
	BranchName       string                `json:"branch_name"`
	DeployRequestID  uint64                `json:"deploy_request_id"`
	DeployRequestURL string                `json:"deploy_request_url,omitempty"`
	DeployedAt       *time.Time            `json:"deployed_at,omitempty"`
	IsInstant        bool                  `json:"is_instant,omitempty"`
	DeferredDeploy   bool                  `json:"deferred_deploy,omitempty"`
	VSchemaStatus    string                `json:"vschema_status,omitempty"`
	VSchemaDiffs     []vschemaKeyspaceDiff `json:"vschema_diffs,omitempty"`
	RevertExpiresAt  *time.Time            `json:"revert_expires_at,omitempty"`
}

// vschemaKeyspaceDiff mirrors the PlanetScale engine's per-keyspace VSchema diff
// shape so the stored-state projection can decode it.
type vschemaKeyspaceDiff struct {
	Namespace string `json:"namespace"`
	Diff      string `json:"diff"`
}

func decodePSMetadataForStorage(s string) (*psMetadataForStorage, error) {
	if s == "" {
		return nil, nil
	}
	var m psMetadataForStorage
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// setRevertExpiresAtMetadata returns the PlanetScale resume-state blob with
// revert_expires_at set to expiresAt, preserving every other key. It merges at
// the JSON-object level rather than re-encoding psMetadataForStorage so engine
// fields the storage struct does not model survive the rewrite. The timestamp
// is normalized to UTC RFC3339 so the value is stable across ticks and the
// comment/CLI parse it the same way.
func setRevertExpiresAtMetadata(metadata string, expiresAt time.Time) (string, error) {
	obj := map[string]json.RawMessage{}
	if metadata != "" {
		if err := json.Unmarshal([]byte(metadata), &obj); err != nil {
			return "", fmt.Errorf("decode planetscale resume metadata to set revert_expires_at: %w", err)
		}
	}
	encoded, err := json.Marshal(expiresAt.UTC().Format(time.RFC3339))
	if err != nil {
		return "", fmt.Errorf("encode revert_expires_at: %w", err)
	}
	obj["revert_expires_at"] = encoded
	out, err := json.Marshal(obj)
	if err != nil {
		return "", fmt.Errorf("re-encode planetscale resume metadata with revert_expires_at: %w", err)
	}
	return string(out), nil
}

// PSDisplayMetadata decodes a PlanetScale engine resume-state blob into the
// display fields surfaced on the progress response (branch_name,
// deploy_request_url, is_instant, deferred_deploy). It is the projection a
// progress response served from storage uses when the engine was not polled,
// mirroring the live-path projection in the PlanetScale engine. It returns a nil
// map when the blob is empty or carries no display fields, so callers render
// without these fields rather than failing.
func PSDisplayMetadata(resumeStateMetadata string) (map[string]string, error) {
	meta, err := decodePSMetadataForStorage(resumeStateMetadata)
	if err != nil {
		return nil, fmt.Errorf("decode planetscale resume metadata for display: %w", err)
	}
	if meta == nil {
		return nil, nil
	}
	var m map[string]string
	set := func(k, v string) {
		if m == nil {
			m = make(map[string]string, 4)
		}
		m[k] = v
	}
	if meta.BranchName != "" {
		set("branch_name", meta.BranchName)
	}
	if meta.DeployRequestURL != "" {
		set("deploy_request_url", meta.DeployRequestURL)
	}
	if meta.IsInstant {
		set("is_instant", "true")
	}
	if meta.DeferredDeploy {
		set("deferred_deploy", "true")
	}
	if meta.RevertExpiresAt != nil {
		set("revert_expires_at", meta.RevertExpiresAt.UTC().Format(time.RFC3339))
	}
	// Per-keyspace VSchema state. Mirrors the engine's live projection: every
	// changed keyspace carries the deploy-level status, encoded as JSON so the
	// CLI and PR comment decode it via apitypes.ParseVSchemaChanges.
	if len(meta.VSchemaDiffs) > 0 {
		changes := make([]apitypes.VSchemaChange, 0, len(meta.VSchemaDiffs))
		for _, d := range meta.VSchemaDiffs {
			changes = append(changes, apitypes.VSchemaChange{
				Namespace: d.Namespace,
				Status:    meta.VSchemaStatus,
				Diff:      d.Diff,
			})
		}
		encoded, err := apitypes.EncodeVSchemaChanges(changes)
		if err != nil {
			return nil, fmt.Errorf("encode planetscale vschema changes for display: %w", err)
		}
		if encoded != "" {
			set(apitypes.VSchemaChangesMetadataKey, encoded)
		}
	}
	return m, nil
}

// PSDisplayMetadataStorageBlob converts a progress response's display
// metadata — the map a data-plane progress poll returns (deploy_request_url, the
// encoded VSchema status, instant/deferred flags) — back into the stored
// psMetadataForStorage JSON that the PR comment's display projection
// (resolveDisplayByOperation → PSDisplayMetadata) reads. For a remote (gRPC)
// apply the engine runs in the data plane, so its resume metadata never lands on
// the control-plane operation; mirroring these display fields is how the comment
// surfaces the deploy-request link and VSchema status. Returns "" when there is
// nothing worth storing, so callers leave the operation's metadata untouched.
func PSDisplayMetadataStorageBlob(md map[string]string) (string, error) {
	if len(md) == 0 {
		return "", nil
	}
	m := psMetadataForStorage{
		BranchName:       md["branch_name"],
		DeployRequestURL: md["deploy_request_url"],
		IsInstant:        md["is_instant"] == "true",
		DeferredDeploy:   md["deferred_deploy"] == "true",
	}
	changes, err := apitypes.ParseVSchemaChanges(md)
	if err != nil {
		return "", fmt.Errorf("parse vschema changes from display metadata: %w", err)
	}
	for _, ch := range changes {
		if m.VSchemaStatus == "" {
			m.VSchemaStatus = ch.Status
		}
		m.VSchemaDiffs = append(m.VSchemaDiffs, vschemaKeyspaceDiff{Namespace: ch.Namespace, Diff: ch.Diff})
	}
	if m.DeployRequestURL == "" && m.BranchName == "" && !m.IsInstant && !m.DeferredDeploy && len(m.VSchemaDiffs) == 0 {
		return "", nil
	}
	encoded, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("encode display metadata for storage: %w", err)
	}
	return string(encoded), nil
}
