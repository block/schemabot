package tern

import (
	"encoding/json"
	"maps"
	"sort"
	"time"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// taskStateToApplyState maps a task state string to an Apply state string.
func taskStateToApplyState(ts string) string {
	switch ts {
	case state.Task.Pending:
		return state.Apply.Pending
	case state.Task.Running:
		return state.Apply.Running
	case state.Task.WaitingForDeploy:
		return state.Apply.WaitingForDeploy
	case state.Task.WaitingForCutover:
		return state.Apply.WaitingForCutover
	case state.Task.CuttingOver:
		return state.Apply.CuttingOver
	case state.Task.RevertWindow:
		return state.Apply.RevertWindow
	case state.Task.Completed:
		return state.Apply.Completed
	case state.Task.Failed:
		return state.Apply.Failed
	case state.Task.Stopped:
		return state.Apply.Stopped
	case state.Task.Reverted:
		return state.Apply.Reverted
	case state.Task.Cancelled:
		return state.Apply.Cancelled
	default:
		return state.Apply.Pending
	}
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
	case engine.StateCompleted:
		return state.Task.Completed
	case engine.StateFailed:
		return state.Task.Failed
	case engine.StateStopped:
		return state.Task.Stopped
	case engine.StateReverted:
		return state.Task.Reverted
	default:
		return state.Task.Pending
	}
}

// storageStateToProto converts a task state string to proto State enum.
func storageStateToProto(ts string) ternv1.State {
	switch ts {
	case state.Task.Pending:
		return ternv1.State_STATE_PENDING
	case state.Task.Running:
		return ternv1.State_STATE_RUNNING
	case state.Task.WaitingForDeploy:
		return ternv1.State_STATE_WAITING_FOR_DEPLOY
	case state.Task.WaitingForCutover:
		return ternv1.State_STATE_WAITING_FOR_CUTOVER
	case state.Task.CuttingOver:
		return ternv1.State_STATE_CUTTING_OVER
	case state.Task.RevertWindow:
		return ternv1.State_STATE_REVERT_WINDOW
	case state.Task.Completed:
		return ternv1.State_STATE_COMPLETED
	case state.Task.Failed:
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
	default:
		// Unknown task state — return PENDING as a safe default so clients
		// continue polling rather than assuming no change is active.
		return ternv1.State_STATE_PENDING
	}
}

// changeTypeToProto converts operation string to proto ChangeType enum.
func changeTypeToProto(op string) ternv1.ChangeType {
	switch op {
	case "create":
		return ternv1.ChangeType_CHANGE_TYPE_CREATE
	case "alter":
		return ternv1.ChangeType_CHANGE_TYPE_ALTER
	case "drop":
		return ternv1.ChangeType_CHANGE_TYPE_DROP
	default:
		return ternv1.ChangeType_CHANGE_TYPE_OTHER
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
	case ternv1.State_STATE_CUTTING_OVER:
		return state.Apply.CuttingOver
	case ternv1.State_STATE_REVERT_WINDOW:
		return state.Apply.RevertWindow
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

// protoToSchemaFiles converts proto SchemaFiles (per-keyspace with separate sql_files
// and vschema_file) to the engine's schema.SchemaFiles (per-namespace with a unified
// Files map).
func protoToSchemaFiles(sf map[string]*ternv1.SchemaFiles) schema.SchemaFiles {
	result := make(schema.SchemaFiles, len(sf))
	for ns, ksFiles := range sf {
		files := make(map[string]string, len(ksFiles.Files))
		maps.Copy(files, ksFiles.Files)
		result[ns] = &schema.Namespace{Files: files}
	}
	return result
}

// psMetadataForStorage is a subset of the PlanetScale engine's metadata
// used for storing deploy request tracking data.
type psMetadataForStorage struct {
	BranchName       string     `json:"branch_name"`
	DeployRequestID  uint64     `json:"deploy_request_id"`
	DeployRequestURL string     `json:"deploy_request_url,omitempty"`
	DeployedAt       *time.Time `json:"deployed_at,omitempty"`
	IsInstant        bool       `json:"is_instant,omitempty"`
	DeferredDeploy   bool       `json:"deferred_deploy,omitempty"`
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
