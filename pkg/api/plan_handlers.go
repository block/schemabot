package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// PlanRequest is the HTTP request body for POST /api/plan.
type PlanRequest struct {
	Database    string                         `json:"database"`
	Deployment  string                         `json:"deployment,omitempty"`
	Environment string                         `json:"environment"`
	Type        string                         `json:"type"` // "mysql" or "vitess"
	SchemaFiles map[string]*ternv1.SchemaFiles `json:"schema_files"`
	Repository  string                         `json:"repository,omitempty"`
	PullRequest *int32                         `json:"pull_request,omitempty"`
	Target      string                         `json:"target,omitempty"`
}

// ApplyRequest is the HTTP request body for POST /api/apply.
type ApplyRequest struct {
	PlanID         string            `json:"plan_id"`
	Database       string            `json:"database,omitempty"` // Used for local mode detection
	Deployment     string            `json:"deployment,omitempty"`
	Environment    string            `json:"environment"`
	Options        map[string]string `json:"options,omitempty"`
	Caller         string            `json:"caller,omitempty"` // Identity of the caller (e.g., "cli:user@host")
	Target         string            `json:"target,omitempty"`
	InstallationID int64             `json:"installation_id,omitempty"` // GitHub App installation ID (for PR comment tracking)
}

// handlePlan handles POST /api/plan requests.
func (s *Service) handlePlan(w http.ResponseWriter, r *http.Request) {
	var req PlanRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if req.Database == "" {
		s.writeError(w, http.StatusBadRequest, "database is required")
		return
	}
	if req.Environment == "" {
		s.writeError(w, http.StatusBadRequest, "environment is required")
		return
	}
	if req.Type != storage.DatabaseTypeMySQL && req.Type != storage.DatabaseTypeVitess {
		s.writeError(w, http.StatusBadRequest, "type must be "+storage.DatabaseTypeMySQL+" or "+storage.DatabaseTypeVitess)
		return
	}
	if warning, err := validateSchemaFiles(req.SchemaFiles); err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	} else if warning != "" {
		s.logger.Warn("plan request has empty schema files", "warning", warning, "database", req.Database)
	}

	resp, err := s.ExecutePlan(r.Context(), req)
	if err != nil {
		s.logger.Error("plan failed", "database", req.Database, "error", err)
		s.writeError(w, http.StatusInternalServerError, "plan failed: "+err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, resp)
}

// ExecutePlan executes a plan request via the Tern client, stores the result,
// and returns the plan response. This is the shared implementation used by both
// the HTTP handler and the webhook handler.
func (s *Service) ExecutePlan(ctx context.Context, req PlanRequest) (*apitypes.PlanResponse, error) {
	if warning, err := validateSchemaFiles(req.SchemaFiles); err != nil {
		return nil, err
	} else if warning != "" {
		s.logger.Warn("plan request has empty schema files", "warning", warning, "database", req.Database)
	}

	deployment := s.resolveDeployment(req.Database, req.Deployment)

	client, err := s.TernClient(deployment, req.Environment)
	if err != nil {
		RecordPlan(ctx, req.Database, req.Environment, "error")
		return nil, fmt.Errorf("tern client: %w", err)
	}

	// Call Tern Plan
	target := req.Target
	if target == "" {
		target = req.Database
	}
	ternReq := &ternv1.PlanRequest{
		Database:    req.Database,
		Type:        req.Type,
		SchemaFiles: req.SchemaFiles,
		Repository:  req.Repository,
		Environment: req.Environment,
		Target:      target,
	}
	if req.PullRequest != nil {
		ternReq.PullRequest = *req.PullRequest
	}

	s.logger.Info("ExecutePlan: calling client.Plan",
		"database", req.Database,
		"type", req.Type,
		"is_remote", client.IsRemote(),
		"schema_file_count", len(req.SchemaFiles),
	)

	resp, err := client.Plan(ctx, ternReq)
	if err != nil {
		RecordPlan(ctx, req.Database, req.Environment, "error")
		return nil, err
	}
	RecordPlan(ctx, req.Database, req.Environment, "success")

	s.logger.Info("ExecutePlan: plan response",
		"plan_id", resp.PlanId,
		"change_count", len(resp.Changes),
	)
	for _, ch := range resp.Changes {
		for _, tc := range ch.TableChanges {
			s.logger.Info("ExecutePlan: table change",
				"table", tc.TableName,
				"change_type", tc.ChangeType.String(),
				"ddl_len", len(tc.Ddl),
			)
		}
	}

	// Store plan in SchemaBot's storage (idempotent — duplicate is ignored)
	prInt := 0
	if req.PullRequest != nil {
		prInt = int(*req.PullRequest)
	}
	storedPlan := &storage.Plan{
		PlanIdentifier: resp.PlanId,
		Database:       req.Database,
		DatabaseType:   req.Type,
		Repository:     req.Repository,
		PullRequest:    prInt,
		Environment:    req.Environment,
		SchemaFiles:    protoToSchemaFiles(req.SchemaFiles),
		Namespaces:     protoChangesToNamespaces(resp.Changes),
		CreatedAt:      time.Now(),
	}
	if _, err := s.storage.Plans().Create(ctx, storedPlan); err != nil && !errors.Is(err, storage.ErrPlanIDExists) {
		return nil, fmt.Errorf("store plan: %w", err)
	}

	return planResponseFromProto(resp), nil
}

// handleApply handles POST /api/apply requests.
func (s *Service) handleApply(w http.ResponseWriter, r *http.Request) {
	var req ApplyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if req.PlanID == "" {
		s.writeError(w, http.StatusBadRequest, "plan_id is required")
		return
	}
	if req.Environment == "" {
		s.writeError(w, http.StatusBadRequest, "environment is required")
		return
	}

	resp, applyID, err := s.ExecuteApply(r.Context(), req)
	if err != nil {
		s.logger.Error("apply failed", "plan_id", req.PlanID, "error", err)
		s.writeError(w, http.StatusInternalServerError, "apply failed: "+err.Error())
		return
	}

	_ = applyID // HTTP handler doesn't need the stored apply ID

	s.writeJSON(w, http.StatusOK, resp)
}

// ExecuteApply executes an apply request via the Tern client, stores the result,
// and returns the apply response. This is the shared implementation used by both
// the HTTP handler and the webhook handler.
//
// Flow:
//  1. Load the plan from SchemaBot storage (source of truth for database, DDL changes).
//  2. Build a ternv1.ApplyRequest with PlanId, DdlChanges, and options.
//  3. Call client.Apply(ctx, ternReq):
//     - LocalClient: looks up plan in same DB, creates apply + task records,
//     starts Spirit in a background goroutine, returns ApplyId (its own UUID).
//     - GRPCClient: passes the request to the remote Tern over gRPC. The remote
//     Tern creates its own records and starts execution. Returns ApplyId
//     (the remote engine's apply identifier).
//  4. Store the result in SchemaBot's storage:
//     - Local (IsRemote=false): the apply record already exists (LocalClient
//     created it in step 3). Look it up and reuse it.
//     - Remote (IsRemote=true): generate a SchemaBot-owned apply_identifier,
//     store Tern's ApplyId as external_id, create apply + task records.
//  5. Return the apply_identifier to the HTTP caller.
//
// Returns the API response, the stored apply ID (0 if not stored), and any error.
func (s *Service) ExecuteApply(ctx context.Context, req ApplyRequest) (*apitypes.ApplyResponse, int64, error) {
	// Load plan first — it's the source of truth for database and type.
	plan, err := s.storage.Plans().Get(ctx, req.PlanID)
	if err != nil {
		return nil, 0, fmt.Errorf("get plan: %w", err)
	}
	if plan == nil {
		return nil, 0, fmt.Errorf("plan not found: %s", req.PlanID)
	}

	deployment := s.resolveDeployment(plan.Database, req.Deployment)

	client, err := s.TernClient(deployment, req.Environment)
	if err != nil {
		return nil, 0, fmt.Errorf("tern client: %w", err)
	}

	// Ensure options map exists and includes environment
	options := req.Options
	if options == nil {
		options = make(map[string]string)
	}
	options["environment"] = req.Environment
	if req.Caller != "" {
		options["caller"] = req.Caller
	}

	applyTarget := req.Target
	if applyTarget == "" {
		applyTarget = plan.Database
	}
	ternReq := &ternv1.ApplyRequest{
		PlanId:      req.PlanID,
		Options:     options,
		Database:    plan.Database,
		Type:        plan.DatabaseType,
		DdlChanges:  storageToProtoTableChanges(plan.FlatDDLChanges()),
		Environment: req.Environment,
		Target:      applyTarget,
	}

	resp, err := client.Apply(ctx, ternReq)
	if err != nil {
		return nil, 0, err
	}

	// Store apply and task records in SchemaBot's storage.
	//
	// Local mode (client.IsRemote() == false):
	//   LocalClient.Apply() already created the apply/task records in the same
	//   database and started its own background poller. Look up the existing
	//   record directly.
	//
	// gRPC mode (client.IsRemote() == true):
	//   Remote Tern has separate storage. Create new apply/task records with a
	//   SchemaBot-owned apply_identifier. Store Tern's apply_id as external_id
	//   for resolveApplyID to translate in subsequent RPCs.
	//
	//   After creating the record, we call ResumeApply to start a background
	//   poller (pollForCompletion) that syncs Tern's real state into local
	//   storage. This must happen here — not in GRPCClient.Apply() — because
	//   the poller needs the storage.Apply record which doesn't exist until
	//   after the record is created above.
	var storedApplyID int64
	applyIdentifier := resp.ApplyId
	if resp.Accepted && resp.ApplyId != "" {
		if !client.IsRemote() {
			// Local mode: LocalClient.Apply() already created the apply + task
			// records in the same database. Just look up the existing record.
			existing, lookupErr := s.storage.Applies().GetByApplyIdentifier(ctx, resp.ApplyId)
			if lookupErr != nil {
				return nil, 0, fmt.Errorf("lookup local apply %s: %w", resp.ApplyId, lookupErr)
			}
			if existing == nil {
				s.logger.Error("local apply not found after LocalClient.Apply()",
					"apply_id", resp.ApplyId,
					"accepted", resp.Accepted,
				)
				return nil, 0, fmt.Errorf("local apply %s not found — LocalClient should have created it", resp.ApplyId)
			}
			storedApplyID = existing.ID
			applyIdentifier = existing.ApplyIdentifier
		} else {
			// gRPC mode: remote Tern has separate storage. Create apply + task
			// records in SchemaBot's storage with a SchemaBot-owned identifier.
			// resp.ApplyId is Tern's own ID (the remote engine's apply identifier) — stored as
			// external_id for resolveApplyID to translate in subsequent RPCs.
			applyIdentifier = "apply-" + strings.ReplaceAll(uuid.New().String(), "-", "")[:16]
			now := time.Now()

			deferCutover := options["defer_cutover"] == "true"
			allowUnsafe := options["allow_unsafe"] == "true"
			applyOpts := storage.ApplyOptions{
				DeferCutover: deferCutover,
				AllowUnsafe:  allowUnsafe,
			}

			var lockID int64
			if lock, err := s.storage.Locks().Get(ctx, plan.Database, plan.DatabaseType); err == nil && lock != nil {
				lockID = lock.ID
			}

			apply := &storage.Apply{
				ApplyIdentifier: applyIdentifier,
				LockID:          lockID,
				PlanID:          plan.ID,
				Database:        plan.Database,
				DatabaseType:    plan.DatabaseType,
				Repository:      plan.Repository,
				PullRequest:     plan.PullRequest,
				Environment:     req.Environment,
				Deployment:      deployment,
				Caller:          req.Caller,
				InstallationID:  req.InstallationID,
				ExternalID:      resp.ApplyId,
				Engine:          storage.EngineSpirit,
				State:           state.Apply.Pending,
				Options:         storage.MarshalApplyOptions(applyOpts),
				CreatedAt:       now,
				UpdatedAt:       now,
			}
			storedApplyID, err = s.storage.Applies().Create(ctx, apply)
			if err != nil {
				return nil, 0, fmt.Errorf("store apply: %w", err)
			}

			for _, ddlChange := range plan.FlatDDLChanges() {
				task := &storage.Task{
					TaskIdentifier: "task-" + strings.ReplaceAll(uuid.New().String(), "-", "")[:16],
					ApplyID:        storedApplyID,
					PlanID:         plan.ID,
					Database:       plan.Database,
					DatabaseType:   plan.DatabaseType,
					Engine:         storage.EngineSpirit,
					Repository:     plan.Repository,
					PullRequest:    plan.PullRequest,
					Environment:    req.Environment,
					State:          state.Task.Pending,
					Options:        storage.MarshalApplyOptions(applyOpts),
					Namespace:      ddlChange.Namespace,
					TableName:      ddlChange.Table,
					DDL:            ddlChange.DDL,
					DDLAction:      ddlChange.Operation,
					CreatedAt:      now,
					UpdatedAt:      now,
				}
				if _, err := s.storage.Tasks().Create(ctx, task); err != nil {
					return nil, 0, fmt.Errorf("store task: %w", err)
				}
			}

			// Start background poller to keep local storage in sync with Tern.
			// ResumeApply spawns pollForCompletion which syncs Tern's real state
			// into SchemaBot's storage — without this, status stays "pending".
			// Must happen after task creation so the poller can sync task state.
			apply.ID = storedApplyID
			if err := client.ResumeApply(ctx, apply); err != nil {
				s.logger.Warn("failed to start progress tracking", "apply_id", applyIdentifier, "error", err)
			}
		}
	}

	applyResp := &apitypes.ApplyResponse{
		Accepted:     resp.Accepted,
		ApplyID:      applyIdentifier,
		ErrorMessage: resp.ErrorMessage,
	}
	if !resp.Accepted && resp.ErrorMessage != "" {
		applyResp.ErrorCode = apitypes.ErrCodeEngineError
	}
	return applyResp, storedApplyID, nil
}

// ExecuteRollbackPlan generates a rollback plan via the Tern client.
// The plan is automatically stored by the Tern client's RollbackPlan method
// (which calls Plan internally). This is the shared implementation used by
// both the HTTP handler and the webhook handler.
func (s *Service) ExecuteRollbackPlan(ctx context.Context, database, environment, deployment string) (*apitypes.PlanResponse, error) {
	deployment = s.resolveDeployment(database, deployment)

	client, err := s.TernClient(deployment, environment)
	if err != nil {
		return nil, fmt.Errorf("tern client: %w", err)
	}

	resp, err := client.RollbackPlan(ctx, database)
	if err != nil {
		return nil, err
	}

	return planResponseFromProto(resp), nil
}

// validateSchemaFiles checks that schema_files has at least one namespace.
// An empty Files map within a namespace is valid (signals "drop all tables"),
// so we only reject when schema_files itself is missing.
//
// Returns a warning message if any namespace has empty files (could indicate
// a JSON field name bug like "sql_files" instead of "files"). Callers should
// log this but not reject the request.
func validateSchemaFiles(schemaFiles map[string]*ternv1.SchemaFiles) (warning string, err error) {
	if len(schemaFiles) == 0 {
		return "", fmt.Errorf("schema_files is required: must contain at least one namespace (JSON field for files is \"files\", not \"sql_files\")")
	}
	for ns, sf := range schemaFiles {
		if sf == nil || len(sf.GetFiles()) == 0 {
			warning = fmt.Sprintf("schema_files[%q] has no files — if this is unintentional, check that the JSON field is \"files\" (not \"sql_files\")", ns)
		}
	}
	return warning, nil
}
