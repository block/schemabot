package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/metrics"
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
	ctx, span := otel.Tracer("schemabot").Start(ctx, "ExecutePlan",
		trace.WithAttributes(
			attribute.String("database", req.Database),
			attribute.String("environment", req.Environment),
			attribute.String("type", req.Type),
		),
	)
	defer span.End()

	if warning, err := validateSchemaFiles(req.SchemaFiles); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "invalid schema files")
		return nil, err
	} else if warning != "" {
		s.logger.Warn("plan request has empty schema files", "warning", warning, "database", req.Database)
	}

	planStart := time.Now()

	deployment := s.ResolveDeployment(req.Database, req.Deployment)

	client, err := s.TernClient(deployment, req.Environment)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "tern client")
		metrics.RecordPlan(ctx, req.Database, req.Environment, "error")
		metrics.RecordPlanDuration(ctx, time.Since(planStart), req.Database, req.Environment, "error")
		return nil, fmt.Errorf("database %q (%s): %w", req.Database, req.Environment, err)
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
		span.RecordError(err)
		span.SetStatus(codes.Error, "plan failed")
		metrics.RecordPlan(ctx, req.Database, req.Environment, "error")
		metrics.RecordPlanDuration(ctx, time.Since(planStart), req.Database, req.Environment, "error")
		return nil, err
	}
	span.SetAttributes(attribute.String("plan_id", resp.PlanId), attribute.Int("change_count", len(resp.Changes)))
	metrics.RecordPlan(ctx, req.Database, req.Environment, "success")
	metrics.RecordPlanDuration(ctx, time.Since(planStart), req.Database, req.Environment, "success")

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
		if errors.Is(err, storage.ErrActiveApplyExists) {
			s.logger.Warn("apply blocked by active apply", "plan_id", req.PlanID, "environment", req.Environment, "error", err)
			s.writeErrorCode(w, http.StatusConflict, apitypes.ErrCodeActiveApplyExists, "apply blocked by active apply: "+err.Error())
			return
		}
		s.logger.Error("apply failed", "plan_id", req.PlanID, "error", err)
		s.writeError(w, http.StatusInternalServerError, "apply failed: "+err.Error())
		return
	}

	_ = applyID // HTTP handler doesn't need the stored apply ID

	s.writeJSON(w, http.StatusOK, resp)
}

func applyMetricStatusForError(err error) string {
	if errors.Is(err, storage.ErrActiveApplyExists) {
		return "conflict"
	}
	return "error"
}

// ExecuteApply queues an apply request in storage and returns once the work is
// durable. Scheduler workers own dispatching queued work through the Tern
// client so request cancellation cannot orphan in-memory execution.
//
// Flow:
//  1. Load the plan from SchemaBot storage (source of truth for database, DDL changes).
//  2. Resolve the Tern client to validate the deployment/environment.
//  3. Create a pending Apply record and pending Task records from the plan.
//  4. Attach any pending observer to the stored apply before dispatch can start.
//  5. Wake a scheduler worker so queued work does not wait for the next poll.
//  6. Return the SchemaBot apply_identifier to the HTTP caller.
//
// Returns the API response, the stored apply ID (0 if not stored), and any error.
func (s *Service) ExecuteApply(ctx context.Context, req ApplyRequest) (*apitypes.ApplyResponse, int64, error) {
	ctx, span := otel.Tracer("schemabot").Start(ctx, "ExecuteApply",
		trace.WithAttributes(
			attribute.String("plan_id", req.PlanID),
			attribute.String("environment", req.Environment),
		),
	)
	defer span.End()

	// Load plan first — it's the source of truth for database and type.
	plan, err := s.storage.Plans().Get(ctx, req.PlanID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "get plan")
		return nil, 0, fmt.Errorf("get plan: %w", err)
	}
	if plan == nil {
		planErr := fmt.Errorf("plan not found: %s", req.PlanID)
		span.RecordError(planErr)
		span.SetStatus(codes.Error, "plan not found")
		return nil, 0, planErr
	}
	span.SetAttributes(attribute.String("database", plan.Database))

	deployment := s.ResolveDeployment(plan.Database, req.Deployment)

	client, err := s.TernClient(deployment, req.Environment)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "tern client")
		metrics.RecordApply(ctx, plan.Database, req.Environment, "error")
		return nil, 0, fmt.Errorf("database %q (%s): %w", plan.Database, req.Environment, err)
	}

	// Store the apply as durable queued work. Scheduler workers own dispatching
	// both local engine work and remote gRPC Tern calls.
	enqueueStart := time.Now()
	options := maps.Clone(req.Options)
	if options == nil {
		options = make(map[string]string)
	}
	applyTarget := req.Target
	if applyTarget == "" {
		applyTarget = plan.Database
	}
	options["target"] = applyTarget

	recordApplyResult := func(status string) {
		metrics.RecordApply(ctx, plan.Database, req.Environment, status)
		metrics.RecordApplyDuration(ctx, time.Since(enqueueStart), plan.Database, req.Environment, status)
	}
	recordApplyError := func(status string, err error) {
		span.RecordError(err)
		span.SetStatus(codes.Error, status)
		recordApplyResult(applyMetricStatusForError(err))
	}

	applyIdentifier, storedApplyID, err := s.enqueueApply(ctx, plan, req, deployment, options)
	if err != nil {
		recordApplyError("enqueue apply", err)
		return nil, 0, err
	}
	if observer := s.consumePendingObserver(plan.Database, deployment, req.Environment); observer != nil {
		type applyIDSetter interface{ SetApplyID(int64) }
		if setter, ok := observer.(applyIDSetter); ok {
			setter.SetApplyID(storedApplyID)
		}
		client.SetObserver(storedApplyID, observer)
	}

	span.SetAttributes(attribute.String("apply_id", applyIdentifier), attribute.Bool("accepted", true))
	if storedApplyID <= 0 {
		applyErr := fmt.Errorf("accepted apply missing stored apply id")
		recordApplyError("apply missing stored id", applyErr)
		return nil, 0, applyErr
	}

	recordApplyResult("success")
	metrics.AdjustActiveApplies(ctx, 1, plan.Database, req.Environment)
	s.wakeScheduler(applyIdentifier, plan.Database, req.Environment)

	applyResp := &apitypes.ApplyResponse{
		Accepted: true,
		ApplyID:  applyIdentifier,
	}
	return applyResp, storedApplyID, nil
}

func (s *Service) enqueueApply(ctx context.Context, plan *storage.Plan, req ApplyRequest, deployment string, options map[string]string) (string, int64, error) {
	applyIdentifier := "apply-" + strings.ReplaceAll(uuid.New().String(), "-", "")[:16]
	now := time.Now()
	applyOpts := storage.ApplyOptionsFromMap(options)

	var lockID int64
	lock, err := s.storage.Locks().Get(ctx, plan.Database, plan.DatabaseType)
	if err != nil {
		return "", 0, fmt.Errorf("lookup lock for %s/%s: %w", plan.Database, plan.DatabaseType, err)
	}
	if lock != nil {
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
		Engine:          storage.EngineForType(plan.DatabaseType),
		State:           state.Apply.Pending,
		Options:         storage.MarshalApplyOptions(applyOpts),
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	storedApplyID, err := s.storage.Applies().Create(ctx, apply)
	if err != nil {
		return "", 0, fmt.Errorf("store apply: %w", err)
	}

	for _, ddlChange := range applyTaskChanges(plan) {
		task := &storage.Task{
			TaskIdentifier: "task-" + strings.ReplaceAll(uuid.New().String(), "-", "")[:16],
			ApplyID:        storedApplyID,
			PlanID:         plan.ID,
			Database:       plan.Database,
			DatabaseType:   plan.DatabaseType,
			Engine:         storage.EngineForType(plan.DatabaseType),
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
			s.failQueuedApply(ctx, apply, storedApplyID, fmt.Sprintf("store task for table %s: %v", ddlChange.Table, err))
			return "", 0, fmt.Errorf("store task for table %s: %w", ddlChange.Table, err)
		}
	}

	if err := s.storage.ApplyLogs().Append(ctx, &storage.ApplyLog{
		ApplyID:   storedApplyID,
		Level:     storage.LogLevelInfo,
		EventType: storage.LogEventInfo,
		Source:    storage.LogSourceSchemaBot,
		Message:   fmt.Sprintf("Apply queued: %s", applyIdentifier),
		NewState:  state.Apply.Pending,
		CreatedAt: now,
	}); err != nil {
		s.logger.Warn("failed to log queued apply", "apply_id", applyIdentifier, "error", err)
	}

	return applyIdentifier, storedApplyID, nil
}

func (s *Service) failQueuedApply(ctx context.Context, apply *storage.Apply, storedApplyID int64, errMsg string) {
	now := time.Now()
	apply.ID = storedApplyID
	apply.State = state.Apply.Failed
	apply.ErrorMessage = errMsg
	apply.CompletedAt = &now
	apply.UpdatedAt = now
	if err := s.storage.Applies().Update(ctx, apply); err != nil {
		s.logger.Error("failed to mark partially queued apply failed",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"environment", apply.Environment,
			"error", err)
	}
}

func applyTaskChanges(plan *storage.Plan) []storage.TableChange {
	changes := append([]storage.TableChange{}, plan.FlatDDLChanges()...)
	for namespace, nsData := range plan.Namespaces {
		if len(nsData.VSchema) == 0 {
			continue
		}
		changes = append(changes, storage.TableChange{
			Table:     "VSchema: " + namespace,
			Namespace: namespace,
			Operation: "vschema_update",
		})
	}
	return changes
}

// ExecuteRollbackPlan generates a rollback plan via the Tern client.
// The plan is automatically stored by the Tern client's RollbackPlan method
// (which calls Plan internally). This is the shared implementation used by
// both the HTTP handler and the webhook handler.
func (s *Service) ExecuteRollbackPlan(ctx context.Context, database, environment, deployment string) (*apitypes.PlanResponse, error) {
	deployment = s.ResolveDeployment(database, deployment)

	client, err := s.TernClient(deployment, environment)
	if err != nil {
		return nil, fmt.Errorf("database %q (%s): %w", database, environment, err)
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
