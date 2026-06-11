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
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/metrics"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// PlanRequest is the HTTP request body for POST /api/plan.
type PlanRequest struct {
	Database    string                         `json:"database"`
	Environment string                         `json:"environment"`
	Type        string                         `json:"type"` // "mysql" or "vitess"
	SchemaFiles map[string]*ternv1.SchemaFiles `json:"schema_files"`
	Repository  string                         `json:"repository,omitempty"`
	PullRequest *int32                         `json:"pull_request,omitempty"`
	// HeadSHA is the PR HEAD SHA at the time the schema files were discovered.
	// Persisted on the plan record and used at apply-confirm time to detect the
	// cross-delivery race where HEAD advances between plan and confirm.
	// Optional — absent for non-webhook callers (e.g. CLI plan invocations without a PR).
	HeadSHA    *string `json:"head_sha,omitempty"`
	SchemaPath string  `json:"-"`

	// SourceTrusted is set by the GitHub webhook path after SchemaBot has
	// discovered the PR source itself. It is deliberately not JSON-decodable:
	// direct API clients cannot attest repo/path ownership.
	SourceTrusted bool `json:"-"`
}

// RemoteDeploymentUnavailableError carries routing metadata for remote
// schema change service availability failures so callers can render actionable
// operator-facing errors without parsing strings.
type RemoteDeploymentUnavailableError struct {
	Deployment string
	Target     string
	Err        error
}

func (e *RemoteDeploymentUnavailableError) Error() string {
	if e.Target == "" {
		return fmt.Sprintf("remote deployment %q unavailable: %v", e.Deployment, e.Err)
	}
	return fmt.Sprintf("remote deployment %q target %q unavailable: %v", e.Deployment, e.Target, e.Err)
}

func (e *RemoteDeploymentUnavailableError) Unwrap() error {
	return e.Err
}

// ApplyRequest is the HTTP request body for POST /api/apply.
type ApplyRequest struct {
	PlanID         string            `json:"plan_id"`
	Environment    string            `json:"environment"`
	Options        map[string]string `json:"options,omitempty"`
	Caller         string            `json:"caller,omitempty"`          // Identity of the caller (e.g., "cli:user@host")
	InstallationID int64             `json:"installation_id,omitempty"` // GitHub App installation ID (for PR comment tracking)
}

// handlePlan handles POST /api/plan requests.
func (s *Service) handlePlan(w http.ResponseWriter, r *http.Request) {
	var req PlanRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
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
	switch req.Type {
	case storage.DatabaseTypeMySQL, storage.DatabaseTypeVitess, storage.DatabaseTypeStrata:
	default:
		s.writeError(w, http.StatusBadRequest, "type must be "+storage.DatabaseTypeMySQL+", "+storage.DatabaseTypeVitess+", or "+storage.DatabaseTypeStrata)
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
		var policyErr *SourcePolicyError
		if errors.As(err, &policyErr) {
			s.writeErrorCode(w, http.StatusForbidden, apitypes.ErrCodeSourcePolicyDenied, "plan failed: "+err.Error())
			return
		}
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
		span.SetStatus(otelcodes.Error, "invalid schema files")
		return nil, err
	} else if warning != "" {
		s.logger.Warn("plan request has empty schema files", "warning", warning, "database", req.Database)
	}

	planStart := time.Now()
	deployment := ""

	resolvedTarget, err := s.config.ResolveDatabaseTarget(req.Database, req.Environment)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "resolve target")
		metrics.RecordPlan(ctx, req.Repository, req.Database, deployment, req.Environment, "error")
		metrics.RecordPlanDuration(ctx, time.Since(planStart), req.Repository, req.Database, deployment, req.Environment, "error")
		return nil, fmt.Errorf("resolve target for %s/%s: %w", req.Database, req.Environment, err)
	}
	deployment = resolvedTarget.Deployment
	if req.Type != resolvedTarget.DatabaseType {
		typeErr := fmt.Errorf("database %q type %q does not match server config type %q", req.Database, req.Type, resolvedTarget.DatabaseType)
		span.RecordError(typeErr)
		span.SetStatus(otelcodes.Error, "type mismatch")
		metrics.RecordPlan(ctx, req.Repository, req.Database, deployment, req.Environment, "error")
		metrics.RecordPlanDuration(ctx, time.Since(planStart), req.Repository, req.Database, deployment, req.Environment, "error")
		return nil, typeErr
	}

	prInt := 0
	if req.PullRequest != nil {
		prInt = int(*req.PullRequest)
	}
	trustedSchemaPath := ""
	if req.SourceTrusted {
		trustedSchemaPath = req.SchemaPath
	}
	// Source policy checks only apply to SchemaBot-discovered PR sources. Direct
	// operator/API plans remain available through the existing endpoint access
	// model until the dedicated auth layer is added.
	if !req.SourceTrusted {
		s.logger.Debug("skipping source policy for direct plan request",
			"database", req.Database,
			"environment", req.Environment,
			"repository", req.Repository,
			"pull_request", prInt)
	} else {
		if err := s.config.AuthorizePlanSource(PlanSourcePolicyRequest{
			Database:    req.Database,
			Repository:  req.Repository,
			PullRequest: prInt,
			SchemaPath:  trustedSchemaPath,
		}); err != nil {
			reason := sourcePolicyReason(err)
			span.RecordError(err)
			span.SetStatus(otelcodes.Error, "source policy")
			metrics.RecordPlan(ctx, req.Repository, req.Database, deployment, req.Environment, "error")
			metrics.RecordPlanDuration(ctx, time.Since(planStart), req.Repository, req.Database, deployment, req.Environment, "error")
			metrics.RecordSourcePolicyBlock(ctx, "plan", req.Database, req.Environment, reason)
			s.logger.Warn("plan blocked by source policy",
				"database", req.Database,
				"environment", req.Environment,
				"repository", req.Repository,
				"pull_request", prInt,
				"schema_path", req.SchemaPath,
				"reason", reason,
				"error", err)
			return nil, fmt.Errorf("source policy: %w", err)
		}
	}

	client, err := s.TernClient(deployment, req.Environment)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "tern client")
		metrics.RecordPlan(ctx, req.Repository, req.Database, deployment, req.Environment, "error")
		metrics.RecordPlanDuration(ctx, time.Since(planStart), req.Repository, req.Database, deployment, req.Environment, "error")
		return nil, fmt.Errorf("database %q (%s): %w", req.Database, req.Environment, err)
	}

	ternReq := &ternv1.PlanRequest{
		Database:    req.Database,
		Type:        resolvedTarget.DatabaseType,
		SchemaFiles: req.SchemaFiles,
		Repository:  req.Repository,
		Environment: req.Environment,
		Target:      resolvedTarget.Target,
		SchemaPath:  trustedSchemaPath,
	}
	if req.PullRequest != nil {
		ternReq.PullRequest = *req.PullRequest
	}
	if req.HeadSHA != nil {
		ternReq.HeadSha = *req.HeadSHA
	}

	s.logger.Info("ExecutePlan: calling client.Plan",
		"database", req.Database,
		"type", resolvedTarget.DatabaseType,
		"deployment", deployment,
		"target", resolvedTarget.Target,
		"is_remote", client.IsRemote(),
		"schema_file_count", len(req.SchemaFiles),
	)

	resp, err := client.Plan(ctx, ternReq)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "plan failed")
		metrics.RecordPlan(ctx, req.Repository, req.Database, deployment, req.Environment, "error")
		metrics.RecordPlanDuration(ctx, time.Since(planStart), req.Repository, req.Database, deployment, req.Environment, "error")
		s.logger.Error("ExecutePlan: client.Plan failed",
			"database", req.Database,
			"type", resolvedTarget.DatabaseType,
			"deployment", deployment,
			"target", resolvedTarget.Target,
			"environment", req.Environment,
			"repository", req.Repository,
			"pull_request", prInt,
			"endpoint", client.Endpoint(),
			"is_remote", client.IsRemote(),
			"error", err,
		)
		if client.IsRemote() && grpcstatus.Code(err) == grpccodes.Unavailable {
			return nil, &RemoteDeploymentUnavailableError{
				Deployment: deployment,
				Target:     resolvedTarget.Target,
				Err:        err,
			}
		}
		return nil, err
	}
	span.SetAttributes(attribute.String("plan_id", resp.PlanId), attribute.Int("change_count", len(resp.Changes)))
	metrics.RecordPlan(ctx, req.Repository, req.Database, deployment, req.Environment, "success")
	metrics.RecordPlanDuration(ctx, time.Since(planStart), req.Repository, req.Database, deployment, req.Environment, "success")

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
	storedPlan := &storage.Plan{
		PlanIdentifier: resp.PlanId,
		Database:       req.Database,
		DatabaseType:   resolvedTarget.DatabaseType,
		Deployment:     deployment,
		Target:         resolvedTarget.Target,
		Repository:     req.Repository,
		PullRequest:    prInt,
		SchemaPath:     trustedSchemaPath,
		Environment:    req.Environment,
		SchemaFiles:    protoToSchemaFiles(req.SchemaFiles),
		Namespaces:     protoChangesToNamespaces(resp.Changes),
		HeadSHA:        ternReq.HeadSha,
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
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
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
		var policyErr *SourcePolicyError
		if errors.As(err, &policyErr) {
			s.writeErrorCode(w, http.StatusForbidden, apitypes.ErrCodeSourcePolicyDenied, "apply failed: "+err.Error())
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
// durable. Operator workers own dispatching queued work through the Tern
// client so request cancellation cannot orphan in-memory execution.
//
// Flow:
//  1. Load the plan from SchemaBot storage (source of truth for database, DDL changes).
//  2. Resolve the Tern client to validate the deployment/environment.
//  3. Create a pending Apply record and pending Task records from the plan.
//  4. Attach any pending observer to the stored apply before dispatch can start.
//  5. Wake an operator worker so fresh applies usually start immediately.
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

	plan, err := s.loadPlanForApply(ctx, span, req)
	if err != nil {
		return nil, 0, err
	}
	if err := s.authorizeStoredPlanSource(ctx, span, plan, req); err != nil {
		return nil, 0, err
	}
	return s.queueValidatedApply(ctx, span, plan, req)
}

// EnqueueAuthorizedApply queues an apply for a stored plan without evaluating
// source policy. The caller asserts that source authorization for this apply
// already happened — for example, a control plane that evaluated source policy
// against its own database config before dispatching to this deployment, or a
// host process that gates its own callers.
//
// This entry point exists because source policy can only be evaluated where
// the database config lives. A deployment that executes applies dispatched by
// a separate control plane has no database config, so re-evaluating the
// policy there can only fail closed.
//
// It is intentionally not reachable from SchemaBot's HTTP API. All execution
// invariants still apply: the plan must exist, match the requested
// environment, and carry stored routing metadata, and storage still enforces
// one active apply per target.
func (s *Service) EnqueueAuthorizedApply(ctx context.Context, req ApplyRequest) (*apitypes.ApplyResponse, int64, error) {
	ctx, span := otel.Tracer("schemabot").Start(ctx, "EnqueueAuthorizedApply",
		trace.WithAttributes(
			attribute.String("plan_id", req.PlanID),
			attribute.String("environment", req.Environment),
		),
	)
	defer span.End()

	plan, err := s.loadPlanForApply(ctx, span, req)
	if err != nil {
		return nil, 0, err
	}
	s.logger.Info("queueing apply without source policy evaluation; caller asserted source authorization",
		"plan_id", req.PlanID,
		"database", plan.Database,
		"deployment", plan.Deployment,
		"environment", req.Environment,
		"repository", plan.Repository,
		"pull_request", plan.PullRequest,
		"schema_path", plan.SchemaPath,
		"caller", req.Caller)
	return s.queueValidatedApply(ctx, span, plan, req)
}

// loadPlanForApply loads the stored plan for an apply request and enforces the
// execution invariants every queue path requires: the plan exists, was created
// for the requested environment, and carries the server-side routing metadata
// (deployment, target) the operator needs to dispatch it.
func (s *Service) loadPlanForApply(ctx context.Context, span trace.Span, req ApplyRequest) (*storage.Plan, error) {
	// Load plan first; it is the source of truth for database, type, and routing.
	plan, err := s.storage.Plans().Get(ctx, req.PlanID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "get plan")
		return nil, fmt.Errorf("get plan: %w", err)
	}
	if plan == nil {
		planErr := fmt.Errorf("plan not found: %s", req.PlanID)
		span.RecordError(planErr)
		span.SetStatus(otelcodes.Error, "plan not found")
		return nil, planErr
	}
	span.SetAttributes(attribute.String("database", plan.Database))
	if plan.Environment != req.Environment {
		applyErr := fmt.Errorf("plan %s was created for environment %q, not %q", req.PlanID, plan.Environment, req.Environment)
		span.RecordError(applyErr)
		span.SetStatus(otelcodes.Error, "environment mismatch")
		metrics.RecordApply(ctx, plan.Repository, plan.Database, plan.Deployment, req.Environment, "error")
		return nil, applyErr
	}
	if plan.Deployment == "" {
		applyErr := fmt.Errorf("plan %s is missing server-side routing metadata field %q; create a new plan and retry apply", req.PlanID, "deployment")
		span.RecordError(applyErr)
		span.SetStatus(otelcodes.Error, "missing stored deployment")
		metrics.RecordApply(ctx, plan.Repository, plan.Database, plan.Deployment, req.Environment, "error")
		return nil, applyErr
	}
	if plan.Target == "" {
		applyErr := fmt.Errorf("plan %s is missing server-side routing metadata field %q; create a new plan and retry apply", req.PlanID, "target")
		span.RecordError(applyErr)
		span.SetStatus(otelcodes.Error, "missing stored target")
		metrics.RecordApply(ctx, plan.Repository, plan.Database, plan.Deployment, req.Environment, "error")
		return nil, applyErr
	}
	return plan, nil
}

// authorizeStoredPlanSource evaluates source policy for a stored plan before
// it is queued. Source policy is evaluated for plans created from SchemaBot's
// trusted GitHub PR discovery path. Direct operator/API plans do not have a
// server-discovered schema path today; those remain governed by endpoint
// access until the dedicated auth layer is added.
func (s *Service) authorizeStoredPlanSource(ctx context.Context, span trace.Span, plan *storage.Plan, req ApplyRequest) error {
	if plan.SchemaPath == "" {
		s.logger.Debug("skipping source policy for apply because stored plan has no trusted schema path",
			"plan_id", req.PlanID,
			"database", plan.Database,
			"deployment", plan.Deployment,
			"environment", req.Environment,
			"repository", plan.Repository,
			"pull_request", plan.PullRequest)
		return nil
	}
	if err := s.config.AuthorizePlanSource(PlanSourcePolicyRequest{
		Database:    plan.Database,
		Repository:  plan.Repository,
		PullRequest: plan.PullRequest,
		SchemaPath:  plan.SchemaPath,
	}); err != nil {
		reason := sourcePolicyReason(err)
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "source policy")
		metrics.RecordApply(ctx, plan.Repository, plan.Database, plan.Deployment, req.Environment, "error")
		metrics.RecordSourcePolicyBlock(ctx, "apply", plan.Database, req.Environment, reason)
		s.logger.Warn("apply blocked by source policy",
			"plan_id", req.PlanID,
			"database", plan.Database,
			"deployment", plan.Deployment,
			"environment", req.Environment,
			"repository", plan.Repository,
			"pull_request", plan.PullRequest,
			"schema_path", plan.SchemaPath,
			"reason", reason,
			"error", err)
		return fmt.Errorf("source policy for plan %s: %w", req.PlanID, err)
	}
	return nil
}

// queueValidatedApply stores the pending apply and tasks for a validated plan
// and wakes an operator worker. Callers must have run loadPlanForApply first;
// gated entry points also run authorizeStoredPlanSource before queueing.
func (s *Service) queueValidatedApply(ctx context.Context, span trace.Span, plan *storage.Plan, req ApplyRequest) (*apitypes.ApplyResponse, int64, error) {
	deployment := plan.Deployment

	client, err := s.TernClient(deployment, req.Environment)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "tern client")
		metrics.RecordApply(ctx, plan.Repository, plan.Database, plan.Deployment, req.Environment, "error")
		return nil, 0, fmt.Errorf("database %q (%s): %w", plan.Database, req.Environment, err)
	}

	options := maps.Clone(req.Options)
	if options == nil {
		options = make(map[string]string)
	}
	options["target"] = plan.Target

	enqueueStart := time.Now()
	recordApplyResult := func(status string) {
		metrics.RecordApply(ctx, plan.Repository, plan.Database, plan.Deployment, req.Environment, status)
		metrics.RecordApplyDuration(ctx, time.Since(enqueueStart), plan.Repository, plan.Database, plan.Deployment, req.Environment, status)
	}
	recordApplyError := func(status string, err error) {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, status)
		recordApplyResult(applyMetricStatusForError(err))
	}

	attachObserver := func(storedApplyID int64) {
		observer := s.consumePendingObserver(plan.Database, deployment, req.Environment)
		if observer == nil {
			return
		}
		type applyIDSetter interface{ SetApplyID(int64) }
		if setter, ok := observer.(applyIDSetter); ok {
			setter.SetApplyID(storedApplyID)
		}
		client.SetObserver(storedApplyID, observer)
	}

	applyIdentifier, storedApplyID, err := s.enqueueApply(ctx, plan, req, deployment, options, attachObserver)
	if err != nil {
		recordApplyError("enqueue apply", err)
		return nil, 0, err
	}
	if storedApplyID <= 0 {
		applyErr := fmt.Errorf("accepted apply missing stored apply id")
		recordApplyError("apply missing stored id", applyErr)
		return nil, 0, applyErr
	}

	span.SetAttributes(attribute.String("apply_id", applyIdentifier), attribute.Bool("accepted", true))
	recordApplyResult("success")
	metrics.AdjustActiveApplies(ctx, 1, plan.Database, plan.Deployment, req.Environment)
	s.wakeOperator(applyIdentifier, plan.Database, req.Environment)

	return &apitypes.ApplyResponse{
		Accepted: true,
		ApplyID:  applyIdentifier,
	}, storedApplyID, nil
}

func (s *Service) enqueueApply(
	ctx context.Context,
	plan *storage.Plan,
	req ApplyRequest,
	deployment string,
	options map[string]string,
	onApplyCreated func(int64),
) (string, int64, error) {
	applyIdentifier := "apply-" + strings.ReplaceAll(uuid.New().String(), "-", "")[:16]
	apply, storedApplyID, err := s.createStoredApply(ctx, plan, req, deployment, options, applyIdentifier, "")
	if err != nil {
		return "", 0, err
	}
	if onApplyCreated != nil {
		onApplyCreated(storedApplyID)
	}
	return apply.ApplyIdentifier, storedApplyID, nil
}

func (s *Service) createStoredApply(
	ctx context.Context,
	plan *storage.Plan,
	req ApplyRequest,
	deployment string,
	options map[string]string,
	applyIdentifier string,
	externalID string,
) (*storage.Apply, int64, error) {
	now := time.Now()
	applyOpts := storage.ApplyOptionsFromMap(options)

	var lockID int64
	lock, err := s.storage.Locks().Get(ctx, plan.Database, plan.DatabaseType)
	if err != nil {
		return nil, 0, fmt.Errorf("lookup lock for %s/%s: %w", plan.Database, plan.DatabaseType, err)
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
		ExternalID:      externalID,
		Engine:          storage.EngineForType(plan.DatabaseType),
		State:           state.Apply.Pending,
		Options:         storage.MarshalApplyOptions(applyOpts),
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	taskChanges := applyTaskChanges(plan)
	tasks := make([]*storage.Task, 0, len(taskChanges))
	for _, ddlChange := range taskChanges {
		task := &storage.Task{
			TaskIdentifier: "task-" + strings.ReplaceAll(uuid.New().String(), "-", "")[:16],
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
		tasks = append(tasks, task)
	}

	// Dual-write one apply_operations row alongside the applies row in the
	// same transaction. Today the plan already carries the resolved
	// (deployment, target) pair from plan-time `ResolveDatabaseTarget`, and
	// the config layer hard-blocks multi-entry deployments maps, so we
	// always write exactly one operation row that mirrors the apply's own
	// routing. The data shape is what the operator claim-loop PR consumes
	// once that gate is lifted and plans become deployment-agnostic.
	operations := []*storage.ApplyOperation{{
		Deployment: apply.Deployment,
		Target:     plan.Target,
		State:      state.ApplyOperation.Pending,
		CreatedAt:  now,
		UpdatedAt:  now,
	}}

	storedApplyID, err := s.storage.Applies().CreateWithTasksAndOperations(ctx, apply, tasks, operations)
	if err != nil {
		return nil, 0, fmt.Errorf("store apply and tasks: %w", err)
	}
	apply.ID = storedApplyID

	if logStore := s.storage.ApplyLogs(); logStore != nil {
		if err := logStore.Append(ctx, &storage.ApplyLog{
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
	}

	return apply, storedApplyID, nil
}

func applyTaskChanges(plan *storage.Plan) []storage.TableChange {
	changes := append([]storage.TableChange{}, plan.FlatDDLChanges()...)
	for namespace, nsData := range plan.Namespaces {
		if len(nsData.VSchema) > 0 {
			changes = append(changes, storage.TableChange{
				Table:     "VSchema: " + namespace,
				Namespace: namespace,
				Operation: "vschema_update",
			})
		}
	}
	return changes
}

// ExecuteRollbackPlan generates a rollback plan via the Tern client.
// The plan is automatically stored by the Tern client's RollbackPlan method
// (which calls Plan internally). This is the shared implementation used by
// both the HTTP handler and the webhook handler.
func (s *Service) ExecuteRollbackPlan(ctx context.Context, database, environment, deployment string) (*apitypes.PlanResponse, error) {
	deployment, err := s.deploymentForDatabaseEnvironment(database, deployment, environment)
	if err != nil {
		return nil, fmt.Errorf("resolve deployment for rollback plan: %w", err)
	}

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
