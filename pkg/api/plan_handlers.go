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
	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/schema"
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

type unsupportedPullSchemaError struct {
	DatabaseType string
}

func (e *unsupportedPullSchemaError) Error() string {
	return fmt.Sprintf("pull schema supports %s and %s databases; got %s", storage.DatabaseTypeMySQL, storage.DatabaseTypeVitess, e.DatabaseType)
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

// handlePullSchema handles POST /api/pull requests.
func (s *Service) handlePullSchema(w http.ResponseWriter, r *http.Request) {
	var req apitypes.PullSchemaRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		s.writeBodyDecodeError(w, err)
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
	if req.Type != "" {
		switch req.Type {
		case storage.DatabaseTypeMySQL, storage.DatabaseTypeVitess, storage.DatabaseTypeStrata:
		default:
			s.writeError(w, http.StatusBadRequest, "type must be "+storage.DatabaseTypeMySQL+", "+storage.DatabaseTypeVitess+", or "+storage.DatabaseTypeStrata)
			return
		}
	}
	if _, err := pullCatalogDetail(req.CatalogDetail); err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	resp, err := s.ExecutePullSchema(r.Context(), req)
	if err != nil {
		var unsupportedErr *unsupportedPullSchemaError
		if errors.As(err, &unsupportedErr) {
			s.logger.Warn("pull schema rejected for unsupported database type", "database", req.Database, "environment", req.Environment, "type", unsupportedErr.DatabaseType)
			s.writeError(w, http.StatusNotImplemented, err.Error())
			return
		}
		var unavailableErr *RemoteDeploymentUnavailableError
		if errors.As(err, &unavailableErr) {
			s.logger.Error("pull schema failed because remote deployment is unavailable",
				"database", req.Database,
				"environment", req.Environment,
				"deployment", unavailableErr.Deployment,
				"target", unavailableErr.Target,
				"error", err)
			s.writeErrorCode(w, http.StatusServiceUnavailable, apitypes.ErrCodeEngineUnavailable, "pull schema failed: "+err.Error())
			return
		}
		s.logger.Error("pull schema failed", "database", req.Database, "environment", req.Environment, "error", err)
		s.writeError(w, http.StatusInternalServerError, "pull schema failed: "+err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, resp)
}

// ExecutePullSchema resolves a configured database route and fetches its live schema.
func (s *Service) ExecutePullSchema(ctx context.Context, req apitypes.PullSchemaRequest) (*apitypes.PullSchemaResponse, error) {
	ctx, span := otel.Tracer("schemabot").Start(ctx, "ExecutePullSchema",
		trace.WithAttributes(
			attribute.String("database", req.Database),
			attribute.String("environment", req.Environment),
			attribute.String("type", req.Type),
		),
	)
	defer span.End()

	// Pull the live schema from the primary deployment (first in rollout order:
	// explicit deployment_order when set, otherwise alphabetical). For a
	// multi-deployment environment the primary is the canonical source for the
	// schema diff; the apply itself fans out across every deployment. This
	// matches the route ExecutePlan resolves and the deployment
	// createStoredApply records.
	resolvedTarget, err := s.config.ResolvePrimaryDatabaseTarget(req.Database, req.Environment)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "resolve target")
		return nil, fmt.Errorf("resolve target for %s/%s: %w", req.Database, req.Environment, err)
	}
	if req.Type != "" && req.Type != resolvedTarget.DatabaseType {
		typeErr := fmt.Errorf("database %q type %q does not match server config type %q", req.Database, req.Type, resolvedTarget.DatabaseType)
		span.RecordError(typeErr)
		span.SetStatus(otelcodes.Error, "type mismatch")
		return nil, typeErr
	}
	if resolvedTarget.DatabaseType != storage.DatabaseTypeMySQL && resolvedTarget.DatabaseType != storage.DatabaseTypeVitess {
		unsupportedErr := &unsupportedPullSchemaError{DatabaseType: resolvedTarget.DatabaseType}
		span.RecordError(unsupportedErr)
		span.SetStatus(otelcodes.Error, "unsupported database type")
		return nil, unsupportedErr
	}
	namespaces, err := pullNamespaces(req.Namespaces)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "invalid namespaces")
		return nil, err
	}
	catalogDetail, err := pullCatalogDetail(req.CatalogDetail)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "invalid catalog detail")
		return nil, err
	}

	client, err := s.TernClient(resolvedTarget.Deployment, req.Environment)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "tern client")
		return nil, fmt.Errorf("database %q (%s): %w", req.Database, req.Environment, err)
	}

	isRemoteTarget := client.IsRemote()
	s.logger.Info("ExecutePullSchema: calling PullSchema",
		"database", req.Database,
		"type", resolvedTarget.DatabaseType,
		"deployment", resolvedTarget.Deployment,
		"target", resolvedTarget.Target,
		"environment", req.Environment,
		"pull_call_count", len(namespaces),
		"explicit_namespace_count", len(req.Namespaces),
		"is_remote", isRemoteTarget,
	)

	merged := &ternv1.PullSchemaResponse{
		Database:    req.Database,
		Type:        resolvedTarget.DatabaseType,
		Environment: req.Environment,
		Namespaces:  make(map[string]*ternv1.PulledNamespace),
	}
	for _, namespace := range namespaces {
		resp, pullErr := client.PullSchema(ctx, &ternv1.PullSchemaRequest{
			Database:      req.Database,
			Type:          resolvedTarget.DatabaseType,
			Target:        resolvedTarget.Target,
			Environment:   req.Environment,
			Namespace:     namespace,
			CatalogDetail: catalogDetail,
		})
		if pullErr != nil {
			span.RecordError(pullErr)
			span.SetStatus(otelcodes.Error, "pull schema failed")
			s.logger.Error("ExecutePullSchema: routing client PullSchema failed",
				"database", req.Database,
				"type", resolvedTarget.DatabaseType,
				"deployment", resolvedTarget.Deployment,
				"target", resolvedTarget.Target,
				"environment", req.Environment,
				"namespace", namespace,
				"endpoint", client.Endpoint(),
				"is_remote", isRemoteTarget,
				"error", pullErr,
			)
			if isRemoteTarget && grpcstatus.Code(pullErr) == grpccodes.Unavailable {
				return nil, &RemoteDeploymentUnavailableError{
					Deployment: resolvedTarget.Deployment,
					Target:     resolvedTarget.Target,
					Err:        pullErr,
				}
			}
			return nil, pullErr
		}
		if err := mergePullSchemaResponse(merged, resp, namespace); err != nil {
			span.RecordError(err)
			span.SetStatus(otelcodes.Error, "merge pull schema response")
			return nil, err
		}
	}

	span.SetAttributes(attribute.Int("table_count", int(merged.TableCount)))
	s.logger.Info("ExecutePullSchema: pull schema response",
		"database", merged.Database,
		"type", merged.Type,
		"environment", merged.Environment,
		"table_count", merged.TableCount,
		"namespace_count", len(merged.Namespaces),
	)

	return pullSchemaResponseFromProto(merged), nil
}

func pullNamespaces(namespaces []string) ([]string, error) {
	if len(namespaces) == 0 {
		return []string{""}, nil
	}
	result := make([]string, 0, len(namespaces))
	seenOutput := make(map[string]struct{}, len(namespaces))
	for _, namespace := range namespaces {
		if strings.TrimSpace(namespace) != namespace || namespace == "" {
			return nil, fmt.Errorf("pull namespace %q must be non-empty and contain no leading or trailing whitespace", namespace)
		}
		if strings.Contains(namespace, "..") || strings.ContainsAny(namespace, `/\`) {
			return nil, fmt.Errorf("pull namespace %q must be a single path component", namespace)
		}
		if strings.Contains(namespace, "$ENV") {
			return nil, fmt.Errorf("pull namespace %q must be a concrete live namespace; resolve $ENV before calling pull", namespace)
		}
		if schema.IsReservedPullNamespace(namespace) {
			return nil, fmt.Errorf("pull namespace %q is reserved and cannot be pulled", namespace)
		}
		if _, ok := seenOutput[namespace]; ok {
			return nil, fmt.Errorf("duplicate pull namespace %q", namespace)
		}
		seenOutput[namespace] = struct{}{}
		result = append(result, namespace)
	}
	return result, nil
}

func pullCatalogDetail(detail string) (ternv1.PullCatalogDetail, error) {
	switch detail {
	case "", "basic":
		return ternv1.PullCatalogDetail_PULL_CATALOG_DETAIL_BASIC, nil
	case "detailed":
		return ternv1.PullCatalogDetail_PULL_CATALOG_DETAIL_DETAILED, nil
	default:
		return ternv1.PullCatalogDetail_PULL_CATALOG_DETAIL_BASIC, fmt.Errorf("catalog_detail %q must be basic or detailed", detail)
	}
}

func mergePullSchemaResponse(merged, resp *ternv1.PullSchemaResponse, requestedNamespace string) error {
	if resp == nil {
		return fmt.Errorf("pull schema response is empty")
	}
	if requestedNamespace != "" {
		if len(resp.Namespaces) != 1 {
			return fmt.Errorf("pull namespace %q returned %d namespaces; expected 1", requestedNamespace, len(resp.Namespaces))
		}
		for responseNamespace, pulled := range resp.Namespaces {
			if responseNamespace != requestedNamespace {
				return fmt.Errorf("pull namespace %q returned namespace %q", requestedNamespace, responseNamespace)
			}
			if _, ok := merged.Namespaces[responseNamespace]; ok {
				return fmt.Errorf("pull schema response contains duplicate namespace %q", responseNamespace)
			}
			merged.Namespaces[responseNamespace] = pulled
		}
		merged.TableCount += resp.TableCount
		return nil
	}
	for responseNamespace, pulled := range resp.Namespaces {
		if _, ok := merged.Namespaces[responseNamespace]; ok {
			return fmt.Errorf("pull schema response contains duplicate namespace %q", responseNamespace)
		}
		merged.Namespaces[responseNamespace] = pulled
	}
	merged.TableCount += resp.TableCount
	return nil
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
		s.writeBodyDecodeError(w, err)
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

	resolvedTarget, err := s.config.ResolvePrimaryDatabaseTarget(req.Database, req.Environment)
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

	route := storedPlanRoute{
		DatabaseType: resolvedTarget.DatabaseType,
		Deployment:   deployment,
		Target:       resolvedTarget.Target,
	}
	if err := s.storePlanResponse(ctx, req, resp, route); err != nil {
		return nil, err
	}

	return planResponseFromProto(resp), nil
}

type storedPlanRoute struct {
	DatabaseType string
	Deployment   string
	Target       string
}

func (s *Service) storePlanResponse(ctx context.Context, req PlanRequest, resp *ternv1.PlanResponse, route storedPlanRoute) error {
	prInt := 0
	if req.PullRequest != nil {
		prInt = int(*req.PullRequest)
	}
	trustedSchemaPath := ""
	if req.SourceTrusted {
		trustedSchemaPath = req.SchemaPath
	}
	headSHA := ""
	if req.HeadSHA != nil {
		headSHA = *req.HeadSHA
	}
	namespaces, err := protoChangesToNamespaces(resp.Changes, req.SchemaFiles)
	if err != nil {
		return fmt.Errorf("convert plan namespaces: %w", err)
	}
	storedPlan := &storage.Plan{
		PlanIdentifier: resp.PlanId,
		Database:       req.Database,
		DatabaseType:   route.DatabaseType,
		Deployment:     route.Deployment,
		Target:         route.Target,
		Repository:     req.Repository,
		PullRequest:    prInt,
		SchemaPath:     trustedSchemaPath,
		Environment:    req.Environment,
		SchemaFiles:    protoToSchemaFiles(req.SchemaFiles),
		Namespaces:     namespaces,
		HeadSHA:        headSHA,
		CreatedAt:      time.Now(),
	}
	if _, err := s.storage.Plans().Create(ctx, storedPlan); err != nil && !errors.Is(err, storage.ErrPlanIDExists) {
		return fmt.Errorf("store plan: %w", err)
	}
	return nil
}

// handleApply handles POST /api/apply requests.
func (s *Service) handleApply(w http.ResponseWriter, r *http.Request) {
	var req ApplyRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		s.writeBodyDecodeError(w, err)
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
// durable. Operator drivers own dispatching queued work through the Tern
// client so request cancellation cannot orphan in-memory execution.
//
// Flow:
//  1. Load the plan from SchemaBot storage (source of truth for database, DDL changes).
//  2. Resolve the Tern client to validate the deployment/environment.
//  3. Create a pending Apply record and pending Task records from the plan.
//  4. Attach any pending observer to the stored apply before dispatch can start.
//  5. Wake an operator driver so fresh applies usually start immediately.
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
// and wakes an operator driver. Callers must have run loadPlanForApply first;
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

	applyIdentifier, storedApplyID, err := s.enqueueApply(ctx, plan, req, options, attachObserver)
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
	options map[string]string,
	onApplyCreated func(int64),
) (string, int64, error) {
	applyIdentifier := "apply-" + strings.ReplaceAll(uuid.New().String(), "-", "")[:16]
	apply, storedApplyID, err := s.createStoredApply(ctx, plan, req, options, applyIdentifier, "")
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
	options map[string]string,
	applyIdentifier string,
	externalID string,
) (*storage.Apply, int64, error) {
	now := time.Now()
	applyOpts := storage.ApplyOptionsFromMap(options)

	// The plan already carries the resolved primary (deployment, target) from
	// plan time, and is authoritative for single-deployment applies and the
	// config-light trusted control-plane enqueue path. Multi-deployment fan-out
	// additionally needs the full ordered target set, which only the server
	// config knows; use it only when it defines more than one deployment so
	// single-deployment creation stays unchanged and does not depend on
	// database config being present.
	targets := []routing.ExecutionTarget{{
		DatabaseType: plan.DatabaseType,
		Deployment:   plan.Deployment,
		Target:       plan.Target,
	}}
	if resolved, err := s.config.ResolveDatabaseTargets(plan.Database, req.Environment); err != nil {
		s.logger.Debug("createStoredApply: using plan's stored target; config did not resolve database targets",
			"database", plan.Database, "environment", req.Environment, "error", err)
	} else if len(resolved) > 1 {
		targets = resolved
	}

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
		Deployment:      targets[0].Deployment,
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
	buildTasks := func() []*storage.Task {
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
		return tasks
	}

	cutoverPolicy := s.config.CutoverPolicyFor(plan.Database, req.Environment)
	onFailure := s.config.OnFailure(plan.Database, req.Environment)
	groups := make([]*storage.ApplyOperationWithTasks, 0, len(targets))
	for _, target := range targets {
		groups = append(groups, &storage.ApplyOperationWithTasks{
			Operation: &storage.ApplyOperation{
				Deployment:    target.Deployment,
				Target:        target.Target,
				State:         state.ApplyOperation.Pending,
				CutoverPolicy: cutoverPolicy,
				OnFailure:     onFailure,
				CreatedAt:     now,
				UpdatedAt:     now,
			},
			Tasks: buildTasks(),
		})
	}

	storedApplyID, err := s.storage.Applies().CreateWithGroupedOperations(ctx, apply, groups)
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
		if nsData.Artifacts[vSchemaArtifactName] != "" {
			changes = append(changes, storage.TableChange{
				Table:     "VSchema: " + namespace,
				Namespace: namespace,
				Operation: "vschema_update",
			})
		}
	}
	return changes
}

// ExecuteRollbackPlanForApply generates a rollback plan for a specific apply.
func (s *Service) ExecuteRollbackPlanForApply(ctx context.Context, apply *storage.Apply) (*apitypes.PlanResponse, error) {
	if apply == nil {
		return nil, fmt.Errorf("apply is required")
	}
	if !state.IsState(apply.State, state.Apply.Completed) {
		return nil, fmt.Errorf("apply %s is in state %q; only completed applies can be rolled back", apply.ApplyIdentifier, apply.State)
	}

	plan, err := s.storage.Plans().GetByID(ctx, apply.PlanID)
	if err != nil {
		return nil, fmt.Errorf("get rollback source plan: %w", err)
	}
	if plan == nil {
		return nil, fmt.Errorf("plan not found for apply %s", apply.ApplyIdentifier)
	}
	if !rollbackSourcePlanMatchesApply(plan, apply) {
		return nil, fmt.Errorf("source plan %s belongs to %s/%s/%s, not apply %s for %s/%s/%s",
			plan.PlanIdentifier, plan.Database, plan.DatabaseType, plan.Environment,
			apply.ApplyIdentifier, apply.Database, apply.DatabaseType, apply.Environment)
	}
	schemaFiles, err := rollbackSchemaFiles(plan)
	if err != nil {
		return nil, err
	}

	deployment, err := storedDeploymentForApply(apply)
	if err != nil {
		return nil, err
	}
	if plan.Target == "" {
		return nil, fmt.Errorf("plan %s is missing server-side routing metadata field %q; create a new plan and retry rollback", plan.PlanIdentifier, "target")
	}
	client, err := s.TernClient(deployment, apply.Environment)
	if err != nil {
		return nil, fmt.Errorf("database %q (%s): %w", apply.Database, apply.Environment, err)
	}

	prNumber := int32(apply.PullRequest)
	req := PlanRequest{
		Database:    apply.Database,
		Environment: apply.Environment,
		Type:        apply.DatabaseType,
		SchemaFiles: schemaFiles,
		Repository:  apply.Repository,
		PullRequest: &prNumber,
	}
	resp, err := client.Plan(ctx, &ternv1.PlanRequest{
		Database:    req.Database,
		Type:        req.Type,
		SchemaFiles: req.SchemaFiles,
		Repository:  req.Repository,
		PullRequest: prNumber,
		Environment: req.Environment,
		Target:      plan.Target,
	})
	if err != nil {
		return nil, err
	}
	route := storedPlanRoute{
		DatabaseType: apply.DatabaseType,
		Deployment:   deployment,
		Target:       plan.Target,
	}
	if err := s.storePlanResponse(ctx, req, resp, route); err != nil {
		return nil, err
	}

	return planResponseFromProto(resp), nil
}

func rollbackSourcePlanMatchesApply(plan *storage.Plan, apply *storage.Apply) bool {
	return plan.Database == apply.Database &&
		plan.DatabaseType == apply.DatabaseType &&
		plan.Environment == apply.Environment
}

func rollbackSchemaFiles(plan *storage.Plan) (map[string]*ternv1.SchemaFiles, error) {
	schemaFiles := make(map[string]*ternv1.SchemaFiles)
	for ns, nsData := range plan.Namespaces {
		if nsData == nil || !nsData.OriginalFilesCaptured {
			return nil, fmt.Errorf("no original schema files available for rollback namespace %q", ns)
		}
		files := make(map[string]string, len(nsData.OriginalFiles))
		maps.Copy(files, nsData.OriginalFiles)
		schemaFiles[ns] = &ternv1.SchemaFiles{Files: files}
	}
	if len(schemaFiles) == 0 {
		return nil, fmt.Errorf("no namespaces available for rollback")
	}
	return schemaFiles, nil
}

// validateSchemaFiles checks that schema_files has at least one namespace and
// that every namespace carries a non-null value. An empty Files map within a
// namespace is valid (signals "drop all tables"), so we only reject when
// schema_files itself is missing or a namespace value is null.
//
// A null namespace value (e.g. JSON `{"default": null}`) is rejected as a hard
// error: it cannot be converted to schema files and is almost always a
// malformed request.
//
// Returns a warning message if any namespace has an empty (but non-null) files
// map (could indicate a JSON field name bug like "sql_files" instead of
// "files"). Callers should log this but not reject the request.
func validateSchemaFiles(schemaFiles map[string]*ternv1.SchemaFiles) (warning string, err error) {
	if len(schemaFiles) == 0 {
		return "", fmt.Errorf("schema_files is required: must contain at least one namespace (JSON field for files is \"files\", not \"sql_files\")")
	}
	for ns, sf := range schemaFiles {
		if sf == nil {
			return "", fmt.Errorf("schema_files[%q] is null: each namespace must be an object with a \"files\" map", ns)
		}
		if len(sf.GetFiles()) == 0 {
			warning = fmt.Sprintf("schema_files[%q] has no files — if this is unintentional, check that the JSON field is \"files\" (not \"sql_files\")", ns)
		}
	}
	return warning, nil
}
