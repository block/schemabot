// plans_handlers.go implements the stored-plan read API: listing recent plans
// and fetching one stored plan's full content. Plans are durable history —
// nothing deletes them after creation — so these endpoints are how operators
// audit what was planned against a database without digging through PRs.
package api

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/storage"
)

const (
	defaultPlansLimit = 20
	// maxPlansLimit is far below the status cap because every listed plan row
	// carries its full stored plan data (each namespace's table DDL and
	// captured original files), so a page of plans is orders of magnitude
	// heavier than a page of applies.
	maxPlansLimit = 200
)

// handlePlansList handles GET /api/plans requests: recent stored plans as
// summaries, newest first.
func (s *Service) handlePlansList(w http.ResponseWriter, r *http.Request) {
	limit, err := parsePlansLimit(r)
	if err != nil {
		s.writeErrorCode(w, http.StatusBadRequest, apitypes.ErrCodeInvalidRequest, err.Error())
		return
	}
	last, err := parsePlansLast(r)
	if err != nil {
		s.writeErrorCode(w, http.StatusBadRequest, apitypes.ErrCodeInvalidRequest, err.Error())
		return
	}
	pullRequest, err := parsePlansPullRequest(r)
	if err != nil {
		s.writeErrorCode(w, http.StatusBadRequest, apitypes.ErrCodeInvalidRequest, err.Error())
		return
	}

	opts := storage.ListPlansOptions{
		Limit:       limit + 1,
		Database:    storage.CanonicalKey(r.URL.Query().Get("database")),
		Environment: storage.CanonicalKey(r.URL.Query().Get("environment")),
		Repository:  storage.CanonicalKey(r.URL.Query().Get("repository")),
		PullRequest: pullRequest,
	}
	if last > 0 {
		opts.Since = time.Now().Add(-last)
	}

	plans, err := s.storage.Plans().List(r.Context(), opts)
	if err != nil {
		s.logger.Error("list plans failed",
			"database", opts.Database,
			"environment", opts.Environment,
			"repository", opts.Repository,
			"pull_request", opts.PullRequest,
			"error", err)
		s.writeErrorCode(w, http.StatusInternalServerError, apitypes.ErrCodeStorageError, "failed to list plans")
		return
	}
	hasMore := len(plans) > limit
	if hasMore {
		plans = plans[:limit]
	}

	resp := &apitypes.PlansResponse{
		Limit:    limit,
		MaxLimit: maxPlansLimit,
		HasMore:  hasMore,
		Plans:    make([]*apitypes.PlanSummaryResponse, 0, len(plans)),
	}
	if last > 0 {
		resp.Last = last.String()
	}
	for _, plan := range plans {
		resp.Plans = append(resp.Plans, planSummaryFromStorage(plan))
	}
	s.writeJSON(w, http.StatusOK, resp)
}

// handlePlanGet handles GET /api/plans/{plan_identifier} requests: one stored
// plan with its full content.
func (s *Service) handlePlanGet(w http.ResponseWriter, r *http.Request) {
	planIdentifier := r.PathValue("plan_identifier")
	if planIdentifier == "" {
		s.writeErrorCode(w, http.StatusBadRequest, apitypes.ErrCodeInvalidRequest, "plan_identifier is required")
		return
	}
	plan, err := s.storage.Plans().Get(r.Context(), planIdentifier)
	if err != nil {
		s.logger.Error("get stored plan failed", "plan_id", planIdentifier, "error", err)
		s.writeErrorCode(w, http.StatusInternalServerError, apitypes.ErrCodeStorageError, "failed to get plan")
		return
	}
	if plan == nil {
		s.writeErrorCode(w, http.StatusNotFound, apitypes.ErrCodeNotFound, "plan not found: "+planIdentifier)
		return
	}
	s.writeJSON(w, http.StatusOK, storedPlanResponseFromStorage(plan))
}

func parsePlansLimit(r *http.Request) (int, error) {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return defaultPlansLimit, nil
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit <= 0 {
		return 0, fmt.Errorf("limit must be a positive integer")
	}
	if limit > maxPlansLimit {
		return maxPlansLimit, nil
	}
	return limit, nil
}

// parsePlansLast parses the optional `last` query parameter bounding the
// listing to plans created within the window. Zero means unbounded.
func parsePlansLast(r *http.Request) (time.Duration, error) {
	raw := r.URL.Query().Get("last")
	if raw == "" {
		return 0, nil
	}
	last, err := time.ParseDuration(raw)
	if err != nil || last <= 0 {
		return 0, fmt.Errorf("last must be a positive duration such as 30m or 24h")
	}
	return last, nil
}

// parsePlansPullRequest parses the optional `pull_request` query parameter.
// A PR number is only meaningful within one repository, so it requires the
// `repository` parameter rather than silently matching the same number across
// every repo.
func parsePlansPullRequest(r *http.Request) (int, error) {
	raw := r.URL.Query().Get("pull_request")
	if raw == "" {
		return 0, nil
	}
	if r.URL.Query().Get("repository") == "" {
		return 0, fmt.Errorf("pull_request requires repository")
	}
	pullRequest, err := strconv.Atoi(raw)
	if err != nil || pullRequest <= 0 {
		return 0, fmt.Errorf("pull_request must be a positive integer")
	}
	return pullRequest, nil
}

// planSummaryFromStorage derives the list-row view of a stored plan: its
// provenance plus change counts, without the plan content.
func planSummaryFromStorage(plan *storage.Plan) *apitypes.PlanSummaryResponse {
	summary := &apitypes.PlanSummaryResponse{
		PlanID:       plan.PlanIdentifier,
		Database:     plan.Database,
		DatabaseType: plan.DatabaseType,
		Deployment:   plan.Deployment,
		Environment:  plan.Environment,
		Repository:   plan.Repository,
		PullRequest:  plan.PullRequest,
		HeadSHA:      plan.HeadSHA,
		CreatedAt:    plan.CreatedAt,
	}
	for _, namespace := range sortedPlanNamespaces(plan.Namespaces) {
		nsData := plan.Namespaces[namespace]
		if nsData == nil {
			continue
		}
		if nsData.ChangesVSchema() {
			summary.VSchemaChangeCount++
		}
		for _, change := range nsData.Tables {
			if summary.ChangeCounts == nil {
				summary.ChangeCounts = map[string]int{}
			}
			operation := change.Operation
			if operation == "" {
				operation = "other"
			}
			summary.ChangeCounts[operation]++
			if change.IsUnsafe {
				summary.UnsafeCount++
			}
			if change.ExecutionMode == engine.ExecutionModeBlocked {
				summary.BlockedCount++
			}
		}
	}
	return summary
}

// storedPlanResponseFromStorage builds the full single-plan response: the
// summary plus the stored plan content.
func storedPlanResponseFromStorage(plan *storage.Plan) *apitypes.StoredPlanResponse {
	return &apitypes.StoredPlanResponse{
		PlanSummaryResponse: *planSummaryFromStorage(plan),
		SchemaPath:          plan.SchemaPath,
		Target:              plan.Target,
		Plan:                planContentFromStorage(plan),
	}
}

// planContentFromStorage reconstructs the POST /api/plan response shape from a
// stored plan so both render through the same code paths. Lint results and
// errors are not persisted with a plan, so they are always empty here.
func planContentFromStorage(plan *storage.Plan) *apitypes.PlanResponse {
	resp := &apitypes.PlanResponse{
		PlanID:       plan.PlanIdentifier,
		Database:     plan.Database,
		DatabaseType: plan.DatabaseType,
		Environment:  plan.Environment,
		Deployment:   plan.Deployment,
		Engine:       storage.EngineForType(plan.DatabaseType),
		Changes:      []*apitypes.SchemaChangeResponse{},
		LintResults:  []*apitypes.LintViolationResponse{},
		Errors:       []string{},
	}
	for _, namespace := range sortedPlanNamespaces(plan.Namespaces) {
		nsData := plan.Namespaces[namespace]
		if nsData == nil {
			continue
		}
		change := &apitypes.SchemaChangeResponse{Namespace: namespace}
		if nsData.ChangesVSchema() {
			// The stored artifact is the desired VSchema document, not a
			// rendered diff, so the namespace is flagged as carrying VSchema
			// work without one.
			change.Metadata = map[string]string{apitypes.VSchemaChangedMetadataKey: "true"}
		}
		for _, table := range nsData.Tables {
			tc := tableChangeResponseFromStorage(table)
			if tc.Namespace == "" {
				tc.Namespace = namespace
			}
			change.TableChanges = append(change.TableChanges, tc)
		}
		resp.Changes = append(resp.Changes, change)
	}
	for _, shard := range plan.Shards {
		apiShard := &apitypes.ShardPlanResponse{Namespace: shard.Namespace, Shard: shard.Shard}
		for _, table := range shard.Changes {
			apiShard.Changes = append(apiShard.Changes, tableChangeResponseFromStorage(table))
		}
		resp.Shards = append(resp.Shards, apiShard)
	}
	return resp
}

func tableChangeResponseFromStorage(change storage.TableChange) *apitypes.TableChangeResponse {
	return &apitypes.TableChangeResponse{
		TableName:     change.Table,
		Namespace:     change.Namespace,
		DDL:           change.DDL,
		ChangeType:    change.Operation,
		IsUnsafe:      change.IsUnsafe,
		UnsafeReason:  change.UnsafeReason,
		ExecutionMode: change.ExecutionMode,
		ModeReason:    change.ModeReason,
	}
}

// sortedPlanNamespaces returns the plan's namespace keys in sorted order so
// responses are stable across requests.
func sortedPlanNamespaces(namespaces map[string]*storage.NamespacePlanData) []string {
	keys := make([]string, 0, len(namespaces))
	for namespace := range namespaces {
		keys = append(keys, namespace)
	}
	sort.Strings(keys)
	return keys
}
