package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// handleRedrive handles POST /api/redrive requests.
func (s *Service) handleRedrive(w http.ResponseWriter, r *http.Request) {
	var req apitypes.ControlRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		s.writeBodyDecodeError(w, err)
		return
	}

	resp, err := s.ExecuteRedrive(r.Context(), req)
	if err != nil {
		status := controlOperationHTTPStatus(err)
		switch {
		case errors.Is(err, storage.ErrApplyNotFound):
			status = http.StatusNotFound
		case errors.Is(err, storage.ErrApplyNotRedrivable), errors.Is(err, storage.ErrActiveApplyExists):
			status = http.StatusConflict
		}
		if status >= http.StatusInternalServerError {
			s.logger.Error("redrive failed", "apply_id", req.ApplyID, "environment", req.Environment, "error", err)
		} else {
			s.logger.Warn("redrive rejected", "apply_id", req.ApplyID, "environment", req.Environment, "error", err)
		}
		s.writeError(w, status, "redrive failed: "+err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, resp)
}

// ExecuteRedrive re-plans a failed apply's stored desired schema before
// redriving the original apply. If the fresh plan differs, the failed apply is
// left untouched so the caller can make an explicit new apply decision.
func (s *Service) ExecuteRedrive(ctx context.Context, req apitypes.ControlRequest) (*apitypes.RedriveResponse, error) {
	if req.ApplyID == "" {
		return nil, controlHTTPErrorf(http.StatusBadRequest, "apply_id is required")
	}
	if req.Environment == "" {
		return nil, controlHTTPErrorf(http.StatusBadRequest, "environment is required")
	}
	if s.storage == nil {
		return nil, fmt.Errorf("storage is not available")
	}

	apply, err := s.storage.Applies().GetByApplyIdentifier(ctx, req.ApplyID)
	if err != nil {
		return nil, fmt.Errorf("load apply %s for redrive: %w", req.ApplyID, err)
	}
	if apply == nil {
		return nil, storage.ErrApplyNotFound
	}
	if apply.Environment != req.Environment {
		return nil, controlHTTPErrorf(http.StatusBadRequest, "apply %q belongs to environment %q, not %q", req.ApplyID, apply.Environment, req.Environment)
	}
	if err := validateRedriveCandidate(apply); err != nil {
		return nil, err
	}

	sourcePlan, err := s.storage.Plans().GetByID(ctx, apply.PlanID)
	if err != nil {
		return nil, fmt.Errorf("load plan %d for redrive apply %s: %w", apply.PlanID, apply.ApplyIdentifier, err)
	}
	if sourcePlan == nil {
		return nil, fmt.Errorf("plan %d not found for redrive apply %s: %w", apply.PlanID, apply.ApplyIdentifier, storage.ErrPlanNotFound)
	}

	freshPlanResp, err := s.ExecutePlan(ctx, redrivePlanRequest(sourcePlan))
	if err != nil {
		return nil, fmt.Errorf("re-plan apply %s for redrive: %w", apply.ApplyIdentifier, err)
	}
	freshPlan, err := s.storage.Plans().Get(ctx, freshPlanResp.PlanID)
	if err != nil {
		return nil, fmt.Errorf("load redrive plan %s for apply %s: %w", freshPlanResp.PlanID, apply.ApplyIdentifier, err)
	}
	if freshPlan == nil {
		return nil, fmt.Errorf("redrive plan %s was not stored for apply %s: %w", freshPlanResp.PlanID, apply.ApplyIdentifier, storage.ErrPlanNotFound)
	}

	expectedPlan, err := s.redriveRemainingPlan(ctx, apply, sourcePlan)
	if err != nil {
		return nil, err
	}
	if !plansEquivalentForRedrive(expectedPlan, freshPlan) {
		s.logger.Warn("redrive rejected because re-plan changed",
			"apply_id", apply.ApplyIdentifier,
			"database", apply.Database,
			"database_type", apply.DatabaseType,
			"deployment", apply.Deployment,
			"environment", apply.Environment,
			"source_plan_id", sourcePlan.PlanIdentifier,
			"fresh_plan_id", freshPlan.PlanIdentifier,
			"requested_by", req.Caller)
		return nil, fmt.Errorf("re-plan for apply %s changed from stored plan %s to fresh plan %s; create a new apply instead: %w",
			apply.ApplyIdentifier, sourcePlan.PlanIdentifier, freshPlan.PlanIdentifier, storage.ErrApplyNotRedrivable)
	}

	redriven, err := s.storage.Applies().RedriveFailed(ctx, apply.ID)
	if err != nil {
		return nil, fmt.Errorf("redrive failed apply %s: %w", apply.ApplyIdentifier, err)
	}
	s.logControlOperationForApply(ctx, redriven, req.Caller, storage.LogEventInfo, "Redrive requested by user")
	s.logger.Info("redrive accepted for failed apply",
		"apply_id", redriven.ApplyIdentifier,
		"database", redriven.Database,
		"database_type", redriven.DatabaseType,
		"deployment", redriven.Deployment,
		"environment", redriven.Environment,
		"source_plan_id", sourcePlan.PlanIdentifier,
		"fresh_plan_id", freshPlan.PlanIdentifier,
		"requested_by", req.Caller)
	s.wakeOperator(redriven.ApplyIdentifier, redriven.Database, redriven.Environment)
	return &apitypes.RedriveResponse{
		Accepted: true,
		Status:   apitypes.RedriveStatusRedriven,
		ApplyID:  redriven.ApplyIdentifier,
		PlanID:   sourcePlan.PlanIdentifier,
		Message:  "Stored plan still matches; the failed apply was queued for redrive.",
	}, nil
}

func validateRedriveCandidate(apply *storage.Apply) error {
	if !state.IsState(apply.State, state.Apply.Failed) {
		return fmt.Errorf("apply %s is %s; only failed applies can be redriven: %w", apply.ApplyIdentifier, apply.State, storage.ErrApplyNotRedrivable)
	}
	if apply.CompletedAt == nil {
		return fmt.Errorf("apply %s has no failure completion time; create a new apply instead: %w", apply.ApplyIdentifier, storage.ErrApplyNotRedrivable)
	}
	if apply.CompletedAt.Before(time.Now().AddDate(0, 0, -storage.RedriveFailureFreshnessDays)) {
		return fmt.Errorf("apply %s failed more than %d day(s) ago; create a new apply instead: %w", apply.ApplyIdentifier, storage.RedriveFailureFreshnessDays, storage.ErrApplyNotRedrivable)
	}
	return nil
}

func (s *Service) redriveRemainingPlan(ctx context.Context, apply *storage.Apply, sourcePlan *storage.Plan) (*storage.Plan, error) {
	selection, err := s.redrivableWorkSelection(ctx, apply, sourcePlan)
	if err != nil {
		return nil, err
	}
	return filterPlanToRedrivableWork(sourcePlan, selection), nil
}

func (s *Service) redrivableWorkSelection(ctx context.Context, apply *storage.Apply, sourcePlan *storage.Plan) (redriveWorkSelection, error) {
	selection := redriveWorkSelection{
		taskKeys:            make(map[redriveTaskKey]bool),
		finalizerNamespaces: make(map[string]bool),
		deployments:         make(map[string]bool),
	}

	operations, err := s.storage.ApplyOperations().ListByApply(ctx, apply.ID)
	if err != nil {
		return selection, fmt.Errorf("load operations for redrive apply %s: %w", apply.ApplyIdentifier, err)
	}
	operationsByID := make(map[int64]*storage.ApplyOperation, len(operations))
	for _, operation := range operations {
		operationsByID[operation.ID] = operation
	}

	legacyTasks, err := s.storage.Tasks().GetByApplyID(ctx, apply.ID)
	if err != nil {
		return selection, fmt.Errorf("load tasks for redrive apply %s: %w", apply.ApplyIdentifier, err)
	}
	for _, task := range legacyTasks {
		if !state.IsState(task.State, state.Task.Failed, state.Task.Cancelled) {
			continue
		}
		if task.ApplyOperationID != nil {
			operation := operationsByID[*task.ApplyOperationID]
			if operation == nil || !state.IsState(operation.State, state.ApplyOperation.Failed) {
				continue
			}
			selection.deployments[operation.Deployment] = true
		} else {
			selection.deployments[sourcePlan.Deployment] = true
		}
		selection.taskKeys[redriveTaskKeyFor(task)] = true
	}

	for _, operation := range operations {
		if !state.IsState(operation.State, state.ApplyOperation.Failed) {
			continue
		}
		selection.deployments[operation.Deployment] = true
		tasks, err := s.storage.Tasks().GetByApplyOperationID(ctx, operation.ID)
		if err != nil {
			return selection, fmt.Errorf("load operation %d tasks for redrive apply %s: %w", operation.ID, apply.ApplyIdentifier, err)
		}
		if len(tasks) == 0 && operation.OperationKind == storage.ApplyOperationKindGroupFinalizer {
			namespace, ok := redriveFinalizerNamespace(operation.OperationKey)
			if !ok {
				return selection, fmt.Errorf("operation %d has malformed finalizer key %q for redrive apply %s: %w", operation.ID, operation.OperationKey, apply.ApplyIdentifier, storage.ErrApplyNotRedrivable)
			}
			selection.finalizerNamespaces[namespace] = true
		}
		for _, task := range tasks {
			if state.IsState(task.State, state.Task.Failed, state.Task.Cancelled) {
				selection.taskKeys[redriveTaskKeyFor(task)] = true
			}
		}
	}

	if len(selection.deployments) == 0 {
		return selection, fmt.Errorf("apply %s has no failed work to redrive: %w", apply.ApplyIdentifier, storage.ErrApplyNotRedrivable)
	}
	if len(selection.deployments) > 1 || !selection.deployments[sourcePlan.Deployment] {
		return selection, fmt.Errorf("apply %s has failed work outside primary deployment %q; create a new apply instead: %w", apply.ApplyIdentifier, sourcePlan.Deployment, storage.ErrApplyNotRedrivable)
	}
	return selection, nil
}

type redriveWorkSelection struct {
	taskKeys            map[redriveTaskKey]bool
	finalizerNamespaces map[string]bool
	deployments         map[string]bool
}

type redriveTaskKey struct {
	Namespace string
	Shard     string
	Table     string
}

func redriveTaskKeyFor(task *storage.Task) redriveTaskKey {
	return redriveTaskKey{Namespace: task.Namespace, Shard: task.Shard, Table: task.TableName}
}

func filterPlanToRedrivableWork(plan *storage.Plan, selection redriveWorkSelection) *storage.Plan {
	filtered := *plan
	filtered.Namespaces = make(map[string]*storage.NamespacePlanData, len(plan.Namespaces))
	for namespace, nsData := range plan.Namespaces {
		if nsData == nil {
			continue
		}
		filteredNS := filterRedrivableNamespaceWork(namespace, nsData, selection)
		if len(filteredNS.Tables) > 0 || len(filteredNS.Artifacts) > 0 {
			filtered.Namespaces[namespace] = filteredNS
		}
	}

	filtered.Shards = make([]storage.ShardPlan, 0, len(plan.Shards))
	for _, shard := range plan.Shards {
		changes := make([]storage.TableChange, 0, len(shard.Changes))
		for _, change := range shard.Changes {
			if selection.taskKeys[redriveTaskKey{Namespace: shard.Namespace, Shard: shard.Shard, Table: change.Table}] {
				changes = append(changes, change)
			}
		}
		if len(changes) == 0 {
			continue
		}
		filtered.Shards = append(filtered.Shards, storage.ShardPlan{Namespace: shard.Namespace, Shard: shard.Shard, Changes: changes})
	}
	return &filtered
}

func filterRedrivableNamespaceWork(namespace string, nsData *storage.NamespacePlanData, selection redriveWorkSelection) *storage.NamespacePlanData {
	filtered := &storage.NamespacePlanData{}
	if selection.finalizerNamespaces[namespace] {
		filtered.Artifacts = copyStringMap(nsData.Artifacts)
	}
	for _, change := range nsData.Tables {
		if selection.taskKeys[redriveTaskKey{Namespace: namespace, Table: change.Table}] {
			filtered.Tables = append(filtered.Tables, change)
		}
	}
	return filtered
}

func redriveFinalizerNamespace(operationKey string) (string, bool) {
	const suffix = "/group_finalizer"
	if !strings.HasSuffix(operationKey, suffix) {
		return "", false
	}
	namespace := strings.TrimSuffix(operationKey, suffix)
	return namespace, namespace != ""
}

func copyStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	maps.Copy(out, in)
	return out
}

func redrivePlanRequest(plan *storage.Plan) PlanRequest {
	req := PlanRequest{
		Database:      plan.Database,
		Environment:   plan.Environment,
		Type:          plan.DatabaseType,
		SchemaFiles:   storageSchemaFilesToProto(plan.SchemaFiles),
		Repository:    plan.Repository,
		SchemaPath:    plan.SchemaPath,
		SourceTrusted: plan.SchemaPath != "",
	}
	if plan.PullRequest > 0 {
		pr := int32(plan.PullRequest)
		req.PullRequest = &pr
	}
	if plan.HeadSHA != "" {
		headSHA := plan.HeadSHA
		req.HeadSHA = &headSHA
	}
	return req
}

func storageSchemaFilesToProto(files schema.SchemaFiles) map[string]*ternv1.SchemaFiles {
	result := make(map[string]*ternv1.SchemaFiles, len(files))
	for namespace, ns := range files {
		protoFiles := make(map[string]string)
		if ns != nil {
			protoFiles = make(map[string]string, len(ns.Files))
			maps.Copy(protoFiles, ns.Files)
		}
		result[namespace] = &ternv1.SchemaFiles{Files: protoFiles}
	}
	return result
}

func plansEquivalentForRedrive(a, b *storage.Plan) bool {
	aJSON, aErr := json.Marshal(redrivePlanFingerprintFor(a))
	bJSON, bErr := json.Marshal(redrivePlanFingerprintFor(b))
	return aErr == nil && bErr == nil && bytes.Equal(aJSON, bJSON)
}

type redrivePlanFingerprint struct {
	Database     string
	DatabaseType string
	Deployment   string
	Target       string
	Environment  string
	Namespaces   map[string]redriveNamespaceFingerprint
	Shards       []storage.ShardPlan
}

type redriveNamespaceFingerprint struct {
	Tables    []storage.TableChange
	Artifacts map[string]string
}

func redrivePlanFingerprintFor(plan *storage.Plan) redrivePlanFingerprint {
	namespaces := make(map[string]redriveNamespaceFingerprint, len(plan.Namespaces))
	for namespace, nsData := range plan.Namespaces {
		if nsData == nil {
			continue
		}
		tables := append([]storage.TableChange(nil), nsData.Tables...)
		sort.Slice(tables, func(i, j int) bool {
			if tables[i].Table != tables[j].Table {
				return tables[i].Table < tables[j].Table
			}
			if tables[i].Operation != tables[j].Operation {
				return tables[i].Operation < tables[j].Operation
			}
			return tables[i].DDL < tables[j].DDL
		})
		namespaces[namespace] = redriveNamespaceFingerprint{Tables: tables, Artifacts: nsData.Artifacts}
	}

	shards := make([]storage.ShardPlan, 0, len(plan.Shards))
	for _, shard := range plan.Shards {
		if len(shard.Changes) == 0 {
			continue
		}
		changes := append([]storage.TableChange(nil), shard.Changes...)
		sort.Slice(changes, func(i, j int) bool {
			if changes[i].Table != changes[j].Table {
				return changes[i].Table < changes[j].Table
			}
			if changes[i].Operation != changes[j].Operation {
				return changes[i].Operation < changes[j].Operation
			}
			return changes[i].DDL < changes[j].DDL
		})
		shards = append(shards, storage.ShardPlan{Namespace: shard.Namespace, Shard: shard.Shard, Changes: changes})
	}
	sort.Slice(shards, func(i, j int) bool {
		if shards[i].Namespace != shards[j].Namespace {
			return shards[i].Namespace < shards[j].Namespace
		}
		return shards[i].Shard < shards[j].Shard
	})
	return redrivePlanFingerprint{
		Database:     plan.Database,
		DatabaseType: plan.DatabaseType,
		Deployment:   plan.Deployment,
		Target:       plan.Target,
		Environment:  plan.Environment,
		Namespaces:   namespaces,
		Shards:       shards,
	}
}
