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
	"github.com/block/schemabot/pkg/metrics"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/schema"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// handleReapply handles POST /api/reapply requests. Reapply is an
// operator-only control surface: it is reached through the Admin CLI and
// direct API calls, gated at the write tier, and is deliberately not exposed
// as a PR comment command — it acts on a failed apply outside the PR workflow.
func (s *Service) handleReapply(w http.ResponseWriter, r *http.Request) {
	var req apitypes.ControlRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		s.writeBodyDecodeError(w, err)
		return
	}

	resp, err := s.ExecuteReapply(r.Context(), req)
	if err != nil {
		status := reapplyHTTPStatus(err)
		message := "reapply failed: " + err.Error()
		if status >= http.StatusInternalServerError {
			s.logger.Error("reapply failed", "apply_id", req.ApplyID, "environment", req.Environment, "error", err)
		} else {
			message = "reapply rejected: " + err.Error()
			s.logger.Warn("reapply rejected", "apply_id", req.ApplyID, "environment", req.Environment, "error", err)
		}
		s.writeError(w, status, message)
		return
	}

	s.writeJSON(w, http.StatusOK, resp)
}

// reapplyHTTPStatus maps reapply errors to HTTP statuses, extending the shared
// control-operation mapping with the storage sentinels the reapply path
// returns: a missing apply is not found, and a non-reappliable or
// target-conflicting apply is a state conflict the requester can act on.
func reapplyHTTPStatus(err error) int {
	switch {
	case errors.Is(err, storage.ErrApplyNotFound):
		return http.StatusNotFound
	case errors.Is(err, storage.ErrApplyNotReappliable), errors.Is(err, storage.ErrActiveApplyExists), errors.Is(err, storage.ErrLockHeld):
		return http.StatusConflict
	}
	return controlOperationHTTPStatus(err)
}

// reapplyMetricStatus classifies a reapply outcome for the control-operation
// counter: rejections the requester can act on versus internal failures an
// operator must investigate.
func reapplyMetricStatus(err error) string {
	switch {
	case err == nil:
		return "success"
	case reapplyHTTPStatus(err) < http.StatusInternalServerError:
		return "rejected"
	default:
		return "error"
	}
}

// ExecuteReapply re-plans a failed apply's stored desired schema before
// reapplying the original apply. If the fresh plan differs, the failed apply is
// left untouched so the caller can make an explicit new apply decision.
func (s *Service) ExecuteReapply(ctx context.Context, req apitypes.ControlRequest) (*apitypes.ReapplyResponse, error) {
	if req.ApplyID == "" {
		return nil, controlHTTPErrorf(http.StatusBadRequest, "apply_id is required")
	}
	if req.Environment == "" {
		return nil, controlHTTPErrorf(http.StatusBadRequest, "environment is required")
	}
	// caller owns the lock acquired below, so require it up front — before any
	// apply/plan reads — rather than failing deep inside lock acquisition.
	caller := resolveCaller(ctx, req.Caller)
	if caller == "" {
		return nil, controlHTTPErrorf(http.StatusBadRequest, "caller is required")
	}
	if s.storage == nil {
		return nil, fmt.Errorf("storage is not available")
	}

	apply, err := s.storage.Applies().GetByApplyIdentifier(ctx, req.ApplyID)
	if err != nil {
		return nil, fmt.Errorf("load apply %s for reapply: %w", req.ApplyID, err)
	}
	if apply == nil {
		return nil, storage.ErrApplyNotFound
	}

	resp, err := s.executeReapplyForApply(ctx, req, apply, caller)
	metrics.RecordControlOperation(ctx, "reapply", apply.Database, apply.Deployment, apply.Environment, reapplyMetricStatus(err))
	return resp, err
}

// executeReapplyForApply runs the reapply decision for a loaded apply: it
// verifies the apply is a fresh terminal failure, re-plans the stored desired
// schema, and requeues the failed work only when the fresh plan still matches.
func (s *Service) executeReapplyForApply(ctx context.Context, req apitypes.ControlRequest, apply *storage.Apply, caller string) (*apitypes.ReapplyResponse, error) {
	if apply.Environment != req.Environment {
		return nil, controlHTTPErrorf(http.StatusBadRequest, "apply %q belongs to environment %q, not %q", req.ApplyID, apply.Environment, req.Environment)
	}
	if err := validateReapplyCandidate(apply); err != nil {
		return nil, err
	}

	sourcePlan, err := s.storage.Plans().GetByID(ctx, apply.PlanID)
	if err != nil {
		return nil, fmt.Errorf("load plan %d for reapply apply %s: %w", apply.PlanID, apply.ApplyIdentifier, err)
	}
	if sourcePlan == nil {
		return nil, fmt.Errorf("plan %d not found for reapply apply %s: %w", apply.PlanID, apply.ApplyIdentifier, storage.ErrPlanNotFound)
	}

	lock, releaseLockOnReject, err := s.acquireReapplyLock(ctx, caller, req.Force, sourcePlan, apply)
	if err != nil {
		return nil, err
	}
	reapplyAccepted := false
	defer func() {
		if !reapplyAccepted && releaseLockOnReject {
			s.releaseReapplyLockAfterReject(ctx, caller, sourcePlan, apply)
		}
	}()

	freshPlanResp, err := s.ExecutePlan(ctx, reapplyPlanRequest(sourcePlan))
	if err != nil {
		return nil, fmt.Errorf("re-plan apply %s for reapply: %w", apply.ApplyIdentifier, err)
	}
	freshPlan, err := s.storage.Plans().Get(ctx, freshPlanResp.PlanID)
	if err != nil {
		return nil, fmt.Errorf("load reapply plan %s for apply %s: %w", freshPlanResp.PlanID, apply.ApplyIdentifier, err)
	}
	if freshPlan == nil {
		return nil, fmt.Errorf("reapply plan %s was not stored for apply %s: %w", freshPlanResp.PlanID, apply.ApplyIdentifier, storage.ErrPlanNotFound)
	}

	expectedPlan, err := s.reapplyRemainingPlan(ctx, apply, sourcePlan)
	if err != nil {
		return nil, err
	}
	equivalent, err := plansEquivalentForReapply(expectedPlan, freshPlan)
	if err != nil {
		return nil, fmt.Errorf("compare reapply plans for apply %s: %w", apply.ApplyIdentifier, err)
	}
	if !equivalent {
		s.logger.Warn("reapply rejected because re-plan changed",
			append(apply.LogAttrs(),
				"source_plan_id", sourcePlan.PlanIdentifier,
				"fresh_plan_id", freshPlan.PlanIdentifier,
				"requested_by", caller)...)
		return nil, fmt.Errorf("re-plan for apply %s changed from stored plan %s to fresh plan %s; create a new apply instead: %w",
			apply.ApplyIdentifier, sourcePlan.PlanIdentifier, freshPlan.PlanIdentifier, storage.ErrApplyNotReappliable)
	}

	reapplied, err := s.storage.Applies().ReapplyFailed(ctx, apply.ID, lock.ID)
	if err != nil {
		return nil, fmt.Errorf("reapply failed apply %s: %w", apply.ApplyIdentifier, err)
	}
	reapplyAccepted = true
	s.logControlOperationForApply(ctx, reapplied, caller, storage.LogEventInfo, "Reapply requested by user")
	s.logger.Info("reapply accepted for failed apply",
		append(reapplied.LogAttrs(),
			"source_plan_id", sourcePlan.PlanIdentifier,
			"fresh_plan_id", freshPlan.PlanIdentifier,
			"requested_by", caller)...)
	s.wakeOperator(reapplied.ApplyIdentifier, reapplied.Database, reapplied.Environment)
	return &apitypes.ReapplyResponse{
		Accepted: true,
		Status:   apitypes.ReapplyStatusReapplied,
		ApplyID:  reapplied.ApplyIdentifier,
		PlanID:   sourcePlan.PlanIdentifier,
		Message:  "Stored plan still matches; the failed apply was queued for reapply.",
	}, nil
}

func (s *Service) acquireReapplyLock(ctx context.Context, caller string, force bool, plan *storage.Plan, apply *storage.Apply) (*storage.Lock, bool, error) {
	existing, err := s.storage.Locks().Get(ctx, plan.Database, plan.DatabaseType)
	if err != nil {
		return nil, false, fmt.Errorf("load reapply lock for %s/%s before acquire: %w", plan.Database, plan.DatabaseType, err)
	}
	releaseOnReject := existing == nil || existing.Owner != caller
	lock := &storage.Lock{
		DatabaseName: plan.Database,
		DatabaseType: plan.DatabaseType,
		Repository:   plan.Repository,
		PullRequest:  plan.PullRequest,
		Owner:        caller,
	}
	if err := s.storage.Locks().Acquire(ctx, lock); err != nil {
		if errors.Is(err, storage.ErrLockHeld) {
			if !force {
				return nil, false, fmt.Errorf("reapply lock for %s/%s is held by another owner; retry with force to take over the lock: %w", plan.Database, plan.DatabaseType, err)
			}
			s.logger.Warn("reapply force releasing database lock", append(apply.LogAttrs(), "requested_by", caller)...)
			// Release-then-acquire is intentionally non-atomic and fails closed: if a
			// third caller wins the lock in the gap, the re-Acquire below returns
			// ErrLockHeld and the reapply is rejected. Force never proceeds without
			// holding the lock.
			if releaseErr := s.storage.Locks().ForceRelease(ctx, plan.Database, plan.DatabaseType); releaseErr != nil {
				return nil, false, fmt.Errorf("force release reapply lock for %s/%s owner %s: %w", plan.Database, plan.DatabaseType, caller, releaseErr)
			}
			if acquireErr := s.storage.Locks().Acquire(ctx, lock); acquireErr != nil {
				return nil, false, fmt.Errorf("acquire reapply lock for %s/%s owner %s after force release: %w", plan.Database, plan.DatabaseType, caller, acquireErr)
			}
		} else {
			return nil, false, fmt.Errorf("acquire reapply lock for %s/%s owner %s: %w", plan.Database, plan.DatabaseType, caller, err)
		}
	}
	acquired, err := s.storage.Locks().Get(ctx, plan.Database, plan.DatabaseType)
	if err != nil {
		return nil, false, fmt.Errorf("load acquired reapply lock for %s/%s: %w", plan.Database, plan.DatabaseType, err)
	}
	if acquired == nil || acquired.Owner != caller {
		return nil, false, fmt.Errorf("reapply lock for %s/%s was not acquired by %s: %w", plan.Database, plan.DatabaseType, caller, storage.ErrLockHeld)
	}
	return acquired, releaseOnReject, nil
}

func (s *Service) releaseReapplyLockAfterReject(ctx context.Context, caller string, plan *storage.Plan, apply *storage.Apply) {
	if err := s.storage.Locks().Release(ctx, plan.Database, plan.DatabaseType, caller); err != nil {
		s.logger.Warn("reapply rejected after acquiring database lock; lock release failed", append(apply.LogAttrs(),
			"requested_by", caller,
			"error", err,
		)...)
		return
	}
	s.logger.Info("reapply rejected after acquiring database lock; lock released", append(apply.LogAttrs(), "requested_by", caller)...)
}

func validateReapplyCandidate(apply *storage.Apply) error {
	if !state.IsState(apply.State, state.Apply.Failed) {
		return fmt.Errorf("apply %s is %s; only failed applies can be reapplied: %w", apply.ApplyIdentifier, apply.State, storage.ErrApplyNotReappliable)
	}
	if apply.CompletedAt == nil {
		return fmt.Errorf("apply %s has no failure completion time; create a new apply instead: %w", apply.ApplyIdentifier, storage.ErrApplyNotReappliable)
	}
	if apply.CompletedAt.Before(time.Now().AddDate(0, 0, -storage.ReapplyFailureFreshnessDays)) {
		return fmt.Errorf("apply %s failed more than %d day(s) ago; create a new apply instead: %w", apply.ApplyIdentifier, storage.ReapplyFailureFreshnessDays, storage.ErrApplyNotReappliable)
	}
	return nil
}

func (s *Service) reapplyRemainingPlan(ctx context.Context, apply *storage.Apply, sourcePlan *storage.Plan) (*storage.Plan, error) {
	selection, err := s.reappliableWorkSelection(ctx, apply, sourcePlan)
	if err != nil {
		return nil, err
	}
	return filterPlanToReappliableWork(sourcePlan, selection), nil
}

func (s *Service) reappliableWorkSelection(ctx context.Context, apply *storage.Apply, sourcePlan *storage.Plan) (reapplyWorkSelection, error) {
	selection := reapplyWorkSelection{
		taskKeys:            make(map[reapplyTaskKey]bool),
		finalizerNamespaces: make(map[string]bool),
		deployments:         make(map[string]bool),
	}

	operations, err := s.storage.ApplyOperations().ListByApply(ctx, apply.ID)
	if err != nil {
		return selection, fmt.Errorf("load operations for reapply apply %s: %w", apply.ApplyIdentifier, err)
	}
	operationsByID := make(map[int64]*storage.ApplyOperation, len(operations))
	for _, operation := range operations {
		operationsByID[operation.ID] = operation
	}

	applyTasks, err := s.storage.Tasks().GetByApplyID(ctx, apply.ID)
	if err != nil {
		return selection, fmt.Errorf("load tasks for reapply apply %s: %w", apply.ApplyIdentifier, err)
	}
	for _, task := range applyTasks {
		if !state.IsState(task.State, state.Task.Failed, state.Task.Cancelled) {
			continue
		}
		if task.ApplyOperationID != nil {
			operation := operationsByID[*task.ApplyOperationID]
			if operation == nil {
				// A failed task referencing a missing operation row is a storage
				// inconsistency (apply_operation_id is not a foreign key). Fail closed
				// rather than drop the task: dropping it could requeue a partial
				// reapply while the task stays permanently failed (the storage reset's
				// join excludes it), or yield a misleading "no failed work" rejection.
				// Surface it so an operator can investigate.
				return selection, fmt.Errorf("reapply apply %s: failed task references missing operation row %d; resolve the storage inconsistency before reapply", apply.ApplyIdentifier, *task.ApplyOperationID)
			}
			if !state.IsState(operation.State, state.ApplyOperation.Failed) {
				// The task's operation isn't failed, so its deployment isn't part of
				// the reappliable set.
				continue
			}
			selection.deployments[operation.Deployment] = true
		} else {
			selection.deployments[sourcePlan.Deployment] = true
		}
		selection.taskKeys[reapplyTaskKeyFor(task)] = true
	}

	for _, operation := range operations {
		if !state.IsState(operation.State, state.ApplyOperation.Failed) {
			continue
		}
		selection.deployments[operation.Deployment] = true
		tasks, err := s.storage.Tasks().GetByApplyOperationID(ctx, operation.ID)
		if err != nil {
			return selection, fmt.Errorf("load operation %d tasks for reapply apply %s: %w", operation.ID, apply.ApplyIdentifier, err)
		}
		if len(tasks) == 0 && operation.OperationKind == storage.ApplyOperationKindGroupFinalizer {
			namespace, ok := reapplyFinalizerNamespace(operation.OperationKey)
			if !ok {
				return selection, fmt.Errorf("operation %d has malformed finalizer key %q for reapply apply %s: %w", operation.ID, operation.OperationKey, apply.ApplyIdentifier, storage.ErrApplyNotReappliable)
			}
			selection.finalizerNamespaces[namespace] = true
		}
		for _, task := range tasks {
			if state.IsState(task.State, state.Task.Failed, state.Task.Cancelled) {
				selection.taskKeys[reapplyTaskKeyFor(task)] = true
			}
		}
	}

	if len(selection.deployments) == 0 {
		return selection, fmt.Errorf("apply %s has no failed work to reapply: %w", apply.ApplyIdentifier, storage.ErrApplyNotReappliable)
	}
	if len(selection.deployments) > 1 || !selection.deployments[sourcePlan.Deployment] {
		return selection, fmt.Errorf("apply %s has failed work outside primary deployment %q; create a new apply instead: %w", apply.ApplyIdentifier, sourcePlan.Deployment, storage.ErrApplyNotReappliable)
	}
	return selection, nil
}

type reapplyWorkSelection struct {
	taskKeys            map[reapplyTaskKey]bool
	finalizerNamespaces map[string]bool
	deployments         map[string]bool
}

type reapplyTaskKey struct {
	Namespace string
	Shard     string
	Table     string
}

func reapplyTaskKeyFor(task *storage.Task) reapplyTaskKey {
	return reapplyTaskKey{Namespace: task.Namespace, Shard: task.Shard, Table: task.TableName}
}

func filterPlanToReappliableWork(plan *storage.Plan, selection reapplyWorkSelection) *storage.Plan {
	filtered := *plan
	filtered.Namespaces = make(map[string]*storage.NamespacePlanData, len(plan.Namespaces))
	for namespace, nsData := range plan.Namespaces {
		if nsData == nil {
			continue
		}
		filteredNS := filterReappliableNamespaceWork(namespace, nsData, selection)
		if len(filteredNS.Tables) > 0 || len(filteredNS.Artifacts) > 0 {
			filtered.Namespaces[namespace] = filteredNS
		}
	}

	filtered.Shards = make([]storage.ShardPlan, 0, len(plan.Shards))
	for _, shard := range plan.Shards {
		changes := make([]storage.TableChange, 0, len(shard.Changes))
		for _, change := range shard.Changes {
			if selection.taskKeys[reapplyTaskKey{Namespace: shard.Namespace, Shard: shard.Shard, Table: change.Table}] {
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

func filterReappliableNamespaceWork(namespace string, nsData *storage.NamespacePlanData, selection reapplyWorkSelection) *storage.NamespacePlanData {
	filtered := &storage.NamespacePlanData{}
	if selection.finalizerNamespaces[namespace] {
		filtered.Artifacts = copyStringMap(nsData.Artifacts)
	}
	for _, change := range nsData.Tables {
		if selection.includesTable(namespace, change.Table) {
			filtered.Tables = append(filtered.Tables, change)
		}
	}
	return filtered
}

func (s reapplyWorkSelection) includesTable(namespace, table string) bool {
	for key := range s.taskKeys {
		if key.Namespace == namespace && key.Table == table {
			return true
		}
	}
	return false
}

func reapplyFinalizerNamespace(operationKey string) (string, bool) {
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

func reapplyPlanRequest(plan *storage.Plan) PlanRequest {
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

// plansEquivalentForReapply reports whether two plans carry the same reappliable
// work. A marshal failure is an invariant violation (the fingerprint is always
// marshalable), not a "plans differ" signal, so it is returned as an error
// rather than being folded into a false result that would masquerade as drift.
func plansEquivalentForReapply(a, b *storage.Plan) (bool, error) {
	aJSON, err := json.Marshal(reapplyPlanFingerprintFor(a))
	if err != nil {
		return false, fmt.Errorf("marshal source plan fingerprint for reapply equivalence: %w", err)
	}
	bJSON, err := json.Marshal(reapplyPlanFingerprintFor(b))
	if err != nil {
		return false, fmt.Errorf("marshal fresh plan fingerprint for reapply equivalence: %w", err)
	}
	return bytes.Equal(aJSON, bJSON), nil
}

type reapplyPlanFingerprint struct {
	Database     string
	DatabaseType string
	Deployment   string
	Target       string
	Environment  string
	Namespaces   map[string]reapplyNamespaceFingerprint
	Shards       []storage.ShardPlan
}

type reapplyNamespaceFingerprint struct {
	Tables    []storage.TableChange
	Artifacts map[string]string
}

func reapplyPlanFingerprintFor(plan *storage.Plan) reapplyPlanFingerprint {
	namespaces := make(map[string]reapplyNamespaceFingerprint, len(plan.Namespaces))
	for namespace, nsData := range plan.Namespaces {
		if nsData == nil {
			continue
		}
		tables := append([]storage.TableChange(nil), nsData.Tables...)
		artifacts := copyStringMap(nsData.Artifacts)
		if len(tables) == 0 && len(artifacts) == 0 {
			continue
		}
		sort.Slice(tables, func(i, j int) bool {
			if tables[i].Table != tables[j].Table {
				return tables[i].Table < tables[j].Table
			}
			if tables[i].Operation != tables[j].Operation {
				return tables[i].Operation < tables[j].Operation
			}
			return tables[i].DDL < tables[j].DDL
		})
		// Canonicalize empty artifacts to nil so a nil vs empty map (which marshal
		// to null vs {}) doesn't make an otherwise-identical plan compare unequal.
		// This matches filterPlanToReappliableWork, which treats empty as absent.
		if len(artifacts) == 0 {
			artifacts = nil
		}
		namespaces[namespace] = reapplyNamespaceFingerprint{Tables: tables, Artifacts: artifacts}
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
	return reapplyPlanFingerprint{
		Database:     plan.Database,
		DatabaseType: plan.DatabaseType,
		Deployment:   plan.Deployment,
		Target:       plan.Target,
		Environment:  plan.Environment,
		Namespaces:   namespaces,
		Shards:       shards,
	}
}
