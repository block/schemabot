package tern

import (
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// finalizerOperationKeySegment is the trailing operation-key segment of a
// namespace's group_finalizer. It matches the control-plane constant of the
// same value in pkg/api.
const finalizerOperationKeySegment = "group_finalizer"

// buildApplyOperationGroups builds the apply_operations (and their tasks) for an
// apply created on a data-plane Tern that claims work at the operation level.
//
// It is the LocalClient counterpart to the control-plane fan-out in pkg/api
// (buildApplyOperationGroups / buildShardedApplyOperationGroups): a sharded
// engine whose plan carries changing shards fans the apply out into one work
// operation per (namespace, shard, table), each carrying a single shard-tagged
// task, so the operator can claim and drive each shard independently and hand
// the engine exactly one target shard. Every namespace that changes its VSchema
// also gets a task-less group_finalizer that runs once its shard work
// completes. Any other apply (no sharded engine, or a plan with no changing
// shards) stays a single whole-deployment work operation carrying all the
// tasks, preserving the pre-fan-out shape.
//
// The data plane resolves no cutover policy / on_failure (it has no environment
// config, unlike the API apply path), so operations are created with the
// store's safe defaults — matching the prior single-operation behavior.
func (c *LocalClient) buildApplyOperationGroups(apply *storage.Apply, plan *storage.Plan, ddlChanges []storage.TableChange, optionsJSON []byte, now time.Time) []*storage.ApplyOperationWithTasks {
	if c.SupportsShardedApplyFanout() {
		if groups := c.buildShardedOperationGroups(apply, plan, ddlChanges, optionsJSON, now); groups != nil {
			return groups
		}
	}

	// Single whole-deployment work operation carrying every task. This is the
	// operation-claiming shape of the legacy single-operation apply, so a
	// non-sharded engine (or a plan with no changing shards) is driven exactly as
	// before, just claimed at the operation level.
	tasks := make([]*storage.Task, len(ddlChanges))
	for i, ddlChange := range ddlChanges {
		tasks[i] = c.newApplyTask(apply, plan, ddlChange, "", optionsJSON, now)
	}
	return []*storage.ApplyOperationWithTasks{{
		Operation: c.newWorkOperation(apply, plan, "", now),
		Tasks:     tasks,
	}}
}

// buildShardedOperationGroups returns one work operation per (namespace, shard,
// table) plus a group_finalizer per VSchema-changing namespace, or nil when the
// plan cannot be fanned out per shard — no changing shards, or a changing
// namespace whose table work has no shard membership — so the caller falls back
// to a single operation. It mirrors api.canBuildShardedOperationGroups and
// api.buildShardedApplyOperationGroups for the single-deployment data plane.
func (c *LocalClient) buildShardedOperationGroups(apply *storage.Apply, plan *storage.Plan, ddlChanges []storage.TableChange, optionsJSON []byte, now time.Time) []*storage.ApplyOperationWithTasks {
	if len(plan.Shards) == 0 || len(ddlChanges) == 0 {
		return nil
	}
	shardsByNamespace := changingShardsByNamespace(plan.Shards)
	if len(shardsByNamespace) == 0 {
		return nil
	}
	for _, ddlChange := range ddlChanges {
		if ddlChange.DDL == "" || ddlChange.Table == "" {
			return nil
		}
		if len(shardsByNamespace[ddlChange.Namespace]) == 0 {
			return nil
		}
	}

	groups := make([]*storage.ApplyOperationWithTasks, 0, len(ddlChanges))
	for _, ddlChange := range ddlChanges {
		for _, shard := range shardsByNamespace[ddlChange.Namespace] {
			op := c.newWorkOperation(apply, plan, shardOperationKey(ddlChange.Namespace, shard.Shard, ddlChange.Table), now)
			task := c.newApplyTask(apply, plan, ddlChange, shard.Shard, optionsJSON, now)
			groups = append(groups, &storage.ApplyOperationWithTasks{
				Operation: op,
				Tasks:     []*storage.Task{task},
			})
		}
	}

	// A VSchema-changing namespace gets a task-less group_finalizer; the operator
	// drives it from the plan once the namespace's shard work completes. A
	// namespace with no shard work would have failed the per-change check above,
	// so a finalizer here always trails real shard work.
	for _, namespace := range vschemaFinalizerNamespaces(plan) {
		op := c.newWorkOperation(apply, plan, finalizerOperationKey(namespace), now)
		op.OperationKind = storage.ApplyOperationKindGroupFinalizer
		groups = append(groups, &storage.ApplyOperationWithTasks{Operation: op})
	}
	return groups
}

// newWorkOperation builds a pending work apply_operation for this deployment.
// CutoverPolicy and OnFailure are left unset so the store applies its safe
// defaults, matching the data plane's prior single-operation behavior.
func (c *LocalClient) newWorkOperation(apply *storage.Apply, plan *storage.Plan, operationKey string, now time.Time) *storage.ApplyOperation {
	return &storage.ApplyOperation{
		Deployment:    apply.Deployment,
		Target:        plan.Target,
		OperationKey:  operationKey,
		OperationKind: storage.ApplyOperationKindWork,
		State:         state.ApplyOperation.Pending,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
}

// newApplyTask builds a pending task for one DDL change on one shard. An empty
// shard targets the whole deployment (non-sharded engines); a non-empty shard
// scopes the task to that shard so the operator hands the engine exactly one
// target shard.
func (c *LocalClient) newApplyTask(apply *storage.Apply, plan *storage.Plan, ddlChange storage.TableChange, shard string, optionsJSON []byte, now time.Time) *storage.Task {
	return &storage.Task{
		TaskIdentifier: "task-" + uuidSuffix(),
		PlanID:         plan.ID,
		Database:       plan.Database,
		DatabaseType:   plan.DatabaseType,
		Engine:         apply.Engine,
		Repository:     plan.Repository,
		PullRequest:    plan.PullRequest,
		Environment:    apply.Environment,
		State:          state.Task.Pending,
		Options:        optionsJSON,
		TableName:      ddlChange.Table,
		Namespace:      ddlChange.Namespace,
		Shard:          shard,
		DDL:            ddlChange.DDL,
		DDLAction:      ddlChange.Operation,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}

func uuidSuffix() string {
	return strings.ReplaceAll(uuid.New().String(), "-", "")[:16]
}

// changingShardsByNamespace groups a plan's changing shards by namespace,
// sorted by shard name. It mirrors the control-plane helper of the same name in
// pkg/api.
func changingShardsByNamespace(shards []storage.ShardPlan) map[string][]storage.ShardPlan {
	byNamespace := make(map[string][]storage.ShardPlan)
	for _, shard := range shards {
		if !shard.NeedsChange {
			continue
		}
		byNamespace[shard.Namespace] = append(byNamespace[shard.Namespace], shard)
	}
	for namespace := range byNamespace {
		sort.Slice(byNamespace[namespace], func(i, j int) bool {
			return byNamespace[namespace][i].Shard < byNamespace[namespace][j].Shard
		})
	}
	return byNamespace
}

// shardOperationKey and finalizerOperationKey build operation keys in the same
// namespace/shard/table format the control plane uses, so a data-plane apply's
// operations are keyed identically to the control-plane fan-out.
func shardOperationKey(namespace, shard, table string) string {
	return namespace + "/" + shard + "/" + table
}

func finalizerOperationKey(namespace string) string {
	return namespace + "/" + finalizerOperationKeySegment
}

// vschemaFinalizerNamespaces returns, sorted, every namespace in the plan that
// carries a VSchema artifact and therefore needs a group_finalizer.
func vschemaFinalizerNamespaces(plan *storage.Plan) []string {
	var namespaces []string
	for namespace, nsData := range plan.Namespaces {
		if namespaceHasVSchemaArtifact(nsData) {
			namespaces = append(namespaces, namespace)
		}
	}
	sort.Strings(namespaces)
	return namespaces
}
