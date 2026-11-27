package tern

import (
	"fmt"
	"log/slog"
	"maps"
	"strings"

	"github.com/block/schemabot/pkg/storage"
)

// planChangeMetadata finds the engine's plan-time Metadata stored with the
// change a task runs, so the engine applies the change against the record it
// made when the operator reviewed the plan (RV-1).
//
// A task is matched to the change at its plan change ordinal: the position,
// among the plan's changes to its namespace and table, recorded when the task
// was created. A plan can carry the same statement twice on one table with
// different metadata, and the ordinal is what tells those changes apart. A
// task written without an ordinal is matched to the change in its namespace
// that names its table and carries its statement. The statement is compared in the dialect's canonical
// form when a canonicalizer is available, because a task's DDL can take a
// deployment's rendering of the reviewed statement on resume or progress sync
// while the stored plan keeps the reviewed text. A statement that matches no
// change gets no metadata, and the engine decides whether the change can run
// without it. There is no fallback to a table's only change, since a record
// borrowed from another statement is not the one the operator reviewed.
//
// The index is built once per plan and then answers each task in time
// proportional to the changes on that task's table, so building a dispatch or
// a grouped drive's changes stays linear in the plan's size.
type planChangeMetadata struct {
	planIdentifier string
	byTable        map[string][]storage.TableChange
	canon          StatementCanonicalizer
}

// newPlanChangeMetadata indexes plan's changes by namespace and table. A nil
// plan yields an index that answers every task with no metadata, and a nil
// canon compares statements by their trimmed text.
func newPlanChangeMetadata(plan *storage.Plan, canon StatementCanonicalizer) planChangeMetadata {
	ix := planChangeMetadata{byTable: storage.PlanChangesByTable(plan), canon: canon}
	if plan != nil {
		ix.planIdentifier = plan.PlanIdentifier
	}
	return ix
}

// planChangeCanonicalizer returns the statement canonicalizer for an apply's
// dialect. An apply whose database type has no registered dialect compares
// statements by text only, which still matches every task whose DDL is the
// reviewed text, and the fallback is logged.
func planChangeCanonicalizer(apply *storage.Apply, logger *slog.Logger) StatementCanonicalizer {
	canon, err := StatementCanonicalizerForDatabaseType(apply.DatabaseType, logger)
	if err != nil {
		logger.Warn("plan-time engine metadata is matched to tasks by statement text only",
			append(apply.IdentityLogAttrs(), "error", err)...)
		return nil
	}
	return canon
}

func (ix planChangeMetadata) statementKey(ddl string) string {
	if ix.canon != nil {
		return ix.canon(ddl)
	}
	return strings.TrimSpace(ddl)
}

// forTask returns a copy of the metadata of the change task runs, or nil when
// no change matches or the match carries none.
//
// A task with an ordinal reads the change at that position. The change must
// still carry the task's statement: a change that does not is not the one the
// task was created from, and is an error rather than a record borrowed from
// another statement. A task without an ordinal is matched by statement, and two
// matching changes that carry different metadata are an error, because either
// could be the one the task was created from.
func (ix planChangeMetadata) forTask(task *storage.Task) (map[string]string, error) {
	want := ix.statementKey(task.DDL)
	changes := ix.byTable[storage.PlanChangeTableKey(task.Namespace, task.TableName)]
	if task.PlanChangeOrdinal > 0 {
		if task.PlanChangeOrdinal > len(changes) {
			return nil, fmt.Errorf("plan %s has %d change(s) to table %q in namespace %q, but the task runs change %d", ix.planIdentifier, len(changes), task.TableName, task.Namespace, task.PlanChangeOrdinal)
		}
		change := changes[task.PlanChangeOrdinal-1]
		if ix.statementKey(change.DDL) != want {
			return nil, fmt.Errorf("plan %s change %d to table %q in namespace %q carries a different statement than the task", ix.planIdentifier, task.PlanChangeOrdinal, task.TableName, task.Namespace)
		}
		return maps.Clone(change.Metadata), nil
	}
	var (
		found    bool
		metadata map[string]string
	)
	for _, change := range changes {
		if ix.statementKey(change.DDL) != want {
			continue
		}
		if found && !maps.Equal(metadata, change.Metadata) {
			return nil, fmt.Errorf("plan %s holds more than one change to table %q in namespace %q with this DDL, and they carry different engine metadata", ix.planIdentifier, task.TableName, task.Namespace)
		}
		found = true
		metadata = change.Metadata
	}
	return maps.Clone(metadata), nil
}

// planMetadataCache holds the planChangeMetadata index of each plan one
// sequential drive reads, so the drive canonicalizes a plan's changes once
// rather than once per task attempt. It is scoped to a single drive, like the
// canonicalizer it holds, and is not safe for concurrent use.
type planMetadataCache struct {
	canon  StatementCanonicalizer
	byPlan map[int64]planChangeMetadata
}

// newPlanMetadataCache returns an empty cache for one drive of apply.
func (c *LocalClient) newPlanMetadataCache(apply *storage.Apply) *planMetadataCache {
	return &planMetadataCache{canon: planChangeCanonicalizer(apply, c.logger), byPlan: map[int64]planChangeMetadata{}}
}

// cached returns the index already built for the plan with this ID.
func (m *planMetadataCache) cached(planID int64) (planChangeMetadata, bool) {
	index, ok := m.byPlan[planID]
	return index, ok
}

// indexFor returns plan's index, building it on first use.
func (m *planMetadataCache) indexFor(plan *storage.Plan) planChangeMetadata {
	if index, ok := m.byPlan[plan.ID]; ok {
		return index
	}
	index := newPlanChangeMetadata(plan, m.canon)
	m.byPlan[plan.ID] = index
	return index
}
