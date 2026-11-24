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
// A task is matched to the change in its namespace that names its table and
// carries its statement. The statement is compared in the dialect's canonical
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
	ix := planChangeMetadata{byTable: map[string][]storage.TableChange{}, canon: canon}
	if plan == nil {
		return ix
	}
	ix.planIdentifier = plan.PlanIdentifier
	for namespace, data := range plan.Namespaces {
		if data == nil {
			continue
		}
		for _, change := range data.Tables {
			if change.Namespace == "" {
				change.Namespace = namespace
			}
			key := progressTableKey(change.Namespace, change.Table)
			ix.byTable[key] = append(ix.byTable[key], change)
		}
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
// no change matches or the match carries none. Two matching changes that
// carry different metadata are an error, because either could be the one the
// task was created from.
func (ix planChangeMetadata) forTask(task *storage.Task) (map[string]string, error) {
	want := ix.statementKey(task.DDL)
	var (
		found    bool
		metadata map[string]string
	)
	for _, change := range ix.byTable[progressTableKey(task.Namespace, task.TableName)] {
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
