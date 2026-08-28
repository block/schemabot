// local_apply_superseded.go records the handoff when a dispatch takes over the
// unfinished work of an apply that stopped.
//
// A stopped apply's copy rests on the target with its checkpoint intact, and a
// later dispatch for the same table meets that copy: the engine either resumes
// it or replaces it, deciding on its own from the checkpoint's statement. Either
// way the stopped apply's work is no longer its own to continue, and a start on
// it would replay a change someone else now owns. The marker is what makes that
// refusable — see ApplyStore.MarkSuperseded.
package tern

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/storage"
)

// supersededHolder is a stopped apply whose resting task the conflict check
// released, named alongside the table that task was changing. The table is what
// decides whether a dispatch actually takes the work over: releasing the hold
// frees the whole database, but only a dispatch that touches the same table
// meets the copy the holder left behind. A holder whose task names no table is
// a multi-table atomic change; its copies are matched by namespace instead
// (see dispatchMeetsHolderWork).
type supersededHolder struct {
	applyID         int64
	applyIdentifier string
	namespace       string
	table           string
}

// markSupersededHolders records successor as the apply that took over the
// unfinished work of every released holder whose work it is about to meet.
//
// It runs after the successor is durably created, so the marker never names an
// apply that does not exist. The cost of that ordering is that a successor which
// fails immediately still leaves the marker behind — the holder is then resolved
// by cancelling it, which reclaims its copy, rather than by starting it.
//
// A failure to mark is reported and not retried: the successor is already
// dispatched and taking the work over regardless, so failing the dispatch here
// would refuse work that is already under way.
func (c *LocalClient) markSupersededHolders(ctx context.Context, successor *storage.Apply, holders []supersededHolder, changes []storage.TableChange) {
	if len(holders) == 0 {
		return
	}

	changedTables, changedNamespaces := c.changedTableKeys(changes)

	// The marker and the refusal it backs are apply-granular, so holders
	// collapse to one takeover per apply: several released tasks of one apply
	// (shards of one table, or several tables) met by one dispatch record one
	// mark and one timeline event naming everything taken over.
	takeovers := map[int64]*supersededTakeover{}
	var order []int64
	for _, holder := range holders {
		if !c.dispatchMeetsHolderWork(holder, changedTables, changedNamespaces) {
			c.logger.Debug("dispatch does not meet the released holder's work, so the holder keeps its own",
				"holder_apply_id", holder.applyIdentifier, "holder_table", holder.table,
				"holder_namespace", holder.namespace, "successor_apply_id", successor.ApplyIdentifier)
			continue
		}
		takeover := takeovers[holder.applyID]
		if takeover == nil {
			takeover = &supersededTakeover{applyIdentifier: holder.applyIdentifier}
			takeovers[holder.applyID] = takeover
			order = append(order, holder.applyID)
		}
		takeover.addTable(holder.table)
	}

	for _, applyID := range order {
		takeover := takeovers[applyID]
		err := c.storage.Applies().MarkSuperseded(ctx, applyID, successor.ApplyIdentifier)
		switch {
		case err == nil:
			c.logger.Info("recorded the apply that took over a stopped apply's unfinished work",
				append(successor.LogAttrs(),
					"superseded_apply_id", takeover.applyIdentifier, "superseded_tables", takeover.tables)...)
			c.logApplyEvent(ctx, applyID, nil, storage.LogLevelInfo, storage.LogEventInfo, storage.LogSourceSchemaBot,
				takeover.event(successor.ApplyIdentifier), "", "")
		case errors.Is(err, storage.ErrApplyAlreadySuperseded):
			// An earlier dispatch already recorded the handoff. The first
			// successor owns the marker, so this one changes nothing.
			c.logger.Info("a stopped apply's work was already taken over by an earlier apply, so this dispatch leaves its marker alone",
				append(successor.LogAttrs(), "superseded_apply_id", takeover.applyIdentifier)...)
		default:
			c.logger.Error("failed to record the apply that took over a stopped apply's unfinished work; a start on the stopped apply will not be refused by its marker",
				append(successor.LogAttrs(),
					"superseded_apply_id", takeover.applyIdentifier, "superseded_tables", takeover.tables, "error", err)...)
		}
	}
}

// supersededTakeover collects, per holder apply, the tables whose work one
// dispatch took over, so the handoff is recorded once for the apply rather
// than once per released task.
type supersededTakeover struct {
	applyIdentifier string
	tables          []string
}

// addTable records a taken-over table once. A multi-table atomic holder names
// no table, so it adds nothing here and the event names the work as a whole.
func (t *supersededTakeover) addTable(table string) {
	if table == "" {
		return
	}
	if slices.Contains(t.tables, table) {
		return
	}
	t.tables = append(t.tables, table)
}

// event is the timeline line the superseded apply keeps. It names the
// successor and the work it took over, and states the consequence the marker
// enforces: the refusal is apply-wide even when the successor met only part of
// the apply's work, because start would replay everything — including the part
// the successor now owns.
func (t *supersededTakeover) event(successorIdentifier string) string {
	work := "this change's unfinished work"
	if len(t.tables) > 0 {
		work = fmt.Sprintf("this change's unfinished work on %s", strings.Join(t.tables, ", "))
	}
	return fmt.Sprintf("Superseded by %s, which took over %s. This apply can no longer be started; any of its work the successor did not take over reaches the database through a fresh apply instead.",
		successorIdentifier, work)
}

// dispatchMeetsHolderWork reports whether the dispatch's changes meet the
// resting copy a released holder left behind. A holder that names its table is
// met exactly on that table. A holder that names none is a multi-table atomic
// change (see storage.Task.TableName): its copies are real but not keyed by
// table, so any table change in its namespace is treated as meeting them.
// Marking is the fail-closed side of that uncertainty — an unmarked holder
// whose copy was in fact met can be started into replaying work the successor
// now owns, while a marked holder is refused and any work the successor did
// not meet still reaches the database through a fresh dispatch.
func (c *LocalClient) dispatchMeetsHolderWork(holder supersededHolder, changedTables map[runningCopyKey]bool, changedNamespaces map[string]bool) bool {
	namespace := c.planNamespace(holder.namespace)
	if holder.table == "" {
		return changedNamespaces[namespace]
	}
	return changedTables[runningCopyKey{namespace, holder.table}]
}

// changedTableKeys is the set of tables a dispatch will change, keyed the way a
// task row names its own table so a holder can be matched against it, together
// with the namespaces those table changes touch, which match the holders whose
// tasks name no table of their own.
func (c *LocalClient) changedTableKeys(changes []storage.TableChange) (map[runningCopyKey]bool, map[string]bool) {
	changedTables := make(map[runningCopyKey]bool, len(changes))
	changedNamespaces := make(map[string]bool, len(changes))
	for _, change := range changes {
		if change.Table == "" {
			// A change that names no table (a VSchema-only change) has no copy
			// on the target for a holder to have left behind.
			c.logger.Debug("dispatch change names no table, so it can take over no stopped apply's copy",
				"operation", change.Operation, "namespace", change.Namespace)
			continue
		}
		namespace := c.planNamespace(change.Namespace)
		changedTables[runningCopyKey{namespace, change.Table}] = true
		changedNamespaces[namespace] = true
	}
	return changedTables, changedNamespaces
}
