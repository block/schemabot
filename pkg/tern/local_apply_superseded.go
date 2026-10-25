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

// supersededTakeovers selects the released holders whose work the dispatch's
// changes meet, one takeover per holder apply, in the order the holders were
// released.
//
// The marker and the refusal it backs are apply-granular, so holders collapse to
// one takeover per apply: several released tasks of one apply (shards of one
// table, or several tables) met by one dispatch are one handoff and one timeline
// event naming everything taken over.
func (c *LocalClient) supersededTakeovers(successor *storage.Apply, holders []supersededHolder, changes []storage.TableChange) []*supersededTakeover {
	if len(holders) == 0 {
		return nil
	}

	changedTables, changedNamespaces := c.changedTableKeys(changes)

	byApply := map[int64]*supersededTakeover{}
	var takeovers []*supersededTakeover
	for _, holder := range holders {
		if !c.dispatchMeetsHolderWork(holder, changedTables, changedNamespaces) {
			c.logger.Debug("dispatch does not meet the released holder's work, so the holder keeps its own",
				"holder_apply_id", holder.applyIdentifier, "holder_table", holder.table,
				"holder_namespace", holder.namespace, "successor_apply_id", successor.ApplyIdentifier)
			continue
		}
		takeover := byApply[holder.applyID]
		if takeover == nil {
			takeover = &supersededTakeover{applyID: holder.applyID, applyIdentifier: holder.applyIdentifier}
			byApply[holder.applyID] = takeover
			takeovers = append(takeovers, takeover)
		}
		takeover.addTable(holder.table)
	}
	return takeovers
}

// takeoverApplyIDs is the row IDs of the stopped applies a dispatch takes over,
// in the form the create or attach records them (storage.Apply.TakesOverApplyIDs).
func takeoverApplyIDs(takeovers []*supersededTakeover) []int64 {
	ids := make([]int64, 0, len(takeovers))
	for _, takeover := range takeovers {
		ids = append(ids, takeover.applyID)
	}
	return ids
}

// announceSupersededTakeovers tells the stopped applies' timelines who took their
// work over, once the successor's create or attach has committed the marker. The handoff
// itself is already durable by then: this only records it for the operator, so a
// failure to read the marker back or to log the event is reported and does not
// fail a dispatch whose work is already under way.
//
// A holder whose marker names a different, earlier successor was not taken over
// by this dispatch, so it gets no event claiming it was.
func (c *LocalClient) announceSupersededTakeovers(ctx context.Context, successor *storage.Apply, takeovers []*supersededTakeover) {
	for _, takeover := range takeovers {
		owner, err := c.storage.Applies().GetSupersededBy(ctx, takeover.applyID)
		if err != nil {
			c.logger.Error("failed to read the marker of a stopped apply this dispatch took over, so its timeline does not name the successor",
				append(successor.LogAttrs(),
					"superseded_apply_id", takeover.applyIdentifier, "superseded_tables", takeover.tables, "error", err)...)
			continue
		}
		if owner != successor.ApplyIdentifier {
			c.logger.Info("a stopped apply's work was already taken over by an earlier apply, so this dispatch left its marker alone",
				append(successor.LogAttrs(),
					"superseded_apply_id", takeover.applyIdentifier, "marker_names", owner)...)
			continue
		}
		c.logger.Info("recorded the apply that took over a stopped apply's unfinished work",
			append(successor.LogAttrs(),
				"superseded_apply_id", takeover.applyIdentifier, "superseded_tables", takeover.tables)...)
		c.logApplyEvent(ctx, takeover.applyID, nil, storage.LogLevelInfo, storage.LogEventInfo, storage.LogSourceSchemaBot,
			takeover.event(successor.ApplyIdentifier), "", "")
	}
}

// supersededTakeover collects, per holder apply, the tables whose work one
// dispatch took over, so the handoff is recorded once for the apply rather
// than once per released task.
type supersededTakeover struct {
	applyID         int64
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
