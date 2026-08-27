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

	"github.com/block/schemabot/pkg/storage"
)

// supersededHolder is a stopped apply whose resting task the conflict check
// released, named alongside the table that task was changing. The table is what
// decides whether a dispatch actually takes the work over: releasing the hold
// frees the whole database, but only a dispatch that touches the same table
// meets the copy the holder left behind.
type supersededHolder struct {
	applyID         int64
	applyIdentifier string
	namespace       string
	table           string
}

// markSupersededHolders records successor as the apply that took over the
// unfinished work of every released holder whose table it is about to change.
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

	changed := c.changedTableKeys(changes)
	for _, holder := range holders {
		key := runningCopyKey{c.planNamespace(holder.namespace), holder.table}
		if !changed[key] {
			c.logger.Debug("dispatch does not change the released holder's table, so the holder keeps its own work",
				"holder_apply_id", holder.applyIdentifier, "holder_table", holder.table,
				"successor_apply_id", successor.ApplyIdentifier)
			continue
		}

		err := c.storage.Applies().MarkSuperseded(ctx, holder.applyID, successor.ApplyIdentifier)
		switch {
		case err == nil:
			c.logger.Info("recorded the apply that took over a stopped apply's unfinished work",
				append(successor.LogAttrs(),
					"superseded_apply_id", holder.applyIdentifier, "superseded_table", holder.table)...)
			c.logApplyEvent(ctx, holder.applyID, nil, storage.LogLevelInfo, storage.LogEventInfo, storage.LogSourceSchemaBot,
				fmt.Sprintf("Superseded by %s, which took over this change's unfinished work on %s", successor.ApplyIdentifier, holder.table),
				"", "")
		case errors.Is(err, storage.ErrApplyAlreadySuperseded):
			// An earlier dispatch already recorded the handoff. The first
			// successor owns the marker, so this one changes nothing.
			c.logger.Info("a stopped apply's work was already taken over by an earlier apply, so this dispatch leaves its marker alone",
				append(successor.LogAttrs(), "superseded_apply_id", holder.applyIdentifier)...)
		default:
			c.logger.Error("failed to record the apply that took over a stopped apply's unfinished work; a start on the stopped apply will not be refused by its marker",
				append(successor.LogAttrs(),
					"superseded_apply_id", holder.applyIdentifier, "superseded_table", holder.table, "error", err)...)
		}
	}
}

// changedTableKeys is the set of tables a dispatch will change, keyed the way a
// task row names its own table so a holder can be matched against it.
func (c *LocalClient) changedTableKeys(changes []storage.TableChange) map[runningCopyKey]bool {
	changed := make(map[runningCopyKey]bool, len(changes))
	for _, change := range changes {
		if change.Table == "" {
			// A change that names no table (a VSchema-only change) has no copy
			// on the target for a holder to have left behind.
			c.logger.Debug("dispatch change names no table, so it can take over no stopped apply's copy",
				"operation", change.Operation, "namespace", change.Namespace)
			continue
		}
		changed[runningCopyKey{c.planNamespace(change.Namespace), change.Table}] = true
	}
	return changed
}
