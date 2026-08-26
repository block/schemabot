package tern

import (
	"context"
	"fmt"

	"github.com/block/schemabot/pkg/engine"
	ternv1 "github.com/block/schemabot/pkg/proto/ternv1"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// runningCopyKey identifies one table whose row copy is in flight, namespaced
// the way a plan's disclosure names it.
type runningCopyKey struct {
	namespace string
	table     string
}

// protoExistingCopies converts the plan's copy disclosures for the wire. It is
// separate from planResultToProtoChanges because a disclosure describes the
// target rather than the change set: it exists whether or not the plan's
// statements differ from what a copy was made for, and that is precisely the
// case it warns about.
//
// Each disclosure is also marked with whether its work is still running, from
// the table set the caller read off this deployment's own task rows rather than
// anything inferred from the engine's checkpoint. The distinction changes what
// applying means and what the copy's age means, so a surface that cannot tell
// them apart has to describe one as the other:
//
//	left behind ─▶ applying resumes it, and its age is staleness
//	still running ─▶ applying joins it, and its age is a heartbeat
//
// A nil running set marks nothing, which is the disclosure exactly as it reads
// without the distinction — a copy described as left behind when it may be
// running, never the reverse.
func (c *LocalClient) protoExistingCopies(result *engine.PlanResult, running map[runningCopyKey]bool) []*ternv1.ExistingCopy {
	if len(result.ExistingCopies) == 0 {
		return nil
	}
	copies := make([]*ternv1.ExistingCopy, len(result.ExistingCopies))
	for i, ec := range result.ExistingCopies {
		copies[i] = &ternv1.ExistingCopy{
			Namespace:   ec.Namespace,
			Disposition: string(ec.Disposition),
			Reason:      ec.Reason,
			Tables:      ec.Tables,
			AgeSeconds:  int64(ec.Age.Seconds()),
			Statement:   ec.Statement,
			Running:     c.copyIsRunning(ec, running),
		}
	}
	return copies
}

// copyIsRunning reports whether any table the disclosure covers is being copied
// right now. Any one is enough: a disclosure is a single unit of work to the
// operator reading it, and the sentence it turns into — applying joins the copy
// rather than restarting it — is true of the whole unit as soon as one of its
// tables is live.
func (c *LocalClient) copyIsRunning(ec *engine.ExistingCopy, running map[runningCopyKey]bool) bool {
	if ec == nil {
		return false
	}
	namespace := c.planNamespace(ec.Namespace)
	for _, table := range ec.Tables {
		if running[runningCopyKey{namespace, table}] {
			return true
		}
	}
	return false
}

// runningCopiesForPlan reads the tables this deployment is copying right now,
// for marking a plan's copy disclosures. It is read only when there is a
// disclosure to mark, so an ordinary plan against a clean target does no extra
// work.
//
// A plan is a read: it describes the target and must never fail because of it.
// A failed read is logged and marks nothing, which leaves the plan exactly as
// it reads without this check.
func (c *LocalClient) runningCopiesForPlan(ctx context.Context, result *engine.PlanResult, environment string) map[runningCopyKey]bool {
	if len(result.ExistingCopies) == 0 {
		return nil
	}
	running, err := c.runningCopyTables(ctx, c.config.Database, environment)
	if err != nil {
		c.logger.Warn("cannot tell whether an existing copy is still running; the plan discloses every copy as left behind",
			"database", c.config.Database, "environment", environment, "error", err)
		return nil
	}
	return running
}

// runningCopyTables returns the tables this deployment is copying right now,
// read from its own task rows.
//
// A task in flight is work on the target, so its table's copy is running
// whatever state the apply above it is in: an apply can be terminal while the
// work it started keeps going, and that is the case a plan most needs to
// describe correctly. Pending and stopped tasks are excluded because neither is
// copying — a stopped copy is one applying really does pick up where it
// stopped.
//
// Task rows are keyed by database name alone, which is not a target: a name can
// be shared by this deployment's staging and production databases, and by both
// database types. A row is only evidence about the target being planned when it
// names that same target, so the scan matches the plan's database type and
// environment the way the conflict check matches type before it blocks. An
// empty environment matches nothing rather than everything, so a request that
// does not name one marks no copy instead of borrowing another environment's.
//
// It is a read with no side effects. The conflict check resolves stale tasks
// against the engine as it scans; a plan must not, since it decides nothing and
// an operator planning twice would otherwise change the target's records. What
// that costs is stated on Running in the proto: the marker trusts stored state,
// so a row left in flight by a crashed server marks a stopped copy as running
// until recovery clears it.
func (c *LocalClient) runningCopyTables(ctx context.Context, database, environment string) (map[runningCopyKey]bool, error) {
	tasks, err := c.storage.Tasks().GetByDatabase(ctx, database)
	if err != nil {
		return nil, fmt.Errorf("load tasks for database %q: %w", database, err)
	}
	running := map[runningCopyKey]bool{}
	for _, t := range tasks {
		if t == nil {
			return nil, fmt.Errorf("database %q carries a nil task row", database)
		}
		if !state.IsInFlightTaskState(t.State) {
			continue
		}
		if !c.taskDescribesPlanTarget(t, environment) {
			continue
		}
		if t.TableName == "" {
			// A task with no table name has no copy to attribute, so it cannot
			// mark a disclosure. Every other task still counts.
			c.logger.Warn("task in flight records no table, so no existing copy can be marked as running from it",
				append(t.LogAttrs(), "database", database)...)
			continue
		}
		running[runningCopyKey{c.planNamespace(t.Namespace), t.TableName}] = true
	}
	return running, nil
}

// taskDescribesPlanTarget reports whether a task row is work on the same target
// the plan describes, rather than a namesake sharing its database name. Both
// halves fail toward "not this target": an unset environment on either side
// matches nothing, so an unattributable row leaves the copy disclosed as left
// behind rather than claiming live work that may belong elsewhere.
func (c *LocalClient) taskDescribesPlanTarget(t *storage.Task, environment string) bool {
	if t.DatabaseType != c.config.Type {
		return false
	}
	if environment == "" || t.Environment != environment {
		return false
	}
	return true
}
