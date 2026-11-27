// local_cancelled_artifacts.go reclaims what a cancelled schema change left on
// the target, so an abandoned row copy does not sit on the target database
// forever.
//
// The engine cannot make this decision alone: its artifacts are named after the
// target's own tables, so a copy another apply is actively writing carries the
// same names as one nobody owns any more. The apply-target lock, and the
// active-apply re-check under it, is what tells the two apart.
//
// That covers the schema changes SchemaBot runs. It does not cover one started
// outside SchemaBot against the same schema, so the engine keeps its own guard
// over the artifacts such a change could own and reports what it declined to
// reclaim. This file surfaces that report rather than treating it as success.
package tern

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/block/schemabot/pkg/ddl"
	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/state"
	"github.com/block/schemabot/pkg/storage"
)

// cancelledArtifactReleaseTimeout bounds the exclusive hold the release takes on
// the apply's target. The work under it is a schema lookup, a rename of the copy
// into quarantine and a drop of the metadata tables, all of which can wait on a
// metadata lock held by unrelated traffic. The budget is generous enough for that
// wait and short enough that a target blocked on something else does not hold
// every apply queued behind it: the hold is what other applies wait on, so an
// unbounded one turns a stuck cancel into a stalled target.
const cancelledArtifactReleaseTimeout = 10 * time.Minute

// namespaceArtifacts collects the tables of one schema whose artifacts a
// release should reclaim, alongside a task from that schema to resolve its
// credentials from.
type namespaceArtifacts struct {
	task   *storage.Task
	tables []string
	// addedConstraints names, per table, the constraints the apply's tasks
	// that ran added.
	addedConstraints map[string][]string
	// validatedConstraints names, per table, the constraints the apply's plan
	// validates in a step of its own.
	validatedConstraints map[string][]string
	// droppedConstraints names, per table, the constraints the apply's plan
	// drops.
	droppedConstraints map[string][]string
	// unreadTables names the tables with a statement the release could not
	// read, on which it removes no constraint.
	unreadTables []string
}

// releaseCancelledArtifacts reclaims the artifacts the cancelled apply left on
// the target, under an exclusive hold on that target so a live apply's copy is
// never mistaken for an abandoned one.
//
// The caller decides what a failure means. It must not abandon the cancel: the
// destructive direction here is continuing the work, not stopping it, and
// leftover artifacts are inert disk where an undeliverable cancel is a wedged
// pull request.
func (c *LocalClient) releaseCancelledArtifacts(ctx context.Context, eng engine.Engine, apply *storage.Apply, tasks []*storage.Task) error {
	return c.releaseArtifacts(ctx, eng, apply, tasks, cancelledRelease)
}

// artifactRelease is one way a schema change can stop short with its
// artifacts on the target: the engine capability that reclaims them, and the
// word the logs use for how the change ended.
type artifactRelease struct {
	outcome string
	release func(context.Context, engine.Engine, *engine.ReleaseArtifactsRequest) (bool, *engine.ReleaseArtifactsResult, error)
}

var (
	cancelledRelease = artifactRelease{outcome: "cancelled", release: engine.ReleaseCancelledArtifacts}
	failedRelease    = artifactRelease{outcome: "failed", release: engine.ReleaseFailedArtifacts}
)

// releaseFailedApplyArtifacts removes the constraints an apply that failed for
// good added to failedTask's table and left there: the apply ends, so nothing
// will reach the step that removes them, and they go on checking the
// application's writes. Only the failed task's table is in scope, since a
// sequential drive starts a table's statements only after the ones before
// them finished. A release that does not complete is logged to the server log
// and the apply log and does not change the apply's outcome.
func (c *LocalClient) releaseFailedApplyArtifacts(ctx context.Context, apply *storage.Apply, tasks []*storage.Task, failedTask *storage.Task) {
	if failedTask == nil || !state.IsState(failedTask.State, state.Task.Failed) {
		return
	}
	eng := c.getEngine()
	if _, ok := eng.(engine.FailedArtifactReleaser); !ok {
		// An engine without the capability adds no constraint a failure strands,
		// and keeps a failed apply's row copy for its resume.
		c.logger.Debug("failed schema change leaves nothing this engine releases",
			append(apply.LogAttrs(), "engine_type", c.config.Type)...)
		return
	}
	var sameTable []*storage.Task
	for _, task := range tasks {
		if task != nil && sameTaskTable(task, failedTask) {
			sameTable = append(sameTable, task)
		}
	}
	if err := c.releaseArtifacts(ctx, eng, apply, sameTable, failedRelease); err != nil {
		c.logSkippedArtifactRelease(ctx, apply, failedRelease, err)
	}
}

// sameTaskTable reports whether two tasks change the same table of the same
// schema, within the same apply operation.
func sameTaskTable(a, b *storage.Task) bool {
	if a.Namespace != b.Namespace || a.TableName != b.TableName {
		return false
	}
	if a.ApplyOperationID == nil || b.ApplyOperationID == nil {
		return a.ApplyOperationID == nil && b.ApplyOperationID == nil
	}
	return *a.ApplyOperationID == *b.ApplyOperationID
}

// releaseArtifacts reclaims what a stopped apply left on the target through
// kind's engine capability, under an exclusive hold on that target.
func (c *LocalClient) releaseArtifacts(ctx context.Context, eng engine.Engine, apply *storage.Apply, tasks []*storage.Task, kind artifactRelease) error {
	if eng == nil {
		return fmt.Errorf("no engine configured for type: %s", c.config.Type)
	}
	if apply == nil {
		return fmt.Errorf("apply is required to reclaim %s schema change artifacts", kind.outcome)
	}

	byNamespace := c.cancelledArtifactTables(apply, tasks)
	if len(byNamespace) == 0 {
		c.logger.Debug("schema change names no tables, so it left no artifacts to reclaim",
			append(apply.LogAttrs(), "outcome", kind.outcome)...)
		return nil
	}

	releaseCtx, cancel := context.WithTimeout(ctx, cancelledArtifactReleaseTimeout)
	defer cancel()

	err := c.storage.Applies().WithExclusiveTarget(releaseCtx, apply, func(ctx context.Context) error {
		reclaimedAny := false
		for _, namespace := range slices.Sorted(maps.Keys(byNamespace)) {
			reclaimed, err := c.releaseNamespaceArtifacts(ctx, eng, apply, namespace, byNamespace[namespace], kind)
			reclaimedAny = reclaimedAny || reclaimed
			if err != nil {
				return &artifactReleaseError{outcome: kind.outcome, namespace: namespace, reclaimedAny: reclaimedAny, err: err}
			}
		}
		return nil
	})
	if err != nil && releaseOutranItsHold(releaseCtx, err) {
		// A statement killed by the deadline can surface as the SQL driver's own
		// error, such as a bad connection, rather than the context's. The
		// release's own clock is what says it ran out of time.
		err = fmt.Errorf("release ran past its %s hold on the target: %w", cancelledArtifactReleaseTimeout, errors.Join(context.DeadlineExceeded, err))
	}
	return err
}

// releaseOutranItsHold reports whether a failed release failed because its hold
// on the target expired, even where the error it returned does not say so.
func releaseOutranItsHold(releaseCtx context.Context, err error) bool {
	return errors.Is(releaseCtx.Err(), context.DeadlineExceeded) && !errors.Is(err, context.DeadlineExceeded)
}

// artifactReleaseError is a release that stopped in one schema, carrying
// whether anything had been reclaimed before it stopped. The two outcomes need
// different words for an operator: "your copy is still where it was" is only
// true when nothing moved.
type artifactReleaseError struct {
	outcome      string
	namespace    string
	reclaimedAny bool
	err          error
}

func (e *artifactReleaseError) Error() string {
	return fmt.Sprintf("reclaim %s schema change artifacts in %s: %v", e.outcome, e.namespace, e.err)
}

func (e *artifactReleaseError) Unwrap() error { return e.err }

// releaseNamespaceArtifacts reclaims one schema's artifacts and records where
// its data went, so an operator can answer "where did my copy go" from the pull
// request timeline rather than a server-log dig. It reports whether anything
// was reclaimed, including by a release that then failed.
func (c *LocalClient) releaseNamespaceArtifacts(ctx context.Context, eng engine.Engine, apply *storage.Apply, namespace string, artifacts *namespaceArtifacts, kind artifactRelease) (bool, error) {
	creds, err := c.credentialsForTask(artifacts.task)
	if err != nil {
		return false, fmt.Errorf("resolve credentials to reclaim artifacts in %s: %w", namespace, err)
	}

	// The engine reaches the tables in the target's own schema, which a schema
	// override can name differently from the namespace the tasks record.
	physical, err := c.physicalNamespace(namespace)
	if err != nil {
		return false, fmt.Errorf("resolve the schema to reclaim artifacts in %s: %w", namespace, err)
	}
	supported, result, err := kind.release(ctx, eng, &engine.ReleaseArtifactsRequest{
		Database:             physical,
		Tables:               artifacts.tables,
		Credentials:          creds,
		AddedConstraints:     artifacts.addedConstraints,
		ValidatedConstraints: artifacts.validatedConstraints,
	})
	if !supported {
		// The engine leaves nothing on the target that this way of stopping
		// strands: its unfinished work lives in the service it drives, or a
		// later apply resumes from it.
		c.logger.Debug("engine leaves no artifacts on the target to reclaim for this outcome",
			append(apply.LogAttrs(), "engine", eng.Name(), "namespace", namespace, "outcome", kind.outcome)...)
		return false, nil
	}
	// What the release did is recorded before its error is returned: a release
	// that fails part-way has still moved the copy, and the entry naming where
	// it went is what an operator follows to recover it.
	reclaimed := c.recordReleaseResult(ctx, apply, namespace, kind.outcome, result)
	if err != nil {
		return reclaimed, err
	}
	if !reclaimed {
		c.logger.Info("schema change left no artifacts on the target",
			append(apply.LogAttrs(), "namespace", namespace, "outcome", kind.outcome, "tables", artifacts.tables)...)
	}
	return reclaimed, nil
}

// recordReleaseResult writes what one schema's release reclaimed and retained to
// the server log and the apply log, and reports whether it reclaimed anything.
func (c *LocalClient) recordReleaseResult(ctx context.Context, apply *storage.Apply, namespace, outcome string, result *engine.ReleaseArtifactsResult) bool {
	if result == nil {
		return false
	}
	for _, constraint := range result.RemovedConstraints {
		c.logger.Info("removed a constraint the schema change added to a live table",
			append(apply.LogAttrs(), "namespace", namespace, "outcome", outcome, "table", constraint.Table, "constraint", constraint.Name)...)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventInfo, storage.LogSourceSchemaBot,
			removedConstraintMessage(constraint, outcome), "", "")
	}
	for _, constraint := range result.RemainingConstraints {
		// The constraint goes on checking the application's writes, which the
		// operator cannot know unless something says so.
		c.logger.Warn("a constraint the schema change added is still on a live table",
			append(apply.LogAttrs(), "namespace", namespace, "outcome", outcome, "table", constraint.Table, "constraint", constraint.Name)...)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelWarn, storage.LogEventInfo, storage.LogSourceSchemaBot,
			remainingConstraintMessage(constraint, outcome), "", "")
	}
	for _, constraint := range result.PreexistingConstraints {
		// The schema change did not add it and the release does not remove
		// it, but the operator may expect the table back as it was.
		c.logger.Warn("a constraint the schema change worked with, but did not add, is still on a live table",
			append(apply.LogAttrs(), "namespace", namespace, "outcome", outcome, "table", constraint.Table, "constraint", constraint.Name)...)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelWarn, storage.LogEventInfo, storage.LogSourceSchemaBot,
			preexistingConstraintMessage(constraint, outcome), "", "")
	}
	reclaimed := len(result.Preserved) > 0 || len(result.Discarded) > 0 || len(result.RemovedConstraints) > 0
	if len(result.Preserved) > 0 || len(result.Discarded) > 0 {
		c.logger.Info("reclaimed cancelled schema change artifacts",
			append(apply.LogAttrs(),
				"namespace", namespace,
				"preserved", len(result.Preserved),
				"discarded", len(result.Discarded))...)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventInfo, storage.LogSourceSchemaBot,
			releasedArtifactsMessage(result), "", "")
	}

	if len(result.Retained) > 0 {
		// A retained artifact is not a failed release: the tables the cancelled
		// schema change owns outright were still reclaimed. It is disk an
		// operator has to reclaim by hand, which they will only do if something
		// tells them it is there.
		c.logger.Warn("cancelled schema change's shared metadata was left on the target",
			append(apply.LogAttrs(),
				"namespace", namespace,
				"retained", result.Retained,
				"reason", result.RetainedReason)...)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelWarn, storage.LogEventInfo, storage.LogSourceSchemaBot,
			retainedArtifactsMessage(result), "", "")
	}
	return reclaimed
}

// removedConstraintMessage describes, for the apply log, a constraint the
// release dropped from a live table.
func removedConstraintMessage(constraint engine.EngineConstraint, outcome string) string {
	return fmt.Sprintf("Removed constraint %s from %s. The %s schema change added it, and it %s",
		constraint.Name, constraint.Table, outcome, constraint.Effect)
}

// remainingConstraintMessage describes, for the apply log, a constraint the
// release could not drop, what it does to writes, and the statement that
// drops it.
func remainingConstraintMessage(constraint engine.EngineConstraint, outcome string) string {
	return fmt.Sprintf("Constraint %s is still on %s. The %s schema change added it, and it %s until it is dropped. Drop it with: %s",
		constraint.Name, constraint.Table, outcome, constraint.Effect, constraint.Removal)
}

// preexistingConstraintMessage describes, for the apply log, a constraint the
// release left because the schema change did not add it, what it does to
// writes, and the statement that drops it.
func preexistingConstraintMessage(constraint engine.EngineConstraint, outcome string) string {
	return fmt.Sprintf("Constraint %s is still on %s. It was there before the %s schema change, so SchemaBot left it, and it %s until it is dropped. Drop it with: %s",
		constraint.Name, constraint.Table, outcome, constraint.Effect, constraint.Removal)
}

// releasedArtifactsMessage describes a release for the apply log, naming where
// preserved data was put so it can be found while it is still recoverable.
func releasedArtifactsMessage(result *engine.ReleaseArtifactsResult) string {
	if len(result.Preserved) == 0 {
		return fmt.Sprintf("Reclaimed %s left by the cancelled schema change", pluralizeTables(len(result.Discarded)))
	}

	destinations := make([]string, 0, len(result.Preserved))
	for _, artifact := range result.Preserved {
		destinations = append(destinations, fmt.Sprintf("%s is recoverable at %s", artifact.Source, artifact.Destination))
	}
	return fmt.Sprintf("Reclaimed the cancelled schema change's copy: %s", strings.Join(destinations, ", "))
}

// retainedArtifactsMessage names what the release deliberately left behind and
// why, in the terms an operator acts on: these are the schema's shared tables,
// so reclaiming them is a decision that needs someone who knows the schema is
// idle. Naming them is what makes that a task rather than a surprise later.
func retainedArtifactsMessage(result *engine.ReleaseArtifactsResult) string {
	return fmt.Sprintf("Left %s in place because %s: %s",
		pluralizeTables(len(result.Retained)),
		result.RetainedReason,
		strings.Join(result.Retained, ", "))
}

func pluralizeTables(count int) string {
	if count == 1 {
		return "1 table"
	}
	return fmt.Sprintf("%d tables", count)
}

// cancelledArtifactTables groups the apply's tables by the schema they live in.
// Each schema has its own credentials, and the engine derives its artifact
// names from the table names within one schema, so a release is per schema.
//
// Every task counts, whatever state it reached. A task that never started
// copying has no artifacts and the engine finds nothing for it, while a task
// the apply believes finished may still have left a swapped-out original
// behind — trusting the recorded state to decide would leave exactly the
// artifacts a cancel exists to clear.
func (c *LocalClient) cancelledArtifactTables(apply *storage.Apply, tasks []*storage.Task) map[string]*namespaceArtifacts {
	byNamespace := map[string]*namespaceArtifacts{}
	for _, task := range tasks {
		if task == nil || task.ApplyID != apply.ID {
			continue
		}
		if task.TableName == "" {
			// A multi-table task records no single table, so its artifact names
			// cannot be derived. Naming the wrong table would destroy the wrong
			// copy, so it is left for an operator to reclaim by hand.
			c.logger.Warn("task records no table, so a cancel cannot reclaim its artifacts",
				append(task.LogAttrs(), "database", apply.Database)...)
			continue
		}
		namespace := task.Namespace
		if byNamespace[namespace] == nil {
			byNamespace[namespace] = &namespaceArtifacts{task: task}
		}
		entry := byNamespace[namespace]
		if !slices.Contains(entry.tables, task.TableName) {
			entry.tables = append(entry.tables, task.TableName)
		}
		constraints := c.taskConstraintChanges(apply, task)
		entry.addedConstraints = appendTableNames(entry.addedConstraints, task.TableName, constraints.added)
		entry.validatedConstraints = appendTableNames(entry.validatedConstraints, task.TableName, constraints.validated)
		entry.droppedConstraints = appendTableNames(entry.droppedConstraints, task.TableName, constraints.dropped)
		if constraints.unread && !slices.Contains(entry.unreadTables, task.TableName) {
			entry.unreadTables = append(entry.unreadTables, task.TableName)
		}
	}
	for _, entry := range byNamespace {
		c.withholdReplacedConstraints(apply, entry)
	}
	return byNamespace
}

// withholdReplacedConstraints takes the constraints the plan replaces under
// the same name out of the ones the release may drop. A replacement drops the
// old rule and adds the new one NOT VALID, in one statement or in steps of
// their own, so once the old rule is gone the unvalidated rule a failed or
// cancelled validation leaves is the only one checking the table's writes.
// Dropping it would leave the table with neither. A name the plan drops and
// never adds again is not in either list, so withholding it changes nothing.
func (c *LocalClient) withholdReplacedConstraints(apply *storage.Apply, entry *namespaceArtifacts) {
	for table, dropped := range entry.droppedConstraints {
		for _, name := range dropped {
			if !slices.Contains(entry.addedConstraints[table], name) && !slices.Contains(entry.validatedConstraints[table], name) {
				continue
			}
			c.logger.Info("a release keeps a constraint the schema change replaced under the same name: it is the only rule on the table",
				append(apply.LogAttrs(), "namespace", entry.task.Namespace, "table", table, "constraint", name)...)
		}
		entry.addedConstraints[table] = withoutNames(entry.addedConstraints[table], dropped)
		entry.validatedConstraints[table] = withoutNames(entry.validatedConstraints[table], dropped)
	}
	for _, table := range entry.unreadTables {
		delete(entry.addedConstraints, table)
		delete(entry.validatedConstraints, table)
	}
}

// appendTableNames adds names to table's entry in byTable, creating the map
// when it is nil and names is not empty.
func appendTableNames(byTable map[string][]string, table string, names []string) map[string][]string {
	if len(names) == 0 {
		return byTable
	}
	if byTable == nil {
		byTable = map[string][]string{}
	}
	byTable[table] = append(byTable[table], names...)
	return byTable
}

// withoutNames returns names with every entry of drop removed.
func withoutNames(names, drop []string) []string {
	return slices.DeleteFunc(slices.Clone(names), func(name string) bool {
		return slices.Contains(drop, name)
	})
}

// constraintChanges is what one task's statement does to its table's
// constraints, as far as a release needs to know.
type constraintChanges struct {
	// added names the constraints the statement adds, when the task ran.
	added []string
	// validated names the constraints the statement validates in a step of
	// its own, whether or not the task ran: the plan's intent is what makes
	// an unvalidated constraint of that name an unfinished step.
	validated []string
	// dropped names the constraints the statement drops, whether or not the
	// task ran: a plan that drops a name and adds it again replaces that
	// rule, whichever step a failure or cancel stops at.
	dropped []string
	// unread is set when the statement could not be read.
	unread bool
}

// taskConstraintChanges reads which constraints a task's statement adds,
// validates and drops. A task that never left pending added nothing, so a
// constraint of the same name on its table is not the apply's to remove. A
// statement that cannot be read marks its table unread, and the release then
// removes no constraint there: it cannot tell whether the statement dropped
// the rule an unvalidated one replaces.
func (c *LocalClient) taskConstraintChanges(apply *storage.Apply, task *storage.Task) constraintChanges {
	parser, err := c.statementParser()
	if err != nil {
		c.logger.Warn("a release cannot read which constraints the task added, so it removes none of them",
			append(task.LogAttrs(), "database", apply.Database, "error", err)...)
		return constraintChanges{unread: true}
	}
	changes, err := parser.ObjectChanges(task.DDL)
	if err != nil {
		c.logger.Warn("a release cannot read which constraints the task added, so it removes none of them",
			append(task.LogAttrs(), "database", apply.Database, "error", err)...)
		return constraintChanges{unread: true}
	}
	result := constraintChanges{
		validated: constraintNames(changes.Validated),
		dropped:   constraintNames(changes.Dropped),
	}
	if !state.IsState(task.State, state.Task.Pending) {
		result.added = constraintNames(changes.Added)
	}
	return result
}

// constraintNames names the constraints among objects.
func constraintNames(objects []ddl.SchemaObject) []string {
	var names []string
	for _, object := range objects {
		if object.Kind == ddl.ObjectConstraint {
			names = append(names, object.Name)
		}
	}
	return names
}

// logSkippedArtifactRelease records that a cancel left artifacts on the target,
// on both the server log and the apply log. An operator seeing a cancelled
// schema change needs to know a copy survived it, and why.
func (c *LocalClient) logSkippedArtifactRelease(ctx context.Context, apply *storage.Apply, kind artifactRelease, err error) {
	reason := skippedArtifactReleaseReason(err)

	var stopped *artifactReleaseError
	if errors.As(err, &stopped) && stopped.reclaimedAny {
		// Part of the release landed, and the entries before this one say where
		// it went. Saying the copy was left in place would contradict them.
		c.logger.Warn("schema change artifact release stopped part-way; the rest was left on the target",
			append(apply.LogAttrs(), "namespace", stopped.namespace, "outcome", kind.outcome, "reason", reason, "error", err)...)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelWarn, storage.LogEventInfo, storage.LogSourceSchemaBot,
			fmt.Sprintf("Reclaiming the %s schema change's artifacts stopped in %s because %s. The entries before this one name what was reclaimed; everything else was left on the target", kind.outcome, stopped.namespace, reason), "", "")
		return
	}

	c.logger.Warn("schema change left its artifacts on the target",
		append(apply.LogAttrs(), "outcome", kind.outcome, "reason", reason, "error", err)...)
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelWarn, storage.LogEventInfo, storage.LogSourceSchemaBot,
		fmt.Sprintf("What the %s schema change left on the target was not reclaimed because %s", kind.outcome, reason), "", "")
}

// skippedArtifactReleaseReason states why a release left artifacts behind, in
// the terms an operator acts on.
func skippedArtifactReleaseReason(err error) string {
	switch {
	case errors.Is(err, storage.ErrActiveApplyExists):
		return "another schema change is running against the same target and may own the copy"
	case errors.Is(err, context.DeadlineExceeded):
		return "the release ran past the time it is allowed to hold the target, so it gave the target back"
	default:
		return "the release failed"
	}
}
