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

	"github.com/block/schemabot/pkg/engine"
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
	if eng == nil {
		return fmt.Errorf("no engine configured for type: %s", c.config.Type)
	}
	if apply == nil {
		return fmt.Errorf("apply is required to reclaim cancelled schema change artifacts")
	}

	byNamespace := c.cancelledArtifactTables(apply, tasks)
	if len(byNamespace) == 0 {
		c.logger.Debug("cancelled schema change names no tables, so it left no artifacts to reclaim",
			apply.LogAttrs()...)
		return nil
	}

	releaseCtx, cancel := context.WithTimeout(ctx, cancelledArtifactReleaseTimeout)
	defer cancel()

	err := c.storage.Applies().WithExclusiveTarget(releaseCtx, apply, func(ctx context.Context) error {
		reclaimedAny := false
		for _, namespace := range slices.Sorted(maps.Keys(byNamespace)) {
			reclaimed, err := c.releaseNamespaceArtifacts(ctx, eng, apply, namespace, byNamespace[namespace])
			reclaimedAny = reclaimedAny || reclaimed
			if err != nil {
				return &artifactReleaseError{namespace: namespace, reclaimedAny: reclaimedAny, err: err}
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
	namespace    string
	reclaimedAny bool
	err          error
}

func (e *artifactReleaseError) Error() string {
	return fmt.Sprintf("reclaim cancelled schema change artifacts in %s: %v", e.namespace, e.err)
}

func (e *artifactReleaseError) Unwrap() error { return e.err }

// releaseNamespaceArtifacts reclaims one schema's artifacts and records where
// its data went, so an operator can answer "where did my copy go" from the pull
// request timeline rather than a server-log dig. It reports whether anything
// was reclaimed, including by a release that then failed.
func (c *LocalClient) releaseNamespaceArtifacts(ctx context.Context, eng engine.Engine, apply *storage.Apply, namespace string, artifacts *namespaceArtifacts) (bool, error) {
	creds, err := c.credentialsForTask(artifacts.task)
	if err != nil {
		return false, fmt.Errorf("resolve credentials to reclaim artifacts in %s: %w", namespace, err)
	}

	supported, result, err := engine.ReleaseCancelledArtifacts(ctx, eng, &engine.ReleaseArtifactsRequest{
		Database:    namespace,
		Tables:      artifacts.tables,
		Credentials: creds,
	})
	if !supported {
		// The engine's unfinished work lives in the service it drives, which
		// released it when the cancel reached it. There is nothing local.
		c.logger.Debug("engine leaves no artifacts on the target, so a cancel reclaims nothing",
			append(apply.LogAttrs(), "engine", eng.Name(), "namespace", namespace)...)
		return false, nil
	}
	// What the release did is recorded before its error is returned: a release
	// that fails part-way has still moved the copy, and the entry naming where
	// it went is what an operator follows to recover it.
	reclaimed := c.recordReleaseResult(ctx, apply, namespace, result)
	if err != nil {
		return reclaimed, err
	}
	if !reclaimed {
		c.logger.Info("cancelled schema change left no artifacts on the target",
			append(apply.LogAttrs(), "namespace", namespace, "tables", artifacts.tables)...)
	}
	return reclaimed, nil
}

// recordReleaseResult writes what one schema's release reclaimed and retained to
// the server log and the apply log, and reports whether it reclaimed anything.
func (c *LocalClient) recordReleaseResult(ctx context.Context, apply *storage.Apply, namespace string, result *engine.ReleaseArtifactsResult) bool {
	if result == nil {
		return false
	}
	reclaimed := len(result.Preserved) > 0 || len(result.Discarded) > 0
	if reclaimed {
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
	}
	return byNamespace
}

// logSkippedArtifactRelease records that a cancel left artifacts on the target,
// on both the server log and the apply log. An operator seeing a cancelled
// schema change needs to know a copy survived it, and why.
func (c *LocalClient) logSkippedArtifactRelease(ctx context.Context, apply *storage.Apply, err error) {
	reason := skippedArtifactReleaseReason(err)

	var stopped *artifactReleaseError
	if errors.As(err, &stopped) && stopped.reclaimedAny {
		// Part of the release landed, and the entries before this one say where
		// it went. Saying the copy was left in place would contradict them.
		c.logger.Warn("cancelled schema change artifact release stopped part-way; the rest was left on the target",
			append(apply.LogAttrs(), "namespace", stopped.namespace, "reason", reason, "error", err)...)
		c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelWarn, storage.LogEventInfo, storage.LogSourceSchemaBot,
			fmt.Sprintf("Reclaiming the cancelled schema change's artifacts stopped in %s because %s. The entries before this one name what was reclaimed; everything else was left on the target", stopped.namespace, reason), "", "")
		return
	}

	c.logger.Warn("cancelled schema change left its artifacts on the target",
		append(apply.LogAttrs(), "reason", reason, "error", err)...)
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelWarn, storage.LogEventInfo, storage.LogSourceSchemaBot,
		fmt.Sprintf("The cancelled schema change's copy was left on the target because %s", reason), "", "")
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
