// local_cancelled_artifacts.go reclaims what a cancelled schema change left on
// the target, so an abandoned row copy does not sit on the target database
// forever.
//
// The engine cannot make this decision alone: its artifacts are named after the
// target's own tables, so a copy another apply is actively writing carries the
// same names as one nobody owns any more. The apply-target lock, and the
// active-apply re-check under it, is what tells the two apart.
package tern

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/engine"
	"github.com/block/schemabot/pkg/storage"
)

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

	return c.storage.Applies().WithExclusiveTarget(ctx, apply, func(ctx context.Context) error {
		for _, namespace := range slices.Sorted(maps.Keys(byNamespace)) {
			if err := c.releaseNamespaceArtifacts(ctx, eng, apply, namespace, byNamespace[namespace]); err != nil {
				return err
			}
		}
		return nil
	})
}

// releaseNamespaceArtifacts reclaims one schema's artifacts and records where
// its data went, so an operator can answer "where did my copy go" from the pull
// request timeline rather than a server-log dig.
func (c *LocalClient) releaseNamespaceArtifacts(ctx context.Context, eng engine.Engine, apply *storage.Apply, namespace string, artifacts *namespaceArtifacts) error {
	creds, err := c.credentialsForTask(artifacts.task)
	if err != nil {
		return fmt.Errorf("resolve credentials to reclaim artifacts in %s: %w", namespace, err)
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
		return nil
	}
	if err != nil {
		return fmt.Errorf("reclaim cancelled schema change artifacts in %s: %w", namespace, err)
	}

	if len(result.Preserved) == 0 && len(result.Discarded) == 0 {
		c.logger.Info("cancelled schema change left no artifacts on the target",
			append(apply.LogAttrs(), "namespace", namespace, "tables", artifacts.tables)...)
		return nil
	}

	c.logger.Info("reclaimed cancelled schema change artifacts",
		append(apply.LogAttrs(),
			"namespace", namespace,
			"preserved", len(result.Preserved),
			"discarded", len(result.Discarded))...)
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelInfo, storage.LogEventInfo, storage.LogSourceSchemaBot,
		releasedArtifactsMessage(result), "", "")
	return nil
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
	reason := "the release failed"
	if errors.Is(err, storage.ErrActiveApplyExists) {
		reason = "another schema change is running against the same target and may own the copy"
	}

	c.logger.Warn("cancelled schema change left its artifacts on the target",
		append(apply.LogAttrs(), "reason", reason, "error", err)...)
	c.logApplyEvent(ctx, apply.ID, nil, storage.LogLevelWarn, storage.LogEventInfo, storage.LogSourceSchemaBot,
		fmt.Sprintf("The cancelled schema change's copy was left on the target because %s", reason), "", "")
}
