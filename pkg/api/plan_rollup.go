package api

import (
	"fmt"

	"github.com/block/schemabot/pkg/tern"
)

// DeploymentClassification is how a deployment's review-time diff compares to the
// reviewed primary plan.
type DeploymentClassification int

const (
	// DeploymentMatch means the deployment would plan exactly the reviewed
	// changes. The primary is always Match against itself.
	DeploymentMatch DeploymentClassification = iota
	// DeploymentDiverged means the deployment would plan a different set of
	// changes than were reviewed — schema drift that must block approval.
	DeploymentDiverged
	// DeploymentErrored means the deployment's diff could not be computed or
	// compared. It must be treated as blocking, never as agreement.
	DeploymentErrored
)

func (c DeploymentClassification) String() string {
	switch c {
	case DeploymentMatch:
		return "match"
	case DeploymentDiverged:
		return "diverged"
	case DeploymentErrored:
		return "errored"
	default:
		return fmt.Sprintf("unknown(%d)", int(c))
	}
}

// DeploymentRollupEntry is one deployment's place in the review-time rollup: how
// it classified against the reviewed plan, the diff when it diverged, and the
// error when it could not be computed or compared.
type DeploymentRollupEntry struct {
	DatabaseType string
	Deployment   string
	Target       string

	Class DeploymentClassification
	Diff  tern.ChangeSetDiff
	Err   error
}

// PlanRollup aggregates every deployment's review-time classification for a
// database. Clean is true only when every deployment matches the reviewed plan;
// any divergence, error, or the primary baseline itself being unusable makes it
// false so the review gate fails closed.
type PlanRollup struct {
	Entries []DeploymentRollupEntry
	Clean   bool
}

// RollupDeploymentDiffs classifies each deployment's review-time diff against the
// reviewed primary plan and reports whether the rollup is clean.
//
// expectedDeployments is the configured deployment set in rollout order, primary
// first — the same order PlanDeploymentDiffs produces. The diffs must match it
// positionally: this turns the producer's structural convention (primary first,
// one entry per configured deployment) into an enforced contract, so a
// reordered, short, or otherwise mismatched result is rejected rather than
// letting a missing or misidentified deployment silently pass the gate.
//
// The primary (index 0) is the reviewed baseline and classifies Match against
// itself; every other deployment is compared to it with tern.CompareChangeSets.
// The result fails closed: a contract mismatch, a primary baseline that errored
// or is otherwise unusable, or any deployment that errored or diverged makes the
// rollup not Clean.
func RollupDeploymentDiffs(diffs []DeploymentPlanDiff, expectedDeployments []string) (PlanRollup, error) {
	if len(expectedDeployments) == 0 {
		return PlanRollup{}, fmt.Errorf("no expected deployments to roll up")
	}
	if len(diffs) != len(expectedDeployments) {
		return PlanRollup{}, fmt.Errorf("expected %d deployment diffs in rollout order, got %d", len(expectedDeployments), len(diffs))
	}
	for i, name := range expectedDeployments {
		if diffs[i].Deployment != name {
			return PlanRollup{}, fmt.Errorf("deployment diff %d is %q, expected %q; diffs must be in rollout order with the primary first and every configured deployment present", i, diffs[i].Deployment, name)
		}
	}

	baseline := tern.ChangeSet{Changes: diffs[0].Changes, Shards: diffs[0].Shards}

	// The baseline is usable only when the primary neither errored in the producer
	// nor carries malformed content. A self-comparison surfaces malformed or
	// unparseable change content that would otherwise let a single-deployment
	// rollup report clean, or classify the primary as a match, without a
	// trustworthy comparison ever running. A self-comparison of well-formed
	// content is provably empty, so it never false-diverges a legitimate baseline.
	baselineCause := diffs[0].Err
	if baselineCause == nil {
		if _, err := tern.CompareChangeSets(baseline, baseline); err != nil {
			baselineCause = err
		}
	}
	baselineUsable := baselineCause == nil

	entries := make([]DeploymentRollupEntry, len(diffs))
	clean := true
	for i, d := range diffs {
		entry := DeploymentRollupEntry{
			DatabaseType: d.DatabaseType,
			Deployment:   d.Deployment,
			Target:       d.Target,
		}
		switch {
		case d.Err != nil:
			entry.Class = DeploymentErrored
			entry.Err = d.Err
			clean = false
		case i == 0:
			// The reviewed primary plan is the baseline. It matches itself only when
			// its own content is well-formed; malformed content makes it unusable.
			// A producer error on the primary was already handled above, so a cause
			// here is a content error.
			if baselineCause != nil {
				entry.Class = DeploymentErrored
				entry.Err = fmt.Errorf("reviewed primary plan is not a usable baseline: %w", baselineCause)
				clean = false
			} else {
				entry.Class = DeploymentMatch
			}
		case !baselineUsable:
			// Without a usable baseline no deployment can be confirmed to match, so
			// every deployment blocks. Wrap the primary's root cause so each entry is
			// self-contained for triage without cross-referencing the primary's.
			entry.Class = DeploymentErrored
			entry.Err = fmt.Errorf("primary reviewed plan is not a usable baseline, cannot confirm deployment matches the reviewed changes: %w", baselineCause)
			clean = false
		default:
			diff, err := tern.CompareChangeSets(baseline, tern.ChangeSet{Changes: d.Changes, Shards: d.Shards})
			switch {
			case err != nil:
				entry.Class = DeploymentErrored
				entry.Err = err
				clean = false
			case !diff.Empty():
				entry.Class = DeploymentDiverged
				entry.Diff = diff
				clean = false
			default:
				entry.Class = DeploymentMatch
			}
		}
		entries[i] = entry
	}

	return PlanRollup{Entries: entries, Clean: clean}, nil
}
