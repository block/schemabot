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
// The primary (index 0) is the reviewed baseline and classifies Match against
// itself; every other deployment is compared to it with tern.CompareChangeSets.
// The result fails closed: an empty result set, a primary baseline that errored
// or is otherwise unusable, or any deployment that errored or diverged makes the
// rollup not Clean. It relies on PlanDeploymentDiffs' contract that the input
// carries exactly one entry per configured deployment, primary first, so a
// missing deployment cannot silently pass.
func RollupDeploymentDiffs(diffs []DeploymentPlanDiff) (PlanRollup, error) {
	if len(diffs) == 0 {
		return PlanRollup{}, fmt.Errorf("no deployment diffs to roll up")
	}

	baseline := tern.ChangeSet{Changes: diffs[0].Changes, Shards: diffs[0].Shards}
	baselineUsable := diffs[0].Err == nil

	// Validate the baseline is internally well-formed before trusting it as the
	// comparison target. A self-comparison surfaces malformed or unparseable
	// change content that would otherwise let a single-deployment rollup report
	// clean, or classify the primary as a match, without a trustworthy comparison
	// ever running.
	var baselineErr error
	if baselineUsable {
		if _, err := tern.CompareChangeSets(baseline, baseline); err != nil {
			baselineUsable = false
			baselineErr = err
		}
	}

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
			if baselineErr != nil {
				entry.Class = DeploymentErrored
				entry.Err = fmt.Errorf("reviewed primary plan is not a usable baseline: %w", baselineErr)
				clean = false
			} else {
				entry.Class = DeploymentMatch
			}
		case !baselineUsable:
			// Without a usable baseline no deployment can be confirmed to match, so
			// every deployment blocks rather than being compared to nothing.
			entry.Class = DeploymentErrored
			entry.Err = fmt.Errorf("primary reviewed plan is not a usable baseline; cannot confirm deployment matches the reviewed changes")
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
