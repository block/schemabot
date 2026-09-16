package api

import (
	"fmt"

	"github.com/block/schemabot/pkg/routing"
	"github.com/block/schemabot/pkg/schema"
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
	// DeploymentPlanned means the member was planned against its own live
	// schema and was never compared to the reviewed plan, because its
	// environment's members are not expected to hold the same schema. Its
	// changes are its own and do not block.
	DeploymentPlanned
)

func (c DeploymentClassification) String() string {
	switch c {
	case DeploymentMatch:
		return "match"
	case DeploymentDiverged:
		return "diverged"
	case DeploymentErrored:
		return "errored"
	case DeploymentPlanned:
		return "planned"
	default:
		return fmt.Sprintf("unknown(%d)", int(c))
	}
}

// MemberPlanning is how the rollout members of one database/environment relate
// to each other, which decides whether a difference between them is drift.
type MemberPlanning int

const (
	// PlanMirrored means the members are expected to hold the same schema, so
	// each is compared to the reviewed plan and any difference is drift that
	// blocks the review. This is the default: an environment opts out of it, and
	// never into it, so a config that does not say otherwise keeps blocking.
	PlanMirrored MemberPlanning = iota
	// PlanIndependent means each member is planned against its own live schema,
	// so a difference between members is ordinary rather than drift. Members are
	// still individually required to be plannable — an error on any of them
	// blocks the review.
	PlanIndependent
)

func (p MemberPlanning) String() string {
	switch p {
	case PlanMirrored:
		return "mirrored"
	case PlanIndependent:
		return "independent"
	default:
		return fmt.Sprintf("unknown(%d)", int(p))
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

// RollupDeploymentDiffs classifies each rollout member's review-time diff
// against the reviewed primary plan and reports whether the rollup is clean.
//
// expectedMembers is the configured member set in rollout order, primary first
// — the same order PlanDeploymentDiffs produces. The diffs must match it
// positionally on both deployment and target: this turns the producer's
// structural convention (primary first, one entry per configured member) into
// an enforced contract, so a reordered, short, or otherwise mismatched result
// is rejected rather than letting a missing or misidentified member silently
// pass the gate. Matching on the deployment alone would not be enough, because
// one deployment can address several targets and they would be
// indistinguishable.
//
// planning decides what a difference between members means. Under
// PlanMirrored the primary (index 0) is the reviewed baseline and classifies
// Match against itself, every other member is compared to it with
// tern.CompareChangeSets, and a difference is drift that blocks. Under
// PlanIndependent no member is compared to another: each was planned against
// its own live schema, so every member that produced a usable diff classifies
// Planned.
//
// The result fails closed under either planning: a contract mismatch, or any
// member that errored, makes the rollup not Clean. Under PlanMirrored a
// diverged member, or a primary baseline that is unusable, also blocks.
func RollupDeploymentDiffs(diffs []DeploymentPlanDiff, expectedMembers []routing.ExecutionTarget, planning MemberPlanning) (PlanRollup, error) {
	if len(expectedMembers) == 0 {
		return PlanRollup{}, fmt.Errorf("no expected rollout members to roll up")
	}
	if len(diffs) != len(expectedMembers) {
		return PlanRollup{}, fmt.Errorf("expected %d member diffs in rollout order, got %d", len(expectedMembers), len(diffs))
	}
	for i, member := range expectedMembers {
		got := routing.ExecutionTarget{Deployment: diffs[i].Deployment, Target: diffs[i].Target}
		if got.Deployment != member.Deployment || got.Target != member.Target {
			return PlanRollup{}, fmt.Errorf("member diff %d is %q, expected %q; diffs must be in rollout order with the primary first and every configured member present", i, got.MemberID(), member.MemberID())
		}
	}

	if planning == PlanIndependent {
		return rollupIndependentMembers(diffs), nil
	}

	baseline := tern.ChangeSet{Changes: diffs[0].Changes, Shards: diffs[0].Shards}

	// The primary's database type selects the grammar every comparison in this
	// rollup classifies and canonicalizes DDL with. An unregistered dialect makes
	// the self-comparison below error, so a primary whose type maps to no known
	// grammar fails the rollup closed rather than being parsed by a guess.
	baselineDialect := schema.DialectForDatabaseType(diffs[0].DatabaseType)

	// The baseline is usable only when the primary neither errored in the producer
	// nor carries malformed content. A self-comparison surfaces malformed or
	// unparseable change content that would otherwise let a single-deployment
	// rollup report clean, or classify the primary as a match, without a
	// trustworthy comparison ever running. A self-comparison of well-formed
	// content is provably empty, so it never false-diverges a legitimate baseline.
	baselineCause := diffs[0].Err
	if baselineCause == nil {
		if _, err := tern.CompareChangeSets(baselineDialect, baseline, baseline); err != nil {
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
		case schema.DialectForDatabaseType(d.DatabaseType) != baselineDialect:
			// Change sets canonicalized under different grammars cannot be compared:
			// a match under the wrong parser proves nothing. A deployment whose
			// dialect differs from the primary's blocks rather than being judged by
			// the primary's grammar. This is defense in depth: the production
			// producer stamps one database's single configured type onto every
			// deployment, so a mixed-dialect rollup only reaches here through a
			// producer bug or a hand-built result.
			entry.Class = DeploymentErrored
			entry.Err = fmt.Errorf("deployment database type %q (dialect %q) differs from the primary's %q (dialect %q); cannot compare change sets across dialects",
				d.DatabaseType, schema.DialectForDatabaseType(d.DatabaseType), diffs[0].DatabaseType, baselineDialect)
			clean = false
		default:
			diff, err := tern.CompareChangeSets(baselineDialect, baseline, tern.ChangeSet{Changes: d.Changes, Shards: d.Shards})
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

// rollupIndependentMembers classifies members that were each planned against
// their own live schema. No member is compared to another, so a difference
// between them is never drift. What still blocks is a member that could not be
// planned at all: a producer error, or change content that will not parse under
// the member's own grammar. Content is checked by comparing a member's change
// set to itself, which is provably empty when the content is well-formed, so the
// check surfaces malformed content without ever false-diverging a real plan.
func rollupIndependentMembers(diffs []DeploymentPlanDiff) PlanRollup {
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
		default:
			own := tern.ChangeSet{Changes: d.Changes, Shards: d.Shards}
			if _, err := tern.CompareChangeSets(schema.DialectForDatabaseType(d.DatabaseType), own, own); err != nil {
				entry.Class = DeploymentErrored
				entry.Err = fmt.Errorf("member plan is not usable: %w", err)
				clean = false
			} else {
				entry.Class = DeploymentPlanned
			}
		}
		entries[i] = entry
	}
	return PlanRollup{Entries: entries, Clean: clean}
}
