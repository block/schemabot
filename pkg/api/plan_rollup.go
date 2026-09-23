package api

import (
	"fmt"

	"github.com/block/schemabot/pkg/engine"
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
	// Blocked is informational beside drift classification. It does not affect
	// Clean: apply admission separately refuses changes blocked by the target.
	Blocked int
	Diff    tern.ChangeSetDiff
	Err     error

	// ChangeSet is what this member would run: the change set its own diff
	// produced, or the reviewed plan's for the primary. Empty for a member that
	// errored, which has no plan to describe.
	//
	// It holds the caller's own change and shard messages rather than copies of
	// them, so a caller that mutates what it passed to RollupDeploymentDiffs
	// mutates what every entry reports. Read it; do not write through it.
	ChangeSet tern.ChangeSet
	// PlanFingerprint keys ChangeSet by the work it would run, so a reader can
	// group members that would run the same plan without comparing every pair.
	// Two members share it exactly when tern.CompareChangeSets reports them
	// identical.
	//
	// Every member that classified as anything but errored has one, including a
	// diverged member: reaching any of those classifications means the member's
	// change set was canonicalized, and the key is read straight off that
	// canonical form.
	//
	// It is empty for a member that errored, and that emptiness is not a group.
	// Grouping on the raw value — keying a map by it — collects every errored
	// member under one key and renders them as members agreeing on a plan, which
	// is the outcome keying nothing was meant to prevent. Check Class before
	// grouping, and leave an errored member out.
	PlanFingerprint string

	// PlanIdentifier names the stored plan this member will run, set when the
	// member was planned on its own and its plan was persisted as a row of its
	// own. Empty means the member runs the plan the apply itself was created
	// from — which is every member under mirrored planning, and the primary
	// under either.
	PlanIdentifier string
}

// PlanRollup aggregates every rollout member's review-time classification for a
// database. Clean means every member passed the contract it was classified
// under: under PlanMirrored that each matches the reviewed plan, under
// PlanIndependent that each produced a usable plan of its own. Any divergence,
// error, or the primary baseline itself being unusable makes it false so the
// review gate fails closed.
type PlanRollup struct {
	Entries []DeploymentRollupEntry
	Clean   bool
	// Planning is the contract the members were classified under, and decides
	// what Clean means. Under PlanMirrored a clean rollup says every member
	// would run the same plan. Under PlanIndependent it says every member
	// produced a plan of its own, which are not expected to match — so a reader
	// of the rollup cannot describe it without knowing which contract produced
	// it.
	Planning MemberPlanning
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
// Match against itself, every other member is compared to it, and a difference
// is drift that blocks. Under PlanIndependent no member is compared to another:
// each was planned against its own live schema, so every member that produced a
// usable diff classifies Planned.
//
// Under either planning every member's change set is canonicalized exactly once
// with tern.Canonicalize, and the comparisons and grouping keys the rollup
// publishes are read off those canonical forms. That is also the gate a member's
// content passes: change content SchemaBot cannot read fails the member closed
// there, before anything is concluded from it.
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
	// canonicalizing the baseline below error, so a primary whose type maps to no
	// known grammar fails the rollup closed rather than being parsed by a guess.
	baselineDialect := schema.DialectForDatabaseType(diffs[0].DatabaseType)

	// The baseline is usable only when the primary neither errored in the producer
	// nor carries malformed content. Canonicalizing it surfaces malformed or
	// unparseable change content that would otherwise let a single-deployment
	// rollup report clean, or classify the primary as a match, without a
	// trustworthy comparison ever running.
	//
	// The canonical form is what every member below is compared against and what
	// the primary is keyed by, so the reviewed plan is parsed once for the whole
	// rollup rather than once per member that is measured against it.
	var baselineCanonical tern.CanonicalChangeSet
	baselineCause := diffs[0].Err
	if baselineCause == nil {
		canonical, err := tern.Canonicalize(baselineDialect, baseline)
		if err != nil {
			baselineCause = err
		} else {
			baselineCanonical = canonical
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
		memberSet := tern.ChangeSet{Changes: d.Changes, Shards: d.Shards}
		// The canonical form of what this member would run, set by whichever branch
		// classifies the member as still passing and read below to key it. A branch
		// that leaves it unset is one that errored the entry, and an errored member
		// is never keyed.
		var memberCanonical tern.CanonicalChangeSet
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
				memberCanonical = baselineCanonical
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
			memberCanonical = classifyAgainstBaseline(&entry, baselineCanonical, baselineDialect, memberSet)
			// Only an exact match passes. The branch classifies Match, Diverged or
			// Errored, and both of the others block the review.
			if entry.Class != DeploymentMatch {
				clean = false
			}
		}
		recordMemberPlan(&entry, memberSet, memberCanonical)
		entries[i] = entry
	}

	return PlanRollup{Entries: entries, Clean: clean, Planning: planning}, nil
}

// classifyAgainstBaseline measures one member's change set against the reviewed
// baseline, records the outcome on the entry, and returns the canonical form the
// member is keyed by — the zero value for a member that errored, which is never
// keyed.
//
// Canonicalizing the member is what reads its DDL, so content the member's own
// grammar cannot parse fails it closed here rather than being compared as if it
// had been understood. The comparison that follows reads two canonical forms and
// parses nothing.
func classifyAgainstBaseline(entry *DeploymentRollupEntry, baseline tern.CanonicalChangeSet, dialect schema.Dialect, member tern.ChangeSet) tern.CanonicalChangeSet {
	canonical, err := tern.Canonicalize(dialect, member)
	if err != nil {
		entry.Class = DeploymentErrored
		entry.Err = fmt.Errorf("deployment plan is not usable: %w", err)
		return tern.CanonicalChangeSet{}
	}
	// Both sides were canonicalized under the primary's dialect, which the caller
	// has already confirmed is this deployment's own, so this refuses nothing a
	// well-formed rollup produces. It stays because a comparison that cannot be
	// performed must never be reported as agreement.
	diff, err := baseline.CompareTo(canonical)
	if err != nil {
		entry.Class = DeploymentErrored
		entry.Err = fmt.Errorf("cannot compare the deployment's plan to the reviewed plan: %w", err)
		return tern.CanonicalChangeSet{}
	}
	if !diff.Empty() {
		entry.Class = DeploymentDiverged
		entry.Diff = diff
		return canonical
	}
	entry.Class = DeploymentMatch
	return canonical
}

// recordMemberPlan records what a classified member would run: the change set
// itself, the key that groups it with members running the same work, and the
// count of changes its target will refuse.
//
// The key comes from the canonical form the member's classification already
// built, so keying a member parses nothing and cannot fail on content that was
// just read successfully. That is why this records rather than gates: the gate
// is the canonicalization each classifying branch performs, and a member that
// failed it arrives here already errored.
//
// An errored member is left alone. It has no plan to describe, and the count
// would be read from the plan the rollup just declared unusable — a
// precise-looking number a reviewer would take for a real refusal total. Its
// empty key is not a group either: a reader must check Class before grouping,
// or every errored member collects under one key and renders as members
// agreeing on a plan.
func recordMemberPlan(entry *DeploymentRollupEntry, cs tern.ChangeSet, canonical tern.CanonicalChangeSet) {
	if entry.Class == DeploymentErrored {
		return
	}
	entry.ChangeSet = cs
	entry.PlanFingerprint = canonical.Fingerprint()
	entry.Blocked = countBlockedChanges(cs)
}

// rollupIndependentMembers classifies members that were each planned against
// their own live schema. No member is compared to another, so a difference
// between them is never drift. What still blocks is a member that could not be
// planned at all: a producer error, or change content that will not parse under
// the member's own grammar. Canonicalizing a member's change set is what reads
// that content, so it surfaces a plan SchemaBot cannot make sense of without
// ever measuring one member against another.
//
// The blocked count each member publishes matters more here than it does under
// mirrored planning. Mirrored members hold the same change set by construction,
// so the primary's count covers them all; independent members hold different
// change sets, so this is the only place a non-primary member's refused DDL can
// surface at review time.
func rollupIndependentMembers(diffs []DeploymentPlanDiff) PlanRollup {
	entries := make([]DeploymentRollupEntry, len(diffs))
	clean := true
	for i, d := range diffs {
		entry := DeploymentRollupEntry{
			DatabaseType: d.DatabaseType,
			Deployment:   d.Deployment,
			Target:       d.Target,
		}
		memberSet := tern.ChangeSet{Changes: d.Changes, Shards: d.Shards}
		var memberCanonical tern.CanonicalChangeSet
		switch {
		case d.Err != nil:
			entry.Class = DeploymentErrored
			entry.Err = d.Err
			clean = false
		default:
			// Each member is read, and keyed, under its own grammar. Members here
			// are never compared to each other, so nothing has established that
			// they share a dialect the way the mirrored path's baseline does.
			canonical, err := tern.Canonicalize(schema.DialectForDatabaseType(d.DatabaseType), memberSet)
			if err != nil {
				entry.Class = DeploymentErrored
				entry.Err = fmt.Errorf("member plan is not usable: %w", err)
				clean = false
			} else {
				memberCanonical = canonical
				entry.Class = DeploymentPlanned
			}
		}
		recordMemberPlan(&entry, memberSet, memberCanonical)
		entries[i] = entry
	}
	return PlanRollup{Entries: entries, Clean: clean, Planning: PlanIndependent}
}

// countBlockedChanges counts the table changes the target's engine will refuse
// at apply. It walks the change set's authoritative representation so a
// sharded namespace, which the plan carries both collapsed and per shard, is
// counted once per shard the same way the drift comparison counts it.
func countBlockedChanges(cs tern.ChangeSet) int {
	blocked := 0
	for _, table := range cs.AuthoritativeTableChanges() {
		if table.GetExecutionMode() == engine.ExecutionModeBlocked {
			blocked++
		}
	}
	return blocked
}
