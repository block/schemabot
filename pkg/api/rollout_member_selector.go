package api

import (
	"fmt"
	"strings"

	"github.com/block/schemabot/pkg/routing"
)

// maxListedRolloutMembers caps how many valid selectors a selection error
// names, so an environment with a long targets list does not produce an
// unreadable error. The remainder is summarized as a count.
const maxListedRolloutMembers = 10

// RolloutMemberSelectionError reports a target selector that does not name
// exactly one rollout member of a database/environment: it names none, or a
// bare target name addresses members in more than one deployment. It is the
// caller's mistake rather than a server failure, and the message lists the
// selectors that would have worked.
type RolloutMemberSelectionError struct {
	Database    string
	Environment string
	Selector    string
	// Matches are the members an ambiguous selector named, as MemberIDs. Empty
	// when the selector named no member.
	Matches []string
	// Valid are the selectors that each name one member, in rollout order.
	Valid []string
}

func (e *RolloutMemberSelectionError) Error() string {
	if len(e.Matches) > 1 {
		return fmt.Sprintf("target %q is ambiguous for database %q environment %q: it names %s; select one as deployment/target",
			e.Selector, e.Database, e.Environment, strings.Join(e.Matches, ", "))
	}
	return fmt.Sprintf("target %q is not a rollout member of database %q environment %q; valid targets: %s",
		e.Selector, e.Database, e.Environment, listSelectors(e.Valid))
}

// listSelectors renders at most maxListedRolloutMembers selectors and says how
// many more there are.
func listSelectors(selectors []string) string {
	if len(selectors) <= maxListedRolloutMembers {
		return strings.Join(selectors, ", ")
	}
	return fmt.Sprintf("%s (and %d more)", strings.Join(selectors[:maxListedRolloutMembers], ", "), len(selectors)-maxListedRolloutMembers)
}

// selectRolloutMember narrows a rollout to the one member a selector names.
//
// A selector names a member by its target alone, or by its MemberID
// (deployment/target). The bare form is what an operator reads in a targets
// list; the qualified form is what disambiguates it when two deployments
// address a target of the same name. A selector that matches more than one
// member is refused rather than resolved to the first match, because picking
// one would run a schema change against a target the operator did not choose.
func selectRolloutMember(database, environment string, members []routing.ExecutionTarget, selector string) (routing.ExecutionTarget, error) {
	var matches []routing.ExecutionTarget
	for _, member := range members {
		if member.Target == selector || member.MemberID() == selector {
			matches = append(matches, member)
		}
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	selErr := &RolloutMemberSelectionError{
		Database:    database,
		Environment: environment,
		Selector:    selector,
		Valid:       rolloutMemberSelectors(members),
	}
	for _, match := range matches {
		selErr.Matches = append(selErr.Matches, match.MemberID())
	}
	return routing.ExecutionTarget{}, selErr
}

// rolloutMemberSelectors returns the selector an operator would pass to name
// each member: its bare target where that is unambiguous, its MemberID where
// another member shares the target name.
func rolloutMemberSelectors(members []routing.ExecutionTarget) []string {
	counts := make(map[string]int, len(members))
	for _, member := range members {
		counts[member.Target]++
	}
	selectors := make([]string, 0, len(members))
	for _, member := range members {
		if counts[member.Target] > 1 {
			selectors = append(selectors, member.MemberID())
			continue
		}
		selectors = append(selectors, member.Target)
	}
	return selectors
}

// resolveRolloutMember resolves a database/environment's rollout and narrows
// it to the member a selector names.
//
// narrows reports whether the selection leaves any member out. Selecting the
// only member of a single-member environment selects the whole rollout, so a
// plan or apply made that way is not narrowed: it speaks for the rollout and
// can be rolled back like any other.
func (s *Service) resolveRolloutMember(database, environment, selector string) (member routing.ExecutionTarget, narrows bool, err error) {
	members, err := s.config.ResolveDatabaseTargets(database, environment)
	if err != nil {
		return routing.ExecutionTarget{}, false, fmt.Errorf("resolve rollout members for %s/%s: %w", database, environment, err)
	}
	member, err = selectRolloutMember(database, environment, members, selector)
	if err != nil {
		return routing.ExecutionTarget{}, false, err
	}
	return member, len(members) > 1, nil
}
