package presentation

import "slices"

// Failure is one failed member of an apply, a shard or a target, with the
// reason a surface shows for it. Each surface sanitizes the reason the way it
// renders errors before grouping, so two raw errors that read the same once
// sanitized fall into one group.
type Failure struct {
	Name   string
	Reason string
}

// FailureReason is one distinct reason some members failed for, with those
// members' names in the order they failed.
type FailureReason struct {
	Reason string
	Names  []string
}

// GroupFailureReasons groups failed members by their reason, so a change that
// fails the same way on many shards reads as one reason with a count instead
// of the same text repeated on every shard. The reason that failed the most
// members comes first; reasons with the same count keep the order their first
// member appears in. An empty reason is a group of its own.
func GroupFailureReasons(failures []Failure) []FailureReason {
	var groups []FailureReason
	index := make(map[string]int, len(failures))
	for _, f := range failures {
		i, ok := index[f.Reason]
		if !ok {
			i = len(groups)
			index[f.Reason] = i
			groups = append(groups, FailureReason{Reason: f.Reason})
		}
		groups[i].Names = append(groups[i].Names, f.Name)
	}
	slices.SortStableFunc(groups, func(a, b FailureReason) int {
		return len(b.Names) - len(a.Names)
	})
	return groups
}
