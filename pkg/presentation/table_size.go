package presentation

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"github.com/block/schemabot/pkg/ui"
)

// TableSizesInlineLimit is how many tables a size section lists in plan
// order. Past it the section lists the largest tables first, since they bound
// how long the apply runs; the PR comment also folds such a section into a
// collapsed block.
const TableSizesInlineLimit = 5

// TableSizesShown caps how many tables a size section lists past
// TableSizesInlineLimit: the largest, with the rest counted in a closing line,
// so a plan that indexes thousands of tables does not spend the PR comment's
// size limit, or a terminal's screen, on sizes.
const TableSizesShown = 64

// MemberTableSize is one rollout member's size estimate for a table.
type MemberTableSize struct {
	// Member names the member the way an operator addresses it.
	Member string
	// EstimatedBytes is the table's approximate on-disk footprint on the
	// member. Nil when the engine reported no estimate.
	EstimatedBytes *int64
}

// SumTableSizeBytes sums a table's byte estimates across the members that
// report one, and counts those members.
func SumTableSizeBytes(sizes []MemberTableSize) (total int64, sized int) {
	for _, s := range sizes {
		if s.EstimatedBytes == nil {
			continue
		}
		total += *s.EstimatedBytes
		sized++
	}
	return total, sized
}

// ListTableSizes returns the tables a size section lists, in the order it
// lists them, how many it leaves out, and how many of those have no estimate:
// every table in plan order up to TableSizesInlineLimit, otherwise the
// TableSizesShown largest by bytes. A table with no estimate sorts after every
// table with one, so it is the first to be left out, and it is counted
// separately so the section never hides a table whose size is unknown behind a
// count that reads as the smallest tables.
func ListTableSizes[T any](tables []T, bytes func(T) *int64) (listed []T, unlisted, unlistedUnsized int) {
	if len(tables) <= TableSizesInlineLimit {
		return tables, 0, 0
	}
	sorted := slices.Clone(tables)
	slices.SortStableFunc(sorted, func(a, b T) int {
		return CompareBytesLargestFirst(bytes(a), bytes(b))
	})
	listed = sorted[:min(len(sorted), TableSizesShown)]
	for _, t := range sorted[len(listed):] {
		if bytes(t) == nil {
			unlistedUnsized++
		}
	}
	return listed, len(sorted) - len(listed), unlistedUnsized
}

// UnlistedTables renders the closing line's count of the tables a size
// section leaves out, naming how many of them have no size estimate.
func UnlistedTables(unlisted, unlistedUnsized int) string {
	word := "tables"
	if unlisted == 1 {
		word = "table"
	}
	line := fmt.Sprintf("…and %d more %s", unlisted, word)
	if unlistedUnsized > 0 {
		line += fmt.Sprintf(" (%d without a size estimate)", unlistedUnsized)
	}
	return line
}

// FormatMemberTableSize renders one table's size clause across the rollout
// members that change it, with each member name rendered by name. A table one
// member changes names that member. A table several members change gives the
// total, then the largest size with its member named, since copy time scales
// with size and the largest member bounds the rollout, then the smallest size.
// A member with no estimate is named, or counted when there are several,
// since the total then understates the table. sizes must not be empty.
func FormatMemberTableSize(noun Noun, sizes []MemberTableSize, name func(string) string) string {
	members := len(sizes)
	total, sized := SumTableSizeBytes(sizes)
	switch {
	case sized == 0 && members == 1:
		return "size estimate unavailable on " + name(sizes[0].Member)
	case sized == 0:
		return fmt.Sprintf("size estimate unavailable on all %d %s", members, noun.Plural)
	case members == 1:
		return fmt.Sprintf("%s on %s", ui.FormatApproxBytes(total), name(sizes[0].Member))
	}
	ordered := slices.Clone(sizes)
	slices.SortStableFunc(ordered, func(a, b MemberTableSize) int {
		return CompareBytesLargestFirst(a.EstimatedBytes, b.EstimatedBytes)
	})
	largest, smallest := ordered[0], ordered[sized-1]
	var parts []string
	switch sized {
	case 1:
		parts = append(parts, fmt.Sprintf("%s on %s", ui.FormatApproxBytes(total), name(largest.Member)))
	case members:
		parts = append(parts, fmt.Sprintf("%s across %d %s", ui.FormatApproxBytes(total), members, noun.Plural))
	default:
		parts = append(parts, fmt.Sprintf("%s across %d of %d %s", ui.FormatApproxBytes(total), sized, members, noun.Plural))
	}
	if sized > 1 {
		parts = append(parts,
			fmt.Sprintf("largest %s on %s", ui.FormatApproxBytes(*largest.EstimatedBytes), name(largest.Member)),
			"smallest "+ui.FormatApproxBytes(*smallest.EstimatedBytes))
	}
	switch unsized := ordered[sized:]; len(unsized) {
	case 0:
	case 1:
		parts = append(parts, "size estimate unavailable on "+name(unsized[0].Member))
	default:
		parts = append(parts, fmt.Sprintf("size estimate unavailable on %d %s", len(unsized), noun.Plural))
	}
	return strings.Join(parts, " · ")
}

// CompareBytesLargestFirst orders two byte estimates largest first, with an
// unknown estimate after every known one: its size is unknown, not small.
func CompareBytesLargestFirst(a, b *int64) int {
	switch {
	case a == nil && b == nil:
		return 0
	case a == nil:
		return 1
	case b == nil:
		return -1
	default:
		return cmp.Compare(*b, *a)
	}
}
