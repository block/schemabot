package presentation

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// An entry is reported only when every plan reports it, in the order the
// first plan lists the entries; a plan that reports nothing, such as a failed
// one, leaves nothing to report.
func TestUnmatchedDropTablesInEveryPlan(t *testing.T) {
	tests := []struct {
		name  string
		plans [][]string
		want  []string
	}{
		{name: "no plans", plans: nil, want: nil},
		{name: "one plan", plans: [][]string{{"_b", "_a"}}, want: []string{"_b", "_a"}},
		{name: "every plan reports both", plans: [][]string{{"_b", "_a"}, {"_a", "_b"}}, want: []string{"_b", "_a"}},
		{name: "another plan matched one", plans: [][]string{{"_b", "_a"}, {"_a"}}, want: []string{"_a"}},
		{name: "a plan that reports nothing", plans: [][]string{{"_a"}, nil}, want: nil},
		{name: "a repeated entry is reported once", plans: [][]string{{"_a", "_a"}, {"_a"}}, want: []string{"_a"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, UnmatchedDropTablesInEveryPlan(tt.plans))
		})
	}
}

// The count line agrees in number with how many entries it counts.
func TestUnmatchedDropTablesMore(t *testing.T) {
	assert.Equal(t, "1 more drop_tables entry names no table on the target, so this plan drops nothing for it. Fix the name, or remove the entry if the table is already gone.",
		UnmatchedDropTablesMore("drop_tables", 1))
	assert.Equal(t, "2 more drop_tables entries name no table on the target, so this plan drops nothing for them. Fix the names, or remove the entries whose tables are already gone.",
		UnmatchedDropTablesMore("drop_tables", 2))
}
