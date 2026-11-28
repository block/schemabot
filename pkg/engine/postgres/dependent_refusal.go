package postgres

import (
	"fmt"
	"strings"

	"github.com/block/pg-sprite/pkg/planner"
)

// dependentObjectCauses names the objects that keep PostgreSQL from
// running a refused column change: the views, row security policies,
// triggers, and generated columns that read a column the statement retypes
// or drops, and the partitioned tables whose key includes it. PostgreSQL
// refuses the change while any of them stands, even a type change that
// rewrites nothing, so the operator's next step is to change those objects,
// and naming them saves a search of the catalog. Each cause is a clause of
// the sentence operationRefusalReason composes.
func dependentObjectCauses(decisions []planner.Decision) []string {
	var causes []string
	for _, d := range decisions {
		if n := len(d.DependentViews); n > 0 {
			causes = append(causes, fmt.Sprintf("it changes or drops a column that %s %s %s; drop or redefine %s outside SchemaBot",
				plural(n, "view", "views"), strings.Join(d.DependentViews, ", "), plural(n, "reads", "read"), plural(n, "the view", "the views")))
		}
		if n := len(d.DependentPolicies); n > 0 {
			causes = append(causes, fmt.Sprintf("it changes or drops a column that row security %s %s %s; drop %s before the change and recreate %s after it",
				plural(n, "policy", "policies"), strings.Join(d.DependentPolicies, ", "), plural(n, "reads", "read"), plural(n, "the policy", "the policies"), plural(n, "it", "them")))
		}
		if n := len(d.DependentTriggers); n > 0 {
			causes = append(causes, fmt.Sprintf("it changes or drops a column that %s %s %s in UPDATE OF or WHEN; drop or redefine %s",
				plural(n, "trigger", "triggers"), strings.Join(d.DependentTriggers, ", "), plural(n, "names", "name"), plural(n, "the trigger", "the triggers")))
		}
		if n := len(d.DependentGeneratedColumns); n > 0 {
			causes = append(causes, fmt.Sprintf("it changes or drops a column that %s %s %s; change %s first",
				plural(n, "generated column", "generated columns"), strings.Join(d.DependentGeneratedColumns, ", "), plural(n, "reads", "read"), plural(n, "the generated column", "the generated columns")))
		}
		if len(d.PartitionKeyTables) > 0 {
			causes = append(causes, fmt.Sprintf("it changes or drops a column in the partition key of %s, which PostgreSQL never allows; create a new partitioned table with the new key and move the rows into it",
				strings.Join(d.PartitionKeyTables, ", ")))
		}
	}
	return causes
}

// plural picks the singular or plural form for n.
func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}
