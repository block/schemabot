package presentation

import "fmt"

// RollbackBlockersListLimit is how many of a namespace's rollback blockers a
// plan names one line each. A namespace can hold many tables a schema file
// cannot describe, and a line per table past this would push the plan comment
// towards GitHub's size limit. The tables past the limit are counted in one
// line.
const RollbackBlockersListLimit = 10

// RollbackNotCaptured is the plan's line for a namespace whose current schema
// the plan could not save as schema files, so an apply of the plan cannot be
// rolled back. The plan still applies; the line says so before anyone applies
// it and names the way to undo the change instead.
//
// namespace arrives already marked up for the surface, so the PR comment and
// the CLI say the same sentence and differ only in how they quote.
func RollbackNotCaptured(namespace string) string {
	return fmt.Sprintf("No rollback for namespace %s: rollback needs a schema file for every table the plan manages, and SchemaBot can't write one yet for the tables below. The apply still runs. To undo it, revert the schema files in a new pull request.", namespace)
}

// RollbackBlockersShown returns how many of a namespace's blockers a plan
// names and how many it counts instead, so the PR comment and the CLI list
// the same tables.
func RollbackBlockersShown(total int) (shown, more int) {
	if total <= RollbackBlockersListLimit {
		return total, 0
	}
	return RollbackBlockersListLimit, total - RollbackBlockersListLimit
}

// RollbackBlockersMore is the line that counts the blockers past
// RollbackBlockersListLimit.
func RollbackBlockersMore(more int) string {
	if more == 1 {
		return "1 more table"
	}
	return fmt.Sprintf("%d more tables", more)
}
