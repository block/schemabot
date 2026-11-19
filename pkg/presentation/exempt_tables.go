package presentation

// ExemptTablesListLimit is how many of a namespace's exempt tables a plan
// disclosure names. A target can hold thousands of tables a naming convention
// skips, such as archive tables partition rotation creates, and naming every
// one would push the plan comment past GitHub's size limit and replace the
// whole plan with a notice. The count is always stated in full.
const ExemptTablesListLimit = 50

// ExemptTableNames returns the exempt tables a disclosure names and how many
// it leaves out, so the PR comment and the CLI list the same names.
func ExemptTableNames(tables []string) (shown []string, more int) {
	if len(tables) <= ExemptTablesListLimit {
		return tables, 0
	}
	return tables[:ExemptTablesListLimit], len(tables) - ExemptTablesListLimit
}
