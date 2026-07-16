package webhook

import (
	"fmt"
	"strings"
)

// maxBaseDriftFilesShown caps how many changed base files the downgrade comment
// lists so a large base change cannot push the PR comment past GitHub's size
// limit and hide the confirmation instructions.
const maxBaseDriftFilesShown = 10

// baseDriftDowngradeReason builds the AutoConfirmDowngradeReason shown when
// automatic apply is paused because the schema directory changed on the base
// branch since the PR diverged. changedFiles may be empty when the specific
// files could not be listed; the reason still tells the operator what to do.
func baseDriftDowngradeReason(baseRef string, changedFiles []string) string {
	base := baseRef
	if base == "" {
		base = "the base branch"
	} else {
		base = "`" + base + "`"
	}

	var b strings.Builder
	fmt.Fprintf(&b, "the schema files changed on %s since this branch diverged, so this plan may not reflect the current base. Update the branch and re-run apply, or review the plan below and run apply-confirm to apply it as-is.", base)

	if len(changedFiles) > 0 {
		fmt.Fprintf(&b, "\n\nChanged on %s:", base)
		shown := changedFiles
		if len(shown) > maxBaseDriftFilesShown {
			shown = shown[:maxBaseDriftFilesShown]
		}
		for _, f := range shown {
			fmt.Fprintf(&b, "\n- `%s`", f)
		}
		if remaining := len(changedFiles) - len(shown); remaining > 0 {
			fmt.Fprintf(&b, "\n- …and %d more", remaining)
		}
	}
	return b.String()
}

// baseDriftUnknownDowngradeReason is shown when SchemaBot could not determine
// whether the base branch changed the schema directory. It fails closed to
// manual confirmation rather than auto-applying against an unverified base.
func baseDriftUnknownDowngradeReason(baseRef string) string {
	base := baseRef
	if base == "" {
		base = "the base branch"
	} else {
		base = "`" + base + "`"
	}
	return fmt.Sprintf("SchemaBot could not verify that this branch is current with %s. Update the branch and re-run apply, or review the plan below and run apply-confirm to apply it as-is.", base)
}
