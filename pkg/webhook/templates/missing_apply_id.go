package templates

import (
	"fmt"
	"strings"
)

// ApplyIDCandidate is an apply on the PR that a command named without an apply
// ID could have meant.
type ApplyIDCandidate struct {
	ApplyID     string
	Database    string
	Environment string
}

// MissingApplyIDData is what a missing-apply-ID reply renders from: the
// command the operator typed and the applies on the PR it could act on.
type MissingApplyIDData struct {
	// Command is the PR comment command the operator typed, such as "cutover".
	Command string
	// Environment is the environment the command named; empty when it named
	// none.
	Environment string
	// Tenant is the deployment's own tenant. When set, every rendered command
	// carries it so pasting one addresses this deployment.
	Tenant string
	// Candidates are the applies on the PR the command could act on, most
	// recent first.
	Candidates []ApplyIDCandidate
	// Complete reports that Candidates is every such apply: the lookup
	// succeeded and no other deployment holds applies for this PR that this
	// one cannot see. Only a complete lookup may say there is nothing to act
	// on.
	Complete bool
}

// RenderControlMissingApplyID renders the reply to an apply-scoped control
// command (stop, start, cancel, cutover, release, revert, skip-revert) that
// named no apply ID. It names the apply the command could act on and the
// exact command to comment, so the operator never has to look the ID up.
func RenderControlMissingApplyID(data MissingApplyIDData) string {
	return offerSupportChannel(renderMissingApplyID(data,
		fmt.Sprintf("This PR has no apply%s that `%s` can act on.", inEnvironment(data.Environment), data.Command),
		"Copy the command from the apply's progress comment. It carries the apply ID.",
		"Applies run by another deployment carry their command on their progress comment."))
}

// RenderRollbackMissingApplyID renders the reply to `schemabot rollback` with
// no apply ID. Candidates are the completed applies a rollback could revert.
// It also names rollback-confirm, which takes no apply ID, since an operator
// confirming a generated rollback types the same words.
func RenderRollbackMissingApplyID(data MissingApplyIDData) string {
	body := renderMissingApplyID(data,
		fmt.Sprintf("This PR has no completed apply%s to roll back.", inEnvironment(data.Environment)),
		"Copy the command from the summary comment of the completed apply. It carries the apply ID.",
		"Applies run by another deployment carry their command on their summary comment.")
	confirm := TenantCommand("schemabot rollback-confirm", environmentOrPlaceholder(data.Environment), data.Tenant)
	return offerSupportChannel(body + fmt.Sprintf("\n\nTo confirm a generated rollback, comment `%s`.", confirm))
}

func renderMissingApplyID(data MissingApplyIDData, nothingToActOn, lookupFallback, otherDeployments string) string {
	var b strings.Builder
	b.WriteString("## Missing Apply ID\n\n")
	switch len(data.Candidates) {
	case 0:
		if data.Complete {
			b.WriteString(nothingToActOn)
			return b.String()
		}
		b.WriteString(lookupFallback)
		return b.String()
	case 1:
		candidate := data.Candidates[0]
		fmt.Fprintf(&b, "Comment this to `%s` the `%s` apply in `%s`:\n", data.Command, candidate.Database, candidate.Environment)
		writeCommandBlock(&b, candidateCommand(data, candidate))
	default:
		b.WriteString("Comment the command for the apply you mean:\n")
		for _, candidate := range data.Candidates {
			fmt.Fprintf(&b, "\n`%s` in `%s`:\n", candidate.Database, candidate.Environment)
			writeCommandBlock(&b, candidateCommand(data, candidate))
		}
	}
	if !data.Complete {
		b.WriteString("\n" + otherDeployments)
	}
	return strings.TrimRight(b.String(), "\n")
}

func candidateCommand(data MissingApplyIDData, candidate ApplyIDCandidate) string {
	return TenantCommand(fmt.Sprintf("schemabot %s %s", data.Command, candidate.ApplyID), candidate.Environment, data.Tenant)
}

func inEnvironment(environment string) string {
	if environment == "" {
		return ""
	}
	return fmt.Sprintf(" in `%s`", environment)
}

func environmentOrPlaceholder(environment string) string {
	if environment == "" {
		return "<environment>"
	}
	return environment
}
