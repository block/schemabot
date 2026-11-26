package commands

import (
	"errors"
	"net/http"
	"strings"

	"github.com/charmbracelet/lipgloss"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
)

// changeGateRetryHint tells a watcher how to go on once an override it asked
// for from the CLI is approved: an approved override admits its requester's
// next request, so Enter needs no reason of its own.
const changeGateRetryHint = "Once a freeze approver other than you approves it, press Enter to try again."

// changeGateOnReasonHint is the hint under the reason override policy. The
// command above runs at once with no approver, and Enter, which sends no
// reason, is refused for as long as the freeze holds.
const changeGateOnReasonHint = "Run that in another terminal: it starts the change at once, and this watch follows it."

// changeGateLines is what the watcher shows when the freeze gate answers a
// start or cutover it sent, and whether err was such an answer. The watcher's
// keys carry no override reason, so a refusal names the exact CLI command that
// asks for one, the same refusal 'start' and 'cutover' print.
func (m WatchModel) changeGateLines(operation string, err error) ([]string, bool) {
	apiErr, ok := errors.AsType[*client.APIError](err)
	if !ok {
		return nil, false
	}
	switch {
	case apiErr.Status == http.StatusConflict && apiErr.ErrorCode == apitypes.ErrCodeChangeGateRefused:
		lines := templates.ChangeGateRefusedText(templates.ChangeGateRefusedData{
			Environment:     m.environment,
			Operation:       operation,
			Blockers:        apiErr.Blockers,
			OverrideRequest: apiErr.OverrideRequest,
			Message:         apiErr.Message,
			OverrideCommand: templates.ControlCommand(operation, m.applyID, m.environment),
		})
		if apiErr.OverrideOnReason {
			return append(lines, changeGateOnReasonHint), true
		}
		return append(lines, changeGateRetryHint), true
	case apiErr.ErrorCode == apitypes.ErrCodeChangeGateUnavailable:
		return []string{"✗ SchemaBot could not check the freezes, so nothing started. Press Enter to try again in a moment."}, true
	}
	return nil, false
}

// changeGateFooter renders the gate's last answer below the keys the footer
// offers, its first line in the error color.
func (m WatchModel) changeGateFooter() string {
	if len(m.changeGateAnswer) == 0 {
		return ""
	}
	errStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("9"))
	var b strings.Builder
	b.WriteString("\n")
	b.WriteString(errStyle.Render(m.changeGateAnswer[0]) + "\n")
	for _, line := range m.changeGateAnswer[1:] {
		b.WriteString(line + "\n")
	}
	return b.String()
}
