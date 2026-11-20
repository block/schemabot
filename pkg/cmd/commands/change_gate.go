package commands

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/internal/templates"
)

// reportChangeGate renders the server's change gate answers to an apply, start
// or cutover and passes every other error through. A freeze refusal is printed
// once as the refusal template, and the error it returns is silent so main does
// not print a second line. A refusal is a failure, not a retry: the same
// request is refused again until the freeze ends or an approver acts.
//
// quiet keeps stdout clean for a caller parsing it (-o json): the refusal then
// travels in the error, which main writes to stderr.
func reportChangeGate(err error, environment, operation string, overrideAsked, quiet bool) error {
	apiErr, ok := errors.AsType[*client.APIError](err)
	if !ok {
		return err
	}
	switch {
	case apiErr.Status == http.StatusConflict && apiErr.ErrorCode == apitypes.ErrCodeChangeGateRefused:
		data := templates.ChangeGateRefusedData{
			Environment:     environment,
			Operation:       operation,
			Blockers:        apiErr.Blockers,
			OverrideRequest: apiErr.OverrideRequest,
			OverrideAsked:   overrideAsked,
			Message:         apiErr.Message,
		}
		if quiet {
			return &changeGateRefusal{text: strings.Join(templates.ChangeGateRefusedText(data), " "), cause: err}
		}
		templates.WriteChangeGateRefused(data)
		return fmt.Errorf("%w: %w", ErrSilent, err)
	case apiErr.ErrorCode == apitypes.ErrCodeChangeGateUnavailable:
		return fmt.Errorf("SchemaBot could not check the freezes, so nothing started; try again in a moment: %w", err)
	}
	return err
}

// changeGateRefusal is a refusal reported in the error, for -o json. Its text
// is the template's, which keeps server strings terminal-safe; the API error
// stays reachable through Unwrap for lock cleanup but is never printed.
type changeGateRefusal struct {
	text  string
	cause error
}

func (e *changeGateRefusal) Error() string { return e.text }
func (e *changeGateRefusal) Unwrap() error { return e.cause }
