package commands

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/cliname"
	"github.com/block/schemabot/pkg/state"
)

// maxConsecutiveProgressFailures bounds how many progress polls in a row any
// watch, interactive or not, tolerates failing transiently before it gives up.
// With the fetch-error backoff this rides out a server restart or a brief
// network partition, while an endpoint that stays unreachable still ends the
// watch instead of leaving a CI job or a terminal polling forever.
const maxConsecutiveProgressFailures = 10

// progressPoller fetches one apply's progress for the non-interactive watch
// loops (log, JSON, and post-cutover). It owns every wait those loops make, so
// a test can drive a loop through its states without real time passing.
type progressPoller struct {
	applyID string
	fetch   func() (*apitypes.ProgressResponse, error)
	sleep   func(time.Duration)
}

func newProgressPoller(endpoint, applyID string) *progressPoller {
	return &progressPoller{
		applyID: applyID,
		fetch: func() (*apitypes.ProgressResponse, error) {
			return client.GetProgress(endpoint, applyID)
		},
		sleep: time.Sleep,
	}
}

// progressRetry describes one transient progress failure the poller is about
// to wait out, so each surface can report it in its own format.
type progressRetry struct {
	err     error
	attempt int
	wait    time.Duration
}

// next returns the apply's current progress. A transient failure (the server
// unreachable, or an API error code marked retryable) is reported through
// onRetry and retried with backoff, up to maxConsecutiveProgressFailures in a
// row. A permanent failure, or one transient failure too many, is returned.
// Watching is read-only, so giving up never affects the apply itself; the
// returned error says how to resume watching it.
func (p *progressPoller) next(onRetry func(progressRetry)) (*apitypes.ProgressResponse, error) {
	for failures := 1; ; failures++ {
		result, err := p.fetch()
		if err == nil {
			return result, nil
		}
		if !isRetryableFetchError(err) {
			return nil, fmt.Errorf("fetch progress for apply %s: %w", p.applyID, err)
		}
		if failures >= maxConsecutiveProgressFailures {
			return nil, fmt.Errorf("%s: %w", progressGiveUpMessage(p.applyID, failures), err)
		}
		wait := progressRetryWait(err, failures)
		onRetry(progressRetry{err: err, attempt: failures, wait: wait})
		p.sleep(wait)
	}
}

// progressGiveUpMessage is what every watch surface reports once progress has
// stayed unreadable for this many polls in a row. The watcher cannot see the
// apply at that point, so the message claims nothing about its state and
// points at the commands that will show it.
func progressGiveUpMessage(applyID string, failures int) string {
	return fmt.Sprintf("fetch progress for apply %s: %d consecutive attempts failed; this watch does not affect the apply; rerun the original watch command, or '%s progress %s', to see its current state",
		applyID, failures, cliname.Name(), applyID)
}

// isRetryableFetchError reports whether a failed progress fetch is worth
// polling again: the server could not be reached, the response was cut off,
// or the server answered with an error it marks transient.
func isRetryableFetchError(err error) bool {
	if isPermanentClientSideFailure(err) {
		return false
	}
	var connErr *client.ConnectionError
	if errors.As(err, &connErr) {
		return true
	}
	var apiErr *client.APIError
	if errors.As(err, &apiErr) {
		if apiErr.ErrorCode != "" {
			return apitypes.IsRetryableErrorCode(apiErr.ErrorCode)
		}
		return isRetryableStatusWithoutCode(apiErr.Status)
	}
	// A response body that failed partway through, such as a connection reset
	// or a read timeout after the headers arrived. Failures to send the request
	// arrive as a ConnectionError above.
	var netErr net.Error
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.As(err, &netErr)
}

// isPermanentClientSideFailure reports a failure the CLI produced on its own
// side that no retry can change: the operator cancelled the watch, or the
// client refused to send the auth token over a plaintext connection. Both can
// wrap a net.Error, so they are ruled out before the transport checks.
func isPermanentClientSideFailure(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, client.ErrInsecureTokenTransport)
}

// isRetryableStatusWithoutCode classifies an error response that carries no
// SchemaBot error code, which is what a proxy or load balancer in front of the
// server sends: a 5xx while the server restarts, or a 429 from a rate limiter.
func isRetryableStatusWithoutCode(status int) bool {
	return status >= http.StatusInternalServerError || status == http.StatusTooManyRequests
}

// progressRetryWait is the fetch-error backoff for this many consecutive
// failures, stretched to any delay the server asked for so a rate-limited
// watch never polls sooner than it was told to.
func progressRetryWait(err error, consecutiveErrors int) time.Duration {
	wait := fetchErrorBackoff(consecutiveErrors)
	var apiErr *client.APIError
	if errors.As(err, &apiErr) {
		if retry, after := apiErr.RetryAfter(); retry && after > wait {
			wait = after
		}
	}
	return wait
}

// printProgressRetry reports a transient progress failure on stderr, keeping
// stdout to the watch's own output so a JSON stream stays machine-readable.
func printProgressRetry(r progressRetry) {
	fmt.Fprintf(os.Stderr, "Progress unavailable (attempt %d/%d), retrying in %s: %v\n",
		r.attempt, maxConsecutiveProgressFailures, r.wait, r.err)
}

// terminalWatchExit is the exit a non-interactive watch reports once the apply
// reaches a terminal state. Only a completed apply succeeds. Every other
// terminal state (stopped, failed, cancelled, reverted, or one added later)
// means the schema change is not on the target, so the watch fails and a
// script gating on its exit status does not proceed. A stopped apply can be
// resumed, so its error says how; the other outcomes are already rendered, so
// their error is silent.
func terminalWatchExit(result *apitypes.ProgressResponse) error {
	switch {
	case state.IsState(result.State, state.Apply.Completed):
		return nil
	case state.IsState(result.State, state.Apply.Stopped):
		return fmt.Errorf("apply %s was stopped, so the schema change is not on the target; use '%s' to resume it",
			result.ApplyID, startCommand(result.ApplyID, result.Environment))
	default:
		return ErrSilent
	}
}

// startCommand is the command that resumes a stopped apply. start requires an
// environment, so a placeholder stands in when the response did not name one.
func startCommand(applyID, environment string) string {
	if environment == "" {
		environment = "<environment>"
	}
	return fmt.Sprintf("%s start -e %s %s", cliname.Name(), environment, applyID)
}

// noActiveChangeError ends a watch whose apply the server reports no active
// schema change for. Progress by apply ID carries the apply's stored state, so
// this answer means the watcher never saw how the apply ended; it fails rather
// than guessing, and points at a command that will show the apply.
func noActiveChangeError(applyID string) error {
	return fmt.Errorf("progress for apply %s reported no active schema change, so this watch cannot tell how the apply ended; check '%s progress %s'",
		applyID, cliname.Name(), applyID)
}
