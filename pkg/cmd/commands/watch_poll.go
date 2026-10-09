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
//
// ctx is the command's run context, which the first Ctrl-C cancels. Both the
// fetch and the wait end as soon as it is done, and the poller then reports
// that the operator stopped the watch.
type progressPoller struct {
	ctx     context.Context
	applyID string
	fetch   func() (*apitypes.ProgressResponse, error)
	sleep   func(time.Duration)
}

func newProgressPoller(ctx context.Context, endpoint, applyID string) *progressPoller {
	return &progressPoller{
		ctx:     ctx,
		applyID: applyID,
		fetch: func() (*apitypes.ProgressResponse, error) {
			return client.GetProgressCtx(ctx, endpoint, applyID)
		},
		sleep: func(d time.Duration) { sleepUnlessDone(ctx, d) },
	}
}

// sleepUnlessDone waits for d, or until ctx is done if that comes first.
func sleepUnlessDone(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

// pause waits d between polls. It returns the stopped-watching error once the
// operator has cancelled the watch, whether before or during the wait.
func (p *progressPoller) pause(d time.Duration) error {
	p.sleep(d)
	if p.ctx.Err() != nil {
		return p.watchStopped()
	}
	return nil
}

// watchStopped tells the operator their Ctrl-C ended only the watch, and how
// to pick it up again. The notice goes to stderr so a JSON stream on stdout
// stays machine-readable, and the error is silent so the CLI exits non-zero
// without repeating it as a raw "context canceled" line.
func (p *progressPoller) watchStopped() error {
	fmt.Fprintln(os.Stderr, progressStoppedMessage(p.applyID))
	return fmt.Errorf("%w: %w", ErrSilent, p.ctx.Err())
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
// returned error says how to resume watching it. A fetch or wait that the
// operator's Ctrl-C cut short is reported as the watch stopping, never as a
// failed fetch to retry.
func (p *progressPoller) next(onRetry func(progressRetry)) (*apitypes.ProgressResponse, error) {
	for failures := 1; ; failures++ {
		result, err := p.fetch()
		if p.ctx.Err() != nil {
			return nil, p.watchStopped()
		}
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
		if err := p.pause(wait); err != nil {
			return nil, err
		}
	}
}

// progressGiveUpMessage is what every watch surface reports once progress has
// stayed unreadable for this many polls in a row. The watcher cannot see the
// apply at that point, so the message claims nothing about its state and
// points at the commands that will show it.
func progressGiveUpMessage(applyID string, failures int) string {
	return fmt.Sprintf("fetch progress for apply %s: %d consecutive attempts failed; this watch does not affect the apply; %s",
		applyID, failures, resumeWatchHint(applyID))
}

// progressStoppedMessage is what every non-interactive watch reports when the
// operator stops it with Ctrl-C. Only the watch ends; the apply carries on.
func progressStoppedMessage(applyID string) string {
	return fmt.Sprintf("Stopped watching apply %s; stopping the watch does not affect the apply; %s",
		applyID, resumeWatchHint(applyID))
}

// resumeWatchHint names the commands that show an apply the watch can no
// longer follow.
func resumeWatchHint(applyID string) string {
	return fmt.Sprintf("rerun the original watch command, or '%s progress %s', to see its current state", cliname.Name(), applyID)
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

// progressRetryWait is the wait before polling again after a failure the
// watch has already judged retryable: the fetch-error backoff for this many
// consecutive failures, stretched to the longest delay the response asked for.
// The delay can come from the body's retry_after_seconds or from a Retry-After
// header, which a proxy or rate limiter may send without a SchemaBot error
// code. When both are present the larger wins, so a rate-limited watch never
// polls sooner than either asked.
func progressRetryWait(err error, consecutiveErrors int) time.Duration {
	return max(fetchErrorBackoff(consecutiveErrors), requestedRetryDelay(err))
}

// requestedRetryDelay is the delay a failed fetch's response asked for: the
// longer of the body's and the Retry-After header's when the error code is
// retryable, and the header alone for a response with no SchemaBot error code,
// as a proxy in front of the server sends. Zero when nothing was asked for.
func requestedRetryDelay(err error) time.Duration {
	var apiErr *client.APIError
	if !errors.As(err, &apiErr) {
		return 0
	}
	if retry, after := apiErr.RetryAfter(); retry {
		return after
	}
	return apiErr.RetryAfterHeader
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
