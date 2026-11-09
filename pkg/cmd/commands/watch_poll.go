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
	"github.com/block/schemabot/pkg/cmd/internal/templates"
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
//
// environment is the environment the command that started the watch targeted,
// which its hints carry so a pasted command reaches the same control plane.
// Empty when the command did not name one.
type progressPoller struct {
	ctx         context.Context
	applyID     string
	environment string
	fetch       func() (*apitypes.ProgressResponse, error)
	sleep       func(time.Duration)
}

func newProgressPoller(ctx context.Context, endpoint, applyID, environment string) *progressPoller {
	return &progressPoller{
		ctx:         ctx,
		applyID:     applyID,
		environment: environment,
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
// stays machine-readable, and the error is silent so the CLI exits with the
// interrupt status without repeating it as a raw "context canceled" line.
func (p *progressPoller) watchStopped() error {
	fmt.Fprintln(os.Stderr, progressStoppedMessage(p.applyID, p.environment))
	return interrupted(p.ctx)
}

// progressRetry describes one transient progress failure the poller is about
// to wait out, so each surface can report it in its own format.
type progressRetry struct {
	err     error
	attempt int
	wait    time.Duration

	// cappedFrom is the longer delay a Retry-After header asked for when the
	// CLI's bound shortened the wait below it, and zero otherwise.
	cappedFrom time.Duration
}

// next returns the apply's current progress. A transient failure (the server
// unreachable, or an API error code marked retryable) is reported through
// onRetry and retried with backoff, up to maxConsecutiveProgressFailures in a
// row. A permanent failure, or one transient failure too many, is returned.
// Watching is read-only, so giving up never affects the apply itself; the
// returned error says how to resume watching it. A fetch or wait that the
// operator's Ctrl-C cut short is reported as the watch stopping, never as a
// failed fetch to retry. A fetch that answered before the Ctrl-C landed is
// still returned: the frame may be the apply's final state, which the
// operator pressed Ctrl-C believing they would not see, and the caller's next
// wait reports the stop.
func (p *progressPoller) next(onRetry func(progressRetry)) (*apitypes.ProgressResponse, error) {
	for failures := 1; ; failures++ {
		result, err := p.fetch()
		if err == nil {
			return result, nil
		}
		if p.ctx.Err() != nil {
			return nil, p.watchStopped()
		}
		if !isRetryableFetchError(err) {
			return nil, fmt.Errorf("fetch progress for apply %s: %w", p.applyID, err)
		}
		if failures >= maxConsecutiveProgressFailures {
			return nil, fmt.Errorf("%s: %w", progressGiveUpMessage(p.applyID, p.environment, failures), err)
		}
		wait := progressRetryWait(err, failures)
		onRetry(progressRetry{err: err, attempt: failures, wait: wait, cappedFrom: cappedRetryDelay(err, wait)})
		if err := p.pause(wait); err != nil {
			return nil, err
		}
	}
}

// progressGiveUpMessage is what every watch surface reports once progress has
// stayed unreadable for this many polls in a row. The watcher cannot see the
// apply at that point, so the message claims nothing about its state and
// points at the commands that will show it.
func progressGiveUpMessage(applyID, environment string, failures int) string {
	return fmt.Sprintf("fetch progress for apply %s: %d consecutive attempts failed; this watch does not affect the apply; %s",
		applyID, failures, resumeWatchHint(applyID, environment))
}

// progressStoppedMessage is what every non-interactive watch reports when the
// operator stops it with Ctrl-C. Only the watch ends; the apply carries on.
func progressStoppedMessage(applyID, environment string) string {
	return fmt.Sprintf("Stopped watching apply %s; stopping the watch does not affect the apply; %s",
		applyID, resumeWatchHint(applyID, environment))
}

// resumeWatchHint names the command that watches an apply this watch can no
// longer follow. It names only a read: the command the operator ran to start
// this watch was an apply, a rollback, or a cutover, and running that again
// would submit a second one rather than show the first.
func resumeWatchHint(applyID, environment string) string {
	return fmt.Sprintf("watch it again with '%s'", templates.ProgressCommand(applyID, environment))
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

// cappedRetryDelay is the delay a failed fetch's Retry-After header asked for
// when the CLI's bound shortened it and the watch will poll again before that
// delay is up. Zero when the watch waits at least as long as was asked.
func cappedRetryDelay(err error, wait time.Duration) time.Duration {
	var apiErr *client.APIError
	if errors.As(err, &apiErr) && apiErr.RetryAfterHeaderUncapped > wait {
		return apiErr.RetryAfterHeaderUncapped
	}
	return 0
}

// maxReportedRetryDelay is the longest capped Retry-After delay the retry
// line states exactly. A header too far out to read, such as a count of
// seconds that overflows or a year-9999 date, parses to a saturated duration
// the server never named, so past this ceiling the line says only that the
// delay was more than a day.
const maxReportedRetryDelay = 24 * time.Hour

// formatCappedRetryDelay renders the delay a capped Retry-After header asked
// for, exactly up to maxReportedRetryDelay and as "more than 24h" beyond it.
func formatCappedRetryDelay(d time.Duration) string {
	if d > maxReportedRetryDelay {
		return "more than 24h"
	}
	return d.String()
}

// printProgressRetry reports a transient progress failure on stderr, keeping
// stdout to the watch's own output so a JSON stream stays machine-readable.
// When the CLI's bound shortened the delay the server asked for, the line says
// so, so the operator knows the next poll comes sooner than asked.
func printProgressRetry(r progressRetry) {
	wait := r.wait.String()
	if r.cappedFrom > 0 {
		wait += fmt.Sprintf(" (server asked for %s, capped)", formatCappedRetryDelay(r.cappedFrom))
	}
	fmt.Fprintf(os.Stderr, "Progress unavailable (attempt %d/%d), retrying in %s: %v\n",
		r.attempt, maxConsecutiveProgressFailures, wait, r.err)
}

// applyEndExit is the exit every watch, interactive or not, reports once it has
// followed an apply to where it ends. Only a completed apply succeeds. Every
// other end (stopped, failed, cancelled, reverted, one added later, or no
// active schema change) means the schema change is not on the target, so the
// watch fails and a script gating on its exit status does not proceed. The
// outcome is already on screen, so the error is silent.
func applyEndExit(applyState string) error {
	if state.IsState(applyState, state.Apply.Completed) {
		return nil
	}
	return ErrSilent
}

// terminalWatchExit is the exit a non-interactive watch reports once the apply
// reaches a terminal state, which is applyEndExit's. A stopped apply can be
// resumed and the log has no view to say how, so its error does; the other
// outcomes are already rendered.
func terminalWatchExit(result *apitypes.ProgressResponse) error {
	if state.IsState(result.State, state.Apply.Stopped) {
		return fmt.Errorf("apply %s was stopped, so the schema change is not on the target; to resume from where it stopped, run '%s'",
			result.ApplyID, startCommand(result.ApplyID, result.Environment))
	}
	return applyEndExit(result.State)
}

// startCommand is the command that resumes a stopped apply. start requires an
// environment, so a placeholder stands in when the response did not name one.
func startCommand(applyID, environment string) string {
	if environment == "" {
		environment = "<environment>"
	}
	return templates.ControlCommand("start", applyID, environment)
}

// noActiveChangeError ends a watch whose apply the server reports no active
// schema change for. Progress by apply ID carries the apply's stored state, so
// this answer means the watcher never saw how the apply ended; it fails rather
// than guessing, and points at a command that will show the apply.
func noActiveChangeError(applyID, environment string) error {
	return fmt.Errorf("progress for apply %s reported no active schema change, so this watch cannot tell how the apply ended; check '%s'",
		applyID, templates.ProgressCommand(applyID, environment))
}
