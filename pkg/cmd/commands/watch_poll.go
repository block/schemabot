package commands

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/block/schemabot/pkg/apitypes"
	"github.com/block/schemabot/pkg/cmd/client"
	"github.com/block/schemabot/pkg/cmd/cliname"
	"github.com/block/schemabot/pkg/state"
)

// maxConsecutiveProgressFailures bounds how many progress polls in a row a
// non-interactive watch tolerates failing transiently before it gives up. With
// the fetch-error backoff this rides out a server restart or a brief network
// partition, while an endpoint that stays unreachable still ends the watch
// instead of leaving a CI job polling forever.
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
			return nil, fmt.Errorf("fetch progress for apply %s: %d consecutive attempts failed; the schema change continues on the server, resume watching with '%s progress %s': %w",
				p.applyID, failures, cliname.Name(), p.applyID, err)
		}
		wait := progressRetryWait(err, failures)
		onRetry(progressRetry{err: err, attempt: failures, wait: wait})
		p.sleep(wait)
	}
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
// reaches a terminal state. A completed apply succeeds. A stopped apply also
// exits cleanly: the stop was an operator's deliberate pause and the apply can
// be started again. Every other terminal state (failed, cancelled, reverted, or
// one added later) means the schema change is not on the target, so the watch
// fails and a script gating on its exit status does not proceed. The watch has
// already rendered the outcome, so the error is silent.
func terminalWatchExit(applyState string) error {
	switch {
	case state.IsState(applyState, state.Apply.Completed):
		return nil
	case state.IsState(applyState, state.Apply.Stopped):
		return nil
	default:
		return ErrSilent
	}
}
