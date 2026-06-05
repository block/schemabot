package github

import (
	"context"
	"time"

	"golang.org/x/sync/singleflight"
)

// CheckStatusRow is the identity-independent check/status data the singleflight
// shares across waiters. It deliberately omits IsSchemaBot because that is
// derived from the calling InstallationClient's appSlug, which is resolved at
// construction time and may differ across the short-lived InstallationClients
// that share this coalescer (e.g. when the slug was unavailable at startup and
// later recovered). The owning InstallationClient projects to PRCheckStatus on
// every read.
type CheckStatusRow struct {
	Name       string
	Status     string
	Conclusion string
	AppSlug    string // empty for legacy commit statuses (StatusContext nodes have no App)
}

// CheckStatusSingleflight coalesces concurrent check/status fetches
// for the same logical key into a single upstream GitHub read via
// singleflight. It does not memoise across calls: once the in-flight
// fetch completes, the next call issues a fresh request. This preserves
// the burst-collapsing win (a webhook delivery that fans out to multiple
// gate checks for the same commit makes one round trip) without ever
// returning stale check state — check status for a SHA is mutable
// (reruns, late-arriving checks, branch-protection adding required
// checks), so any TTL window would risk converting a now-failing gate
// into a passing one.
//
// One instance is owned by the Client factory and shared across every
// InstallationClient it produces, so concurrent fetches actually collapse
// across the short-lived InstallationClients spawned per webhook
// delivery — which is the dedup pattern that justifies this layer.
// Identity-dependent classification (IsSchemaBot) is re-derived per call
// by the reading InstallationClient, so a shared fetch delivered to N
// waiters with different appSlug snapshots is classified correctly for
// each.
type CheckStatusSingleflight struct {
	group singleflight.Group
}

// NewCheckStatusSingleflight constructs an empty coalescer.
func NewCheckStatusSingleflight() *CheckStatusSingleflight {
	return &CheckStatusSingleflight{}
}

// sharedFetchTimeout bounds the singleflight shared fetch when it is
// decoupled from individual callers' deadlines. It mirrors the
// InstallationClient's underlying http.Client timeout so the shared
// fetch cannot outlive the GitHub round trip it wraps.
const sharedFetchTimeout = 30 * time.Second

// Do invokes fetch for (repo, key), collapsing concurrent callers for
// the same key into a single fetch invocation. Each caller observes its
// own ctx for cancellation/deadline: a caller whose ctx fires while
// waiting on another caller's in-flight fetch returns promptly with
// ctx.Err(), without aborting the shared fetch (other waiters and
// future callers can still receive its result).
//
// The shared fetch runs on a ctx that is decoupled from any individual
// caller's cancellation — built via context.WithoutCancel so the
// leader's values (tracing/logging) are preserved while its
// cancellation and deadline are stripped, then bounded by
// sharedFetchTimeout. This means a caller cancelling or timing out —
// including the caller that won the singleflight — cannot abort the
// shared GitHub request and fail unrelated waiters whose own contexts
// are still valid.
func (c *CheckStatusSingleflight) Do(ctx context.Context, repo, key string, fetch func(context.Context) ([]CheckStatusRow, error)) ([]CheckStatusRow, error) {
	key = repo + "@" + key

	ch := c.group.DoChan(key, func() (any, error) {
		// Decouple the shared fetch from the caller's ctx so a leader's
		// cancellation or timeout does not fail unrelated waiters.
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sharedFetchTimeout)
		defer cancel()
		return fetch(fetchCtx)
	})

	select {
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.([]CheckStatusRow), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
